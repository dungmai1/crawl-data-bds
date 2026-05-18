"""
V-Nexus: Nhatot Fast Scraper — Tối ưu tốc độ tối đa
Thiết kế cho cào liên tục mỗi 1 giờ

Kiến trúc 2 tầng:
  Tầng 1 (NHANH): API gateway → lấy listings + data (0.3s/request, ~3000 listings/phút)
  Tầng 2 (CHẬM): Playwright → chỉ reveal SĐT cho listings MỚI (~7s/listing)

Tối ưu:
  - API layer: async httpx, 10 concurrent requests
  - Phone layer: N browser tabs song song (default 5)
  - Chỉ reveal phone cho listings CHƯA CÓ trong DB
  - Browser pool: reuse browser, mở nhiều tabs
  - Skip expired/duplicate listings

Usage:
    python nhatot_fast_scraper.py                     # Cào mới, 5 tabs
    python nhatot_fast_scraper.py --tabs 10           # 10 tabs song song
    python nhatot_fast_scraper.py --api-only           # Chỉ cào API, không reveal phone
    python nhatot_fast_scraper.py --loop               # Chạy liên tục mỗi 1h
"""

import asyncio
import json
import re
import os
import time
import logging
import unicodedata
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path
import sys

_pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)
from unified_pipeline import process_batch, AddressMapper

try:
    import httpx
except ImportError:
    print("pip install httpx")
    exit(1)

from config import log_path

# Persistent phone cache: tracks mọi list_id đã thử reveal (cả thành công lẫn thất bại)
# Format: {"list_id_as_str": "0912345678" | null}
# null = đã thử nhưng không reveal được → không retry để tránh phí thời gian
PHONE_CACHE_FILE = Path("data/phone_cache.json")


def load_phone_cache() -> dict:
    """Load persistent phone cache. Returns {list_id_str: phone|null}."""
    if not PHONE_CACHE_FILE.exists():
        return {}
    try:
        with open(PHONE_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Phone cache load failed: {e}")
        return {}


def save_phone_cache(cache: dict):
    """Save persistent phone cache atomically."""
    PHONE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PHONE_CACHE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    tmp.replace(PHONE_CACHE_FILE)


# Failed URLs log: ghi append-only mọi listing reveal phone thất bại
# Format: 1 JSON object/dòng (JSONL) — an toàn cả khi process crash giữa cycle
FAILED_URLS_FILE = Path("data/failed_phone_urls.jsonl")


def log_failed_url(list_id, url: str, reason: str, **extra):
    """Append failed URL record to JSONL log for later investigation."""
    FAILED_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now().isoformat(),
        "list_id": list_id,
        "url": url,
        "reason": reason,
        **extra,
    }
    try:
        with open(FAILED_URLS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.debug(f"Cannot write failed_urls log: {e}")


def _vn_slug(text: str) -> str:
    """Convert Vietnamese text to URL-safe slug (no diacritics, lowercase, dash-separated)."""
    nfkd = unicodedata.normalize("NFKD", text)
    no_accent = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_accent.replace("đ", "d").replace("Đ", "d").lower().replace(" ", "-")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_path("nhatot"), encoding="utf-8"),
    ],
)
log = logging.getLogger("nhatot")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# Chotot gateway phân biệt sale vs rent qua query param `st`:
#   st=s → mua bán (ad.type='s')
#   st=u → cho thuê (ad.type='u')
# Categories DÙNG CHUNG cho cả 2 mode: 1010 Chung cư, 1020 Nhà ở, 1030 VP/MB, 1040 Đất.
# Riêng 1050 (Phòng trọ) chỉ tồn tại ở mode cho thuê.
SALE_CATEGORIES = [1020, 1010, 1040, 1030]
RENT_CATEGORIES = [1020, 1010, 1040, 1030, 1050]
ST_BY_TRANSACTION = {"ban": "s", "cho-thue": "u"}

# ===========================================================
# TẦNG 1: API SCRAPER (NHANH — ~3000 listings/phút)
# ===========================================================

async def scrape_api_batch(
    region: int = 13000,
    max_listings: int = 200,
    categories: list = None,
    offset_shift: int = 0,
    transaction_type: str = "ban",
) -> list[dict]:
    """Cào listings từ gateway API. Async, nhanh, không cần browser.

    transaction_type: "ban" (mua bán) | "cho-thue" — chọn category mặc định
    offset_shift: cộng thêm vào offset để lấy page ads khác (cho accumulator mode).
    """

    if categories is None:
        categories = RENT_CATEGORIES if transaction_type == "cho-thue" else SALE_CATEGORIES
    st = ST_BY_TRANSACTION.get(transaction_type, "s")

    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        tasks = []
        for cg in categories:
            # API trả 50/call — đảm bảo ít nhất 1 request mỗi category
            per_cat = max(max_listings // len(categories), 50)
            for offset in range(0, per_cat, 50):
                real_offset = offset + offset_shift
                url = (
                    f"https://gateway.chotot.com/v1/public/ad-listing"
                    f"?cg={cg}&limit=50&o={real_offset}&st={st}&region_v2={region}"
                )
                tasks.append((cg, real_offset, client.get(url)))

        # Run 10 concurrent API calls
        sem = asyncio.Semaphore(10)

        async def fetch(cg, offset, coro):
            async with sem:
                try:
                    resp = await coro
                    ads = resp.json().get("ads", [])
                    return ads
                except Exception as e:
                    log.warning(f"API error cg={cg} o={offset}: {e}")
                    return []

        results = await asyncio.gather(
            *[fetch(cg, off, coro) for cg, off, coro in tasks]
        )

    # Group results theo category
    cat_to_ads = {cg: [] for cg in categories}
    for (cg, _, _), ads in zip(tasks, results):
        cat_to_ads[cg].extend(ads)

    # Round-robin: mỗi vòng lấy 1 ad từ từng category → phân phối đều kể cả khi max_listings nhỏ
    all_ads = []
    seen_ids = set()
    dist = {cg: 0 for cg in categories}
    iters = {cg: iter(cat_to_ads[cg]) for cg in categories}
    active = list(categories)
    while active and len(all_ads) < max_listings:
        for cg in list(active):
            if len(all_ads) >= max_listings:
                break
            while True:
                try:
                    ad = next(iters[cg])
                except StopIteration:
                    active.remove(cg)
                    break
                aid = ad.get("ad_id")
                if aid and aid not in seen_ids:
                    seen_ids.add(aid)
                    all_ads.append(ad)
                    dist[cg] += 1
                    break

    log.info(f"API [{transaction_type}]: {len(all_ads)} unique listings — per-cat: {dist}")
    return all_ads


def _build_detail_url(ad: dict) -> str:
    """Build nhatot detail URL theo loại giao dịch (slug đúng path tránh redirect).

    Cho thuê: cho-thue-nha-dat-{district}-tp-ho-chi-minh
    Mua bán: mua-ban-nha-dat-{district}-tp-ho-chi-minh
    Slug chỉ cần khớp section (sale/rent) — server tìm listing bằng list_id.
    """
    list_id = ad.get("list_id", "")
    district = _vn_slug(ad.get("area_name", ""))
    # Pipeline gán cho-thue khi raw["type"] in ("k","u") — match đây để consistent
    is_rent = ad.get("type") in ("k", "u")
    prefix = "cho-thue-nha-dat" if is_rent else "mua-ban-nha-dat"
    return f"https://www.nhatot.com/{prefix}-{district}-tp-ho-chi-minh/{list_id}.htm"


async def enrich_with_detail(ads: list[dict], max_concurrent: int = 10) -> list[dict]:
    """Bổ sung data từ detail API (masked phone, extra fields)."""

    sem = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:

        async def fetch_detail(ad):
            lid = ad.get("list_id")
            if not lid:
                return ad
            async with sem:
                try:
                    resp = await client.get(
                        f"https://gateway.chotot.com/v1/public/ad-listing/{lid}"
                    )
                    detail = resp.json().get("ad", {})
                    ad["_detail"] = {
                        "body": detail.get("body", ""),
                        "street_name": detail.get("street_name", ""),
                        "ward_name": detail.get("ward_name", ""),
                        "property_legal_document": detail.get("property_legal_document"),
                    }
                except Exception as e:
                    log.warning(f"Detail error {lid}: {e}")
            return ad

        enriched = await asyncio.gather(*[fetch_detail(ad) for ad in ads])

    log.info(f"Detail: enriched {len(enriched)} listings")
    return enriched


# ===========================================================
# TẦNG 2: PHONE REVEALER (Playwright, song song tabs)
# ===========================================================

async def reveal_phones_batch(
    ads: list[dict],
    num_tabs: int = 5,
    existing_phones: set = None,
    batch_size: int = 50,
    cooldown_seconds: int = 15,
) -> dict[int, str]:
    """Reveal SĐT cho nhiều listings cùng lúc bằng nhiều tabs.

    Restart browser context sau mỗi `batch_size` reveals + cooldown `cooldown_seconds`s
    để tránh shadow-ban từ chotot anti-bot (sau ~150 reveals server strip button).
    """
    if existing_phones is None:
        existing_phones = set()

    # Filter: chỉ reveal cho listings chưa có phone
    to_reveal = [
        ad for ad in ads
        if ad.get("list_id") and ad.get("list_id") not in existing_phones
    ]

    if not to_reveal:
        log.info("Phone: no new listings to reveal")
        return {}

    log.info(f"Phone: {len(to_reveal)} listings to reveal with {num_tabs} tabs")

    # url_map rỗng — mọi listing dùng fallback URL (server tìm listing bằng list_id, slug chỉ là SEO)
    url_map = {}

    phone_results = {}  # list_id -> phone, accumulated across all batches
    total = len(to_reveal)
    n_batches = (total + batch_size - 1) // batch_size
    log.info(
        f"Phone: split {total} reveals into {n_batches} batch(es) of {batch_size}, "
        f"cooldown={cooldown_seconds}s between batches"
    )

    for batch_idx in range(n_batches):
        batch_start = batch_idx * batch_size
        current_batch = to_reveal[batch_start:batch_start + batch_size]
        log.info(
            f"=== Browser batch {batch_idx + 1}/{n_batches}: "
            f"ads {batch_start + 1}-{batch_start + len(current_batch)} of {total} ==="
        )

        await _process_phone_batch(
            current_batch=current_batch,
            batch_offset=batch_start,
            total=total,
            num_tabs=num_tabs,
            url_map=url_map,
            phone_results=phone_results,
        )

        # Cooldown trước batch tiếp theo (skip nếu là batch cuối)
        if batch_idx + 1 < n_batches:
            log.info(
                f"  Phones cumulative: {len(phone_results)} | "
                f"Cooldown {cooldown_seconds}s before next browser session..."
            )
            await asyncio.sleep(cooldown_seconds)

    log.info(f"Phone: revealed {len(phone_results)}/{len(to_reveal)} phones")
    return phone_results


async def _process_phone_batch(
    current_batch: list[dict],
    batch_offset: int,
    total: int,
    num_tabs: int,
    url_map: dict,
    phone_results: dict,
):
    """Process 1 batch of listings với fresh browser context.

    Mục đích: restart browser giữa các batch để tránh shadow-ban từ chotot anti-bot.
    `phone_results` được mutate in-place để accumulate cross-batch.
    """
    from playwright.async_api import async_playwright

    queue = asyncio.Queue()
    for i, ad in enumerate(current_batch):
        queue.put_nowait((batch_offset + i + 1, ad))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 800},
            locale="vi-VN",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )

        # Create tab workers
        async def tab_worker(tab_id):
            page = await context.new_page()
            processed = 0

            while not queue.empty():
                try:
                    idx, ad = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                list_id = ad["list_id"]
                title = ad.get("subject", "")[:35]
                # `total` đã được pass vào từ caller

                # Get URL — fallback chọn slug theo loại giao dịch để khỏi bị redirect
                detail_url = url_map.get(list_id) or _build_detail_url(ad)

                phone_found = None
                phone_api_status = None
                phone_api_body = None

                # Intercept
                async def on_resp(response):
                    nonlocal phone_found, phone_api_status, phone_api_body
                    try:
                        if "gateway.chotot.com" in response.url and "/phone" in response.url:
                            phone_api_status = response.status
                            if response.status == 200:
                                body = await response.json()
                                bs = json.dumps(body)
                                full = re.findall(r'"phone"\s*:\s*"(0\d{8,9})"', bs)
                                if full:
                                    phone_found = full[0]
                                else:
                                    phone_api_body = bs[:300]
                                    log.warning(f"[T{tab_id}] phone API 200 nhưng body không có phone: {bs[:200]}")
                            else:
                                try:
                                    err_body = await response.text()
                                    phone_api_body = err_body[:300]
                                    log.warning(f"[T{tab_id}] phone API status={response.status}: {err_body[:200]}")
                                except Exception:
                                    log.warning(f"[T{tab_id}] phone API status={response.status} (không đọc được body)")
                    except Exception as e:
                        log.debug(f"on_resp parse error: {e}")

                page.on("response", on_resp)

                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)

                    # Đợi button "Hiện số" thật sự render (max 8s) — thay cho sleep cố định
                    try:
                        await page.wait_for_function(
                            """() => {
                                return Array.from(document.querySelectorAll('button, a')).some(
                                    el => /hiện số/i.test(el.textContent) && el.offsetWidth > 0
                                );
                            }""",
                            timeout=8000,
                        )
                    except Exception:
                        # Button không xuất hiện trong 8s — có thể expired/captcha/anti-bot
                        pass

                    # Check expired
                    expired = await page.evaluate(
                        "()=>document.body.innerText.includes('không còn tồn tại')"
                    )
                    if expired:
                        log.debug(f"[T{tab_id}] ({idx}/{total}) EXPIRED: {title}")
                        page.remove_listener("response", on_resp)
                        continue

                    # Click reveal button
                    btn = await page.evaluate("""
                        () => {
                            for (const el of document.querySelectorAll('button, a')) {
                                if (/hiện số/i.test(el.textContent) && el.offsetWidth > 0) {
                                    const r = el.getBoundingClientRect();
                                    return {x: r.x + r.width/2, y: r.y + r.height/2};
                                }
                            }
                            return null;
                        }
                    """)

                    if btn:
                        await page.mouse.click(btn["x"], btn["y"])
                        await page.wait_for_timeout(2000)

                        # Fallback: read from button text
                        if not phone_found:
                            visible = await page.evaluate("""
                                () => {
                                    for (const el of document.querySelectorAll('button, a, span')) {
                                        const t = el.textContent.trim();
                                        if (/^0\\d{9}$/.test(t.replace(/\\s/g, ''))) return t.replace(/\\s/g, '');
                                    }
                                    return null;
                                }
                            """)
                            if visible:
                                phone_found = visible

                    if phone_found:
                        phone_results[list_id] = phone_found
                        log.info(
                            f"[T{tab_id}] ({idx}/{total}) OK: {phone_found} | {title}"
                        )
                    else:
                        # Capture chẩn đoán: tại sao không có phone?
                        btn_found = btn is not None
                        try:
                            final_url = page.url
                        except Exception:
                            final_url = None
                        try:
                            page_title = await page.title()
                        except Exception:
                            page_title = None
                        try:
                            has_captcha = await page.evaluate("""
                                () => !!document.querySelector(
                                    'iframe[src*="recaptcha"], iframe[src*="challenge"], iframe[title*="captcha" i]'
                                )
                            """)
                        except Exception:
                            has_captcha = None
                        try:
                            needs_login = await page.evaluate("""
                                () => /vui lòng đăng nhập|please log in/i.test(document.body.innerText.slice(0, 5000))
                            """)
                        except Exception:
                            needs_login = None

                        log.info(
                            f"[T{tab_id}] ({idx}/{total}) NO_PHONE | "
                            f"btn={btn_found} captcha={has_captcha} login={needs_login} "
                            f"api_status={phone_api_status} | {title}"
                        )
                        log_failed_url(
                            list_id,
                            detail_url,
                            reason="no_phone_revealed",
                            api_status=phone_api_status,
                            api_body=phone_api_body,
                            btn_found=btn_found,
                            final_url=final_url,
                            page_title=(page_title[:100] if page_title else None),
                            has_captcha=has_captcha,
                            needs_login=needs_login,
                            used_fallback_url=(url_map.get(list_id) is None),
                            title=title,
                        )

                    processed += 1

                except Exception as e:
                    log.warning(f"[T{tab_id}] ({idx}/{total}) ERROR: {str(e)[:60]}")
                    log_failed_url(
                        list_id,
                        detail_url,
                        reason="exception",
                        error=str(e)[:200],
                        api_status=phone_api_status,
                        title=title,
                    )

                finally:
                    page.remove_listener("response", on_resp)

                # Rate limit per tab
                await asyncio.sleep(1.5)

            await page.close()
            log.info(f"[T{tab_id}] Done — processed {processed}")

        # Launch all tabs
        await asyncio.gather(*[tab_worker(i + 1) for i in range(num_tabs)])
        await browser.close()


# ===========================================================
# MAIN: KẾT HỢP CẢ 2 TẦNG
# ===========================================================

async def run_cycle(
    max_listings: int = 200,
    num_tabs: int = 5,
    batch_size: int = 50,
    api_only: bool = False,
    region: int = 13000,
    offset_shift: int = 0,
    transaction: str = "both",
):
    """Chạy 1 chu kỳ cào: API + Phone reveal.

    transaction: "ban" | "cho-thue" | "both" — chia max_listings cho từng loại khi "both".
    """

    start = time.time()
    log.info(f"\n{'='*60}")
    log.info(f"  CYCLE START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(
        f"  Listings: {max_listings} | Tabs: {num_tabs} | Batch size: {batch_size} | "
        f"API-only: {api_only} | Offset shift: {offset_shift} | Transaction: {transaction}"
    )
    log.info(f"{'='*60}")

    # Tầng 1: API — scrape từng loại giao dịch yêu cầu, chia đều quota
    tx_list = ["ban", "cho-thue"] if transaction == "both" else [transaction]
    per_tx = max(max_listings // len(tx_list), 1)
    ads: list[dict] = []
    for tx in tx_list:
        batch = await scrape_api_batch(
            region=region,
            max_listings=per_tx,
            offset_shift=offset_shift,
            transaction_type=tx,
        )
        ads.extend(batch)
    ads = await enrich_with_detail(ads, max_concurrent=10)

    # Tầng 2: Phone (nếu không phải api-only)
    phone_map = {}
    if not api_only and ads:
        phone_map = await reveal_phones_batch(ads, num_tabs, batch_size=batch_size)

    # === MERGE PHONE INTO RAW ADS BEFORE PIPELINE ===
    # Inject phone directly into raw ad dict so pipeline gets full data + phone together
    if phone_map:
        for ad in ads:
            lid = ad.get("list_id")
            if lid and lid in phone_map:
                ad["_phone_full"] = phone_map[lid]

    # Process through unified pipeline (full 35-field PropertyDTO)
    ref_dir = str(Path(__file__).resolve().parent.parent / "pipeline" / "reference")
    try:
        mapper = AddressMapper(ref_dir)
    except Exception as e:
        log.warning(f"AddressMapper load failed: {e}, skipping address mapping")
        mapper = None

    results = process_batch(ads, "nhatot", mapper)

    # Double-check: ensure phone is in DTO (belt + suspenders)
    # Build lookup by both ad_id and list_id
    if phone_map:
        phone_lookup = {}
        for ad in ads:
            lid = ad.get("list_id")
            aid = ad.get("ad_id")
            if lid and lid in phone_map:
                phone = phone_map[lid]
                phone_lookup[str(lid)] = phone
                if aid:
                    phone_lookup[str(aid)] = phone

        for dto in results:
            if not dto.phone_full:
                dto.phone_full = phone_lookup.get(dto.source_id)

    # Save
    from config import raw_path
    output_file = raw_path("nhatot")

    phones_found = sum(1 for r in results if r.phone_full)

    output = {
        "source": "nhatot",
        "total": len(results),
        "full_phone": phones_found,
        "cycle_time_sec": int(time.time() - start),
        "processed_at": datetime.now().isoformat(),
        "listings": [asdict(r) for r in results],
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    elapsed = int(time.time() - start)
    log.info(f"\n{'='*60}")
    log.info(f"  CYCLE DONE in {elapsed}s")
    log.info(f"  Total: {len(results)} | Phones: {phones_found}")
    log.info(f"  Saved: {output_file}")
    log.info(f"{'='*60}")

    return output


async def loop_forever(interval_minutes: int = 60, **kwargs):
    """Chạy liên tục mỗi N phút."""
    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'#'*60}")
        log.info(f"  LOOP CYCLE #{cycle}")
        log.info(f"{'#'*60}")

        try:
            await run_cycle(**kwargs)
        except Exception as e:
            log.error(f"Cycle error: {e}")

        log.info(f"Next cycle in {interval_minutes} minutes...")
        await asyncio.sleep(interval_minutes * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="V-Nexus Nhatot Fast Scraper")
    parser.add_argument("--count", type=int, default=200, help="Max listings per cycle")
    parser.add_argument("--tabs", type=int, default=5, help="Parallel browser tabs")
    parser.add_argument("--batch-size", type=int, default=50, help="Phone reveal batch size before cooldown")
    parser.add_argument("--api-only", action="store_true", help="Only API, no phone reveal")
    parser.add_argument("--region", type=int, default=13000, help="13000=HCM, 12000=HN")
    parser.add_argument("--loop", action="store_true", help="Run continuously every hour")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in minutes")
    parser.add_argument("--offset-shift", type=int, default=0, help="Offset shift (advance past already-scraped ads)")
    parser.add_argument(
        "--transaction",
        choices=["ban", "cho-thue", "both"],
        default="both",
        help="Loại giao dịch crawl: ban | cho-thue | both (default: both — chia đều quota)",
    )

    args = parser.parse_args()

    if args.loop:
        asyncio.run(loop_forever(
            interval_minutes=args.interval,
            max_listings=args.count,
            num_tabs=args.tabs,
            batch_size=args.batch_size,
            region=args.region,
            transaction=args.transaction,
        ))
    else:
        asyncio.run(run_cycle(
            max_listings=args.count,
            num_tabs=args.tabs,
            batch_size=args.batch_size,
            api_only=args.api_only,
            region=args.region,
            offset_shift=args.offset_shift,
            transaction=args.transaction,
        ))
