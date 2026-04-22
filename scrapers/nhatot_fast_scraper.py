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
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path
import sys

_pipeline_dir = str(Path(__file__).resolve().parent.parent / "pipeline")
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)
from unified_pipeline import process_batch, AddressMapper, calc_quality_score

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

# ===========================================================
# TẦNG 1: API SCRAPER (NHANH — ~3000 listings/phút)
# ===========================================================

async def scrape_api_batch(
    region: int = 13000,
    max_listings: int = 500,
    categories: list = None,
    offset_shift: int = 0,
) -> list[dict]:
    """Cào listings từ gateway API. Async, nhanh, không cần browser.

    offset_shift: cộng thêm vào offset để lấy page ads khác (cho accumulator mode).
    """

    if categories is None:
        categories = [1020, 1010, 1040, 1030]  # Nhà ở, Chung cư, Đất, Văn phòng/Mặt bằng

    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        tasks = []
        for cg in categories:
            # API trả 50/call — đảm bảo ít nhất 1 request mỗi category
            per_cat = max(max_listings // len(categories), 50)
            for offset in range(0, per_cat, 50):
                real_offset = offset + offset_shift
                url = (
                    f"https://gateway.chotot.com/v1/public/ad-listing"
                    f"?cg={cg}&limit=50&o={real_offset}&st=s&region_v2={region}"
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

    log.info(f"API: {len(all_ads)} unique listings — per-cat: {dist}")
    return all_ads


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
) -> dict[int, str]:
    """Reveal SĐT cho nhiều listings cùng lúc bằng nhiều tabs."""

    from playwright.async_api import async_playwright

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

    # Build URL map from search pages first
    url_map = await _build_url_map(to_reveal)

    phone_results = {}  # list_id -> phone
    queue = asyncio.Queue()
    for i, ad in enumerate(to_reveal):
        queue.put_nowait((i + 1, ad))

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
                total = len(to_reveal)

                # Get URL
                detail_url = url_map.get(list_id)
                if not detail_url:
                    district = ad.get("area_name", "").lower().replace(" ", "-")
                    detail_url = f"https://www.nhatot.com/mua-ban-{district}-tp-ho-chi-minh/{list_id}.htm"

                phone_found = None

                # Intercept
                async def on_resp(response):
                    nonlocal phone_found
                    try:
                        if (
                            "gateway.chotot.com" in response.url
                            and "/phone" in response.url
                            and response.status == 200
                        ):
                            body = await response.json()
                            bs = json.dumps(body)
                            full = re.findall(r'"phone"\s*:\s*"(0\d{8,9})"', bs)
                            if full:
                                phone_found = full[0]
                    except:
                        pass

                page.on("response", on_resp)

                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(2500)

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
                        log.info(f"[T{tab_id}] ({idx}/{total}) NO_PHONE | {title}")

                    processed += 1

                except Exception as e:
                    log.warning(f"[T{tab_id}] ({idx}/{total}) ERROR: {str(e)[:60]}")

                finally:
                    page.remove_listener("response", on_resp)

                # Rate limit per tab
                await asyncio.sleep(1.5)

            await page.close()
            log.info(f"[T{tab_id}] Done — processed {processed}")

        # Launch all tabs
        await asyncio.gather(*[tab_worker(i + 1) for i in range(num_tabs)])
        await browser.close()

    log.info(f"Phone: revealed {len(phone_results)}/{len(to_reveal)} phones")
    return phone_results


async def _build_url_map(ads: list[dict]) -> dict[int, str]:
    """Lấy correct listing URLs từ search page."""
    from playwright.async_api import async_playwright

    url_map = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 800},
            locale="vi-VN",
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()

        pages_needed = max(1, len(ads) // 20)
        for pg in range(1, min(pages_needed + 1, 10)):
            search_url = "https://www.nhatot.com/mua-ban-nha-dat-tp-ho-chi-minh"
            if pg > 1:
                search_url += f"?page={pg}"

            try:
                await page.goto(search_url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)

                links = await page.evaluate("""
                    () => [...document.querySelectorAll('a[href]')]
                        .filter(a => /\\/\\d+\\.htm/.test(a.href) && a.href.includes('nhatot.com'))
                        .map(a => a.href)
                """)

                for link in links:
                    m = re.search(r"/(\d+)\.htm", link)
                    if m:
                        url_map[int(m.group(1))] = link.split("#")[0]

            except Exception as e:
                log.warning(f"Search page {pg} error: {e}")

        await browser.close()

    log.info(f"URL map: {len(url_map)} URLs from search pages")
    return url_map


# ===========================================================
# MAIN: KẾT HỢP CẢ 2 TẦNG
# ===========================================================

async def run_cycle(
    max_listings: int = 500,
    num_tabs: int = 5,
    api_only: bool = False,
    region: int = 13000,
    offset_shift: int = 0,
):
    """Chạy 1 chu kỳ cào: API + Phone reveal."""

    start = time.time()
    log.info(f"\n{'='*60}")
    log.info(f"  CYCLE START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Listings: {max_listings} | Tabs: {num_tabs} | API-only: {api_only} | Offset shift: {offset_shift}")
    log.info(f"{'='*60}")

    # Load persistent phone cache — skip các list_id đã thử (cả success lẫn fail)
    phone_cache = load_phone_cache()
    attempted_ids = {int(k) for k in phone_cache.keys() if str(k).isdigit()}
    phone_known = {int(k): v for k, v in phone_cache.items() if str(k).isdigit() and v}
    log.info(f"Phone cache: {len(phone_known)} có phone, {len(attempted_ids) - len(phone_known)} đã thử (fail)")

    # Tầng 1: API
    ads = await scrape_api_batch(region=region, max_listings=max_listings, offset_shift=offset_shift)
    ads = await enrich_with_detail(ads, max_concurrent=10)

    # Filter new (chưa attempted)
    new_ads = [a for a in ads if a.get("list_id") not in attempted_ids]
    log.info(f"New listings (cần reveal): {len(new_ads)}/{len(ads)}")

    # Tầng 2: Phone (nếu không phải api-only)
    phone_map = dict(phone_known)  # Kế thừa phones đã biết
    if not api_only and new_ads:
        new_phone_map = await reveal_phones_batch(new_ads, num_tabs, attempted_ids)
        phone_map.update(new_phone_map)

        # Cập nhật cache: mọi new_ad đã thử → ghi vào cache (success = phone, fail = null)
        for ad in new_ads:
            lid = ad.get("list_id")
            if lid:
                phone_cache[str(lid)] = new_phone_map.get(lid)
        save_phone_cache(phone_cache)
        log.info(f"Phone cache updated: {len(phone_cache)} total entries")

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
                if dto.phone_full:
                    dto.quality_score = calc_quality_score(dto)

    # Save
    from config import raw_path
    output_file = raw_path("nhatot")

    phones_found = sum(1 for r in results if r.phone_full)
    avg_quality = sum(r.quality_score for r in results) / max(len(results), 1)

    output = {
        "source": "nhatot",
        "total": len(results),
        "new": len(new_ads),
        "full_phone": phones_found,
        "avg_quality": round(avg_quality, 1),
        "cycle_time_sec": int(time.time() - start),
        "processed_at": datetime.now().isoformat(),
        "listings": [asdict(r) for r in results],
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    elapsed = int(time.time() - start)
    log.info(f"\n{'='*60}")
    log.info(f"  CYCLE DONE in {elapsed}s")
    log.info(f"  Total: {len(results)} | New: {len(new_ads)} | Phones: {phones_found}")
    log.info(f"  Avg quality: {avg_quality:.0f}/100")
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
    parser.add_argument("--count", type=int, default=500, help="Max listings per cycle")
    parser.add_argument("--tabs", type=int, default=5, help="Parallel browser tabs")
    parser.add_argument("--api-only", action="store_true", help="Only API, no phone reveal")
    parser.add_argument("--region", type=int, default=13000, help="13000=HCM, 12000=HN")
    parser.add_argument("--loop", action="store_true", help="Run continuously every hour")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in minutes")
    parser.add_argument("--offset-shift", type=int, default=0, help="Offset shift (advance past already-scraped ads)")

    args = parser.parse_args()

    if args.loop:
        asyncio.run(loop_forever(
            interval_minutes=args.interval,
            max_listings=args.count,
            num_tabs=args.tabs,
            region=args.region,
        ))
    else:
        asyncio.run(run_cycle(
            max_listings=args.count,
            num_tabs=args.tabs,
            api_only=args.api_only,
            region=args.region,
            offset_shift=args.offset_shift,
        ))
