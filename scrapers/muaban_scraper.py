"""
V-Nexus: muaban.net Full Scraper (API + Detail Enrichment)
Cào toàn bộ BĐS listings + FULL phone number + FULL Images/Details

Strategy (proven):
  1. Playwright mở homepage → lấy Cloudflare cookie
  2. Click vào /bat-dong-san → browser có session hợp lệ
  3. Browser gọi /listing/v1/classifieds/latest... → API trả items/page + FULL PHONE
  4. Pagination: offset 0 → max items
  5. Detail Enrichment: Truy cập source_url từng item, bóc __NEXT_DATA__ JSON
     để lấy mảng ảnh gốc, tên đường, pháp lý, số tầng, TÊN NGƯỜI ĐĂNG...
     (muaban API/__NEXT_DATA__ không trả lat/lng nên không cào tọa độ)

Usage:
    python muaban_scraper.py                     # Default: HCM, 500 items
    python muaban_scraper.py --per-city 1000     # 1000 listings per city
    python muaban_scraper.py --loop --interval 60  # Run hourly
"""

import asyncio
import json
import os
import sys
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Setup Config & Logging
_cfg_dir = str(Path(__file__).resolve().parent.parent)
if _cfg_dir not in sys.path: sys.path.insert(0, _cfg_dir)
from config import log_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_path("muaban"), encoding="utf-8"),
    ],
)
log = logging.getLogger("muaban")

CITIES_ALL = [
    (30, "HCM"), (24, "HN"), (15, "Da Nang"), (28, "Binh Duong"),
    (42, "Dong Nai"), (37, "Can Tho"), (39, "Khanh Hoa"), (29, "Ba Ria Vung Tau"),
]

CITIES_DEFAULT = [
    (30, "HCM"),
]

CATEGORY_BDS = 33  # All BDS (fallback)

CATEGORY_SLUGS = {
    "all": None,  # no subcategory → all BDS
    "dat-tho-cu": "ban-dat-tho-cu",
}


async def init_browser_session(p, subcategory_slug: str | None = None):
    """Open browser, load homepage for CF cookies, navigate to BDS to get category_id."""
    browser = await p.chromium.launch(
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
        locale="vi-VN",
    )
    await context.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    page = await context.new_page()

    captured = {"category_id": None}

    def on_request(request):
        if captured["category_id"] is not None: return
        url = request.url
        if "/listing/v1/classifieds/" not in url: return
        qs = parse_qs(urlparse(url).query)
        cid = qs.get("category_id", [None])[0]
        if cid and cid.isdigit():
            captured["category_id"] = int(cid)
            log.info(f"Discovered category_id={cid} from {urlparse(url).path}")

    page.on("request", on_request)

    log.info("Initializing browser session...")
    await page.goto("https://muaban.net/", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(4000)

    target_path = f"/bat-dong-san/{subcategory_slug}" if subcategory_slug else "/bat-dong-san"
    target_url = f"https://muaban.net{target_path}"
    try:
        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        log.info(f"Browser session ready at {target_path}")
    except Exception as e:
        log.warning(f"Navigation to {target_path} failed: {e}")

    page.remove_listener("request", on_request)

    category_id = captured["category_id"] or CATEGORY_BDS
    if subcategory_slug and captured["category_id"] is None:
        log.warning(f"Could not discover category_id for {subcategory_slug}, falling back to {CATEGORY_BDS}")

    return browser, page, category_id


async def fetch_listings(page, category_id: int, city_id: int, city_name: str, max_items: int = 10) -> list[dict]:
    """Fetch listings with full phone via /latest API pagination."""
    items = []
    seen_ids = set()

    for offset in range(0, max_items, 20):
        try:
            result = await page.evaluate(f"""
                async () => {{
                    try {{
                        const r = await fetch('/listing/v1/classifieds/latest?category_id={category_id}&city_id={city_id}&offset={offset}&limit=20');
                        if (!r.ok) return {{ error: r.status }};
                        return await r.json();
                    }} catch(e) {{ return {{ error: e.message }}; }}
                }}
            """)

            if isinstance(result, dict) and "error" in result:
                log.warning(f"  [{city_name}] offset={offset}: API error {result['error']}")
                break

            batch = result.get("items", [])
            if not batch: break

            new_count = 0
            for item in batch:
                iid = item.get("id")
                if iid and iid not in seen_ids:
                    seen_ids.add(iid)
                    items.append(item)
                    new_count += 1

            if offset % 200 == 0:
                total_avail = result.get("total", 0)
                log.info(f"  [{city_name}] API offset={offset}: +{new_count} (collected: {len(items)}, available: {total_avail})")

            if len(batch) < 20: break
            await asyncio.sleep(0.3)

        except Exception as e:
            log.error(f"  [{city_name}] offset={offset}: {str(e)[:60]}")
            break

    return items

async def enrich_detail_data(page, source_url: str) -> dict:
    """
    Truy cập URL trang chi tiết và trích xuất thông tin chuyên sâu trực tiếp từ HTML DOM 
    và __NEXT_DATA__ JSON để lấy FULL thông tin.
    """
    enriched_data = {
        "images": [],
        "street": None,
        "direction": None,
        "floors": None,
        "legal_document": None,
        "contact_name": None,
        "phone_full": None,
        "property_type": None,
    }

    try:
        await page.goto(source_url, wait_until="domcontentloaded", timeout=15000)
        html_content = await page.content()
        soup = BeautifulSoup(html_content, "html.parser")

        # ---------------------------------------------------------
        # 1. Bóc tách Toàn bộ Hình Ảnh (Image Gallery)
        # ---------------------------------------------------------
        img_tags = soup.find_all("img")
        seen_imgs = set()
        
        for img in img_tags:
            src = img.get("src", "")
            if "cloud.muaban.net" in src and "/images/" in src:
                if "/cdn-cgi/image/" in src:
                    src = "https://cloud.muaban.net/images/" + src.split("/images/")[1]
                hi_res_url = src.replace("/thumb-detail/", "/orig/").replace("/thumb-md/", "/orig/").replace("/crop/", "/orig/")
                
                if hi_res_url not in seen_imgs:
                    seen_imgs.add(hi_res_url)
                    enriched_data["images"].append(hi_res_url)

        # ---------------------------------------------------------
        # 2. Bóc tách Thông số kỹ thuật (Pháp lý, Hướng, Tầng...)
        # ---------------------------------------------------------
        for label_tag in soup.find_all("span", class_="label"):
            label_text = label_tag.get_text(strip=True).lower()
            parent = label_tag.parent
            
            if parent:
                value_span = parent.find("span", class_="value")
                if value_span:
                    val = value_span.get_text(strip=True)
                else:
                    val = parent.get_text(strip=True).replace(label_tag.get_text(strip=True), "").strip()
                
                if "tầng" in label_text: enriched_data["floors"] = val
                elif "pháp lý" in label_text or "giấy tờ" in label_text: enriched_data["legal_document"] = val
                elif "hướng" in label_text: enriched_data["direction"] = val
                elif "loại hình" in label_text: enriched_data["property_type"] = val

        # ---------------------------------------------------------
        # 3. Số điện thoại bổ sung
        # ---------------------------------------------------------
        phone_tag = soup.find(attrs={"data-phone": True})
        if phone_tag:
            enriched_data["phone_full"] = phone_tag["data-phone"]

        # ---------------------------------------------------------
        # 4. Tên người liên hệ (DOM Fallback)
        # ---------------------------------------------------------
        # Quét các class phổ biến thường chứa tên thay vì chỉ `span.title`
        for name_selector in [".user-info__name", ".seller-name", ".agent-name", ".name"]:
            name_tag = soup.select_one(name_selector)
            if name_tag and name_tag.get_text(strip=True):
                enriched_data["contact_name"] = name_tag.get_text(strip=True)
                break

        # ---------------------------------------------------------
        # 5. Khai thác dữ liệu vàng từ __NEXT_DATA__ JSON
        # (Chính xác nhất cho contact_name, street)
        # ---------------------------------------------------------
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if script_tag:
            try:
                data = json.loads(script_tag.string)
                classified = data.get("props", {}).get("pageProps", {}).get("classified", {}) or {}

                if classified:
                    # Tên đường
                    address = classified.get("address", "")
                    if address:
                        enriched_data["street"] = address.split(",")[0].strip()

                    # --> BÓC TÊN NGƯỜI ĐĂNG TỪ API NỘI BỘ <--
                    # Mua Bán thường lưu tên ở contact_name hoặc trong object user
                    api_contact = classified.get("contact_name")
                    if not api_contact and classified.get("user"):
                        api_contact = classified["user"].get("fullname") or classified["user"].get("username")

                    if api_contact:
                        enriched_data["contact_name"] = api_contact.strip()

            except Exception as e:
                log.warning(f"  [__NEXT_DATA__ parse failed] at {source_url}: {e}")

    except Exception as e:
        log.warning(f"  [Detail Enrichment Error] at {source_url}: {e}")

    return enriched_data



async def run_cycle(per_city: int = 10, cities: list = None, category: str = "all"):
    """Run one scrape cycle across all cities."""
    os.makedirs("logs", exist_ok=True)

    if cities is None: cities = CITIES_DEFAULT
    if category not in CATEGORY_SLUGS:
        raise ValueError(f"Unknown category '{category}'. Available: {list(CATEGORY_SLUGS)}")
    
    subcategory_slug = CATEGORY_SLUGS[category]
    start = time.time()
    
    log.info(f"{'=' * 60}")
    log.info(f"  CYCLE START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Cities: {len(cities)} | Per city: {per_city} | Category: {category}")
    log.info(f"{'=' * 60}")

    async with async_playwright() as p:
        browser, page, category_id = await init_browser_session(p, subcategory_slug)
        log.info(f"  Using category_id={category_id}")

        all_items = []
        city_stats = {}

        for city_id, city_name in cities:
            log.info(f"\n  [{city_name}] Phasing 1: Scraping API for basic list...")
            items = await fetch_listings(page, category_id, city_id, city_name, per_city)
            
            log.info(f"  [{city_name}] Phasing 2: Enriching {len(items)} items from Detail pages...")
            for idx, item in enumerate(items):
                
                raw_url = item.get("url") or item.get("source_url")
                if raw_url:
                    full_url = f"https://muaban.net{raw_url}" if raw_url.startswith("/") else raw_url
                    
                    log.info(f"    -> [Detail {idx + 1}/{len(items)}] Đang cào: {full_url}")

                    # Enrich data
                    detail_data = await enrich_detail_data(page, full_url)
                    
                    # Cập nhật data chi tiết vào item (bao gồm contact_name)
                    for key, value in detail_data.items():
                        if value is not None and (not isinstance(value, list) or len(value) > 0):
                            item[key] = value
                    
                    # Update image count
                    if item.get("images"):
                        item["image_count"] = len(item["images"])
                    
                    # Ghi log tên người liên hệ lấy được để bạn dễ kiểm tra
                    log.info(f"       + Liên hệ: {item.get('contact_name', 'Không tìm thấy')}")
                    
                    await asyncio.sleep(0.8)

            all_items.extend(items)
            phones = sum(1 for i in items if i.get("phone"))
            city_stats[city_name] = {"total": len(items), "phones": phones}

        await browser.close()

    # Deduplicate
    seen = set()
    unique = []
    for item in all_items:
        iid = item.get("id") or item.get("source_id")
        if iid not in seen:
            seen.add(iid)
            unique.append(item)

    total = len(unique)
    has_phone = sum(1 for i in unique if i.get("phone") or i.get("phone_full"))
    elapsed = int(time.time() - start)

    # Save
    import sys as _sys
    _cfg = str(Path(__file__).resolve().parent.parent)
    if _cfg not in _sys.path: _sys.path.insert(0, _cfg)
    from config import raw_path
    
    out = raw_path("muaban")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {
                "total": total,
                "with_phone": has_phone,
                "elapsed_seconds": elapsed,
                "scraped_at": datetime.now().isoformat(),
                "category": category,
                "category_id": category_id,
                "city_stats": city_stats,
                "items": unique,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    log.info(f"\n{'=' * 60}")
    log.info(f"  CYCLE DONE in {elapsed}s")
    log.info(f"  Total: {total} unique listings")
    log.info(f"  Full phone: {has_phone} ({has_phone / max(total, 1) * 100:.0f}%)")
    log.info(f"  Speed: {total / max(elapsed, 1) * 60:.0f} listings/min")

    for city, stats in city_stats.items():
        log.info(f"    {city}: {stats['total']} listings, {stats['phones']} phones")

    log.info(f"  Saved: {out}")
    log.info(f"{'=' * 60}")

    return {"total": total, "phones": has_phone, "file": out, "elapsed": elapsed}


async def loop(interval_min: int = 60, **kwargs):
    """Run continuously."""
    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'#' * 60}")
        log.info(f"  LOOP CYCLE #{cycle}")
        log.info(f"{'#' * 60}")
        try:
            await run_cycle(**kwargs)
        except Exception as e:
            log.error(f"Cycle failed: {e}")
        log.info(f"Next cycle in {interval_min} min...")
        await asyncio.sleep(interval_min * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V-Nexus muaban.net Scraper")
    parser.add_argument("--per-city", type=int, default=10, help="Max listings per city")
    parser.add_argument("--all-cities", action="store_true", help="Scrape all cities (default: HCM only)")
    parser.add_argument(
        "--category",
        default="all",
        choices=list(CATEGORY_SLUGS),
        help="Subcategory filter (default: all BDS)",
    )
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval minutes")
    args = parser.parse_args()

    cities = CITIES_ALL if args.all_cities else CITIES_DEFAULT

    if args.loop:
        asyncio.run(loop(interval_min=args.interval, per_city=args.per_city, cities=cities, category=args.category))
    else:
        asyncio.run(run_cycle(per_city=args.per_city, cities=cities, category=args.category))