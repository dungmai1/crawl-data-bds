"""
V-Nexus: Cloudflare R2 image uploader

Download ảnh từ URL gốc của portal → upload lên R2 bucket →
thay URL gốc trong field `images` bằng URL public R2.

Key format: properties/{source}/{source_id}/{index}.{ext}
  - Deterministic → re-run cùng listing sẽ skip (head_object trả hit).

Env vars (xem .env.example):
  CF_R2_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY,
  CF_R2_BUCKET, CF_R2_PUBLIC_BASE
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aioboto3
import httpx
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("image_uploader")

_DOWNLOAD_SEM = asyncio.Semaphore(20)

_EXT_TO_CT = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
}


def _r2_config() -> dict:
    cfg = {
        "account_id": os.getenv("CF_R2_ACCOUNT_ID", "").strip(),
        "access_key": os.getenv("CF_R2_ACCESS_KEY_ID", "").strip(),
        "secret_key": os.getenv("CF_R2_SECRET_ACCESS_KEY", "").strip(),
        "bucket": os.getenv("CF_R2_BUCKET", "").strip(),
        "public_base": os.getenv("CF_R2_PUBLIC_BASE", "").strip().rstrip("/"),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing R2 env vars: {', '.join(missing)}. "
            f"See .env.example for required keys."
        )
    return cfg


def _extract_ext(url: str) -> str:
    path = urlparse(url).path.lower()
    m = re.search(r"\.(jpg|jpeg|png|webp|gif)(?:$|[?#])", path)
    return f".{m.group(1)}" if m else ".jpg"


def _build_key(source: str, source_id: str, index: int, src_url: str) -> str:
    ext = _extract_ext(src_url)
    return f"properties/{source}/{source_id}/{index}{ext}"


def _public_url(public_base: str, key: str) -> str:
    return f"{public_base}/{key}"


async def _download(client: httpx.AsyncClient, url: str, attempts: int = 3) -> Optional[bytes]:
    delay = 1.0
    for i in range(attempts):
        try:
            async with _DOWNLOAD_SEM:
                r = await client.get(url, follow_redirects=True)
            if r.status_code == 200:
                return r.content
            log.warning(f"download {url} → HTTP {r.status_code}")
        except Exception as e:
            log.warning(f"download {url} attempt {i+1} failed: {e}")
        if i < attempts - 1:
            await asyncio.sleep(delay)
            delay *= 2
    return None


async def _head_exists(s3, bucket: str, key: str) -> bool:
    try:
        await s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


async def _upload_one(
    http: httpx.AsyncClient,
    s3,
    cfg: dict,
    src_url: str,
    key: str,
    stats: dict,
) -> Optional[str]:
    """Upload 1 ảnh. Return public URL khi OK, None khi fail."""
    try:
        if await _head_exists(s3, cfg["bucket"], key):
            stats["skipped"] += 1
            return _public_url(cfg["public_base"], key)
    except Exception as e:
        log.warning(f"head_object {key} failed: {e}")

    data = await _download(http, src_url)
    if data is None:
        stats["failed"] += 1
        return None

    ct = _EXT_TO_CT.get(_extract_ext(src_url), "image/jpeg")
    try:
        await s3.put_object(
            Bucket=cfg["bucket"],
            Key=key,
            Body=data,
            ContentType=ct,
            CacheControl="public, max-age=31536000, immutable",
        )
        stats["uploaded"] += 1
        return _public_url(cfg["public_base"], key)
    except Exception as e:
        log.warning(f"put_object {key} failed: {e}")
        stats["failed"] += 1
        return None


async def _upload_listing(
    http: httpx.AsyncClient,
    s3,
    cfg: dict,
    listing: dict,
    stats: dict,
) -> None:
    source = listing.get("source")
    source_id = str(listing.get("source_id", "")).strip()
    images = listing.get("images") or []
    if not source or not source_id or not images:
        return

    async def process(i: int, url: str) -> tuple[int, str]:
        if not isinstance(url, str) or not url.startswith("http"):
            return i, url
        if cfg["public_base"] and url.startswith(cfg["public_base"]):
            return i, url  # already R2
        key = _build_key(source, source_id, i, url)
        new_url = await _upload_one(http, s3, cfg, url, key, stats)
        return i, new_url or url

    results = await asyncio.gather(*[process(i, u) for i, u in enumerate(images)])
    for i, new_url in results:
        listing["images"][i] = new_url


async def upload_all_in_final_file(
    final_path: str,
    max_concurrent_listings: int = 10,
    progress_every: int = 25,
) -> dict:
    """
    Load final merged JSON, upload toàn bộ ảnh lên R2, ghi đè file
    (thay URL gốc bằng URL R2). Return stats dict.

    Idempotent: re-run cùng file sẽ skip ảnh đã tồn tại trên R2.
    """
    cfg = _r2_config()
    endpoint = f"https://{cfg['account_id']}.r2.cloudflarestorage.com"

    with open(final_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    listings = data.get("listings", [])
    total_images = sum(len(l.get("images") or []) for l in listings)
    stats = {"total_images": total_images, "uploaded": 0, "skipped": 0, "failed": 0}

    if total_images == 0:
        log.info("No images to upload.")
        return stats

    log.info(f"Uploading {total_images} images across {len(listings)} listings → R2")

    session = aioboto3.Session()
    boto_cfg = BotoConfig(
        signature_version="s3v4",
        retries={"max_attempts": 3, "mode": "standard"},
    )
    sem = asyncio.Semaphore(max_concurrent_listings)

    async with httpx.AsyncClient(
        timeout=30.0,
        headers={"User-Agent": os.getenv("SCRAPE_USER_AGENT", "Mozilla/5.0")},
    ) as http, session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="auto",
        config=boto_cfg,
    ) as s3:

        done = 0

        async def run_one(listing):
            nonlocal done
            async with sem:
                await _upload_listing(http, s3, cfg, listing, stats)
                done += 1
                if done % progress_every == 0:
                    log.info(
                        f"  progress: {done}/{len(listings)} listings "
                        f"| uploaded={stats['uploaded']} "
                        f"skipped={stats['skipped']} "
                        f"failed={stats['failed']}"
                    )

        await asyncio.gather(*[run_one(l) for l in listings])

    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info(
        f"Done: uploaded={stats['uploaded']} "
        f"skipped={stats['skipped']} failed={stats['failed']} "
        f"/ total={stats['total_images']}"
    )
    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if len(sys.argv) < 2:
        print("Usage: python image_uploader.py <final_merged.json>")
        sys.exit(1)
    asyncio.run(upload_all_in_final_file(sys.argv[1]))
