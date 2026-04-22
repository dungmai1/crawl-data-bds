"""
One-off: upload tất cả ảnh trong 1 file merged JSON lên Cloudflare R2,
ghi kết quả ra file mới `{stem}_r2.json` (giữ nguyên bản gốc).

Usage:
    python scripts/migrate_sample_images.py
    python scripts/migrate_sample_images.py --input path/to/file.json
    python scripts/migrate_sample_images.py --input foo.json --output bar.json
    python scripts/migrate_sample_images.py --in-place   # ghi đè input
"""

import argparse
import asyncio
import json
import logging
import shutil
import sys
from pathlib import Path

_base = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_base))
sys.path.insert(0, str(_base / "pipeline"))

from image_uploader import upload_all_in_final_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("migrate")


DEFAULT_INPUT = _base / "data" / "sample" / "test_upload.json"


async def main():
    parser = argparse.ArgumentParser(description="Migrate sample images → Cloudflare R2")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input merged JSON")
    parser.add_argument("--output", default=None, help="Output path (default: <stem>_r2.json)")
    parser.add_argument("--in-place", action="store_true", help="Overwrite input file")
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        log.error(f"Input not found: {src}")
        sys.exit(1)

    if args.in_place:
        target = src
    else:
        if args.output:
            target = Path(args.output)
        else:
            target = src.with_name(f"{src.stem}_r2{src.suffix}")
        shutil.copy2(src, target)
        log.info(f"Copied {src.name} → {target.name}")

    log.info(f"Migrating images in: {target}")
    stats = await upload_all_in_final_file(str(target))

    log.info(
        f"\nResult:\n"
        f"  total_images = {stats['total_images']}\n"
        f"  uploaded     = {stats['uploaded']}\n"
        f"  skipped      = {stats['skipped']} (already on R2)\n"
        f"  failed       = {stats['failed']}\n"
        f"  output file  = {target}"
    )


if __name__ == "__main__":
    asyncio.run(main())
