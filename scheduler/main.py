"""
Scheduler entrypoint — APScheduler BlockingScheduler.

Chạy `run_all_sources_once` mỗi N phút (default 5). Job phụ thuộc nhau (cùng
DB) nên dùng `max_instances=1` + `coalesce=True` để tránh overlap khi cycle
trước chưa xong.

Usage:
    python -m scheduler.main                 # mặc định 5 phút/cycle
    python -m scheduler.main --interval 1    # 1 phút/cycle
    python -m scheduler.main --once          # chạy 1 lần rồi exit
    python -m scheduler.main --nhatot-only
    python -m scheduler.main --muaban-only

Recommend production: chạy như systemd/supervisor service.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

_base = Path(__file__).resolve().parent.parent
if str(_base) not in sys.path:
    sys.path.insert(0, str(_base))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from scheduler.jobs import (
    run_all_sources_once,
    run_muaban_job,
    run_nhatot_job,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("scheduler.main")


def _sync_runner(coro_factory):
    """Wrap async coroutine factory → sync function cho APScheduler.

    Mỗi tick tạo event loop riêng (đảm bảo isolation, đặc biệt với Playwright
    có thể giữ resource giữa các run).
    """
    def runner():
        log.info("=" * 60)
        log.info("CYCLE START %s", datetime.now().isoformat(timespec="seconds"))
        log.info("=" * 60)
        try:
            asyncio.run(coro_factory())
        except Exception:
            log.exception("Cycle crashed")
        log.info("CYCLE END %s", datetime.now().isoformat(timespec="seconds"))
    return runner


def main():
    parser = argparse.ArgumentParser(description="V-Nexus scheduled scraper")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("SCRAPE_INTERVAL_MIN", "5")),
        help="Interval in minutes (1-5 recommended)",
    )
    parser.add_argument("--once", action="store_true", help="Run 1 cycle and exit")
    parser.add_argument("--nhatot-only", action="store_true")
    parser.add_argument("--muaban-only", action="store_true")
    args = parser.parse_args()

    if args.nhatot_only:
        coro_factory = run_nhatot_job
    elif args.muaban_only:
        coro_factory = run_muaban_job
    else:
        coro_factory = run_all_sources_once

    if args.once:
        log.info("Running ONCE then exit")
        asyncio.run(coro_factory())
        return

    if not (1 <= args.interval <= 60):
        log.warning("Interval %d not in 1-60, forcing 5", args.interval)
        args.interval = 5

    scheduler = BlockingScheduler(timezone="Asia/Ho_Chi_Minh")
    job = _sync_runner(coro_factory)
    scheduler.add_job(
        job,
        trigger=IntervalTrigger(minutes=args.interval),
        id="scrape_cycle",
        name="V-Nexus scrape cycle",
        coalesce=True,        # gộp các trigger trễ thành 1
        max_instances=1,      # không cho 2 cycle chạy song song
        misfire_grace_time=60,
        next_run_time=datetime.now(),  # chạy ngay lúc start, không chờ interval đầu
    )

    def _shutdown(signum, frame):
        log.info("Signal %s received — stopping scheduler", signum)
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    log.info("Scheduler starting — every %d min", args.interval)
    scheduler.start()


if __name__ == "__main__":
    main()
