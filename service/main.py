"""Clipper service entry point. Starts the Twitch monitor loop.
Future steps (capture, chat, clipping, upload) register as hooks on the monitor.

Run:
    python -m service.main                 # full pipeline (once implemented)
    python -m service.main --monitor-only  # step 1: just poll live status + log
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from . import config
from .twitch import TwitchClient
from .db import Supabase
from .monitor import Monitor


def setup_logging(cfg):
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(cfg.log_dir / "clipper.log"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )
    # Shush noisy libs
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


def ensure_data_dirs(cfg):
    for sub in ("buffers", "clips", "processed", "pending", "uploaded", "logs"):
        (cfg.data_dir / sub).mkdir(parents=True, exist_ok=True)


async def main_async(args):
    cfg = config.load()
    setup_logging(cfg)
    ensure_data_dirs(cfg)
    log = logging.getLogger("clipper")
    log.info("Starting clipper. monitor-only=%s streamers=%s", args.monitor_only, cfg.streamers)

    stop = asyncio.Event()

    def _handle_stop():
        log.info("Stop signal received, shutting down.")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except NotImplementedError:
            # Windows: signal handlers on asyncio are limited; ignore.
            pass

    async with TwitchClient(cfg.twitch_client_id, cfg.twitch_client_secret) as twitch, \
               Supabase(cfg.supabase_url, cfg.supabase_service_key) as db:
        monitor = Monitor(cfg, twitch, db)

        if not args.monitor_only:
            # TODO: wire up capture + chat + clip workers here as we add them.
            # monitor.on_live = capture_manager.start_for
            # monitor.on_offline = capture_manager.stop_for
            log.info("Full pipeline not yet wired - falling back to monitor only.")

        run_task = asyncio.create_task(monitor.run())
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            [run_task, stop_task], return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        for t in done:
            if t is run_task:
                try:
                    t.result()
                except Exception:
                    log.exception("monitor crashed")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--monitor-only", action="store_true",
                   help="Just poll Twitch live status + log to Supabase.")
    args = p.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
