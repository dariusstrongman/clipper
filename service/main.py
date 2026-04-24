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
from .capture import CaptureManager
from .chat import ChatManager
from .clipper import ClipExtractor
from .process import Processor
from .cleanup import Cleanup


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

    if not cfg.twitch_client_secret and not cfg.twitch_app_access_token:
        log.error("Set TWITCH_CLIENT_SECRET (preferred) or TWITCH_APP_ACCESS_TOKEN in .env")
        return
    async with TwitchClient(cfg.twitch_client_id, cfg.twitch_client_secret,
                            static_token=cfg.twitch_app_access_token or None) as twitch, \
               Supabase(cfg.supabase_url, cfg.supabase_service_key) as db:
        monitor = Monitor(cfg, twitch, db)
        capture_mgr: CaptureManager | None = None
        chat_mgr: ChatManager | None = None
        processor: Processor | None = None
        cleanup: Cleanup | None = None

        if not args.monitor_only:
            capture_mgr = CaptureManager(cfg)
            clip_extractor = ClipExtractor(cfg, db)
            chat_mgr = ChatManager(cfg, db, on_spike=clip_extractor.on_spike)
            processor = Processor(cfg, db)
            cleanup = Cleanup(cfg, db)
            await processor.start()
            await cleanup.start()

            async def _on_live(login, stream_id, stream_meta):
                await asyncio.gather(
                    capture_mgr.on_live(login, stream_id, stream_meta),
                    chat_mgr.on_live(login, stream_id, stream_meta),
                    return_exceptions=True,
                )

            async def _on_offline(login, stream_id):
                await asyncio.gather(
                    capture_mgr.on_offline(login, stream_id),
                    chat_mgr.on_offline(login, stream_id),
                    return_exceptions=True,
                )

            monitor.on_live = _on_live
            monitor.on_offline = _on_offline
            log.info("Capture + chat wired; will record streams and log chat spikes.")

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
        if capture_mgr:
            await capture_mgr.shutdown()
        if chat_mgr:
            await chat_mgr.shutdown()
        if processor:
            await processor.stop()
        if cleanup:
            await cleanup.stop()


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
