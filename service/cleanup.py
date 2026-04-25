"""Periodic disk cleanup.

Runs every hour and prunes files we no longer need. Keeps the 100 GB EBS
volume from filling up if nobody reviews clips for a while.

Retention policy:
  - Rejected clips:   delete processed files 24h after rejection.
  - Uploaded clips:   delete processed files 30d after upload.
  - Failed clips:     delete processed files 48h after failure.
  - Orphan files:     files on disk with no matching DB row and older than
                      48h are removed (safety net for service crashes).
  - Ready + approved clips are NEVER auto-deleted.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config
from .db import Supabase

log = logging.getLogger(__name__)


# Extensions we manage inside /clips and /processed. Anything else is ignored.
_CLIP_EXTS = {".mp4", ".ts"}
_PROC_EXTS = {".mp4", ".srt", ".jpg", ".png", ".wav"}


def _age_hours(path: Path) -> float:
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return 0.0
    return (datetime.now().timestamp() - mtime) / 3600.0


def _unlink_quiet(p: Path) -> bool:
    try:
        p.unlink(missing_ok=True)
        return True
    except Exception:
        log.warning("cleanup: failed to delete %s", p, exc_info=True)
        return False


class Cleanup:
    """Background loop. Sleeps ~1 hour between sweeps."""

    def __init__(self, cfg: Config, db: Supabase, interval_seconds: int = 3600):
        self.cfg = cfg
        self.db = db
        self.interval = interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except Exception:
                self._task.cancel()

    async def _run(self) -> None:
        log.info("cleanup: started (interval=%ds)", self.interval)
        # Run once on startup so long-running services don't wait an hour for the first sweep.
        try:
            await self.sweep()
        except Exception:
            log.exception("cleanup: startup sweep failed")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self.sweep()
            except Exception:
                log.exception("cleanup: sweep failed")

    async def sweep(self) -> None:
        """One pass: prune DB-driven files + orphan files."""
        stats = {"rejected": 0, "uploaded": 0, "failed": 0, "orphans": 0, "bytes": 0}
        stats["bytes"] += await self._prune_rejected(hours=24)
        stats["rejected"] = stats["bytes"] and 1 or 0
        stats["bytes"] += await self._prune_uploaded(days=30)
        stats["bytes"] += await self._prune_failed(hours=48)
        stats["bytes"] += await self._prune_orphans(hours=48)
        log.info("cleanup: sweep done freed=%.1f MB", stats["bytes"] / 1e6)

    # ---------- DB-driven prunes ----------
    # Note: use 'Z' UTC suffix instead of '+00:00' because PostgREST query strings
    # interpret '+' as a space, which corrupts ISO-8601 timestamps with offsets.

    @staticmethod
    def _utc_iso(dt) -> str:
        """ISO-8601 UTC string with 'Z' suffix (URL-safe)."""
        return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    async def _prune_rejected(self, hours: int) -> int:
        cutoff = self._utc_iso(datetime.now(timezone.utc) - timedelta(hours=hours))
        rows = await self._select_old(
            f"status=eq.rejected&created_at=lt.{cutoff}&"
            "or=(processed_path.not.is.null,thumbnail_path.not.is.null,source_path.not.is.null)"
        )
        return await self._delete_clip_files(rows, "rejected")

    async def _prune_uploaded(self, days: int) -> int:
        cutoff = self._utc_iso(datetime.now(timezone.utc) - timedelta(days=days))
        # Use uploaded_at if set, fallback to created_at
        rows = await self._select_old(
            f"status=eq.uploaded&or=(uploaded_at.lt.{cutoff},and(uploaded_at.is.null,created_at.lt.{cutoff}))&"
            "or=(processed_path.not.is.null,thumbnail_path.not.is.null,source_path.not.is.null)"
        )
        return await self._delete_clip_files(rows, "uploaded")

    async def _prune_failed(self, hours: int) -> int:
        cutoff = self._utc_iso(datetime.now(timezone.utc) - timedelta(hours=hours))
        rows = await self._select_old(
            f"status=eq.failed&created_at=lt.{cutoff}&"
            "or=(processed_path.not.is.null,thumbnail_path.not.is.null,source_path.not.is.null)"
        )
        return await self._delete_clip_files(rows, "failed")

    async def _select_old(self, filter_str: str) -> list[dict]:
        try:
            return await self.db.select(
                "clipper_clips",
                filter_str + "&select=id,source_path,processed_path,thumbnail_path&limit=500",
            )
        except Exception:
            log.exception("cleanup: select failed %s", filter_str)
            return []

    async def _delete_clip_files(self, rows: list[dict], tag: str) -> int:
        """Delete files referenced by these rows + null the path columns so they
        aren't revisited next sweep."""
        freed = 0
        for r in rows:
            paths = []
            for col in ("source_path", "processed_path", "thumbnail_path"):
                p = r.get(col)
                if not p:
                    continue
                fp = Path(p)
                # Also try to find the sidecar .srt next to processed_path
                sidecars = []
                if col == "processed_path":
                    sidecars = [fp.with_suffix(".srt"), fp.with_suffix(".vertical.mp4"), fp.with_suffix(".wav")]
                paths.append(fp)
                paths.extend(sidecars)

            for fp in paths:
                if not fp.exists():
                    continue
                sz = 0
                try: sz = fp.stat().st_size
                except Exception: pass
                if _unlink_quiet(fp):
                    freed += sz

            # Null the path columns so we don't revisit
            try:
                await self.db.update(
                    "clipper_clips", f"id=eq.{r['id']}",
                    {"source_path": None, "processed_path": None, "thumbnail_path": None},
                )
            except Exception:
                log.exception("cleanup: failed to null paths for %s", r.get("id"))

            log.info("cleanup[%s]: purged files for clip %s", tag, r.get("id"))
        return freed

    # ---------- Orphan file prune ----------

    async def _prune_orphans(self, hours: int) -> int:
        """Files in /clips and /processed older than `hours` with no DB row.
        Safety net for crashes where Python died before inserting a row."""
        freed = 0
        # Collect referenced paths from DB so we don't delete active files
        referenced: set[str] = set()
        try:
            rows = await self.db.select(
                "clipper_clips",
                "select=source_path,processed_path,thumbnail_path&limit=5000",
            )
            for r in rows:
                for col in ("source_path", "processed_path", "thumbnail_path"):
                    v = r.get(col)
                    if v:
                        referenced.add(v)
        except Exception:
            log.exception("cleanup: could not load referenced paths, skipping orphan pass")
            return 0

        roots = [
            (self.cfg.data_dir / "clips", _CLIP_EXTS),
            (self.cfg.data_dir / "processed", _PROC_EXTS),
        ]
        for root, allowed_exts in roots:
            if not root.exists():
                continue
            for f in root.rglob("*"):
                if not f.is_file():
                    continue
                if f.suffix.lower() not in allowed_exts:
                    continue
                if str(f) in referenced:
                    continue
                if _age_hours(f) < hours:
                    continue
                try: sz = f.stat().st_size
                except Exception: sz = 0
                if _unlink_quiet(f):
                    freed += sz
                    log.info("cleanup: orphan removed %s (%.1f MB, %.0fh old)",
                             f, sz / 1e6, _age_hours(f))
        return freed
