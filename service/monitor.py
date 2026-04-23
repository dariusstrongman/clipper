"""Twitch live-status monitor. Polls Twitch for the configured streamers every
POLL_INTERVAL_SECONDS. Opens a clipper_streams row when a streamer goes live;
closes it when they go offline."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict

from .config import Config
from .twitch import TwitchClient
from .db import Supabase

log = logging.getLogger(__name__)


class Monitor:
    """Owns the live-status loop. Hands off to capture/chat/clip tasks (next step)
    via on_live / on_offline callbacks."""

    def __init__(self, cfg: Config, twitch: TwitchClient, db: Supabase):
        self.cfg = cfg
        self.twitch = twitch
        self.db = db
        # login (lowercase) -> {"user_id": str, "display_name": str}
        self._users: Dict[str, dict] = {}
        # login -> active stream_id (None if offline)
        self._active_stream_ids: Dict[str, str | None] = {}
        # Hooks the next steps will register on
        self.on_live = None     # async def (login, stream_id, stream_meta) -> None
        self.on_offline = None  # async def (login, stream_id) -> None

    async def _resolve_users(self) -> None:
        """Look up numeric user_ids once at startup. Always start with empty
        _active_stream_ids so the first tick fires on_live for any live
        streamer (capture + chat listener must always start, even on restart).
        Duplicate stream rows are prevented in _tick by checking for an
        existing open row before inserting."""
        users = await self.twitch.get_users(self.cfg.streamers)
        by_login = {u["login"].lower(): u for u in users}
        missing = [s for s in self.cfg.streamers if s not in by_login]
        if missing:
            log.warning("Unknown Twitch logins: %s", missing)
        self._users = by_login
        self._active_stream_ids = {login: None for login in by_login}
        log.info("Monitoring %s", ", ".join(f"{u['login']}({u['id']})" for u in by_login.values()))

    async def _tick(self) -> None:
        """One poll cycle: check who's live, open/close stream rows, fire hooks."""
        user_ids = [u["id"] for u in self._users.values()]
        live = await self.twitch.get_streams(user_ids)
        live_by_id = {s["user_id"]: s for s in live}
        now = datetime.now(timezone.utc).isoformat()

        for login, user in self._users.items():
            uid = user["id"]
            was_live_id = self._active_stream_ids.get(login)
            is_live = uid in live_by_id

            if is_live and not was_live_id:
                # Went online since last tick (or service just started while this
                # stream was already live). Reuse an existing open row for this
                # streamer if one exists so restarts don't create duplicates.
                s = live_by_id[uid]
                stream_id: str | None = None
                try:
                    open_rows = await self.db.select(
                        "clipper_streams",
                        f"streamer=eq.{login}&ended_at=is.null&order=started_at.desc&limit=1&select=id",
                    )
                    if open_rows:
                        stream_id = open_rows[0]["id"]
                        log.info("monitor: reusing existing open stream for %s -> %s",
                                 login, stream_id)
                except Exception:
                    log.exception("monitor: open-stream lookup failed for %s", login)

                if not stream_id:
                    row = await self.db.insert("clipper_streams", {
                        "streamer": login,
                        "twitch_user_id": uid,
                        "started_at": s.get("started_at") or now,
                        "title": s.get("title"),
                        "game": s.get("game_name"),
                        "peak_viewers": s.get("viewer_count"),
                    })
                    stream_id = row.get("id")

                self._active_stream_ids[login] = stream_id
                log.info("LIVE %s | %s | %s viewers | id=%s",
                         login, (s.get("title") or "")[:60], s.get("viewer_count"), stream_id)
                if self.on_live:
                    try:
                        await self.on_live(login, stream_id, s)
                    except Exception:
                        log.exception("on_live hook failed for %s", login)

            elif is_live and was_live_id:
                # Still live - update peak viewer count if higher
                s = live_by_id[uid]
                try:
                    await self.db.update(
                        "clipper_streams",
                        f"id=eq.{was_live_id}",
                        {"peak_viewers": s.get("viewer_count")},
                    )
                except Exception:
                    log.exception("peak_viewers update failed for %s", login)

            elif not is_live and was_live_id:
                # Went offline
                try:
                    await self.db.update(
                        "clipper_streams",
                        f"id=eq.{was_live_id}",
                        {"ended_at": now},
                    )
                except Exception:
                    log.exception("ended_at update failed for %s", login)
                log.info("OFFLINE %s (stream ended id=%s)", login, was_live_id)
                if self.on_offline:
                    try:
                        await self.on_offline(login, was_live_id)
                    except Exception:
                        log.exception("on_offline hook failed for %s", login)
                self._active_stream_ids[login] = None

            else:
                # Still offline, nothing to do
                pass

    async def run(self) -> None:
        """Main poll loop; never returns."""
        await self._resolve_users()
        while True:
            try:
                await self._tick()
            except Exception:
                log.exception("monitor tick failed")
            await asyncio.sleep(self.cfg.poll_interval_seconds)
