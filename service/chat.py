"""Twitch IRC chat listener + message-velocity spike detector.

Anonymous IRC connection per live streamer. Tracks a rolling window of
message timestamps; when the window exceeds the threshold, fires a spike
event. Spikes are logged to Supabase and handed off to an on_spike callback
(which step 4 will use to trigger clip extraction).

Usage: register ChatManager.on_live / on_offline as Monitor hooks.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import deque
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List

import websockets

from .config import Config
from .db import Supabase

log = logging.getLogger(__name__)

IRC_WS_URL = "wss://irc-ws.chat.twitch.tv:443"

# Callback signature: (streamer, stream_id, spike_row) -> None
OnSpike = Callable[[str, str, dict], Awaitable[None]]


class ChatListener:
    """One WS connection per streamer. Reconnects automatically on drop."""

    def __init__(self, streamer: str, stream_id: str, cfg: Config,
                 report_spike: Callable[[str, str, float, int, int, List[str]], Awaitable[None]]):
        self.streamer = streamer
        self.stream_id = stream_id
        self.cfg = cfg
        self._report_spike = report_spike
        self._msg_times: deque[float] = deque()
        self._recent_msgs: deque[str] = deque(maxlen=15)
        self._last_spike_at: float = 0.0
        self._task: asyncio.Task | None = None
        self._running = False
        self._ws = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
            except Exception:
                pass

    async def _run(self) -> None:
        while self._running:
            try:
                nick = f"justinfan{random.randint(10000, 99999)}"
                async with websockets.connect(IRC_WS_URL, ping_interval=30) as ws:
                    self._ws = ws
                    await ws.send("PASS SCHMOOPIIE")
                    await ws.send(f"NICK {nick}")
                    await ws.send(f"JOIN #{self.streamer}")
                    log.info("chat[%s]: connected as %s", self.streamer, nick)
                    async for raw in ws:
                        if not self._running:
                            break
                        for line in str(raw).strip().split("\r\n"):
                            await self._handle_line(line)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    log.warning("chat[%s]: connection lost (%s); reconnecting in 5s",
                                self.streamer, type(e).__name__)
                    await asyncio.sleep(5)
            finally:
                self._ws = None

    async def _handle_line(self, line: str) -> None:
        if line.startswith("PING "):
            try:
                if self._ws is not None:
                    await self._ws.send("PONG " + line[5:])
            except Exception:
                pass
            return
        if " PRIVMSG " not in line:
            return
        try:
            after = line.split(" PRIVMSG ", 1)[1]
            _, text = after.split(" :", 1)
        except ValueError:
            return
        now = time.time()
        self._msg_times.append(now)
        self._recent_msgs.append(text[:80])
        window = self.cfg.chat_spike_window_seconds
        cutoff = now - window
        while self._msg_times and self._msg_times[0] < cutoff:
            self._msg_times.popleft()
        count = len(self._msg_times)
        if (count >= self.cfg.chat_spike_min_msgs
                and (now - self._last_spike_at) > self.cfg.chat_spike_cooldown_seconds):
            self._last_spike_at = now
            sample = list(self._recent_msgs)
            log.info("chat[%s]: SPIKE %d msgs in %ds (e.g. %s)",
                     self.streamer, count, window,
                     (sample[-1] if sample else "")[:50])
            try:
                await self._report_spike(self.streamer, self.stream_id, now,
                                         count, window, sample)
            except Exception:
                log.exception("chat[%s]: spike report failed", self.streamer)


class ChatManager:
    """Owns active ChatListeners; registers on Monitor hooks."""

    def __init__(self, cfg: Config, db: Supabase, on_spike: OnSpike | None = None):
        self.cfg = cfg
        self.db = db
        self._on_spike = on_spike
        self.listeners: Dict[str, ChatListener] = {}

    async def on_live(self, login: str, stream_id: str, stream_meta: dict) -> None:
        if login in self.listeners:
            log.warning("chat[%s]: already listening, skip", login)
            return
        listener = ChatListener(login, stream_id, self.cfg, self._report_spike)
        await listener.start()
        self.listeners[login] = listener

    async def on_offline(self, login: str, stream_id: str) -> None:
        listener = self.listeners.pop(login, None)
        if listener:
            await listener.stop()

    async def _report_spike(self, streamer: str, stream_id: str, detected_at_ts: float,
                            count: int, window: int, sample: List[str]) -> None:
        """Insert spike row; pass resulting row to external callback if any."""
        try:
            row = await self.db.insert("clipper_spikes", {
                "stream_id": stream_id,
                "streamer": streamer,
                "detected_at": datetime.fromtimestamp(detected_at_ts, tz=timezone.utc).isoformat(),
                "messages_in_window": count,
                "window_seconds": window,
                "sample_messages": json.dumps(sample, ensure_ascii=False),
            })
        except Exception:
            log.exception("chat[%s]: spike insert failed", streamer)
            return
        if self._on_spike:
            try:
                await self._on_spike(streamer, stream_id, row)
            except Exception:
                log.exception("chat[%s]: external on_spike handler failed", streamer)

    async def shutdown(self) -> None:
        for login, listener in list(self.listeners.items()):
            try:
                await listener.stop()
            except Exception:
                log.exception("chat[%s]: stop failed", login)
        self.listeners.clear()
