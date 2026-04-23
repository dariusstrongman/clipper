"""Twitch Helix API wrapper. App-token auth (client_credentials flow) since we
only read public data - no user OAuth needed."""
from __future__ import annotations

import time
import logging
import aiohttp

log = logging.getLogger(__name__)

HELIX = "https://api.twitch.tv/helix"
OAUTH = "https://id.twitch.tv/oauth2/token"


class TwitchClient:
    def __init__(self, client_id: str, client_secret: str):
        self._cid = client_id
        self._csec = client_secret
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    async def _ensure_token(self) -> str:
        """App-token auth. Twitch tokens last ~60 days; we refresh 5 min early."""
        if self._token and time.time() < self._token_expires_at - 300:
            return self._token
        assert self._session is not None
        async with self._session.post(
            OAUTH,
            data={
                "client_id": self._cid,
                "client_secret": self._csec,
                "grant_type": "client_credentials",
            },
        ) as r:
            r.raise_for_status()
            data = await r.json()
        self._token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        log.info("twitch: new app token, expires in %ss", data.get("expires_in"))
        return self._token

    async def _get(self, path: str, params: dict) -> dict:
        assert self._session is not None
        token = await self._ensure_token()
        headers = {"Client-Id": self._cid, "Authorization": f"Bearer {token}"}
        async with self._session.get(f"{HELIX}/{path}", params=params, headers=headers) as r:
            if r.status == 401:
                # Token may have been revoked; force refresh once.
                self._token = None
                token = await self._ensure_token()
                headers["Authorization"] = f"Bearer {token}"
                async with self._session.get(f"{HELIX}/{path}", params=params, headers=headers) as r2:
                    r2.raise_for_status()
                    return await r2.json()
            r.raise_for_status()
            return await r.json()

    async def get_users(self, logins: list[str]) -> list[dict]:
        """Returns [{id, login, display_name, ...}] for each given login."""
        if not logins:
            return []
        out: list[dict] = []
        # Helix allows up to 100 logins per call, we'll have <= 10 usually
        for i in range(0, len(logins), 100):
            batch = logins[i:i + 100]
            params = [("login", l) for l in batch]
            data = await self._get("users", params)
            out.extend(data.get("data", []))
        return out

    async def get_streams(self, user_ids: list[str]) -> list[dict]:
        """Returns live streams only; missing from result = offline."""
        if not user_ids:
            return []
        out: list[dict] = []
        for i in range(0, len(user_ids), 100):
            batch = user_ids[i:i + 100]
            params = [("user_id", uid) for uid in batch]
            data = await self._get("streams", params)
            out.extend(data.get("data", []))
        return out
