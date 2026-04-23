"""Supabase REST client. We use the service role key from config, so writes work."""
from __future__ import annotations

import logging
import aiohttp

log = logging.getLogger(__name__)


class Supabase:
    def __init__(self, url: str, service_key: str):
        self._url = url.rstrip("/")
        self._key = service_key
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(headers={
            "apikey": self._key,
            "Authorization": f"Bearer {self._key}",
            "Content-Type": "application/json",
        })
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    async def insert(self, table: str, row: dict) -> dict:
        assert self._session is not None
        async with self._session.post(
            f"{self._url}/rest/v1/{table}",
            json=row,
            headers={"Prefer": "return=representation"},
        ) as r:
            r.raise_for_status()
            data = await r.json()
            return data[0] if isinstance(data, list) and data else {}

    async def update(self, table: str, filt: str, row: dict) -> None:
        """filt: e.g. 'id=eq.abc-123' (PostgREST filter syntax)."""
        assert self._session is not None
        async with self._session.patch(
            f"{self._url}/rest/v1/{table}?{filt}",
            json=row,
            headers={"Prefer": "return=minimal"},
        ) as r:
            r.raise_for_status()

    async def select(self, table: str, query: str = "") -> list[dict]:
        assert self._session is not None
        url = f"{self._url}/rest/v1/{table}"
        if query:
            url += f"?{query}"
        async with self._session.get(url) as r:
            r.raise_for_status()
            return await r.json()
