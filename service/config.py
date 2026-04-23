"""Load configuration from .env + provide typed accessors."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root (one level up from this file)
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _env(key: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val or ""


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Config:
    twitch_client_id: str
    twitch_client_secret: str
    twitch_app_access_token: str  # optional; if set, used as-is (no refresh)
    supabase_url: str
    supabase_service_key: str
    openai_api_key: str
    data_dir: Path
    log_dir: Path
    streamers: list[str]
    poll_interval_seconds: int
    chat_spike_window_seconds: int
    chat_spike_min_msgs: int
    chat_spike_cooldown_seconds: int
    clip_pre_seconds: int
    clip_post_seconds: int
    buffer_max_minutes: int
    clip_min_score: int


def load() -> Config:
    streamers = [s.strip().lower() for s in _env("STREAMERS", "").split(",") if s.strip()]
    static_token = _env("TWITCH_APP_ACCESS_TOKEN", "")
    # Only one of (secret, static token) is required
    return Config(
        twitch_client_id=_env("TWITCH_CLIENT_ID", required=True),
        twitch_client_secret=_env("TWITCH_CLIENT_SECRET", ""),
        twitch_app_access_token=static_token,
        supabase_url=_env("SUPABASE_URL", required=True),
        supabase_service_key=_env("SUPABASE_SERVICE_KEY", required=True),
        openai_api_key=_env("OPENAI_API_KEY", ""),
        data_dir=Path(_env("DATA_DIR", "/mnt/clipper-storage/clipper")),
        log_dir=Path(_env("LOG_DIR", "/mnt/clipper-storage/clipper/logs")),
        streamers=streamers,
        poll_interval_seconds=_int("POLL_INTERVAL_SECONDS", 30),
        chat_spike_window_seconds=_int("CHAT_SPIKE_WINDOW_SECONDS", 5),
        chat_spike_min_msgs=_int("CHAT_SPIKE_MIN_MSGS", 40),
        chat_spike_cooldown_seconds=_int("CHAT_SPIKE_COOLDOWN_SECONDS", 90),
        clip_pre_seconds=_int("CLIP_PRE_SECONDS", 12),
        clip_post_seconds=_int("CLIP_POST_SECONDS", 18),
        buffer_max_minutes=_int("BUFFER_MAX_MINUTES", 10),
        clip_min_score=_int("CLIP_MIN_SCORE", 6),
    )
