"""Configuration, read once from the environment.

Follows the same shape as telegram_signal_k2.config so both bots feel like one codebase, but
keeps its own prefix (COPY_BOT_*) so the two can never read each other's settings by accident —
they share a process host, and a token mix-up would post trades into the signals channel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    bot_token: str
    allowed_user_id: int
    database_url: str
    encryption_key: str

    # Copy behaviour
    max_followers: int
    order_retry_attempts: int
    reconcile_seconds: int

    # Master monitoring
    ws_ping_seconds: float
    ws_reconnect_max_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            bot_token=_require("COPY_BOT_TOKEN"),
            # A single-owner bot: anyone else who finds it gets refused rather than shown the menu.
            allowed_user_id=int(_require("COPY_BOT_ALLOWED_USER_ID")),
            database_url=_require("COPY_BOT_DATABASE_URL"),
            encryption_key=_require("COPY_BOT_ENCRYPTION_KEY"),
            max_followers=_int("COPY_BOT_MAX_FOLLOWERS", 9),
            # Only transient failures are retried (see copy engine); 3 attempts covers a blip
            # without turning a rejected order into a storm.
            order_retry_attempts=_int("COPY_BOT_RETRY_ATTEMPTS", 3),
            reconcile_seconds=_int("COPY_BOT_RECONCILE_SECONDS", 60),
            # MEXC closes a private socket that goes 60s without a ping.
            ws_ping_seconds=_float("COPY_BOT_WS_PING_SECONDS", 15.0),
            ws_reconnect_max_seconds=_float("COPY_BOT_WS_RECONNECT_MAX_SECONDS", 30.0),
        )
