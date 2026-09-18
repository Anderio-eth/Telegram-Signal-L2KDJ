"""Runtime configuration, read once from the environment.

Kept tiny and explicit: every value the bot needs to talk to Telegram, the database, and the two
venues, plus the credential-encryption key. Missing required values fail loudly at startup rather
than at the first trade.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required env var {name}")
    return value


def _ids(name: str) -> set[int]:
    raw = os.getenv(name, "").strip()
    return {int(p) for p in raw.replace(";", ",").split(",") if p.strip()}


@dataclass(frozen=True)
class Config:
    bot_token: str
    allowed_user_ids: set[int]
    database_url: str
    encryption_key: str
    hyperliquid_api_url: str = "https://api.hyperliquid.xyz"
    lighter_api_url: str = "https://api.rh.lighter.xyz"
    entropy_dex: str = "io"

    @staticmethod
    def load() -> "Config":
        # DELTA_BOT_* prefixes: this runs as a third process in the same Render worker as the copy
        # bot (COPY_BOT_*) and the signal bot, so its env vars must not collide. The database URL can
        # point at the SAME shared Postgres — the bot's tables are hb_-prefixed.
        return Config(
            bot_token=_required("DELTA_BOT_TOKEN"),
            allowed_user_ids=_ids("DELTA_BOT_ALLOWED_USER_IDS"),
            database_url=_required("DELTA_BOT_DATABASE_URL"),
            encryption_key=_required("DELTA_BOT_ENCRYPTION_KEY"),
            hyperliquid_api_url=os.getenv("HYPERLIQUID_API_URL", "https://api.hyperliquid.xyz").strip(),
            lighter_api_url=os.getenv("LIGHTER_API_URL", "https://api.rh.lighter.xyz").strip(),
            entropy_dex=os.getenv("ENTROPY_DEX", "io").strip(),
        )
