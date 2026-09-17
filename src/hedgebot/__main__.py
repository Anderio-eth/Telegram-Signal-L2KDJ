"""Entry point: build the store and the bot, then run polling.

Deploys onto the existing Frankfurt Render Python service (start command: `python -m hedgebot.main`
with src on PYTHONPATH) and reuses the shared Postgres.
"""

from __future__ import annotations

import logging

from telegram.ext import Application

from .config import Config
from .core.crypto import CredentialCipher
from .db.store import Store
from .telegram.bot import HedgeBot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def _post_init(app: Application) -> None:
    await app.bot_data["store"].connect()
    logging.getLogger(__name__).info("store connected; bot ready")


async def _post_shutdown(app: Application) -> None:
    await app.bot_data["store"].close()


def main() -> None:
    cfg = Config.load()
    store = Store(cfg.database_url, CredentialCipher(cfg.encryption_key))
    app = (
        Application.builder()
        .token(cfg.bot_token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    app.bot_data["store"] = store
    HedgeBot(cfg, store).register(app)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
