"""Entry point: build the store, the auto-session engine and the bot, then run polling.

Deploys onto the existing Frankfurt Render Python service (`python -m hedgebot`, src on PYTHONPATH)
and reuses the shared Postgres.
"""

from __future__ import annotations

import logging

from telegram.constants import ParseMode
from telegram.ext import Application

from .config import Config
from .core.crypto import CredentialCipher
from .core.session import SessionEngine
from .db.store import Store
from .telegram.bot import HedgeBot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def _on_error(update: object, context) -> None:
    logging.getLogger(__name__).error("handler error", exc_info=context.error)


async def _post_init(app: Application) -> None:
    await app.bot_data["store"].connect()
    await app.bot_data["engine"].start()   # resume any session that was running before a restart
    logging.getLogger(__name__).info("store + engine ready")


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

    async def notify(owner_id: int, text: str) -> None:
        await app.bot.send_message(chat_id=owner_id, text=text, parse_mode=ParseMode.HTML)

    engine = SessionEngine(store, cfg, notify=notify)
    app.bot_data["store"] = store
    app.bot_data["engine"] = engine
    app.add_error_handler(_on_error)
    HedgeBot(cfg, store, engine).register(app)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
