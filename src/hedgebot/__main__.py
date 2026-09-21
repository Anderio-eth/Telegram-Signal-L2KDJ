"""Entry point: build the store, the auto-session engine and the bot, then run polling.

Deploys onto the existing Frankfurt Render Python service (`python -m hedgebot`, src on PYTHONPATH)
and reuses the shared Postgres.
"""

from __future__ import annotations

import contextlib
import logging

from telegram.constants import ParseMode
from telegram.ext import Application

from .config import Config
from .core.crypto import CredentialCipher
from .core.pricefeed import PriceFeed
from .core.session import SessionEngine
from .core.sheets import SheetsLogger
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
    app.bot_data["feed"].start()           # realtime io price websocket (public)
    await app.bot_data["engine"].start()   # resume any session that was running before a restart
    logging.getLogger(__name__).info("store + engine + pricefeed ready")


async def _post_shutdown(app: Application) -> None:
    with contextlib.suppress(Exception):
        await app.bot_data["feed"].stop()
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
        # Keep the menu pinned to the bottom: re-post it under the notification, delete the old one.
        bot = app.bot_data.get("hedgebot")
        if bot:
            await bot.push_menu(app, owner_id)

    sheets = SheetsLogger(store, notify=notify)
    feed = PriceFeed(cfg.hyperliquid_api_url, cfg.entropy_dex)
    engine = SessionEngine(store, cfg, notify=notify, sheets=sheets, feed=feed)
    app.bot_data["store"] = store
    app.bot_data["engine"] = engine
    app.bot_data["feed"] = feed
    app.add_error_handler(_on_error)
    hedgebot = HedgeBot(cfg, store, engine, sheets=sheets, feed=feed)
    app.bot_data["hedgebot"] = hedgebot
    hedgebot.register(app)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
