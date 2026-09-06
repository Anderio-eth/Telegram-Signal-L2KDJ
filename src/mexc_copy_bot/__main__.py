"""Entry point: `python -m mexc_copy_bot`.

Runs as its own process, separate from telegram_signal_k2. They share a Render worker but not an
interpreter — a crash or a slow matplotlib render in the signal bot cannot stall order execution
here, and this process holds trading state the other one has no business touching.
"""

from __future__ import annotations

import asyncio
import logging

from telegram.ext import Application

from .config import Settings
from .core.service import CopyService
from .db.store import Store
from .security.encryption import CredentialCipher
from .telegram.bot import CopyBot

LOGGER = logging.getLogger("mexc_copy_bot")


async def _startup(app: Application) -> None:
    service: CopyService = app.bot_data["service"]
    store: Store = app.bot_data["store"]

    # Resume whatever state we were in before the restart (spec §31). Deliberate: a redeploy in
    # the middle of a trading session should not silently stop mirroring the master.
    if await store.is_running():
        LOGGER.info("was running before restart — resuming")
        LOGGER.info(await service.start())


async def _shutdown(app: Application) -> None:
    service: CopyService = app.bot_data["service"]
    store: Store = app.bot_data["store"]
    # Note: stop() here tears down the socket but does NOT flip the persisted running flag off,
    # so a restart resumes. Only an explicit STOP from Telegram clears it.
    running = await store.is_running()
    await service.stop()
    await store.set_running(running)
    await store.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

    settings = Settings.from_env()
    cipher = CredentialCipher(settings.encryption_key)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    store = loop.run_until_complete(Store.connect(settings.database_url, cipher))

    service = CopyService(
        store,
        retry_attempts=settings.order_retry_attempts,
        reconcile_seconds=settings.reconcile_seconds,
        ws_reconnect_max_seconds=settings.ws_reconnect_max_seconds,
    )
    bot = CopyBot(settings, store, service)
    app = bot.build()
    app.bot_data["service"] = service
    app.bot_data["store"] = store
    app.post_init = _startup
    app.post_shutdown = _shutdown

    LOGGER.info("mexc copy bot starting (followers max %d)", settings.max_followers)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
