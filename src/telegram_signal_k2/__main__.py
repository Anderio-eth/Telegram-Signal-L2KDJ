from __future__ import annotations

import logging

from telegram_signal_k2.config import Settings
from telegram_signal_k2.telegram_app import create_application


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = Settings.from_env()
    application = create_application(settings)
    if settings.telegram_run_mode == "webhook":
        if not settings.webhook_url:
            raise RuntimeError("WEBHOOK_URL is required when TELEGRAM_RUN_MODE=webhook")
        application.run_webhook(
            listen=settings.webhook_listen,
            port=settings.webhook_port,
            url_path=settings.webhook_path.lstrip("/"),
            webhook_url=f"{settings.webhook_url.rstrip('/')}{settings.webhook_path}",
            secret_token=settings.webhook_secret_token,
            allowed_updates=UpdateTypes.ALL,
        )
    else:
        application.run_polling(allowed_updates=UpdateTypes.ALL)


class UpdateTypes:
    ALL = ["message", "callback_query"]


if __name__ == "__main__":
    main()
