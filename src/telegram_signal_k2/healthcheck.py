from __future__ import annotations

import argparse
import json
from pathlib import Path

from telegram_signal_k2.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram Signal K2 healthcheck")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    settings = Settings.from_env()
    state_path = settings.state_file
    state_path.parent.mkdir(parents=True, exist_ok=True)

    probe = state_path.parent / ".healthcheck"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink(missing_ok=True)

    payload = {
        "status": "ok",
        "run_mode": settings.telegram_run_mode,
        "state_file": str(Path(state_path)),
        "charts_enabled": settings.charts_enabled,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print("ok")


if __name__ == "__main__":
    main()
