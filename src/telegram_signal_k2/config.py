from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper().replace("/", "").replace("-", "")
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    if not symbol.endswith("USDT"):
        symbol = f"{symbol}USDT"
    return symbol


def display_symbol(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}/USDT"
    return symbol


def parse_topic_threads(raw: str | None) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in _split_csv(raw):
        if ":" not in item:
            raise ValueError(f"Invalid TOPIC_THREADS item: {item!r}")
        timeframe, thread_id = item.split(":", 1)
        result[timeframe.strip()] = int(thread_id.strip())
    return result


def parse_chat_id(raw: str | None) -> int | str | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_run_mode: str
    webhook_url: str | None
    webhook_listen: str
    webhook_port: int
    webhook_path: str
    webhook_secret_token: str | None
    telegram_chat_id: int | str | None
    symbols: list[str]
    timeframes: list[str]
    topic_threads: dict[str, int]
    poll_seconds: int
    kline_limit: int
    kdj_n1: int
    kdj_m1: int
    kdj_m2: int
    buy_alert_limit: float
    sell_alert_limit: float
    indicator_scale_min: float
    indicator_scale_max: float
    volume_ma_period: int
    min_volume_ratio: float
    strong_volume_ratio: float
    require_volume_for_confirmed: bool
    signal_cooldown_candles: int
    state_file: Path
    charts_enabled: bool
    chart_candles: int
    chart_width: float
    chart_height: float
    combined_default_timeframes: list[str]
    combined_default_rule: str
    combined_default_cooldown_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()

        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required. Copy .env.example to .env first.")
        if token == "123456:replace_me" or "replace_me" in token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN still contains the .env.example placeholder.")

        symbols = [normalize_symbol(item) for item in _split_csv(os.getenv("SYMBOLS"))]
        if not symbols:
            symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        timeframes = _split_csv(os.getenv("TIMEFRAMES")) or [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
        ]

        kwargs: dict[str, Any] = {
            "telegram_bot_token": token,
            "telegram_run_mode": os.getenv("TELEGRAM_RUN_MODE", "polling").strip().lower(),
            "webhook_url": os.getenv("WEBHOOK_URL", "").strip() or None,
            "webhook_listen": os.getenv("WEBHOOK_LISTEN", "0.0.0.0").strip(),
            "webhook_port": int(os.getenv("WEBHOOK_PORT", "8080")),
            "webhook_path": os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip(),
            "webhook_secret_token": os.getenv("WEBHOOK_SECRET_TOKEN", "").strip() or None,
            "telegram_chat_id": parse_chat_id(os.getenv("TELEGRAM_CHAT_ID")),
            "symbols": symbols,
            "timeframes": timeframes,
            "topic_threads": parse_topic_threads(os.getenv("TOPIC_THREADS")),
            "poll_seconds": int(os.getenv("POLL_SECONDS", "20")),
            "kline_limit": int(os.getenv("KLINE_LIMIT", "160")),
            "kdj_n1": int(os.getenv("L2_KDJ_N1", "18")),
            "kdj_m1": int(os.getenv("L2_KDJ_M1", "4")),
            "kdj_m2": int(os.getenv("L2_KDJ_M2", "4")),
            "buy_alert_limit": float(os.getenv("BUY_ALERT_LIMIT", "0")),
            "sell_alert_limit": float(os.getenv("SELL_ALERT_LIMIT", "100")),
            "indicator_scale_min": float(os.getenv("INDICATOR_SCALE_MIN", "-10")),
            "indicator_scale_max": float(os.getenv("INDICATOR_SCALE_MAX", "110")),
            "volume_ma_period": int(os.getenv("VOLUME_MA_PERIOD", "20")),
            "min_volume_ratio": float(os.getenv("MIN_VOLUME_RATIO", "0.80")),
            "strong_volume_ratio": float(os.getenv("STRONG_VOLUME_RATIO", "1.30")),
            "require_volume_for_confirmed": env_bool("REQUIRE_VOLUME_FOR_CONFIRMED", False),
            "signal_cooldown_candles": int(os.getenv("SIGNAL_COOLDOWN_CANDLES", "3")),
            "state_file": Path(os.getenv("STATE_FILE", "data/state.json")),
            "charts_enabled": env_bool("CHARTS_ENABLED", True),
            "chart_candles": int(os.getenv("CHART_CANDLES", "80")),
            "chart_width": float(os.getenv("CHART_WIDTH", "12")),
            "chart_height": float(os.getenv("CHART_HEIGHT", "7")),
            "combined_default_timeframes": _split_csv(os.getenv("COMBINED_DEFAULT_TIMEFRAMES"))
            or ["1m", "3m", "5m", "15m"],
            "combined_default_rule": os.getenv("COMBINED_DEFAULT_RULE", "all_match").strip(),
            "combined_default_cooldown_seconds": int(
                os.getenv("COMBINED_DEFAULT_COOLDOWN_SECONDS", "3600")
            ),
        }
        if kwargs["telegram_run_mode"] not in {"polling", "webhook"}:
            raise ValueError("TELEGRAM_RUN_MODE must be either polling or webhook")
        if not kwargs["webhook_path"].startswith("/"):
            raise ValueError("WEBHOOK_PATH must start with /")
        return cls(**kwargs)
