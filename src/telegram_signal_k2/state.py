from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CombinedConfig:
    enabled: bool = False
    timeframes: list[str] = field(default_factory=list)
    rule: str = "all_match"
    cooldown_seconds: int = 3600
    thread_id: int | None = None
    symbols_whitelist: list[str] = field(default_factory=list)
    symbols_blacklist: list[str] = field(default_factory=list)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "CombinedConfig":
        return cls(
            enabled=bool(payload.get("enabled", False)),
            timeframes=list(payload.get("timeframes", [])),
            rule=str(payload.get("rule", "all_match")),
            cooldown_seconds=int(payload.get("cooldown_seconds", 3600)),
            thread_id=int(payload["thread_id"]) if payload.get("thread_id") is not None else None,
            symbols_whitelist=list(payload.get("symbols_whitelist", [])),
            symbols_blacklist=list(payload.get("symbols_blacklist", [])),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "timeframes": self.timeframes,
            "rule": self.rule,
            "cooldown_seconds": self.cooldown_seconds,
            "thread_id": self.thread_id,
            "symbols_whitelist": self.symbols_whitelist,
            "symbols_blacklist": self.symbols_blacklist,
        }


@dataclass
class BotState:
    chat_id: int | str | None
    available_symbols: list[str]
    enabled_symbols: list[str]
    topic_threads: dict[str, int]
    signals_enabled: bool = True
    last_alerts: dict[str, int] = field(default_factory=dict)
    latest_confirmed_signals: dict[str, dict[str, Any]] = field(default_factory=dict)
    combined_configs: dict[str, CombinedConfig] = field(default_factory=dict)
    combined_last_alerts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BotState":
        return cls(
            chat_id=payload.get("chat_id"),
            available_symbols=list(payload.get("available_symbols", [])),
            enabled_symbols=list(payload.get("enabled_symbols", [])),
            topic_threads={str(key): int(value) for key, value in payload.get("topic_threads", {}).items()},
            signals_enabled=bool(payload.get("signals_enabled", True)),
            last_alerts={str(key): int(value) for key, value in payload.get("last_alerts", {}).items()},
            latest_confirmed_signals={
                str(key): dict(value)
                for key, value in payload.get("latest_confirmed_signals", {}).items()
            },
            combined_configs={
                str(key): CombinedConfig.from_payload(value)
                for key, value in payload.get("combined_configs", {}).items()
            },
            combined_last_alerts={
                str(key): int(value) for key, value in payload.get("combined_last_alerts", {}).items()
            },
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "available_symbols": self.available_symbols,
            "enabled_symbols": self.enabled_symbols,
            "topic_threads": self.topic_threads,
            "signals_enabled": self.signals_enabled,
            "last_alerts": self.last_alerts,
            "latest_confirmed_signals": self.latest_confirmed_signals,
            "combined_configs": {
                str(key): value.to_payload() for key, value in self.combined_configs.items()
            },
            "combined_last_alerts": self.combined_last_alerts,
        }


class StateStore:
    def __init__(self, path: Path, initial_state: BotState) -> None:
        self._path = path
        self._state = initial_state
        self._lock = asyncio.Lock()

    async def load(self) -> BotState:
        async with self._lock:
            if self._path.exists():
                payload = json.loads(self._path.read_text(encoding="utf-8"))
                self._state = BotState.from_payload(payload)
            return self._copy_state()

    async def get(self) -> BotState:
        async with self._lock:
            return self._copy_state()

    async def update(self, mutator) -> BotState:  # type: ignore[no-untyped-def]
        async with self._lock:
            mutator(self._state)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(self._state.to_payload(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return self._copy_state()

    def _copy_state(self) -> BotState:
        return BotState.from_payload(self._state.to_payload())
