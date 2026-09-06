"""Turning master position pushes into copy instructions.

This is the part spec §11 warns about: MEXC sends position STATE, and three master orders adding
up to one position produce three pushes. Treating each push as "open a position" would leave
followers with triple exposure. So every push is diffed against the last known size for that
(symbol, side), and the difference is what gets copied.

Pure functions and a plain dict of state — no network, no database. That is deliberate: this is
the logic most likely to be wrong, and it needs to be testable without either.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

# MEXC position states (see futures docs → Enum Values).
STATE_HOLDING = 1
STATE_SYSTEM_CUSTODY = 2
STATE_CLOSED = 3

# Volumes below this are treated as zero: floating point round-trips through JSON can leave a
# closed position reading as 1e-12 rather than 0.
DUST = 1e-9


class Action(str, Enum):
    OPEN = "OPEN"
    INCREASE = "INCREASE"
    DECREASE = "DECREASE"
    CLOSE = "CLOSE"


@dataclass(frozen=True)
class PositionSnapshot:
    """The master's position for one (symbol, side), as the exchange last reported it."""

    symbol: str
    position_type: int  # 1 long, 2 short
    hold_vol: float
    leverage: int
    open_type: int
    state: int
    version: int | None = None

    @property
    def key(self) -> tuple[str, int]:
        return (self.symbol, self.position_type)

    @property
    def is_open(self) -> bool:
        return self.hold_vol > DUST and self.state != STATE_CLOSED


@dataclass(frozen=True)
class MasterEvent:
    """What changed, expressed so a follower can act on it directly."""

    action: Action
    symbol: str
    position_type: int
    master_vol: float  # master's size AFTER the change
    delta_vol: float  # how much it moved (always positive; action says the direction)
    leverage: int
    open_type: int
    dedupe_key: str
    raw: dict[str, Any] | None = None


def parse_position(data: dict[str, Any]) -> PositionSnapshot | None:
    """Read a push.personal.position frame. Returns None if it isn't usable."""
    symbol = data.get("symbol")
    position_type = data.get("positionType")
    if not symbol or position_type not in (1, 2):
        return None
    try:
        return PositionSnapshot(
            symbol=str(symbol),
            position_type=int(position_type),
            hold_vol=float(data.get("holdVol") or 0),
            leverage=int(data.get("leverage") or 0),
            open_type=int(data.get("openType") or 2),
            state=int(data.get("state") or STATE_HOLDING),
            version=int(data["version"]) if data.get("version") is not None else None,
        )
    except (TypeError, ValueError):
        return None


class MasterPositionTracker:
    """Remembers the master's last known size per (symbol, side) and reports what changed.

    `resync()` replaces the whole picture without emitting events — used after a websocket
    reconnect, where the true current state must become the new baseline rather than being
    mistaken for a giant increase the followers should copy.
    """

    def __init__(self) -> None:
        self._positions: dict[tuple[str, int], PositionSnapshot] = {}

    def snapshot(self) -> dict[tuple[str, int], PositionSnapshot]:
        return dict(self._positions)

    def resync(self, positions: list[PositionSnapshot]) -> None:
        self._positions = {p.key: p for p in positions if p.is_open}

    def apply(self, incoming: PositionSnapshot) -> MasterEvent | None:
        """Fold one push into the tracked state, returning the event it represents (if any)."""
        key = incoming.key
        previous = self._positions.get(key)
        before = previous.hold_vol if previous else 0.0
        after = incoming.hold_vol if incoming.state != STATE_CLOSED else 0.0
        delta = after - before

        if abs(delta) <= DUST:
            # Position pushes also fire for things we don't copy — PnL ticks, margin changes,
            # funding. Same size means nothing to mirror.
            if after > DUST:
                self._positions[key] = incoming
            else:
                self._positions.pop(key, None)
            return None

        if before <= DUST and after > DUST:
            action = Action.OPEN
        elif after <= DUST:
            action = Action.CLOSE
        elif delta > 0:
            action = Action.INCREASE
        else:
            action = Action.DECREASE

        if after > DUST:
            self._positions[key] = incoming
        else:
            self._positions.pop(key, None)

        return MasterEvent(
            action=action,
            symbol=incoming.symbol,
            position_type=incoming.position_type,
            master_vol=after,
            delta_vol=abs(delta),
            leverage=incoming.leverage or (previous.leverage if previous else 0),
            open_type=incoming.open_type,
            dedupe_key=_dedupe_key(incoming, action, after),
            raw=None,
        )


def _dedupe_key(snapshot: PositionSnapshot, action: Action, after: float) -> str:
    """Stable identity for one observed change.

    MEXC's `version` increments per position update, so (position, version) identifies a change
    exactly — that is the ideal key. Where a frame arrives without one, the resulting size plus
    action is used instead: replaying the same frame yields the same key, while a genuinely new
    change moves the size and produces a different one.
    """
    if snapshot.version is not None:
        return f"{snapshot.symbol}:{snapshot.position_type}:v{snapshot.version}"
    return f"{snapshot.symbol}:{snapshot.position_type}:{action.value}:{after:.10f}"
