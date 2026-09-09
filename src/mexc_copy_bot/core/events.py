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
    # Which position this is. MEXC opens a new one every time the pair is entered again, and the
    # id is the only thing in the frame that tells two of them apart — `version` counts updates
    # inside one position and starts again at 1 for the next.
    position_id: int | None = None

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

    @property
    def realized_pnl(self) -> float | None:
        """What the master's own position settled at, or None if this is not a close.

        MEXC's `realised` on the position frame, which is the same field the followers' figures
        come from — so the master's line and theirs mean the same thing and can be added up. It is
        net of fees: on the SILVER close it read -1.333, being -0.3608 of price movement and
        -0.9722 of fees, which is what actually left the balance.

        Only for a close. An opening frame carries a `realised` too — the entry fee — and showing
        that as the trade's PnL would report a loss on every position the moment it opened.
        """
        if self.action is not Action.CLOSE or not self.raw:
            return None
        value = self.raw.get("realised")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


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
            position_id=int(data["positionId"]) if data.get("positionId") is not None else None,
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

    The position id has to be in here. `version` counts updates WITHIN one position and starts
    again at 1 for the next one, so a key of symbol + side + version repeats itself the moment the
    same pair is traded a second time — and the second trade is then dropped as a duplicate of the
    first. Silently: a swallowed event never reaches a follower and never reports anything, so the
    master opens, nothing happens anywhere, and the bot says nothing.

    That is not hypothetical. SILVER_USDT was opened twice; the second open produced
    "SILVER_USDT:1:v1" all over again, collided with the first, and was discarded. Every symbol
    worked exactly once and was then permanently deaf on that side.

    Without an id — no frame seen so far lacks one — the resulting size plus action is the
    fallback: replaying the same frame yields the same key, while a genuinely different change
    produces a different one.
    """
    scope = f"#{snapshot.position_id}" if snapshot.position_id is not None else ""
    if snapshot.version is not None:
        return f"{snapshot.symbol}:{snapshot.position_type}{scope}:v{snapshot.version}"
    return f"{snapshot.symbol}:{snapshot.position_type}{scope}:{action.value}:{after:.10f}"
