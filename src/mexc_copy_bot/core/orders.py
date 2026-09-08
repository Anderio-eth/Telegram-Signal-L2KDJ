"""Watching the master's ORDERS, not just the positions they end up producing.

Why this exists: `push.personal.position` only reports a position once it exists. A limit order
resting in the book is invisible there, so the bot learned about the trade at the moment the
master's limit filled — and then mirrored it with a market order. The followers therefore took the
spread and the taker fee on a trade the master had deliberately placed as a maker, on both the
open and the close. That is exactly what was reported.

`push.personal.order` reports the order itself: its type, its price, and its lifecycle. With it a
resting limit on the master becomes a resting limit on every follower, at the same price, filling
alongside it instead of chasing it afterwards.

Nothing here talks to the exchange or the database — it turns frames into decisions, so the
decisions can be tested without either.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

# MEXC order types. 5 is market; everything else rests in the book in some form.
ORDER_TYPE_LIMIT = 1
ORDER_TYPE_POST_ONLY = 2
ORDER_TYPE_IOC = 3
ORDER_TYPE_FOK = 4
ORDER_TYPE_MARKET = 5
ORDER_TYPE_MARKET_TO_LIMIT = 6

RESTING_TYPES = frozenset({ORDER_TYPE_LIMIT, ORDER_TYPE_POST_ONLY})

# MEXC order states.
STATE_UNINFORMED = 1
STATE_RESTING = 2      # live in the book, wholly or partly unfilled
STATE_FILLED = 3
STATE_CANCELLED = 4
STATE_INVALID = 5

# MEXC order sides.
SIDE_OPEN_LONG = 1
SIDE_CLOSE_SHORT = 2
SIDE_CLOSE_LONG = 3
SIDE_OPEN_SHORT = 4

OPENING_SIDES = frozenset({SIDE_OPEN_LONG, SIDE_OPEN_SHORT})
CLOSING_SIDES = frozenset({SIDE_CLOSE_LONG, SIDE_CLOSE_SHORT})

# Which position a side acts on, so an order can be attributed to a long or a short.
SIDE_TO_POSITION = {
    SIDE_OPEN_LONG: 1,
    SIDE_CLOSE_LONG: 1,
    SIDE_OPEN_SHORT: 2,
    SIDE_CLOSE_SHORT: 2,
}


class OrderAction(str, Enum):
    PLACE = "PLACE"    # a resting order appeared on the master; mirror it
    CANCEL = "CANCEL"  # the master pulled it; pull the followers' copies too
    FILL = "FILL"      # it traded; nothing to send, the copies fill on their own


@dataclass(frozen=True)
class MasterOrder:
    order_id: str
    symbol: str
    side: int
    order_type: int
    price: float
    vol: float
    leverage: int
    open_type: int
    state: int
    # How much of it has actually traded. This is what the position channel will report a moment
    # later, and what has to be discounted there so one fill is not mirrored twice.
    deal_vol: float = 0.0
    external_oid: str | None = None

    @property
    def is_resting(self) -> bool:
        """A live order sitting in the book, which is what can be mirrored as a limit."""
        return self.order_type in RESTING_TYPES and self.state == STATE_RESTING

    @property
    def is_market(self) -> bool:
        return self.order_type not in RESTING_TYPES

    @property
    def position_type(self) -> int | None:
        return SIDE_TO_POSITION.get(self.side)

    @property
    def is_opening(self) -> bool:
        return self.side in OPENING_SIDES


@dataclass(frozen=True)
class OrderEvent:
    action: OrderAction
    order: MasterOrder


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_order(data: dict[str, Any]) -> MasterOrder | None:
    """Turn one `push.personal.order` frame into an order, or None if it is unusable.

    Rejecting rather than guessing: an order with no id cannot be cancelled later, and one with no
    symbol cannot be routed anywhere. Acting on half a frame is worse than ignoring it.
    """
    if not isinstance(data, dict):
        return None
    order_id = data.get("orderId") or data.get("id")
    symbol = data.get("symbol")
    if not order_id or not symbol:
        return None
    try:
        side = int(data.get("side"))
        state = int(data.get("state"))
    except (TypeError, ValueError):
        return None
    if side not in SIDE_TO_POSITION:
        return None

    return MasterOrder(
        order_id=str(order_id),
        symbol=str(symbol),
        side=side,
        order_type=int(data.get("orderType") or ORDER_TYPE_MARKET),
        price=_num(data.get("price")),
        vol=_num(data.get("vol")),
        leverage=int(_num(data.get("leverage"), 0)),
        open_type=int(_num(data.get("openType"), 1)),
        state=state,
        deal_vol=_num(data.get("dealVol")),
        external_oid=(str(data["externalOid"]) if data.get("externalOid") else None),
    )


class MasterOrderTracker:
    """Decides what, if anything, a master order frame means for the followers.

    MEXC re-sends an order's frame on every state change and sometimes repeats a state. Tracking
    what has already been acted on is what keeps a re-sent "resting" frame from placing a second
    copy on every follower.
    """

    def __init__(self) -> None:
        # order id -> the state we last acted on
        self._acted: dict[str, int] = {}

    def forget(self, order_id: str) -> None:
        self._acted.pop(order_id, None)

    def reset(self) -> None:
        self._acted.clear()

    def apply(self, order: MasterOrder) -> OrderEvent | None:
        previous = self._acted.get(order.order_id)

        if order.state == STATE_RESTING:
            if not order.is_resting:
                return None
            if previous == STATE_RESTING:
                # Already mirrored. A partial fill re-sends this frame, and treating it as new
                # would stack a second resting order on every follower.
                return None
            self._acted[order.order_id] = STATE_RESTING
            return OrderEvent(OrderAction.PLACE, order)

        if order.state == STATE_CANCELLED:
            # Only worth relaying if we actually placed something to cancel.
            if previous != STATE_RESTING:
                self._acted[order.order_id] = STATE_CANCELLED
                return None
            self._acted[order.order_id] = STATE_CANCELLED
            return OrderEvent(OrderAction.CANCEL, order)

        if order.state == STATE_FILLED:
            was_mirrored = previous == STATE_RESTING
            self._acted[order.order_id] = STATE_FILLED
            # Reported either way, but the caller acts differently: a mirrored resting order needs
            # nothing (the followers' own copies fill), while a market order still has to be
            # mirrored from the position change.
            return OrderEvent(OrderAction.FILL, order) if was_mirrored else None

        return None
