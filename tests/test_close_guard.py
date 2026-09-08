"""A close must never be mirrored onto an account that has nothing to close.

Found from a live trade. Exiting a long came off the master as side=4 — "open short" — which is
byte-for-byte what genuinely opening a short looks like. The only thing separating them is whether
a long was being held at the time.

That matters because followers do not always fill together. If one misses the entry and then
receives the exit, mirroring it blindly leaves that account holding a fresh naked position facing
the wrong way, with nothing to close it later. On nine accounts that is nine surprise positions.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.orders import (  # noqa: E402
    ORDER_TYPE_LIMIT,
    SIDE_CLOSE_LONG,
    SIDE_CLOSE_SHORT,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    STATE_RESTING,
    parse_order,
    reduces_position,
)

LONG, SHORT = 1, 2


def order(side: int):
    parsed = parse_order({
        "orderId": "X", "symbol": "BTC_USDT", "side": side, "orderType": ORDER_TYPE_LIMIT,
        "price": "78340", "vol": "20", "state": STATE_RESTING, "openType": 1, "leverage": 20,
    })
    assert parsed is not None
    return parsed


def test_selling_while_long_is_a_close():
    """The exact case from the live trade: side=4 against a held long."""
    assert reduces_position(order(SIDE_OPEN_SHORT), {LONG: 20.0}) == LONG


def test_selling_while_flat_is_opening_a_short():
    assert reduces_position(order(SIDE_OPEN_SHORT), {}) is None


def test_buying_while_short_is_a_close():
    assert reduces_position(order(SIDE_OPEN_LONG), {SHORT: 20.0}) == SHORT


def test_buying_while_flat_is_opening_a_long():
    assert reduces_position(order(SIDE_OPEN_LONG), {}) is None


def test_explicit_close_sides_are_always_closes():
    """In hedge mode MEXC names them outright; no position lookup needed to know."""
    assert reduces_position(order(SIDE_CLOSE_LONG), {}) == LONG
    assert reduces_position(order(SIDE_CLOSE_SHORT), {}) == SHORT


def test_a_position_on_the_other_side_does_not_make_it_a_close():
    """Holding a short does not turn "sell" into a close — it adds to the short."""
    assert reduces_position(order(SIDE_OPEN_SHORT), {SHORT: 20.0}) is None
    assert reduces_position(order(SIDE_OPEN_LONG), {LONG: 20.0}) is None


def test_a_zero_holding_counts_as_no_position():
    """MEXC reports a closed position as volume 0 rather than dropping it."""
    assert reduces_position(order(SIDE_OPEN_SHORT), {LONG: 0.0}) is None
