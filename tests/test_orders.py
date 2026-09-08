"""Tests for reading the master's order stream.

This is the layer that decides whether a follower places a resting limit, cancels one, or does
nothing. The failure that matters is doing something twice: MEXC re-sends an order's frame on
every state change, and on partial fills it re-sends the same state. A resent "resting" frame that
gets treated as new stacks a second live order on all nine accounts.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.orders import (  # noqa: E402
    ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET,
    ORDER_TYPE_POST_ONLY,
    SIDE_CLOSE_LONG,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    STATE_CANCELLED,
    STATE_FILLED,
    STATE_RESTING,
    MasterOrderTracker,
    OrderAction,
    parse_order,
)

LONG, SHORT = 1, 2


def frame(**kw):
    base = dict(
        orderId="A1", symbol="BTC_USDT", side=SIDE_OPEN_LONG, orderType=ORDER_TYPE_LIMIT,
        price="79000", vol="12", leverage="20", openType=1, state=STATE_RESTING,
    )
    base.update(kw)
    return base


def order(**kw):
    parsed = parse_order(frame(**kw))
    assert parsed is not None
    return parsed


# ── parsing ────────────────────────────────────────────────────────────────
def test_a_resting_limit_is_recognised():
    o = order()
    assert o.is_resting and not o.is_market
    assert o.price == 79000.0 and o.vol == 12.0
    assert o.position_type == LONG and o.is_opening


def test_post_only_also_rests():
    assert order(orderType=ORDER_TYPE_POST_ONLY).is_resting


def test_a_market_order_never_counts_as_resting():
    o = order(orderType=ORDER_TYPE_MARKET)
    assert o.is_market and not o.is_resting


def test_a_filled_limit_is_no_longer_resting():
    assert not order(state=STATE_FILLED).is_resting


def test_sides_map_to_the_position_they_act_on():
    assert order(side=SIDE_OPEN_SHORT).position_type == SHORT
    assert order(side=SIDE_CLOSE_LONG).position_type == LONG
    assert order(side=SIDE_CLOSE_LONG).is_opening is False


def test_unusable_frames_are_refused_rather_than_guessed():
    assert parse_order({}) is None
    assert parse_order({"symbol": "BTC_USDT"}) is None                      # no id to cancel later
    assert parse_order({"orderId": "1", "side": 1, "state": 2}) is None     # nowhere to route it
    assert parse_order(frame(side=99)) is None                              # unknown side
    assert parse_order("not a dict") is None


# ── decisions ──────────────────────────────────────────────────────────────
def test_a_new_resting_limit_is_mirrored():
    ev = MasterOrderTracker().apply(order())
    assert ev and ev.action is OrderAction.PLACE


def test_the_same_resting_frame_twice_is_mirrored_once():
    """A partial fill re-sends the resting frame. Acting again doubles every follower."""
    t = MasterOrderTracker()
    assert t.apply(order()) is not None
    assert t.apply(order()) is None
    assert t.apply(order(vol="6")) is None


def test_market_orders_are_left_to_the_position_path():
    """Nothing rests, so there is nothing to place ahead of the fill."""
    t = MasterOrderTracker()
    assert t.apply(order(orderType=ORDER_TYPE_MARKET)) is None
    assert t.apply(order(orderType=ORDER_TYPE_MARKET, state=STATE_FILLED)) is None


def test_cancelling_a_mirrored_order_is_relayed():
    t = MasterOrderTracker()
    t.apply(order())
    ev = t.apply(order(state=STATE_CANCELLED))
    assert ev and ev.action is OrderAction.CANCEL


def test_cancelling_something_never_mirrored_does_nothing():
    """Otherwise a cancel for an order the followers never had sends a pointless request each."""
    assert MasterOrderTracker().apply(order(state=STATE_CANCELLED)) is None


def test_a_fill_of_a_mirrored_order_needs_no_action():
    """The followers hold their own copies at the same price; they fill by themselves."""
    t = MasterOrderTracker()
    t.apply(order())
    ev = t.apply(order(state=STATE_FILLED))
    assert ev and ev.action is OrderAction.FILL


def test_a_fill_of_an_unmirrored_order_is_left_to_the_position_path():
    assert MasterOrderTracker().apply(order(state=STATE_FILLED)) is None


def test_two_orders_are_tracked_independently():
    t = MasterOrderTracker()
    assert t.apply(order(orderId="A1")).action is OrderAction.PLACE
    assert t.apply(order(orderId="A2")).action is OrderAction.PLACE
    assert t.apply(order(orderId="A1", state=STATE_CANCELLED)).action is OrderAction.CANCEL
    assert t.apply(order(orderId="A2")) is None      # A2 still resting, already mirrored


def test_reset_forgets_everything():
    """After a reconnect the previous run's ids mean nothing."""
    t = MasterOrderTracker()
    t.apply(order())
    t.reset()
    assert t.apply(order()).action is OrderAction.PLACE
