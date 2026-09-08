"""The one that guards real money: a limit fill must never be copied twice.

With limit mirroring on, one master trade shows up on the websocket twice — first as an order that
rests and then fills, and a moment later as a position that grew. The order path has already put a
matching limit on every follower. If the position path then also fires, every account opens a
second time, at market, doubling exposure on nine accounts at once.

These drive CopyService's real handlers against fake frames, so the arithmetic that prevents it is
tested without an exchange or a database.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402

from mexc_copy_bot.core.orders import (  # noqa: E402
    ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET,
    SIDE_OPEN_LONG,
    STATE_FILLED,
    STATE_RESTING,
)
from mexc_copy_bot.core.service import CopyService  # noqa: E402

OWNER = 42
LONG = 1


class FakeStore:
    """Only what the handlers touch. mirror_limits is on: that is the case under test."""

    def __init__(self):
        self.mirror = True

    async def get_mirror_limits(self, owner_id): return self.mirror
    async def get_mode(self, owner_id): return ("COPY", None)
    async def list_accounts(self, owner_id, kind=None): return []
    async def record_mirrored_order(self, **kw): pass
    async def get_mirrored_orders(self, owner_id, mid): return []
    async def clear_mirrored_order(self, owner_id, mid): pass
    async def get_master(self, owner_id): return None
    async def get_credentials(self, aid, oid): return None


def service(monkeypatch):
    svc = CopyService(FakeStore(), OWNER)
    dispatched = []

    async def spy(event, raw):
        dispatched.append(event)

    monkeypatch.setattr(svc, "_dispatch", spy)
    # Placing the mirrored copies needs an exchange; the question here is the bookkeeping.
    async def noop_place(order): pass
    monkeypatch.setattr(svc, "_place_mirrored", noop_place)
    return svc, dispatched


def order_frame(state, *, order_type=ORDER_TYPE_LIMIT, vol="12", deal="12"):
    return {
        "orderId": "M1", "symbol": "BTC_USDT", "side": SIDE_OPEN_LONG, "orderType": order_type,
        "price": "79000", "vol": vol, "dealVol": deal, "leverage": "20", "openType": 1,
        "state": state,
    }


def position_frame(hold_vol):
    return {
        "symbol": "BTC_USDT", "positionType": LONG, "holdVol": hold_vol, "leverage": 20,
        "openType": 1, "state": 1, "version": int(hold_vol),
    }


def test_a_mirrored_limit_fill_is_not_copied_again_by_the_position_path(monkeypatch):
    svc, dispatched = service(monkeypatch)

    async def scenario():
        await svc._handle_order(order_frame(STATE_RESTING))            # mirrored as a limit
        await svc._handle_order(order_frame(STATE_FILLED))             # it fills
        await svc._handle_position(position_frame(12))                 # position reports the same

    asyncio.run(scenario())
    assert dispatched == [], "the fill was copied a second time at market"


def test_a_market_order_still_goes_through_the_position_path(monkeypatch):
    """Nothing rested, so nothing was pre-placed and the position path is the only copy."""
    svc, dispatched = service(monkeypatch)

    async def scenario():
        await svc._handle_order(order_frame(STATE_FILLED, order_type=ORDER_TYPE_MARKET))
        await svc._handle_position(position_frame(12))

    asyncio.run(scenario())
    assert len(dispatched) == 1
    assert dispatched[0].delta_vol == 12


def test_only_the_uncovered_part_is_copied(monkeypatch):
    """The master's limit filled 12, then bought 8 more at market. Only the 8 needs mirroring."""
    svc, dispatched = service(monkeypatch)

    async def scenario():
        await svc._handle_order(order_frame(STATE_RESTING))
        await svc._handle_order(order_frame(STATE_FILLED))
        await svc._handle_position(position_frame(20))

    asyncio.run(scenario())
    assert len(dispatched) == 1
    assert dispatched[0].delta_vol == pytest.approx(8.0)


def test_a_later_unrelated_trade_is_copied_normally(monkeypatch):
    """The discount must be consumed once, not left suppressing everything that follows."""
    svc, dispatched = service(monkeypatch)

    async def scenario():
        await svc._handle_order(order_frame(STATE_RESTING))
        await svc._handle_order(order_frame(STATE_FILLED))
        await svc._handle_position(position_frame(12))   # covered
        await svc._handle_position(position_frame(19))   # +7 at market, must copy

    asyncio.run(scenario())
    assert len(dispatched) == 1
    assert dispatched[0].delta_vol == pytest.approx(7.0)


def test_with_mirroring_off_nothing_is_discounted(monkeypatch):
    """The old behaviour has to stay exactly as it was while the toggle is off."""
    svc, dispatched = service(monkeypatch)
    svc._store.mirror = False

    async def scenario():
        await svc._handle_order(order_frame(STATE_RESTING))
        await svc._handle_order(order_frame(STATE_FILLED))
        await svc._handle_position(position_frame(12))

    asyncio.run(scenario())
    assert len(dispatched) == 1
    assert dispatched[0].delta_vol == 12


def test_a_reconnect_drops_any_pending_discount(monkeypatch):
    """After a resync the baseline already includes the fill; a stale discount would then
    silently swallow the next real trade."""
    svc, dispatched = service(monkeypatch)

    async def scenario():
        await svc._handle_order(order_frame(STATE_RESTING))
        await svc._handle_order(order_frame(STATE_FILLED))
        svc._order_tracker.reset()
        svc._filled_by_limit.clear()
        await svc._handle_position(position_frame(12))

    asyncio.run(scenario())
    assert len(dispatched) == 1
