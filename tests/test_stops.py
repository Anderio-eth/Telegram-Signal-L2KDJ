"""Tests for mirroring the master's stop-loss / take-profit.

Two rules carry the risk here:

  · An unset stop is None, never 0. MEXC sends "no stop" as null, 0 or "0" depending on endpoint,
    and a zero that survives into an order is a stop price of zero — which on a long is a stop
    that can never trigger, and on a short is one that triggers instantly.

  · A hedge gets no stops at all. The master's level sits on the wrong side of a reversed entry,
    so it would cut the hedge's profit and let its loss run — the exact opposite of a stop.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402

from mexc_copy_bot.core.copy_engine import CopyEngine  # noqa: E402
from mexc_copy_bot.core.events import Action, MasterEvent  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, Account, PositionRow  # noqa: E402
from mexc_copy_bot.mexc.rest import PositionStops, _opt_float  # noqa: E402

LONG, SHORT = 1, 2


def follower(n: int = 1) -> Account:
    return Account(
        id=n, owner_id=7, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
    )


def open_event(side: int = LONG) -> MasterEvent:
    return MasterEvent(
        symbol="ADA_USDT", position_type=side, action=Action.OPEN, master_vol=10.0,
        delta_vol=10.0, leverage=5, open_type=2, dedupe_key="k",
    )


class FakeStore:
    def __init__(self) -> None:
        self.positions: dict[tuple, PositionRow] = {}

    async def create_task(self, **kw):
        return 1

    async def get_credentials(self, account_id, owner_id):
        return ("key", "secret")

    async def finish_task(self, *a, **kw):
        pass

    async def set_account_error(self, *a, **kw):
        pass

    async def get_positions(self, account_id):
        return {}

    async def upsert_position(self, row):
        self.positions[(row.account_id, row.symbol, row.position_type)] = row

    async def delete_position(self, *a):
        pass


class FakeClient:
    instances: list["FakeClient"] = []

    def __init__(self, *_a, **_kw):
        self.orders: list[dict] = []
        FakeClient.instances.append(self)

    async def submit_order(self, **kw):
        self.orders.append(kw)
        return {"ok": True}

    async def set_leverage(self, **kw):
        pass

    async def close_all(self, symbol=None):
        pass

    async def get_open_positions(self, symbol=None):
        return []

    async def get_closed_positions(self, symbol=None, **kw):
        return []


@pytest.fixture(autouse=True)
def _fake(monkeypatch):
    FakeClient.instances.clear()
    monkeypatch.setattr("mexc_copy_bot.core.copy_engine.MexcRestClient", FakeClient)


def run_open(*, reverse: bool, stops):
    engine = CopyEngine(FakeStore(), session=None, retry_attempts=1)
    asyncio.run(engine.execute(open_event(), 1, [follower()], reverse=reverse, stops=stops))
    return FakeClient.instances[0].orders[0]


def test_master_levels_ride_along_with_the_opening_order():
    order = run_open(reverse=False, stops=(78000.0, 84000.0))
    assert order["stop_loss_price"] == 78000.0
    assert order["take_profit_price"] == 84000.0


def test_no_stops_means_none_not_zero():
    """A zero price reaching MEXC is a real stop at zero, not the absence of one."""
    order = run_open(reverse=False, stops=(None, None))
    assert order["stop_loss_price"] is None
    assert order["take_profit_price"] is None


def test_only_one_of_the_pair_is_fine():
    order = run_open(reverse=False, stops=(78000.0, None))
    assert order["stop_loss_price"] == 78000.0
    assert order["take_profit_price"] is None


def test_a_hedge_is_opened_without_the_masters_stops():
    """On the opposite side the master's stop is a take-profit and its take-profit is a stop."""
    order = run_open(reverse=True, stops=(None, None))
    assert order["stop_loss_price"] is None
    assert order["take_profit_price"] is None


@pytest.mark.parametrize("raw", [None, "", 0, "0", 0.0, "0.0"])
def test_every_way_mexc_says_no_stop_becomes_none(raw):
    assert _opt_float(raw) is None


@pytest.mark.parametrize("raw,expected", [("78000.5", 78000.5), (78000.5, 78000.5), ("0.0001", 0.0001)])
def test_real_prices_survive(raw, expected):
    assert _opt_float(raw) == expected


def test_position_stops_knows_when_it_is_empty():
    assert not PositionStops(1, 2, "ADA_USDT", LONG, None, None).is_set
    assert PositionStops(1, 2, "ADA_USDT", LONG, 0.5, None).is_set
    assert PositionStops(1, 2, "ADA_USDT", LONG, None, 0.9).is_set
