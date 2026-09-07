"""Tests for REVERSE mode — the automatic hedge.

The hazard is a half-applied reversal: the order goes out on the opposite side while the leverage
call, the task row or the expected-position row still carry the master's side. That desyncs
reconciliation, which then sees a position it never opened and one it thinks is missing.

So these drive the real engine against a fake MEXC client and assert on every side it touches.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402

from mexc_copy_bot.core.copy_engine import OPPOSITE_SIDE, CopyEngine  # noqa: E402
from mexc_copy_bot.core.events import Action, MasterEvent  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, Account, PositionRow  # noqa: E402
from mexc_copy_bot.mexc.rest import SIDE_OPEN_LONG, SIDE_OPEN_SHORT, Position  # noqa: E402

LONG, SHORT = 1, 2


def follower(n: int = 1) -> Account:
    return Account(
        id=n, owner_id=7, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
    )


def event(action: Action, position_type: int) -> MasterEvent:
    return MasterEvent(
        symbol="ADA_USDT", position_type=position_type, action=action, master_vol=10.0,
        delta_vol=10.0, leverage=5, open_type=2, dedupe_key=f"k-{action.value}-{position_type}",
    )


class FakeStore:
    """Just enough of Store for the engine, recording what it was told."""

    def __init__(self) -> None:
        self.tasks: list[dict] = []
        self.positions: dict[tuple[int, str, int], PositionRow] = {}
        self.deleted: list[tuple[int, str, int]] = []

    async def create_task(self, **kw):
        self.tasks.append(kw)
        return len(self.tasks)

    async def get_credentials(self, account_id, owner_id):
        return ("key", "secret")

    async def finish_task(self, *a, **kw):
        pass

    async def set_account_error(self, *a, **kw):
        pass

    async def get_positions(self, account_id):
        return {(s, p): row for (a, s, p), row in self.positions.items() if a == account_id}

    async def upsert_position(self, row: PositionRow):
        self.positions[(row.account_id, row.symbol, row.position_type)] = row

    async def delete_position(self, account_id, symbol, position_type):
        self.deleted.append((account_id, symbol, position_type))
        self.positions.pop((account_id, symbol, position_type), None)


class FakeClient:
    """Stands in for MexcRestClient, capturing every call the engine makes."""

    instances: list["FakeClient"] = []

    # Holding something by default: a close against a flat account is now a no-op, so a fake
    # that always reports no positions would never exercise the close path at all.
    holds = True

    def __init__(self, *_a, **_kw):
        self.orders: list[dict] = []
        self.leverage_calls: list[dict] = []
        self.closed: list[str] = []
        FakeClient.instances.append(self)

    async def submit_order(self, **kw):
        self.orders.append(kw)
        return {"ok": True}

    async def set_leverage(self, **kw):
        self.leverage_calls.append(kw)

    async def close_all(self, symbol=None):
        self.closed.append(symbol)

    async def get_open_positions(self, symbol=None):
        if not FakeClient.holds:
            return []
        return [Position(
            position_id=555, symbol=symbol or "ADA_USDT", position_type=SHORT, open_type=2,
            hold_vol=10.0, leverage=5, open_avg_price=1.0, state=1,
        )]

    async def get_closed_positions(self, symbol=None, **kw):
        return []


@pytest.fixture(autouse=True)
def _fake_client(monkeypatch):
    FakeClient.instances.clear()
    FakeClient.holds = True
    monkeypatch.setattr("mexc_copy_bot.core.copy_engine.MexcRestClient", FakeClient)


async def run(action: Action, master_side: int, *, reverse: bool):
    store = FakeStore()
    engine = CopyEngine(store, session=None, retry_attempts=1)
    results = await engine.execute(event(action, master_side), 1, [follower()], reverse=reverse)
    return store, results


def test_opposite_side_is_a_clean_swap():
    assert OPPOSITE_SIDE[LONG] == SHORT
    assert OPPOSITE_SIDE[SHORT] == LONG


def test_master_long_makes_the_hedge_go_short():
    store, results = asyncio.run(run(Action.OPEN, LONG, reverse=True))
    assert results[0].ok
    assert FakeClient.instances[0].orders[0]["side"] == SIDE_OPEN_SHORT


def test_master_short_makes_the_hedge_go_long():
    asyncio.run(run(Action.OPEN, SHORT, reverse=True))
    assert FakeClient.instances[0].orders[0]["side"] == SIDE_OPEN_LONG


def test_copy_mode_is_untouched():
    asyncio.run(run(Action.OPEN, LONG, reverse=False))
    assert FakeClient.instances[0].orders[0]["side"] == SIDE_OPEN_LONG


def test_leverage_is_set_on_the_side_actually_being_opened():
    """Set on the master's side, MEXC would configure the leg the follower never opens."""
    asyncio.run(run(Action.OPEN, LONG, reverse=True))
    assert FakeClient.instances[0].leverage_calls[0]["position_type"] == SHORT


def test_expected_position_is_recorded_on_the_hedged_side():
    """Reconciliation compares against this; the master's side here would report false drift."""
    store, _ = asyncio.run(run(Action.OPEN, LONG, reverse=True))
    assert list(store.positions) == [(1, "ADA_USDT", SHORT)]


def test_the_task_row_records_the_hedged_side():
    store, _ = asyncio.run(run(Action.OPEN, LONG, reverse=True))
    assert store.tasks[0]["position_type"] == SHORT


def test_increase_also_hedges_and_accumulates_on_one_side():
    store = FakeStore()
    engine = CopyEngine(store, session=None, retry_attempts=1)

    async def both():
        await engine.execute(event(Action.OPEN, LONG), 1, [follower()], reverse=True)
        await engine.execute(event(Action.INCREASE, LONG), 2, [follower()], reverse=True)

    asyncio.run(both())
    row = store.positions[(1, "ADA_USDT", SHORT)]
    assert row.hold_vol == 20.0
    assert (1, "ADA_USDT", LONG) not in store.positions


def test_close_clears_the_hedged_side_not_the_masters():
    store, _ = asyncio.run(run(Action.CLOSE, LONG, reverse=True))
    # close_all takes the whole symbol, so the exchange call is side-agnostic; the bookkeeping
    # must still clear the row that was actually created.
    assert FakeClient.instances[0].closed == ["ADA_USDT"]
    assert store.deleted == [(1, "ADA_USDT", SHORT)]


def test_a_close_against_an_already_flat_account_is_not_an_error():
    """The follower is where the master is. Reporting that as a failed close produced nine red
    lines per close on accounts whose open had failed earlier, burying the real cause."""
    FakeClient.holds = False
    store, results = asyncio.run(run(Action.CLOSE, LONG, reverse=False))
    assert results[0].ok
    assert results[0].error is None
    assert FakeClient.instances[0].closed == []       # nothing sent to the exchange
    assert store.deleted == [(1, "ADA_USDT", LONG)]   # bookkeeping still cleared
