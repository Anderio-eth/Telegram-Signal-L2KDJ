"""The scheduled hedged-entry maths and the firing loop.

The plan is pure arithmetic, tested exactly; the executor is driven against a fake exchange with a
fake clock, so a whole timed ladder runs in no real time and every order it would send is inspected.
"""

from __future__ import annotations

import asyncio

import pytest

from mexc_copy_bot.core.ladder import LadderConfig, LadderExecutor, plan_ladder
from mexc_copy_bot.mexc.rest import SIDE_OPEN_LONG, SIDE_OPEN_SHORT, MexcError

T = 1_000_000.0  # a round "target" epoch


def config(**over):
    base = dict(symbol="XRP_USDT", long_account=1, short_account=2, leverage=1000,
                margin_usd=150.0, parts=5, step_seconds=1.0, target_epoch=T)
    base.update(over)
    return LadderConfig(**base)


def make_plan(**over):
    kw = dict(price=1.5, size_precision=2, min_order=10.0, latency_seconds=0.2,
              available_long=200.0, available_short=200.0, now=T - 100)
    for key in list(over):
        if key in kw:
            kw[key] = over.pop(key)
    return plan_ladder(config(**over), **kw)


# ── the maths ──────────────────────────────────────────────────────────────────────────────────
def test_size_is_margin_times_leverage():
    plan = make_plan(margin_usd=150, leverage=1000)
    assert plan.target_notional == 150_000
    # $150k / $1.5 = 100,000 contracts, in 5 slices of 20,000
    assert plan.total_amount == "100000"
    assert [s.amount for s in plan.slices] == ["20000"] * 5


def test_the_last_slice_absorbs_the_rounding():
    # 100 / $3 = 33.33 contracts, precision 2, over 3 slices: 11.11 each, last takes the remainder
    plan = make_plan(margin_usd=0.1, leverage=1000, price=3.0, parts=3, min_order=1.0)
    amounts = [s.amount for s in plan.slices]
    assert amounts == ["11.11", "11.11", "11.11"]
    assert plan.total_amount == "33.33"
    # a case where flooring actually leaves a tail on the last slice
    plan2 = make_plan(margin_usd=0.1, leverage=1000, price=3.0, parts=4, min_order=1.0)
    a = [s.amount for s in plan2.slices]
    assert sum(float(x) for x in a) == pytest.approx(33.33)
    assert a[-1] != a[0]


def test_the_last_slice_lands_on_T_and_the_first_is_step_times_earlier():
    plan = make_plan(parts=5, step_seconds=1.0, latency_seconds=0.2)
    assert plan.slices[-1].send_epoch == pytest.approx(T - 0.2)          # fills at T
    assert plan.slices[0].send_epoch == pytest.approx(T - 0.2 - 4.0)      # 4 steps earlier
    assert plan.start_epoch == pytest.approx(T - 4.2)


def test_a_single_slice_just_beats_T_by_the_latency():
    plan = make_plan(parts=1)
    assert plan.ok
    assert len(plan.slices) == 1
    assert plan.slices[0].send_epoch == pytest.approx(T - 0.2)


def test_starting_in_the_past_is_refused_not_fired_late():
    # 5 slices, 1s apart, need 4.2s before T; only 2s remain
    plan = make_plan(parts=5, step_seconds=1.0, now=T - 2.0)
    assert not plan.ok
    assert any("too late" in e for e in plan.errors)


def test_a_slice_below_the_minimum_is_refused_with_advice():
    plan = make_plan(margin_usd=0.02, leverage=100, price=1.5, parts=5, min_order=10.0)
    assert not plan.ok
    assert any("below the minimum" in e for e in plan.errors)


def test_margin_beyond_the_free_balance_is_refused_per_account():
    plan = make_plan(margin_usd=150, available_short=100.0)
    assert not plan.ok
    assert any("short account" in e and "needs $150" in e for e in plan.errors)


def test_a_step_under_the_latency_warns_but_still_plans():
    plan = make_plan(parts=3, step_seconds=0.1, latency_seconds=0.2)
    assert plan.ok
    assert any("under the" in w for w in plan.warnings)


def test_the_two_accounts_must_differ():
    plan = make_plan(long_account=1, short_account=1)
    assert not plan.ok
    assert any("must be different" in e for e in plan.errors)


# ── the firing loop ──────────────────────────────────────────────────────────────────────────────
class FakeClock:
    """A clock the test advances by hand; sleeping just jumps it forward."""

    def __init__(self, start):
        self.t = start

    def now(self):
        return self.t

    async def sleep(self, seconds):
        self.t += max(0.0, seconds)


class FakeClient:
    def __init__(self, *, fail_on=None):
        self.orders = []
        self.leverage_set = None
        self._fail_on = fail_on or set()

    async def set_leverage(self, **kw):
        self.leverage_set = kw["leverage"]

    async def submit_order(self, *, symbol, side, vol, leverage, open_type, external_oid):
        self.orders.append({"symbol": symbol, "side": side, "vol": vol, "oid": external_oid})
        if len(self.orders) in self._fail_on:
            raise MexcError(2005, "insufficient balance", endpoint="/order")
        return {"orderId": f"o{len(self.orders)}"}


def run_ladder(plan, long_client, short_client):
    ex = LadderExecutor(long_client, short_client, symbol=plan.config.symbol, leverage=plan.config.leverage)
    clock = FakeClock(plan.start_epoch - 5)  # start well before the first slice

    async def go():
        await ex.prepare()
        return await ex.run(plan, sleep=clock.sleep, clock=clock.now)

    return asyncio.run(go())


def test_a_clean_run_fires_every_slice_on_both_sides():
    plan = make_plan(parts=4)
    long_c, short_c = FakeClient(), FakeClient()
    report = run_ladder(plan, long_c, short_c)

    assert len(long_c.orders) == 4 and len(short_c.orders) == 4
    assert {o["side"] for o in long_c.orders} == {SIDE_OPEN_LONG}
    assert {o["side"] for o in short_c.orders} == {SIDE_OPEN_SHORT}
    assert report.filled_long == report.filled_short == float(plan.total_amount)
    assert long_c.leverage_set == short_c.leverage_set == 1000
    assert all(r.ok for r in report.results)


def test_both_sides_of_a_slice_carry_the_same_amount():
    plan = make_plan(parts=4, margin_usd=0.1, leverage=1000, price=3.0, min_order=1.0)
    long_c, short_c = FakeClient(), FakeClient()
    run_ladder(plan, long_c, short_c)
    assert [o["vol"] for o in long_c.orders] == [o["vol"] for o in short_c.orders]


def test_slices_are_sent_in_order_at_their_scheduled_times():
    plan = make_plan(parts=3, step_seconds=2.0)
    sent = []

    class Recording(FakeClient):
        async def submit_order(self, **kw):
            sent.append(self_clock.now())
            return await super().submit_order(**kw)

    ex = LadderExecutor(Recording(), Recording(), symbol="XRP_USDT", leverage=1000)
    self_clock = FakeClock(plan.start_epoch - 5)
    asyncio.run(ex.run(plan, sleep=self_clock.sleep, clock=self_clock.now))
    # three slices, two accounts each -> the send times pair up at the three scheduled epochs
    assert sorted(set(sent)) == [pytest.approx(s.send_epoch) for s in plan.slices]


def test_a_refused_slice_is_recorded_and_the_rest_go_on():
    plan = make_plan(parts=4)
    long_c = FakeClient(fail_on={2})   # the long account's 2nd order is refused
    short_c = FakeClient()
    report = run_ladder(plan, long_c, short_c)

    # every slice was still attempted on both sides
    assert len(long_c.orders) == 4 and len(short_c.orders) == 4
    failed = [r for r in report.results if not r.ok]
    assert len(failed) == 1 and failed[0].side == "LONG"
    # the long side is short by exactly the one refused slice; the short side is whole
    assert report.filled_short == float(plan.total_amount)
    assert report.filled_long == float(plan.total_amount) - float(plan.slices[1].amount)
    assert "insufficient balance" in report.summary()
