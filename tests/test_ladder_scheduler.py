"""The background loop that fires armed ladders: when it claims one, that it claims only once, and
how it records the outcome. No network — `_run` is stubbed — so what is under test is the timing and
the bookkeeping, not the order sending (which test_ladder covers)."""

from __future__ import annotations

import asyncio
import time

import pytest

from mexc_copy_bot.core.ladder import LadderPlan, LadderReport, SliceResult, LadderConfig
from mexc_copy_bot.core.ladder_scheduler import LadderScheduler


def ladder(id, target_in, parts=3, step=1.0, status="ARMED"):
    return {"id": id, "owner_id": 7, "folder_id": 1, "symbol": "XAG_USDT",
            "long_account": 35, "short_account": 36, "leverage": 1000, "margin_usd": 4.0,
            "parts": parts, "step_seconds": step, "target_epoch": time.time() + target_in,
            "status": status}


class FakeStore:
    def __init__(self, ladders):
        self.ladders = {l["id"]: dict(l) for l in ladders}
        self.finished = {}
        self.expired = False

    async def expire_stuck_ladders(self):
        for l in self.ladders.values():
            if l["status"] == "RUNNING":
                l["status"] = "INTERRUPTED"
        self.expired = True

    async def due_ladders(self, cutoff):
        return [dict(l) for l in self.ladders.values()
                if l["status"] == "ARMED" and l["target_epoch"] <= cutoff]

    async def claim_ladder(self, ladder_id):
        l = self.ladders.get(ladder_id)
        if l and l["status"] == "ARMED":
            l["status"] = "RUNNING"
            return True
        return False

    async def finish_ladder(self, ladder_id, status, report):
        self.ladders[ladder_id]["status"] = status
        self.finished[ladder_id] = (status, report)


def a_report(config, failed=0):
    plan = LadderPlan(config=config, price=63.0, target_notional=4000.0, total_amount="60",
                      slices=[], start_epoch=config.target_epoch)
    results = [SliceResult(i, 35, "LONG", "20", ok=(i >= failed), sent_epoch=0) for i in range(3)]
    return LadderReport(plan=plan, results=results)


def config_of(l):
    return LadderConfig(symbol=l["symbol"], leverage=l["leverage"], margin_usd=l["margin_usd"],
                        parts=l["parts"], step_seconds=l["step_seconds"], target_epoch=l["target_epoch"])


async def drain(scheduler):
    # let the tasks _tick spawned run to completion
    for _ in range(5):
        await asyncio.sleep(0)


def test_a_ladder_outside_its_window_is_not_claimed():
    store = FakeStore([ladder(1, target_in=3600)])  # an hour away
    sched = LadderScheduler(store)

    async def go():
        await sched._tick()
        await drain(sched)
    asyncio.run(go())
    assert store.ladders[1]["status"] == "ARMED"


def test_a_ladder_inside_its_window_is_claimed_and_fired():
    store = FakeStore([ladder(1, target_in=5, parts=3, step=1.0)])  # lead = 8 + 2 = 10 > 5
    reports = []
    sched = LadderScheduler(store, on_report=lambda o, f, t: reports.append((o, f, t)))

    async def fake_run(l, session):
        return a_report(config_of(l))
    sched._run = fake_run

    async def go():
        await sched._tick()
        await drain(sched)
    asyncio.run(go())

    assert store.ladders[1]["status"] == "DONE"
    assert reports and reports[0][0] == 7 and "XAG_USDT" in reports[0][2]


def test_a_partial_fill_is_recorded_partial():
    store = FakeStore([ladder(1, target_in=5)])
    sched = LadderScheduler(store)
    sched._run = lambda l, s: _wrap(a_report(config_of(l), failed=1))
    asyncio.run(_tick_and_drain(sched))
    assert store.ladders[1]["status"] == "PARTIAL"


def test_a_plan_that_cannot_run_is_recorded_failed():
    store = FakeStore([ladder(1, target_in=5)])
    sched = LadderScheduler(store)

    async def fake_run(l, session):
        return "too late"   # _run returns a reason string when the plan does not fit
    sched._run = fake_run
    asyncio.run(_tick_and_drain(sched))
    assert store.ladders[1]["status"] == "FAILED"


def test_a_ladder_is_only_fired_once_even_across_two_ticks():
    store = FakeStore([ladder(1, target_in=5)])
    fired = []
    sched = LadderScheduler(store)

    async def fake_run(l, session):
        fired.append(l["id"])
        await asyncio.sleep(0.05)
        return a_report(config_of(l))
    sched._run = fake_run

    async def go():
        await sched._tick()   # claims and starts firing
        await sched._tick()   # must not claim again (status is RUNNING, and _running guards it)
        for _ in range(10):
            await asyncio.sleep(0.02)
    asyncio.run(go())
    assert fired == [1]


def test_startup_marks_an_interrupted_ladder_and_never_refires_it():
    store = FakeStore([ladder(1, target_in=-10, status="RUNNING")])  # was firing when the process died

    async def go():
        sched = LadderScheduler(store)
        await store.expire_stuck_ladders()
        await sched._tick()
        await drain(sched)
    asyncio.run(go())
    assert store.ladders[1]["status"] == "INTERRUPTED"


async def _wrap(value):
    return value


async def _tick_and_drain(sched):
    await sched._tick()
    for _ in range(5):
        await asyncio.sleep(0)
