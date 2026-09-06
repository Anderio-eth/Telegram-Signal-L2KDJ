"""Tests for the master-event logic.

This is the code that decides what followers do with real money, and the failure modes are not
theoretical — the "three orders, one position" case (spec §11) and the post-reconnect replay case
are exactly how a copy bot ends up opening triple exposure.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.events import (  # noqa: E402
    STATE_CLOSED,
    Action,
    MasterPositionTracker,
    PositionSnapshot,
    parse_position,
)


def snap(vol: float, *, symbol: str = "BTC_USDT", side: int = 1, lev: int = 20, state: int = 1, version: int | None = None):
    return PositionSnapshot(
        symbol=symbol, position_type=side, hold_vol=vol, leverage=lev, open_type=2, state=state, version=version
    )


def test_open_is_reported_once():
    tracker = MasterPositionTracker()
    event = tracker.apply(snap(10))
    assert event is not None
    assert event.action is Action.OPEN
    assert event.delta_vol == 10
    assert event.master_vol == 10


def test_three_fills_of_one_position_copy_the_deltas_not_the_totals():
    """The core hazard: a master position built by three orders must not become three opens."""
    tracker = MasterPositionTracker()
    first = tracker.apply(snap(3))
    second = tracker.apply(snap(6))
    third = tracker.apply(snap(10))

    assert first.action is Action.OPEN and first.delta_vol == 3
    assert second.action is Action.INCREASE and second.delta_vol == 3
    assert third.action is Action.INCREASE and third.delta_vol == 4
    # Follower ends up with 3+3+4 = 10, matching the master, not 3+6+10 = 19.
    assert first.delta_vol + second.delta_vol + third.delta_vol == 10


def test_decrease_reports_the_reduction():
    tracker = MasterPositionTracker()
    tracker.apply(snap(15))
    event = tracker.apply(snap(8))
    assert event.action is Action.DECREASE
    assert event.delta_vol == 7
    assert event.master_vol == 8


def test_close_by_zero_volume():
    tracker = MasterPositionTracker()
    tracker.apply(snap(10))
    event = tracker.apply(snap(0))
    assert event.action is Action.CLOSE
    assert event.master_vol == 0
    assert tracker.snapshot() == {}


def test_close_by_state_even_if_volume_still_reported():
    """MEXC can send state=closed while holdVol still carries the last size."""
    tracker = MasterPositionTracker()
    tracker.apply(snap(10))
    event = tracker.apply(snap(10, state=STATE_CLOSED))
    assert event.action is Action.CLOSE
    assert event.master_vol == 0


def test_unchanged_size_emits_nothing():
    """Position pushes also fire for PnL and margin ticks — those must not be copied."""
    tracker = MasterPositionTracker()
    tracker.apply(snap(10))
    assert tracker.apply(snap(10)) is None


def test_long_and_short_are_tracked_separately_in_hedge_mode():
    tracker = MasterPositionTracker()
    long_event = tracker.apply(snap(5, side=1))
    short_event = tracker.apply(snap(7, side=2))
    assert long_event.action is Action.OPEN and long_event.position_type == 1
    assert short_event.action is Action.OPEN and short_event.position_type == 2
    # Closing the long must not disturb the short.
    tracker.apply(snap(0, side=1))
    assert set(tracker.snapshot()) == {("BTC_USDT", 2)}


def test_resync_sets_baseline_without_emitting():
    """After a reconnect the current state is the new baseline, not a giant increase to copy."""
    tracker = MasterPositionTracker()
    tracker.resync([snap(12)])
    assert tracker.apply(snap(12)) is None
    event = tracker.apply(snap(15))
    assert event.action is Action.INCREASE and event.delta_vol == 3


def test_dedupe_key_is_stable_for_a_replayed_frame():
    """The same frame delivered twice must produce the same key, so the DB rejects the second."""
    a = snap(10, version=42)
    tracker_one, tracker_two = MasterPositionTracker(), MasterPositionTracker()
    assert tracker_one.apply(a).dedupe_key == tracker_two.apply(a).dedupe_key


def test_dedupe_key_differs_between_real_changes():
    tracker = MasterPositionTracker()
    first = tracker.apply(snap(10, version=1))
    second = tracker.apply(snap(14, version=2))
    assert first.dedupe_key != second.dedupe_key


def test_dust_volumes_are_treated_as_flat():
    tracker = MasterPositionTracker()
    tracker.apply(snap(10))
    event = tracker.apply(snap(1e-12))
    assert event.action is Action.CLOSE


def test_parse_position_rejects_unusable_frames():
    assert parse_position({}) is None
    assert parse_position({"symbol": "BTC_USDT"}) is None
    assert parse_position({"symbol": "BTC_USDT", "positionType": 9}) is None
    parsed = parse_position({"symbol": "BTC_USDT", "positionType": 1, "holdVol": "5", "leverage": "20", "state": 1})
    assert parsed is not None and parsed.hold_vol == 5.0 and parsed.leverage == 20


def test_size_multiplier_applies_to_the_delta():
    """A half-size follower mirrors half of each change, not half of the master's total."""
    tracker = MasterPositionTracker()
    tracker.apply(snap(10))
    event = tracker.apply(snap(16))
    assert event.delta_vol * 0.5 == 3.0
