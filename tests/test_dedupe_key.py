"""Two separate positions on the same pair must not look like one.

Taken from live frames. SILVER_USDT was traded twice:

    positionId 1493960301   version 1 -> OPEN 3608     22:17
    positionId 1493960301   version 3 -> CLOSE 3608    22:17
    positionId 1493987022   version 1 -> OPEN ...      23:07   <- swallowed
    positionId 1493987022   version 4 -> CLOSE 1686    23:07

The dedupe key was symbol + side + version, and MEXC's `version` counts updates inside ONE
position, starting again at 1 for the next. So the second open produced "SILVER_USDT:1:v1" for a
second time, hit the unique index, and was discarded as a duplicate of a trade that had finished
fifty minutes earlier.

Nothing announces that. A dropped event reaches no follower and reports nothing, so the master
opened, no account moved, and the bot said not a word. Every symbol worked exactly once and was
deaf on that side from then on — the event ids in the table run 26, 27, then 33, with the gap
being conflicting inserts burning the sequence.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.events import MasterPositionTracker, parse_position  # noqa: E402

FIRST, SECOND = 1493960301, 1493987022


def frame(position_id: int, version: int, hold_vol: float, state: int = 1) -> dict:
    return {
        "symbol": "SILVER_USDT", "positionType": 1, "holdVol": hold_vol, "leverage": 981,
        "openType": 1, "state": state, "version": version, "positionId": position_id,
    }


def keys_for(frames) -> list[str]:
    tracker = MasterPositionTracker()
    out = []
    for f in frames:
        event = tracker.apply(parse_position(f))
        if event:
            out.append(event.dedupe_key)
    return out


def test_the_second_position_does_not_reuse_the_first_ones_keys():
    """The bug, in the exact shape the venue produced it."""
    first = keys_for([frame(FIRST, 1, 3608), frame(FIRST, 3, 0, state=3)])
    second = keys_for([frame(SECOND, 1, 1686), frame(SECOND, 4, 0, state=3)])
    assert first and second
    assert not set(first) & set(second), (
        f"a new position reused the old one's keys: {sorted(set(first) & set(second))}"
    )


def test_the_position_id_is_carried_off_the_frame():
    assert parse_position(frame(FIRST, 1, 3608)).position_id == FIRST


def test_a_frame_without_an_id_is_still_parsed():
    """Older recordings have no positionId; losing the event entirely would be worse than a
    weaker key."""
    bare = frame(FIRST, 1, 3608)
    del bare["positionId"]
    snapshot = parse_position(bare)
    assert snapshot is not None and snapshot.position_id is None


def test_replaying_the_same_frame_still_dedupes():
    """The key must stay stable for the same change, or the socket redelivering a frame would
    place a second order — which is what dedupe exists to prevent."""
    tracker = MasterPositionTracker()
    first = tracker.apply(parse_position(frame(FIRST, 1, 3608)))
    tracker_again = MasterPositionTracker()
    again = tracker_again.apply(parse_position(frame(FIRST, 1, 3608)))
    assert first.dedupe_key == again.dedupe_key


def test_two_sides_of_the_same_symbol_stay_distinct():
    long_frame = frame(FIRST, 1, 3608)
    short_frame = dict(frame(SECOND, 1, 3608), positionType=2)
    assert parse_position(long_frame) and parse_position(short_frame)
    a = MasterPositionTracker().apply(parse_position(long_frame))
    b = MasterPositionTracker().apply(parse_position(short_frame))
    assert a.dedupe_key != b.dedupe_key
