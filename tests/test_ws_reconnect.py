"""The master socket must never reconnect in a tight loop.

Verified against the live venue: MEXC closes a quiet socket after about sixty seconds, and aiohttp
reports that as the frame iterator simply ending — no exception. The reconnect loop used to sleep
only in its `except` branch, so a venue-side close came straight back round with no delay at all.

Each pass re-connects, re-logs in, and calls on_resync, which reads positions over REST. A socket
that would not stay up therefore became an unpaced stream of REST calls — and being refused for
sending too many requests, while placing no orders whatsoever, is exactly what that looks like
from the outside.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.mexc.websocket import MasterWebSocket  # noqa: E402


def drive(session_lifetime: float, rounds: int, outcome: str = "normal"):
    """Run the reconnect loop for `rounds` sessions and collect how long it slept between them."""
    slept: list[float] = []
    sessions = {"n": 0}

    async def ignore(*args, **kwargs):
        return None

    ws = MasterWebSocket("k", "s", on_position=ignore, reconnect_max_seconds=30.0)
    ws._running = True

    async def fake_session():
        sessions["n"] += 1
        if sessions["n"] > rounds:
            ws._running = False
        # Time passes without really waiting: the loop reads the clock, so the clock is moved.
        loop = asyncio.get_running_loop()
        base = loop.time
        loop.time = lambda _b=base, _n=sessions["n"]: _b() + session_lifetime * _n
        if outcome == "raise":
            raise RuntimeError("socket error")

    async def fake_sleep(seconds):
        slept.append(seconds)

    ws._session_loop = fake_session

    async def run():
        original = asyncio.sleep
        asyncio.sleep = fake_sleep
        try:
            await ws._run()
        finally:
            asyncio.sleep = original

    asyncio.run(run())
    return slept


def test_a_venue_side_close_is_not_reconnected_instantly():
    """The regression: closed without an exception, so nothing waited before trying again."""
    slept = drive(session_lifetime=0.0, rounds=3)
    assert slept, "a socket closed by the venue reconnected with no delay at all"
    assert all(s > 0 for s in slept)


def test_a_socket_that_will_not_stay_up_backs_off():
    slept = drive(session_lifetime=0.0, rounds=5)
    assert slept == sorted(slept), f"delays must grow, got {slept}"
    assert slept[-1] > slept[0]


def test_the_backoff_is_capped():
    slept = drive(session_lifetime=0.0, rounds=12)
    assert max(slept) <= 30.0


def test_a_session_that_lasted_starts_over_from_a_short_delay():
    """A socket up for an hour and then dropped is a fresh problem, not an escalating one — it
    must not inherit a thirty-second wait from trouble that happened long before."""
    slept = drive(session_lifetime=3600.0, rounds=4)
    assert all(s <= 1.0 for s in slept), f"a healthy session should reset the delay, got {slept}"


def test_an_exception_still_backs_off_as_it_always_did():
    slept = drive(session_lifetime=0.0, rounds=4, outcome="raise")
    assert slept and slept == sorted(slept) and slept[-1] > slept[0]
