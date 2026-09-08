"""Tests for the master socket's status reporting.

These exist because of a real outage. Changing `_status` from taking a message to taking a
connected flag left the log line inside it still referring to the old variable. The name error
fired the moment the socket connected, the reconnect loop caught it and retried, and the retry
raised the same error from the disconnect path — an endless loop in which the socket connected and
died every second, while the menu simply said "master reconnecting".

The bug was invisible to every other test: nothing had ever called this method. It costs four
lines to make that impossible again.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.mexc.websocket import MasterWebSocket  # noqa: E402


async def _nothing(_data):
    pass


def socket(on_status=None) -> MasterWebSocket:
    return MasterWebSocket("key", "secret", on_position=_nothing, on_status=on_status)


def test_reporting_a_connection_does_not_raise():
    asyncio.run(socket()._status(True))


def test_reporting_a_drop_does_not_raise():
    asyncio.run(socket()._status(False, "socket closed by server"))


def test_both_states_reach_the_callback():
    seen = []

    async def on_status(connected, detail=""):
        seen.append((connected, detail))

    ws = socket(on_status)

    async def scenario():
        await ws._status(True)
        await ws._status(False, "boom")

    asyncio.run(scenario())
    assert seen == [(True, ""), (False, "boom")]


def test_a_failing_callback_cannot_kill_the_socket():
    """The callback ends up sending a Telegram message; an outage there must not take the
    connection down with it."""

    async def explode(connected, detail=""):
        raise RuntimeError("telegram is down")

    asyncio.run(socket(explode)._status(True))


def test_connection_state_tracks_the_event():
    ws = socket()
    assert ws.connected is False

    async def scenario():
        assert await ws.wait_connected(0.01) is False
        ws._connected = True
        ws._connected_event.set()
        assert await ws.wait_connected(0.01) is True

    asyncio.run(scenario())
