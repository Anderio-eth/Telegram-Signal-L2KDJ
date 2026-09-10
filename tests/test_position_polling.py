"""Positions must reach the bot without the websocket.

The socket's host, contract.mexc.com, is the one MEXC blocks at the CDN for some networks — the
REST client has pointed at api.mexc.com since order submission there returned 403. From Render the
socket now gets "403 Invalid response status" on every attempt, forever, and with positions
arriving only that way the bot went silent while its log filled with reconnects.

The REST rows carry the same fields the frames do (positionId, version, state, realised), so the
same parser and the same tracker serve both. A close is the awkward one: over REST a closed
position is not a row saying "closed", it is a row that stopped being there.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.service import CopyService  # noqa: E402

SYMBOL = "SILVER_USDT"
POSITION_ID = 1494344224


def open_row(vol: float = 1.0, version: int = 1) -> dict:
    return {
        "symbol": SYMBOL, "positionType": 1, "holdVol": vol, "leverage": 20, "openType": 1,
        "state": 1, "version": version, "positionId": POSITION_ID, "realised": -0.0005,
    }


def closed_row(realised: float = -0.0013) -> dict:
    return dict(open_row(vol=0.0, version=3), state=3, realised=realised)


class Client:
    def __init__(self, open_rows, closed_rows=()):
        self.open_rows = list(open_rows)
        self.closed_rows = list(closed_rows)

    async def get_open_positions_raw(self, symbol=None):
        return list(self.open_rows)

    async def get_closed_positions_raw(self, symbol=None, *, page_size=50):
        return list(self.closed_rows)


class CopyModeStore:
    """The master path is a COPY folder; REVERSE is watched by the group poller instead."""

    async def get_mode(self, folder_id):
        return ("COPY", None)


def service(client) -> tuple[CopyService, list]:
    svc = CopyService(store=CopyModeStore(), folder_id=1, owner_id=1)
    seen = []

    async def dispatch(event, raw):
        seen.append((event, raw))

    async def master_client():
        return client

    svc._dispatch = dispatch
    svc._master_client = master_client
    return svc, seen


def test_an_open_is_noticed_over_rest():
    svc, seen = service(Client([open_row()]))
    asyncio.run(svc._poll_positions_once())
    assert [e.action.value for e, _ in seen] == ["OPEN"]
    assert seen[0][0].delta_vol == 1.0


def test_a_close_is_noticed_by_the_row_disappearing():
    """No row says "closed"; the position simply stops being listed. The settled row is then
    looked up, and it carries holdVol 0, state 3 and the realised PnL."""
    client = Client([open_row()])
    svc, seen = service(client)
    asyncio.run(svc._poll_positions_once())

    client.open_rows = []
    client.closed_rows = [closed_row()]
    asyncio.run(svc._poll_positions_once())

    assert [e.action.value for e, _ in seen] == ["OPEN", "CLOSE"]


def test_the_close_carries_the_masters_realised_pnl():
    client = Client([open_row()])
    svc, seen = service(client)
    asyncio.run(svc._poll_positions_once())
    client.open_rows, client.closed_rows = [], [closed_row(-1.333)]
    asyncio.run(svc._poll_positions_once())

    _, raw = seen[-1]
    assert raw["realised"] == -1.333


def test_an_unsettled_close_is_retried_rather_than_forgotten():
    """Settlement is not instant. Dropping the position from memory on the first miss would mean
    the close is never seen at all."""
    client = Client([open_row()])
    svc, seen = service(client)
    asyncio.run(svc._poll_positions_once())

    client.open_rows, client.closed_rows = [], []      # gone, but not settled yet
    asyncio.run(svc._poll_positions_once())
    assert [e.action.value for e, _ in seen] == ["OPEN"]

    client.closed_rows = [closed_row()]                 # settles a moment later
    asyncio.run(svc._poll_positions_once())
    assert [e.action.value for e, _ in seen] == ["OPEN", "CLOSE"]


def test_an_unchanged_position_is_not_re_copied():
    client = Client([open_row()])
    svc, seen = service(client)
    for _ in range(4):
        asyncio.run(svc._poll_positions_once())
    assert len(seen) == 1


def test_a_failed_poll_changes_nothing():
    class Broken(Client):
        async def get_open_positions_raw(self, symbol=None):
            from mexc_copy_bot.mexc.rest import MexcError
            raise MexcError(510, "Requests are too frequent", endpoint="/x")

    client = Broken([open_row()])
    svc, seen = service(client)
    asyncio.run(svc._poll_positions_once())
    assert seen == []
    assert svc._seen_positions == {}, "a failed read must not be mistaken for everything closing"


# ── what the event carries onwards ──────────────────────────────────────────
def test_the_report_receives_an_event_that_carries_the_frame():
    """The bug this caught: the tracker builds events from a diff and sets raw=None, so the
    master's realised PnL — which exists only on the frame — never reached the report. The unit
    test for that feature passed anyway, because it built its event by hand instead of letting
    the real dispatch build it.

    So this asserts on the object the REPORT is handed, which is the only thing that matters.
    """
    from mexc_copy_bot.core import service as service_module
    from mexc_copy_bot.core.events import Action, MasterEvent

    reported = []

    class Store(CopyModeStore):
        async def record_event(self, **kw):
            return 1

    class Engine:
        def __init__(self, *a, **kw):
            pass

        async def execute(self, *a, **kw):
            return []

    svc = CopyService(store=Store(), folder_id=1, owner_id=1)
    svc._session = object()
    svc.on_report = lambda event, results: _remember(reported, event)

    async def one_follower(**kwargs):
        return ([object()], [])

    async def no_stops(event):
        return (None, None)

    svc._eligibility = one_follower
    svc._master_stops = no_stops

    event = MasterEvent(
        action=Action.CLOSE, symbol=SYMBOL, position_type=1, master_vol=0.0, delta_vol=1.0,
        leverage=20, open_type=1, dedupe_key="k", raw=None,
    )
    assert event.realized_pnl is None, "the tracker's event starts with no frame"

    original = service_module.CopyEngine
    service_module.CopyEngine = Engine
    try:
        asyncio.run(svc._dispatch(event, closed_row(-1.333)))
    finally:
        service_module.CopyEngine = original

    assert reported, "the report was never called"
    assert reported[0].realized_pnl == -1.333


async def _remember(sink, event):
    sink.append(event)


def test_the_menu_counts_rest_as_being_connected():
    """Keyed to the socket alone, the menu said "reconnecting" forever on a network where the
    socket cannot connect, while every trade was being copied correctly."""
    svc = CopyService(store=CopyModeStore(), folder_id=1, owner_id=1)

    async def check():
        assert svc.master_connected is False, "nothing seen yet"
        svc._last_position_poll = asyncio.get_running_loop().time()
        assert svc.master_connected is True, "a fresh REST poll means the master is being watched"
        svc._last_position_poll = asyncio.get_running_loop().time() - 3600
        assert svc.master_connected is False, "a stale poll must not read as connected"

    asyncio.run(check())
