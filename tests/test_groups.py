"""Two legs, no master: whoever trades first sets the direction.

There is no account in charge. Whichever one is opened by hand becomes the trigger; the rest of
its leg takes the same side, and the other leg takes the opposite, at the same symbol, size and
leverage.

The hazard that dominates this file is not the arithmetic — it is that the bot's own orders are
themselves position changes. Copy an entry to five accounts and five accounts have just opened
something. If the watcher reads those as five more people trading by hand, it copies them, and
copies the copies, and does not stop. Every test about reseeding is about that.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core import service as service_module  # noqa: E402
from mexc_copy_bot.core.copy_engine import FollowerResult, side_for_group  # noqa: E402
from mexc_copy_bot.core.events import Action, PositionSnapshot  # noqa: E402
from mexc_copy_bot.core.service import CopyService  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MASTER, MODE_REVERSE, Account  # noqa: E402

LONG, SHORT = 1, 2


def account(account_id: int, group: int, kind: str = FOLLOWER) -> Account:
    return Account(
        id=account_id, owner_id=1, label="acc " + str(account_id), kind=kind, api_key_hint="k",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
        direction="COPY" if group == 1 else "REVERSE",
    )


# ── which side each leg takes ──────────────────────────────────────────────
def test_the_triggers_own_leg_follows_it():
    assert side_for_group(account(1, 1), trigger_group=1, trigger_side=LONG) == LONG
    assert side_for_group(account(1, 1), trigger_group=1, trigger_side=SHORT) == SHORT


def test_the_other_leg_takes_the_opposite():
    assert side_for_group(account(2, 2), trigger_group=1, trigger_side=LONG) == SHORT
    assert side_for_group(account(2, 2), trigger_group=1, trigger_side=SHORT) == LONG


def test_it_works_the_same_whichever_leg_moved_first():
    """There is no first-among-equals: leg 2 triggering must mirror leg 1 triggering exactly."""
    assert side_for_group(account(1, 1), trigger_group=2, trigger_side=LONG) == SHORT
    assert side_for_group(account(2, 2), trigger_group=2, trigger_side=LONG) == LONG


def test_the_master_row_is_just_another_account():
    """In REVERSE it holds no authority — only whichever leg somebody put it in."""
    master_in_two = account(9, 2, kind=MASTER)
    assert master_in_two.group == 2
    assert side_for_group(master_in_two, trigger_group=1, trigger_side=LONG) == SHORT


# ── watching every account ─────────────────────────────────────────────────
def snapshot(symbol="BTC_USDT", side=LONG, vol=10.0, version=1, position_id=1):
    return PositionSnapshot(
        symbol=symbol, position_type=side, hold_vol=vol, leverage=5, open_type=1,
        state=1, version=version, position_id=position_id,
    )


class Store:
    def __init__(self, accounts):
        self.accounts = accounts
        self.events = []

    async def get_mode(self, folder_id):
        return (MODE_REVERSE, None)

    async def get_master(self, folder_id):
        return next((a for a in self.accounts if a.kind == MASTER), None)

    async def list_accounts(self, folder_id, kind=None):
        return [a for a in self.accounts if a.kind == FOLLOWER]

    async def get_credentials(self, account_id, owner_id):
        return ("k", "s")

    async def record_event(self, **kw):
        self.events.append(kw)
        return len(self.events)


class Engine:
    """Stands in for the orders. Everything around it — expectations, reseeding, the guard
    against copying its own work — is the real code under test."""

    def __init__(self, *args, **kwargs):
        pass

    async def execute_groups(self, event, event_id, accounts, trigger_group):
        return [FollowerResult(a, True, event.action, event.delta_vol) for a in accounts]


def build(accounts, holdings):
    """A service whose account reads come from `holdings`: account id -> list of snapshots.

    The real _copy_across_groups runs; only the order placement is stubbed.
    """
    svc = CopyService(Store(accounts), folder_id=1, owner_id=1)
    svc._session = object()
    copied = []

    async def read(acc):
        return list(holdings.get(acc.id, []))

    svc._read_snapshots = read

    original = svc._copy_across_groups

    async def watched(trigger, event):
        copied.append((trigger.id, event))
        service_module.CopyEngine, previous = Engine, service_module.CopyEngine
        try:
            await original(trigger, event)
        finally:
            service_module.CopyEngine = previous

    svc._copy_across_groups = watched
    return svc, copied


# ── the trigger ────────────────────────────────────────────────────────────
def test_an_account_opening_by_hand_becomes_the_trigger():
    accounts = [account(1, 1), account(2, 2)]
    holdings = {}
    svc, copied = build(accounts, holdings)

    asyncio.run(svc._poll_groups_once())          # everyone flat; nothing to do
    assert copied == []

    holdings[1] = [snapshot()]                    # somebody opens on account 1
    asyncio.run(svc._poll_groups_once())
    assert [t for t, _ in copied] == [1]
    assert copied[0][1].action is Action.OPEN
    assert copied[0][1].delta_vol == 10.0


def test_the_bots_own_copies_are_not_read_as_new_trades():
    """The runaway this exists to prevent: five accounts open because the bot opened them, and
    each of those looks exactly like somebody trading by hand."""
    accounts = [account(1, 1), account(2, 2), account(3, 2)]
    holdings = {}
    svc, copied = build(accounts, holdings)
    asyncio.run(svc._poll_groups_once())

    holdings[1] = [snapshot()]
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1

    # The copy lands: the other two are now holding something too.
    holdings[2] = [snapshot(side=SHORT)]
    holdings[3] = [snapshot(side=SHORT)]
    for _ in range(5):
        asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1, f"copied {len(copied)} times; the bot is chasing its own orders"


def test_nothing_is_copied_while_a_copy_is_being_placed():
    accounts = [account(1, 1), account(2, 2)]
    svc, copied = build(accounts, {1: [snapshot()]})
    svc._copying = True
    asyncio.run(svc._poll_groups_once())
    assert copied == []


def test_only_one_trigger_is_taken_from_a_single_pass():
    """Two accounts opened by hand within the same second would otherwise each instruct the other,
    and the two instructions would fight."""
    accounts = [account(1, 1), account(2, 2)]
    svc, copied = build(accounts, {1: [snapshot()], 2: [snapshot(side=SHORT, position_id=2)]})
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1
    # And it must be the first one seen, not whichever happened to be read last: the two are
    # opposite instructions, so which one wins has to be decided, not left to ordering luck.
    assert copied[0][0] == 1


def test_a_closed_position_is_not_copied_and_does_not_linger():
    """Exits are never copied here. The tracker still has to forget them, or reopening the same
    pair later would look like an increase on top of a position that is gone."""
    accounts = [account(1, 1), account(2, 2)]
    holdings = {1: [snapshot()]}
    svc, copied = build(accounts, holdings)
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1

    holdings[1] = []                               # closed by hand
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1, "a close must not be copied"

    holdings[1] = [snapshot(position_id=2, version=1)]   # opened again, fresh position
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 2, "a new entry after a close must be copied like any other"
    assert copied[1][1].action is Action.OPEN


def test_adding_to_a_position_by_hand_is_copied_too():
    accounts = [account(1, 1), account(2, 2)]
    holdings = {1: [snapshot(vol=10.0)]}
    svc, copied = build(accounts, holdings)
    asyncio.run(svc._poll_groups_once())

    holdings[1] = [snapshot(vol=25.0, version=2)]
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 2
    assert copied[1][1].action is Action.INCREASE
    assert copied[1][1].delta_vol == 15.0, "only the addition is copied, not the whole position"


def test_an_account_that_cannot_be_read_does_not_trigger_anything():
    accounts = [account(1, 1), account(2, 2)]
    svc, copied = build(accounts, {})

    async def unreadable(acc):
        return None if acc.id == 1 else []

    svc._read_snapshots = unreadable
    asyncio.run(svc._poll_groups_once())
    assert copied == []


def test_a_folder_with_one_account_copies_nothing():
    svc, copied = build([account(1, 1)], {1: [snapshot()]})
    asyncio.run(svc._poll_groups_once())
    assert copied == []


# ── the lights ─────────────────────────────────────────────────────────────
def test_watching_keeps_every_light_current():
    accounts = [account(1, 1), account(2, 2)]
    holdings = {1: [snapshot()]}
    svc, _ = build(accounts, holdings)
    asyncio.run(svc._poll_groups_once())
    assert svc.account_status == {1: True, 2: False}

    holdings[1] = []
    asyncio.run(svc._poll_groups_once())
    assert svc.account_status[1] is False, "a liquidated or hand-closed leg must go dark"


def test_watching_stays_within_one_accounts_allowance():
    """Every account is read once per pass, and each has its own allowance of roughly ten requests
    a second. The poll interval is the largest part of the delay before a copy exists, so it is
    kept short — but not so short that watching starves the orders it exists to trigger."""
    from mexc_copy_bot.core.service import GROUP_POLL_SECONDS
    from mexc_copy_bot.mexc.rest import PRIVATE_INTERVAL

    per_account = 1.0 / GROUP_POLL_SECONDS
    allowed = 1.0 / PRIVATE_INTERVAL
    assert per_account < allowed / 2, (
        f"watching alone would use {per_account:.1f} of the {allowed:.1f} requests a second each "
        f"account allows, leaving too little for the orders"
    )


# ── groups and the COPY-mode machinery must not both act ───────────────────
class OrderStore(Store):
    def __init__(self, accounts, mode=MODE_REVERSE):
        super().__init__(accounts)
        self._mode = mode

    async def get_mode(self, folder_id):
        return (self._mode, None)

    async def set_running(self, folder_id, running):
        self.running = running


def test_the_masters_resting_orders_are_not_mirrored_in_groups():
    """Otherwise the same trade is copied twice: once as a limit placed here, and again at market
    when the master's own limit fills and the group poller reads it as somebody opening by hand."""
    svc = CopyService(OrderStore([account(1, 1), account(2, 2)]), folder_id=1, owner_id=1)
    reached = []

    async def master_client():
        reached.append("asked the venue")
        raise AssertionError("groups mode must not poll the master's orders")

    svc._master_client = master_client
    asyncio.run(svc._poll_orders_once())
    assert reached == []


def test_a_copy_folder_still_mirrors_them():
    """The guard is about groups only; a copy folder's limit mirroring is untouched."""
    svc = CopyService(OrderStore([account(1, 1)], mode="COPY"), folder_id=1, owner_id=1)
    asked = []

    async def master_client():
        asked.append(True)
        return None

    svc._master_client = master_client
    asyncio.run(svc._poll_orders_once())
    assert asked == [True]


def test_groups_start_without_a_master():
    """There is no master in this mode, so requiring one would leave a correctly configured
    folder unable to start."""
    svc = CopyService(OrderStore([account(1, 1), account(2, 2)]), folder_id=1, owner_id=1)
    started = asyncio.run(svc.start())
    assert "Master" not in started
    assert svc.running, "the background work must be going even with no socket"
    asyncio.run(svc.stop())


def test_one_account_is_not_enough_to_start():
    """Two legs need two accounts; starting with one would watch and copy nothing while looking
    like it was working."""
    svc = CopyService(OrderStore([account(1, 1)]), folder_id=1, owner_id=1)
    assert "два акаунти" in asyncio.run(svc.start())
    assert not svc.running


# ── the whole cycle, start to finish ────────────────────────────────────────
def test_open_copy_then_close_one_leg():
    """A full round with no network: somebody opens by hand, both legs fill, one leg is closed
    with the button and reports what it made, and the other is left alone."""
    accounts = [account(1, 1), account(2, 1), account(3, 2), account(4, 2)]
    holdings = {}
    svc, copied = build(accounts, holdings)
    asyncio.run(svc._poll_groups_once())

    # 1. Opened by hand on 1 — group 1 goes LONG, group 2 goes SHORT.
    holdings[1] = [snapshot(side=LONG)]
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1 and copied[0][0] == 1

    # 2. The copies land.
    holdings[2] = [snapshot(side=LONG)]
    holdings[3] = [snapshot(side=SHORT)]
    holdings[4] = [snapshot(side=SHORT)]
    asyncio.run(svc._poll_groups_once())
    assert len(copied) == 1, "the copies must not be read as four more people trading"
    assert svc.account_status == {1: True, 2: True, 3: True, 4: True}
    assert svc.account_sides == {1: LONG, 2: LONG, 3: SHORT, 4: SHORT}

    # 3. One account of leg 2 is closed by hand; its light goes out on its own.
    holdings[4] = []
    asyncio.run(svc._poll_groups_once())
    assert svc.account_status[4] is False
    assert len(copied) == 1, "a close is never copied"

    # 4. Leg 1 is closed with the button. Only what is holding is touched.
    class Pos:
        def __init__(self, symbol, vol, pid):
            self.symbol, self.hold_vol, self.position_id = symbol, vol, pid

    class Settled:
        def __init__(self, pid, realised):
            self.position_id, self.realised = pid, realised

    class Client:
        def __init__(self, positions, settled):
            self.positions, self.settled, self.closed = positions, settled, []

        async def get_open_positions(self, symbol=None):
            return list(self.positions)

        async def get_closed_positions(self, symbol=None, *, page_size=50):
            return list(self.settled)

        async def close_all(self, symbol=None):
            self.closed.append(symbol)

    clients = iter([
        Client([Pos("BTC_USDT", 10.0, 11)], [Settled(11, 3.5)]),
        Client([Pos("BTC_USDT", 10.0, 22)], [Settled(22, -1.25)]),
    ])
    original = service_module.MexcRestClient
    service_module.MexcRestClient = lambda *a, **k: next(clients)
    try:
        closed, failed, skipped = asyncio.run(svc.close_accounts([1, 2]))
    finally:
        service_module.MexcRestClient = original

    assert sorted(closed) == [("acc 1", 3.5), ("acc 2", -1.25)]
    assert not failed and not skipped
    assert svc.account_status[1] is False and svc.account_status[2] is False
    # Leg 2 was not named, so it was not touched.
    assert svc.account_status[3] is True
