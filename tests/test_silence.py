"""A master action that reaches no account must still be reported.

The trade that prompted this: the master opened SILVER, the follower's order was refused three
times, and the follower stayed flat. When the master then closed, the follower had nothing to
close, so the close applied to nobody — and the code returned without a word. From the outside,
a bot that had failed a trade and a bot that had stopped running looked identical.

Silence is only ever right when nothing happened. Here something did.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402

from mexc_copy_bot.core.events import Action, MasterEvent  # noqa: E402
from mexc_copy_bot.core.service import CopyService  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MODE_COPY, MODE_REVERSE, Account  # noqa: E402

OWNER, FOLDER = 7, 3


def follower(n: int, active: bool = True) -> Account:
    return Account(
        id=n, owner_id=OWNER, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=1.0, active=active, position_mode=1, last_error=None, direction="REVERSE",
    )


class FakeStore:
    def __init__(self, accounts, detached=(), mode=MODE_COPY):
        self._accounts = accounts
        self._detached = set(detached)
        self._mode = mode
        self.events = []

    async def list_accounts(self, folder_id, kind=None):
        return list(self._accounts)

    async def detached_account_ids(self, owner_id):
        return set(self._detached)

    async def get_mode(self, folder_id):
        return (self._mode, None)

    async def record_event(self, **kwargs):
        self.events.append(kwargs)
        return 1

    async def get_positions(self, account_id):
        return {}


def event(action=Action.CLOSE):
    return MasterEvent(
        dedupe_key="SILVER_USDT:1:v3", symbol="SILVER_USDT", position_type=1, action=action,
        master_vol=0.0, delta_vol=3608.0, leverage=20, open_type=1, raw={},
    )


def run(store, held, action=Action.CLOSE):
    """Apply one master event and collect whatever was sent to Telegram."""
    sent: list[str] = []
    svc = CopyService(store, FOLDER, OWNER)
    svc.on_notice = lambda text: _record(sent, text)

    async def fake_held(account, symbol):
        return held.get(account.id, 0.0)

    svc._held_any = fake_held
    asyncio.run(svc._dispatch(event(action), {}))
    return sent


async def _record(sink, text):
    sink.append(text)


def test_a_close_that_reaches_nobody_is_reported():
    """The exact case: the follower never got in, so there is nothing to get out of."""
    store = FakeStore([follower(1)])
    sent = run(store, held={})
    assert sent, "the master closed and not one account acted, and nothing was said"
    assert "SILVER_USDT" in sent[0]
    assert "нема відкритої позиції" in sent[0]


def test_the_message_names_each_account_and_why_it_sat_out():
    store = FakeStore([follower(1), follower(2, active=False), follower(3)], detached={3})
    sent = run(store, held={})
    assert "Follower #1" in sent[0] and "нема відкритої позиції" in sent[0]
    assert "Follower #2" in sent[0] and "на паузі" in sent[0]
    assert "Follower #3" in sent[0] and "відчеплений" in sent[0]


def test_a_folder_with_no_followers_still_says_so():
    sent = run(FakeStore([]), held={})
    assert sent and "нема жодного фоловера" in sent[0]


def test_nothing_extra_is_said_when_accounts_do_act():
    """The warning must not fire on an ordinary trade; that would train everyone to ignore it."""
    store = FakeStore([follower(1)])
    svc = CopyService(store, FOLDER, OWNER)
    sent: list[str] = []
    svc.on_notice = lambda text: _record(sent, text)

    async def fake_held(account, symbol):
        return 20.0

    svc._held_any = fake_held
    with pytest.raises(AssertionError):
        # Reaching the engine needs a live session, which this test deliberately does not build:
        # getting that far is itself the proof that the "nobody acted" branch was not taken.
        asyncio.run(svc._dispatch(event(), {}))
    assert sent == []


# ── reverse: the master's exit is not the hedge's exit ──────────────────────
def test_reverse_does_not_mirror_the_masters_close():
    """A hedge is held AGAINST the master's position. Following the master out would close the
    very thing that was protecting it, so the accounts stay in and are closed by hand."""
    store = FakeStore([follower(1)], mode=MODE_REVERSE)
    sent = run(store, held={1: 20.0}, action=Action.CLOSE)
    assert sent, "the master left and nothing was said"
    assert "НЕ КОПІЮЄТЬСЯ" in sent[0]
    assert "SILVER_USDT" in sent[0]


def test_the_reverse_skip_says_the_positions_are_still_open():
    """After this the accounts hold something the master does not, which is the whole point of
    being told about it."""
    sent = run(FakeStore([follower(1)], mode=MODE_REVERSE), held={1: 20.0}, action=Action.CLOSE)
    assert "лишаються відкритими" in sent[0]


def test_reverse_still_mirrors_an_opening():
    """Only the exit is dropped. An entry is the whole reason the mode exists, and reaching the
    engine — which needs a session this test does not build — proves it was not skipped."""
    import pytest as _pytest

    store = FakeStore([follower(1)], mode=MODE_REVERSE)
    svc = CopyService(store, FOLDER, OWNER)
    sent: list[str] = []
    svc.on_notice = lambda text: _record(sent, text)

    async def flat(account, symbol):
        return 0.0

    svc._held_any = flat
    with _pytest.raises(AssertionError):
        asyncio.run(svc._dispatch(event(Action.OPEN), {}))
    assert sent == [], "an entry must not be reported as skipped"


def test_copy_mode_still_mirrors_a_close():
    """The rule is about REVERSE only; a copy folder must keep following the master out."""
    import pytest as _pytest

    store = FakeStore([follower(1)], mode=MODE_COPY)
    svc = CopyService(store, FOLDER, OWNER)
    sent: list[str] = []
    svc.on_notice = lambda text: _record(sent, text)

    async def holding(account, symbol):
        return 20.0

    svc._held_any = holding
    with _pytest.raises(AssertionError):
        asyncio.run(svc._dispatch(event(Action.CLOSE), {}))
    assert sent == [], "a copy folder's close must reach the engine, not be skipped"
