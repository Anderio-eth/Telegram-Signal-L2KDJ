"""Tests for accounts that fall out of step with the master.

Two things carry the money here.

A stranded account must stop hearing the master entirely. It holds something the master does not,
or lacks something the master has; any instruction aimed at an account in the master's state lands
somewhere else entirely.

And an account that has been sorted out must not dive into the position the master is already in.
The entry price is long gone — it would be joining a trade half way through, at a price nobody
chose. It waits for the next one, which by definition starts with both of them flat.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402

from mexc_copy_bot.core.events import Action  # noqa: E402
from mexc_copy_bot.core.service import CopyService  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MODE_COPY, Account  # noqa: E402

OWNER = 7


def follower(n: int) -> Account:
    return Account(
        id=n, owner_id=OWNER, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
    )


class FakeStore:
    def __init__(self, detached=(), accounts=(1, 2, 3)):
        self._detached = set(detached)
        self._accounts = [follower(n) for n in accounts]

    async def list_accounts(self, owner_id, kind=None):
        return list(self._accounts)

    async def detached_account_ids(self, owner_id):
        return set(self._detached)

    async def get_mode(self, owner_id):
        return (MODE_COPY, None)

    async def get_credentials(self, account_id, owner_id):
        return ("k", "s")


def service(monkeypatch, store, held: dict[int, float]):
    svc = CopyService(store, OWNER)

    async def fake_held(follower_account, symbol):
        return held.get(follower_account.id, 0.0)

    monkeypatch.setattr(svc, "_held_any", fake_held)
    return svc


def ids(accounts):
    return sorted(a.id for a in accounts)


# ── detachment ─────────────────────────────────────────────────────────────
def test_a_detached_account_hears_nothing(monkeypatch):
    svc = service(monkeypatch, FakeStore(detached={2}), held={})
    assert ids(asyncio.run(svc._eligible_followers())) == [1, 3]


def test_detachment_applies_to_every_symbol(monkeypatch):
    """It is stuck on one token, but its balance and margin are not. Until it is sorted out it
    takes no instruction at all."""
    svc = service(monkeypatch, FakeStore(detached={2}), held={})
    got = asyncio.run(svc._eligible_followers(opening=True, symbol="SOL_USDT"))
    assert ids(got) == [1, 3]


def test_nothing_detached_means_everyone(monkeypatch):
    svc = service(monkeypatch, FakeStore(), held={})
    assert ids(asyncio.run(svc._eligible_followers())) == [1, 2, 3]


# ── staying in step ────────────────────────────────────────────────────────
def test_an_account_already_holding_does_not_join_a_new_entry(monkeypatch):
    """The freed-account case: the master opened while it was stranded, so it is flat and the
    master is not. It must wait for the next entry rather than buy in at a different price."""
    svc = service(monkeypatch, FakeStore(), held={2: 20.0})
    got = asyncio.run(svc._eligible_followers(opening=True, symbol="BTC_USDT"))
    assert ids(got) == [1, 3]


def test_an_empty_account_is_not_asked_to_close(monkeypatch):
    svc = service(monkeypatch, FakeStore(), held={1: 20.0, 3: 20.0})
    got = asyncio.run(svc._eligible_followers(opening=False, symbol="BTC_USDT"))
    assert ids(got) == [1, 3]


def test_everyone_flat_opens_together(monkeypatch):
    svc = service(monkeypatch, FakeStore(), held={})
    got = asyncio.run(svc._eligible_followers(opening=True, symbol="BTC_USDT"))
    assert ids(got) == [1, 2, 3]


def test_everyone_holding_closes_together(monkeypatch):
    svc = service(monkeypatch, FakeStore(), held={1: 20.0, 2: 20.0, 3: 20.0})
    got = asyncio.run(svc._eligible_followers(opening=False, symbol="BTC_USDT"))
    assert ids(got) == [1, 2, 3]


def test_detached_and_out_of_step_are_both_excluded(monkeypatch):
    """#2 is stranded, #3 is holding something of its own. Only #1 is in the master's state."""
    svc = service(monkeypatch, FakeStore(detached={2}), held={3: 20.0})
    got = asyncio.run(svc._eligible_followers(opening=True, symbol="BTC_USDT"))
    assert ids(got) == [1]


def test_without_a_symbol_only_detachment_filters(monkeypatch):
    """Some callers have no symbol in hand; they must still get the detachment filter."""
    svc = service(monkeypatch, FakeStore(detached={1}), held={2: 5.0})
    assert ids(asyncio.run(svc._eligible_followers())) == [2, 3]
