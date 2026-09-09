"""What the bot does when the exchange will not say what an account holds.

MEXC refuses requests that arrive too fast (code 510). Measured against a live account, private
endpoints start refusing somewhere above ~12 requests a second from one IP — and the pacing had
been set to 16.6/s, so refusals were routine rather than exceptional.

The refusal itself was survivable. What was not: every position read answered it with 0.0, the
same value that means "this account is flat". A close then skipped the account for having nothing
to close, and a real position was left open with nothing watching it.

So an unreadable position is now its own answer, and the two directions are decided by what being
wrong would cost rather than by a shared default.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.service import CopyService  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MODE_COPY, Account  # noqa: E402
from mexc_copy_bot.mexc.rest import MexcError  # noqa: E402

OWNER, FOLDER = 7, 3


def follower(n: int) -> Account:
    return Account(
        id=n, owner_id=OWNER, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
    )


class FakeStore:
    def __init__(self, accounts=(1, 2, 3)):
        self._accounts = [follower(n) for n in accounts]

    async def list_accounts(self, folder_id, kind=None):
        return list(self._accounts)

    async def detached_account_ids(self, owner_id):
        return set()

    async def get_mode(self, folder_id):
        return (MODE_COPY, None)


def service(monkeypatch, held: dict[int, float | None]):
    """`held` maps account id -> what the venue reported; None means it would not say."""
    svc = CopyService(FakeStore(), FOLDER, OWNER)

    async def fake_held(account, symbol):
        return held.get(account.id, 0.0)

    monkeypatch.setattr(svc, "_held_any", fake_held)
    return svc


def ids(accounts):
    return sorted(a.id for a in accounts)


def test_a_close_still_reaches_an_account_whose_position_could_not_be_read(monkeypatch):
    """The bug that cost a live position: #2 was holding, the read was refused, and the close
    passed it by. Closing an account that turns out to be flat is a no-op; not closing one that
    was holding is a position nobody is watching."""
    svc = service(monkeypatch, held={1: 20.0, 2: None, 3: 20.0})
    got = asyncio.run(svc._eligible_followers(opening=False, symbol="SILVER_USDT"))
    assert ids(got) == [1, 2, 3]


def test_an_open_leaves_out_an_account_whose_position_could_not_be_read(monkeypatch):
    """The other direction is not symmetric. Opening an account that already holds something
    doubles the position, and nothing undoes that."""
    svc = service(monkeypatch, held={1: 0.0, 2: None, 3: 0.0})
    got = asyncio.run(svc._eligible_followers(opening=True, symbol="SILVER_USDT"))
    assert ids(got) == [1, 3]


def test_a_readable_zero_is_still_treated_as_flat(monkeypatch):
    """Unknown and zero must stay distinct; a genuine zero keeps behaving exactly as before."""
    svc = service(monkeypatch, held={1: 0.0, 2: 0.0, 3: 0.0})
    assert ids(asyncio.run(svc._eligible_followers(opening=True, symbol="SILVER_USDT"))) == [1, 2, 3]
    assert asyncio.run(svc._eligible_followers(opening=False, symbol="SILVER_USDT")) == []


# ── recognising the refusal ────────────────────────────────────────────────
def test_the_venues_rate_limit_code_is_recognised():
    """Matching on wording alone missed a refusal reported as a bare code, and a rate limit read
    as a permanent error is an account dropped from a trade for no reason."""
    assert MexcError(510, "Requests are too frequent, please try again later", endpoint="/x").is_rate_limited
    assert MexcError(510, "", endpoint="/x").is_rate_limited
    assert MexcError(429, "Too Many Requests", endpoint="/x").is_rate_limited
    assert MexcError(None, "rate limit exceeded", endpoint="/x").is_rate_limited


def test_an_ordinary_failure_is_not_mistaken_for_a_rate_limit():
    assert not MexcError(600, "Insufficient balance", endpoint="/x").is_rate_limited
    assert not MexcError(None, "Contract not activated", endpoint="/x").is_rate_limited
