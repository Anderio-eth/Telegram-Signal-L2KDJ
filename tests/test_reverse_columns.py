"""The REVERSE screen: two legs, one cell per account, and a button that closes one leg.

The screen answers one question at a glance — which accounts are in, and with how much. So each
cell carries a name saying which way it trades, a balance, and a light; and the legs sit side by
side rather than in a list, because a keyboard aligns its columns and a message does not.

The dangerous part is the Close all under each leg. On MEXC an order on the opposite side of a
flat account OPENS a position rather than closing one, so a blanket close sent to an account
somebody had already closed by hand would put it straight back in, facing the other way. Every
test here about skipping flat accounts is about that, not about tidiness.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core import service as service_module  # noqa: E402
from mexc_copy_bot.core.service import CopyService  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, MASTER, Account  # noqa: E402
from mexc_copy_bot.mexc.rest import MexcError  # noqa: E402
from mexc_copy_bot.telegram.bot import _column_rows  # noqa: E402
from mexc_copy_bot.telegram.messages import Balance, compact_money, reverse_columns  # noqa: E402


def account(account_id: int, kind: str = FOLLOWER, direction: str = "COPY") -> Account:
    return Account(
        id=account_id, owner_id=1, label="Follower " + str(account_id), kind=kind,
        api_key_hint="k", size_multiplier=1.0, active=True, position_mode=1, last_error=None,
        direction=direction,
    )


MASTER_ACC = account(9, MASTER)


# ── how the legs are split ─────────────────────────────────────────────────
def test_the_master_sits_with_the_accounts_trading_its_way():
    """It is listed there rather than above because after the entry that is exactly what it is:
    its own exit moves no other account."""
    left, right = reverse_columns(MASTER_ACC, [account(1), account(2, direction="REVERSE")], {}, {})
    assert [c.account_id for c in left] == [9, 1]
    assert [c.account_id for c in right] == [2]


def test_the_master_is_named_master_and_takes_no_followers_number():
    left, _ = reverse_columns(MASTER_ACC, [account(1), account(2)], {}, {})
    assert [c.label for c in left] == ["Master", "as master 1", "as master 2"]


def test_each_leg_numbers_from_one():
    left, right = reverse_columns(
        MASTER_ACC,
        [account(1), account(2, direction="REVERSE"), account(3, direction="REVERSE"), account(4)],
        {}, {},
    )
    assert [c.label for c in left] == ["Master", "as master 1", "as master 2"]
    assert [c.label for c in right] == ["opposite 1", "opposite 2"]


def test_a_folder_with_no_master_still_shows_its_legs():
    left, right = reverse_columns(None, [account(1), account(2, direction="REVERSE")], {}, {})
    assert [c.label for c in left] == ["as master 1"]
    assert [c.label for c in right] == ["opposite 1"]


# ── what a cell says ───────────────────────────────────────────────────────
def test_a_cell_carries_the_name_the_balance_and_the_light():
    left, _ = reverse_columns(None, [account(1)], {1: Balance(equity=120.0, available=100.0)}, {1: True})
    assert left[0].text() == "as master 1  $100  🟢"


def test_an_account_with_nothing_open_shows_red():
    left, _ = reverse_columns(None, [account(1)], {1: Balance(equity=1.0, available=1.0)}, {})
    assert left[0].text().endswith("🔴")


def test_the_balance_is_the_one_a_position_can_be_opened_against():
    """Equity includes what is already committed; this screen is about what is left."""
    left, _ = reverse_columns(None, [account(1)], {1: Balance(equity=300.0, available=50.0)}, {})
    assert "$50" in left[0].text()
    assert "$300" not in left[0].text()


def test_an_unreadable_balance_is_a_dash_not_a_zero():
    """A zero would read as "this account is empty", which is a different problem entirely."""
    left, _ = reverse_columns(None, [account(1)], {1: Balance(error="no keys")}, {})
    assert "—" in left[0].text()


def test_cents_are_dropped_above_a_dollar_and_kept_below_it():
    assert compact_money(1247.38) == "$1,247"
    assert compact_money(0.42) == "$0.42"
    assert compact_money(0.0) == "$0", "an empty wallet reads better as $0 than $0.00"
    assert compact_money(None) == "—"


# ── the keyboard ───────────────────────────────────────────────────────────
def rows(left, right):
    return _column_rows(left, right, "uk")


def test_the_shorter_leg_is_padded_so_cells_stay_in_their_column():
    """Without padding Telegram slides the longer leg's remaining cells across, and an account
    trading one way appears under the heading for the other."""
    left, right = reverse_columns(
        MASTER_ACC, [account(1), account(2), account(3, direction="REVERSE")], {}, {}
    )
    cells = rows(left, right)[:-1]
    assert all(len(row) == 2 for row in cells)
    assert cells[2][1].text.strip() == "", "the right leg ran out; it must be blank, not borrowed"


def test_each_leg_gets_its_own_close_button():
    left, right = reverse_columns(MASTER_ACC, [account(1), account(2, direction="REVERSE")], {}, {})
    last = rows(left, right)[-1]
    assert last[0].callback_data == "closecol:master"
    assert last[1].callback_data == "closecol:opposite"


def test_cells_do_nothing_when_pressed():
    left, right = reverse_columns(MASTER_ACC, [account(1, direction="REVERSE")], {}, {})
    for row in rows(left, right)[:-1]:
        for button in row:
            assert button.callback_data == "noop"


# ── closing one leg ────────────────────────────────────────────────────────
class Position:
    def __init__(self, symbol, hold_vol):
        self.symbol, self.hold_vol = symbol, hold_vol


class Client:
    def __init__(self, positions, fail=None):
        self.positions = positions
        self.fail = fail
        self.closed = []

    async def get_open_positions(self, symbol=None):
        if isinstance(self.positions, Exception):
            raise self.positions
        return list(self.positions)

    async def close_all(self, symbol=None):
        if self.fail:
            raise self.fail
        self.closed.append(symbol)


class Store:
    def __init__(self, accounts):
        self.accounts = accounts

    async def get_master(self, folder_id):
        return next((a for a in self.accounts if a.kind == MASTER), None)

    async def list_accounts(self, folder_id, kind=None):
        return [a for a in self.accounts if a.kind == FOLLOWER]

    async def get_credentials(self, account_id, owner_id):
        return ("k", "s")


def run_close(accounts, clients, ids):
    """Drive close_accounts with a stub client per account, in call order."""
    svc = CopyService(Store(accounts), 1, 1)
    svc._session = object()
    handed = []

    def factory(key, secret, *, session):
        handed.append(len(handed))
        return clients[handed[-1]]

    original = service_module.MexcRestClient
    service_module.MexcRestClient = factory
    try:
        return asyncio.run(svc.close_accounts(ids)), svc
    finally:
        service_module.MexcRestClient = original


def test_an_account_already_flat_is_never_sent_an_order():
    """The whole safety of the button: a close sent to a flat account opens the opposite side."""
    client = Client(positions=[])
    (closed, failed, skipped), _ = run_close([account(1)], [client], [1])
    assert closed == []
    assert failed == []
    assert skipped == ["Follower 1"]
    assert client.closed == [], "nothing may be sent to an account holding nothing"


def test_an_account_holding_something_is_closed():
    client = Client(positions=[Position("SILVER_USDT", 100.0)])
    (closed, failed, skipped), _ = run_close([account(1)], [client], [1])
    assert closed == ["Follower 1"]
    assert not failed and not skipped
    assert client.closed == ["SILVER_USDT"]


def test_a_position_that_cannot_be_read_is_reported_not_assumed_flat():
    """Unknown is not "already closed". Calling it skipped would leave a live position with
    nobody told about it."""
    client = Client(positions=MexcError(510, "Requests are too frequent", endpoint="/x"))
    (closed, failed, skipped), _ = run_close([account(1)], [client], [1])
    assert not closed and not skipped
    assert failed and failed[0][0] == "Follower 1"


def test_the_light_goes_out_for_everything_that_was_closed():
    client = Client(positions=[Position("SILVER_USDT", 100.0)])
    _, svc = run_close([account(1)], [client], [1])
    assert svc.account_status[1] is False


def test_only_the_named_accounts_are_touched():
    """Close all under one leg must not reach into the other."""
    client = Client(positions=[Position("SILVER_USDT", 100.0)])
    (closed, _, _), _ = run_close([account(1), account(2, direction="REVERSE")], [client], [1])
    assert closed == ["Follower 1"]
