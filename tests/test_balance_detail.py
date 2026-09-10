"""A refused order must say which money it was measured against.

"Balance insufficient" on its own says nothing — not how much was needed, not how much was there,
and not which of MEXC's several balance figures it was compared with. There are several, and they
are not interchangeable:

    equity            the total
    availableBalance  the wallet figure a screen shows
    availableOpen     what the venue lets a NEW position be opened against
    bonus             credit that counts towards the first and not the last

A menu showing availableBalance while orders are refused against availableOpen reads as "there is
plenty of money, why will it not open" — and that is exactly how an afternoon went, arguing about
leverage and fees while the numbers were one request away.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.copy_engine import CopyEngine, _is_short_of_money  # noqa: E402
from mexc_copy_bot.core.events import Action, MasterEvent  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, Account  # noqa: E402
from mexc_copy_bot.mexc.rest import AccountBalance, MexcError  # noqa: E402
from mexc_copy_bot.telegram.messages import Balance  # noqa: E402


def follower(multiplier: float = 1.0) -> Account:
    return Account(
        id=28, owner_id=1, label="Follower #1", kind=FOLLOWER, api_key_hint="k",
        size_multiplier=multiplier, active=True, position_mode=1, last_error=None,
    )


def event() -> MasterEvent:
    """The SILVER open as the venue reported it: $150,022 of position on $156.44 of margin."""
    return MasterEvent(
        action=Action.OPEN, symbol="SILVER_USDT", position_type=1, master_vol=230343.0,
        delta_vol=230343.0, leverage=959, open_type=1, dedupe_key="k",
        raw={"im": 156.435853324725, "oim": 156.435853324725, "holdVol": 230343,
             "openAvgPrice": 65.13, "leverage": 959},
    )


class Client:
    def __init__(self, balance):
        self.balance = balance

    async def get_usdt_snapshot(self):
        if isinstance(self.balance, Exception):
            raise self.balance
        return self.balance


def detail(balance, multiplier: float = 1.0) -> str:
    engine = CopyEngine(store=None, session=None, retry_attempts=1)
    return asyncio.run(
        engine._money_detail(Client(balance), event(), follower(multiplier), 230343.0,
                             "Balance insufficient")
    )


# ── the message ────────────────────────────────────────────────────────────
def test_the_message_carries_what_was_needed_and_what_was_there():
    text = detail(AccountBalance(equity=301.12, available=300.98, available_open=18.40, bonus=282.58))
    assert "Balance insufficient" in text
    assert "156.44" in text, "the requirement must be stated"
    assert "18.40" in text, "so must what could actually be used"
    assert "301.12" in text


def test_bonus_credit_is_named_when_it_is_the_reason():
    """The trap: the wallet reads full, but the venue will not open a position against bonus."""
    text = detail(AccountBalance(equity=301.12, available=300.98, available_open=18.40, bonus=282.58))
    assert "бонус" in text


def test_no_bonus_line_when_bonus_is_not_the_problem():
    text = detail(AccountBalance(equity=20.0, available=20.0, available_open=20.0))
    assert "бонус" not in text


def test_the_requirement_scales_with_the_followers_multiplier():
    """Half the master's size needs half the margin, and the message must say so."""
    text = detail(AccountBalance(equity=10.0, available=10.0, available_open=10.0), multiplier=0.5)
    assert "78.22" in text


def test_a_balance_that_cannot_be_read_leaves_the_original_message():
    """Better the venue's bare wording than an invented number."""
    text = detail(MexcError(510, "Requests are too frequent", endpoint="/x"))
    assert text == "Balance insufficient"


# ── recognising the failure ────────────────────────────────────────────────
def test_the_venues_wording_is_recognised():
    assert _is_short_of_money(MexcError(None, "Balance insufficient", endpoint="/x"))
    assert _is_short_of_money(MexcError(None, "Insufficient position", endpoint="/x"))
    assert not _is_short_of_money(MexcError(None, "Contract not activated", endpoint="/x"))


# ── the menu ───────────────────────────────────────────────────────────────
def test_the_menu_shows_what_can_be_opened_against():
    line = Balance(equity=301.12, available=18.40, wallet=300.98).line("uk")
    assert "18.40" in line
    assert "не йде під позицію" in line


def test_the_menu_stays_quiet_when_every_figure_agrees():
    """Two identical numbers on nine accounts would bury the one case that matters."""
    line = Balance(equity=300.0, available=300.0, wallet=300.0).line("uk")
    assert "не йде під позицію" not in line


def test_a_wallet_with_no_openable_figure_is_not_reported_as_broke():
    """Some accounts omit availableOpen entirely; treating that as zero would call them all empty."""
    assert AccountBalance(equity=50.0, available=50.0).openable == 50.0


def test_openable_prefers_the_figure_the_venue_opens_against():
    """The whole point. Returning the wallet figure here is the bug this file exists for: the
    menu would show $300 while the venue refuses anything over $18."""
    balance = AccountBalance(equity=301.12, available=300.98, available_open=18.40, bonus=282.58)
    assert balance.openable == 18.40
    assert "openable $18.40" in balance.detail()
