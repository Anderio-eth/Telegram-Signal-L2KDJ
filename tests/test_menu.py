"""Tests for the main menu text.

It is the screen you read before pressing START, so what matters is that it stays honest when
some accounts fail to report: a missing balance must show as a missing balance, never as a zero,
and never by taking the rest of the menu down with it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.db.store import FOLLOWER, MASTER, Account  # noqa: E402
from mexc_copy_bot.telegram.messages import Balance, main_menu  # noqa: E402


def account(account_id: int, kind: str, label: str, hint: str, active: bool = True) -> Account:
    return Account(
        id=account_id, owner_id=1, label=label, kind=kind, api_key_hint=hint,
        size_multiplier=1.0, active=active, position_mode=1, last_error=None,
    )


MASTER_ACC = account(1, MASTER, "Master", "BbB0")
F1 = account(2, FOLLOWER, "Follower #1", "AA11")
F2 = account(3, FOLLOWER, "Follower #2", "BB22")


def render(**kwargs) -> str:
    base = dict(running=False, master=MASTER_ACC, followers=[], max_followers=9, master_connected=False)
    return main_menu(**{**base, **kwargs})


def test_every_follower_shows_its_own_balance():
    text = render(
        followers=[F1, F2],
        balances={
            1: Balance(equity=2.10, available=2.10),
            2: Balance(equity=50.0, available=48.0),
            3: Balance(equity=17.5, available=17.5),
        },
    )
    assert "$2.10" in text
    assert "$50.00 (вільно $48.00)" in text
    assert "$17.50" in text


def test_one_broken_follower_does_not_hide_the_others():
    text = render(
        followers=[F1, F2],
        balances={
            1: Balance(equity=2.10, available=2.10),
            2: Balance(error="invalid api key"),
            3: Balance(equity=17.5, available=17.5),
        },
    )
    assert "invalid api key" in text
    assert "$17.50" in text
    assert "Follower #1" in text and "Follower #2" in text


def test_total_excludes_accounts_that_did_not_report_and_says_so():
    """A total that quietly skips a failed account reads as the real figure. It must not."""
    text = render(
        followers=[F1, F2],
        balances={2: Balance(equity=50.0, available=50.0), 3: Balance(error="timeout")},
    )
    assert "Total" not in text  # only one follower reported; a "total" would just repeat it

    text = render(
        followers=[F1, F2, account(4, FOLLOWER, "Follower #3", "CC33")],
        balances={
            2: Balance(equity=50.0, available=50.0),
            3: Balance(equity=20.0, available=20.0),
            4: Balance(error="timeout"),
        },
    )
    assert "$70.00" in text
    assert "відповіли 2/3" in text


def test_total_is_clean_when_everyone_reports():
    text = render(
        followers=[F1, F2],
        balances={2: Balance(equity=50.0, available=50.0), 3: Balance(equity=20.0, available=20.0)},
    )
    assert "<b>Разом:</b> $70.00" in text
    assert "відповіли" not in text


def test_missing_balance_is_not_rendered_as_zero():
    text = render(followers=[F1], balances={})
    assert "$0" not in text


def test_paused_follower_is_marked():
    text = render(followers=[account(2, FOLLOWER, "Follower #1", "AA11", active=False)], balances={})
    assert "(на паузі)" in text


def test_menu_without_a_master_still_lists_followers():
    text = render(master=None, followers=[F1], balances={2: Balance(equity=9.0, available=9.0)})
    assert "НЕМАЄ MASTER" in text
    assert "Follower #1" in text and "$9.00" in text
