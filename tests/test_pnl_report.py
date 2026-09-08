"""Tests for realised-PnL reporting.

The rule these encode: a number shown next to money must be one the exchange actually confirmed.
An unread settlement is "pending", never zero, and a total that covers only some of the accounts
says so instead of presenting itself as the whole figure.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core.copy_engine import FollowerResult  # noqa: E402
from mexc_copy_bot.core.events import Action, MasterEvent  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, Account  # noqa: E402
from mexc_copy_bot.telegram.messages import event_report, history, signed_money  # noqa: E402


def follower(n: int) -> Account:
    return Account(
        id=n, owner_id=1, label=f"Follower #{n}", kind=FOLLOWER, api_key_hint=f"K{n:03d}",
        size_multiplier=1.0, active=True, position_mode=1, last_error=None,
    )


def close_event() -> MasterEvent:
    return MasterEvent(
        symbol="BTC_USDT", position_type=1, action=Action.CLOSE, master_vol=0.0,
        delta_vol=12.0, leverage=20, open_type=2, dedupe_key="k",
    )


def closed(n: int, pnl: float | None) -> FollowerResult:
    return FollowerResult(follower(n), True, Action.CLOSE, 12.0, None, pnl)


def test_each_follower_shows_its_own_realised_pnl():
    text = event_report(close_event(), [closed(1, -0.1879), closed(2, -0.1879), closed(3, 2.42)], None)
    assert "Follower #1 — ЗАКРИТО  −$0.1879" in text
    assert "Follower #3 — ЗАКРИТО  +$2.42" in text


def test_total_is_the_sum_and_carries_its_sign():
    text = event_report(close_event(), [closed(1, -0.50), closed(2, 2.00)], None)
    assert "Загальний PnL: +$1.50" in text

    text = event_report(close_event(), [closed(1, -0.50), closed(2, -2.00)], None)
    assert "Загальний PnL: −$2.50" in text


def test_unknown_settlement_is_pending_not_zero():
    """A close whose settlement could not be read must not read as "broke even"."""
    text = event_report(close_event(), [closed(1, None)], None)
    assert "(PnL рахується)" in text
    assert "$0" not in text
    assert "Загальний PnL" not in text


def test_partial_total_admits_it_is_partial():
    text = event_report(close_event(), [closed(1, -1.0), closed(2, None), closed(3, -1.0)], None)
    assert "Загальний PnL: −$2.00" in text
    assert "(порахували 2/3)" in text


def test_a_failed_follower_is_not_counted_into_pnl():
    results = [closed(1, -1.0), FollowerResult(follower(2), False, Action.CLOSE, 12.0, "insufficient balance")]
    text = event_report(close_event(), results, None)
    assert "Загальний PnL: −$1.00" in text
    assert "Успішно: 1/2" in text


def test_opens_report_no_pnl_at_all():
    event = MasterEvent(
        symbol="BTC_USDT", position_type=1, action=Action.OPEN, master_vol=12.0,
        delta_vol=12.0, leverage=20, open_type=2, dedupe_key="k",
    )
    text = event_report(event, [FollowerResult(follower(1), True, Action.OPEN, 12.0)], 1000.0)
    assert "PnL" not in text


def test_history_shows_pnl_for_closes():
    rows = [
        {"symbol": "BTC_USDT", "position_type": 1, "action": "CLOSE", "ok": 3, "failed": 0,
         "pnl": -0.5637, "pnl_count": 3, "observed_at": datetime(2026, 9, 7, 1, 10)},
        {"symbol": "ETH_USDT", "position_type": 2, "action": "OPEN", "ok": 3, "failed": 0,
         "pnl": None, "pnl_count": 0, "observed_at": datetime(2026, 9, 7, 1, 5)},
    ]
    text = history(rows)
    assert "−$0.5637" in text
    assert text.count("$") == 1  # the open carries no figure


def test_history_flags_a_partly_reported_close():
    rows = [{"symbol": "BTC_USDT", "position_type": 1, "action": "CLOSE", "ok": 3, "failed": 0,
             "pnl": -1.0, "pnl_count": 2, "observed_at": datetime(2026, 9, 7, 1, 10)}]
    assert "(2/3)" in history(rows)


def test_signed_money_never_hides_a_loss():
    assert signed_money(-0.05).startswith("−")
    assert signed_money(0.0).startswith("+")
    assert signed_money(12.5) == "+$12.50"
