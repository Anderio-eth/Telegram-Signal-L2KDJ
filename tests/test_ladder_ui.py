"""The scheduled-entry form's pure logic: default draft, field parsing, and time parsing.

The screens themselves are thin wrappers over these plus the store and _send, which the topic tests
already exercise; what is worth pinning here is that a typed value becomes the right field or is
rejected, and that a time in any accepted shape lands in the future.
"""

from __future__ import annotations

import time

import pytest

from mexc_copy_bot.telegram.bot import CopyBot

apply = CopyBot._apply_ladder_field
parse = CopyBot._parse_target
bracket = CopyBot._parse_bracket


def test_the_default_symbol_matches_the_exchange():
    # Silver (XAG_USDT) exists on HIBT but not MEXC, so the prefilled ticker must follow the folder's
    # exchange — otherwise a MEXC draft opens on a contract the venue doesn't list.
    assert CopyBot._new_draft("hibt")["symbol"] == "XAG_USDT"
    assert CopyBot._new_draft("mexc")["symbol"] == "BTC_USDT"
    assert CopyBot._new_draft()["symbol"] == "BTC_USDT"   # default exchange is MEXC


def test_the_default_draft_shape():
    d = CopyBot._new_draft("hibt")
    assert d["leverage"] == 1000 and d["parts"] == 5
    assert d["margin_usd"] is None and d["target"] is None  # the two that must be filled in


def test_each_field_parses_into_the_draft():
    d = CopyBot._new_draft()
    assert apply(d, "symbol", "xag-usdt") and d["symbol"] == "XAG_USDT"
    assert apply(d, "leverage", "500") and d["leverage"] == 500
    assert apply(d, "parts", "8") and d["parts"] == 8
    assert apply(d, "margin", "150") and d["margin_usd"] == 150.0
    assert apply(d, "margin", "1,5") and d["margin_usd"] == 1.5      # comma decimal
    assert apply(d, "step", "0.5") and d["step_seconds"] == 0.5


def test_bad_values_are_rejected_and_leave_the_draft_untouched():
    d = CopyBot._new_draft()
    d["margin_usd"] = 10.0
    assert not apply(d, "margin", "abc") and d["margin_usd"] == 10.0
    assert not apply(d, "margin", "-5") and d["margin_usd"] == 10.0    # non-positive
    assert not apply(d, "leverage", "1.5x") and d["leverage"] == 1000
    assert not apply(d, "target", "not a time")


def test_size_is_margin_times_leverage_from_the_form():
    d = CopyBot._new_draft()
    apply(d, "leverage", "1000")
    apply(d, "margin", "150")
    assert d["margin_usd"] * d["leverage"] == 150_000


def test_stop_and_take_parse_as_percent_or_dollars():
    d = CopyBot._new_draft()
    assert apply(d, "sl", "2%") and d["sl"].kind == "percent" and d["sl"].value == 2.0
    assert apply(d, "tp", "5") and d["tp"].kind == "usd" and d["tp"].value == 5.0
    assert apply(d, "tp", "$10") and d["tp"].kind == "usd" and d["tp"].value == 10.0
    assert apply(d, "sl", "1,5%") and d["sl"].kind == "percent" and d["sl"].value == 1.5


def test_clearing_a_bracket():
    for blank in ("-", "0", "", "нема"):
        d = CopyBot._new_draft()
        d["sl"] = bracket("2%")
        assert apply(d, "sl", blank) and d["sl"] is None


def test_bracket_labels_read_back():
    assert bracket("2%").label() == "2%"
    assert bracket("$5").label() == "$5"


def test_a_non_number_bracket_is_rejected_by_the_form():
    d = CopyBot._new_draft()
    assert not apply(d, "sl", "abc") and d["sl"] is None


@pytest.mark.parametrize("text", ["+90", "90s", "16:30", "16:30:00", "23:59"])
def test_accepted_time_shapes_land_in_the_future(text):
    assert parse(text) > time.time()


def test_seconds_from_now_is_about_right():
    assert parse("+120") == pytest.approx(time.time() + 120, abs=2)


def test_a_bare_clock_time_already_past_rolls_to_tomorrow():
    # one minute ago on the clock should schedule ~24h out, not in the past
    past = time.strftime("%H:%M", time.localtime(time.time() - 120))
    assert parse(past) > time.time() + 23 * 3600


def test_an_unreadable_time_raises():
    with pytest.raises(ValueError):
        parse("banana")
