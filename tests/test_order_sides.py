"""The side numbers, pinned to what the venue actually does.

These were wrong for as long as the bot has existed. `SIDE_OPEN_SHORT` was 3 and `SIDE_CLOSE_LONG`
was 4, the other way round from reality, so every request to open a short was really a request to
close a long — and on an account holding no long, MEXC answers that with

    [2009] Position is nonexistent or closed

which is exactly the error a follower in REVERSE mode kept reporting while never opening anything.

Measured on a live account, one contract at a time:

    side=3, holding nothing   -> a SHORT of 1 contract appeared
    side=4, holding a SHORT   -> [2009] Position is nonexistent or closed
    side=4, holding nothing   -> [2009] Position is nonexistent or closed
    side=1, holding nothing   -> a LONG of 1 contract appeared

MEXC's own documentation lists these in the other order, so anyone checking the docs against this
file will conclude it is wrong. It is not. That is why this test exists.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mexc_copy_bot.core import orders  # noqa: E402
from mexc_copy_bot.core.copy_engine import side_for  # noqa: E402
from mexc_copy_bot.db.store import FOLLOWER, Account  # noqa: E402
from mexc_copy_bot.mexc import rest  # noqa: E402

LONG, SHORT = 1, 2


def test_three_opens_a_short_and_four_closes_a_long():
    assert rest.SIDE_OPEN_LONG == 1
    assert rest.SIDE_CLOSE_SHORT == 2
    assert rest.SIDE_OPEN_SHORT == 3, "side=3 opened a SHORT on the live account"
    assert rest.SIDE_CLOSE_LONG == 4, "side=4 needed a LONG to act on"


def test_both_copies_of_the_enum_agree():
    """orders.py keeps its own copy for parsing the master's frames. Two definitions of the same
    venue constant is exactly the arrangement that drifts."""
    for name in ("SIDE_OPEN_LONG", "SIDE_CLOSE_SHORT", "SIDE_OPEN_SHORT", "SIDE_CLOSE_LONG"):
        assert getattr(orders, name) == getattr(rest, name), name


def test_the_four_sides_are_distinct():
    values = {rest.SIDE_OPEN_LONG, rest.SIDE_CLOSE_SHORT, rest.SIDE_OPEN_SHORT, rest.SIDE_CLOSE_LONG}
    assert values == {1, 2, 3, 4}


def test_opening_and_closing_sets_do_not_overlap():
    assert not orders.OPENING_SIDES & orders.CLOSING_SIDES
    assert orders.OPENING_SIDES == {1, 3}
    assert orders.CLOSING_SIDES == {2, 4}


def test_each_side_is_attributed_to_the_right_position():
    assert orders.SIDE_TO_POSITION[rest.SIDE_OPEN_LONG] == LONG
    assert orders.SIDE_TO_POSITION[rest.SIDE_CLOSE_LONG] == LONG
    assert orders.SIDE_TO_POSITION[rest.SIDE_OPEN_SHORT] == SHORT
    assert orders.SIDE_TO_POSITION[rest.SIDE_CLOSE_SHORT] == SHORT


def test_a_reversed_follower_is_sent_the_side_that_actually_opens_a_short():
    """The end of the chain, and the thing that was broken: master LONG, follower REVERSE, and the
    order that goes out must be one the venue will accept from a flat account."""
    follower = Account(
        id=1, owner_id=1, label="F", kind=FOLLOWER, api_key_hint="k", size_multiplier=1.0,
        active=True, position_mode=1, last_error=None, direction="REVERSE",
    )
    taken = side_for(follower, master_side=LONG, honour_direction=True)
    assert taken == SHORT

    order_side = rest.SIDE_OPEN_LONG if taken == LONG else rest.SIDE_OPEN_SHORT
    assert order_side == 3, "a reversed follower must be sent side=3, the one that opens a short"


# ── both modes, both master directions ──────────────────────────────────────
def _follower(direction: str) -> Account:
    return Account(
        id=1, owner_id=1, label="F", kind=FOLLOWER, api_key_hint="k", size_multiplier=1.0,
        active=True, position_mode=1, last_error=None, direction=direction,
    )


def _order_side(mode: str, direction: str, master_side: int) -> int:
    """The number that actually goes to the venue, derived the way copy_engine derives it."""
    taken = side_for(_follower(direction), master_side, honour_direction=(mode == "REVERSE"))
    return rest.SIDE_OPEN_LONG if taken == LONG else rest.SIDE_OPEN_SHORT


def test_every_mode_and_direction_sends_a_side_that_opens():
    """The whole matrix, in one place.

    Only two venue facts are needed, and both were measured: side=1 opened a LONG and side=3
    opened a SHORT on a flat account. Everything else here is which of those two gets sent, which
    is arithmetic — so this needs no live trade to be trustworthy.

    Three of the four used to send side=4, the number that closes a long, and failed against a
    flat account with [2009]. Only COPY on a LONG master was ever exercised, which is why the bot
    looked like it worked.
    """
    cases = {
        # (mode, follower direction, master side): (side the follower takes, number sent)
        ("COPY", "COPY", LONG): (LONG, 1),
        ("COPY", "COPY", SHORT): (SHORT, 3),
        ("REVERSE", "REVERSE", LONG): (SHORT, 3),
        ("REVERSE", "REVERSE", SHORT): (LONG, 1),
        # A follower left on COPY inside a REVERSE folder still follows the master.
        ("REVERSE", "COPY", LONG): (LONG, 1),
        ("REVERSE", "COPY", SHORT): (SHORT, 3),
    }
    for (mode, direction, master_side), (expected_side, expected_number) in cases.items():
        taken = side_for(_follower(direction), master_side, honour_direction=(mode == "REVERSE"))
        assert taken == expected_side, f"{mode}/{direction}, master {master_side}"
        got = _order_side(mode, direction, master_side)
        assert got == expected_number, f"{mode}/{direction}, master {master_side}: sent {got}"
        assert got in orders.OPENING_SIDES, f"{mode}/{direction} sends a side that does not open"


def test_a_copy_folder_ignores_a_stray_reverse_setting():
    """Directions are kept when the mode is switched back, so they must not leak into COPY."""
    assert side_for(_follower("REVERSE"), LONG, honour_direction=False) == LONG
    assert side_for(_follower("REVERSE"), SHORT, honour_direction=False) == SHORT
