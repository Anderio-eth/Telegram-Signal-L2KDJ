"""Message formatting for Telegram.

Sizes are shown as NOTIONAL (contracts × contract size × price), because "1 contract" means
nothing to a human while "$1,000" does — that was the explicit ask. Contract counts stay internal:
they are what actually gets sent to the exchange, so that a follower ends up with the master's
position rather than a rounded dollar approximation.

Every function takes the reader's language and pulls its wording from i18n, so there is one
version of each screen rather than two that drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.copy_engine import FollowerResult
from ..core.events import Action, MasterEvent
from ..db.store import MODE_REVERSE, Account
from .i18n import DEFAULT, t

ACTION_ICON = {
    Action.OPEN: "📈",
    Action.INCREASE: "📈",
    Action.DECREASE: "📉",
    Action.CLOSE: "📕",
}

ACTION_KEY = {
    Action.OPEN: "act_open",
    Action.INCREASE: "act_increase",
    Action.DECREASE: "act_decrease",
    Action.CLOSE: "act_close",
}


def side_name(position_type: int) -> str:
    return "LONG" if position_type == 1 else "SHORT"


def money(value: float) -> str:
    if value >= 1000:
        return f"${value:,.0f}"
    if value >= 1:
        return f"${value:,.2f}"
    # Under a dollar, cents are not enough resolution for a PnL on a small position, but padding
    # every figure to four places turns "half a dollar" into $0.5000. Keep the digits that carry
    # information and drop the rest, never going below the two everyone expects on money.
    digits = f"{value:.4f}".rstrip("0")
    whole, _, frac = digits.partition(".")
    return f"${whole}.{(frac + '00')[:max(2, len(frac))]}"


def signed_money(value: float) -> str:
    """PnL with an explicit sign, so a loss can never be misread as a gain at a glance."""
    return f"{'+' if value >= 0 else '−'}{money(abs(value))}"


def mode_name(mode: int | None) -> str:
    # Left untranslated: these are the words MEXC itself uses in its own interface.
    if mode == 1:
        return "hedge"
    if mode == 2:
        return "one-way"
    return "unknown"


@dataclass(frozen=True)
class Balance:
    """What one account is worth right now, or why we could not find out.

    An error is carried alongside rather than raised: one follower with a revoked key must not
    blank out everybody else's numbers on a screen you read before pressing START.
    """

    equity: float | None = None
    # What a new position can actually be opened against — MEXC's availableOpen, not the wallet
    # figure. They are usually the same; when they are not, the wallet figure is the one that
    # makes a refused order look inexplicable.
    available: float | None = None
    # The wallet figure, kept only to show the gap when there is one.
    wallet: float | None = None
    error: str | None = None

    def line(self, lang: str = DEFAULT) -> str:
        if self.error:
            return f"❌ {self.error}"
        if self.equity is None:
            return "…"
        line = t(lang, "available", equity=money(self.equity), available=money(self.available or 0.0))
        # Only when they disagree. Printing two identical numbers on every account would bury the
        # one case that matters under nine that do not.
        if self.wallet is not None and self.available is not None and self.wallet - self.available > 0.01:
            line += t(lang, "not_openable", amount=money(self.wallet - self.available))
        return line


def main_menu(
    *,
    running: bool,
    master: Account | None,
    followers: list[Account],
    max_followers: int,
    master_connected: bool,
    balances: dict[int, Balance] | None = None,
    mode: str = "COPY",
    lang: str = DEFAULT,
    listed_in_columns: bool = False,
) -> str:
    """The main screen.

    Carries every account's details inline — balance, mode, key hint — rather than leaving them
    in the one-off "account added" message that scrolls away: this is the screen you look at
    before pressing START, and it should answer "is everything connected and funded" on its own.
    A follower that is out of money fails at the first mirrored trade, and that is worth seeing
    beforehand rather than in an error report afterwards.
    """
    balances = balances or {}
    if not master:
        status = t(lang, "status_no_master")
    elif running and master_connected:
        status = t(lang, "status_running")
    elif running:
        status = t(lang, "status_reconnecting")
    else:
        status = t(lang, "status_stopped")

    lines = ["🤖 <b>MEXC COPY BOT</b>", "", t(lang, "menu_status", status=status)]

    if mode == MODE_REVERSE:
        # Each account carries its own direction, so the headline is a count of both sides rather
        # than one nominated account — with nine followers split five/four, naming one of them
        # would describe almost nothing about what the folder is going to do.
        reversed_count = sum(1 for f in followers if f.is_reversed)
        lines.append(
            t(lang, "menu_mode_reverse_none")
            if not followers
            else t(
                lang, "menu_mode_reverse_split",
                reverse=reversed_count, copy=len(followers) - reversed_count,
            )
        )
    else:
        lines.append(t(lang, "menu_mode_copy"))
    lines.append("")

    if listed_in_columns:
        # The accounts are on the keyboard below, one cell each with a balance and a light.
        # Repeating them here would put the same numbers on screen twice, and the two copies
        # would disagree the moment one of them was a moment older than the other.
        if not master:
            lines.append(t(lang, "master_not_set"))
        return chr(10).join(lines).rstrip()

    if not master:
        lines.append(t(lang, "master_not_set"))
    else:
        lines.append(f"👤 <b>Master</b> — …{master.api_key_hint}")
        lines.append(f"     {balances.get(master.id, Balance()).line(lang)}")
        lines.append(t(lang, "master_mode", mode=mode_name(master.position_mode)))

    lines.append("")
    lines.append(t(lang, "followers_count", n=len(followers), max=max_followers))
    for follower in followers:
        mark = "" if follower.active else t(lang, "paused")
        lines.append(f"     • {follower.label} — …{follower.api_key_hint}{mark}")
        lines.append(f"          {balances.get(follower.id, Balance()).line(lang)}")

    # Only with several followers: repeating one follower's own balance as a "total" underneath
    # it is noise, and a total that quietly omits the accounts that failed to report would be
    # worse than none.
    known = [balances[f.id].equity for f in followers if balances.get(f.id) and balances[f.id].equity is not None]
    if len(known) > 1:
        suffix = (
            ""
            if len(known) == len(followers)
            else t(lang, "reporting_suffix", n=len(known), total=len(followers))
        )
        lines.append(t(lang, "total", amount=money(sum(known)), suffix=suffix))

    return "\n".join(lines)


def event_report(
    event: MasterEvent, results: list[FollowerResult], notional: float | None, lang: str = DEFAULT
) -> str:
    icon = ACTION_ICON[event.action]
    title = t(lang, ACTION_KEY[event.action])
    size_line = (
        t(lang, "size_usd", amount=money(notional))
        if notional
        else t(lang, "size_contracts", n=f"{event.delta_vol:g}")
    )

    lines = [
        f"{icon} <b>{title}</b>",
        "",
        f"<b>{event.symbol}</b> {side_name(event.position_type)}",
    ]
    if event.action is not Action.CLOSE:
        lines.append(t(lang, "leverage", n=event.leverage))
        lines.append(size_line)
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("")

    # The master goes first and in the same shape as the accounts below it: it is the position
    # every other line is a copy of, and a report that showed what the copies made while leaving
    # out what the original made answered half the question. Its number comes from the same field
    # theirs do, so the total underneath is a sum of like with like.
    master_pnl = event.realized_pnl if event.action is Action.CLOSE else None
    if master_pnl is not None:
        detail = f"{t(lang, 'closed')}  {signed_money(master_pnl)}"
        lines.append(t(lang, "master_line", detail=detail))

    ok = 0
    for result in results:
        if result.ok:
            ok += 1
            detail = t(lang, "closed") if event.action is Action.CLOSE else result.action.value
            # Named per account, not per master action: with per-account directions a single
            # master move puts some accounts long and others short, and one shared heading would
            # be wrong for half of them.
            taken = result.position_type or event.position_type
            marker = "" if taken == event.position_type else f" {side_name(taken)}"
            line = f"✅ {result.account.label} — {detail}{marker}"
            if event.action is Action.CLOSE:
                # An unknown settlement says so rather than printing a zero, which would read as
                # "this trade broke even".
                line += (
                    f"  {signed_money(result.realized_pnl)}"
                    if result.realized_pnl is not None
                    else t(lang, "pnl_pending")
                )
            lines.append(line)
        else:
            lines.append(f"❌ {result.account.label} — {result.error or t(lang, 'failed')}")

    lines.append("")
    lines.append(t(lang, "success_count", ok=ok, total=len(results)))

    if event.action is Action.CLOSE:
        known = [r.realized_pnl for r in results if r.ok and r.realized_pnl is not None]
        # "Success" counts mirroring, so the master is deliberately not in it — no order was placed
        # on the master's behalf and there was nothing there to succeed or fail. It is in the
        # total, because the total is money and every line above it is money.
        counted = known + ([master_pnl] if master_pnl is not None else [])
        if counted:
            reported = len(known)
            closed = sum(1 for r in results if r.ok)
            suffix = "" if reported == closed else t(lang, "counted_suffix", n=reported, total=closed)
            lines.append(t(lang, "total_pnl", amount=signed_money(sum(counted)), suffix=suffix))

    return "\n".join(lines)


def mode_screen(
    mode: str, reverse_account: Account | None, followers: list[Account], lang: str = DEFAULT
) -> str:
    lines = [t(lang, "mode_title"), ""]
    if mode == MODE_REVERSE:
        lines.append(t(lang, "mode_now_reverse"))
    else:
        lines.append(t(lang, "mode_now_copy"))
        lines.append(t(lang, "mode_mirroring_all", n=len(followers)))

    if mode == MODE_REVERSE:
        lines.append("")
        for follower in followers:
            group = "2️⃣" if follower.is_reversed else "1️⃣"
            paused = t(lang, "paused") if not follower.active else ""
            lines.append(f"   {group} {follower.label}{paused}")
        lines.append("")
        lines.append(t(lang, "dir_pick"))

    lines += [
        "",
        "━━━━━━━━━━━━━━",
        "",
        t(lang, "mode_explain_copy"),
        "",
        t(lang, "mode_explain_reverse"),
        "",
        t(lang, "mode_one_at_a_time"),
        "",
        "━━━━━━━━━━━━━━",
        "",
        t(lang, "limits_title"),
        "",
        t(lang, "limits_explain"),
    ]
    return "\n".join(lines)


def stuck_screen(groups: list, lang: str = DEFAULT) -> str:
    """Every group of stranded accounts, with what is outstanding on each.

    Groups are never merged: a later failure is its own group, so it stays obvious which accounts
    are stuck on which trade rather than becoming one undifferentiated pile.
    """
    if not groups:
        return t(lang, "stuck_none")

    lines = [t(lang, "stuck_title"), ""]
    for group in groups:
        kind = t(lang, "stuck_kind_entry" if group.is_entry else "stuck_kind_exit")
        lines.append(
            t(
                lang, "stuck_group_head",
                id=group.id, kind=kind, symbol=group.symbol,
                side=side_name(group.position_type),
                time=group.created_at.strftime("%H:%M"),
            )
        )
        lines.append(
            t(lang, "stuck_group_limit", price=f"{group.limit_price:g}")
            if group.limit_price
            else t(lang, "stuck_group_nolimit")
        )
        lines.append(t(lang, "stuck_explain_entry" if group.is_entry else "stuck_explain_exit"))
        for member in group.members:
            lines.append(f"     • {member.label} — {member.vol:g}")
        lines.append("")
    lines.append(t(lang, "back_under_master"))
    return "\n".join(lines)


def folders_screen(folders, active_id, counts, lang=DEFAULT) -> str:
    """Every folder this owner has: which one is on screen, and which are trading.

    The running marker matters more than the selected one. Folders keep copying whether or not
    anyone is looking at them, and a list that showed only the selection would hide exactly the
    thing worth knowing before you walk away from the bot.
    """
    lines = [t(lang, "folders_title"), "", t(lang, "folders_explain"), ""]
    for folder in folders:
        lines.append(
            t(
                lang, "folder_row",
                mark="▶️" if folder.id == active_id else "  ",
                name=folder.name,
                accounts=counts.get(folder.id, 0),
                state=t(lang, "folder_running" if folder.running else "folder_stopped"),
            )
        )
    return "\n".join(lines)


def accounts_list(master: Account | None, followers: list[Account], lang: str = DEFAULT) -> str:
    lines = [t(lang, "accounts_title"), ""]
    if master:
        state = t(lang, "acc_error") if master.last_error else t(lang, "acc_connected")
        lines.append(f"👑 <b>{master.label}</b> (…{master.api_key_hint}) {state}")
        if master.last_error:
            lines.append(f"    {master.last_error[:80]}")
    else:
        lines.append(t(lang, "acc_master_not_set"))

    lines.append("")
    if not followers:
        lines.append(t(lang, "acc_no_followers"))
    else:
        lines.append(t(lang, "acc_followers"))
        for f in followers:
            state = "🔴" if f.last_error else ("🟢" if f.active else "⏸")
            multiplier = "" if abs(f.size_multiplier - 1.0) < 1e-9 else f" ×{f.size_multiplier:g}"
            lines.append(f"{state} {f.label} (…{f.api_key_hint}){multiplier}")
            if f.last_error:
                lines.append(f"    {f.last_error[:80]}")
    return "\n".join(lines)


def history(events: list[dict], lang: str = DEFAULT) -> str:
    if not events:
        return t(lang, "history_empty")
    lines = [t(lang, "history_title"), ""]
    for e in events:
        when = e["observed_at"].strftime("%d.%m %H:%M")
        line = (
            f"{when}  <b>{e['symbol']}</b> {side_name(e['position_type'])} {e['action']}"
            f"  ✅{e['ok']} ❌{e['failed']}"
        )
        # Only closes settle into a PnL; pnl_count guards against showing a sum that covers only
        # some of the accounts as though it covered them all.
        if e.get("pnl") is not None and e.get("pnl_count"):
            line += f"  {signed_money(float(e['pnl']))}"
            if e["pnl_count"] < e["ok"]:
                line += f" ({e['pnl_count']}/{e['ok']})"
        lines.append(line)
    return "\n".join(lines)

# ── the REVERSE screen's two columns ────────────────────────────────────────────────────────
def compact_money(value: float | None) -> str:
    """A balance small enough to sit in a button next to a name and a light.

    Whole dollars above one, because the cents in "$1,247.38" cost three characters to say
    nothing you would act on. Under a dollar they are the only part that carries information, so
    they stay.
    """
    if value is None:
        return "—"
    # An emptied wallet comes back as 6e-09 rather than 0 — the venue's own rounding dust. Printed
    # faithfully it reads "$0.00", which looks like a balance that is merely small.
    if abs(value) < 0.005:
        return "$0"
    if abs(value) >= 1:
        return f"${value:,.0f}"
    return f"${value:.2f}"


@dataclass(frozen=True)
class Cell:
    """One account as it appears in a column."""

    account_id: int
    label: str
    balance: float | None
    holding: bool

    def text(self) -> str:
        # The light goes last so the eye can run down the right-hand edge of a column and see
        # which accounts are in and which are not, without reading a single name.
        return f"{self.label}  {compact_money(self.balance)}  {'🟢' if self.holding else '🔴'}"


def group_title(cells: list[Cell], sides: dict[int, int], name: str) -> str:
    """A leg's heading: its name, and the way it is currently facing.

    Which way a leg trades is not a property of the leg — it is decided by whoever opens first —
    so the name alone is what it says while nothing is open. Once a position exists the heading
    reports the side actually held, and goes back to the bare name when everything is closed.
    """
    held = [sides.get(cell.account_id) for cell in cells]
    facing = {side for side in held if side}
    if len(facing) != 1:
        # Nothing open, or the leg disagrees with itself — which can happen while somebody is
        # closing it by hand. Claiming a single direction then would be a guess.
        return name
    return f"{name} — {side_name(facing.pop())}"


def reverse_columns(
    master: Account | None,
    followers: list[Account],
    balances: dict[int, Balance],
    holding: dict[int, bool],
) -> tuple[list[Cell], list[Cell]]:
    """Split a folder into the two legs the REVERSE screen shows.

    Left is everything trading the master's way — the master itself included. It is listed there
    rather than above because after the entry it is exactly that and nothing more: its own exit
    moves no other account, so singling it out would suggest an authority it does not have.

    Right is everything trading against the master.

    Numbering runs within each column. The account's stored label is not used: "Follower #7" says
    nothing about which way it trades, and which way it trades is the only thing this screen is
    about.
    """
    def balance_of(account: Account) -> float | None:
        entry = balances.get(account.id)
        return entry.available if entry and entry.error is None else None

    left: list[Cell] = []
    right: list[Cell] = []

    if master:
        # Numbered with everyone else. In this mode it is one more account in whichever leg it was
        # put, and calling it "Master" on the screen would suggest an authority it does not have.
        left.append(Cell(master.id, "1.1", balance_of(master), holding.get(master.id, False)))

    for account in followers:
        column = right if account.is_reversed else left
        side = "2." if account.is_reversed else "1."
        # Counted over the accounts already placed in this column under the same heading, so the
        # master sitting at the top of the left one does not consume a follower's number.
        index = len(column) + 1
        column.append(
            Cell(account.id, f"{side}{index}", balance_of(account), holding.get(account.id, False))
        )

    return left, right
