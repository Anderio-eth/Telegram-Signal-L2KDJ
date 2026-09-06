"""Message formatting for Telegram.

Sizes are shown as NOTIONAL (contracts × contract size × price), because "1 contract" means
nothing to a human while "$1,000" does — that was the explicit ask. Contract counts stay internal:
they are what actually gets sent to the exchange, so that a follower ends up with the master's
position rather than a rounded dollar approximation.
"""

from __future__ import annotations

from ..core.copy_engine import FollowerResult
from ..core.events import Action, MasterEvent
from ..db.store import Account

ACTION_ICON = {
    Action.OPEN: "📈",
    Action.INCREASE: "📈",
    Action.DECREASE: "📉",
    Action.CLOSE: "📕",
}

ACTION_TITLE = {
    Action.OPEN: "POSITION OPENED",
    Action.INCREASE: "POSITION INCREASED",
    Action.DECREASE: "POSITION DECREASED",
    Action.CLOSE: "POSITION CLOSED",
}


def side_name(position_type: int) -> str:
    return "LONG" if position_type == 1 else "SHORT"


def money(value: float) -> str:
    if value >= 1000:
        return f"${value:,.0f}"
    if value >= 1:
        return f"${value:,.2f}"
    return f"${value:.4f}"


def main_menu(
    *, running: bool, master: Account | None, follower_count: int, max_followers: int, master_connected: bool
) -> str:
    if not master:
        status = "⚪️ NO MASTER"
    elif running and master_connected:
        status = "🟢 RUNNING"
    elif running:
        status = "🟡 RUNNING (master reconnecting)"
    else:
        status = "🔴 STOPPED"

    master_line = f"👤 Master: {master.label} (…{master.api_key_hint})" if master else "👤 Master: not set"
    return (
        "🤖 <b>MEXC COPY BOT</b>\n\n"
        f"Status: {status}\n\n"
        f"{master_line}\n"
        f"👥 Followers: {follower_count}/{max_followers}"
    )


def event_report(event: MasterEvent, results: list[FollowerResult], notional: float | None) -> str:
    icon = ACTION_ICON[event.action]
    title = ACTION_TITLE[event.action]
    size_line = f"Size: {money(notional)}" if notional else f"Size: {event.delta_vol:g} contracts"

    lines = [
        f"{icon} <b>{title}</b>",
        "",
        f"<b>{event.symbol}</b> {side_name(event.position_type)}",
    ]
    if event.action is not Action.CLOSE:
        lines.append(f"Leverage: {event.leverage}x")
        lines.append(size_line)
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("")

    ok = 0
    for result in results:
        if result.ok:
            ok += 1
            detail = "CLOSED" if event.action is Action.CLOSE else f"{result.action.value}"
            lines.append(f"✅ {result.account.label} — {detail}")
        else:
            lines.append(f"❌ {result.account.label} — {result.error or 'failed'}")

    lines.append("")
    lines.append(f"Success: {ok}/{len(results)}")
    return "\n".join(lines)


def accounts_list(master: Account | None, followers: list[Account]) -> str:
    lines = ["👥 <b>ACCOUNTS</b>", ""]
    if master:
        state = "🔴 error" if master.last_error else "🟢 connected"
        lines.append(f"👑 <b>{master.label}</b> (…{master.api_key_hint}) {state}")
        if master.last_error:
            lines.append(f"    {master.last_error[:80]}")
    else:
        lines.append("👑 Master: not set")

    lines.append("")
    if not followers:
        lines.append("No followers yet.")
    else:
        lines.append("<b>Followers:</b>")
        for f in followers:
            state = "🔴" if f.last_error else ("🟢" if f.active else "⏸")
            multiplier = "" if abs(f.size_multiplier - 1.0) < 1e-9 else f" ×{f.size_multiplier:g}"
            lines.append(f"{state} {f.label} (…{f.api_key_hint}){multiplier}")
            if f.last_error:
                lines.append(f"    {f.last_error[:80]}")
    return "\n".join(lines)


def history(events: list[dict]) -> str:
    if not events:
        return "📜 <b>HISTORY</b>\n\nNothing copied yet."
    lines = ["📜 <b>HISTORY</b>", ""]
    for e in events:
        when = e["observed_at"].strftime("%d.%m %H:%M")
        lines.append(
            f"{when}  <b>{e['symbol']}</b> {side_name(e['position_type'])} {e['action']}"
            f"  ✅{e['ok']} ❌{e['failed']}"
        )
    return "\n".join(lines)
