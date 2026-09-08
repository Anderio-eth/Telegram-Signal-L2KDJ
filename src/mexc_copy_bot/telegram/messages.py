"""Message formatting for Telegram.

Sizes are shown as NOTIONAL (contracts × contract size × price), because "1 contract" means
nothing to a human while "$1,000" does — that was the explicit ask. Contract counts stay internal:
they are what actually gets sent to the exchange, so that a follower ends up with the master's
position rather than a rounded dollar approximation.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.copy_engine import FollowerResult
from ..core.events import Action, MasterEvent
from ..db.store import MODE_REVERSE, Account

ACTION_ICON = {
    Action.OPEN: "📈",
    Action.INCREASE: "📈",
    Action.DECREASE: "📉",
    Action.CLOSE: "📕",
}

ACTION_TITLE = {
    Action.OPEN: "ПОЗИЦІЮ ВІДКРИТО",
    Action.INCREASE: "ПОЗИЦІЮ ЗБІЛЬШЕНО",
    Action.DECREASE: "ПОЗИЦІЮ ЗМЕНШЕНО",
    Action.CLOSE: "ПОЗИЦІЮ ЗАКРИТО",
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


@dataclass(frozen=True)
class Balance:
    """What one account is worth right now, or why we could not find out.

    An error is carried alongside rather than raised: one follower with a revoked key must not
    blank out everybody else's numbers on a screen you read before pressing START.
    """

    equity: float | None = None
    available: float | None = None
    error: str | None = None

    def line(self) -> str:
        if self.error:
            return f"❌ {self.error}"
        if self.equity is None:
            return "…"
        return f"{money(self.equity)} (available {money(self.available or 0.0)})"


def signed_money(value: float) -> str:
    """PnL with an explicit sign, so a loss can never be misread as a gain at a glance."""
    return f"{'+' if value >= 0 else '−'}{money(abs(value))}"


def mode_name(mode: int | None) -> str:
    if mode == 1:
        return "hedge"
    if mode == 2:
        return "one-way"
    return "unknown"


def main_menu(
    *,
    running: bool,
    master: Account | None,
    followers: list[Account],
    max_followers: int,
    master_connected: bool,
    balances: dict[int, Balance] | None = None,
    mode: str = "COPY",
    reverse_account: Account | None = None,
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
        status = "⚪️ НЕМАЄ MASTER"
    elif running and master_connected:
        status = "🟢 ПРАЦЮЄ"
    elif running:
        status = "🟡 ПРАЦЮЄ (майстер перепідключається)"
    else:
        status = "🔴 ЗУПИНЕНО"

    lines = ["🤖 <b>MEXC COPY BOT</b>", "", f"Статус: {status}"]

    if mode == MODE_REVERSE:
        target = reverse_account.label if reverse_account else "⚠️ акаунт не обрано"
        lines.append(f"Режим: 🔁 <b>РЕВЕРС</b> → {target}")
    else:
        lines.append("Режим: 📋 Копіювання (усі followers)")
    lines.append("")

    if not master:
        lines.append("👤 <b>Master:</b> не додано")
    else:
        lines.append(f"👤 <b>Master</b> — …{master.api_key_hint}")
        lines.append(f"     {balances.get(master.id, Balance()).line()}")
        lines.append(f"     Режим: {mode_name(master.position_mode)}")

    lines.append("")
    lines.append(f"👥 <b>Followers:</b> {len(followers)}/{max_followers}")
    for follower in followers:
        mark = "" if follower.active else "  (на паузі)"
        lines.append(f"     • {follower.label} — …{follower.api_key_hint}{mark}")
        lines.append(f"          {balances.get(follower.id, Balance()).line()}")

    # Only with several followers: repeating one follower's own balance as a "total" underneath
    # it is noise, and a total that quietly omits the accounts that failed to report would be
    # worse than none.
    known = [balances[f.id].equity for f in followers if balances.get(f.id) and balances[f.id].equity is not None]
    if len(known) > 1:
        suffix = "" if len(known) == len(followers) else f" (відповіли {len(known)}/{len(followers)})"
        lines.append(f"     <b>Разом:</b> {money(sum(known))}{suffix}")

    return "\n".join(lines)


def event_report(event: MasterEvent, results: list[FollowerResult], notional: float | None) -> str:
    icon = ACTION_ICON[event.action]
    title = ACTION_TITLE[event.action]
    size_line = f"Обсяг: {money(notional)}" if notional else f"Обсяг: {event.delta_vol:g} контрактів"

    lines = [
        f"{icon} <b>{title}</b>",
        "",
        f"<b>{event.symbol}</b> {side_name(event.position_type)}",
    ]
    if event.action is not Action.CLOSE:
        lines.append(f"Плече: {event.leverage}x")
        lines.append(size_line)
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("")

    ok = 0
    for result in results:
        if result.ok:
            ok += 1
            detail = "ЗАКРИТО" if event.action is Action.CLOSE else f"{result.action.value}"
            line = f"✅ {result.account.label} — {detail}"
            if event.action is Action.CLOSE:
                # An unknown settlement says so rather than printing a zero, which would read as
                # "this trade broke even".
                line += (
                    f"  {signed_money(result.realized_pnl)}"
                    if result.realized_pnl is not None
                    else "  (PnL рахується)"
                )
            lines.append(line)
        else:
            lines.append(f"❌ {result.account.label} — {result.error or 'помилка'}")

    lines.append("")
    lines.append(f"Успішно: {ok}/{len(results)}")

    if event.action is Action.CLOSE:
        known = [r.realized_pnl for r in results if r.ok and r.realized_pnl is not None]
        if known:
            reported = len(known)
            closed = sum(1 for r in results if r.ok)
            suffix = "" if reported == closed else f"  (порахували {reported}/{closed})"
            lines.append(f"<b>Загальний PnL: {signed_money(sum(known))}</b>{suffix}")

    return "\n".join(lines)


def mode_screen(mode: str, reverse_account: Account | None, followers: list[Account]) -> str:
    lines = ["⚙️ <b>РЕЖИМ</b>", ""]
    if mode == MODE_REVERSE:
        lines.append("Зараз: 🔁 <b>РЕВЕРС</b>")
        lines.append(
            f"Хеджує на: <b>{reverse_account.label}</b>"
            if reverse_account
            else "⚠️ Акаунт не обрано — нічого копіюватись не буде."
        )
    else:
        lines.append("Зараз: 📋 <b>КОПІЮВАННЯ</b>")
        lines.append(f"Дзеркалить майстра на всі {len(followers)} акаунт(и).")

    lines += [
        "",
        "━━━━━━━━━━━━━━",
        "",
        "📋 <b>Копіювання</b> — кожен follower відкриває <i>ту саму</i> сторону, що майстер.",
        "",
        "🔁 <b>Реверс</b> — один обраний акаунт відкриває <i>протилежну</i>: майстер у LONG, "
        "він у SHORT. Автоматичний хедж.",
        "",
        "Одночасно працює лише один режим.",
        "",
        "━━━━━━━━━━━━━━",
        "",
        "📌 <b>Лімітні ордери:</b> копіюються завжди",
        "",
        "Лімітка, що стоїть у майстра, виставляється на всіх followers за тією самою ціною — "
        "і на відкриття, і на закриття. Вони заповнюються разом із майстром, а не наздоганяють "
        "його маркетом.",
    ]
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
        return "📜 <b>ІСТОРІЯ</b>\n\nЩе нічого не копіювалось."
    lines = ["📜 <b>ІСТОРІЯ</b>", ""]
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
