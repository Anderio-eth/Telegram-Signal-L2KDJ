"""Open a hedged position across two groups at a set time, sliced into parts.

At a known moment T two sides must be fully in position — group 1 long, group 2 short — at a size too
large to send as one market order without walking the book and, at high leverage, without risking
liquidation on the entry itself. So the size is split into N equal market orders spaced `step` apart,
fired on every account of both groups at once, timed so the LAST slice fills at T.

There is no long/short choice: the folder's groups already decide sides. Every account in group 1
buys, every account in group 2 sells, each opening `margin × leverage` at the same sliced sizes.

The maths, in one place, is `plan_ladder` — pure, so it is tested without a network or a clock:

    size per account    = margin × leverage        (e.g. $150 × 1000 = $150k)
    contracts total     = size / price             (HIBT sizes are the base asset)
    contracts per slice = total / N                (the last slice takes the rounding remainder)
    slice i is sent at  = T − latency − (N−1−i)×step   (so slice N−1 fills at T)

A market order fills in the same second it is accepted, ~215 ms after it leaves here, so `latency`
is that round trip and `step` must not be smaller than it. If the start time works out to be in the
past, the plan says so rather than firing late.

`LadderExecutor` runs a plan against the two groups of live clients. It is deliberately dumb about
failure: a slice a venue refuses is recorded and the rest go on unchanged — it never doubles up to
catch up and never unwinds what filled. What opened, on which account, and why the rest did not, is
the whole report.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal, InvalidOperation

from ..mexc.rest import SIDE_OPEN_LONG, SIDE_OPEN_SHORT, MexcError

LOGGER = logging.getLogger(__name__)


def _dec(value) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _floor(value: Decimal, precision: int) -> Decimal:
    step = Decimal(1).scaleb(-max(0, int(precision)))
    return value.quantize(step, rounding=ROUND_DOWN)


def fmt_time(epoch: float) -> str:
    """A target/slice time as the person set it — Kyiv wall clock, HH:MM:SS, no milliseconds.
    Formatted in a fixed zone rather than the server's, so it reads the same on a UTC host."""
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(epoch, ZoneInfo("Europe/Kyiv")).strftime("%H:%M:%S")
    except Exception:  # noqa: BLE001 — no tz data: fall back to the host clock
        return datetime.fromtimestamp(epoch).strftime("%H:%M:%S")


@dataclass(frozen=True)
class LadderConfig:
    """The numeric plan of a scheduled hedged entry, as set in the bot. Which accounts take part is
    the folder's groups, resolved when the plan is built, not stored here."""

    symbol: str                 # BTC_USDT spelling; the client lowercases it
    leverage: int
    margin_usd: float           # collateral per account; size = margin × leverage
    parts: int                  # how many slices
    step_seconds: float         # gap between slices
    target_epoch: float         # when the position must be FULLY open (unix seconds)


@dataclass(frozen=True)
class Slice:
    index: int
    send_epoch: float           # when to send this slice (all accounts at once)
    amount: str                 # contracts, as the string the venue is given


@dataclass(frozen=True)
class LadderPlan:
    config: LadderConfig
    price: float
    target_notional: float      # margin × leverage, per account
    total_amount: str           # contracts per account, summed over slices
    slices: list[Slice]
    start_epoch: float          # when the first slice is sent
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def plan_ladder(
    config: LadderConfig,
    *,
    price: float,
    size_precision: int,
    min_order: float,
    latency_seconds: float,
    available: list[float],
    now: float,
) -> LadderPlan:
    """Work out the slices and their send times, or the reasons it cannot be done.

    `available` is the free balance of every account that will take part (both groups). Each account
    opens `margin` of collateral, so every one of them has to be able to afford it.

    Pure: every input that depends on the world is passed in, so the arithmetic can be checked.
    """
    errors: list[str] = []
    warnings: list[str] = []

    target_notional = float(config.margin_usd) * float(config.leverage)

    if config.parts < 1:
        errors.append("parts must be at least 1")
    if config.leverage < 1:
        errors.append("leverage must be at least 1")
    if config.margin_usd <= 0:
        errors.append("margin must be positive")
    if price <= 0:
        errors.append("no price for the symbol")
    if len(available) < 2:
        errors.append("need at least two accounts, one in each group")
    poorest = min(available) if available else 0.0
    if available and config.margin_usd > poorest + 1e-9:
        errors.append(f"an account has only ${poorest:,.2f} free, needs ${config.margin_usd:,.2f} margin")

    slices: list[Slice] = []
    total_amount = Decimal(0)
    if not errors:
        total = _floor(_dec(target_notional) / _dec(price), size_precision)
        per = _floor(total / config.parts, size_precision)
        if per <= 0 or float(per) < min_order:
            errors.append(
                f"each of {config.parts} slices would be {per} {config.symbol}, below the minimum "
                f"{min_order:g}; raise the margin or use fewer parts"
            )
        else:
            # Equal slices, the last absorbing what flooring dropped so the total is exact and every
            # account gets the identical schedule.
            remainder = _floor(total - per * (config.parts - 1), size_precision)
            amounts = [per] * (config.parts - 1) + [remainder]
            total_amount = sum(amounts, Decimal(0))
            start = config.target_epoch - latency_seconds - (config.parts - 1) * config.step_seconds
            slices = [
                Slice(index=i, send_epoch=start + i * config.step_seconds,
                      amount=format(amounts[i].normalize(), "f"))
                for i in range(config.parts)
            ]

    start_epoch = slices[0].send_epoch if slices else config.target_epoch
    if slices and start_epoch < now:
        late_by = now - start_epoch
        errors.append(
            f"too late: the first slice needed to go {(config.target_epoch - start_epoch):.1f}s "
            f"before T, which was {late_by:.1f}s ago"
        )
    if config.step_seconds < latency_seconds and config.parts > 1:
        warnings.append(
            f"step {config.step_seconds:.2f}s is under the ~{latency_seconds:.2f}s a request takes; "
            "slices may not keep their spacing"
        )

    return LadderPlan(
        config=config, price=price, target_notional=target_notional,
        total_amount=format(total_amount.normalize(), "f") if total_amount else "0",
        slices=slices, start_epoch=start_epoch, errors=errors, warnings=warnings,
    )


@dataclass
class SliceResult:
    index: int
    account: int
    side: str                   # "LONG" / "SHORT"
    amount: str
    ok: bool
    sent_epoch: float
    ack_ms: float | None = None
    order_id: str | None = None
    error: str | None = None


@dataclass
class LadderReport:
    plan: LadderPlan
    results: list[SliceResult]
    final: dict[int, str] = field(default_factory=dict)   # account id -> what it holds afterwards

    def filled(self, account: int) -> float:
        return sum(float(r.amount) for r in self.results if r.ok and r.account == account)

    @property
    def accounts(self) -> list[int]:
        seen = []
        for r in self.results:
            if r.account not in seen:
                seen.append(r.account)
        return seen

    def summary(self, labels: dict[int, str] | None = None) -> str:
        labels = labels or {}
        cfg = self.plan.config
        lines = [
            f"{cfg.symbol}  ${self.plan.target_notional:,.0f}/акаунт × {cfg.leverage}x, {cfg.parts} частин",
        ]
        for account in self.accounts:
            side = next((r.side for r in self.results if r.account == account), "?")
            name = labels.get(account, f"#{account}")
            got = self.filled(account)
            now = self.final.get(account)
            line = f"  {'🟢' if side == 'LONG' else '🔴'} {name} {side}: {got:g} / {self.plan.total_amount}"
            if now is not None:
                line += f" (тримає {now})"
            lines.append(line)
        for r in self.results:
            if not r.ok:
                lines.append(f"  ✗ {labels.get(r.account, r.account)} частина {r.index+1} {r.side} {r.amount}: {r.error}")
        acks = [r.ack_ms for r in self.results if r.ok and r.ack_ms is not None]
        if acks:
            lines.append(f"  {len(acks)} ордерів, {min(acks):.0f}-{max(acks):.0f}мс кожен")
        return "\n".join(lines)


class LadderExecutor:
    """Fires a plan across two groups. `long`/`short` are lists of (account_id, client). Every slice
    goes to all of them at once — group 1 buys, group 2 sells — and a refused order is recorded
    without doubling up or unwinding."""

    def __init__(self, long: list, short: list, *, symbol: str, leverage: int, open_type: int = 2) -> None:
        self._long = long
        self._short = short
        self._symbol = symbol
        self._leverage = leverage
        self._open_type = open_type

    async def prepare(self) -> None:
        """Set leverage on every account before the clock starts, so no slice pays for it in time."""
        await asyncio.gather(
            *(self._set_leverage(c) for _, c in self._long + self._short),
            return_exceptions=True,
        )

    async def _set_leverage(self, client) -> None:
        try:
            await client.set_leverage(position_id=None, leverage=self._leverage,
                                      open_type=self._open_type, symbol=self._symbol, position_type=1)
        except MexcError as err:
            LOGGER.warning("could not preset leverage: %s", err.message)

    async def run(self, plan: LadderPlan, *, sleep=asyncio.sleep, clock=time.time) -> LadderReport:
        results: list[SliceResult] = []
        for part in plan.slices:
            wait = part.send_epoch - clock()
            if wait > 0:
                await sleep(wait)
            fired = await asyncio.gather(
                *(self._fire(c, aid, "LONG", SIDE_OPEN_LONG, part, clock) for aid, c in self._long),
                *(self._fire(c, aid, "SHORT", SIDE_OPEN_SHORT, part, clock) for aid, c in self._short),
            )
            results.extend(fired)
        return LadderReport(plan=plan, results=results)

    async def _fire(self, client, account_id, side_name, side, part: Slice, clock) -> SliceResult:
        sent = clock()
        try:
            result = await client.submit_order(
                symbol=self._symbol, side=side, vol=float(part.amount),
                leverage=self._leverage, open_type=self._open_type,
                external_oid=f"ld{int(part.send_epoch)}-{account_id}-{part.index}",
            )
            order_id = result.get("orderId") if isinstance(result, dict) else str(result)
            return SliceResult(part.index, account_id, side_name, part.amount, True,
                               sent, (clock() - sent) * 1000, order_id)
        except MexcError as err:
            return SliceResult(part.index, account_id, side_name, part.amount, False,
                               sent, error=err.message or str(err))
        except Exception as err:  # noqa: BLE001 — one order's failure must not stop the ladder
            return SliceResult(part.index, account_id, side_name, part.amount, False,
                               sent, error=f"{type(err).__name__}: {err}")
