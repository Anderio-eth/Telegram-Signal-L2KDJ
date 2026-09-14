"""Open a hedged position on two accounts at a set time, sliced into parts.

The job: at a known moment T, two accounts must be fully in position — one long, one short — at a size
too large to send as a single market order without walking the book and, at high leverage, without
risking liquidation on the entry itself. So the size is split into N equal market orders spaced
`step` apart, fired on both accounts at once, and timed so the LAST slice fills at T. Everything is
set in the bot: which group goes long, leverage, the margin to commit, how many slices, the gap
between them, and T.

The maths, in one place, is `plan_ladder` — pure, so it is tested without a network or a clock:

    size per account   = margin × leverage          (margin is the collateral; e.g. $150 × 1000 = $150k)
    contracts total    = size / price               (HIBT sizes are the base asset)
    contracts per slice= total / N                  (the last slice takes the rounding remainder)
    slice i is sent at = T − latency − (N−1−i)×step  (so slice N−1 fills at T)

A market order fills in the same second it is accepted, ~215 ms after it is sent from here, so
`latency` is that round trip and `step` must not be smaller than it or the slices queue. If the
start time it works out to is already in the past, the plan says so rather than firing late.

`LadderExecutor` runs a plan against two live clients. It is deliberately dumb about failure: a
slice that is refused is recorded and the rest go on unchanged — it never doubles up to catch up and
never unwinds what filled. What opened, and why the rest did not, is the whole report.
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


@dataclass(frozen=True)
class LadderConfig:
    """Everything a scheduled hedged entry needs, as set in the bot."""

    symbol: str                 # BTC_USDT spelling; the client lowercases it
    long_account: int           # account id that goes LONG
    short_account: int          # account id that goes SHORT
    leverage: int
    margin_usd: float           # collateral per account; size = margin × leverage
    parts: int                  # how many slices
    step_seconds: float         # gap between slices
    target_epoch: float         # when the position must be FULLY open (unix seconds)


@dataclass(frozen=True)
class Slice:
    index: int
    send_epoch: float           # when to send this slice (both accounts at once)
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
    available_long: float,
    available_short: float,
    now: float,
) -> LadderPlan:
    """Work out the slices and their send times, or the reasons it cannot be done.

    Pure: every input that depends on the world — price, the accounts' free balance, the measured
    latency, and the current time — is passed in, so the arithmetic can be checked exactly.
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
    if config.long_account == config.short_account:
        errors.append("the long and short accounts must be different")

    for label, available in (("long", available_long), ("short", available_short)):
        if config.margin_usd > available + 1e-9:
            errors.append(
                f"{label} account has ${available:,.2f} free, needs ${config.margin_usd:,.2f} margin"
            )

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
            # Equal slices, with the last one absorbing what flooring dropped so the total is exact
            # and both accounts get the identical schedule.
            remainder = _floor(total - per * (config.parts - 1), size_precision)
            amounts = [per] * (config.parts - 1) + [remainder]
            total_amount = sum(amounts, Decimal(0))
            # slice N-1 fills at T; each earlier one is `step` before the next; latency shifts all
            # of them earlier so the order lands, not just leaves, on time.
            start = config.target_epoch - latency_seconds - (config.parts - 1) * config.step_seconds
            slices = [
                Slice(
                    index=i,
                    send_epoch=start + i * config.step_seconds,
                    amount=format(amounts[i].normalize(), "f"),
                )
                for i in range(config.parts)
            ]

    start_epoch = slices[0].send_epoch if slices else config.target_epoch
    if slices and start_epoch < now:
        late_by = now - start_epoch
        errors.append(
            f"too late: the first slice needed to go at {_clock(start_epoch)} "
            f"({(config.target_epoch - start_epoch):.1f}s before T), which was {late_by:.1f}s ago"
        )
    if config.step_seconds < latency_seconds and config.parts > 1:
        warnings.append(
            f"step {config.step_seconds:.2f}s is under the ~{latency_seconds:.2f}s a request takes; "
            "slices may not keep their spacing"
        )

    return LadderPlan(
        config=config,
        price=price,
        target_notional=target_notional,
        total_amount=format(total_amount.normalize(), "f") if total_amount else "0",
        slices=slices,
        start_epoch=start_epoch,
        errors=errors,
        warnings=warnings,
    )


def _clock(epoch: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(epoch)) + f".{int(epoch % 1 * 1000):03d}"


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
    filled_long: float = 0.0
    filled_short: float = 0.0
    final_long: str | None = None   # what the account holds afterwards, read back
    final_short: str | None = None

    def summary(self) -> str:
        cfg = self.plan.config
        ok = [r for r in self.results if r.ok]
        bad = [r for r in self.results if not r.ok]
        lines = [
            f"{cfg.symbol}  target ${self.plan.target_notional:,.0f}/account at {cfg.leverage}x, "
            f"{cfg.parts} slices",
            f"filled: long {self.filled_long:g}, short {self.filled_short:g} "
            f"(of {self.plan.total_amount} each)",
        ]
        if self.final_long is not None or self.final_short is not None:
            lines.append(f"positions now: long {self.final_long}, short {self.final_short}")
        if ok:
            acks = [r.ack_ms for r in ok if r.ack_ms is not None]
            if acks:
                lines.append(f"{len(ok)} slices sent, {min(acks):.0f}-{max(acks):.0f}ms each")
        for r in bad:
            lines.append(f"  ✗ slice {r.index} {r.side} {r.amount}: {r.error}")
        return "\n".join(lines)


class LadderExecutor:
    """Fires a plan against two live clients. Both sides of a slice go at once; a refused slice is
    recorded and the rest continue."""

    def __init__(self, long_client, short_client, *, symbol: str, leverage: int, open_type: int = 2) -> None:
        self._long = long_client
        self._short = short_client
        self._symbol = symbol
        self._leverage = leverage
        self._open_type = open_type

    async def prepare(self) -> None:
        """Set leverage on both accounts before the clock starts, so no slice pays for it in time."""
        await asyncio.gather(
            self._set_leverage(self._long),
            self._set_leverage(self._short),
            return_exceptions=True,
        )

    async def _set_leverage(self, client) -> None:
        try:
            await client.set_leverage(
                position_id=None, leverage=self._leverage, open_type=self._open_type,
                symbol=self._symbol, position_type=1,
            )
        except MexcError as err:
            LOGGER.warning("could not preset leverage: %s", err.message)

    async def run(self, plan: LadderPlan, *, sleep=asyncio.sleep, clock=time.time) -> LadderReport:
        results: list[SliceResult] = []
        for part in plan.slices:
            wait = part.send_epoch - clock()
            if wait > 0:
                await sleep(wait)
            pair = await asyncio.gather(
                self._fire(self._long, plan.config.long_account, "LONG", SIDE_OPEN_LONG, part, clock),
                self._fire(self._short, plan.config.short_account, "SHORT", SIDE_OPEN_SHORT, part, clock),
            )
            results.extend(pair)

        report = LadderReport(plan=plan, results=results)
        for r in results:
            if r.ok and r.side == "LONG":
                report.filled_long += float(r.amount)
            elif r.ok and r.side == "SHORT":
                report.filled_short += float(r.amount)
        return report

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
        except Exception as err:  # noqa: BLE001 — one slice's failure must not stop the ladder
            return SliceResult(part.index, account_id, side_name, part.amount, False,
                               sent, error=f"{type(err).__name__}: {err}")
