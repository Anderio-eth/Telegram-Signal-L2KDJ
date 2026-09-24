"""Maker-first hedge execution: Entropy post-only limit first, Lighter market as it fills.

Why this order: Entropy charges a taker fee and RH-Lighter doesn't, so the Entropy leg rests as a
MAKER (post-only / ALO, a couple of ticks off the mid) and only once it actually fills do we hit
Lighter at market for the matching size. Posting both at once as takers paid Entropy's taker fee on
every open and close.

The same routine runs both ways:
  • open  — Entropy grows a position; each filled chunk is hedged on Lighter (opposite side).
  • close — Entropy reduce-only limit shrinks the position; each chunk is closed on Lighter
            reduce-only. The caller then flattens whatever is left at market, so a close always
            finishes even if the maker order never fills.

Chunking: RH-Lighter rejects orders under $10 (min_quote) or min_base, so a partial Entropy fill is
only hedged once the chunk clears those minimums AND leaves either nothing or another valid chunk
behind. Once Entropy is complete the residual is hedged whatever its size (it is then the whole
remainder, which the plan already sized above the minimums).

Repricing is purely on the clock (user's rule): the resting order sits for `reprice_s` — whatever the
user configured — and is only then cancelled and re-quoted, even if the market has walked away from
it in the meantime. Every re-quote first cancels and re-reads the position, so a fill that lands
during the cancel can't make the new order oversized, and the new order is sized to what is left.

Fill detection reads the Entropy POSITION (not order status): it's the ground truth for delta, and it
is the same read on open and close.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass, field

import aiohttp

from ..exchanges import market_data as md
from ..exchanges.market_data import EntropyMarket, LighterMarket

LOGGER = logging.getLogger(__name__)

MAKER_TICKS = 2            # how far off the mid the Entropy limit rests (user: "2-3 мінімальні кроки")
REPRICE_S = 8.0            # default seconds a resting order is left to work; per-session overridable
# Once the order has STARTED filling, keep working it to the full size for up to this long (the
# plain timeout only covers "nothing filled yet"). A big order on a thin io book fills in pieces —
# giving up at the first timeout left a $5k hedge at $2k. Applies only WITHOUT until_complete, i.e.
# to closes and to manual opens.
PARTIAL_GRACE_S = 600.0
PROGRESS_PING_S = 300.0    # how often to report "still filling" while a long fill works
POLL_S = 0.4               # position poll cadence while the maker order works
LIGHTER_SLIPPAGE = 0.01    # IOC limit 1% through the mid: fills at the book, the cap is only a guard
ENTROPY_MIN_NOTIONAL = 10.0
_EPS = 1e-12


def entropy_tick(price: float, sz_decimals: int) -> float:
    """Hyperliquid's price step: at most 5 significant figures AND at most (6 - szDecimals) decimals,
    whichever is coarser. io:ANTH at ~2155 with szDecimals 3 -> 0.1."""
    if price <= 0:
        return 0.0
    by_sig = 10 ** (math.floor(math.log10(price)) - 4)
    by_dec = 10 ** -(6 - sz_decimals)
    return max(by_sig, by_dec)


def _tick_decimals(tick: float) -> int:
    return max(0, -int(math.floor(math.log10(tick)))) if tick < 1 else 0


def maker_price(best_bid: float, best_ask: float, is_buy: bool, tick: float, ticks: int = MAKER_TICKS) -> float:
    """A post-only price `ticks` steps off the mid, on the passive side, never crossing the book.

    Buy: mid - ticks, rounded DOWN to the tick, and at most one tick under the best ask (so ALO
    can't be rejected for crossing). Sell mirrors it. On a wide spread this improves on the best
    bid/ask (better fill odds); on a one-tick spread it sits a tick or two behind it."""
    mid = (best_bid + best_ask) / 2
    if is_buy:
        # round() before floor/ceil: 1684.9 / 0.1 is 16848.999… in floats and would drop a whole tick.
        px = math.floor(round((mid - ticks * tick) / tick, 6)) * tick
        px = min(px, best_ask - tick)
    else:
        px = math.ceil(round((mid + ticks * tick) / tick, 6)) * tick
        px = max(px, best_bid + tick)
    return round(px, _tick_decimals(tick))


def _floor(x: float, decimals: int) -> float:
    f = 10 ** decimals
    return math.floor(x * f + 1e-9) / f


def split_sizes(e_total: float, l_total: float, emk: EntropyMarket, lmk: LighterMarket,
                e_px: float, l_px: float, parts: int) -> list[tuple[float, float]]:
    """Cut a planned hedge into `parts` (entropy_size, lighter_size) chunks, largest-legal-first.

    Splitting exists to stop one big market order from walking Lighter's book: each chunk is filled
    on Entropy as a maker order and mirrored on Lighter straight away, so the taker side arrives in
    slices instead of all at once.

    `parts` is a ceiling, not a promise. Both venues reject dust (Entropy under $10, Lighter under
    its min_base/min_quote), so asking for more parts than the size can carry would produce chunks
    that no venue accepts and an open that never completes -- the count is lowered instead, and the
    caller tells the user. Rounding remainders all land on the LAST chunk so the parts still sum to
    the full planned size.
    """
    parts = max(1, int(parts))
    if e_total <= 0 or l_total <= 0:
        return [(e_total, l_total)]
    # The smallest chunk each venue will take, with headroom so a price tick can't push it under.
    min_e = (ENTROPY_MIN_NOTIONAL * 1.15 / e_px) if e_px > 0 else 0.0
    min_l = max(lmk.min_base, ((lmk.min_quote or 0) * 1.15 / l_px) if l_px > 0 else 0.0)
    if min_e > 0:
        parts = min(parts, max(1, int(e_total // min_e)))
    if min_l > 0:
        parts = min(parts, max(1, int(l_total // min_l)))
    if parts <= 1:
        return [(e_total, l_total)]
    e_step = _floor(e_total / parts, emk.sz_decimals)
    l_step = _floor(l_total / parts, lmk.size_decimals)
    if e_step <= 0 or l_step <= 0:
        return [(e_total, l_total)]
    out = [(e_step, l_step) for _ in range(parts - 1)]
    out.append((round(e_total - e_step * (parts - 1), emk.sz_decimals + 2),
                round(l_total - l_step * (parts - 1), lmk.size_decimals + 2)))
    return out


@dataclass
class FillResult:
    e_filled: float = 0.0          # Entropy size that filled (units of the asset)
    l_done: float = 0.0            # Lighter size sent as hedge (units of the asset)
    complete: bool = False         # Entropy reached its target
    error: str | None = None       # a hard failure (order rejected, Lighter hedge failed)
    unhedged: float = 0.0          # Lighter size still owed (below its minimum) — caller must resolve
    e_is_buy: bool | None = None   # the side actually used (side_check may have flipped it)
    notes: list[str] = field(default_factory=list)


class MakerExecutor:
    def __init__(self, cfg) -> None:
        self._cfg = cfg
        self._http: aiohttp.ClientSession | None = None

    async def http(self) -> aiohttp.ClientSession:
        # One pooled session for all book reads — a fresh TLS handshake per poll was the slow part.
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6, connect=4))
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.closed:
            await self._http.close()

    async def entropy_book(self, coin: str) -> tuple[float, float] | None:
        """(best_bid, best_ask) for an io market from Hyperliquid's public l2Book."""
        try:
            s = await self.http()
            async with s.post(f"{self._cfg.hyperliquid_api_url}/info", json={"type": "l2Book", "coin": coin}) as r:
                data = await r.json()
            bids, asks = data["levels"][0], data["levels"][1]
            if bids and asks:
                return float(bids[0]["px"]), float(asks[0]["px"])
        except Exception:  # noqa: BLE001 — a missed read just skips one re-quote
            LOGGER.debug("entropy l2Book read failed", exc_info=True)
        return None

    async def lighter_mid(self, market_id: int) -> float | None:
        try:
            return await md.lighter_mark(await self.http(), self._cfg.lighter_api_url, market_id)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    async def entropy_szi(ent, market: str) -> float | None:
        """Signed Entropy position size, 0.0 when flat, None when the read FAILED (never confuse a
        failed read with 'flat' — that would look like a full close/fill)."""
        try:
            p = await ent.position(market)
        except Exception:  # noqa: BLE001
            return None
        return float(p.get("szi", 0) or 0) if p else 0.0

    async def lighter_market_order(self, lit, lmk: LighterMarket, size: float, is_ask: bool,
                                   *, reduce_only: bool) -> str | None:
        """Hit Lighter at market: an IOC limit LIGHTER_SLIPPAGE through the mid. Returns an error
        string, or None if accepted."""
        mid = await self.lighter_mid(lmk.market_id)
        if not mid:
            return "немає ціни Lighter"
        px = mid * (1 - LIGHTER_SLIPPAGE if is_ask else 1 + LIGHTER_SLIPPAGE)
        base, price_int = md.lighter_amounts(lmk, size, px)
        if base <= 0:
            return None
        last = None
        for attempt in range(2):
            try:
                resp = await lit.limit_order(lmk.market_id, base, price_int, is_ask, reduce_only=reduce_only, ioc=True)
                last = lit.order_error(resp)
            except Exception as e:  # noqa: BLE001
                last = str(e)[:200]
            if not last:
                return None
            if "nonce" in last.lower() and attempt == 0:
                await asyncio.sleep(0.6)   # optimistic nonce manager resyncs on the retry
                continue
            break
        return last

    def _chunk_ok(self, lmk: LighterMarket, size: float, px: float) -> bool:
        return size >= max(lmk.min_base, _EPS) and size * px >= (lmk.min_quote or 0)

    async def fill(
        self,
        ent,
        lit,
        emk: EntropyMarket,
        lmk: LighterMarket,
        *,
        e_is_buy: bool,
        e_target: float,
        l_is_ask: bool,
        l_target: float,
        closing: bool,
        timeout: float,
        stopping=None,
        until_complete: bool = False,
        on_progress=None,
        reprice_s: float | None = None,
        side_check=None,
    ) -> FillResult:
        """Work a post-only Entropy order for `e_target` and mirror each fill on Lighter.

        `l_target` is the Lighter size matching the FULL Entropy target (sizes differ slightly because
        each venue's price and rounding differ); fills are mirrored pro rata. `stopping` is an async
        callable -> bool (STOP pressed). Never returns with a resting Entropy order.

        `timeout` applies only when `until_complete` is False (closes, manual opens): it bounds the
        wait while nothing has filled, and PARTIAL_GRACE_S extends it once something has.

        With `until_complete=True` there is NO deadline of any kind. The quote is re-priced to within
        MAKER_TICKS of the CURRENT price every `reprice_s`, so an unfilled order costs nothing to
        leave standing, and giving up on it would only abandon a hedge that the next re-quote was
        about to fill. Only STOP ends that loop.
        `on_progress(filled, target)` is called about every PROGRESS_PING_S during a long fill.
        `reprice_s` is how long one quote is left to work before it is cancelled and re-quoted
        (defaults to REPRICE_S).

        `side_check` is an async callable -> bool|None consulted before every quote while NOTHING has
        filled yet: it returns which side Entropy should take right now. Nothing is on the book yet at
        that point, so the direction is still free to change — and over a long unfilled wait the
        cross-venue gap can invert, which would otherwise leave the hedge opened on the wrong side of
        it. Once the first lot fills the side is locked, and the side actually used comes back on the
        result."""
        market = emk.name
        hold_s = max(1.0, float(reprice_s if reprice_s is not None else REPRICE_S))
        res = FillResult()
        res.e_is_buy = e_is_buy
        start_szi = await self.entropy_szi(ent, market)
        if start_szi is None:
            res.error = "не вдалось прочитати позицію Entropy"
            return res
        ratio = l_target / e_target if e_target > 0 else 0.0
        # No deadline when the caller wants the whole size: see the docstring. A close never passes
        # until_complete, so it keeps its timeout and its caller's market flatten.
        deadline = math.inf if until_complete else time.monotonic() + max(5.0, float(timeout))
        extended = False
        last_ping = time.monotonic()
        order_px: float | None = None
        placed_at = 0.0
        tick = 0.0
        progress = 0.0

        async def hedge_owed(final: bool) -> bool:
            """Send the Lighter size owed for fills so far. False on a hard Lighter failure."""
            if final and closing:
                return True      # a close's Lighter remainder is flattened whole by the caller's market pass
            owed = _floor(progress * ratio - res.l_done, lmk.size_decimals)
            if owed <= 0:
                return True
            lpx = await self.lighter_mid(lmk.market_id) or 0.0
            left_after = _floor(l_target - res.l_done - owed, lmk.size_decimals)
            send = final or (self._chunk_ok(lmk, owed, lpx) and (left_after <= 0 or self._chunk_ok(lmk, left_after, lpx)))
            if not send:
                return True
            if not closing and not self._chunk_ok(lmk, owed, lpx):
                res.unhedged = owed          # below Lighter's minimum — the caller unwinds it
                return True
            err = await self.lighter_market_order(lit, lmk, owed, l_is_ask, reduce_only=closing)
            if err:
                res.error = f"Lighter: {err}"
                return False
            res.l_done += owed
            return True

        async def cancel_resting() -> None:
            with contextlib.suppress(Exception):
                await ent.cancel_all(market, include_triggers=False)   # keep the stop-loss

        try:
            while True:
                szi = await self.entropy_szi(ent, market)
                if szi is not None:
                    # Not capped at the target: a bumped-up last order (see the $10 floor below) can fill
                    # a little past it, and Lighter must mirror what actually filled.
                    progress = abs(szi - start_szi)
                    res.e_filled = progress
                    if progress > 0 and not extended and not until_complete:
                        # First fill: stop counting the "nothing happened" timeout.
                        deadline = max(deadline, time.monotonic() + PARTIAL_GRACE_S)
                        extended = True
                    # Report even at 0%: with no deadline, silence is indistinguishable from a hang,
                    # and "what is it waiting for" is exactly the question an unbounded wait raises.
                    if until_complete and on_progress and time.monotonic() - last_ping >= PROGRESS_PING_S:
                        last_ping = time.monotonic()
                        with contextlib.suppress(Exception):
                            await on_progress(progress, e_target)
                    if not await hedge_owed(final=False):
                        return res
                    if progress >= e_target * 0.999:
                        await cancel_resting()
                        res.complete = True
                        await hedge_owed(final=True)
                        return res

                if (stopping and await stopping()) or time.monotonic() > deadline:
                    await cancel_resting()
                    await asyncio.sleep(0.3)
                    szi = await self.entropy_szi(ent, market)
                    if szi is not None:
                        progress = abs(szi - start_szi)
                        res.e_filled = progress
                    res.notes.append("лімітка не заповнилась вчасно" if progress <= 0 else "лімітка заповнилась частково")
                    await hedge_owed(final=True)
                    return res

                book = await self.entropy_book(market)
                if book is None:
                    await asyncio.sleep(POLL_S)
                    continue
                bid, ask = book
                tick = tick or entropy_tick((bid + ask) / 2, emk.sz_decimals)
                # Strictly on the clock: a quote gets its full `hold_s` even if the price has walked
                # away from it. The user set this rule deliberately — re-quoting on every wiggle made
                # the order jump around far more often than the interval they asked for.
                if order_px is not None and time.monotonic() - placed_at >= hold_s:
                    await cancel_resting()
                    order_px = None
                    continue                     # re-read the position before sizing the new quote

                if order_px is None:
                    if side_check is not None and progress <= 0:
                        # Still flat, so the direction is still ours to choose.
                        want = await side_check()
                        if want is not None and want != e_is_buy:
                            e_is_buy = want
                            l_is_ask = want        # hedge.py: Lighter is the opposite leg
                            res.e_is_buy = e_is_buy
                            res.notes.append("напрямок перевернуто: ціни помінялись місцями")
                            LOGGER.info("%s: flipping side before the first fill -> entropy %s",
                                        market, "LONG" if e_is_buy else "SHORT")
                    remaining = _floor(e_target - progress, emk.sz_decimals)
                    mid = (bid + ask) / 2
                    if remaining <= 0:
                        res.complete = True
                        await hedge_owed(final=True)
                        return res
                    if remaining * mid < ENTROPY_MIN_NOTIONAL:
                        if closing:
                            # A reduce-only order can't exceed the position; the caller's market pass
                            # closes this small remainder.
                            res.notes.append("залишок менший за мінімум Entropy")
                            return res
                        # Opening: Entropy won't take an order under $10, so a $4-of-$12 partial could
                        # never be completed. Quote the $10 minimum instead — the hedge ends a few
                        # dollars larger than planned, and Lighter mirrors the actual fill, so delta
                        # stays flat.
                        remaining = math.ceil(ENTROPY_MIN_NOTIONAL * 1.01 / mid * 10 ** emk.sz_decimals) / 10 ** emk.sz_decimals
                    px = maker_price(bid, ask, e_is_buy, tick)
                    try:
                        resp = await ent.limit_order(market, e_is_buy, remaining, px, post_only=True, reduce_only=closing)
                        err = ent.order_error(resp)
                    except Exception as e:  # noqa: BLE001
                        err = str(e)[:200]
                    if err:
                        low = err.lower()
                        if "post only" in low or "immediately match" in low or "alo" in low:
                            await asyncio.sleep(POLL_S)   # book moved under us — re-quote next pass
                            continue
                        res.error = f"Entropy: {err}"
                        return res
                    order_px, placed_at = px, time.monotonic()

                await asyncio.sleep(POLL_S)
        finally:
            if not res.complete:
                await cancel_resting()
