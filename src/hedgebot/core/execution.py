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

Repricing: the resting order is re-quoted when it has sat for REPRICE_S without completing, or as
soon as the market runs away from it by more than the tick offset. Every re-quote first cancels and
re-reads the position, so a fill that lands during the cancel can't make the new order oversized.

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
REPRICE_S = 8.0            # re-quote a resting order that hasn't completed after this long
# Once the order has STARTED filling, keep working it to the full size for up to this long (the
# plain timeout only covers "nothing filled yet"). A big order on a thin io book fills in pieces —
# giving up at the first timeout left a $5k hedge at $2k.
PARTIAL_GRACE_S = 600.0
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


@dataclass
class FillResult:
    e_filled: float = 0.0          # Entropy size that filled (units of the asset)
    l_done: float = 0.0            # Lighter size sent as hedge (units of the asset)
    complete: bool = False         # Entropy reached its target
    error: str | None = None       # a hard failure (order rejected, Lighter hedge failed)
    unhedged: float = 0.0          # Lighter size still owed (below its minimum) — caller must resolve
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
    ) -> FillResult:
        """Work a post-only Entropy order for `e_target` and mirror each fill on Lighter.

        `l_target` is the Lighter size matching the FULL Entropy target (sizes differ slightly because
        each venue's price and rounding differ); fills are mirrored pro rata. `stopping` is an async
        callable -> bool (STOP pressed). Never returns with a resting Entropy order."""
        market = emk.name
        res = FillResult()
        start_szi = await self.entropy_szi(ent, market)
        if start_szi is None:
            res.error = "не вдалось прочитати позицію Entropy"
            return res
        ratio = l_target / e_target if e_target > 0 else 0.0
        deadline = time.monotonic() + max(5.0, float(timeout))
        extended = False
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
                await ent.cancel_all(market)

        try:
            while True:
                szi = await self.entropy_szi(ent, market)
                if szi is not None:
                    # Not capped at the target: a bumped-up last order (see the $10 floor below) can fill
                    # a little past it, and Lighter must mirror what actually filled.
                    progress = abs(szi - start_szi)
                    res.e_filled = progress
                    if progress > 0 and not extended:
                        deadline = max(deadline, time.monotonic() + PARTIAL_GRACE_S)
                        extended = True
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
                ran_away = order_px is not None and (
                    (e_is_buy and bid > order_px + MAKER_TICKS * tick) or
                    (not e_is_buy and ask < order_px - MAKER_TICKS * tick))
                stale = order_px is not None and time.monotonic() - placed_at > REPRICE_S

                if order_px is not None and (ran_away or stale):
                    await cancel_resting()
                    order_px = None
                    continue                     # re-read the position before sizing the new quote

                if order_px is None:
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
