"""Turn a hedge request into two concrete limit orders — pure arithmetic, so it can be tested alone.

Delta-neutral: the same asset, the same USD notional, opposite sides on the two venues. `entropy_long`
picks the direction of the Entropy leg; the Lighter leg takes the opposite. Each leg's size is the
notional divided by that venue's own price (they differ slightly, so sizes differ slightly). Limit
prices cross the mid by a small offset so both legs actually fill; post-only mode places at the mid
instead. Sizes are rounded to each venue's decimals and checked against its minimum.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..exchanges.market_data import EntropyMarket, LighterMarket, lighter_amounts, round_size
from ..pairs import Pair

MIN_NOTIONAL_USD = 10.0  # both venues reject dust; a floor that clears each side's minimum


@dataclass(frozen=True)
class EntropyLeg:
    market: str          # "io:ANTH"
    is_buy: bool
    size: float
    limit_px: float


@dataclass(frozen=True)
class LighterLeg:
    market_index: int
    is_ask: bool         # True = sell/short
    size: float
    limit_px: float
    base_amount: int     # scaled int for the SDK
    price_int: int       # scaled int for the SDK


@dataclass(frozen=True)
class HedgePlan:
    pair: Pair
    notional_usd: float
    entropy: EntropyLeg
    lighter: LighterLeg
    errors: list[str]
    post_only: bool = True   # maker orders that rest and wait (lower fees) vs crossing takers

    @property
    def ok(self) -> bool:
        return not self.errors


def _limit_px(price: float, is_buy: bool, offset_pct: float, maker: bool) -> float:
    """Where to put the limit.

    Taker (maker=False): lean INTO the book so it fills now — above mid to buy, below to sell.
    Maker (maker=True): rest PASSIVELY so it waits for the market and pays the maker fee — below mid
    to buy, above mid to sell (a post-only order that would cross is rejected, so it must sit back)."""
    up = 1 + offset_pct / 100
    down = 1 - offset_pct / 100
    if maker:
        return price * down if is_buy else price * up
    return price * up if is_buy else price * down


def _round_px(price: float) -> float:
    """Round a price to a sane tick by magnitude (~5 significant figures)."""
    if price <= 0:
        return price
    import math
    digits = max(0, min(8, 5 - int(math.floor(math.log10(price))) - 1))
    return round(price, digits)


def plan_hedge(
    pair: Pair,
    notional_usd: float,
    *,
    entropy_long: bool,
    entropy_price: float,
    lighter_price: float,
    entropy_market: EntropyMarket,
    lighter_market: LighterMarket,
    offset_pct: float = 0.05,
    post_only: bool = True,
) -> HedgePlan:
    errors: list[str] = []
    if notional_usd < MIN_NOTIONAL_USD:
        errors.append(f"notional ${notional_usd:g} is below the ${MIN_NOTIONAL_USD:g} minimum")
    if entropy_price <= 0 or lighter_price <= 0:
        errors.append("no price for one of the venues")

    # Sizes: same USD notional per leg, each at its own venue price.
    e_size = round_size(notional_usd / entropy_price, entropy_market.sz_decimals) if entropy_price > 0 else 0.0
    l_size = round_size(notional_usd / lighter_price, lighter_market.size_decimals) if lighter_price > 0 else 0.0

    if lighter_market.min_base and l_size < lighter_market.min_base:
        errors.append(f"Lighter size {l_size:g} < min {lighter_market.min_base:g} — raise the notional")
    if e_size <= 0:
        errors.append("Entropy size rounds to zero — raise the notional")

    # The exchange checks the ACTUAL rounded notional, not the requested one — a size rounded down can
    # fall under the floor even when the request was fine (this is the "Min Trade Ntl Rejected" seen
    # on Entropy). Validate both legs at their own price so the plan fails clearly instead of at order.
    e_notional = e_size * entropy_price
    l_notional = l_size * lighter_price
    if e_size > 0 and e_notional < MIN_NOTIONAL_USD:
        errors.append(f"Entropy notional ${e_notional:.2f} < ${MIN_NOTIONAL_USD:g} after rounding — raise the notional")
    if lighter_market.min_quote and l_notional < lighter_market.min_quote:
        errors.append(f"Lighter notional ${l_notional:.2f} < min ${lighter_market.min_quote:g} — raise the notional")

    e_is_buy = entropy_long
    l_is_ask = entropy_long  # opposite side: if Entropy is long, Lighter is short (ask)

    e_px = _round_px(_limit_px(entropy_price, e_is_buy, offset_pct, post_only))
    # Lighter: `not l_is_ask` is the buy flag, so _limit_px prices for that direction.
    l_px_raw = _limit_px(lighter_price, not l_is_ask, offset_pct, post_only)
    l_base, l_price_int = lighter_amounts(lighter_market, l_size, l_px_raw)

    return HedgePlan(
        pair=pair,
        notional_usd=notional_usd,
        entropy=EntropyLeg(pair.entropy, e_is_buy, e_size, e_px),
        lighter=LighterLeg(lighter_market.market_id, l_is_ask, l_size, l_px_raw, l_base, l_price_int),
        errors=errors,
        post_only=post_only,
    )
