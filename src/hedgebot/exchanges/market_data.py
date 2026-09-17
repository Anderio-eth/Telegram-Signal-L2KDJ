"""Public market metadata and prices from both venues.

The bot needs, per asset: how to scale an order's size/price for each venue, the minimum order, and a
reference price to (a) size both legs to the same USD notional and (b) place limit orders at a sane
offset. None of this needs credentials, so it lives here and is fetched over plain HTTP.

Endpoints verified 2026-09-17:
  Hyperliquid  POST /info {"type":"meta","dex":"io"}              -> universe (szDecimals, maxLeverage)
               POST /info {"type":"metaAndAssetCtxs","dex":"io"}  -> mark prices, index-aligned
  Lighter      GET  /api/v1/orderBooks                            -> market_id, size/price decimals, mins
"""

from __future__ import annotations

from dataclasses import dataclass

import aiohttp


@dataclass(frozen=True)
class EntropyMarket:
    name: str            # "io:ANTH"
    sz_decimals: int
    max_leverage: int


@dataclass(frozen=True)
class LighterMarket:
    symbol: str          # "ANTHROPIC"
    market_id: int
    size_decimals: int
    price_decimals: int
    min_base: float
    min_quote: float


async def _post(session: aiohttp.ClientSession, url: str, payload: dict):
    async with session.post(f"{url}/info", json=payload) as r:
        return await r.json()


async def _get(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as r:
        return await r.json()


# ── Entropy (Hyperliquid io builder) ─────────────────────────────────────────────────────────────
async def entropy_markets(session, api_url: str, dex: str = "io") -> dict[str, EntropyMarket]:
    meta = await _post(session, api_url, {"type": "meta", "dex": dex})
    out: dict[str, EntropyMarket] = {}
    for u in meta.get("universe", []):
        out[u["name"]] = EntropyMarket(u["name"], int(u.get("szDecimals", 0)), int(u.get("maxLeverage", 1)))
    return out


async def entropy_marks(session, api_url: str, dex: str = "io") -> dict[str, float]:
    """Mark price per io market. metaAndAssetCtxs returns [meta, ctxs] with ctxs index-aligned to
    meta.universe, so the mark is matched to the market by position."""
    meta, ctxs = await _post(session, api_url, {"type": "metaAndAssetCtxs", "dex": dex})
    names = [u["name"] for u in meta.get("universe", [])]
    out: dict[str, float] = {}
    for name, ctx in zip(names, ctxs):
        px = ctx.get("markPx") or ctx.get("midPx") or ctx.get("oraclePx")
        if px is not None:
            out[name] = float(px)
    return out


# ── Lighter ──────────────────────────────────────────────────────────────────────────────────────
async def lighter_markets(session, api_url: str) -> dict[str, LighterMarket]:
    data = await _get(session, f"{api_url}/api/v1/orderBooks")
    out: dict[str, LighterMarket] = {}
    for o in data.get("order_books", []):
        if o.get("market_type") != "perp" or o.get("status") != "active":
            continue
        out[o["symbol"]] = LighterMarket(
            symbol=o["symbol"],
            market_id=int(o["market_id"]),
            size_decimals=int(o.get("supported_size_decimals", 0)),
            price_decimals=int(o.get("supported_price_decimals", 0)),
            min_base=float(o.get("min_base_amount", 0) or 0),
            min_quote=float(o.get("min_quote_amount", 0) or 0),
        )
    return out


async def lighter_mark(session, api_url: str, market_id: int) -> float | None:
    """Mid price from the top of Lighter's book for one market. Endpoint field names are confirmed at
    integration time; wrapped so a miss returns None rather than breaking a size calc."""
    try:
        data = await _get(session, f"{api_url}/api/v1/orderBookOrders?market_id={market_id}&limit=1")
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if bids and asks:
            return (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
    except Exception:  # noqa: BLE001 — a missing price must not crash sizing; caller handles None
        return None
    return None


# ── scaling helpers ──────────────────────────────────────────────────────────────────────────────
def lighter_amounts(m: LighterMarket, size: float, price: float) -> tuple[int, int]:
    """Lighter's create_order takes base_amount and price as INTEGERS scaled by the market's decimals."""
    return round(size * 10 ** m.size_decimals), round(price * 10 ** m.price_decimals)


def round_size(size: float, decimals: int) -> float:
    return round(size, decimals)
