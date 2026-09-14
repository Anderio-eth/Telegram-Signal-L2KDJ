"""HIBT perpetual futures REST client — the same interface as MexcRestClient, for a second venue.

Everything the rest of the bot does is written against MEXC's shapes: positions arrive as rows with
positionId / positionType / holdVol / state, sides are MEXC's 1-4, symbols read BTC_USDT. Rather than
teach the tracker, the group poller, dedupe and the reports a second vocabulary, this client
translates at its own boundary. What comes out of it looks exactly like what comes out of the MEXC
client, so everything above it works unchanged — and none of that tested logic had to be touched.

Endpoints and field names are from HIBT's OpenAPI docs (apidoc.hibt.co), cross-checked against the
live public API. The PRIVATE side has not yet been exercised with a real key. Every assumption that
could only be settled by one is marked UNVERIFIED below, so the first live session knows exactly
where to look.

What was measured rather than read:

  · The trading host answers from a Render datacentre IP — REST reached the API (220003, "no key")
    rather than being turned away at the edge, which is what killed MEXC's socket there.
  · Cloudflare in front of it bans clients by User-Agent NAME, not by TLS fingerprint: a request
    calling itself "Python-urllib/3.13" gets 403 "error code: 1010"; curl, a browser string or no
    User-Agent at all pass. aiohttp sends none today, but that is a library default, not a promise,
    so this client always sends its own.
  · Errors come back with HTTP 200 and the failure in the body's `code`. Anything that trusts the
    status line reads a refusal as a success.
  · Symbols are lowercase with an underscore (xag_usdt). The website's own URL says XAG-USDT, and
    the API answers that form with "param error".
  · Order sizes are in units of the base asset, not contracts: the minimum ETH order is 0.18.

That last point is why a folder has ONE exchange. The copy engine mirrors raw size — master volume
times a multiplier — which is correct between two MEXC accounts and catastrophic between venues:
100 MEXC BTC contracts are 0.01 BTC, and the same 100 sent here would be 100 BTC. Keeping every
account of a folder on the same venue makes that mix impossible rather than merely unlikely.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any

import aiohttp

from ..mexc.rest import (
    ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET,
    ORDER_TYPE_POST_ONLY,
    RATE_LIMIT_BACKOFF,
    SIDE_CLOSE_LONG,
    SIDE_CLOSE_SHORT,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    AccountBalance,
    ClosedPosition,
    ContractSpec,
    MexcError,
    Position,
    PositionStops,
    _Throttle,
)
from .auth import now_ms, signed_headers

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://fapi.hibt0.com/open-api"

# Anything that does not name a client library. See the module docstring: the edge bans by name.
USER_AGENT = "Mozilla/5.0 (compatible; copy-bot/1.0)"

# HIBT's code for "Too many requests" (from the docs' error table). Treated like MEXC's 510.
RATE_LIMITED_CODE = 220017

# Signature and timestamp refusals. Retrying cannot fix a wrong signature, but naming it matters: it
# is the single most likely failure on the first live call, and it looks nothing like a bad key.
SIGNATURE_FAILED_CODE = 220008
TIMESTAMP_EXPIRED_CODE = 220002

# Venue enums. HIBT keeps opening and closing apart — /order/open and /order/close are different
# endpoints — so a side is only ever a direction: 1 buy, 2 sell. The MEXC trap where side=3 meant
# "open short" while the docs said "close long" has no counterpart here.
VENUE_SIDE_BUY = 1
VENUE_SIDE_SELL = 2
VENUE_TYPE_LIMIT = 1
VENUE_TYPE_MARKET = 2

# HIBT order state -> MEXC order state (see core/orders.py), so resting/filled/cancelled mean the
# same thing to the order tracker whichever venue the row came from.
#   HIBT: 1 active, 2 filled, 3 cancelled, 4 partially filled, 5 partial then cancelled, 6 cancelling
#   MEXC: 2 resting, 3 filled, 4 cancelled
_ORDER_STATE = {1: 2, 4: 2, 6: 2, 2: 3, 3: 4, 5: 4}

# Pacing. HIBT does not publish its private limits (only the spot ones), so these are MEXC's
# measured figures reused as a conservative starting point — NOT measurements of this venue.
# UNVERIFIED: measure on a live key before trusting them with ten accounts at once.
PRIVATE_INTERVAL = 0.14
PRIVATE_CONCURRENCY = 3
_ACCOUNT_THROTTLES: dict[str, _Throttle] = {}
PUBLIC_THROTTLE = _Throttle(max_concurrent=6, min_interval=0.03)

# Contract rules change rarely; asking on every order would cost a round trip per follower per trade.
SYMBOLS_TTL_SECONDS = 600.0
_symbols_cache: dict[str, dict[str, Any]] = {}
_symbols_loaded_at = 0.0
_symbols_lock = asyncio.Lock()


class HibtError(MexcError):
    """A refused HIBT call.

    A MexcError on purpose: every `except MexcError` in the engine, the poller and the menu already
    knows what to do with a venue refusal, and a second exception type would have to be taught to
    each of them — or, where one was missed, crash an account out of a trade.
    """

    @property
    def is_rate_limited(self) -> bool:
        if self.code in (RATE_LIMITED_CODE, 429):
            return True
        lowered = (self.message or "").lower()
        return any(phrase in lowered for phrase in ("too frequent", "rate limit", "too many requests"))


def _throttle_for(api_key: str) -> _Throttle:
    """One queue per account, keyed by a hash of the key so no dictionary of live keys exists."""
    import hashlib

    fingerprint = hashlib.sha256(api_key.encode()).hexdigest()[:16]
    throttle = _ACCOUNT_THROTTLES.get(fingerprint)
    if throttle is None:
        throttle = _Throttle(max_concurrent=PRIVATE_CONCURRENCY, min_interval=PRIVATE_INTERVAL)
        _ACCOUNT_THROTTLES[fingerprint] = throttle
    return throttle


# ── translation ──────────────────────────────────────────────────────────────────────────────────
def to_venue(symbol: str) -> str:
    """BTC_USDT -> btc_usdt. The bot's own spelling is MEXC's, and this venue's is lowercase."""
    return symbol.strip().lower()


def from_venue(symbol: str) -> str:
    """btc_usdt -> BTC_USDT, so a HIBT position reads the same in a report as a MEXC one."""
    return str(symbol or "").strip().upper()


def _num(value: Any, default: float = 0.0) -> float:
    """HIBT sends numbers as strings, and an unset one as "" — which float() refuses."""
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def _position_id(value: Any) -> int:
    """HIBT ids are 29-digit strings. Python ints hold them exactly, and nothing stores a position id
    in a database column, so converting keeps `Position.position_id: int` true for both venues."""
    return _int(value, 0)


def _opt_price(value: Any) -> float | None:
    price = _num(value, 0.0)
    return price or None


def position_row(raw: dict[str, Any]) -> dict[str, Any]:
    """One HIBT position, in the shape of a MEXC position row.

    That is the shape `parse_position` reads, which is what feeds the tracker, dedupe and the group
    poller. `version` is left out on purpose: HIBT has none, and the dedupe key falls back to
    size-plus-action when it is absent — which is exactly what that fallback exists for.
    """
    amount = _num(raw.get("amount"))
    side = _int(raw.get("side"))
    return {
        "positionId": _position_id(raw.get("positionID")),
        "symbol": from_venue(raw.get("symbol")),
        # 1 buy holds a long, 2 sell holds a short — MEXC's positionType numbers are the same pair.
        "positionType": 1 if side == VENUE_SIDE_BUY else 2 if side == VENUE_SIDE_SELL else 0,
        "holdVol": amount,
        "openAvgPrice": _num(raw.get("price")),
        "leverage": _int(raw.get("leverage")),
        # UNVERIFIED: the position carries no margin mode, and the API has no way to set one. It is
        # chosen in the HIBT app. 2 (cross) is only a placeholder MEXC calls understand; nothing on
        # this venue reads it.
        "openType": 2,
        "state": 1 if amount > 0 else 3,
        # What _money_detail reads as the position's initial margin.
        "im": _num(raw.get("margin")),
        "unrealised": _num(raw.get("openProfit")),
        "stopLossPrice": _opt_price(raw.get("slPrice")),
        "takeProfitPrice": _opt_price(raw.get("spPrice")),
        "updateTime": _int(raw.get("updatedAt")),
    }


def order_row(raw: dict[str, Any]) -> dict[str, Any]:
    """One HIBT order, in the shape of a MEXC order row.

    UNVERIFIED: a HIBT order row says buy or sell, but not whether it opens or closes — that lives in
    `action`, whose values the docs do not list. So `side` here is mapped as an OPENING side. That is
    safe for the two things that read these rows on this venue — recovering an order id by its
    customID, and cancelling — and is exactly why mirroring a master's resting limits is switched
    off for HIBT folders: a closing limit read as an opening one would be copied the wrong way.
    """
    side = _int(raw.get("side"))
    return {
        "orderId": str(raw.get("id") or ""),
        "externalOid": raw.get("customID") or None,
        "symbol": from_venue(raw.get("symbol")),
        "side": SIDE_OPEN_LONG if side == VENUE_SIDE_BUY else SIDE_OPEN_SHORT,
        "orderType": ORDER_TYPE_LIMIT if _int(raw.get("type")) == VENUE_TYPE_LIMIT else ORDER_TYPE_MARKET,
        "price": _num(raw.get("price")),
        "vol": _num(raw.get("amount")),
        "dealVol": _num(raw.get("filledAmount")),
        "leverage": _int(raw.get("leverage")),
        "openType": 2,
        "state": _ORDER_STATE.get(_int(raw.get("state")), 0),
        "positionId": _position_id(raw.get("positionID")),
        "action": raw.get("action"),
        "profit": _num(raw.get("profit")),
        "fee": _num(raw.get("fee")),
        "updateTime": _int(raw.get("updatedAt")),
    }


def closed_rows(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Finished positions, rebuilt from finished orders.

    HIBT has no position-history endpoint; realised PnL is on each ORDER, keyed by positionID. So
    the orders of one position are summed into one row shaped like MEXC's settled position —
    holdVol 0, state 3, realised — which is the shape the poller turns into a close event.

    `realised` is profit minus fees, to match MEXC, whose figure is net: it is what actually left or
    reached the balance. UNVERIFIED whether HIBT's `profit` already has the fee taken out; if the
    bot's PnL disagrees with the app by roughly the trading fee, that subtraction is the cause.
    """
    grouped: dict[int, dict[str, Any]] = {}
    for raw in orders:
        # The trade history names it positionId; the order endpoints positionID.
        position_id = _position_id(raw.get("positionID") or raw.get("positionId"))
        if not position_id:
            continue
        row = grouped.setdefault(
            position_id,
            {
                "positionId": position_id,
                "symbol": from_venue(raw.get("symbol")),
                "positionType": 0,
                "holdVol": 0.0,
                "state": 3,
                "realisedGross": 0.0,
                "fee": 0.0,
                "closeVol": 0.0,
                "updateTime": 0,
            },
        )
        profit = _num(raw.get("profit"))
        row["realisedGross"] += profit
        row["fee"] += _num(raw.get("fee"))
        if profit:
            # Only a closing fill realises anything, so a non-zero profit marks the order that
            # closed. Its filled amount is how much of the position was closed.
            row["closeVol"] += _num(raw.get("filledAmount"))
        row["updateTime"] = max(row["updateTime"], _int(raw.get("updatedAt")))
        if not row["positionType"]:
            # The order that opened a long was a buy. A closing order faces the other way, so the
            # first order seen is not guaranteed to be the opening one — but a position is never
            # both, and any row that names a side settles it for display.
            side = _int(raw.get("side"))
            row["positionType"] = 1 if side == VENUE_SIDE_BUY else 2 if side == VENUE_SIDE_SELL else 0

    for row in grouped.values():
        row["realised"] = row["realisedGross"] - row["fee"]
    return sorted(grouped.values(), key=lambda r: r["updateTime"], reverse=True)


def floor_amount(vol: float, precision: int) -> Decimal:
    """Round a size DOWN to what the venue accepts.

    Down, never to nearest: rounding up would send a follower a larger position than the multiplier
    says, and the copy engine never sizes above what it was asked for. A multiplier of 1.5 on 0.18
    ETH is 0.27, which fits two decimals; one of 1.33 is 0.2394, which does not, and would otherwise
    be refused outright as a parameter error.
    """
    try:
        value = Decimal(repr(float(vol)))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)
    step = Decimal(1).scaleb(-max(0, int(precision)))
    return value.quantize(step, rounding=ROUND_DOWN)


def size_precision(rules: dict[str, Any]) -> int:
    """How many decimals a size may have on a contract.

    The venue's own figures disagree with each other on some contracts. Measured 2026-09-14:
    cl_usdt reports volumePrecision 0 alongside a minimum order of 0.01 — flooring to whole units
    turned the smallest order the venue advertises into zero. The minimum is the more specific of
    the two claims, so a size is never floored more coarsely than the minimum itself is written.
    """
    declared = _int(rules.get("volumePrecision"), 0)
    minimum = str(rules.get("marketMiniAmount") or rules.get("limitMiniAmount") or "")
    implied = len(minimum.split(".", 1)[1].rstrip("0")) if "." in minimum else 0
    return max(declared, implied)


def _rows(data: Any) -> list[dict[str, Any]]:
    """A list endpoint's rows. HIBT returns some lists bare and pages others as {total, page, data}."""
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("data", "list", "rows"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [row for row in inner if isinstance(row, dict)]
    return []


# ── public market data (no auth) ─────────────────────────────────────────────────────────────────
async def _public_get(session: aiohttp.ClientSession, path: str, params: dict[str, Any] | None = None) -> Any:
    async with PUBLIC_THROTTLE.slot():
        async with session.get(
            f"{BASE_URL}{path}", params=params, headers={"User-Agent": USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            text = await response.text()
            try:
                payload = await response.json(content_type=None)
            except (ValueError, aiohttp.ContentTypeError):
                raise HibtError(response.status, f"not JSON: {text[:120]}", endpoint=path) from None
    if not isinstance(payload, dict) or payload.get("code") != 0:
        code = payload.get("code") if isinstance(payload, dict) else None
        msg = payload.get("msg") if isinstance(payload, dict) else str(payload)[:120]
        raise HibtError(code, str(msg), endpoint=path)
    return payload.get("data")


async def load_symbols(session: aiohttp.ClientSession, *, force: bool = False) -> dict[str, dict[str, Any]]:
    """Every contract's trading rules, keyed by the bot's spelling (BTC_USDT). Cached."""
    global _symbols_loaded_at
    fresh = time.monotonic() - _symbols_loaded_at < SYMBOLS_TTL_SECONDS
    if _symbols_cache and fresh and not force:
        return _symbols_cache
    async with _symbols_lock:
        if _symbols_cache and time.monotonic() - _symbols_loaded_at < SYMBOLS_TTL_SECONDS and not force:
            return _symbols_cache
        rows = _rows(await _public_get(session, "/v2/market/symbols"))
        _symbols_cache.clear()
        for row in rows:
            if row.get("symbol"):
                _symbols_cache[from_venue(row["symbol"])] = row
        _symbols_loaded_at = time.monotonic()
    return _symbols_cache


def _spec_from(symbol: str, row: dict[str, Any]) -> ContractSpec:
    return ContractSpec(
        symbol=symbol,
        # Sizes are already in the base asset, so one "contract" is one unit of it.
        contract_size=1.0,
        # The bot opens with market orders, so the market minimum is the one that binds.
        min_vol=_num(row.get("marketMiniAmount"), 0.0),
        vol_scale=_int(row.get("volumePrecision"), 0),
        max_leverage=0,  # not published on this venue
        api_allowed=bool(row.get("supportTrade", True)),
    )


async def get_contract_specs(session: aiohttp.ClientSession, symbol: str | None = None) -> dict[str, ContractSpec]:
    symbols = await load_symbols(session)
    if symbol:
        row = symbols.get(from_venue(symbol))
        return {from_venue(symbol): _spec_from(from_venue(symbol), row)} if row else {}
    return {name: _spec_from(name, row) for name, row in symbols.items()}


async def get_ticker_price(session: aiohttp.ClientSession, symbol: str) -> float:
    wanted = to_venue(symbol)
    for row in _rows(await _public_get(session, "/v2/market/tickers")):
        if str(row.get("symbol", "")).lower() == wanted:
            return _num(row.get("close"))
    return 0.0


# ── the account ──────────────────────────────────────────────────────────────────────────────────
class HibtRestClient:
    """One authenticated HIBT futures account. Same public surface as MexcRestClient."""

    def __init__(self, api_key: str, secret: str, *, session: aiohttp.ClientSession, timeout: float = 10.0) -> None:
        self._api_key = api_key
        self._secret = secret
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        """One signed call, retried only when the venue says it came too fast."""
        for attempt in range(len(RATE_LIMIT_BACKOFF) + 1):
            try:
                return await self._request_once(method, path, params)
            except HibtError as err:
                if not err.is_rate_limited or attempt == len(RATE_LIMIT_BACKOFF):
                    raise
                delay = RATE_LIMIT_BACKOFF[attempt] * (1.0 + random.random() * 0.4)
                LOGGER.info("%s rate limited, retrying in %.1fs", path, delay)
                await asyncio.sleep(delay)
        raise AssertionError("unreachable")

    async def _request_once(self, method: str, path: str, params: dict[str, Any] | None) -> Any:
        url = f"{BASE_URL}{path}"
        # Stamped INSIDE the slot. A signature carries its time and is refused after five minutes; a
        # request signed before it queued can reach the venue already stale.
        async with _throttle_for(self._api_key).slot():
            signed = {key: value for key, value in (params or {}).items() if value is not None}
            signed["timestamp"] = now_ms()
            headers = signed_headers(self._api_key, self._secret, signed)
            headers["User-Agent"] = USER_AGENT
            if method == "GET":
                request = self._session.request(
                    method, url, headers=headers, timeout=self._timeout,
                    params={key: _query_value(value) for key, value in signed.items()},
                )
            else:
                headers["Content-Type"] = "application/json"
                request = self._session.request(method, url, headers=headers, timeout=self._timeout, json=signed)
            async with request as response:
                text = await response.text()
                if response.status != 200:
                    # Headers deliberately left out of the message: they carry the signature.
                    raise HibtError(response.status, text[:200], endpoint=path)
                try:
                    payload = await response.json(content_type=None)
                except (ValueError, aiohttp.ContentTypeError):
                    # A 200 that is not JSON is the edge talking, not the API.
                    raise HibtError(response.status, f"not JSON: {text[:120]}", endpoint=path) from None

        if not isinstance(payload, dict):
            raise HibtError(None, f"unexpected reply: {str(payload)[:120]}", endpoint=path)
        code = payload.get("code")
        if code != 0:
            # The failure is in the body, under HTTP 200. See the module docstring.
            message = str(payload.get("msg") or "unknown")
            if code == SIGNATURE_FAILED_CODE:
                message += " (signature scheme mismatch — see hibt/auth.py)"
            elif code == TIMESTAMP_EXPIRED_CODE:
                message += " (server clock differs from this machine's)"
            raise HibtError(code, message, endpoint=path)
        return payload.get("data")

    async def _spec_row(self, symbol: str) -> dict[str, Any] | None:
        try:
            return (await load_symbols(self._session)).get(from_venue(symbol))
        except (HibtError, aiohttp.ClientError, asyncio.TimeoutError) as err:
            LOGGER.info("could not load HIBT contract rules: %s", err)
            return None

    # ── read ────────────────────────────────────────────────────────────────────────────────
    async def ping(self) -> bool:
        await _public_get(self._session, "/v2/market/symbols")
        return True

    async def get_assets(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/v2/account/balance")
        return _rows(data) or ([data] if isinstance(data, dict) else [])

    async def get_usdt_snapshot(self) -> AccountBalance:
        """The futures wallet in the bot's own terms.

        UNVERIFIED which figure `balance` is. The docs list it beside `frozen` (held by open orders)
        and `margin` (held by positions) as siblings, which reads as the FREE part rather than the
        total — so that is how it is taken, with equity rebuilt as free + held + unrealised. If the
        menu's balance disagrees with the app while positions are open, this is the line to change.
        """
        chosen: dict[str, Any] | None = None
        for asset in await self.get_assets():
            coin = str(asset.get("coin") or "").lower()
            if coin in ("usdt", ""):
                chosen = asset
                if coin == "usdt":
                    break
        if not chosen:
            return AccountBalance()
        free = _num(chosen.get("balance"))
        frozen = _num(chosen.get("frozen"))
        margin = _num(chosen.get("margin"))
        unrealised = _num(chosen.get("profit"))
        return AccountBalance(
            equity=free + frozen + margin + unrealised,
            available=free,
            available_open=free,
            bonus=_num(chosen.get("point")),
            cash=free + frozen + margin,
            position_margin=margin,
            frozen=frozen,
            unrealized=unrealised,
        )

    async def get_usdt_balance(self) -> tuple[float, float]:
        snapshot = await self.get_usdt_snapshot()
        return snapshot.equity, snapshot.openable

    async def get_open_positions_raw(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": to_venue(symbol)} if symbol else None
        rows = _rows(await self._request("GET", "/v2/account/position", params))
        return [position_row(row) for row in rows]

    async def get_open_positions(self, symbol: str | None = None) -> list[Position]:
        return [
            Position(
                position_id=row["positionId"],
                symbol=row["symbol"],
                position_type=row["positionType"],
                open_type=row["openType"],
                hold_vol=row["holdVol"],
                leverage=row["leverage"],
                open_avg_price=row["openAvgPrice"],
                state=row["state"],
            )
            for row in await self.get_open_positions_raw(symbol)
            if row["positionType"] in (1, 2)
        ]

    async def _finished_orders(self, symbol: str | None) -> list[dict[str, Any]]:
        """Filled orders on one symbol, with their realised PnL.

        Measured on a live key, 2026-09-14: /v2/order/finished refuses every parameter set the docs
        suggest (210001 "param error" with and without symbol, page, size, pageSize, limit, time
        range). /v2/account/order — "trading history with fills and realised P&L" — answers with a
        symbol, and refuses without one. So a symbol is required, and without it there is nothing
        to ask for.
        """
        if not symbol:
            return []
        return _rows(await self._request("GET", "/v2/account/order", {"symbol": to_venue(symbol)}))

    async def get_closed_positions_raw(self, symbol: str | None = None, *, page_size: int = 50) -> list[dict[str, Any]]:
        return closed_rows(await self._finished_orders(symbol))[:page_size]

    async def get_closed_positions(self, symbol: str | None = None, *, page_size: int = 50) -> list[ClosedPosition]:
        return [
            ClosedPosition(
                position_id=row["positionId"],
                symbol=row["symbol"],
                position_type=row["positionType"],
                realised=row["realised"],
                close_vol=row["closeVol"],
                update_time=row["updateTime"],
            )
            for row in await self.get_closed_positions_raw(symbol, page_size=page_size)
        ]

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": to_venue(symbol)} if symbol else None
        return [order_row(row) for row in _rows(await self._request("GET", "/v2/order/unFinish", params))]

    async def get_recent_orders(self, page_size: int = 20) -> list[dict[str, Any]]:
        return [order_row(row) for row in await self._finished_orders(None)][:page_size]

    async def get_stop_orders(self, symbol: str | None = None) -> list[PositionStops]:
        """Stops as they sit on each position.

        HIBT keeps take-profit and stop-loss ON the position (spPrice / slPrice) rather than on the
        order that opened it, so the position is where they are read from. The position id stands in
        for MEXC's order id, which is the handle set_position_stops would need.
        """
        out: list[PositionStops] = []
        for row in await self.get_open_positions_raw(symbol):
            if row["positionType"] not in (1, 2):
                continue
            out.append(
                PositionStops(
                    order_id=row["positionId"],
                    position_id=row["positionId"],
                    symbol=row["symbol"],
                    position_type=row["positionType"],
                    stop_loss_price=row["stopLossPrice"],
                    take_profit_price=row["takeProfitPrice"],
                )
            )
        return out

    async def set_position_stops(
        self, *, order_id: int, stop_loss_price: float | None, take_profit_price: float | None
    ) -> Any:
        """Not supported yet. HIBT's docs have no call that changes an open position's stops — only
        conditional orders (/v2/entrust/add), which are a different thing with different failure
        modes. Refused out loud rather than approximated: a stop that silently did not move is worse
        than a message saying it could not."""
        raise HibtError(None, "changing an open position's stops is not supported on HIBT yet", endpoint="stops")

    async def get_position_mode(self) -> int:
        """1 = hedge. HIBT opens and closes through separate endpoints and closes by position id, so
        a buy never reduces a sell: that is hedge behaviour. UNVERIFIED on a live account."""
        return 1

    async def get_leverage(self, symbol: str) -> list[dict[str, Any]]:
        return [
            {"symbol": row["symbol"], "positionType": row["positionType"], "leverage": row["leverage"]}
            for row in await self.get_open_positions_raw(symbol)
        ]

    # ── write ───────────────────────────────────────────────────────────────────────────────
    async def set_leverage(self, *, position_id: int | None, leverage: int, open_type: int, symbol: str, position_type: int) -> Any:
        """Per symbol. HIBT also takes leverage on the order itself, so this is belt and braces."""
        return await self._request(
            "POST", "/v2/account/setLeverage", {"symbol": to_venue(symbol), "leverage": int(leverage)}
        )

    async def submit_order(
        self,
        *,
        symbol: str,
        side: int,
        vol: float,
        leverage: int | None = None,
        open_type: int = 2,
        order_type: int = ORDER_TYPE_MARKET,
        external_oid: str | None = None,
        price: float | None = None,
        stop_loss_price: float | None = None,
        take_profit_price: float | None = None,
    ) -> Any:
        """Open or add to a position. Takes MEXC's side numbers, as every caller does.

        Only the two OPENING sides are accepted. On this venue an order can only open — closing is a
        separate call by position id — so a closing side sent here would be read as an order to open
        the opposite way, leaving the account holding both. That is refused rather than translated.
        """
        endpoint = "/v2/order/open"
        if side in (SIDE_CLOSE_LONG, SIDE_CLOSE_SHORT):
            raise HibtError(None, "HIBT cannot close through an order; the bot closes with close_all", endpoint=endpoint)
        if side not in (SIDE_OPEN_LONG, SIDE_OPEN_SHORT):
            raise HibtError(None, f"unknown side {side}", endpoint=endpoint)
        if not leverage:
            # HIBT requires it on every order. Guessing one would open a position at a leverage
            # nobody chose, so the order is refused and the reason says why.
            raise HibtError(None, "HIBT needs a leverage on every order and none was known", endpoint=endpoint)

        is_market = order_type == ORDER_TYPE_MARKET
        if not is_market and price is None:
            raise HibtError(None, "limit order requires a price", endpoint=endpoint)

        rules = await self._spec_row(symbol)
        minimum = _num(rules.get("marketMiniAmount" if is_market else "limitMiniAmount")) if rules else 0.0
        precision = size_precision(rules) if rules else 8
        amount = floor_amount(vol, precision)
        if amount <= 0 or (minimum and float(amount) < minimum):
            raise HibtError(
                None,
                f"size {vol:g} rounds to {amount} — below HIBT's minimum of {minimum:g} on {from_venue(symbol)}",
                endpoint=endpoint,
            )
        if rules and not rules.get("supportTrade", True):
            raise HibtError(None, f"{from_venue(symbol)}: HIBT does not allow trading this contract", endpoint=endpoint)

        params: dict[str, Any] = {
            "symbol": to_venue(symbol),
            "type": VENUE_TYPE_MARKET if is_market else VENUE_TYPE_LIMIT,
            "side": VENUE_SIDE_BUY if side == SIDE_OPEN_LONG else VENUE_SIDE_SELL,
            "leverage": int(leverage),
            # "10", not "10.00": the venue writes sizes without trailing zeros, and it is not worth
            # finding out on a live order whether it accepts them.
            "amount": format(amount.normalize(), "f"),
        }
        if order_type == ORDER_TYPE_POST_ONLY:
            # HIBT has no post-only type. A plain limit can take liquidity if the book has moved,
            # which a post-only would have refused — noted, because the fee difference is real.
            LOGGER.info("HIBT has no post-only; placing %s as a plain limit", symbol)
        if not is_market:
            params["price"] = repr(float(price))
        if external_oid:
            params["customID"] = external_oid
        if stop_loss_price or take_profit_price:
            params["triggerType"] = 1  # by last trade price
        if stop_loss_price:
            params["isSetSl"] = True
            params["slPrice"] = repr(float(stop_loss_price))
        if take_profit_price:
            params["isSetSp"] = True
            params["spPrice"] = repr(float(take_profit_price))

        data = await self._request("POST", endpoint, params)
        # Returned in MEXC's shape so _order_id_from reads it without knowing which venue answered.
        order_id = data.get("orderID") if isinstance(data, dict) else data
        return {"orderId": str(order_id)} if order_id else None

    async def cancel_orders(self, order_ids: list[str | int]) -> Any:
        """UNVERIFIED parameter name: the docs say "by ID or custom identifier" without naming the
        field; `orderID` matches how the venue names it in every response."""
        for order_id in order_ids or []:
            await self._request("POST", "/v2/order/cancel", {"orderID": str(order_id)})
        return None

    async def cancel_all_orders(self, symbol: str | None = None) -> Any:
        orders = await self.get_open_orders(symbol)
        await self.cancel_orders([row["orderId"] for row in orders if row["orderId"]])
        return None

    async def close_all(self, symbol: str | None = None) -> Any:
        """Close every position on a symbol, both sides — or on every symbol held.

        /v2/order/closeAll with {symbol} is documented exactly, request and response alike, which is
        why it is used instead of closing each position by id. With no symbol it is called once per
        symbol actually held, because the endpoint wants one.
        """
        if symbol:
            return await self._request("POST", "/v2/order/closeAll", {"symbol": to_venue(symbol)})
        held = sorted({p.symbol for p in await self.get_open_positions() if p.hold_vol > 0})
        results = []
        for name in held:
            results.append(await self._request("POST", "/v2/order/closeAll", {"symbol": to_venue(name)}))
        return results


def _query_value(value: Any) -> str:
    """A query-string value, rendered the way it was signed."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
