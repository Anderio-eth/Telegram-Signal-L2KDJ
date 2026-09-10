"""MEXC futures REST client — only the endpoints copy trading actually needs.

Every path here is taken from MEXC's published futures documentation; none are guessed. The
host, however, is NOT the one the docs give — see the BASE_URL note below.

Sizes are in CONTRACTS, not USD. One BTC_USDT contract is 0.0001 BTC, so a "$1000 position"
is meaningless to this API — the copy engine works in `vol` (contract count) precisely so a
follower ends up with the same position as the master rather than a rounded dollar guess.
Notional is derived only for display: vol × contractSize × price.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from dataclasses import dataclass
from typing import Any

import aiohttp

from .auth import rest_body_string, sign_rest

LOGGER = logging.getLogger(__name__)


class _Throttle:
    """Paces outbound calls so nine followers do not trip MEXC's per-IP limit.

    The limit is per IP, not per account — every follower is the same Render instance as far as
    MEXC is concerned. Nine accounts each sending a leverage call and an order is eighteen requests
    in one burst, which is what produced "Requests are too frequent" and lost a whole trade.

    Concurrency is capped and a minimum gap is kept between starts. Ordering is not guaranteed and
    does not need to be: each follower's own calls are sequential, and the accounts are independent
    by design.
    """

    def __init__(self, max_concurrent: int, min_interval: float) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._min_interval = min_interval
        self._lock = asyncio.Lock()
        self._next_slot = 0.0

    @contextlib.asynccontextmanager
    async def slot(self):
        async with self._semaphore:
            async with self._lock:
                now = asyncio.get_running_loop().time()
                wait = max(0.0, self._next_slot - now)
                self._next_slot = max(now, self._next_slot) + self._min_interval
            if wait:
                await asyncio.sleep(wait)
            yield


# Measured against a live account rather than taken from the docs, which give a figure the venue
# does not actually honour. Private endpoints start returning code 510 above roughly 12 requests
# per second from one IP, whatever mix of accounts they belong to:
#
#     60 private reads paced 0.06s apart (16.6/s)  ->  12 refused
#     60 private reads paced 0.14s apart ( 7.1/s)  ->   0 refused
#
# The old 0.06s was chosen to be "well under" a published limit and was in fact above the real
# one, which is why a single follower on an otherwise idle account could still be told its
# requests were too frequent. Public market data is on a separate, far looser allowance — 30/s
# went through untouched — so it is paced separately instead of competing with the trading path.
PRIVATE_THROTTLE = _Throttle(max_concurrent=3, min_interval=0.14)
PUBLIC_THROTTLE = _Throttle(max_concurrent=6, min_interval=0.03)

# MEXC's code for "Requests are too frequent". It refuses the request before acting on it, so a
# retry cannot duplicate an order — and orders carry an externalOid besides.
RATE_LIMITED_CODE = 510

# Long enough for the window to roll over. Retrying in 100ms just spends the next allowance, which
# is how a burst turns into a refusal that lasts.
RATE_LIMIT_BACKOFF = (0.4, 1.2, 3.0)

# api.mexc.com, NOT contract.mexc.com — verified against a live account:
#   contract.mexc.com  read 200 / order submit 403 "Access Denied" (blocked at the CDN)
#   api.mexc.com       read 200 / order submit 200 (order actually filled)
# MEXC's own futures docs point at contract.mexc.com, which is why this looks wrong; that host
# only serves reads. ccxt uses api.mexc.com for the same endpoints, which is why trading works
# there. Changing this back will make every order fail with an HTML error page.
BASE_URL = "https://api.mexc.com/api/v1"

# MEXC's own enums. Verified against the live venue, because the futures documentation lists
# these in a different order than the API actually behaves — and 3 and 4 were the wrong way round
# here, which meant every attempt to open a SHORT was really an attempt to close a LONG:
#
#   side=3, holding nothing  -> a SHORT position of 1 contract appeared
#   side=4, holding a SHORT  -> [2009] Position is nonexistent or closed
#   side=4, holding nothing  -> [2009] Position is nonexistent or closed
#
# So 3 opens a short and 4 closes a long. Do not "correct" these back to match the docs.
SIDE_OPEN_LONG = 1
SIDE_CLOSE_SHORT = 2
SIDE_OPEN_SHORT = 3
SIDE_CLOSE_LONG = 4

ORDER_TYPE_LIMIT = 1
ORDER_TYPE_POST_ONLY = 2
ORDER_TYPE_MARKET = 5

OPEN_TYPE_ISOLATED = 1
OPEN_TYPE_CROSS = 2

POSITION_TYPE_LONG = 1
POSITION_TYPE_SHORT = 2


class MexcError(RuntimeError):
    """A MEXC API call that returned success=false, carrying its code for retry decisions."""

    def __init__(self, code: int | None, message: str, *, endpoint: str) -> None:
        super().__init__(f"{endpoint}: [{code}] {message}")
        self.code = code
        self.message = message
        self.endpoint = endpoint

    @property
    def is_rate_limited(self) -> bool:
        """Refused for arriving too fast, and therefore worth trying again.

        Both the code and the wording are checked: 510 is what the futures API returns, 429 what
        the edge in front of it returns, and the text catches either being reported some third way
        without this quietly deciding the call had failed for good.
        """
        if self.code in (RATE_LIMITED_CODE, 429):
            return True
        lowered = (self.message or "").lower()
        return any(phrase in lowered for phrase in ("too frequent", "rate limit", "too many requests"))


def _opt_float(value: Any) -> float | None:
    """MEXC sends an unset price as null, 0 or "0" depending on the endpoint. All three mean
    "no stop", and none of them may become a real price of zero."""
    if value in (None, "", 0, "0"):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out or None


@dataclass(frozen=True)
class AccountBalance:
    """The USDT futures wallet, in full.

    `available` is the wallet figure; `available_open` is what the venue lets you open against.
    Which one is right depends on the question, so both are kept rather than one being chosen here.
    """

    equity: float = 0.0
    available: float = 0.0
    available_open: float = 0.0
    bonus: float = 0.0
    cash: float = 0.0
    position_margin: float = 0.0
    frozen: float = 0.0
    unrealized: float = 0.0

    @property
    def openable(self) -> float:
        """What a new position can actually be opened against.

        Falls back to `available` when the venue reports no `availableOpen` at all: some accounts
        omit it, and treating a missing field as zero would report every one of them as broke.
        """
        return self.available_open or self.available

    def detail(self) -> str:
        """Every figure on one line, for the moment an order is refused."""
        parts = [
            f"equity ${self.equity:,.2f}",
            f"available ${self.available:,.2f}",
            f"openable ${self.openable:,.2f}",
        ]
        if self.bonus:
            parts.append(f"bonus ${self.bonus:,.2f}")
        if self.position_margin:
            parts.append(f"in positions ${self.position_margin:,.2f}")
        if self.frozen:
            parts.append(f"frozen ${self.frozen:,.2f}")
        return ", ".join(parts)


@dataclass(frozen=True)
class Position:
    position_id: int
    symbol: str
    position_type: int  # 1 long, 2 short
    open_type: int  # 1 isolated, 2 cross
    hold_vol: float  # contracts
    leverage: int
    open_avg_price: float
    state: int  # 1 holding, 2 system custody, 3 closed

    @property
    def is_long(self) -> bool:
        return self.position_type == POSITION_TYPE_LONG


@dataclass(frozen=True)
class ClosedPosition:
    """One finished position, as the exchange settled it.

    `realised` is MEXC's own realised PnL for the position — taken rather than recomputed from
    entry and exit prices, because the venue's number is the one that moved the balance.
    """

    position_id: int
    symbol: str
    position_type: int
    realised: float
    close_vol: float
    update_time: int


@dataclass(frozen=True)
class PositionStops:
    """A position's stop-loss / take-profit, as MEXC holds it.

    MEXC attaches these to the ORDER that opened the position, not to the position itself, which
    is why `order_id` is what has to be carried around to change them later.
    """

    order_id: int
    position_id: int
    symbol: str
    position_type: int
    stop_loss_price: float | None
    take_profit_price: float | None

    @property
    def is_set(self) -> bool:
        return bool(self.stop_loss_price or self.take_profit_price)


@dataclass(frozen=True)
class ContractSpec:
    symbol: str
    contract_size: float
    min_vol: float
    vol_scale: int
    max_leverage: int
    # MEXC blocks API trading outright on some contracts — typically new or low-liquidity
    # listings. They are tradable in the app, which is why a master can hold a position the bot
    # is not permitted to mirror. The venue reports the attempt as "Contract not activated".
    api_allowed: bool = True

    def contracts_for_notional(self, notional_usd: float, price: float) -> float:
        """How many contracts a given dollar exposure is, at this price."""
        if price <= 0 or self.contract_size <= 0:
            return 0.0
        return notional_usd / (price * self.contract_size)

    def notional(self, vol: float, price: float) -> float:
        return vol * self.contract_size * price


class MexcRestClient:
    """One authenticated MEXC futures account.

    Holds no global state: a copy-trading run creates one of these per account, so a failure on
    one follower cannot disturb another.
    """

    def __init__(self, api_key: str, secret: str, *, session: aiohttp.ClientSession, timeout: float = 10.0) -> None:
        self._api_key = api_key
        self._secret = secret
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None) -> Any:
        """One signed call, retried if the venue says it came too fast.

        Retried here rather than only in the copy engine because most callers are reads, and a
        read that gives up returns "nothing" — which downstream is indistinguishable from an
        account that genuinely holds nothing. Pacing alone is not enough: this process shares its
        IP allowance with whatever else Render is running on the same address.
        """
        for attempt in range(len(RATE_LIMIT_BACKOFF) + 1):
            try:
                return await self._request_once(method, path, params=params, body=body)
            except MexcError as err:
                if not err.is_rate_limited or attempt == len(RATE_LIMIT_BACKOFF):
                    raise
                delay = RATE_LIMIT_BACKOFF[attempt] * (1.0 + random.random() * 0.4)
                LOGGER.info("%s rate limited, retrying in %.1fs", path, delay)
                await asyncio.sleep(delay)
        raise AssertionError("unreachable")

    async def _request_once(self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None) -> Any:
        url = f"{BASE_URL}{path}"
        data = rest_body_string(body) if body is not None else None

        # Signed INSIDE the slot, never before it. The signature carries a timestamp the venue
        # checks, so a request stamped and then held in the queue goes out already stale — under
        # the slower pacing a queue of 120 calls turned into "Confirming signature failed" on the
        # ones that waited longest. The wait has to happen first, then the clock is read.
        async with PRIVATE_THROTTLE.slot():
            headers = sign_rest(self._api_key, self._secret, params=params, body=body)
            async with self._session.request(
                method, url, params=params, data=data, headers=headers, timeout=self._timeout
            ) as response:
                text = await response.text()
                if response.status != 200:
                    # Deliberately does not include headers: they carry the signature.
                    raise MexcError(response.status, text[:200], endpoint=path)
                payload = await response.json(content_type=None)

        if isinstance(payload, dict) and payload.get("success") is False:
            raise MexcError(payload.get("code"), str(payload.get("message", "unknown")), endpoint=path)
        return payload.get("data") if isinstance(payload, dict) else payload

    # ── read ────────────────────────────────────────────────────────────────────────────────
    async def ping(self) -> bool:
        await self._request("GET", "/contract/ping")
        return True

    async def get_assets(self) -> list[dict[str, Any]]:
        return await self._request("GET", "/private/account/assets") or []

    async def get_usdt_snapshot(self) -> "AccountBalance":
        """Every number MEXC reports for the USDT futures wallet, not just one of them.

        There are several, and they are not the same. `availableBalance` is what a wallet screen
        shows; `availableOpen` is what the venue measures a new position against. They can differ —
        bonus credit is the usual reason — and when they do, a menu showing the first while orders
        are refused against the second reads as "there is plenty of money, why will it not open".

        Reported together so that question is answered by looking, rather than argued about.
        """
        for asset in await self.get_assets():
            if asset.get("currency") == "USDT":
                num = lambda key: float(asset.get(key) or 0)  # noqa: E731
                return AccountBalance(
                    equity=num("equity"),
                    available=num("availableBalance"),
                    available_open=num("availableOpen"),
                    bonus=num("bonus"),
                    cash=num("cashBalance"),
                    position_margin=num("positionMargin"),
                    frozen=num("frozenBalance"),
                    unrealized=num("unrealized"),
                )
        return AccountBalance()

    async def get_usdt_balance(self) -> tuple[float, float]:
        """(equity, what can actually be used to open) in USDT."""
        snapshot = await self.get_usdt_snapshot()
        return snapshot.equity, snapshot.openable

    async def get_open_positions_raw(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Open positions exactly as the venue sends them.

        The rows carry the same fields the websocket's position frames do — positionId, version,
        state, realised — so they can go through the same parser and produce the same events. That
        is what lets the bot keep working on a network where the socket host is blocked.
        """
        params = {"symbol": symbol} if symbol else None
        rows = await self._request("GET", "/private/position/open_positions", params=params) or []
        return [row for row in rows if isinstance(row, dict)]

    async def get_closed_positions_raw(self, symbol: str | None = None, *, page_size: int = 50) -> list[dict[str, Any]]:
        """Finished positions, unparsed. A settled row reads holdVol 0, state 3 and its realised
        PnL — the same shape as the socket's closing frame."""
        params: dict[str, Any] = {"page_num": 1, "page_size": page_size}
        if symbol:
            params["symbol"] = symbol
        rows = await self._request("GET", "/private/position/list/history_positions", params=params) or []
        return [row for row in rows if isinstance(row, dict)]

    async def get_open_positions(self, symbol: str | None = None) -> list[Position]:
        params = {"symbol": symbol} if symbol else None
        rows = await self._request("GET", "/private/position/open_positions", params=params) or []
        out: list[Position] = []
        for row in rows:
            out.append(
                Position(
                    position_id=int(row.get("positionId", 0)),
                    symbol=str(row.get("symbol", "")),
                    position_type=int(row.get("positionType", 0)),
                    open_type=int(row.get("openType", 0)),
                    hold_vol=float(row.get("holdVol", 0) or 0),
                    leverage=int(row.get("leverage", 0) or 0),
                    open_avg_price=float(row.get("openAvgPrice", 0) or 0),
                    state=int(row.get("state", 0) or 0),
                )
            )
        return out

    async def get_closed_positions(self, symbol: str | None = None, *, page_size: int = 50) -> list[ClosedPosition]:
        """Recently closed positions, newest first.

        Used after a close to report what the position actually made. Settlement is not always
        instant, so callers poll this rather than reading it once.
        """
        params: dict[str, Any] = {"page_num": 1, "page_size": page_size}
        if symbol:
            params["symbol"] = symbol
        rows = await self._request("GET", "/private/position/list/history_positions", params=params) or []
        out: list[ClosedPosition] = []
        for row in rows:
            try:
                out.append(
                    ClosedPosition(
                        position_id=int(row.get("positionId") or 0),
                        symbol=str(row.get("symbol") or ""),
                        position_type=int(row.get("positionType") or 0),
                        realised=float(row.get("realised") or 0.0),
                        close_vol=float(row.get("closeVol") or 0.0),
                        update_time=int(row.get("updateTime") or 0),
                    )
                )
            except (TypeError, ValueError):
                # A row we cannot parse is skipped rather than failing the whole lookup: this is
                # reporting, and a malformed entry must not break the close it is describing.
                LOGGER.debug("unparseable closed position row: %s", row)
        return out

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Orders currently resting in the book. This is how a limit becomes visible before it
        fills — the position feed says nothing until it already has."""
        path = f"/private/order/list/open_orders/{symbol}" if symbol else "/private/order/list/open_orders"
        return await self._request("GET", path) or []

    async def get_recent_orders(self, page_size: int = 20) -> list[dict[str, Any]]:
        """Recently finished orders, newest first. Used to tell a fill from a cancel once an order
        has left the book — the two need opposite reactions and look identical from outside."""
        return await self._request(
            "GET", "/private/order/list/history_orders",
            params={"page_num": 1, "page_size": page_size},
        ) or []

    async def get_stop_orders(self, symbol: str | None = None) -> list[PositionStops]:
        """Live stop-loss / take-profit entries. Unfinished ones only — a triggered stop is
        history, and mirroring it would put a stop on a position that is already gone."""
        params: dict[str, Any] = {"page_num": 1, "page_size": 100, "is_finished": 0}
        if symbol:
            params["symbol"] = symbol
        rows = await self._request("GET", "/private/stoporder/list/orders", params=params) or []
        out: list[PositionStops] = []
        for row in rows:
            try:
                out.append(
                    PositionStops(
                        order_id=int(row.get("orderId") or row.get("id") or 0),
                        position_id=int(row.get("positionId") or 0),
                        symbol=str(row.get("symbol") or ""),
                        position_type=int(row.get("positionType") or 0),
                        stop_loss_price=_opt_float(row.get("stopLossPrice")),
                        take_profit_price=_opt_float(row.get("takeProfitPrice")),
                    )
                )
            except (TypeError, ValueError):
                LOGGER.debug("unparseable stop order row: %s", row)
        return out

    async def set_position_stops(
        self, *, order_id: int, stop_loss_price: float | None, take_profit_price: float | None
    ) -> Any:
        """Attach or change the stop-loss / take-profit on the order that opened a position.

        Per MEXC: both empty or zero means "cancel them", which is how a cleared stop on the
        master gets mirrored as a cleared stop rather than being left in place.
        """
        body: dict[str, Any] = {"orderId": order_id}
        body["stopLossPrice"] = stop_loss_price or 0
        body["takeProfitPrice"] = take_profit_price or 0
        return await self._request("POST", "/private/stoporder/change_price", body=body)

    async def get_position_mode(self) -> int:
        """1 = hedge, 2 = one-way. Master and follower must match, or sides get misread."""
        return int(await self._request("GET", "/private/position/position_mode"))

    async def get_leverage(self, symbol: str) -> list[dict[str, Any]]:
        return await self._request("GET", "/private/position/leverage", params={"symbol": symbol}) or []

    # ── write ───────────────────────────────────────────────────────────────────────────────
    async def set_leverage(self, *, position_id: int | None, leverage: int, open_type: int, symbol: str, position_type: int) -> Any:
        """Set leverage. With no open position MEXC wants symbol+positionType+openType;
        with one open it wants the positionId instead."""
        body: dict[str, Any] = {"leverage": leverage}
        if position_id:
            body["positionId"] = position_id
        else:
            body |= {"symbol": symbol, "openType": open_type, "positionType": position_type}
        return await self._request("POST", "/private/position/change_leverage", body=body)

    async def submit_order(
        self,
        *,
        symbol: str,
        side: int,
        vol: float,
        leverage: int | None = None,
        open_type: int = OPEN_TYPE_CROSS,
        order_type: int = ORDER_TYPE_MARKET,
        external_oid: str | None = None,
        price: float | None = None,
        stop_loss_price: float | None = None,
        take_profit_price: float | None = None,
    ) -> Any:
        """Place an order. `vol` is in CONTRACTS.

        external_oid is what makes a retry safe: MEXC rejects a duplicate client order id, so a
        network timeout that actually succeeded cannot become a second position.
        """
        body: dict[str, Any] = {"symbol": symbol, "side": side, "vol": vol, "type": order_type, "openType": open_type}
        if leverage is not None:
            body["leverage"] = leverage
        if external_oid:
            body["externalOid"] = external_oid
        if price is not None:
            body["price"] = price
        # Stops go on with the order rather than afterwards. MEXC hangs a position's stop-loss and
        # take-profit off the order that opened it, so there is no entry to modify until one
        # exists — attaching them here is the only moment a follower can be given the master's
        # levels without a position first sitting unprotected.
        if stop_loss_price:
            body["stopLossPrice"] = stop_loss_price
        if take_profit_price:
            body["takeProfitPrice"] = take_profit_price
        if order_type != ORDER_TYPE_MARKET and price is None:
            # A limit order without a price is not an order MEXC can place; failing here names the
            # problem instead of letting the venue reject it with something less specific.
            raise MexcError(None, "limit order requires a price", endpoint="/private/order/submit")
        return await self._request("POST", "/private/order/submit", body=body)

    async def cancel_orders(self, order_ids: list[str | int]) -> Any:
        """Cancel specific orders by id.

        Used to pull a follower's resting copy when the master pulls theirs. Cancelling by id
        rather than cancel-all, because a follower may be holding resting orders from other
        symbols or from an earlier mirrored trade that is still perfectly valid.
        """
        if not order_ids:
            return None
        return await self._request(
            "POST", "/private/order/cancel", body=[int(o) for o in order_ids]
        )

    async def cancel_all_orders(self, symbol: str | None = None) -> Any:
        """Every open order, optionally on one symbol. The blunt instrument, for emergencies."""
        body: dict[str, Any] = {}
        if symbol:
            body["symbol"] = symbol
        return await self._request("POST", "/private/order/cancel_all", body=body)

    async def close_all(self, symbol: str | None = None) -> Any:
        """Close every position on a symbol.

        This is the ONLY way this bot closes. Do not "close" by submitting the opposite side:
        verified on a live hedge-mode account, sending side=3 against an open long did not close
        it — MEXC opened a fresh SHORT alongside it, at the account's default leverage rather than
        the position's. Which is exactly right, side=3 being "open short": the enum above named it
        "close long" at the time, and that mislabel is what made this look like a venue quirk.

            before:  ADA LONG  vol=1 lev=20
            after:   ADA LONG  vol=1 lev=20  +  ADA SHORT vol=1 lev=5

        For a copy bot that failure mode is the worst one possible: a "close" instruction would
        leave every follower doubly exposed in both directions. close_all cleared both instantly.
        """
        body = {"symbol": symbol} if symbol else {}
        return await self._request("POST", "/private/position/close_all", body=body)


# ── public market data (no auth) ───────────────────────────────────────────────────────────
async def get_contract_specs(session: aiohttp.ClientSession, symbol: str | None = None) -> dict[str, ContractSpec]:
    """Contract sizes and limits. Needed to convert contracts↔dollars for display, and to
    round a follower's size to a volume the venue will accept."""
    params = {"symbol": symbol} if symbol else None
    async with session.get(f"{BASE_URL}/contract/detail", params=params) as response:
        payload = await response.json(content_type=None)
    rows = payload.get("data") or []
    if isinstance(rows, dict):
        rows = [rows]
    specs: dict[str, ContractSpec] = {}
    for row in rows:
        specs[row["symbol"]] = ContractSpec(
            symbol=row["symbol"],
            contract_size=float(row.get("contractSize", 0) or 0),
            min_vol=float(row.get("minVol", 1) or 1),
            vol_scale=int(row.get("volScale", 0) or 0),
            max_leverage=int(row.get("maxLeverage", 0) or 0),
            api_allowed=bool(row.get("apiAllowed", True)),
        )
    return specs


async def get_ticker_price(session: aiohttp.ClientSession, symbol: str) -> float:
    async with session.get(f"{BASE_URL}/contract/ticker", params={"symbol": symbol}) as response:
        payload = await response.json(content_type=None)
    return float((payload.get("data") or {}).get("lastPrice", 0) or 0)
