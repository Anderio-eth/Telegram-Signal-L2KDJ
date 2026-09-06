"""MEXC futures REST client — only the endpoints copy trading actually needs.

Every path here is taken from MEXC's published futures documentation; none are guessed. The
host, however, is NOT the one the docs give — see the BASE_URL note below.

Sizes are in CONTRACTS, not USD. One BTC_USDT contract is 0.0001 BTC, so a "$1000 position"
is meaningless to this API — the copy engine works in `vol` (contract count) precisely so a
follower ends up with the same position as the master rather than a rounded dollar guess.
Notional is derived only for display: vol × contractSize × price.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from .auth import rest_body_string, sign_rest

LOGGER = logging.getLogger(__name__)

# api.mexc.com, NOT contract.mexc.com — verified against a live account:
#   contract.mexc.com  read 200 / order submit 403 "Access Denied" (blocked at the CDN)
#   api.mexc.com       read 200 / order submit 200 (order actually filled)
# MEXC's own futures docs point at contract.mexc.com, which is why this looks wrong; that host
# only serves reads. ccxt uses api.mexc.com for the same endpoints, which is why trading works
# there. Changing this back will make every order fail with an HTML error page.
BASE_URL = "https://api.mexc.com/api/v1"

# MEXC's own enums (see futures docs → Enum Values).
SIDE_OPEN_LONG = 1
SIDE_CLOSE_SHORT = 2
SIDE_CLOSE_LONG = 3
SIDE_OPEN_SHORT = 4

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
class ContractSpec:
    symbol: str
    contract_size: float
    min_vol: float
    vol_scale: int
    max_leverage: int

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
        url = f"{BASE_URL}{path}"
        headers = sign_rest(self._api_key, self._secret, params=params, body=body)
        data = rest_body_string(body) if body is not None else None

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

    async def get_usdt_balance(self) -> tuple[float, float]:
        """(equity, availableBalance) in USDT."""
        for asset in await self.get_assets():
            if asset.get("currency") == "USDT":
                return float(asset.get("equity", 0)), float(asset.get("availableBalance", 0))
        return 0.0, 0.0

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
        return await self._request("POST", "/private/order/submit", body=body)

    async def close_all(self, symbol: str | None = None) -> Any:
        """Close every position on a symbol.

        This is the ONLY way this bot closes. Do not "close" by submitting the opposite side:
        verified on a live hedge-mode account, sending side=3 (close long) against an open long
        did not close it — MEXC opened a fresh SHORT alongside it, at the account's default
        leverage rather than the position's:

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
        )
    return specs


async def get_ticker_price(session: aiohttp.ClientSession, symbol: str) -> float:
    async with session.get(f"{BASE_URL}/contract/ticker", params={"symbol": symbol}) as response:
        payload = await response.json(content_type=None)
    return float((payload.get("data") or {}).get("lastPrice", 0) or 0)
