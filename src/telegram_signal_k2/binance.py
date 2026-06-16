from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import aiohttp


BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"


@dataclass(frozen=True)
class Kline:
    open_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time: int
    quote_volume: Decimal
    trades: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def from_binance_row(cls, row: list[Any]) -> "Kline":
        return cls(
            open_time=int(row[0]),
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
            close_time=int(row[6]),
            quote_volume=Decimal(str(row[7])),
            trades=int(row[8]),
            taker_buy_volume=Decimal(str(row[9])),
            taker_buy_quote_volume=Decimal(str(row[10])),
        )


class BinanceFuturesClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        base_url: str = BINANCE_FUTURES_BASE_URL,
        timeout_seconds: int = 10,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def klines(self, symbol: str, interval: str, *, limit: int = 160) -> list[Kline]:
        url = f"{self._base_url}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        async with self._session.get(url, params=params, timeout=self._timeout) as response:
            payload = await response.json()
            if response.status >= 400:
                message = payload.get("msg") if isinstance(payload, dict) else payload
                raise RuntimeError(f"Binance error {response.status} for {symbol} {interval}: {message}")
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected Binance response for {symbol} {interval}: {payload!r}")
            return [Kline.from_binance_row(row) for row in payload]

