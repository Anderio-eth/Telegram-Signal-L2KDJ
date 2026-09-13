"""Which venue an account trades on, and the one place a client for it is made.

Every account belongs to a folder, and a folder trades on exactly one exchange. That rule is not
a convenience — it is what keeps the copy engine safe. The engine mirrors raw size, master volume
times a multiplier, and a size means different things on different venues: MEXC counts contracts
(one BTC contract is 0.0001 BTC), HIBT counts the asset itself. Mirroring 100 between them turns
0.01 BTC into 100 BTC. A folder holding accounts from both would do exactly that on its first trade,
so a folder cannot hold both.

Clients are made here and nowhere else. A MexcRestClient constructed by hand somewhere in the code
would quietly send a HIBT account's keys to MEXC — which fails, but fails as "invalid key" on an
account that is perfectly fine. test_exchange.py fails the build if one appears.
"""

from __future__ import annotations

from typing import Any

import aiohttp

EXCHANGE_MEXC = "mexc"
EXCHANGE_HIBT = "hibt"
EXCHANGES = (EXCHANGE_MEXC, EXCHANGE_HIBT)
DEFAULT_EXCHANGE = EXCHANGE_MEXC

EXCHANGE_NAMES = {EXCHANGE_MEXC: "MEXC", EXCHANGE_HIBT: "HIBT"}


def exchange_name(exchange: str | None) -> str:
    return EXCHANGE_NAMES.get(normalize(exchange), "MEXC")


def normalize(exchange: str | None) -> str:
    """An unknown or missing value is MEXC: every row that predates this column is a MEXC account."""
    value = (exchange or "").strip().lower()
    return value if value in EXCHANGES else DEFAULT_EXCHANGE


class Credentials(tuple):
    """(api_key, secret) that also knows which exchange the keys belong to.

    Still a two-item tuple, so every `api_key, secret = credentials` already in the code keeps
    working. The exchange rides along as an attribute rather than a third item — a third item would
    break each of those unpackings, and a plain two-tuple (from a test, or older code) simply means
    MEXC.
    """

    exchange: str

    def __new__(cls, api_key: str, secret: str, exchange: str | None = None) -> "Credentials":
        obj = super().__new__(cls, (api_key, secret))
        obj.exchange = normalize(exchange)
        return obj

    def __repr__(self) -> str:
        # Never the secret, and not the key either: this ends up in logs through tracebacks.
        return f"Credentials(exchange={self.exchange!r}, key=…{str(self[0])[-4:]})"


def exchange_of(credentials: Any) -> str:
    return normalize(getattr(credentials, "exchange", None))


def make_rest_client(credentials: Any, *, session: aiohttp.ClientSession):
    """The client for whichever venue these keys belong to."""
    api_key, secret = credentials[0], credentials[1]
    if exchange_of(credentials) == EXCHANGE_HIBT:
        from .hibt.rest import HibtRestClient

        return HibtRestClient(api_key, secret, session=session)
    from .mexc.rest import MexcRestClient

    return MexcRestClient(api_key, secret, session=session)


async def contract_specs(session: aiohttp.ClientSession, exchange: str | None, symbol: str | None = None):
    if normalize(exchange) == EXCHANGE_HIBT:
        from .hibt.rest import get_contract_specs
    else:
        from .mexc.rest import get_contract_specs
    return await get_contract_specs(session, symbol)


async def ticker_price(session: aiohttp.ClientSession, exchange: str | None, symbol: str) -> float:
    if normalize(exchange) == EXCHANGE_HIBT:
        from .hibt.rest import get_ticker_price
    else:
        from .mexc.rest import get_ticker_price
    return await get_ticker_price(session, symbol)
