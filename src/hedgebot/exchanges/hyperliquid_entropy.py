"""Entropy leg of the hedge — i.e. a Hyperliquid account trading the `io` builder's markets.

Entropy is HIP-3 on Hyperliquid, so this is just the Hyperliquid SDK pointed at the `io` perp DEX.
Auth is a main wallet ADDRESS plus an API/agent wallet PRIVATE KEY (created in Hyperliquid settings);
the agent signs, the address owns the funds. Markets are named `io:<ASSET>`.

The official SDK is synchronous (requests-based), so every call is run in a thread to keep the bot's
event loop free.

VERIFY on first live run (cannot be checked without keys):
  - the `perp_dexs=[dex]` kwarg name on this SDK version (it makes Exchange/Info resolve `io:` names);
  - Hyperliquid price rounding rules for `limit_px` (≤5 significant figures and ≤ szDecimals) — we
    pre-round to szDecimals here, which is the usual requirement.
"""

from __future__ import annotations

import asyncio

from eth_account import Account as EthAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info


class EntropyClient:
    def __init__(self, api_url: str, wallet_address: str, agent_private_key: str, dex: str = "io") -> None:
        self._address = wallet_address
        self._dex = dex
        self._agent = EthAccount.from_key(agent_private_key)
        # perp_dexs loads the builder DEX meta so order()/positions resolve "io:<ASSET>" names.
        self._info = Info(api_url, skip_ws=True, perp_dexs=[dex])
        self._exchange = Exchange(self._agent, api_url, account_address=wallet_address, perp_dexs=[dex])

    @staticmethod
    def order_error(resp) -> str | None:
        """Extract a rejection reason from an order() response, or None if it was accepted. Hyperliquid
        does NOT raise on a rejected order — it returns {"status":"ok",...,"statuses":[{"error":...}]}
        (or status "err"), so callers that only catch exceptions would think a reject succeeded."""
        if not isinstance(resp, dict):
            return None
        if resp.get("status") != "ok":
            return str(resp.get("response") or resp)[:200]
        try:
            for st in resp["response"]["data"]["statuses"]:
                if isinstance(st, dict) and st.get("error"):
                    return str(st["error"])[:200]
        except (KeyError, TypeError, IndexError):
            return None
        return None

    async def limit_order(self, market: str, is_buy: bool, size: float, price: float,
                          *, post_only: bool = False, reduce_only: bool = False) -> dict:
        """Place a limit order on an io market. `market` is e.g. "io:ANTH". tif Alo = add-liquidity-only
        (post-only), Gtc otherwise."""
        tif = "Alo" if post_only else "Gtc"
        order_type = {"limit": {"tif": tif}}
        return await asyncio.to_thread(
            self._exchange.order, market, is_buy, size, price, order_type, reduce_only,
        )

    async def balance(self) -> dict:
        """Account value and free (withdrawable) collateral in the io DEX. marginSummary is the
        equity picture; withdrawable is what isn't tied up as margin."""
        state = await asyncio.to_thread(self._info.user_state, self._address, self._dex)
        ms = state.get("marginSummary", {}) or {}
        return {
            "total": float(ms.get("accountValue", 0) or 0),
            "used": float(ms.get("totalMarginUsed", 0) or 0),
            "free": float(state.get("withdrawable", 0) or 0),
        }

    async def spot_usdc(self) -> float:
        """USDC sitting in the Hyperliquid SPOT wallet (not the io perp account). Used to hint the user
        when io shows $0 but the money is just in spot and needs transferring into io to trade."""
        state = await asyncio.to_thread(self._info.spot_user_state, self._address)
        for b in state.get("balances", []) or []:
            if b.get("coin") == "USDC":
                return float(b.get("total", 0) or 0)
        return 0.0

    async def positions(self) -> list[dict]:
        """Open io positions for this account: [{coin, szi, entryPx, leverage, ...}]."""
        state = await asyncio.to_thread(self._info.user_state, self._address, self._dex)
        return [p["position"] for p in state.get("assetPositions", []) if p.get("position")]

    async def position(self, market: str) -> dict | None:
        """The open position on one io market, or None if flat. Carries `unrealizedPnl` for stats."""
        for p in await self.positions():
            if p.get("coin") == market:
                return p
        return None

    async def cancel_all(self, market: str | None = None) -> None:
        """Cancel this account's resting orders (optionally just one io market). Best-effort — used to
        pull the unfilled maker leg after the position itself has been flattened."""
        try:
            orders = await asyncio.to_thread(self._info.open_orders, self._address, self._dex)
        except TypeError:
            orders = await asyncio.to_thread(self._info.open_orders, self._address)
        for o in orders or []:
            coin, oid = o.get("coin"), o.get("oid")
            if oid is None or (market and coin != market):
                continue
            try:
                await asyncio.to_thread(self._exchange.cancel, coin, oid)
            except Exception:  # noqa: BLE001 — a single stuck cancel must not block the rest
                continue

    async def close_market(self, market: str) -> dict:
        """Flatten one io market at market price (reduce-only, opposite side of the held size)."""
        for pos in await self.positions():
            if pos.get("coin") == market:
                szi = float(pos.get("szi", 0))
                if szi == 0:
                    return {"status": "flat"}
                return await asyncio.to_thread(self._exchange.market_close, market)
        return {"status": "flat"}

    async def set_leverage(self, market: str, leverage: int) -> dict:
        """Set isolated leverage for one io market (io markets are strictIsolated)."""
        return await asyncio.to_thread(self._exchange.update_leverage, leverage, market, False)

    async def realized_since(self, market: str, start_ms: int) -> tuple[float, float]:
        """Sum realized PnL and fees from this account's fills for one io market since `start_ms`
        (ms). This is the exchange's own closedPnl/fee per fill — the accurate figure for a hedge
        leg, not an unrealised estimate. Returns (pnl, fee); (0, 0) on any error."""
        try:
            fills = await asyncio.to_thread(self._info.user_fills, self._address)
        except Exception:  # noqa: BLE001
            return 0.0, 0.0
        pnl = fee = 0.0
        for f in fills or []:
            if f.get("coin") != market:
                continue
            if int(f.get("time", 0) or 0) < start_ms - 2000:   # small buffer for clock skew
                continue
            pnl += float(f.get("closedPnl", 0) or 0)
            fee += float(f.get("fee", 0) or 0)
        return pnl, fee
