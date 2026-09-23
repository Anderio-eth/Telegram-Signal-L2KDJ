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
    def __init__(self, api_url: str, wallet_address: str, agent_private_key: str, dex: str = "io",
                 proxy: str | None = None) -> None:
        self._address = wallet_address
        self._dex = dex
        self._agent = EthAccount.from_key(agent_private_key)
        # perp_dexs loads the builder DEX meta so order()/positions resolve "io:<ASSET>" names.
        self._info = Info(api_url, skip_ws=True, perp_dexs=[dex])
        self._exchange = Exchange(self._agent, api_url, account_address=wallet_address, perp_dexs=[dex])
        if proxy:
            # The SDK is requests-based and exposes its Session, so a proxy is just session.proxies
            # (requests also understands socks5:// when PySocks is installed). Both objects need it:
            # Info reads state, Exchange signs and sends -- a proxy on only one would still expose
            # the server's own IP on half the traffic, which defeats the point of having one.
            for api in (self._info, self._exchange):
                api.session.proxies = {"http": proxy, "https": proxy}

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
        """Tradeable balance = io-perp equity + the spot USDC NOT already backing it.

        When io draws its collateral from spot (entropy.io's default), the SAME dollars show up twice:
        as the io account value AND as spot USDC `hold` (verified on live accounts: spot total ==
        hold == io accountValue for an all-in trader). Adding io + spot total double-counted exactly
        that (the $1,315 shown for a ~$680 account). Spot minus its hold is the unencumbered part;
        with a separately funded io account hold is 0 and this is plain io + spot."""
        state, (spot_total, spot_hold) = await asyncio.gather(
            asyncio.to_thread(self._info.user_state, self._address, self._dex),
            self.spot_usdc_detail(),
        )
        ms = state.get("marginSummary", {}) or {}
        io_total = float(ms.get("accountValue", 0) or 0)
        io_free = float(state.get("withdrawable", 0) or 0)
        spot_free = max(0.0, spot_total - spot_hold)
        return {
            "total": io_total + spot_free,
            "used": float(ms.get("totalMarginUsed", 0) or 0),
            "free": io_free + spot_free,
            "io": io_total,
            "spot": spot_free,
        }

    async def ensure_margin(self, needed_usd: float) -> str | None:
        """Make sure the io-perp account has >= needed_usd of FREE margin, moving USDC from the spot
        wallet if it's short — this is the spot->io transfer the entropy.io frontend does on order,
        which raw API orders skip (hence "not enough margin"). Self-transfer only (destination is our
        own address). Returns None if nothing needed / it succeeded, else an error string."""
        state, spot = await asyncio.gather(
            asyncio.to_thread(self._info.user_state, self._address, self._dex),
            self.spot_usdc(),
        )
        io_free = float(state.get("withdrawable", 0) or 0)
        if io_free >= needed_usd:
            return None
        move = min(spot, needed_usd - io_free + 0.10)   # +buffer for fees/rounding
        if move < 0.01:
            return None                                  # nothing in spot to move; let the order speak
        try:
            resp = await asyncio.to_thread(
                self._exchange.send_asset, self._address, "spot", self._dex, "USDC", round(move, 2))
            return self.order_error(resp)
        except Exception as e:  # noqa: BLE001
            return str(e)[:200]

    async def spot_usdc(self) -> float:
        """Free USDC in the Hyperliquid SPOT wallet (total minus what's held as io collateral/orders)."""
        total, hold = await self.spot_usdc_detail()
        return max(0.0, total - hold)

    async def spot_usdc_detail(self) -> tuple[float, float]:
        """(total, hold) of spot USDC. `hold` is USDC locked as collateral for io positions (when io
        draws from spot) or by open spot orders — it must not be added on top of the io balance."""
        state = await asyncio.to_thread(self._info.spot_user_state, self._address)
        for b in state.get("balances", []) or []:
            if b.get("coin") == "USDC":
                return float(b.get("total", 0) or 0), float(b.get("hold", 0) or 0)
        return 0.0, 0.0

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

    async def cancel_all(self, market: str | None = None, *, include_triggers: bool = True) -> None:
        """Cancel this account's resting orders (optionally just one io market). Best-effort — used to
        pull the unfilled maker leg after the position itself has been flattened.

        include_triggers=False leaves stop-loss trigger orders alone: re-quoting the maker limit must
        not strip the protective stop off the position. frontend_open_orders is used because it
        carries the `isTrigger` flag (plain open_orders doesn't)."""
        try:
            orders = await asyncio.to_thread(self._info.frontend_open_orders, self._address, self._dex)
        except TypeError:
            orders = await asyncio.to_thread(self._info.frontend_open_orders, self._address)
        for o in orders or []:
            coin, oid = o.get("coin"), o.get("oid")
            if oid is None or (market and coin != market):
                continue
            if not include_triggers and o.get("isTrigger"):
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

    @staticmethod
    def round_px(px: float, sz_decimals: int) -> float:
        """Hyperliquid price rule (same as the SDK's own _slippage_price): 5 significant figures and
        at most 6 - szDecimals decimals for perps."""
        return round(float(f"{px:.5g}"), 6 - sz_decimals)

    async def stop_loss(self, market: str, is_buy: bool, size: float, trigger_px: float,
                        limit_px: float) -> dict:
        """Reduce-only stop-market: fires at trigger_px and executes as a market order capped at
        limit_px (the worst price accepted). is_buy=False protects a long, True a short. Returns the
        raw response — use order_error() to read a rejection."""
        order_type = {"trigger": {"triggerPx": trigger_px, "isMarket": True, "tpsl": "sl"}}
        return await asyncio.to_thread(
            self._exchange.order, market, is_buy, size, limit_px, order_type, True,
        )

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
