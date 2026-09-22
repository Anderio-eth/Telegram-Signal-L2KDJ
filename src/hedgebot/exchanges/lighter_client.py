"""Lighter leg of the hedge.

Auth is the API key's PRIVATE KEY plus the Account Index and API Key Index (default 0) from Lighter's
API Keys page. The SignerClient signs orders and manages the per-key nonce itself.

Sizes and prices go to the SDK as INTEGERS scaled by each market's decimals (see market_data.
lighter_amounts); market_index is the order book's market_id.

Call signatures are verified against the elliottech/lighter-python SDK source: create_order (with
order_expiry matching the TIF), update_leverage(market_index, margin_mode, leverage),
cancel_all_orders(time_in_force, timestamp_ms), and AccountApi.account(by, value). Account reads reuse
the signer's own HTTP connector.
"""

from __future__ import annotations

import contextlib
import time

from lighter import SignerClient


class LighterClient:
    # mirror of the SDK constants, kept local so callers don't import the SDK
    ORDER_TYPE_LIMIT = 0
    TIF_IOC = 0
    TIF_GTT = 1
    TIF_POST_ONLY = 2
    # order_expiry MUST match the TIF: IOC needs 0, GTT/post-only the 28-day sentinel. Sending an IOC
    # with the 28-day expiry gets the order rejected — which silently broke reduce-only closing.
    IOC_EXPIRY = 0
    GTT_EXPIRY = -1
    CROSS_MARGIN_MODE = 0
    ISOLATED_MARGIN_MODE = 1
    CANCEL_ALL_TIF_IMMEDIATE = 0

    def __init__(self, api_url: str, account_index: int, api_key_private_key: str,
                 api_key_index: int = 0) -> None:
        self._url = api_url
        self._account_index = int(account_index)
        self._api_key_index = int(api_key_index)
        self._signer = SignerClient(
            url=api_url,
            account_index=int(account_index),
            api_private_keys={int(api_key_index): api_key_private_key},
        )

    async def _account(self):
        """The account object, read over the SIGNER's existing HTTP connector (no new ApiClient per
        call — that connector setup was the slow part of every balance/position read)."""
        import lighter
        resp = await lighter.AccountApi(self._signer.api_client).account(
            by="index", value=str(self._account_index))
        return resp.accounts[0]

    def _pos_dict(self, p) -> dict:
        raw = self._num(p, "position", "base_amount", "size") or 0.0
        sign = self._num(p, "sign")
        signed = raw * (1 if (sign is None or sign >= 0) else -1)
        return {
            "market_id": int(self._num(p, "market_id", "market_index") or -1),
            "size": signed, "abs": abs(signed),
            "entry": self._num(p, "avg_entry_price", "entry_price"),
            "unrealized_pnl": self._num(p, "unrealized_pnl", "unrealizedPnl") or 0.0,
            "realized_pnl": self._num(p, "realized_pnl", "realizedPnl") or 0.0,
            # Isolated positions carry their own liquidation price (verified live, e.g. isolated
            # ANTHROPIC long entry 2168.8 -> liq 1973.04); cross-margin positions report 0 because
            # liquidation is account-wide — callers treat 0/None as "unknown".
            "liq": self._num(p, "liquidation_price") or None,
            "margin_mode": self._num(p, "margin_mode"),
        }

    async def balance(self) -> dict:
        """Account balance: total asset value and what's free (fields per the AccountApi model)."""
        acc = await self._account()
        pick = lambda *names: next((float(getattr(acc, n)) for n in names
                                    if getattr(acc, n, None) not in (None, "")), None)
        return {
            "total": pick("total_asset_value", "collateral", "portfolio_value"),
            "available": pick("available_balance", "cross_asset_value", "collateral"),
        }

    async def position(self, market_index: int) -> dict | None:
        """The account's open position on one market, or None if flat. `size > 0` long, `< 0` short."""
        acc = await self._account()
        for p in (getattr(acc, "positions", None) or []):
            mid = self._num(p, "market_id", "market_index")
            if mid is None or int(mid) != int(market_index):
                continue
            return self._pos_dict(p)
        return None

    @staticmethod
    def order_error(resp) -> str | None:
        """Rejection reason from a create_order return, or None if accepted.

        The SDK returns a 3-tuple: (CreateOrder, RespSendTx, None) on success and (None, None,
        error_string) on failure — so the THIRD element is the authoritative error (any non-empty
        value means the order did NOT go through, regardless of wording). Other shapes are handled
        best-effort."""
        if resp is None:
            return None
        if isinstance(resp, tuple):
            if len(resp) >= 3:                       # canonical (result, tx_hash, err)
                err = resp[2]
                return str(err)[:200] if err else None
            for part in resp:
                if isinstance(part, Exception):
                    return str(part)[:200]
            resp = resp[-1] if resp else None
        for attr in ("error", "err", "message", "reason"):
            # objects expose these as attributes, dict responses as keys — check both
            v = resp.get(attr) if isinstance(resp, dict) else getattr(resp, attr, None)
            if v:
                return str(v)[:200]
        s = str(resp)
        low = s.lower()
        if any(k in low for k in ("error", "rejected", "insufficient", "invalid", "fail")):
            return s[:200]
        return None

    async def open_positions(self) -> list[dict]:
        """All non-flat positions on the account: [{market_id, size(signed), abs, entry, ...}]. One
        read — used to flatten everything fast on STOP without a per-coin round trip."""
        try:
            acc = await self._account()
        except Exception:  # noqa: BLE001
            return []
        return [self._pos_dict(p) for p in (getattr(acc, "positions", None) or [])
                if (self._num(p, "position", "base_amount", "size") or 0.0) != 0]

    @staticmethod
    def _num(obj, *names):
        for n in names:
            v = getattr(obj, n, None)
            if v not in (None, ""):
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return None
        return None

    async def limit_order(self, market_index: int, base_amount: int, price: int, is_ask: bool,
                          *, post_only: bool = False, reduce_only: bool = False, ioc: bool = False,
                          client_order_index: int = 0) -> object:
        """Place a limit order. `is_ask` True = sell/short, False = buy/long. `base_amount` and
        `price` are already integer-scaled for this market. `ioc` crosses now-or-cancels the rest
        (used with a deliberately aggressive price + reduce_only to flatten a leg)."""
        tif = self.TIF_IOC if ioc else (self.TIF_POST_ONLY if post_only else self.TIF_GTT)
        order_expiry = self.IOC_EXPIRY if ioc else self.GTT_EXPIRY
        return await self._signer.create_order(
            market_index=market_index,
            client_order_index=client_order_index,
            base_amount=base_amount,
            price=price,
            is_ask=is_ask,
            order_type=self.ORDER_TYPE_LIMIT,
            time_in_force=tif,
            reduce_only=reduce_only,
            order_expiry=order_expiry,
            api_key_index=self._api_key_index,
        )

    async def stop_loss(self, market_index: int, base_amount: int, trigger_price: int, price: int,
                        is_ask: bool, client_order_index: int = 0) -> object:
        """Reduce-only stop-loss (market on trigger). Integer-scaled amounts like limit_order; `price` is
        the worst execution price accepted. SDK: create_sl_order(market_index, client_order_index,
        base_amount, trigger_price, price, is_ask, reduce_only) — it sets ORDER_TYPE_STOP_LOSS with
        IOC + the 28-day expiry itself. Returns the SDK's (order, resp, err) tuple."""
        return await self._signer.create_sl_order(
            market_index, client_order_index, base_amount, trigger_price, price, is_ask, True,
            api_key_index=self._api_key_index,
        )

    async def set_leverage(self, market_index: int, leverage: int, *, isolated: bool = True) -> object:
        """Set leverage for a market. SDK signature is update_leverage(market_index, margin_mode,
        leverage) with margin_mode 0=cross, 1=isolated — passing it wrong (my earlier guess) failed
        silently and left the old leverage on the exchange. Returns the SDK's (tx, resp, err) tuple;
        use order_error() to read the reason."""
        mode = self.ISOLATED_MARGIN_MODE if isolated else self.CROSS_MARGIN_MODE
        return await self._signer.update_leverage(market_index, mode, int(leverage))

    async def cancel_all(self) -> object:
        # SDK requires (time_in_force, timestamp_ms); calling it bare raised and was swallowed, so STOP
        # never actually cancelled Lighter orders. Immediate mode + a short future deadline.
        return await self._signer.cancel_all_orders(
            self.CANCEL_ALL_TIF_IMMEDIATE, int(time.time() * 1000) + 60_000)

    async def close(self) -> None:
        """Release the SDK's HTTP/WS resources."""
        await self._signer.close()
