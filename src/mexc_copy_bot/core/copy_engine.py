"""Executes one master event across every active follower.

Two rules drive the whole design:

  · Accounts are independent (spec §16). Followers run concurrently and each one's outcome is
    recorded on its own; one account out of balance must never cancel, roll back or delay the
    other eight. Nothing here raises past a single account's boundary.

  · A follower's side is decided here, not inherited. In REVERSE mode the chosen account takes
    the opposite side of the master — an automatic hedge — and everything downstream (the order
    side, the leverage call, the expected-position row) has to use that side rather than the
    master's, or reconciliation will chase a position that was never opened.

  · Closing goes through close_all, never through an opposite-side order. Verified on a live
    hedge-mode account: submitting side=3 ("close long") against an open long did not close it —
    MEXC opened a fresh short alongside, at the account's default leverage. For a copy bot that
    is the worst possible failure, since a "close" would leave every follower doubly exposed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import uuid
from dataclasses import dataclass
from typing import Any

import aiohttp

from ..db.store import Account, PositionRow, Store
from ..mexc.rest import (
    ORDER_TYPE_LIMIT,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    MexcError,
    MexcRestClient,
    get_contract_specs,
)
from .orders import MasterOrder
from .events import Action, MasterEvent

LOGGER = logging.getLogger(__name__)

# Errors that retrying cannot fix (spec §17). Retrying these just burns rate limit and delays the
# report; they are reported to the user immediately instead.
PERMANENT_ERROR_CODES = {
    2005,  # insufficient balance
    1002,  # contract does not exist / invalid symbol
    3005,  # invalid leverage
    600,   # parameter error
    602,   # signature/authentication failure
}
PERMANENT_ERROR_TEXT = ("insufficient", "balance", "invalid", "permission", "signature")

# A close that finds no position has already achieved what it was asked for. Reported as a
# failure it produced nine red lines per close on accounts whose open had failed earlier —
# alarming, and hiding the one failure that actually mattered.
ALREADY_FLAT_TEXT = ("position is nonexistent", "position not exist", "no position")

# A contract MEXC does not allow through the API at all. Retrying cannot help, and the message it
# returns ("Contract not activated") explains nothing to whoever reads the report.
BLOCKED_CONTRACT_TEXT = ("not activated", "contract not activated")

# Rate limiting is transient, but not on the timescale of an ordinary blip: retrying in 200ms just
# spends the next allowance. These waits are long enough for the window to actually roll over.
RETRY_DELAYS = (0.2, 0.5, 1.0)
RATE_LIMIT_DELAYS = (1.5, 4.0, 8.0)
RATE_LIMIT_TEXT = ("too frequent", "rate limit", "too many requests")

# MEXC's position types: 1 long, 2 short. Reversing is just swapping the two.
OPPOSITE_SIDE = {1: 2, 2: 1}


def side_for(follower: Account, master_side: int, honour_direction: bool) -> int:
    """Which side this account takes for a master action on `master_side`.

    In COPY mode everyone follows the master and the per-account setting is ignored, so it
    survives switching modes back and forth instead of being reset. In REVERSE mode each account
    trades the way it was set, which is what lets five go long while three go short on the same
    master move.
    """
    if honour_direction and follower.is_reversed:
        return OPPOSITE_SIDE[master_side]
    return master_side


@dataclass
class FollowerResult:
    account: Account
    ok: bool
    action: Action
    vol: float
    error: str | None = None
    # Realised PnL for a close, straight from the exchange. None means "not known" — an open, or
    # a close whose settlement could not be read — and must never be shown as a zero.
    realized_pnl: float | None = None
    # The side this account actually took. Worth carrying separately from the master's: with
    # per-account directions they are not the same thing, and a report that showed only the
    # master's would say every account went long while half of them went short.
    position_type: int | None = None


# How long to keep asking the exchange what a just-closed position settled at. Settlement is not
# instant, and reporting "unknown" on a close that simply had not settled yet would be noise.
PNL_POLL_DELAYS = (0.0, 0.5, 1.0, 2.0)


def _order_id_from(payload: Any) -> str | None:
    """MEXC returns a new order id either bare or wrapped; accept both, reject neither silently."""
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get("orderId") or payload.get("id")
        return str(value) if value else None
    if isinstance(payload, (str, int)):
        return str(payload)
    return None


def _is_already_flat(err: MexcError) -> bool:
    text = (err.message or "").lower()
    return any(word in text for word in ALREADY_FLAT_TEXT)


def _is_rate_limit(err: MexcError) -> bool:
    text = (err.message or "").lower()
    return any(word in text for word in RATE_LIMIT_TEXT)


def _is_blocked_contract(err: MexcError) -> bool:
    text = (err.message or "").lower()
    return any(word in text for word in BLOCKED_CONTRACT_TEXT)


def _is_permanent(err: MexcError) -> bool:
    if _is_rate_limit(err):
        # Transient despite reading like a rejection; it just needs a longer wait.
        return False
    if _is_blocked_contract(err):
        return True
    if err.code in PERMANENT_ERROR_CODES:
        return True
    text = (err.message or "").lower()
    return any(word in text for word in PERMANENT_ERROR_TEXT)


class CopyEngine:
    def __init__(self, store: Store, session: aiohttp.ClientSession, *, retry_attempts: int = 3) -> None:
        self._store = store
        self._session = session
        self._retry_attempts = max(1, retry_attempts)

    async def execute(
        self,
        event: MasterEvent,
        event_id: int,
        followers: list[Account],
        *,
        reverse: bool = False,
        stops: tuple[float | None, float | None] = (None, None),
    ) -> list[FollowerResult]:
        """Apply one master event to every follower, concurrently.

        `reverse` means "honour each account's own direction" — in that mode a follower marked
        REVERSE takes the opposite side and one marked COPY still follows the master, so a single
        master move can put some accounts long and others short.
        `stops` are the master's (stop loss, take profit) to put on the opening order.
        """
        if not followers:
            return []

        blocked = await self._blocked_contract_reason(event)
        if blocked:
            # One lookup instead of nine rejected orders, and a message that names the real cause.
            LOGGER.warning("skipping %s: %s", event.symbol, blocked)
            return [FollowerResult(f, False, event.action, 0.0, blocked) for f in followers]

        results = await asyncio.gather(
            *(self._run_follower(event, event_id, follower, reverse, stops) for follower in followers),
            return_exceptions=True,
        )

        out: list[FollowerResult] = []
        for follower, result in zip(followers, results, strict=True):
            if isinstance(result, FollowerResult):
                out.append(result)
            else:
                # gather() with return_exceptions keeps one account's crash from cancelling the
                # rest; it still has to be reported rather than swallowed.
                LOGGER.exception("follower %s crashed", follower.id, exc_info=result)
                out.append(FollowerResult(follower, False, event.action, 0.0, str(result)[:200]))
        return out

    async def mirror_resting_order(
        self,
        order: MasterOrder,
        followers: list[Account],
        *,
        reverse: bool = False,
        vol_by_account: dict[int, float] | None = None,
    ) -> list[tuple[Account, str | None, str | None]]:
        """Place each follower's own copy of a resting master order.

        The point of the whole exercise: the follower's limit sits in the book at the master's
        price and fills alongside it, instead of the bot noticing the master's fill afterwards and
        paying the spread and taker fee to catch up.

        Returns (account, follower order id, error) per account.
        """
        async def one(follower: Account) -> tuple[Account, str | None, str | None]:
            credentials = await self._store.get_credentials(follower.id, follower.owner_id)
            if not credentials:
                return follower, None, "credentials missing"
            client = MexcRestClient(*credentials, session=self._session)
            side = order.side
            if reverse and follower.is_reversed:
                # Opposite side of the same book. Not a price change: both accounts want the same
                # price, they just want opposite exposure at it.
                side = {1: 4, 4: 1, 2: 3, 3: 2}[order.side]
            try:
                if order.leverage:
                    position_type = order.position_type
                    with contextlib.suppress(MexcError):
                        await client.set_leverage(
                            position_id=None, leverage=order.leverage,
                            open_type=order.open_type, symbol=order.symbol,
                            position_type=position_type,
                        )
                tag = f"lm{order.order_id}-{follower.id}-{uuid.uuid4().hex[:6]}"
                vol = (
                    vol_by_account[follower.id]
                    if vol_by_account is not None
                    else order.vol * follower.size_multiplier
                )
                result = await client.submit_order(
                    symbol=order.symbol,
                    side=side,
                    vol=vol,
                    leverage=order.leverage or None,
                    open_type=order.open_type,
                    order_type=ORDER_TYPE_LIMIT,
                    price=order.price,
                    external_oid=tag,
                )
                order_id = _order_id_from(result)
                if not order_id:
                    # The order exists on the exchange but we cannot name it, which would leave it
                    # resting with no way to pull it when the master cancels. Find it by the tag we
                    # sent, rather than leaving an untrackable live order on someone's account.
                    order_id = await self._find_by_tag(client, order.symbol, tag)
                return follower, order_id, None
            except MexcError as err:
                return follower, None, err.message or str(err)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                return follower, None, f"network: {err}"

        results = await asyncio.gather(*(one(f) for f in followers), return_exceptions=True)
        out: list[tuple[Account, str | None, str | None]] = []
        for follower, res in zip(followers, results, strict=True):
            if isinstance(res, tuple):
                out.append(res)
            else:
                LOGGER.exception("follower %s crashed placing a limit", follower.id, exc_info=res)
                out.append((follower, None, str(res)[:200]))
        return out

    @staticmethod
    async def _find_by_tag(client: MexcRestClient, symbol: str, tag: str) -> str | None:
        """Recover an order's id from the client id we chose for it."""
        try:
            for row in await client.get_open_orders(symbol):
                if str(row.get("externalOid") or "") == tag:
                    return str(row.get("orderId"))
        except (MexcError, aiohttp.ClientError, asyncio.TimeoutError) as err:
            LOGGER.warning("could not recover order id for %s: %s", tag, err)
        return None

    async def cancel_mirrored_orders(self, pairs: list[tuple[Account, str]]) -> int:
        """Pull the followers' copies. Best effort: an order that already filled or was cancelled
        by hand is not an error worth surfacing."""
        async def one(follower: Account, order_id: str) -> bool:
            credentials = await self._store.get_credentials(follower.id, follower.owner_id)
            if not credentials:
                return False
            client = MexcRestClient(*credentials, session=self._session)
            try:
                await client.cancel_orders([order_id])
                return True
            except MexcError as err:
                LOGGER.info("follower %s cancel failed: %s", follower.id, err.message)
                return False
            except (aiohttp.ClientError, asyncio.TimeoutError):
                return False

        results = await asyncio.gather(*(one(f, o) for f, o in pairs), return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _blocked_contract_reason(self, event: MasterEvent) -> str | None:
        """Whether MEXC forbids API orders on this contract, as a message worth showing.

        Checked before ordering, not after: the venue's own rejection is "Contract not activated",
        which sounds like something the account owner can switch on. They cannot — it is a
        per-contract flag on MEXC's side, usually on new or thin listings, and the position stays
        openable in the app while no API can mirror it.

        A close is never blocked: getting out must not depend on being allowed in.
        """
        if event.action is Action.CLOSE or self._session is None:
            return None
        try:
            specs = await get_contract_specs(self._session, event.symbol)
        except Exception:  # noqa: BLE001 — an unavailable check must not stop a trade
            return None
        spec = specs.get(event.symbol)
        if spec and not spec.api_allowed:
            return f"{event.symbol}: MEXC blocks API trading on this contract"
        return None

    async def _run_follower(
        self,
        event: MasterEvent,
        event_id: int,
        follower: Account,
        reverse: bool = False,
        stops: tuple[float | None, float | None] = (None, None),
    ) -> FollowerResult:
        vol = event.delta_vol * follower.size_multiplier
        side = side_for(follower, event.position_type, reverse)
        external_oid = f"cp{event_id}-{follower.id}-{uuid.uuid4().hex[:8]}"

        task_id = await self._store.create_task(
            event_id=event_id,
            account_id=follower.id,
            action=event.action.value,
            symbol=event.symbol,
            position_type=side,
            vol=vol,
            leverage=event.leverage,
            open_type=event.open_type,
            external_oid=external_oid,
        )
        if task_id is None:
            # A task already exists for this (event, follower) — the duplicate guard from the
            # schema. Nothing to do, and definitely nothing to re-send.
            LOGGER.info("task already exists for event=%s follower=%s, skipping", event_id, follower.id)
            return FollowerResult(follower, True, event.action, vol, None, None, side)

        credentials = await self._store.get_credentials(follower.id, follower.owner_id)
        if not credentials:
            await self._store.finish_task(task_id, status="FAILED", attempts=0, error="credentials missing")
            return FollowerResult(follower, False, event.action, vol, "credentials missing", None, side)

        api_key, secret = credentials
        client = MexcRestClient(api_key, secret, session=self._session)

        attempts = 0
        last_error: str | None = None
        delays = RETRY_DELAYS
        for attempt in range(self._retry_attempts):
            attempts = attempt + 1
            try:
                realized = await self._apply(client, event, follower, vol, external_oid, side, stops)
                await self._store.finish_task(
                    task_id, status="SUCCESS", attempts=attempts, error=None, realized_pnl=realized
                )
                await self._store.set_account_error(follower.id, None)
                await self._record_expected_position(event, follower, vol, side)
                return FollowerResult(follower, True, event.action, vol, None, realized, side)
            except MexcError as err:
                last_error = err.message or str(err)
                if _is_blocked_contract(err):
                    # Say what actually happened. "Contract not activated" reads like an account
                    # setting the user could fix; it is MEXC refusing API orders on this contract.
                    last_error = f"{event.symbol}: MEXC blocks API trading on this contract"
                    LOGGER.warning("follower %s: %s", follower.id, last_error)
                    break
                if _is_permanent(err):
                    LOGGER.warning("follower %s permanent failure: %s", follower.id, last_error)
                    break
                rate_limited = _is_rate_limit(err)
                delays = RATE_LIMIT_DELAYS if rate_limited else RETRY_DELAYS
                LOGGER.info("follower %s attempt %s failed: %s", follower.id, attempts, last_error)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                last_error = f"network: {err}"
                delays = RETRY_DELAYS
            if attempt < self._retry_attempts - 1:
                # Jittered so nine followers backing off together do not retry in lockstep and
                # recreate the burst that rate-limited them in the first place.
                base = delays[min(attempt, len(delays) - 1)]
                await asyncio.sleep(base * (1.0 + random.random() * 0.4))

        await self._store.finish_task(task_id, status="FAILED", attempts=attempts, error=last_error)
        await self._store.set_account_error(follower.id, last_error)
        return FollowerResult(follower, False, event.action, vol, last_error, None, side)

    async def _apply(
        self,
        client: MexcRestClient,
        event: MasterEvent,
        follower: Account,
        vol: float,
        external_oid: str,
        side: int,
        stops: tuple[float | None, float | None] = (None, None),
    ) -> float | None:
        """Returns realised PnL when this was a close and the exchange reported it."""
        if event.action is Action.CLOSE:
            # See the module docstring: close_all is the only reliable way out of a hedge-mode
            # position. It closes both sides of the symbol, which is correct here — the master
            # holding nothing means the follower should hold nothing.
            #
            # The ids are read first because that is the only reliable way to attribute the
            # settlement afterwards: history is per symbol, and matching on "most recent" would
            # pick up an unrelated earlier close on the same symbol.
            try:
                open_ids = [p.position_id for p in await client.get_open_positions(event.symbol) if p.hold_vol > 0]
            except MexcError:
                open_ids = []
            if not open_ids:
                # Nothing to close, so the follower is already where the master is. Calling
                # close_all anyway returns an error that would be reported as a failed close.
                LOGGER.info("follower %s already flat on %s", follower.id, event.symbol)
                return None
            try:
                await client.close_all(event.symbol)
            except MexcError as err:
                if not _is_already_flat(err):
                    raise
                LOGGER.info("follower %s: %s was already closed", follower.id, event.symbol)
                return None
            return await self._realized_pnl(client, event.symbol, open_ids)

        if event.action is Action.DECREASE:
            # A partial reduction has the same hazard as a close: an opposite-side order would
            # open a hedge instead of reducing. Until a verified reduce-only path exists, the
            # honest thing is to refuse rather than silently double the follower's exposure.
            raise MexcError(
                None,
                "partial decrease not supported yet (opposite-side orders open a hedge on MEXC)",
                endpoint="decrease",
            )

        # OPEN and INCREASE are both "add this many contracts on this side".
        # (No realised PnL to report for these; the return below is the whole method's result.)
        order_side = SIDE_OPEN_LONG if side == 1 else SIDE_OPEN_SHORT
        if event.leverage:
            # Leverage must be right BEFORE the order, or the position opens with the account's
            # previous setting (spec §13). Failing to set it is not fatal on its own — MEXC
            # rejects impossible leverage at order time anyway — so it is logged, not raised.
            try:
                await client.set_leverage(
                    position_id=None,
                    leverage=event.leverage,
                    open_type=event.open_type,
                    symbol=event.symbol,
                    position_type=side,
                )
            except MexcError as err:
                LOGGER.info("follower %s leverage set failed (continuing): %s", follower.id, err.message)

        stop_loss, take_profit = stops
        await client.submit_order(
            symbol=event.symbol,
            side=order_side,
            vol=vol,
            leverage=event.leverage or None,
            open_type=event.open_type,
            external_oid=external_oid,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit,
        )

    async def _realized_pnl(
        self, client: MexcRestClient, symbol: str, position_ids: list[int]
    ) -> float | None:
        """Sum what the just-closed positions actually settled at.

        Returns None unless every position is accounted for. A partial sum presented as "the PnL"
        is worse than no number at all — it looks authoritative while quietly omitting a leg,
        which in hedge mode is exactly the leg that went the other way.
        """
        if not position_ids:
            return None

        outstanding = set(position_ids)
        total = 0.0
        for delay in PNL_POLL_DELAYS:
            if delay:
                await asyncio.sleep(delay)
            try:
                closed = await client.get_closed_positions(symbol)
            except (MexcError, aiohttp.ClientError, asyncio.TimeoutError) as err:
                LOGGER.info("could not read settlement for %s: %s", symbol, err)
                continue
            for position in closed:
                if position.position_id in outstanding:
                    total += position.realised
                    outstanding.discard(position.position_id)
            if not outstanding:
                return total

        LOGGER.info("settlement incomplete for %s: %s still unsettled", symbol, sorted(outstanding))
        return None

    async def _record_expected_position(
        self, event: MasterEvent, follower: Account, vol: float, side: int
    ) -> None:
        """Track what this follower's position should now be, for reconciliation to check.

        Keyed on the side the follower actually took, which in REVERSE mode is not the master's.
        """
        if event.action is Action.CLOSE:
            await self._store.delete_position(follower.id, event.symbol, side)
            return

        existing = await self._store.get_positions(follower.id)
        current = existing.get((event.symbol, side))
        new_vol = (current.hold_vol if current else 0.0) + vol
        await self._store.upsert_position(
            PositionRow(
                account_id=follower.id,
                symbol=event.symbol,
                position_type=side,
                hold_vol=new_vol,
                leverage=event.leverage,
                open_type=event.open_type,
            )
        )
