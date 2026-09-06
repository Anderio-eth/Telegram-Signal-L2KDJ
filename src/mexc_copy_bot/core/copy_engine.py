"""Executes one master event across every active follower.

Two rules drive the whole design:

  · Accounts are independent (spec §16). Followers run concurrently and each one's outcome is
    recorded on its own; one account out of balance must never cancel, roll back or delay the
    other eight. Nothing here raises past a single account's boundary.

  · Closing goes through close_all, never through an opposite-side order. Verified on a live
    hedge-mode account: submitting side=3 ("close long") against an open long did not close it —
    MEXC opened a fresh short alongside, at the account's default leverage. For a copy bot that
    is the worst possible failure, since a "close" would leave every follower doubly exposed.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass

import aiohttp

from ..db.store import Account, PositionRow, Store
from ..mexc.rest import (
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    MexcError,
    MexcRestClient,
)
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
PERMANENT_ERROR_TEXT = ("insufficient", "balance", "invalid", "not exist", "permission", "signature")

RETRY_DELAYS = (0.2, 0.5, 1.0)


@dataclass
class FollowerResult:
    account: Account
    ok: bool
    action: Action
    vol: float
    error: str | None = None


def _is_permanent(err: MexcError) -> bool:
    if err.code in PERMANENT_ERROR_CODES:
        return True
    text = (err.message or "").lower()
    return any(word in text for word in PERMANENT_ERROR_TEXT)


class CopyEngine:
    def __init__(self, store: Store, session: aiohttp.ClientSession, *, retry_attempts: int = 3) -> None:
        self._store = store
        self._session = session
        self._retry_attempts = max(1, retry_attempts)

    async def execute(self, event: MasterEvent, event_id: int, followers: list[Account]) -> list[FollowerResult]:
        """Apply one master event to every follower, concurrently."""
        if not followers:
            return []
        results = await asyncio.gather(
            *(self._run_follower(event, event_id, follower) for follower in followers),
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

    async def _run_follower(self, event: MasterEvent, event_id: int, follower: Account) -> FollowerResult:
        vol = event.delta_vol * follower.size_multiplier
        external_oid = f"cp{event_id}-{follower.id}-{uuid.uuid4().hex[:8]}"

        task_id = await self._store.create_task(
            event_id=event_id,
            account_id=follower.id,
            action=event.action.value,
            symbol=event.symbol,
            position_type=event.position_type,
            vol=vol,
            leverage=event.leverage,
            open_type=event.open_type,
            external_oid=external_oid,
        )
        if task_id is None:
            # A task already exists for this (event, follower) — the duplicate guard from the
            # schema. Nothing to do, and definitely nothing to re-send.
            LOGGER.info("task already exists for event=%s follower=%s, skipping", event_id, follower.id)
            return FollowerResult(follower, True, event.action, vol, None)

        credentials = await self._store.get_credentials(follower.id, follower.owner_id)
        if not credentials:
            await self._store.finish_task(task_id, status="FAILED", attempts=0, error="credentials missing")
            return FollowerResult(follower, False, event.action, vol, "credentials missing")

        api_key, secret = credentials
        client = MexcRestClient(api_key, secret, session=self._session)

        attempts = 0
        last_error: str | None = None
        for attempt in range(self._retry_attempts):
            attempts = attempt + 1
            try:
                await self._apply(client, event, follower, vol, external_oid)
                await self._store.finish_task(task_id, status="SUCCESS", attempts=attempts, error=None)
                await self._store.set_account_error(follower.id, None)
                await self._record_expected_position(event, follower, vol)
                return FollowerResult(follower, True, event.action, vol, None)
            except MexcError as err:
                last_error = err.message or str(err)
                if _is_permanent(err):
                    LOGGER.warning("follower %s permanent failure: %s", follower.id, last_error)
                    break
                LOGGER.info("follower %s attempt %s failed: %s", follower.id, attempts, last_error)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                last_error = f"network: {err}"
            if attempt < self._retry_attempts - 1:
                await asyncio.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])

        await self._store.finish_task(task_id, status="FAILED", attempts=attempts, error=last_error)
        await self._store.set_account_error(follower.id, last_error)
        return FollowerResult(follower, False, event.action, vol, last_error)

    async def _apply(
        self, client: MexcRestClient, event: MasterEvent, follower: Account, vol: float, external_oid: str
    ) -> None:
        if event.action is Action.CLOSE:
            # See the module docstring: close_all is the only reliable way out of a hedge-mode
            # position. It closes both sides of the symbol, which is correct here — the master
            # holding nothing means the follower should hold nothing.
            await client.close_all(event.symbol)
            return

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
        side = SIDE_OPEN_LONG if event.position_type == 1 else SIDE_OPEN_SHORT
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
                    position_type=event.position_type,
                )
            except MexcError as err:
                LOGGER.info("follower %s leverage set failed (continuing): %s", follower.id, err.message)

        await client.submit_order(
            symbol=event.symbol,
            side=side,
            vol=vol,
            leverage=event.leverage or None,
            open_type=event.open_type,
            external_oid=external_oid,
        )

    async def _record_expected_position(self, event: MasterEvent, follower: Account, vol: float) -> None:
        """Track what this follower's position should now be, for reconciliation to check."""
        if event.action is Action.CLOSE:
            await self._store.delete_position(follower.id, event.symbol, event.position_type)
            return

        existing = await self._store.get_positions(follower.id)
        current = existing.get((event.symbol, event.position_type))
        new_vol = (current.hold_vol if current else 0.0) + vol
        await self._store.upsert_position(
            PositionRow(
                account_id=follower.id,
                symbol=event.symbol,
                position_type=event.position_type,
                hold_vol=new_vol,
                leverage=event.leverage,
                open_type=event.open_type,
            )
        )
