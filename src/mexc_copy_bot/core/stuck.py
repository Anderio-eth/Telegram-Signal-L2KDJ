"""Hands-on management of accounts that fell out of step with the master.

A follower whose limit did not fill is stranded: it holds a position the master has exited, or
missed one the master is in. It stops following the master (see CopyService._eligible_followers)
until a person decides what to do with it, and this is what carries those decisions out.

Three things can be done, and which of them make sense depends on how the account got stuck:

  · stuck on the way OUT — still holding. Close it at market, or move the resting limit.
  · stuck on the way IN  — holding nothing. Enter at market, move the limit, or drop the entry.

Everything here works on a group at a time but takes an explicit list of accounts, because the
user chooses who to act on: closing eight of nine and leaving one is a legitimate thing to want.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import aiohttp

from ..db.store import KIND_EXIT, Account, StuckGroup, Store
from ..mexc.rest import (
    ORDER_TYPE_LIMIT,
    SIDE_CLOSE_LONG,
    SIDE_CLOSE_SHORT,
    SIDE_OPEN_LONG,
    SIDE_OPEN_SHORT,
    MexcError,
    MexcRestClient,
)

LOGGER = logging.getLogger(__name__)


class StuckManager:
    def __init__(self, store: Store, session: aiohttp.ClientSession, owner_id: int) -> None:
        self._store = store
        self._session = session
        self._owner_id = owner_id

    async def _client(self, account_id: int) -> MexcRestClient | None:
        credentials = await self._store.get_credentials(account_id, self._owner_id)
        if not credentials:
            return None
        return MexcRestClient(*credentials, session=self._session)

    # ── closing what is held ────────────────────────────────────────────────────────────────
    async def close_at_market(
        self, group: StuckGroup, account_ids: list[int]
    ) -> list[tuple[str, str]]:
        """Get out now, at whatever the book offers. Returns (label, outcome) per account."""
        members = {m.account_id: m for m in group.members if m.account_id in account_ids}

        async def one(account_id: int) -> tuple[str, str]:
            member = members[account_id]
            client = await self._client(account_id)
            if not client:
                return member.label, "немає ключів"
            # The resting limit goes first. Left in place it could fill moments after the market
            # close and reopen the position from the other side.
            if member.follower_order_id:
                try:
                    await client.cancel_orders([member.follower_order_id])
                except MexcError as err:
                    LOGGER.info("account %s: cancel before close failed: %s", account_id, err.message)
            try:
                await client.close_all(group.symbol)
                return member.label, "закрито"
            except MexcError as err:
                return member.label, err.message or "помилка"

        results = await asyncio.gather(*(one(a) for a in members), return_exceptions=True)
        out: list[tuple[str, str]] = []
        resolved: list[int] = []
        for account_id, res in zip(members, results, strict=True):
            if isinstance(res, tuple):
                out.append(res)
                if res[1] == "закрито":
                    resolved.append(account_id)
            else:
                out.append((members[account_id].label, str(res)[:80]))
        await self._store.resolve_stuck_accounts(group.id, resolved)
        return out

    # ── entering what was missed ────────────────────────────────────────────────────────────
    async def enter_at_market(
        self, group: StuckGroup, account_ids: list[int]
    ) -> list[tuple[str, str]]:
        """Take the entry the limit never got, at market."""
        members = {m.account_id: m for m in group.members if m.account_id in account_ids}
        side = SIDE_OPEN_LONG if group.position_type == 1 else SIDE_OPEN_SHORT

        async def one(account_id: int) -> tuple[str, str]:
            member = members[account_id]
            client = await self._client(account_id)
            if not client:
                return member.label, "немає ключів"
            if member.follower_order_id:
                try:
                    await client.cancel_orders([member.follower_order_id])
                except MexcError as err:
                    LOGGER.info("account %s: cancel before entry failed: %s", account_id, err.message)
            try:
                await client.submit_order(
                    symbol=group.symbol, side=side, vol=member.vol,
                    leverage=group.leverage or None, open_type=group.open_type,
                    external_oid=f"st{group.id}-{account_id}-{uuid.uuid4().hex[:6]}",
                )
                return member.label, "увійшов"
            except MexcError as err:
                return member.label, err.message or "помилка"

        results = await asyncio.gather(*(one(a) for a in members), return_exceptions=True)
        out: list[tuple[str, str]] = []
        resolved: list[int] = []
        for account_id, res in zip(members, results, strict=True):
            if isinstance(res, tuple):
                out.append(res)
                if res[1] == "увійшов":
                    resolved.append(account_id)
            else:
                out.append((members[account_id].label, str(res)[:80]))
        await self._store.resolve_stuck_accounts(group.id, resolved)
        return out

    # ── giving up on an entry ───────────────────────────────────────────────────────────────
    async def cancel_entry(self, group: StuckGroup, account_ids: list[int]) -> list[tuple[str, str]]:
        """Drop the unfilled entry. The account goes back under the master, but stays out of the
        position the master is currently in — it waits for the next one."""
        members = {m.account_id: m for m in group.members if m.account_id in account_ids}

        async def one(account_id: int) -> tuple[str, str]:
            member = members[account_id]
            client = await self._client(account_id)
            if not client:
                return member.label, "немає ключів"
            if not member.follower_order_id:
                return member.label, "скасовано"
            try:
                await client.cancel_orders([member.follower_order_id])
            except MexcError as err:
                # Already gone is the outcome we wanted anyway.
                LOGGER.info("account %s: cancel returned %s", account_id, err.message)
            return member.label, "скасовано"

        results = await asyncio.gather(*(one(a) for a in members), return_exceptions=True)
        out = [r if isinstance(r, tuple) else (str(r)[:40], "помилка") for r in results]
        await self._store.resolve_stuck_accounts(
            group.id, [a for a, r in zip(members, results, strict=True) if isinstance(r, tuple)]
        )
        return out

    # ── moving the resting limit ────────────────────────────────────────────────────────────
    async def move_limit(self, group: StuckGroup, price: float) -> list[tuple[str, str]]:
        """Cancel each account's resting order and place a fresh one at the new price.

        Cancel-then-place rather than an amend: MEXC has no amend for these, and placing before
        cancelling would leave both live for a moment — long enough, on a fast market, to fill
        twice.
        """
        if group.kind == KIND_EXIT:
            side = SIDE_CLOSE_LONG if group.position_type == 1 else SIDE_CLOSE_SHORT
        else:
            side = SIDE_OPEN_LONG if group.position_type == 1 else SIDE_OPEN_SHORT

        async def one(member) -> tuple[str, str, str | None]:
            client = await self._client(member.account_id)
            if not client:
                return member.label, "немає ключів", None
            if member.follower_order_id:
                try:
                    await client.cancel_orders([member.follower_order_id])
                except MexcError as err:
                    LOGGER.info("account %s: cancel failed: %s", member.account_id, err.message)
            try:
                result = await client.submit_order(
                    symbol=group.symbol, side=side, vol=member.vol,
                    leverage=group.leverage or None, open_type=group.open_type,
                    order_type=ORDER_TYPE_LIMIT, price=price,
                    external_oid=f"mv{group.id}-{member.account_id}-{uuid.uuid4().hex[:6]}",
                )
                return member.label, "переставлено", str(result) if result is not None else None
            except MexcError as err:
                return member.label, err.message or "помилка", None

        results = await asyncio.gather(*(one(m) for m in group.members), return_exceptions=True)
        out: list[tuple[str, str]] = []
        order_ids: dict[int, str | None] = {}
        for member, res in zip(group.members, results, strict=True):
            if isinstance(res, tuple):
                label, outcome, order_id = res
                out.append((label, outcome))
                if outcome == "переставлено":
                    order_ids[member.account_id] = order_id
            else:
                out.append((member.label, str(res)[:80]))
        if order_ids:
            await self._store.set_stuck_limit(group.id, price, order_ids)
        return out
