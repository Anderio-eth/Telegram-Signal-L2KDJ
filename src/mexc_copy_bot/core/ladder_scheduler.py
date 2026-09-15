"""Fires armed ladders at their time, from one background loop, whoever is watching.

An armed entry is a row in the database, not a task tied to the menu that armed it: the target time
may be hours off, the person will not sit and wait, and a redeploy must not lose it. So this loop
polls for entries whose start window has arrived, claims each atomically (ARMED → RUNNING, so a
second process could never double-fire one), re-plans it against live price and balances at that
moment, and runs it through the same LadderExecutor the tests cover. The outcome is stored and
handed to `on_report` for the owner's topic.

Re-planning at fire time rather than trusting numbers frozen at arm time is deliberate: price and
free balance move, and a ladder that no longer fits (too late, a slice below the minimum, margin
gone) should say so and be marked FAILED, not send a broken half of itself.

A ladder found still RUNNING at startup was interrupted by a crash mid-fire; it is marked, never
re-fired — re-opening a timed position late is worse than not opening it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

import aiohttp

from ..exchange import make_rest_client
from ..hibt import rest as hibt_rest
from .ladder import LadderConfig, LadderExecutor, plan_ladder

LOGGER = logging.getLogger(__name__)

POLL_SECONDS = 1.0
# Claim a ladder this many seconds before its slices need to start, so re-planning and leverage
# presetting are done before the clock matters. Comfortably more than any measured latency.
CLAIM_LEAD_SECONDS = 8.0
# How far ahead to fetch candidates. The precise per-ladder check is in _tick; this only has to be
# wide enough that a ladder with many widely-spaced slices is fetched before it needs claiming.
LOOKAHEAD_SECONDS = CLAIM_LEAD_SECONDS + 3600.0


class LadderScheduler:
    def __init__(self, store, *, on_report=None, registry=None) -> None:
        self._store = store
        self._on_report = on_report        # async (owner_id, folder_id, text) -> None
        self._registry = registry          # to pause a running group poller while a ladder fires
        self._task: asyncio.Task[None] | None = None
        self._running: set[int] = set()     # ladder ids firing in this process right now
        self._labels: dict[int, str] = {}   # account id -> label, for the report

    async def start(self) -> None:
        await self._store.expire_stuck_ladders()
        self._task = asyncio.create_task(self._loop(), name="ladder-scheduler")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — a bad tick must not end the loop
                LOGGER.exception("ladder scheduler tick failed")
            await asyncio.sleep(POLL_SECONDS)

    async def _tick(self) -> None:
        now = time.time()
        for ladder in await self._store.due_ladders(now + LOOKAHEAD_SECONDS):
            if ladder["id"] in self._running:
                continue
            lead = CLAIM_LEAD_SECONDS + (ladder["parts"] - 1) * ladder["step_seconds"]
            if ladder["target_epoch"] - now > lead:
                continue  # its window has not arrived yet
            if not await self._store.claim_ladder(ladder["id"]):
                continue  # someone/something else took it
            self._running.add(ladder["id"])
            asyncio.create_task(self._fire(ladder), name=f"ladder-{ladder['id']}")

    async def _fire(self, ladder: dict) -> None:
        ladder_id = ladder["id"]
        # Hold the folder's group poller (if it is running) around the whole fire, so it never reads
        # the ladder's own fills as a manual trade to copy; resume hands it the new positions as the
        # baseline.
        service = self._registry.running_service(ladder["folder_id"]) if self._registry else None
        try:
            if service:
                await service.suspend_group_copy()
            async with aiohttp.ClientSession() as session:
                result = await self._run(ladder, session)
            if isinstance(result, str):
                # a reason it could not run at all: bad plan, missing keys, symbol gone
                status, text = "FAILED", result
            else:
                opened = [r for r in result.results if r.ok]
                # nothing opened is a failure, not a partial; some-but-not-all is partial
                status = "DONE" if len(opened) == len(result.results) else ("PARTIAL" if opened else "FAILED")
                text = result.summary(self._labels)
        except Exception as err:  # noqa: BLE001 — a firing that crashes must still be recorded
            LOGGER.exception("ladder %s failed", ladder_id)
            status, text = "FAILED", f"{type(err).__name__}: {err}"
        finally:
            if service:
                with contextlib.suppress(Exception):
                    await service.resume_group_copy()
        # One place records and reports, so every outcome — done, partial or failed — reaches the owner.
        await self._store.finish_ladder(ladder_id, status, text)
        await self._notify(ladder, f"⏱ <b>{ladder['symbol']}</b> запланований вхід — {status}\n\n{text}")
        self._running.discard(ladder_id)

    async def _run(self, ladder: dict, session: aiohttp.ClientSession):
        """Returns a LadderReport, or a plain string reason it could not run at all.

        The two sides are the folder's groups, resolved now: group 1 buys, group 2 sells. Resolved
        at fire time so a group edited after arming is honoured, and so nothing is opened on a side
        that has no accounts.
        """
        symbol = ladder["symbol"]
        owner_id = ladder["owner_id"]
        accounts = await self._store.folder_accounts(ladder["folder_id"])
        group1 = [a for a in accounts if a.group == 1]
        group2 = [a for a in accounts if a.group == 2]
        if not group1 or not group2:
            return "потрібен щонайменше один акаунт у кожній групі (Група 1 → лонг, Група 2 → шорт)"

        self._labels = {a.id: a.label for a in accounts}
        long, short = [], []
        for bucket, group in ((long, group1), (short, group2)):
            for account in group:
                creds = await self._store.get_credentials(account.id, owner_id)
                if not creds:
                    return f"немає ключів для {account.label}"
                bucket.append((account.id, make_rest_client(creds, session=session)))

        rules = (await hibt_rest.load_symbols(session)).get(symbol)
        if not rules:
            return f"{symbol} not listed on HIBT"
        price = await hibt_rest.get_ticker_price(session, symbol)
        all_clients = [c for _, c in long + short]
        latency = await self._measure(all_clients[0])
        snaps = await asyncio.gather(*(c.get_usdt_snapshot() for c in all_clients))

        config = LadderConfig(
            symbol=symbol, leverage=ladder["leverage"], margin_usd=ladder["margin_usd"],
            parts=ladder["parts"], step_seconds=ladder["step_seconds"], target_epoch=ladder["target_epoch"],
        )
        plan = plan_ladder(
            config, price=price, size_precision=hibt_rest.size_precision(rules),
            min_order=float(rules.get("marketMiniAmount") or 0), latency_seconds=latency,
            available=[s.openable for s in snaps], now=time.time(),
        )
        if not plan.ok:
            return "; ".join(plan.errors)

        executor = LadderExecutor(long, short, symbol=symbol, leverage=config.leverage)
        await executor.prepare()
        report = await executor.run(plan)
        await asyncio.sleep(1.0)
        with contextlib.suppress(Exception):
            for account_id, client in long + short:
                report.final[account_id] = f"{await self._held(client, symbol):g}"
        return report

    @staticmethod
    async def _measure(client) -> float:
        best = 1.0
        for _ in range(3):
            t = time.perf_counter()
            with contextlib.suppress(Exception):
                await client.get_usdt_snapshot()
            best = min(best, time.perf_counter() - t)
        return best

    @staticmethod
    async def _held(client, symbol) -> float:
        return sum(p.hold_vol for p in await client.get_open_positions(symbol) if p.hold_vol > 0)

    async def _notify(self, ladder: dict, text: str) -> None:
        if self._on_report:
            with contextlib.suppress(Exception):
                await self._on_report(ladder["owner_id"], ladder["folder_id"], text)
