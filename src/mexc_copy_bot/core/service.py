"""The copy-trading service: watches the master, drives the engine, reconciles drift.

Everything stateful lives here so the Telegram layer stays a pure control surface (spec §34):
handlers call start()/stop()/status() and never touch MEXC or the database directly.

Restart safety (spec §31): run state is persisted, and on boot the master's real positions become
the baseline before monitoring resumes — otherwise the first push after a restart would look like
the master had just opened everything from scratch, and every follower would copy it again.

One instance per FOLDER (see core/registry.py). A folder is a self-contained setup — its own
master, its own followers, its own mode and run state — and folders run independently of which one
their owner happens to be looking at. Switching the view must never stop a live one.

Account lookups are therefore scoped by `folder_id`, not by owner: an owner can keep several
setups, and mixing them is how a master ends up mirroring onto accounts from a different one.
`owner_id` is still carried, because that is who gets told what happened.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

import aiohttp

from ..db.store import (
    FOLLOWER,
    KIND_ENTRY,
    KIND_EXIT,
    MODE_REVERSE,
    Account,
    PositionRow,
    Store,
)
from ..mexc.rest import MexcError, MexcRestClient, PositionStops
from ..mexc.websocket import MasterWebSocket
from .copy_engine import CopyEngine, FollowerResult, _is_already_flat
from .events import Action, MasterEvent, MasterPositionTracker, PositionSnapshot, parse_position
from .orders import MasterOrderTracker, OrderAction, parse_order, reduces_position

LOGGER = logging.getLogger(__name__)

# How long START waits for the master socket before answering. The login takes about a second;
# waiting for it means the menu drawn immediately afterwards shows the true state, instead of a
# "reconnecting" that was only ever a race and then sits there, because a Telegram message
# never redraws itself.
CONNECT_TIMEOUT_SECONDS = 12.0

# How often the master's resting orders are re-read. Limit mirroring is only worth anything if
# the follower's order is in the book at about the same time as the master's, so this is fast.
# It is one request per second against an endpoint that has no per-order cost.
ORDER_POLL_SECONDS = 1.0

# How often the master's POSITIONS are re-read over REST.
#
# The websocket is the fast path, but it is not a dependency. Its host, contract.mexc.com, is the
# one MEXC blocks at the CDN for some networks — the REST client has pointed at api.mexc.com for
# exactly that reason since order submission there returned 403 — and from Render the socket now
# gets "403 Invalid response status" on every attempt, forever. With positions arriving only over
# that socket, the whole bot went quiet while its logs filled with reconnects.
#
# The REST rows carry the same fields the frames do: positionId, version, state, realised. So the
# same parser, the same tracker and the same dedupe key serve both, and whichever notices a change
# first, it is copied exactly once.
POSITION_POLL_SECONDS = 2.0

# How long a successful REST poll keeps counting as "the master is being watched". A few missed
# polls are a blip; beyond this the reader should be told something is wrong.
POSITION_POLL_STALE_SECONDS = 15.0

# How often every account in the folder is checked for whether it is holding anything.
#
# The master alone is not enough here. A follower can be liquidated, or closed by hand in the app,
# and nothing about that reaches this process — the position simply stops existing on an account
# nobody is reading. The REVERSE screen shows a light per account, and a light that only tells the
# truth after you press refresh is worse than no light.
#
# Ten accounts three times a minute is half a request a second against an allowance of seven, so
# it costs nothing worth counting.
STATUS_POLL_SECONDS = 20.0

# Named rather than inlined: patching this file has repeatedly turned an escaped newline into a
# real one, which is a syntax error that only surfaces at import.
NEWLINE = chr(10)

# What counts as the master getting OUT. A DECREASE is a partial exit and is the same thing for
# this purpose: in REVERSE the hedge is not unwound just because the master trimmed.
CLOSING_ACTIONS = frozenset({Action.CLOSE, Action.DECREASE})

ReportCallback = Callable[[MasterEvent, list[FollowerResult]], Awaitable[None]]
NoticeCallback = Callable[[str], Awaitable[None]]


@dataclass
class Drift:
    account: Account
    symbol: str
    position_type: int
    expected_vol: float
    actual_vol: float


class CopyService:
    def __init__(
        self,
        store: Store,
        folder_id: int,
        owner_id: int,
        *,
        retry_attempts: int = 3,
        reconcile_seconds: int = 60,
        ws_reconnect_max_seconds: float = 30.0,
    ) -> None:
        self._store = store
        self._folder_id = folder_id
        self._owner_id = owner_id
        self._retry_attempts = retry_attempts
        self._reconcile_seconds = reconcile_seconds
        self._ws_reconnect_max = ws_reconnect_max_seconds

        self._session: aiohttp.ClientSession | None = None
        self._ws: MasterWebSocket | None = None
        self._tracker = MasterPositionTracker()
        self._order_tracker = MasterOrderTracker()
        # (symbol, position type) -> volume already mirrored as resting limit orders and now
        # filled. The position channel reports that same fill a moment later; without
        # discounting it there, one trade would be copied twice — once as the limit that was
        # placed ahead of it, and again as a market order chasing the result.
        self._filled_by_limit: dict[tuple[str, int], float] = {}
        self._reconcile_task: asyncio.Task[None] | None = None
        self._order_poll_task: asyncio.Task[None] | None = None
        self._position_poll_task: asyncio.Task[None] | None = None
        self._status_poll_task: asyncio.Task[None] | None = None
        # (symbol, position type) -> position id, as of the last REST poll. A closed position
        # simply stops being listed, so the only way to notice a close over REST is to remember
        # what was there a moment ago.
        self._seen_positions: dict[tuple[str, int], int] = {}
        # When the REST poll last succeeded, so the menu can tell "being watched over REST" from
        # "not being watched at all".
        self._last_position_poll: float | None = None
        # account id -> is it holding anything at all. Any symbol counts: this answers "is there a
        # position on this account", which is what someone watching a hedge needs to know, and a
        # position opened by hand in the app is still a position.
        self._account_status: dict[int, bool] = {}
        # Master order ids currently resting in the book, as of the last poll.
        self._resting: set[str] = set()
        # None until the socket first reports in, so the initial connect can stay quiet.
        self._was_connected: bool | None = None
        # master order id -> the position side it closes, or None if it opens exposure.
        # Recorded when the copies are placed, because by the time it fills the master's
        # position is already gone and the question can no longer be answered.
        self._closing_intent: dict[str, int | None] = {}
        # master order id -> volume each follower was asked for. Needed to tell a straggler
        # from an account that was never in the trade at all.
        self._mirrored_vol: dict[str, dict[int, float]] = {}
        # Events are handled one at a time. Master actions arrive in order and often in bursts
        # (three orders filling one position); processing them concurrently would race the
        # position diff and could mirror the same delta twice.
        self._lock = asyncio.Lock()

        self.on_report: ReportCallback | None = None
        self.on_notice: NoticeCallback | None = None

    @property
    def owner_id(self) -> int:
        return self._owner_id

    @property
    def folder_id(self) -> int:
        return self._folder_id

    # ── lifecycle ───────────────────────────────────────────────────────────────────────────
    @property
    def running(self) -> bool:
        return self._ws is not None

    @property
    def master_connected(self) -> bool:
        """Whether the master is actually being watched, by either route.

        Not "is the socket up". The socket is the fast path, not the only one, and on a network
        where its host is blocked it never comes up at all — a menu keyed to it alone would have
        said "reconnecting" forever while every trade was being copied correctly over REST. What
        the reader needs to know is whether the master is being seen, so that is what this answers.
        """
        if self._ws and self._ws.connected:
            return True
        if self._last_position_poll is None:
            return False
        age = asyncio.get_running_loop().time() - self._last_position_poll
        return age <= POSITION_POLL_STALE_SECONDS

    async def start(self) -> str:
        if self._ws:
            return "Вже працює."

        master = await self._store.get_master(self._folder_id)
        if not master:
            return "Master акаунт не додано."

        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return "Не вдалося прочитати ключі майстра."

        self._session = aiohttp.ClientSession()
        api_key, secret = credentials
        self._ws = MasterWebSocket(
            api_key,
            secret,
            on_position=self._handle_position,
            on_order=self._handle_order,
            on_stop_order=self._handle_stop_order,
            on_resync=self._resync_master,
            on_status=self._master_status,
            reconnect_max_seconds=self._ws_reconnect_max,
        )
        self._ws.start()
        self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="reconcile")
        self._order_poll_task = asyncio.create_task(self._order_poll_loop(), name="orders")
        self._position_poll_task = asyncio.create_task(self._position_poll_loop(), name="positions")
        self._status_poll_task = asyncio.create_task(self._status_poll_loop(), name="status")
        await self._store.set_running(self._folder_id, True)

        if await self._ws.wait_connected(CONNECT_TIMEOUT_SECONDS):
            return "✅ Копіювання запущено — майстер підключений."
        # No socket. That used to mean nothing was watching the master; it no longer does, because
        # positions are polled over REST as well. Which of the two is true matters to whoever just
        # pressed START, so they are told apart rather than both reported as a warning.
        if self.master_connected:
            return (
                "✅ Копіювання запущено — майстер читається через REST."
                + NEWLINE
                + "(Вебсокет недоступний з цієї мережі, копіювання це не спиняє.)"
            )
        return "⚠️ Копіювання запущено, але майстра ще не видно — пробую далі."

    async def stop(self) -> str:
        """Stop copying NEW actions. Existing follower positions are left untouched (spec §7)."""
        for name in ("_reconcile_task", "_order_poll_task", "_position_poll_task", "_status_poll_task"):
            task = getattr(self, name)
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                setattr(self, name, None)
        if self._ws:
            await self._ws.stop()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None
        await self._store.set_running(self._folder_id, False)
        return "Копіювання зупинено. Відкриті позиції лишились як були."

    async def emergency_close_all(self) -> list[tuple[Account, str]]:
        """Close every position on this owner's followers, after explicit confirmation.

        Scoped to the caller's own accounts: an emergency stop is drastic enough that reaching
        another owner's positions with it would be unforgivable, so the account list it walks
        is filtered in SQL, not here.
        """
        outcomes: list[tuple[Account, str]] = []
        session = self._session or aiohttp.ClientSession()
        own_session = self._session is None
        try:
            for follower in await self._store.list_accounts(self._folder_id, FOLLOWER):
                credentials = await self._store.get_credentials(follower.id, self._owner_id)
                if not credentials:
                    outcomes.append((follower, "credentials missing"))
                    continue
                client = MexcRestClient(*credentials, session=session)
                try:
                    await client.close_all()
                    await self._clear_expected_positions(follower.id)
                    outcomes.append((follower, "closed"))
                except MexcError as err:
                    outcomes.append((follower, err.message or str(err)))
        finally:
            if own_session:
                await session.close()
        return outcomes

    # ── master monitoring ───────────────────────────────────────────────────────────────────
    async def _notify(self, text: str) -> None:
        """Send an unsolicited message, never letting a Telegram failure reach the trading path."""
        if self.on_notice:
            with contextlib.suppress(Exception):
                await self.on_notice(text)

    async def _report_limit_fill(self, order) -> None:
        """Say where the mirrored copies actually ended up.

        Read from the exchange rather than assumed. The whole point of a limit is that it fills
        only if price reaches it, so "the master filled" does not by itself mean the followers did
        — a copy can still be resting, and whoever is watching needs to be told that rather than
        handed a tidy success message.
        """
        followers = await self._eligible_followers()
        if not followers or not self._session:
            return
        # A moment for the venue to settle the copies before asking about them.
        await asyncio.sleep(1.5)

        async def holding(follower: Account):
            credentials = await self._store.get_credentials(follower.id, follower.owner_id)
            if not credentials:
                return None, "credentials missing"
            client = MexcRestClient(*credentials, session=self._session)
            try:
                positions = await client.get_open_positions(order.symbol)
            except MexcError as err:
                return None, err.message
            return sum(
                p.hold_vol for p in positions
                if p.position_type == order.position_type and p.hold_vol > 0
            ), None

        results = await asyncio.gather(*(holding(f) for f in followers), return_exceptions=True)

        # Record what each follower now holds as the expectation. The limit path places these
        # orders itself and never went through the position bookkeeping, so without this the
        # reconciler compares a real position against a blank row and reports drift on every
        # single mirrored trade — which is exactly what it did.
        await self._sync_expected(order, followers, results)
        await self._detach_stragglers(order, followers, results)

        closing_side = self._closing_intent.pop(order.order_id, None)
        if closing_side is not None:
            headline = f"CLOSE {'LONG' if closing_side == 1 else 'SHORT'}"
        else:
            headline = f"OPEN {'LONG' if order.position_type == 1 else 'SHORT'}"

        lines = [
            "✅ <b>ЛІМІТКУ ЗАПОВНЕНО</b>",
            "",
            f"<b>{order.symbol}</b> {headline} @ {order.price:g}",
            f"Майстер заповнив {order.deal_vol or order.vol:g}",
            "",
        ]
        for follower, res in zip(followers, results, strict=True):
            if not isinstance(res, tuple):
                lines.append(f"❓ {follower.label} — не вдалося перевірити")
                continue
            vol, error = res
            if error:
                lines.append(f"❌ {follower.label} — {error}")
            elif closing_side is not None:
                # Closing: an empty position is the goal, so it is success, not a missed fill.
                lines.append(
                    f"✅ {follower.label} — закрито"
                    if not vol
                    else f"⏳ {follower.label} — ще відкрито {vol:g}"
                )
            elif vol:
                lines.append(f"✅ {follower.label} — тримає {vol:g}")
            else:
                lines.append(f"⏳ {follower.label} — ще не заповнилось")
        await self._notify("\n".join(lines))

    async def _detach_stragglers(self, order, followers, results) -> None:
        """Find the accounts that did not keep up, and take them out of the master's control.

        The master's limit filled. Every follower had a copy at the same price, but one behind in
        the queue may still be sitting there while the price walks away. That account now holds
        something the master does not, or lacks something the master has, and must stop taking
        instructions until a person sorts it out — otherwise the next master action lands on an
        account in an entirely different state.
        """
        closing_side = self._closing_intent.get(order.order_id)
        asked = self._mirrored_vol.pop(order.order_id, {})
        order_ids = dict(await self._store.get_mirrored_orders(self._owner_id, order.order_id))

        outstanding: list[tuple[int, float, str | None]] = []
        labels: list[str] = []
        for follower, res in zip(followers, results, strict=True):
            if not isinstance(res, tuple):
                continue
            vol, error = res
            if error or follower.id not in asked:
                continue
            if closing_side is not None:
                # Meant to close. Whatever is still held is what did not close, and a partial
                # close is not a close.
                remaining = vol or 0.0
            else:
                # Meant to open. Whatever is missing from the requested size never filled.
                remaining = max(0.0, asked[follower.id] - (vol or 0.0))
            if remaining > 1e-9:
                outstanding.append((follower.id, remaining, order_ids.get(follower.id)))
                labels.append(follower.label)

        if not outstanding:
            return

        kind = KIND_EXIT if closing_side is not None else KIND_ENTRY
        group_id = await self._store.create_stuck_group(
            owner_id=self._owner_id,
            symbol=order.symbol,
            position_type=closing_side if closing_side is not None else order.position_type,
            kind=kind,
            limit_price=order.price,
            leverage=order.leverage,
            open_type=order.open_type,
            members=outstanding,
        )
        LOGGER.warning(
            "group %s: %d account(s) stranded on %s (%s)",
            group_id, len(outstanding), order.symbol, kind,
        )
        await self._notify(
            "\n".join(
                [
                    f"⚠️ <b>ЗАВИСЛО {len(outstanding)} АКАУНТ(ІВ)</b>",
                    "",
                    f"<b>{order.symbol}</b> — лімітка не заповнилась.",
                    ", ".join(labels),
                    "",
                    "Ці акаунти більше не слухають майстра, поки не розрулиш вручну.",
                    "Меню → ⚠️ Завислі",
                ]
            )
        )

    async def _sync_expected(self, order, followers, results) -> None:
        for follower, res in zip(followers, results, strict=True):
            if not isinstance(res, tuple):
                continue
            vol, error = res
            if error:
                continue
            if vol:
                await self._store.upsert_position(
                    PositionRow(
                        account_id=follower.id,
                        symbol=order.symbol,
                        position_type=order.position_type,
                        hold_vol=vol,
                        leverage=order.leverage,
                        open_type=order.open_type,
                    )
                )
            else:
                for side in (1, 2):
                    await self._store.delete_position(follower.id, order.symbol, side)

    async def _master_status(self, connected: bool, detail: str = "") -> None:
        """Say something only when the connection actually changes state.

        The socket reconnects on its own for all sorts of reasons, and a "connected" line on each
        one is noise — it trains you to ignore the row it appears in, which is the row a real
        disconnection will appear in too. The first connect is silent as well: pressing START
        already answered that question.
        """
        was = self._was_connected
        self._was_connected = connected

        if connected:
            if was is False:
                await self._notify("✅ <b>Майстер знову підключений.</b>")
            return

        if was:
            await self._notify(
                NEWLINE.join(
                    [
                        "⚠️ <b>Майстер відключився</b>",
                        "",
                        detail or "звʼязок втрачено",
                        "",
                        "Копіювання триває — позиції читаються через REST. "
                        "Перепідключаюсь автоматично.",
                    ]
                )
            )

    async def _resync_master(self) -> None:
        """Make the master's real positions the baseline, emitting nothing.

        Called at connect and after every reconnect. Without this, the first push following a
        gap would be diffed against a stale size and copied as a phantom change.
        """
        master = await self._store.get_master(self._folder_id)
        if not master or not self._session:
            return
        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return
        client = MexcRestClient(*credentials, session=self._session)
        try:
            positions = await client.get_open_positions()
        except MexcError as err:
            LOGGER.warning("master resync failed: %s", err)
            return

        # A reconnect invalidates both: order ids from the previous session cannot be acted on,
        # and any fill we were about to discount has already been folded into the fresh baseline.
        self._order_tracker.reset()
        self._filled_by_limit.clear()
        self._resting.clear()
        self._tracker.resync(
            [
                PositionSnapshot(
                    symbol=p.symbol,
                    position_type=p.position_type,
                    hold_vol=p.hold_vol,
                    leverage=p.leverage,
                    open_type=p.open_type,
                    state=p.state,
                )
                for p in positions
                if p.hold_vol > 0
            ]
        )
        LOGGER.info("master baseline set from %d open position(s)", len(self._tracker.snapshot()))

    async def _master_stops(self, event: MasterEvent) -> tuple[float | None, float | None]:
        """The master's stop-loss / take-profit for the side being opened, if it has any.

        Looked up per event rather than cached: the master can set a stop between two top-ups, and
        a follower opening after that should get the level the master is actually holding.
        """
        if event.action is Action.CLOSE:
            return (None, None)
        master = await self._store.get_master(self._folder_id)
        if not master or not self._session:
            return (None, None)
        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return (None, None)
        try:
            stops = await MexcRestClient(*credentials, session=self._session).get_stop_orders(event.symbol)
        except MexcError as err:
            # Not fatal: a missing stop is worth less than a missed trade, so the order still goes.
            LOGGER.info("could not read master stops for %s: %s", event.symbol, err)
            return (None, None)
        for stop in stops:
            if stop.symbol == event.symbol and stop.position_type == event.position_type:
                return (stop.stop_loss_price, stop.take_profit_price)
        return (None, None)

    async def _handle_stop_order(self, channel: str, data: dict) -> None:
        """Mirror the master's stop-loss / take-profit onto the followers.

        Prices are copied unchanged, which is right because every account trades the same contract
        at the same price: a stop at 78,000 means the same thing on all of them.

        Read back from REST rather than trusted from the frame. The push says something changed;
        the authoritative current pair is what the account actually holds, and this way a partial
        or unfamiliar frame shape cannot turn into a wrong price on nine live accounts.
        """
        LOGGER.info("master stop-order frame on %s: %s", channel, data)

        mode, _ = await self._store.get_mode(self._folder_id)
        if mode == MODE_REVERSE:
            # Deliberate: a hedge holds the opposite side, so the master's stop price sits on the
            # wrong side of its entry — it would close the hedge for a profit and let the loss run.
            # Reversing the levels is a different feature; until it exists, do nothing.
            LOGGER.info("reverse mode: master stops are not mirrored")
            return

        master = await self._store.get_master(self._folder_id)
        if not master or not self._session:
            return
        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return

        symbol = str(data.get("symbol") or "") or None
        try:
            master_stops = await MexcRestClient(*credentials, session=self._session).get_stop_orders(symbol)
        except MexcError as err:
            LOGGER.warning("could not read master stops: %s", err)
            return

        wanted = {(s.symbol, s.position_type): s for s in master_stops}
        if not wanted:
            LOGGER.info("master has no active stops for %s", symbol or "any symbol")
            return

        followers = [a for a in await self._store.list_accounts(self._folder_id, FOLLOWER) if a.active]
        results = await asyncio.gather(
            *(self._apply_stops_to(f, wanted) for f in followers), return_exceptions=True
        )

        applied = sum(1 for r in results if r is True)
        if applied and self.on_notice:
            lines = [f"🛡 Stops mirrored from master ({applied}/{len(followers)} accounts)"]
            for (sym, side), stop in wanted.items():
                bits = []
                if stop.stop_loss_price:
                    bits.append(f"SL {stop.stop_loss_price:g}")
                if stop.take_profit_price:
                    bits.append(f"TP {stop.take_profit_price:g}")
                lines.append(f"{sym} {'LONG' if side == 1 else 'SHORT'}: {', '.join(bits)}")
            with contextlib.suppress(Exception):
                await self.on_notice("\n".join(lines))

    async def _apply_stops_to(self, follower: Account, wanted: dict[tuple[str, int], PositionStops]) -> bool:
        """Put the master's levels on one follower. True if anything was actually changed."""
        credentials = await self._store.get_credentials(follower.id, follower.owner_id)
        if not credentials or not self._session:
            return False
        client = MexcRestClient(*credentials, session=self._session)
        try:
            own = {(s.symbol, s.position_type): s for s in await client.get_stop_orders()}
        except MexcError as err:
            LOGGER.info("follower %s: could not read stops: %s", follower.id, err)
            return False

        changed = False
        for key, target in wanted.items():
            mine = own.get(key)
            if not mine:
                # No stop entry of its own to modify. MEXC hangs stops off the opening order, so
                # there is nothing to point change_price at; the levels go on at open time instead.
                LOGGER.info("follower %s has no stop entry for %s to modify", follower.id, key)
                continue
            if (mine.stop_loss_price, mine.take_profit_price) == (
                target.stop_loss_price,
                target.take_profit_price,
            ):
                continue
            try:
                await client.set_position_stops(
                    order_id=mine.order_id,
                    stop_loss_price=target.stop_loss_price,
                    take_profit_price=target.take_profit_price,
                )
                changed = True
            except MexcError as err:
                LOGGER.warning("follower %s: setting stops failed: %s", follower.id, err.message)
                await self._store.set_account_error(follower.id, f"stops: {err.message}")
        return changed

    async def _handle_order(self, data: dict) -> None:
        """React to the master's own orders, which is the only way a resting limit is visible."""
        order = parse_order(data)
        if not order:
            return
        LOGGER.info(
            "master order %s %s %s type=%s state=%s price=%s vol=%s dealt=%s",
            order.order_id, order.symbol, order.side, order.order_type,
            order.state, order.price, order.vol, order.deal_vol,
        )

        async with self._lock:
            event = self._order_tracker.apply(order)
            if event is None:
                return

            if event.action is OrderAction.PLACE:
                await self._place_mirrored(order)
            elif event.action is OrderAction.CANCEL:
                await self._cancel_mirrored(order.order_id)
            elif event.action is OrderAction.FILL:
                # The followers' own copies are filling at the same price. Remember how much, so
                # the position update that follows is not mirrored a second time.
                key = (order.symbol, order.position_type)
                filled = order.deal_vol or order.vol
                self._filled_by_limit[key] = self._filled_by_limit.get(key, 0.0) + filled
                await self._store.clear_mirrored_order(self._owner_id, order.order_id)
                LOGGER.info("limit fill of %s on %s discounted from the position path", filled, key)
                asyncio.create_task(self._report_limit_fill(order))

    async def _master_client(self) -> MexcRestClient | None:
        master = await self._store.get_master(self._folder_id)
        if not master or not self._session:
            return None
        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return None
        return MexcRestClient(*credentials, session=self._session)

    # ── what each account is holding ────────────────────────────────────────────────────────
    @property
    def account_status(self) -> dict[int, bool]:
        """Last known "is this account holding something", by account id.

        A copy, so a screen rendering it cannot be caught mid-update by the poller.
        """
        return dict(self._account_status)

    async def refresh_account_status(self) -> dict[int, bool]:
        """Ask every account in the folder whether it is holding anything.

        Concurrent, because ten sequential round trips between pressing a button and seeing the
        answer is the difference between a screen that feels live and one that does not. Failures
        are per account: one revoked key must not blank out everybody else's light — the previous
        answer is kept instead, which is closer to the truth than inventing "flat".
        """
        if not self._session:
            return self.account_status

        master = await self._store.get_master(self._folder_id)
        accounts = ([master] if master else []) + await self._store.list_accounts(
            self._folder_id, FOLLOWER
        )
        if not accounts:
            return {}

        async def holding(account: Account) -> tuple[int, bool | None]:
            credentials = await self._store.get_credentials(account.id, self._owner_id)
            if not credentials:
                return account.id, None
            client = MexcRestClient(*credentials, session=self._session)
            try:
                positions = await client.get_open_positions()
            except (MexcError, aiohttp.ClientError, asyncio.TimeoutError) as err:
                LOGGER.info("could not read %s's positions: %s", account.label, err)
                return account.id, None
            return account.id, any(p.hold_vol > 0 for p in positions)

        for account_id, held in await asyncio.gather(*(holding(a) for a in accounts)):
            if held is not None:
                self._account_status[account_id] = held

        # Accounts that have since been deleted must not keep a light on the screen.
        live = {a.id for a in accounts}
        self._account_status = {k: v for k, v in self._account_status.items() if k in live}
        return self.account_status

    async def _status_poll_loop(self) -> None:
        while True:
            await asyncio.sleep(STATUS_POLL_SECONDS)
            try:
                await self.refresh_account_status()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — a status light must never kill the service
                LOGGER.exception("account status poll failed")

    async def close_accounts(
        self, account_ids: list[int]
    ) -> tuple[list[str], list[tuple[str, str]], list[str]]:
        """Close everything on the named accounts, at market.

        Returns (closed, failed, skipped-because-already-flat).

        Each account is read before it is touched, and the flat ones are left completely alone.
        This is the point of the whole method rather than a nicety: on MEXC an order on the
        opposite side of a flat account OPENS a position instead of closing one, so a blanket
        "close them all" sent to an account someone had already closed by hand would put it
        straight back in, facing the other way. close_all on a flat account is the same hazard
        wearing a safer name — it returns an error the caller would report as a failure — so the
        read comes first either way.

        Deliberately not scoped to a symbol: this answers "get me out of this leg", and a leg is
        held in whatever the master has been trading.
        """
        if not self._session or not account_ids:
            return [], [], []

        wanted = set(account_ids)
        master = await self._store.get_master(self._folder_id)
        accounts = [
            a
            for a in (([master] if master else []) + await self._store.list_accounts(self._folder_id, FOLLOWER))
            if a.id in wanted
        ]

        closed: list[str] = []
        failed: list[tuple[str, str]] = []
        skipped: list[str] = []

        async def one(account: Account) -> None:
            credentials = await self._store.get_credentials(account.id, self._owner_id)
            if not credentials:
                failed.append((account.label, "немає ключів"))
                return
            client = MexcRestClient(*credentials, session=self._session)
            try:
                symbols = {p.symbol for p in await client.get_open_positions() if p.hold_vol > 0}
            except MexcError as err:
                # Unknown is not "flat". Reported, so nobody reads silence as "already closed".
                failed.append((account.label, err.message or "не вдалося прочитати позиції"))
                return
            if not symbols:
                skipped.append(account.label)
                self._account_status[account.id] = False
                return
            for symbol in symbols:
                try:
                    await client.close_all(symbol)
                except MexcError as err:
                    if not _is_already_flat(err):
                        failed.append((account.label, err.message or "не вдалося закрити"))
                        return
            closed.append(account.label)
            self._account_status[account.id] = False
            await self._clear_expected_positions(account.id)

        await asyncio.gather(*(one(a) for a in accounts), return_exceptions=True)
        return closed, failed, skipped

    async def _skip_reverse_close(self, event: MasterEvent) -> None:
        """In REVERSE the master's exit is not the hedge's exit.

        A hedge is held against the master's position, not alongside it, so following the master
        out closes the very thing that was protecting it. The accounts stay in, and are closed by
        hand when the person who put them there decides to.

        Said out loud rather than done quietly: after this the accounts hold something the master
        does not, which is exactly the state worth knowing you are in.
        """
        side = "LONG" if event.position_type == 1 else "SHORT"
        pnl = event.realized_pnl
        lines = [
            "🔁 <b>РЕВЕРС — ЗАКРИТТЯ НЕ КОПІЮЄТЬСЯ</b>",
            "",
            f"Майстер вийшов з <b>{event.symbol}</b> {side}"
            + (f" ({'+' if pnl >= 0 else '−'}${abs(pnl):,.2f})" if pnl is not None else "")
            + ".",
            "",
            "Реверсні позиції лишаються відкритими — закривай вручну.",
        ]
        LOGGER.info("reverse mode: not mirroring the master's exit from %s", event.symbol)
        await self._notify(NEWLINE.join(lines))

    async def _position_poll_loop(self) -> None:
        """Read the master's positions over REST, continuously.

        Runs alongside the websocket rather than instead of it. The socket is faster when it is
        available; this is what keeps the bot working when it is not, which on Render is always —
        contract.mexc.com answers the socket with 403 there, the same CDN block that already forced
        REST onto api.mexc.com.

        Nothing is emitted for the first pass. Positions already open when copying starts belong to
        before the bot's time, and announcing them would copy a trade whose entry price is long
        gone.
        """
        client = await self._master_client()
        if client:
            with contextlib.suppress(Exception):
                for position in await client.get_open_positions():
                    if position.hold_vol > 0:
                        self._seen_positions[(position.symbol, position.position_type)] = position.position_id
                self._last_position_poll = asyncio.get_running_loop().time()
                LOGGER.info("seeded with %d open master position(s)", len(self._seen_positions))

        while True:
            await asyncio.sleep(POSITION_POLL_SECONDS)
            try:
                await self._poll_positions_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — polling must never kill the service
                LOGGER.exception("position poll failed")

    async def _poll_positions_once(self) -> None:
        client = await self._master_client()
        if not client:
            return
        try:
            rows = await client.get_open_positions_raw()
        except MexcError as err:
            LOGGER.info("could not read master positions: %s", err)
            return

        self._last_position_poll = asyncio.get_running_loop().time()

        live: dict[tuple[str, int], int] = {}
        for row in rows:
            snapshot = parse_position(row)
            if not snapshot or snapshot.hold_vol <= 0:
                continue
            live[snapshot.key] = snapshot.position_id or 0
            await self._handle_position(row)

        # Anything held a moment ago and not listed now has been closed. Over REST a close is an
        # absence, so it has to be looked up: the finished row carries holdVol 0, state 3 and the
        # realised PnL, which is the same shape the socket's close frame has and therefore produces
        # the same event.
        for key, position_id in list(self._seen_positions.items()):
            if key in live:
                continue
            symbol, _ = key
            closed_row = await self._closed_row(client, symbol, position_id)
            if closed_row is None:
                # Not settled yet. Left in place so the next pass tries again rather than
                # forgetting a close ever happened.
                continue
            await self._handle_position(closed_row)
            self._seen_positions.pop(key, None)

        self._seen_positions.update(live)

    async def _closed_row(self, client: MexcRestClient, symbol: str, position_id: int) -> dict | None:
        """The finished position, exactly as the venue reports it."""
        try:
            rows = await client.get_closed_positions_raw(symbol)
        except MexcError as err:
            LOGGER.info("could not read the settled position for %s: %s", symbol, err)
            return None
        for row in rows:
            if int(row.get("positionId") or 0) == position_id:
                return row
        return None

    async def _order_poll_loop(self) -> None:
        """Read the master's resting orders over REST, continuously.

        The websocket order channel is also handled, but this is the path that is known to work:
        the endpoint and its exact fields were verified against the live account, while MEXC's
        documentation has already been wrong twice about channel names. Both feed the same tracker,
        so whichever notices an order first, it is still mirrored exactly once.
        """
        # Seed silently. Orders already resting when copying starts have either been mirrored
        # already or belong to before the bot's time; either way, placing copies now would be
        # wrong.
        client = await self._master_client()
        if client:
            with contextlib.suppress(Exception):
                self._resting = {str(o.get("orderId")) for o in await client.get_open_orders()}
                LOGGER.info("seeded with %d resting master order(s)", len(self._resting))

        while True:
            await asyncio.sleep(ORDER_POLL_SECONDS)
            try:
                await self._poll_orders_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — polling must never kill the service
                LOGGER.exception("order poll failed")

    async def _poll_orders_once(self) -> None:
        client = await self._master_client()
        if not client:
            return
        try:
            live = await client.get_open_orders()
        except MexcError as err:
            LOGGER.info("could not read master orders: %s", err)
            return

        by_id = {str(o.get("orderId")): o for o in live}
        appeared = set(by_id) - self._resting
        vanished = self._resting - set(by_id)
        self._resting = set(by_id)

        for order_id in appeared:
            parsed = parse_order(by_id[order_id])
            if not parsed:
                continue
            async with self._lock:
                event = self._order_tracker.apply(parsed)
                if event and event.action is OrderAction.PLACE:
                    LOGGER.info("master placed %s %s @ %s", parsed.symbol, parsed.vol, parsed.price)
                    await self._place_mirrored(parsed)

        if vanished:
            await self._resolve_vanished(client, vanished)

    async def _resolve_vanished(self, client: MexcRestClient, order_ids: set[str]) -> None:
        """An order left the book. Whether it filled or was cancelled decides everything.

        A fill means the followers' copies filled too, and the position change that follows must
        not be copied again. A cancel means their copies are still live and have to be pulled.
        From the open-orders list alone the two are indistinguishable, so the finished order is
        read back.
        """
        try:
            recent = {str(o.get("orderId")): o for o in await client.get_recent_orders()}
        except MexcError as err:
            LOGGER.warning("could not resolve %d vanished order(s): %s", len(order_ids), err)
            return

        for order_id in order_ids:
            row = recent.get(order_id)
            if not row:
                LOGGER.info("order %s vanished but is not in recent history yet", order_id)
                continue
            parsed = parse_order(row)
            if not parsed:
                continue
            async with self._lock:
                event = self._order_tracker.apply(parsed)
                if not event:
                    continue
                if event.action is OrderAction.CANCEL:
                    LOGGER.info("master cancelled %s; pulling the copies", order_id)
                    await self._cancel_mirrored(order_id)
                elif event.action is OrderAction.FILL:
                    key = (parsed.symbol, parsed.position_type)
                    filled = parsed.deal_vol or parsed.vol
                    self._filled_by_limit[key] = self._filled_by_limit.get(key, 0.0) + filled
                    await self._store.clear_mirrored_order(self._owner_id, order_id)
                    LOGGER.info("master limit filled %s on %s; discounted", filled, key)
                    # Off the lock: it reads every follower and must not hold up the next event.
                    asyncio.create_task(self._report_limit_fill(parsed))

    async def _place_mirrored(self, order) -> None:
        assert self._session is not None

        # Is the master getting OUT of something? On MEXC a close is expressed as an open on the
        # opposite side, so this cannot be read off the order alone — it needs the master's
        # position. Everything downstream depends on the answer, including who is eligible.
        try:
            closing_side = await self._closing_side(order)
        except MexcError as err:
            # Better a limit that was not mirrored, and said so, than nine accounts opening the
            # opposite side because the one call that tells open from close did not come back.
            LOGGER.warning("not mirroring %s: %s", order.symbol, err)
            await self._notify(
                NEWLINE.join(
                    [
                        "⚠️ <b>ЛІМІТКУ НЕ СКОПІЙОВАНО</b>",
                        "",
                        f"Майстер виставив ордер на <b>{order.symbol}</b>, але не вдалося",
                        "прочитати його позицію, тому невідомо, це вхід чи вихід.",
                        f"Причина: {err.message}",
                        "",
                        "Нічого не відправлено — інакше вихід міг би скопіюватись",
                        "як вхід у протилежний бік.",
                    ]
                )
            )
            return

        mode, _ = await self._store.get_mode(self._folder_id)
        reverse = mode == MODE_REVERSE

        # Same rule as a market exit: in REVERSE the master leaving does not take the hedge with
        # it. Checked before anything is placed, not after.
        if reverse and closing_side is not None:
            LOGGER.info("reverse mode: not mirroring the master's exit limit on %s", order.symbol)
            await self._notify(
                NEWLINE.join(
                    [
                        "🔁 <b>РЕВЕРС — ЛІМІТКА НА ВИХІД НЕ КОПІЮЄТЬСЯ</b>",
                        "",
                        f"Майстер виставив вихід з <b>{order.symbol}</b> @ {order.price:g}.",
                        "",
                        "Реверсні позиції лишаються відкритими.",
                    ]
                )
            )
            return

        followers = await self._eligible_followers(
            opening=closing_side is None, symbol=order.symbol
        )
        if not followers:
            return
        self._closing_intent[order.order_id] = closing_side
        vol_by_account: dict[int, float] | None = None
        skipped: list[str] = []
        if closing_side is not None:
            vol_by_account = {}
            eligible = []
            for follower in followers:
                held = await self._held(follower, order.symbol, closing_side)
                if held is None:
                    # Unknown, and a limit needs an exact size — capping at "what they hold" is
                    # the whole reason for reading it, and guessing high would open the opposite
                    # side. Named in the report rather than dropped silently, so whoever is
                    # watching can close it by hand.
                    skipped.append(f"{follower.label} (не вдалося прочитати позицію)")
                    continue
                if held <= 0:
                    skipped.append(f"{follower.label} (нема що закривати)")
                    continue
                # Never more than they actually hold, or the surplus opens the opposite side.
                vol_by_account[follower.id] = min(order.vol * follower.size_multiplier, held)
                eligible.append(follower)
            followers = eligible
            if not followers:
                await self._notify(
                    "\u26a0\ufe0f <b>CLOSE NOT MIRRORED</b>\n\n"
                    f"The master is closing <b>{order.symbol}</b>, but no follower holds that "
                    "position. Nothing was sent \u2014 mirroring it would have opened the opposite "
                    "side instead of closing anything."
                )
                return

        engine = CopyEngine(self._store, self._session, retry_attempts=self._retry_attempts)
        results = await engine.mirror_resting_order(
            order, followers, reverse=reverse, vol_by_account=vol_by_account
        )

        placed, failed = 0, []
        asked: dict[int, float] = {}
        for follower, follower_order_id, error in results:
            if follower_order_id:
                placed += 1
                asked[follower.id] = (
                    vol_by_account[follower.id]
                    if vol_by_account is not None
                    else order.vol * follower.size_multiplier
                )
                await self._store.record_mirrored_order(
                    owner_id=self._owner_id, master_order_id=order.order_id,
                    account_id=follower.id, follower_order_id=follower_order_id,
                    symbol=order.symbol,
                )
            else:
                failed.append(f"{follower.label}: {error}")

        self._mirrored_vol[order.order_id] = asked

        # Described by what it does, not by MEXC's side number: in one-way mode an exit is sent
        # as "open short", and reporting that literally is how a close reads as a new position.
        if closing_side is not None:
            headline = f"CLOSE {'LONG' if closing_side == 1 else 'SHORT'}"
        else:
            headline = f"OPEN {'LONG' if order.position_type == 1 else 'SHORT'}"
        lines = [
            "📌 <b>ЛІМІТКУ ВИСТАВЛЕНО</b> — копія з майстра",
            "",
            f"<b>{order.symbol}</b> {headline}",
            f"Price: {order.price:g}   Size: {order.vol:g}",
            "",
            f"Виставлено на {placed}/{len(followers)} акаунт(ах)",
        ]
        lines += [f"❌ {f}" for f in failed]
        if skipped:
            lines.append("")
            lines.append(f"⏭ Пропущено: {', '.join(skipped)}")
        await self._notify("\n".join(lines))

    async def _closing_side(self, order) -> int | None:
        """Which position side the master is closing with this order, if any.

        Raises if the master's position cannot be read. On MEXC a close is expressed as an open on
        the opposite side, so this lookup is the only thing separating the two — and answering it
        with a default meant a failed read could turn the master getting OUT into every follower
        opening a fresh position the other way. There is no defensible guess, so the caller is
        made to deal with not knowing.
        """
        client = await self._master_client()
        if not client:
            raise MexcError(None, "master credentials unavailable", endpoint="closing_side")
        positions = await client.get_open_positions(order.symbol)
        held = {p.position_type: p.hold_vol for p in positions if p.hold_vol > 0}
        return reduces_position(order, held)

    async def _held(self, follower: Account, symbol: str, position_type: int) -> float | None:
        """How much of one side this account holds. None means the venue would not say.

        None rather than 0.0, because the two are opposite instructions. This used to answer a
        rate-limited read with "holds nothing", and "holds nothing" is exactly what makes an
        account skipped when the master closes — a transient refusal became a position left open
        with no copy of it anywhere.
        """
        credentials = await self._store.get_credentials(follower.id, follower.owner_id)
        if not credentials or not self._session:
            return None
        client = MexcRestClient(*credentials, session=self._session)
        try:
            positions = await client.get_open_positions(symbol)
        except MexcError as err:
            LOGGER.warning("could not read %s on %s: %s", follower.label, symbol, err)
            return None
        return sum(p.hold_vol for p in positions if p.position_type == position_type and p.hold_vol > 0)

    async def _cancel_mirrored(self, master_order_id: str) -> None:
        pairs = await self._store.get_mirrored_orders(self._owner_id, master_order_id)
        if not pairs:
            return
        accounts = {a.id: a for a in await self._store.list_accounts(self._folder_id, FOLLOWER)}
        todo = [(accounts[aid], oid) for aid, oid in pairs if aid in accounts]

        assert self._session is not None
        engine = CopyEngine(self._store, self._session, retry_attempts=self._retry_attempts)
        cancelled = await engine.cancel_mirrored_orders(todo)
        await self._store.clear_mirrored_order(self._owner_id, master_order_id)
        LOGGER.info("cancelled %s/%s mirrored copies of %s", cancelled, len(todo), master_order_id)
        await self._notify(
            "🚫 <b>ЛІМІТКУ СКАСОВАНО</b>\n\n"
            "Майстер зняв свій ордер.\n"
            f"Скасовано на {cancelled}/{len(todo)} акаунт(ах)."
        )

    async def _eligible_followers(self, *, opening: bool | None = None, symbol: str | None = None) -> list[Account]:
        """Who acts. See `_eligibility`, which also says who did not and why."""
        matched, _ = await self._eligibility(opening=opening, symbol=symbol)
        return matched

    async def _eligibility(
        self, *, opening: bool | None = None, symbol: str | None = None
    ) -> tuple[list[Account], list[str]]:
        """Who an action from the master applies to. Every such action goes through here.

        Three filters, in order of how badly getting them wrong would hurt:

        1. Detached accounts are excluded outright. An account that failed to follow the master
           holds something the master does not, or lacks something the master has; until that is
           sorted out by hand it must hear nothing at all, on any symbol.

        2. The mode decides whether this is a mirror or a one-account hedge.

        3. An account only acts with the master when its position on this symbol already matches
           the master's before the action. This is what stops a freshly un-stuck account from
           diving into a position the master opened while it was stranded — the entry price is
           gone, and it would be joining a trade half way through. It waits for the next one
           instead, which by definition starts with both of them flat.
        """
        reasons: list[str] = []
        everyone = await self._store.list_accounts(self._folder_id, FOLLOWER)
        active = [a for a in everyone if a.active]
        reasons += [f"{a.label} — на паузі" for a in everyone if not a.active]

        detached = await self._store.detached_account_ids(self._owner_id)
        if detached:
            skipped = [a.label for a in active if a.id in detached]
            active = [a for a in active if a.id not in detached]
            reasons += [f"{label} — відчеплений від майстра" for label in skipped]
            LOGGER.info("skipping detached accounts: %s", ", ".join(skipped))

        # REVERSE no longer means "one nominated account and the rest idle": every active
        # follower trades, each on the side it was set to. The mode only decides whether those
        # per-account settings are honoured at all, which is settled where the trade is placed.

        if opening is None or symbol is None or not active:
            return active, reasons

        # Opening: the account must be flat here, or it is already in something of its own.
        # Closing: it must be holding, or there is nothing of its to close.
        matched = []
        for follower in active:
            held = await self._held_any(follower, symbol)
            if held is None:
                # The venue would not say what this account holds. There is no safe default, so
                # the two directions are decided by what going wrong would cost:
                #   closing — send it anyway. Closing an account that turns out to be flat is a
                #             no-op the engine already handles; skipping one that was holding
                #             leaves a live position with nothing watching it.
                #   opening — leave it out. Opening an account that turns out to hold something
                #             doubles the position, and there is no undo for that.
                LOGGER.warning(
                    "could not read what %s holds on %s; %s",
                    follower.label, symbol,
                    "closing anyway" if not opening else "not opening it",
                )
                if not opening:
                    matched.append(follower)
                else:
                    reasons.append(f"{follower.label} — не вдалося прочитати позицію")
                continue
            if (held <= 0) == opening:
                matched.append(follower)
            else:
                reasons.append(
                    f"{follower.label} — "
                    + ("вже щось тримає по цьому токену" if opening else "нема відкритої позиції")
                )
                LOGGER.info(
                    "%s is out of step on %s (holds %g, master is %s); waiting for the next trade",
                    follower.label, symbol, held, "opening" if opening else "closing",
                )
        return matched, reasons

    async def _held_any(self, follower: Account, symbol: str) -> float | None:
        """Total this account holds on a symbol, either side. None means unknown — see `_held`."""
        credentials = await self._store.get_credentials(follower.id, follower.owner_id)
        if not credentials or not self._session:
            return None
        client = MexcRestClient(*credentials, session=self._session)
        try:
            positions = await client.get_open_positions(symbol)
        except MexcError as err:
            LOGGER.warning("could not read %s on %s: %s", follower.label, symbol, err)
            return None
        return sum(p.hold_vol for p in positions if p.hold_vol > 0)

    async def _handle_position(self, data: dict) -> None:
        snapshot = parse_position(data)
        if not snapshot:
            return

        async with self._lock:
            event = self._tracker.apply(snapshot)
            if event is None:
                return

            # Discount anything a mirrored limit already covered.
            key = (event.symbol, event.position_type)
            pending = self._filled_by_limit.get(key, 0.0)
            if pending > 0:
                covered = min(pending, event.delta_vol)
                remaining = pending - covered
                if remaining > 1e-9:
                    self._filled_by_limit[key] = remaining
                else:
                    self._filled_by_limit.pop(key, None)
                if event.delta_vol - covered <= 1e-9:
                    LOGGER.info(
                        "position change on %s already covered by mirrored limits; not re-copied", key
                    )
                    return
                # Only part of it came from the mirrored limit; copy the rest.
                event = replace(event, delta_vol=event.delta_vol - covered)

            await self._dispatch(event, data)

    async def _dispatch(self, event: MasterEvent, raw: dict) -> None:
        # The tracker builds the event from a diff and has no frame to attach, so the frame is
        # attached here — before anything reads it. Without this the master's realised PnL is
        # always missing, because the only place it exists is the frame.
        event = replace(event, raw=raw)
        event_id = await self._store.record_event(
            owner_id=self._owner_id,
            dedupe_key=event.dedupe_key,
            symbol=event.symbol,
            position_type=event.position_type,
            action=event.action.value,
            master_vol=event.master_vol,
            delta_vol=event.delta_vol,
            leverage=event.leverage,
            open_type=event.open_type,
            raw=raw,
        )
        if event_id is None:
            # A dropped event reaches nobody and reports nothing, so when the key was wrong this
            # was the whole of the evidence. Logged loudly enough to find in a deploy's output.
            LOGGER.warning(
                "master event dropped as a duplicate: %s (%s %s)",
                event.dedupe_key, event.action.value, event.symbol,
            )
            return

        mode, _ = await self._store.get_mode(self._folder_id)
        reverse = mode == MODE_REVERSE

        if reverse and event.action in CLOSING_ACTIONS:
            await self._skip_reverse_close(event)
            return

        followers, skipped = await self._eligibility(
            opening=event.action is not Action.CLOSE, symbol=event.symbol
        )
        if not followers:
            # Silence here is what made a failed trade indistinguishable from a bot that had
            # stopped working. The master did something, and nothing happened on any account —
            # that is exactly the moment worth being told about, not the moment to say nothing.
            LOGGER.info("no active followers for event %s", event_id)
            action = "закрив" if event.action is Action.CLOSE else "відкрив"
            side = "LONG" if event.position_type == 1 else "SHORT"
            lines = [
                "⚠️ <b>НІЧОГО НЕ СКОПІЙОВАНО</b>",
                "",
                f"Майстер {action} <b>{event.symbol}</b> {side}, але жоден акаунт не спрацював.",
                "",
            ]
            lines += [f"   • {reason}" for reason in skipped] or ["   • нема жодного фоловера"]
            await self._notify(NEWLINE.join(lines))
            return

        assert self._session is not None
        # Only for a same-side mirror: on a hedge the master's levels sit on the wrong side of the
        # entry, so they are deliberately left off (see _handle_stop_order).
        stops = (None, None) if reverse else await self._master_stops(event)

        engine = CopyEngine(self._store, self._session, retry_attempts=self._retry_attempts)
        results = await engine.execute(event, event_id, followers, reverse=reverse, stops=stops)

        if self.on_report:
            try:
                await self.on_report(event, results)
            except Exception:  # noqa: BLE001 — a failed report must not undo a placed trade
                # Logged, not swallowed. A report that never arrives looks exactly like a bot that
                # stopped working, and suppressing this left nothing behind to tell them apart.
                LOGGER.exception("could not deliver the report for event %s", event_id)

    # ── reconciliation ──────────────────────────────────────────────────────────────────────
    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reconcile_seconds)
            try:
                drifts = await self.reconcile()
                if drifts and self.on_notice:
                    lines = [
                        f"{d.account.label}: {d.symbol} очікувалось {d.expected_vol:g}, фактично {d.actual_vol:g}"
                        for d in drifts
                    ]
                    await self.on_notice("⚠️ Розбіжність позицій\n" + "\n".join(lines))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — reconciliation must never kill the service
                LOGGER.exception("reconciliation failed")

    async def reconcile(self) -> list[Drift]:
        """Compare expected follower positions against the exchange (spec §23).

        Reports rather than auto-corrects: silently "fixing" a difference could just as easily
        double a position as repair one, and the user should decide.
        """
        if not self._session:
            return []
        drifts: list[Drift] = []
        for follower in await self._store.list_accounts(self._folder_id, FOLLOWER):
            credentials = await self._store.get_credentials(follower.id, self._owner_id)
            if not credentials:
                continue
            client = MexcRestClient(*credentials, session=self._session)
            try:
                actual_positions = await client.get_open_positions()
            except MexcError as err:
                await self._store.set_account_error(follower.id, err.message)
                continue

            actual = {(p.symbol, p.position_type): p.hold_vol for p in actual_positions if p.hold_vol > 0}
            expected = await self._store.get_positions(follower.id)

            for key, row in expected.items():
                got = actual.get(key, 0.0)
                if abs(got - row.hold_vol) > max(1e-6, row.hold_vol * 0.01):
                    drifts.append(Drift(follower, key[0], key[1], row.hold_vol, got))
            for key, vol in actual.items():
                if key not in expected:
                    drifts.append(Drift(follower, key[0], key[1], 0.0, vol))
        return drifts

    async def _clear_expected_positions(self, account_id: int) -> None:
        for (symbol, position_type) in list((await self._store.get_positions(account_id)).keys()):
            await self._store.delete_position(account_id, symbol, position_type)
