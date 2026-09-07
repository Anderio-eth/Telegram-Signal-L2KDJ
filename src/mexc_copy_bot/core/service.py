"""The copy-trading service: watches the master, drives the engine, reconciles drift.

Everything stateful lives here so the Telegram layer stays a pure control surface (spec §34):
handlers call start()/stop()/status() and never touch MEXC or the database directly.

Restart safety (spec §31): run state is persisted, and on boot the master's real positions become
the baseline before monitoring resumes — otherwise the first push after a restart would look like
the master had just opened everything from scratch, and every follower would copy it again.

One instance per owner (see core/registry.py). Each has its own master socket, its own
followers and its own run state, so one person starting, stopping or emergency-closing never
reaches into somebody else's accounts. `owner_id` is passed to every store call rather than
filtered afterwards.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiohttp

from ..db.store import FOLLOWER, MODE_REVERSE, Account, Store
from ..mexc.rest import MexcError, MexcRestClient, PositionStops
from ..mexc.websocket import MasterWebSocket
from .copy_engine import CopyEngine, FollowerResult
from .events import Action, MasterEvent, MasterPositionTracker, PositionSnapshot, parse_position

LOGGER = logging.getLogger(__name__)

# How long START waits for the master socket before answering. The login takes about a second;
# waiting for it means the menu drawn immediately afterwards shows the true state, instead of a
# "reconnecting" that was only ever a race and then sits there, because a Telegram message
# never redraws itself.
CONNECT_TIMEOUT_SECONDS = 12.0

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
        owner_id: int,
        *,
        retry_attempts: int = 3,
        reconcile_seconds: int = 60,
        ws_reconnect_max_seconds: float = 30.0,
    ) -> None:
        self._store = store
        self._owner_id = owner_id
        self._retry_attempts = retry_attempts
        self._reconcile_seconds = reconcile_seconds
        self._ws_reconnect_max = ws_reconnect_max_seconds

        self._session: aiohttp.ClientSession | None = None
        self._ws: MasterWebSocket | None = None
        self._tracker = MasterPositionTracker()
        self._reconcile_task: asyncio.Task[None] | None = None
        # Events are handled one at a time. Master actions arrive in order and often in bursts
        # (three orders filling one position); processing them concurrently would race the
        # position diff and could mirror the same delta twice.
        self._lock = asyncio.Lock()

        self.on_report: ReportCallback | None = None
        self.on_notice: NoticeCallback | None = None

    @property
    def owner_id(self) -> int:
        return self._owner_id

    # ── lifecycle ───────────────────────────────────────────────────────────────────────────
    @property
    def running(self) -> bool:
        return self._ws is not None

    @property
    def master_connected(self) -> bool:
        return bool(self._ws and self._ws.connected)

    async def start(self) -> str:
        if self._ws:
            return "Already running."

        master = await self._store.get_master(self._owner_id)
        if not master:
            return "No master account configured."

        credentials = await self._store.get_credentials(master.id, self._owner_id)
        if not credentials:
            return "Master credentials could not be read."

        self._session = aiohttp.ClientSession()
        api_key, secret = credentials
        self._ws = MasterWebSocket(
            api_key,
            secret,
            on_position=self._handle_position,
            on_stop_order=self._handle_stop_order,
            on_resync=self._resync_master,
            on_status=self._notice,
            reconnect_max_seconds=self._ws_reconnect_max,
        )
        self._ws.start()
        self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="reconcile")
        await self._store.set_running(self._owner_id, True)

        if await self._ws.wait_connected(CONNECT_TIMEOUT_SECONDS):
            return "✅ Copy trading started — master connected."
        # Not an error: the socket keeps retrying on its own. But reporting a flat "started" while
        # nothing is listening to the master is the kind of half-truth that gets noticed only
        # after a missed trade.
        return "⚠️ Copy trading started, but the master is not connected yet — still retrying."

    async def stop(self) -> str:
        """Stop copying NEW actions. Existing follower positions are left untouched (spec §7)."""
        if self._reconcile_task:
            self._reconcile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconcile_task
            self._reconcile_task = None
        if self._ws:
            await self._ws.stop()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None
        await self._store.set_running(self._owner_id, False)
        return "Copy trading stopped. Open positions were left as they are."

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
            for follower in await self._store.list_accounts(self._owner_id, FOLLOWER):
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
    async def _notice(self, message: str) -> None:
        if self.on_notice:
            with contextlib.suppress(Exception):
                await self.on_notice(f"Master: {message}")

    async def _resync_master(self) -> None:
        """Make the master's real positions the baseline, emitting nothing.

        Called at connect and after every reconnect. Without this, the first push following a
        gap would be diffed against a stale size and copied as a phantom change.
        """
        master = await self._store.get_master(self._owner_id)
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
        master = await self._store.get_master(self._owner_id)
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

        mode, _ = await self._store.get_mode(self._owner_id)
        if mode == MODE_REVERSE:
            # Deliberate: a hedge holds the opposite side, so the master's stop price sits on the
            # wrong side of its entry — it would close the hedge for a profit and let the loss run.
            # Reversing the levels is a different feature; until it exists, do nothing.
            LOGGER.info("reverse mode: master stops are not mirrored")
            return

        master = await self._store.get_master(self._owner_id)
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

        followers = [a for a in await self._store.list_accounts(self._owner_id, FOLLOWER) if a.active]
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

    async def _handle_position(self, data: dict) -> None:
        snapshot = parse_position(data)
        if not snapshot:
            return

        async with self._lock:
            event = self._tracker.apply(snapshot)
            if event is None:
                return
            await self._dispatch(event, data)

    async def _dispatch(self, event: MasterEvent, raw: dict) -> None:
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
            LOGGER.info("duplicate master event ignored: %s", event.dedupe_key)
            return

        active = [a for a in await self._store.list_accounts(self._owner_id, FOLLOWER) if a.active]
        mode, reverse_account_id = await self._store.get_mode(self._owner_id)
        reverse = mode == MODE_REVERSE

        if reverse:
            # Exactly one account hedges the master. If it was deleted or deactivated, do nothing
            # and say so: quietly falling back to copying every follower would open positions on
            # the same side as the master, the precise opposite of what was asked for.
            followers = [a for a in active if a.id == reverse_account_id]
            if not followers:
                LOGGER.warning("reverse mode has no usable account for owner %s", self._owner_id)
                await self._notice(
                    "⚠️ Reverse mode is on but its account is missing or paused — nothing was mirrored."
                )
                return
        else:
            followers = active

        if not followers:
            LOGGER.info("no active followers for event %s", event_id)
            return

        assert self._session is not None
        # Only for a same-side mirror: on a hedge the master's levels sit on the wrong side of the
        # entry, so they are deliberately left off (see _handle_stop_order).
        stops = (None, None) if reverse else await self._master_stops(event)

        engine = CopyEngine(self._store, self._session, retry_attempts=self._retry_attempts)
        results = await engine.execute(event, event_id, followers, reverse=reverse, stops=stops)

        if self.on_report:
            with contextlib.suppress(Exception):
                await self.on_report(event, results)

    # ── reconciliation ──────────────────────────────────────────────────────────────────────
    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reconcile_seconds)
            try:
                drifts = await self.reconcile()
                if drifts and self.on_notice:
                    lines = [
                        f"{d.account.label}: {d.symbol} expected {d.expected_vol:g}, actual {d.actual_vol:g}"
                        for d in drifts
                    ]
                    await self.on_notice("⚠️ Position drift detected\n" + "\n".join(lines))
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
        for follower in await self._store.list_accounts(self._owner_id, FOLLOWER):
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
