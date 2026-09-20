"""Auto-trading session engine.

A session opens a delta-neutral hedge, holds it a random time within the user's range, closes it,
optionally pauses a random time, and repeats until the session's duration elapses or the user stops
it. Each session is a row in hb_sessions and runs as its own asyncio task; on startup every RUNNING
row is resumed, so a redeploy doesn't abandon a session.

Two modes:
  • dry_run  — the full schedule runs but NO real orders are placed; each cycle is simulated and
               logged, so the timing/loop logic can be checked safely. This is the default.
  • live     — real maker orders on both venues, with the fill-timeout rule below.

Fill-timeout rule (live): after BOTH legs are posted, we watch fills. The timeout only bites once at
least one leg has STARTED filling — because two resting orders that simply haven't been touched (the
market didn't move) are no open position and no risk. But if one leg (partially) filled and the other
hasn't completed within `fill_timeout`, we are exposed (naked delta), so we cancel: pull the unfilled
order and flatten the filled leg.

Live fill detection reads real positions on both venues; close is reduce-only on both (Entropy market
close, Lighter reduce-only IOC crossing the book). PnL logged to the sheet is each leg's unrealised
PnL read just before flattening; fee itemisation is still pending (RH-Lighter is 0%, Entropy maker is
negligible), so the fees column is 0 for now. Field names on the Lighter position read are best-effort
across SDK versions — confirm the numbers on the first small live run.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import random
import time
from datetime import datetime, timedelta, timezone

import aiohttp

from ..exchanges import market_data as md
from ..exchanges.hyperliquid_entropy import EntropyClient
from ..exchanges.lighter_client import LighterClient
from .hedge import plan_hedge
from ..pairs import PAIRS, get as get_pair

LOGGER = logging.getLogger(__name__)


class SessionEngine:
    def __init__(self, store, cfg, notify=None, sheets=None, feed=None) -> None:
        self._store = store
        self._cfg = cfg
        self._notify = notify                       # async (owner_id, text) -> None
        self._sheets = sheets                       # SheetsLogger | None — per-session stats to a sheet
        self._feed = feed                           # PriceFeed | None — realtime io mids over WS
        self._tasks: dict[int, asyncio.Task] = {}

    async def start(self) -> None:
        """Resume RUNNING sessions after a restart — but only ONE per owner. Earlier double-taps can
        leave several RUNNING rows for the same user; resuming them all would recreate the margin
        fight, so keep the newest and retire the rest."""
        seen: set[int] = set()
        for s in await self._store.running_sessions():
            owner = s.get("owner_id")
            if owner in seen:
                with contextlib.suppress(Exception):
                    await self._store.set_session_status(s["id"], "STOPPED")  # retire the duplicate
                continue
            seen.add(owner)
            self._spawn(s, resumed=True)
        LOGGER.info("session engine started; resumed %d session(s)", len(self._tasks))

    async def start_session(self, owner_id: int, config: dict) -> int:
        # Guard against stacking (double-tap / race): one running session per owner. Concurrent
        # sessions fight over the same margin and flood "not enough margin".
        existing = await self._store.active_session(owner_id)
        if existing and existing.get("status") == "RUNNING":
            return existing["id"]
        sid = await self._store.create_session(owner_id, config)
        self._spawn({"id": sid, "owner_id": owner_id, "config": config})
        return sid

    async def stop_session(self, session_id: int) -> None:
        # The running task polls its status and winds down (closes the open hedge) when it sees this.
        await self._store.set_session_status(session_id, "STOPPING")

    def _spawn(self, s: dict, resumed: bool = False) -> None:
        if s["id"] in self._tasks:
            return
        self._tasks[s["id"]] = asyncio.create_task(self._run(s, resumed), name=f"session-{s['id']}")

    async def _say(self, owner_id: int, text: str) -> None:
        if self._notify:
            with contextlib.suppress(Exception):
                await self._notify(owner_id, text)

    async def _hedge_alert(self, cfg: dict, owner_id: int, text: str) -> None:
        """Per-hedge open/close chatter. Off by default (stats go to the sheet instead of the chat);
        the user can flip `notify_each` on in the session config if they want the pings back."""
        if cfg.get("notify_each"):
            await self._say(owner_id, text)

    async def _stat(self, owner_id: int, sid: int, **row) -> None:
        if self._sheets:
            with contextlib.suppress(Exception):
                await self._sheets.append_hedge(owner_id, sid, row)

    # ── the loop ─────────────────────────────────────────────────────────────────────────────────
    async def _run(self, s: dict, resumed: bool = False) -> None:
        sid, owner, cfg = s["id"], s["owner_id"], s["config"]
        ends_at = time.time() + float(cfg.get("duration", 86400))
        mode = "DRY-RUN" if cfg.get("dry_run", True) else "LIVE"
        if self._sheets:
            with contextlib.suppress(Exception):
                await self._sheets.ensure_sheet(owner, sid)
        verb = "🔄 Сесію відновлено після перезапуску" if resumed else "▶️ Сесію запущено"
        await self._say(owner, f"{verb} ({mode}). Триватиме ~{self._fmt(cfg.get('duration',86400))}.")
        try:
            while time.time() < ends_at:
                if await self._stopping(sid):
                    break
                try:
                    await self._one_cycle(owner, sid, cfg)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 — one bad cycle must not end the whole session
                    LOGGER.exception("cycle failed in session %s", sid)
                    await self._say(owner, "⚠️ Цикл впав з помилкою — сесія триває, пробую далі.")
                    await self._interruptible_sleep(sid, 5)
                if await self._stopping(sid):
                    break
                await self._interruptible_sleep(sid, 5)   # small gap so a failing cycle can't tight-loop/spam
                if cfg.get("pause_on"):
                    pause = random.uniform(cfg.get("pause_min", 300), cfg.get("pause_max", 1800))
                    await self._hedge_alert(cfg, owner, f"⏸ Пауза {self._fmt(pause)} до наступного хеджа.")
                    await self._interruptible_sleep(sid, pause)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a session must not crash the whole engine
            LOGGER.exception("session %s crashed", sid)
        finally:
            await self._finish(sid, owner)
            self._tasks.pop(sid, None)

    async def _one_cycle(self, owner: int, sid: int, cfg: dict) -> None:
        pair = get_pair(random.choice(cfg["coins"]))
        if not pair:
            return
        leverage = int(cfg["leverage"])
        notional = float(cfg["margin"]) * leverage
        entropy_long = bool(random.getrandbits(1))   # randomise side each cycle
        hold = random.uniform(cfg.get("hold_min", 1800), cfg.get("hold_max", 7200))
        side = "LONG" if entropy_long else "SHORT"

        if cfg.get("dry_run", True):
            opened_at = datetime.now(timezone.utc)
            close_at = opened_at + timedelta(seconds=hold)
            hid = await self._store.new_hedge(owner, sid, pair.key, notional, side, "OPEN",
                                              {"mode": "dry", "hold": hold, "close_at": close_at.isoformat()})
            await self._hedge_alert(cfg, owner, f"🧪 [dry] Відкрив {pair.label} Entropy {side} ${notional:g}. "
                                                f"Закрию через {self._fmt(hold)}.")
            await self._interruptible_sleep(sid, hold)
            await self._store.update_hedge(hid, status="CLOSED", realized_pnl=0.0, fees=0.0,
                                           entropy_vol=notional * 2, lighter_vol=notional * 2)
            await self._hedge_alert(cfg, owner, f"🧪 [dry] Закрив {pair.label}. (симуляція)")
            await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED (dry)",
                             pnl=0.0, fees=0.0, lighter_vol=notional * 2, entropy_vol=notional * 2)
            return

        # ── live ──────────────────────────────────────────────────────────────────────────────────
        await self._live_cycle(owner, sid, pair, leverage, notional, entropy_long, hold, cfg)

    async def _live_cycle(self, owner, sid, pair, leverage, notional, entropy_long, hold, cfg) -> None:
        side = "LONG" if entropy_long else "SHORT"
        opened_at = datetime.now(timezone.utc)
        opened_ms = int(opened_at.timestamp() * 1000)
        plan = await self._build_plan(pair, notional, entropy_long)
        if plan is None or not plan.ok:
            why = plan.errors[0] if (plan and plan.errors) else "не вдалось скласти план"
            await self._say(owner, f"⚠️ {pair.label}: {html.escape(str(why))} — пропускаю цикл.")
            return
        close_at = opened_at + timedelta(seconds=hold)
        hid = await self._store.new_hedge(owner, sid, pair.key, notional, side, "OPENING",
                                          {"hold": hold, "close_at": close_at.isoformat()})
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        if not ent or not lit:
            await self._store.update_hedge(hid, status="FAILED")
            await self._say(owner, "⚠️ Немає ключів — зупиняю цикл.")
            return
        try:
            e, l = plan.entropy, plan.lighter
            with contextlib.suppress(Exception):
                await ent.set_leverage(e.market, leverage)
            with contextlib.suppress(Exception):
                await lit.set_leverage(l.market_index, leverage)
            # Fire BOTH legs at once (gather) so they hit the market simultaneously — sequential posting
            # let the price drift between legs and widened the hedge's PnL. Each response is checked:
            # a rejected order (margin, min notional) doesn't raise, so this is where it surfaces.
            e_err, l_err = await asyncio.gather(
                self._place(lambda: ent.limit_order(e.market, e.is_buy, e.size, e.limit_px, post_only=plan.post_only),
                            ent.order_error),
                self._place(lambda: lit.limit_order(l.market_index, l.base_amount, l.price_int, l.is_ask, post_only=plan.post_only),
                            lit.order_error),
            )
            if e_err or l_err:
                # At least one leg didn't rest — flatten anything that did and report why.
                await self._cancel_hedge(ent, lit, pair, owner=owner)
                await self._store.update_hedge(hid, status="FAILED")
                parts = []
                if e_err:
                    parts.append(f"Entropy: {html.escape(str(e_err))}")
                if l_err:
                    parts.append(f"Lighter: {html.escape(str(l_err))}")
                await self._say(owner, f"⛔ {pair.label}: ордер відхилено.\n" + "\n".join(parts))
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="FAILED", status="FAILED",
                                 pnl=0.0, fees=0.0, lighter_vol=0.0, entropy_vol=0.0)
                return

            # Wait for both to fill, applying the timeout-after-first-fill rule.
            ok = await self._await_fills(sid, ent, lit, pair, e, l, cfg.get("fill_timeout", 20))
            if not ok:
                res = await self._cancel_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms)
                await self._store.update_hedge(hid, status="CANCELLED",
                                               realized_pnl=res["pnl"], fees=res["fees"])
                await self._hedge_alert(cfg, owner, f"✖️ {pair.label}: одна нога не заповнилась вчасно — скасовано.")
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="CANCELLED", status="CANCELLED",
                                 pnl=res["pnl"], fees=res["fees"], lighter_vol=0.0, entropy_vol=0.0)
                return
            await self._store.update_hedge(hid, status="OPEN")
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} відкрито (обидві ноги). Закрию через {self._fmt(hold)}.")
            await self._interruptible_sleep(sid, hold)
            res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms)
            await self._store.update_hedge(hid, status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"],
                                           entropy_vol=notional * 2, lighter_vol=notional * 2)
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} закрито. PnL ≈ ${res['pnl']:g}.")
            await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED",
                             pnl=res["pnl"], fees=res["fees"], lighter_vol=notional * 2, entropy_vol=notional * 2)
        finally:
            with contextlib.suppress(Exception):
                await lit.close()

    # ── live helpers ────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    async def _place(do_order, error_parser) -> str | None:
        """Run one leg's order and return a rejection reason (or None if accepted). Covers both a
        raised exception and a response that merely reports an error without raising."""
        try:
            resp = await do_order()
        except Exception as err:  # noqa: BLE001
            return str(err)[:200]
        try:
            return error_parser(resp)
        except Exception:  # noqa: BLE001
            return None

    async def _await_fills(self, sid: int, ent, lit, pair, e, l, timeout: float) -> bool:
        """True once both legs are fully filled. Timeout only counts from the first partial fill.
        Bails out early (returns False) when the user hits STOP, so a stop isn't stuck behind a fill
        wait — the caller then flattens whatever filled."""
        deadline = None
        start = time.time()
        e_target, l_target = e.size * 0.999, l.size * 0.999   # tolerance for size rounding
        while time.time() - start < 120:  # hard ceiling
            if await self._stopping(sid):
                return False
            e_filled = await self._entropy_filled(ent, pair.entropy, e.size)
            l_filled = await self._lighter_filled(lit, l.market_index, l.size)
            if e_filled >= e_target and l_filled >= l_target:
                return True
            any_started = e_filled > 0 or l_filled > 0
            if any_started and deadline is None:
                deadline = time.time() + timeout
            if deadline and time.time() > deadline:
                return False
            await asyncio.sleep(1)
        return False

    async def _entropy_filled(self, ent, market: str, target: float) -> float:
        with contextlib.suppress(Exception):
            p = await ent.position(market)
            if p:
                return abs(float(p.get("szi", 0) or 0))
        return 0.0

    async def _lighter_filled(self, lit, market_index: int, target: float) -> float:
        with contextlib.suppress(Exception):
            pos = await lit.position(market_index)
            if pos:
                return float(pos.get("abs", 0) or 0)
        return 0.0

    async def _cancel_hedge(self, ent, lit, pair, owner=None, since_ms=None) -> dict:
        # Same flatten path as a normal close: flatten whatever filled on both venues and pull the
        # unfilled maker legs, so a one-sided fill can never be left as naked delta.
        return await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms)

    async def _close_hedge(self, ent, lit, pair, owner=None, since_ms=None) -> dict:
        """Flatten BOTH legs simultaneously and return {pnl, fees, errors}.

        PnL/fees: the Entropy leg's realized PnL and fee are read from the exchange's own fills since
        `since_ms` (accurate, includes the close); the Lighter leg's directional PnL is its unrealised
        PnL read just before flattening (RH-Lighter fee is 0). A failed close is reported, not swallowed
        — a leg left open ties up margin and blocks the next hedge."""
        errors: list[str] = []
        lmk = mark = None
        with contextlib.suppress(Exception):
            timeout = aiohttp.ClientTimeout(total=8, connect=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
                if lmk:
                    mark = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)

        # Lighter directional PnL, captured just before we flatten it (fee = 0 on RH).
        l_pnl = 0.0
        if lmk:
            with contextlib.suppress(Exception):
                lp = await lit.position(lmk.market_id)
                if lp:
                    l_pnl = float(lp.get("unrealized_pnl", 0) or 0)

        # Flatten both venues AT ONCE so neither leg sits naked while the other closes.
        async def _close_ent():
            with contextlib.suppress(Exception):
                await ent.close_market(pair.entropy)
            with contextlib.suppress(Exception):
                await ent.cancel_all(pair.entropy)

        async def _close_lit():
            err = await self._close_lighter(lit, lmk, mark) if lmk else None
            with contextlib.suppress(Exception):
                await lit.cancel_all()
            return err

        _, l_err = await asyncio.gather(_close_ent(), _close_lit())
        if l_err:
            errors.append(f"Lighter не закрився: {l_err}")

        # Accurate Entropy realized PnL + fee from the exchange's fills (give them a moment to register).
        e_pnl = e_fee = 0.0
        if since_ms is not None:
            await asyncio.sleep(1.5)
            with contextlib.suppress(Exception):
                e_pnl, e_fee = await ent.realized_since(pair.entropy, since_ms)

        if owner and errors:
            await self._say(owner, f"⚠️ {pair.label}: " + "; ".join(html.escape(str(e)) for e in errors))
        return {"pnl": round(e_pnl + l_pnl, 4), "fees": round(e_fee, 4), "errors": errors}

    async def _close_lighter(self, lit, lmk, mark) -> str | None:
        """Reduce-only close crossing the book (IOC). Returns None once flat, or an error string if the
        position is still open after retries — so the caller can surface it instead of hiding it."""
        for attempt in range(2):
            pos = None
            with contextlib.suppress(Exception):
                pos = await lit.position(lmk.market_id)
            if not pos or pos.get("abs", 0) <= 0:
                return None                                  # already flat
            ref = float(mark or pos.get("entry") or 0)
            if ref <= 0:
                return "немає ціни для закриття"
            is_long = pos["size"] > 0
            off = 0.02 * (attempt + 1)                        # 2% then 4% — cross for sure
            px = ref * (1 - off if is_long else 1 + off)
            base_amount, price_int = md.lighter_amounts(lmk, pos["abs"], px)
            try:
                resp = await lit.limit_order(lmk.market_id, base_amount, price_int, is_long,
                                             reduce_only=True, ioc=True)
                err = lit.order_error(resp)
            except Exception as e:  # noqa: BLE001
                err = str(e)[:200]
            if err:
                return err
            await asyncio.sleep(1)                             # let the fill settle, then re-read
        pos = None
        with contextlib.suppress(Exception):
            pos = await lit.position(lmk.market_id)
        return "позиція ще відкрита після закриття" if (pos and pos.get("abs", 0) > 0) else None

    # ── shared build/clients (mirrors the bot's) ───────────────────────────────────────────────────
    async def _build_plan(self, pair, notional, entropy_long):
        try:
            timeout = aiohttp.ClientTimeout(total=8, connect=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                emk = (await md.entropy_markets(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
                lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
                if not emk or not lmk:
                    return None
                # Prefer the realtime WS mid; fall back to a REST mark if the feed is cold/stale.
                eprice = (self._feed.mid(pair.entropy) if self._feed else None)
                if not eprice:
                    eprice = (await md.entropy_marks(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
                lprice = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)
                if not eprice or not lprice:
                    return None
            return plan_hedge(pair, notional, entropy_long=entropy_long, entropy_price=eprice,
                              lighter_price=lprice, entropy_market=emk, lighter_market=lmk)
        except Exception:  # noqa: BLE001
            LOGGER.exception("session build_plan failed")
            return None

    async def _entropy_client(self, owner):
        c = await self._store.get_credentials(owner, "entropy")
        if not c:
            return None
        return await asyncio.to_thread(
            EntropyClient, self._cfg.hyperliquid_api_url, c.meta["wallet_address"], c.secret, self._cfg.entropy_dex)

    async def _lighter_client(self, owner):
        c = await self._store.get_credentials(owner, "lighter")
        if not c:
            return None
        # MUST build on the event loop: the Lighter SDK creates an aiohttp connector in its
        # constructor, which calls asyncio.get_running_loop() — a worker thread has none, so
        # to_thread here raised "no running event loop" and killed every Lighter action. The
        # constructor does no network, so building inline is cheap.
        return LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                             int(c.meta.get("api_key_index", 0)))

    # ── lifecycle bits ─────────────────────────────────────────────────────────────────────────────
    async def _stopping(self, sid: int) -> bool:
        fresh = await self._store.active_session_by_id(sid)
        return (fresh is None) or (fresh.get("status") == "STOPPING")

    async def _interruptible_sleep(self, sid: int, seconds: float) -> None:
        """Sleep, but wake often to notice a STOP so the user isn't left waiting when they hit stop."""
        end = time.time() + seconds
        while time.time() < end:
            if await self._stopping(sid):
                return
            await asyncio.sleep(min(1.5, max(0.05, end - time.time())))

    async def _finish(self, sid: int, owner: int) -> None:
        # STOP means: pull EVERY resting order and flatten EVERY position on both venues — not just
        # the hedges the DB knows about (a cancelled cycle can leave a resting leg behind).
        sess = await self._store.active_session_by_id(sid)
        coins = list((sess or {}).get("config", {}).get("coins", []) or [p.key for p in PAIRS])
        with contextlib.suppress(Exception):
            ent = await self._entropy_client(owner)
            lit = await self._lighter_client(owner)
            try:
                if ent:
                    with contextlib.suppress(Exception):
                        await ent.cancel_all()          # cancel all resting Entropy orders
                if lit:
                    with contextlib.suppress(Exception):
                        await lit.cancel_all()          # cancel all resting Lighter orders
                # Flatten any open position for every coin the session could have touched.
                for key in coins:
                    pair = get_pair(key)
                    if ent and lit and pair:
                        with contextlib.suppress(Exception):
                            await self._close_hedge(ent, lit, pair, owner=owner)
            finally:
                if lit:
                    with contextlib.suppress(Exception):
                        await lit.close()
        # Mark any DB hedges still marked open as closed.
        with contextlib.suppress(Exception):
            for h in await self._store.session_hedges(sid):
                if h["status"] in ("OPEN", "OPENING"):
                    await self._store.update_hedge(h["id"], status="CLOSED")
        await self._store.set_session_status(sid, "DONE")
        if self._sheets:
            with contextlib.suppress(Exception):
                closed = [h for h in await self._store.session_hedges(sid) if h["status"] == "CLOSED"]
                rows = [{"pnl": h.get("realized_pnl"), "fees": h.get("fees"),
                         "lighter_vol": h.get("lighter_vol"), "entropy_vol": h.get("entropy_vol")}
                        for h in closed]
                await self._sheets.append_totals(owner, sid, rows)
        await self._say(owner, await self._summary(sid))

    async def _summary(self, sid: int) -> str:
        hedges = await self._store.session_hedges(sid)
        closed = [h for h in hedges if h["status"] == "CLOSED"]
        pnl = sum((h.get("realized_pnl") or 0) for h in closed)
        fees = sum((h.get("fees") or 0) for h in closed)
        evol = sum((h.get("entropy_vol") or 0) for h in closed)
        lvol = sum((h.get("lighter_vol") or 0) for h in closed)
        return (f"🏁 Сесію завершено.\nХеджів: {len(hedges)} (закрито {len(closed)})\n"
                f"PnL: ${pnl:,.2f}  ·  комісії: ${fees:,.2f}\n"
                f"Обсяг Entropy: ${evol:,.0f}  ·  Lighter: ${lvol:,.0f}")

    @staticmethod
    def _fmt(seconds: float) -> str:
        seconds = int(seconds)
        if seconds >= 86400:
            return f"{seconds/86400:.1f}д"
        if seconds >= 3600:
            return f"{seconds/3600:.1f}г"
        return f"{seconds/60:.0f}хв"
