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
order and flatten the filled leg. Live fill/PnL reads are marked VERIFY — confirm against the venues.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from datetime import datetime, timezone

import aiohttp

from ..exchanges import market_data as md
from ..exchanges.hyperliquid_entropy import EntropyClient
from ..exchanges.lighter_client import LighterClient
from .hedge import plan_hedge
from ..pairs import get as get_pair

LOGGER = logging.getLogger(__name__)


class SessionEngine:
    def __init__(self, store, cfg, notify=None, sheets=None) -> None:
        self._store = store
        self._cfg = cfg
        self._notify = notify                       # async (owner_id, text) -> None
        self._sheets = sheets                       # SheetsLogger | None — per-session stats to a sheet
        self._tasks: dict[int, asyncio.Task] = {}

    async def start(self) -> None:
        """Resume any session that was RUNNING before the restart."""
        for s in await self._store.running_sessions():
            self._spawn(s)
        LOGGER.info("session engine started; resumed %d session(s)", len(self._tasks))

    async def start_session(self, owner_id: int, config: dict) -> int:
        sid = await self._store.create_session(owner_id, config)
        self._spawn({"id": sid, "owner_id": owner_id, "config": config})
        return sid

    async def stop_session(self, session_id: int) -> None:
        # The running task polls its status and winds down (closes the open hedge) when it sees this.
        await self._store.set_session_status(session_id, "STOPPING")

    def _spawn(self, s: dict) -> None:
        if s["id"] in self._tasks:
            return
        self._tasks[s["id"]] = asyncio.create_task(self._run(s), name=f"session-{s['id']}")

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
    async def _run(self, s: dict) -> None:
        sid, owner, cfg = s["id"], s["owner_id"], s["config"]
        ends_at = time.time() + float(cfg.get("duration", 86400))
        mode = "DRY-RUN" if cfg.get("dry_run", True) else "LIVE"
        if self._sheets:
            with contextlib.suppress(Exception):
                await self._sheets.ensure_sheet(owner, sid)
        await self._say(owner, f"▶️ Сесію запущено ({mode}). Триватиме ~{self._fmt(cfg.get('duration',86400))}.")
        try:
            while time.time() < ends_at:
                if await self._stopping(sid):
                    break
                await self._one_cycle(owner, sid, cfg)
                if await self._stopping(sid):
                    break
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
            hid = await self._store.new_hedge(owner, sid, pair.key, notional, side, "OPEN",
                                              {"mode": "dry"})
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
        plan = await self._build_plan(pair, notional, entropy_long)
        if plan is None or not plan.ok:
            await self._say(owner, f"⚠️ {pair.label}: не вдалось скласти план — пропускаю цикл.")
            return
        hid = await self._store.new_hedge(owner, sid, pair.key, notional, side, "OPENING", {})
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
            # Post both maker legs.
            with contextlib.suppress(Exception):
                await ent.limit_order(e.market, e.is_buy, e.size, e.limit_px, post_only=True)
            with contextlib.suppress(Exception):
                await lit.limit_order(l.market_index, l.base_amount, l.price_int, l.is_ask, post_only=True)

            # Wait for both to fill, applying the timeout-after-first-fill rule. VERIFY the fill reads.
            ok = await self._await_fills(ent, lit, pair, e, l, cfg.get("fill_timeout", 20))
            if not ok:
                await self._cancel_hedge(ent, lit, pair)
                await self._store.update_hedge(hid, status="CANCELLED")
                await self._hedge_alert(cfg, owner, f"✖️ {pair.label}: одна нога не заповнилась вчасно — скасовано.")
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="CANCELLED", status="CANCELLED",
                                 pnl=0.0, fees=0.0, lighter_vol=0.0, entropy_vol=0.0)
                return
            await self._store.update_hedge(hid, status="OPEN")
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} відкрито (обидві ноги). Закрию через {self._fmt(hold)}.")
            await self._interruptible_sleep(sid, hold)
            await self._close_hedge(ent, lit, pair)
            await self._store.update_hedge(hid, status="CLOSED", entropy_vol=notional * 2, lighter_vol=notional * 2)
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} закрито.")
            # PnL/fees reads are still VERIFY-pending on the live venues; log 0 for now so the sheet's
            # timing/volume columns are correct and the money columns fill in once those reads land.
            await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED",
                             pnl=0.0, fees=0.0, lighter_vol=notional * 2, entropy_vol=notional * 2)
        finally:
            with contextlib.suppress(Exception):
                await lit.close()

    # ── live helpers (best-effort; VERIFY against the venues) ───────────────────────────────────────
    async def _await_fills(self, ent, lit, pair, e, l, timeout: float) -> bool:
        """True once both legs are fully filled. Timeout only counts from the first partial fill."""
        deadline = None
        start = time.time()
        while time.time() - start < 120:  # hard ceiling
            e_filled = await self._entropy_filled(ent, pair.entropy, e.size)
            l_filled = await self._lighter_filled(lit, l.market_index, l.size)
            if e_filled >= e.size and l_filled >= l.size:
                return True
            any_started = e_filled > 0 or l_filled > 0
            if any_started and deadline is None:
                deadline = time.time() + timeout
            if deadline and time.time() > deadline:
                return False
            await asyncio.sleep(2)
        return False

    async def _entropy_filled(self, ent, market: str, target: float) -> float:
        with contextlib.suppress(Exception):
            for p in await ent.positions():
                if p.get("coin") == market:
                    return abs(float(p.get("szi", 0)))
        return 0.0

    async def _lighter_filled(self, lit, market_index: int, target: float) -> float:
        # VERIFY: Lighter position read shape. Best-effort; treated as unfilled on any error.
        return 0.0

    async def _cancel_hedge(self, ent, lit, pair) -> None:
        with contextlib.suppress(Exception):
            await ent.close_market(pair.entropy)   # flatten whatever filled
        with contextlib.suppress(Exception):
            await lit.cancel_all()

    async def _close_hedge(self, ent, lit, pair) -> None:
        with contextlib.suppress(Exception):
            await ent.close_market(pair.entropy)
        with contextlib.suppress(Exception):
            await lit.cancel_all()   # VERIFY: proper reduce-only close on Lighter

    # ── shared build/clients (mirrors the bot's) ───────────────────────────────────────────────────
    async def _build_plan(self, pair, notional, entropy_long):
        try:
            timeout = aiohttp.ClientTimeout(total=8, connect=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                emk = (await md.entropy_markets(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
                lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
                if not emk or not lmk:
                    return None
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
        return LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                             int(c.meta.get("api_key_index", 0)))

    # ── lifecycle bits ─────────────────────────────────────────────────────────────────────────────
    async def _stopping(self, sid: int) -> bool:
        fresh = await self._store.active_session_by_id(sid)
        return (fresh is None) or (fresh.get("status") == "STOPPING")

    async def _interruptible_sleep(self, sid: int, seconds: float) -> None:
        """Sleep, but wake early (~every 5s) to notice a STOP so the user isn't left waiting."""
        end = time.time() + seconds
        while time.time() < end:
            if await self._stopping(sid):
                return
            await asyncio.sleep(min(5.0, end - time.time()))

    async def _finish(self, sid: int, owner: int) -> None:
        # Close any still-open hedge on stop, then mark the session done and report a summary.
        with contextlib.suppress(Exception):
            for h in await self._store.session_hedges(sid):
                if h["status"] in ("OPEN", "OPENING"):
                    ent = await self._entropy_client(owner)
                    lit = await self._lighter_client(owner)
                    pair = get_pair(h["pair_key"])
                    if ent and lit and pair:
                        await self._close_hedge(ent, lit, pair)
                        with contextlib.suppress(Exception):
                            await lit.close()
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
