"""Auto-trading session engine.

A session opens a delta-neutral hedge, holds it a random time within the user's range, closes it,
optionally pauses a random time, and repeats until the session's duration elapses or the user stops
it. Each session is a row in hb_sessions and runs as its own asyncio task; on startup every RUNNING
row is resumed, so a redeploy doesn't abandon a session.

Two modes:
  • dry_run  — the full schedule runs but NO real orders are placed; each cycle is simulated and
               logged, so the timing/loop logic can be checked safely. This is the default.
  • live     — maker-first execution (core/execution.py): the Entropy leg rests as a post-only limit
               a couple of ticks off the mid, and each fill is hedged on Lighter at market straight
               away. Closing is the same in reverse (reduce-only limit on Entropy, then Lighter),
               followed by a market pass that flattens anything the maker order didn't.

`fill_timeout` is how long the Entropy limit may work (re-quoting as the price moves). Nothing filled
by then -> the cycle is skipped with no position. Partly filled -> the filled part is kept if it could
be hedged, otherwise both legs are flattened so no naked delta is ever left.

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
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone

import aiohttp

from ..exchanges import market_data as md
from ..exchanges.hyperliquid_entropy import EntropyClient
from ..exchanges.lighter_client import LighterClient
from .execution import MakerExecutor
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
        # owner -> {"entropy": client, "lighter": client}. Built once per session and reused across
        # cycles — rebuilding per cycle re-loaded Entropy market meta every time (seconds of latency).
        self._clients: dict[int, dict] = {}
        self._exec = MakerExecutor(cfg)
        # (owner, pair.key) -> watcher task for hedges opened by hand in the bot (sessions guard their
        # own hedges inline while holding).
        self._watchers: dict[tuple, asyncio.Task] = {}
        # (owner, pair.key) being closed on purpose right now — the leg guard must not "react" to a
        # leg going flat because WE are closing it.
        self._closing: set[tuple] = set()

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

    @staticmethod
    async def _lit_equity(lit) -> float | None:
        """Lighter account equity (total asset value), or None. Flat-to-flat, its change across a hedge
        is that leg's exact realized PnL (RH fee is 0), which beats reading authed trade history."""
        try:
            b = await lit.balance()
            v = b.get("total")
            return float(v) if v is not None else None
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    async def _noop(default=None):
        return default

    @staticmethod
    async def _guard(coro, default=None):
        """Await a coroutine, swallowing errors and returning a default — for concurrent best-effort
        cleanup where one failure must not abort the gather."""
        try:
            return await coro
        except Exception:  # noqa: BLE001
            return default

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
            if resumed and not cfg.get("dry_run", True):
                await self._adopt_open_hedges(owner, sid, cfg)   # finish hedges left open by the restart
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
                    await self._drop_clients(owner)   # rebuild clients next cycle (connector may be dead)
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
            await self._finish(sid, owner, cfg)
            self._tasks.pop(sid, None)

    async def _one_cycle(self, owner: int, sid: int, cfg: dict) -> None:
        pair = get_pair(random.choice(cfg["coins"]))
        if not pair:
            return
        leverage = int(cfg["leverage"])
        margin = float(cfg["margin"])
        hold = random.uniform(cfg.get("hold_min", 1800), cfg.get("hold_max", 7200))

        if cfg.get("dry_run", True):
            entropy_long = bool(random.getrandbits(1))   # no prices in a dry run — any side will do
            side = "LONG" if entropy_long else "SHORT"
            notional = margin * leverage
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
        # Live: the side comes from the prices (long where it's cheaper) — see plan_hedge.
        await self._live_cycle(owner, sid, pair, leverage, margin, None, hold, cfg)

    async def _live_cycle(self, owner, sid, pair, leverage, margin, entropy_long, hold, cfg) -> None:
        opened_at = datetime.now(timezone.utc)
        opened_ms = int(opened_at.timestamp() * 1000)
        # Clamp leverage to what BOTH venues allow for this coin (Lighter caps OAI/ANTH at 5x); the
        # notional follows the effective leverage so the margin used stays the user's chosen amount.
        plan, leverage = await self._build_plan(pair, margin, leverage, entropy_long)
        if plan is None or not plan.ok:
            why = plan.errors[0] if (plan and plan.errors) else "не вдалось скласти план"
            await self._say(owner, f"⚠️ {pair.label}: {html.escape(str(why))} — пропускаю цикл.")
            return
        notional = plan.notional_usd
        entropy_long = plan.entropy.is_buy
        side = "LONG" if entropy_long else "SHORT"
        if int(leverage) < int(cfg["leverage"]):
            await self._say(owner, f"ℹ️ {pair.label}: плече знижено до {leverage}x (макс для цієї монети).")
        close_at = opened_at + timedelta(seconds=hold)
        hid = await self._store.new_hedge(owner, sid, pair.key, notional, side, "OPENING",
                                          {"hold": hold, "close_at": close_at.isoformat(),
                                           "entropy_px": plan.entropy_price, "lighter_px": plan.lighter_price})
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        if not ent or not lit:
            await self._store.update_hedge(hid, status="FAILED")
            await self._say(owner, "⚠️ Немає ключів — зупиняю цикл.")
            return
        # Never stack a new hedge on top of leftovers from a previous one (a leg that didn't close,
        # a resting order): flatten whatever is still open first.
        if not await self._ensure_flat(owner, ent, lit, pair):
            await self._store.update_hedge(hid, status="FAILED")
            await self._say(owner, f"⚠️ {pair.label}: попередня позиція ще відкрита — пропускаю цикл.")
            return
        try:
            e, l = plan.entropy, plan.lighter
            lit_equity_before = await self._lit_equity(lit)   # for exact Lighter PnL (equity delta)
            # (No spot->io transfer: io draws margin from spot on its own, and the agent API wallet
            # can't move funds anyway — positions open fine without it.)
            with contextlib.suppress(Exception):
                await ent.set_leverage(e.market, leverage)
            # Lighter leverage MUST be set correctly (isolated) or the position opens at the old
            # leverage — surface a failure so a wrong leverage doesn't go unnoticed.
            lev_err = await self._place(lambda: lit.set_leverage(l.market_index, leverage), lit.order_error)
            if lev_err:
                await self._say(owner, f"⚠️ {pair.label}: не вдалось виставити плече {leverage}x на Lighter: "
                                       f"{html.escape(str(lev_err))}")
            # Maker-first: Entropy post-only limit, Lighter at market as it fills (see core/execution).
            # until_complete: a hedge is either opened at the size the user asked for, or not opened.
            # Sessions never move on to the next cycle with a half-filled leg.
            fr = await self.open_maker_first(ent, lit, plan, timeout=cfg.get("fill_timeout", 90), sid=sid,
                                             until_complete=True, owner=owner, pair=pair)
            if fr.error or fr.unhedged > 0:
                # A leg failed, or a partial fill is too small to hedge on Lighter — flatten both.
                res = await self._cancel_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                               lit_equity_before=lit_equity_before)
                await self._store.update_hedge(hid, status="FAILED", realized_pnl=res["pnl"], fees=res["fees"])
                why = fr.error or "частковий філ менший за мінімум Lighter"
                await self._say(owner, f"⛔ {pair.label}: {html.escape(str(why))} — закрив обидві ноги.")
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="FAILED", status="FAILED",
                                 pnl=res["pnl"], fees=res["fees"], lighter_vol=0.0, entropy_vol=0.0)
                return
            if fr.e_filled <= 0:
                # The limit never filled: no position anywhere, nothing to unwind. Skip this cycle.
                await self._store.update_hedge(hid, status="CANCELLED")
                await self._hedge_alert(cfg, owner, f"⏭ {pair.label}: лімітка на Entropy не заповнилась за "
                                                    f"{self._fmt(cfg.get('fill_timeout', 90))} — пропускаю цикл.")
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="НЕ ЗАПОВНИЛОСЬ", status="CANCELLED",
                                 pnl=0.0, fees=0.0, lighter_vol=0.0, entropy_vol=0.0)
                return
            if not fr.complete:
                # With until_complete this only happens on STOP: keep what filled (it is hedged),
                # say so, and let the session wind down instead of starting a hold.
                planned = notional
                notional = round(notional * fr.e_filled / e.size, 2)
                await self._store.update_hedge(hid, notional_usd=notional)
                await self._say(owner, f"ℹ️ {pair.label}: зупинка під час набору — відкрито "
                                       f"${notional:g} з ${planned:g}/ногу (обидві ноги захеджовані).")
            await self._store.update_hedge(hid, status="OPEN")
            l_side = "SHORT" if entropy_long else "LONG"
            stops = await self.place_stops(ent, lit, pair, owner=owner)
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} відкрито (Entropy {side} ✓ / Lighter {l_side} ✓). "
                                                f"{self.stops_text(stops)}Закрию через {self._fmt(hold)}.")
            hit = await self.guard_legs(owner, ent, lit, pair, seconds=hold, sid=sid)
            if hit:
                # A stop (or a liquidation) took one leg out; guard_legs already flattened the other.
                res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                              lit_equity_before=lit_equity_before)
                await self._store.update_hedge(hid, status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"],
                                               entropy_vol=notional * 2, lighter_vol=notional * 2)
                await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="OK", status=f"STOP ({hit})",
                                 pnl=res["pnl"], fees=res["fees"], lighter_vol=notional * 2, entropy_vol=notional * 2)
                return
            res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                          lit_equity_before=lit_equity_before, maker=True, sid=sid,
                                          timeout=cfg.get("fill_timeout", 90))
            await self._store.update_hedge(hid, status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"],
                                           entropy_vol=notional * 2, lighter_vol=notional * 2)
            lit_mark = "✓" if not res.get("errors") else "✗"
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} закрито (Entropy ✓ / Lighter {lit_mark}). "
                                                f"PnL ≈ ${res['pnl']:g}, комісія ${res['fees']:g}.")
            await self._stat(owner, sid, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED",
                             pnl=res["pnl"], fees=res["fees"], lighter_vol=notional * 2, entropy_vol=notional * 2)
        finally:
            pass  # clients are cached for the whole session; closed in _finish

    async def _ensure_flat(self, owner, ent, lit, pair) -> bool:
        """True when both venues are flat for this pair. Anything left over (a leg that survived a
        failed close, a resting order) is flattened first — a new hedge must never be opened on top
        of an old one."""
        leftovers = []
        with contextlib.suppress(Exception):
            if await self._exec.entropy_szi(ent, pair.entropy):
                leftovers.append("Entropy")
        lmk = None
        with contextlib.suppress(Exception):
            lmk = (await md.lighter_markets(await self._exec.http(), self._cfg.lighter_api_url)).get(pair.lighter)
            if lmk:
                lp = await lit.position(lmk.market_id)
                if lp and lp.get("abs"):
                    leftovers.append("Lighter")
        if not leftovers:
            return True
        await self._say(owner, f"🧹 {pair.label}: лишилась стара позиція ({', '.join(leftovers)}) — закриваю перед новим хеджем.")
        await self._close_hedge(ent, lit, pair, owner=owner)
        with contextlib.suppress(Exception):
            if await self._exec.entropy_szi(ent, pair.entropy):
                return False
            if lmk:
                lp = await lit.position(lmk.market_id)
                if lp and lp.get("abs"):
                    return False
        return True

    async def _adopt_open_hedges(self, owner, sid, cfg) -> None:
        """After a restart, finish hedges that were left OPEN — wait out the rest of their planned hold
        (from detail.close_at) then close them, instead of abandoning the position and opening a new
        one on top (which fought for margin)."""
        hedges = await self._store.session_hedges(sid)
        openh = [h for h in hedges if h["status"] in ("OPEN", "OPENING")]
        if not openh:
            return
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        for h in openh:
            if await self._stopping(sid):
                return
            pair = get_pair(h["pair_key"])
            if not (pair and ent and lit):
                await self._store.update_hedge(h["id"], status="CLOSED")
                continue
            det = h.get("detail")
            if isinstance(det, str):
                with contextlib.suppress(Exception):
                    det = json.loads(det or "{}")
            det = det if isinstance(det, dict) else {}
            remaining = 0.0
            with contextlib.suppress(Exception):
                if det.get("close_at"):
                    remaining = (datetime.fromisoformat(det["close_at"]) - datetime.now(timezone.utc)).total_seconds()
            await self._say(owner, f"↩️ {pair.label}: підхопив відкритий хедж після рестарту — "
                                   f"{('закрию за ' + self._fmt(remaining)) if remaining > 0 else 'закриваю зараз'}.")
            since_ms = int(h["opened_at"].timestamp() * 1000) if h.get("opened_at") else None
            if remaining > 0:
                await self.place_stops(ent, lit, pair, owner=owner)
                hit = await self.guard_legs(owner, ent, lit, pair, seconds=remaining, sid=sid)
                if hit:
                    res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms)
                    await self._store.update_hedge(h["id"], status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"])
                    continue
            res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms, maker=True, sid=sid,
                                          timeout=cfg.get("fill_timeout", 90))
            await self._store.update_hedge(h["id"], status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"])
            vol = float(h.get("notional_usd", 0) or 0) * 2
            await self._stat(owner, sid, opened_at=h.get("opened_at"), closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=h.get("entropy_side", ""), open_status="OK", status="CLOSED",
                             pnl=res["pnl"], fees=res["fees"], lighter_vol=vol, entropy_vol=vol)

    # ── live helpers ────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    async def _place(do_order, error_parser) -> str | None:
        """Run one leg's order and return a rejection reason (or None if accepted). Covers a raised
        exception and a response that reports an error without raising. Retries once on a nonce error
        (the optimistic nonce manager can drift after a restart/failed tx and resyncs on the retry)."""
        err = None
        for attempt in range(2):
            try:
                resp = await do_order()
            except Exception as e:  # noqa: BLE001
                return str(e)[:200]
            try:
                err = error_parser(resp)
            except Exception:  # noqa: BLE001
                err = None
            if err and "nonce" in err.lower() and attempt == 0:
                await asyncio.sleep(0.6)   # let the nonce manager resync, then retry once
                continue
            return err
        return err

    # ── stop-losses 1% before liquidation ───────────────────────────────────────────────────────────
    STOP_BUFFER = 0.01          # stop sits 1% of price before the liquidation price (user's rule)
    STOP_SLIPPAGE = 0.03        # worst execution price accepted once the stop fires

    async def place_stops(self, ent, lit, pair, owner=None) -> dict:
        """Put a reduce-only stop-market on each leg at liquidation ±1% of price:
        long -> liq × 1.01, short -> liq × 0.99. Liquidation prices come from the venues themselves
        (Hyperliquid position.liquidationPx; Lighter isolated position.liquidation_price). Idempotent:
        existing orders on the market are cleared first, so re-placing never stacks stops.
        Returns {"entropy": {...}, "lighter": {...}} with liq/stop or an "error"."""
        out: dict = {"entropy": {}, "lighter": {}}
        http = await self._exec.http()
        emk = lmk = None
        with contextlib.suppress(Exception):
            emk = (await md.entropy_markets(http, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
        with contextlib.suppress(Exception):
            lmk = (await md.lighter_markets(http, self._cfg.lighter_api_url)).get(pair.lighter)

        # Entropy
        try:
            pos = await ent.position(pair.entropy)
            szi = float(pos.get("szi", 0) or 0) if pos else 0.0
            liq = float(pos.get("liquidationPx") or 0) if pos else 0.0
            if not szi:
                out["entropy"]["error"] = "позиції немає"
            elif not liq or not emk:
                out["entropy"]["error"] = "біржа не віддала ціну ліквідації"
            else:
                long = szi > 0
                trig = ent.round_px(liq * (1 + self.STOP_BUFFER if long else 1 - self.STOP_BUFFER), emk.sz_decimals)
                limit = ent.round_px(trig * (1 - self.STOP_SLIPPAGE if long else 1 + self.STOP_SLIPPAGE), emk.sz_decimals)
                with contextlib.suppress(Exception):
                    await ent.cancel_all(pair.entropy)          # no stacked stops
                err = ent.order_error(await ent.stop_loss(pair.entropy, not long, abs(szi), trig, limit))
                out["entropy"] = {"liq": liq, "stop": trig} if not err else {"liq": liq, "error": err}
        except Exception as e:  # noqa: BLE001
            out["entropy"]["error"] = str(e)[:200]

        # Lighter
        try:
            lp = await lit.position(lmk.market_id) if lmk else None
            if not lp or not lp.get("abs"):
                out["lighter"]["error"] = "позиції немає" if lmk else "ринок не знайдено"
            elif not lp.get("liq"):
                out["lighter"]["error"] = "біржа не віддала ціну ліквідації (маржа не ізольована?)"
            else:
                long = lp["size"] > 0
                liq = float(lp["liq"])
                trig = liq * (1 + self.STOP_BUFFER if long else 1 - self.STOP_BUFFER)
                worst = trig * (1 - self.STOP_SLIPPAGE if long else 1 + self.STOP_SLIPPAGE)
                base_amount, price_int = md.lighter_amounts(lmk, lp["abs"], worst)
                trig_int = round(trig * 10 ** lmk.price_decimals)
                with contextlib.suppress(Exception):
                    await lit.cancel_all()                    # one hedge at a time per owner
                err = await self._place(lambda: lit.stop_loss(lmk.market_id, base_amount, trig_int, price_int, long),
                                        lit.order_error)
                out["lighter"] = {"liq": liq, "stop": trig_int / 10 ** lmk.price_decimals} if not err else {"liq": liq, "error": err}
        except Exception as e:  # noqa: BLE001
            out["lighter"]["error"] = str(e)[:200]

        failed = [f"{v}: {out[v]['error']}" for v in ("entropy", "lighter") if out[v].get("error")]
        if owner and failed:
            await self._say(owner, f"⚠️ {pair.label}: стоп не виставлено — " + "; ".join(html.escape(f) for f in failed))
        return out

    @staticmethod
    def stops_text(stops: dict) -> str:
        parts = []
        for name, key in (("Entropy", "entropy"), ("Lighter", "lighter")):
            v = stops.get(key) or {}
            if v.get("stop"):
                parts.append(f"{name} стоп {v['stop']:g} (ліквідація {v['liq']:g})")
        return ("🛡 " + ", ".join(parts) + ". ") if parts else ""

    async def guard_legs(self, owner, ent, lit, pair, *, seconds: float, sid: int | None = None,
                         poll: float = 5.0) -> str | None:
        """Hold for `seconds` (or until STOP) while watching both legs. If one leg disappears — its stop
        fired or it was liquidated — the other is closed at market at once, so the hedge never sits as
        naked delta. Returns "entropy"/"lighter" (the leg that went first) or None if the hold ended
        normally. Reads that fail are skipped, never mistaken for a flat position."""
        key = (owner, pair.key)
        lmk = None
        with contextlib.suppress(Exception):
            lmk = (await md.lighter_markets(await self._exec.http(), self._cfg.lighter_api_url)).get(pair.lighter)
        if not lmk:
            await self._interruptible_sleep(sid, seconds) if sid is not None else await asyncio.sleep(seconds)
            return None
        start_e = start_l = None
        end = time.time() + seconds
        while time.time() < end:
            if sid is not None and await self._stopping(sid):
                return None
            if key not in self._closing:
                e_szi = await self._exec.entropy_szi(ent, pair.entropy)
                l_abs = None
                with contextlib.suppress(Exception):
                    lp = await lit.position(lmk.market_id)
                    l_abs = float(lp.get("abs", 0) or 0) if lp else 0.0
                if e_szi is not None and l_abs is not None:
                    e_abs = abs(e_szi)
                    if start_e is None and e_abs > 0 and l_abs > 0:
                        start_e, start_l = e_abs, l_abs
                    if start_e:
                        e_gone, l_gone = e_abs <= start_e * 0.05, l_abs <= start_l * 0.05
                        if e_gone and l_gone:
                            return None                       # both closed elsewhere — nothing to hedge
                        if e_gone or l_gone:
                            first = "entropy" if e_gone else "lighter"
                            other = "Lighter" if e_gone else "Entropy"
                            self._closing.add(key)
                            try:
                                if e_gone:
                                    mark = await self._exec.lighter_mid(lmk.market_id)
                                    err = await self._close_lighter(lit, lmk, mark)
                                else:
                                    resp = await ent.close_market(pair.entropy)
                                    err = ent.order_error(resp) if isinstance(resp, dict) else None
                                with contextlib.suppress(Exception):
                                    await ent.cancel_all(pair.entropy)
                                with contextlib.suppress(Exception):
                                    await lit.cancel_all()
                            finally:
                                self._closing.discard(key)
                            tail = f" Помилка закриття: {html.escape(str(err))}" if err else ""
                            await self._say(owner, f"🛑 {pair.label}: нога на {'Entropy' if e_gone else 'Lighter'} закрилась "
                                                   f"(стоп або ліквідація) — закрив {other} маркетом.{tail}")
                            return first
            await asyncio.sleep(min(poll, max(0.2, end - time.time())))
        return None

    def watch_manual(self, owner, ent, lit, pair) -> None:
        """Guard a hedge opened by hand in the bot until it is closed (no hold timer)."""
        key = (owner, pair.key)
        old = self._watchers.pop(key, None)
        if old:
            old.cancel()

        async def run():
            with contextlib.suppress(asyncio.CancelledError):
                await self.guard_legs(owner, ent, lit, pair, seconds=30 * 86400)
            self._watchers.pop(key, None)

        self._watchers[key] = asyncio.create_task(run(), name=f"guard-{owner}-{pair.key}")

    async def open_maker_first(self, ent, lit, plan, *, timeout: float, sid: int | None = None,
                               until_complete: bool = False, owner=None, pair=None):
        """Open a planned hedge maker-first and verify the Lighter leg actually landed.

        Returns core.execution.FillResult. Shared with the bot's manual open. Assumes Lighter is flat
        on this market beforehand (one session per owner, hedges closed between cycles)."""
        e, l = plan.entropy, plan.lighter
        stopping = (lambda: self._stopping(sid)) if sid is not None else None
        async def ping(filled: float, target: float) -> None:
            if owner:
                await self._say(owner, f"⏳ {pair.label if pair else ''}: лімітка на Entropy заповнена на "
                                       f"{filled / target * 100:.0f}% ({filled:g} з {target:g}) — дотягую до повного розміру.")

        fr = await self._exec.fill(ent, lit, plan.entropy_market, plan.lighter_market,
                                   e_is_buy=e.is_buy, e_target=e.size, l_is_ask=l.is_ask, l_target=l.size,
                                   closing=False, timeout=timeout, stopping=stopping,
                                   until_complete=until_complete, on_progress=ping)
        if fr.error or fr.l_done <= 0:
            return fr
        # IOC can fill short on a thin book — top the Lighter leg up once if it's missing a real chunk.
        await asyncio.sleep(0.5)
        have = 0.0
        with contextlib.suppress(Exception):
            lp = await lit.position(l.market_index)
            have = float(lp.get("abs", 0) or 0) if lp else 0.0
        short = round(fr.l_done - have, plan.lighter_market.size_decimals)
        if short > 0:
            lpx = await self._exec.lighter_mid(l.market_index) or 0.0
            lmk = plan.lighter_market
            if short >= lmk.min_base and short * lpx >= (lmk.min_quote or 0):
                err = await self._exec.lighter_market_order(lit, lmk, short, l.is_ask, reduce_only=False)
                if err:
                    fr.error = f"Lighter (добір): {err}"
            elif short * lpx > 1.0:
                fr.unhedged = short   # a real gap that can't be topped up — caller flattens both
        return fr

    async def _maker_close(self, ent, lit, pair, lmk, *, timeout: float, sid: int | None) -> None:
        """Reduce the hedge maker-first: Entropy reduce-only limit, Lighter reduce-only at market as it
        fills. Whatever is left afterwards is flattened by the caller's market pass."""
        emk = None
        with contextlib.suppress(Exception):
            emk = (await md.entropy_markets(await self._exec.http(), self._cfg.hyperliquid_api_url,
                                            self._cfg.entropy_dex)).get(pair.entropy)
        szi = await self._exec.entropy_szi(ent, pair.entropy)
        if not emk or not szi:
            return
        lpos = None
        with contextlib.suppress(Exception):
            lpos = await lit.position(lmk.market_id)
        l_abs = float(lpos.get("abs", 0) or 0) if lpos else 0.0
        l_is_ask = (lpos["size"] > 0) if lpos else (szi < 0)   # sell to close a Lighter long
        # No `stopping` here: STOP is usually what asked for this close, so it must not abort it.
        # The maker order gets its timeout, then the caller's market pass finishes the job.
        fr = await self._exec.fill(ent, lit, emk, lmk, e_is_buy=szi < 0, e_target=abs(szi),
                                   l_is_ask=l_is_ask, l_target=l_abs, closing=True,
                                   timeout=timeout, stopping=None)
        if fr.error:
            LOGGER.warning("maker close %s: %s — falling back to market", pair.label, fr.error)

    async def _cancel_hedge(self, ent, lit, pair, owner=None, since_ms=None, lit_equity_before=None) -> dict:
        # Same flatten path as a normal close: flatten whatever filled on both venues and pull the
        # unfilled maker legs, so a one-sided fill can never be left as naked delta.
        return await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms,
                                       lit_equity_before=lit_equity_before)

    async def _close_hedge(self, ent, lit, pair, owner=None, since_ms=None, lit_equity_before=None,
                           maker=False, sid=None, timeout: float = 60) -> dict:
        """Flatten BOTH legs simultaneously and return {pnl, fees, errors}.

        PnL/fees: the Entropy leg's realized PnL and fee are read from the exchange's own fills since
        `since_ms` (accurate, includes the close); the Lighter leg's directional PnL is its unrealised
        PnL read just before flattening (RH-Lighter fee is 0). A failed close is reported, not swallowed
        — a leg left open ties up margin and blocks the next hedge."""
        errors: list[str] = []
        key = (owner, pair.key)
        self._closing.add(key)
        try:
            return await self._close_hedge_inner(ent, lit, pair, owner, since_ms, lit_equity_before, maker, sid, timeout,
                                                 errors)
        finally:
            self._closing.discard(key)
            w = self._watchers.pop(key, None)
            if w:
                w.cancel()

    async def _close_hedge_inner(self, ent, lit, pair, owner, since_ms, lit_equity_before, maker, sid, timeout,
                                 errors) -> dict:
        lmk = mark = None
        with contextlib.suppress(Exception):
            http_timeout = aiohttp.ClientTimeout(total=8, connect=5)   # not `timeout`: that's the maker-close budget
            async with aiohttp.ClientSession(timeout=http_timeout) as s:
                lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
                if lmk:
                    mark = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)

        # Lighter directional PnL fallback: unrealised value just before we flatten (fee = 0 on RH).
        # Preferred is the exact equity delta computed after the flatten (below).
        l_pnl_unrealized = 0.0
        if lmk:
            with contextlib.suppress(Exception):
                lp = await lit.position(lmk.market_id)
                if lp:
                    l_pnl_unrealized = float(lp.get("unrealized_pnl", 0) or 0)

        # Maker-first close (normal closes): saves Entropy's taker fee. Anything it didn't close — timeout,
        # STOP, a leftover under the $10 floor — falls through to the market flatten below.
        if maker and lmk:
            await self._maker_close(ent, lit, pair, lmk, timeout=timeout, sid=sid)
            with contextlib.suppress(Exception):
                mark = await md.lighter_mark(await self._exec.http(), self._cfg.lighter_api_url, lmk.market_id) or mark

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

        # Let both venues settle, then read exact figures.
        e_pnl = e_fee = 0.0
        if since_ms is not None:
            await asyncio.sleep(1.5)
            with contextlib.suppress(Exception):
                e_pnl, e_fee = await ent.realized_since(pair.entropy, since_ms)

        # Exact Lighter PnL = equity change across the (flat→flat) hedge; fall back to the unrealised
        # read if we couldn't measure equity on both ends.
        l_pnl = l_pnl_unrealized
        if lit_equity_before is not None:
            after = await self._lit_equity(lit)
            if after is not None:
                l_pnl = after - lit_equity_before

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
    async def _build_plan(self, pair, margin, leverage, entropy_long):
        """Returns (plan, effective_leverage). Leverage is clamped to the lower of the two venues'
        per-coin maxima, and the notional = margin × that effective leverage."""
        try:
            timeout = aiohttp.ClientTimeout(total=8, connect=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                emk = (await md.entropy_markets(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
                lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
                if not emk or not lmk:
                    return None, leverage
                # Prefer the realtime WS mid; fall back to a REST mark if the feed is cold/stale.
                eprice = (self._feed.mid(pair.entropy) if self._feed else None)
                if not eprice:
                    eprice = (await md.entropy_marks(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
                lprice = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)
                if not eprice or not lprice:
                    return None, leverage
            eff = max(1, min(int(leverage), int(emk.max_leverage or leverage), int(lmk.max_leverage or leverage)))
            notional = float(margin) * eff
            plan = plan_hedge(pair, notional, entropy_long=entropy_long, entropy_price=eprice,
                              lighter_price=lprice, entropy_market=emk, lighter_market=lmk)
            return plan, eff
        except Exception:  # noqa: BLE001
            LOGGER.exception("session build_plan failed")
            return None, leverage

    async def _entropy_client(self, owner):
        cached = self._clients.get(owner, {}).get("entropy")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "entropy")
        if not c:
            return None
        client = await asyncio.to_thread(
            EntropyClient, self._cfg.hyperliquid_api_url, c.meta["wallet_address"], c.secret, self._cfg.entropy_dex)
        self._clients.setdefault(owner, {})["entropy"] = client
        return client

    async def _lighter_client(self, owner):
        cached = self._clients.get(owner, {}).get("lighter")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "lighter")
        if not c:
            return None
        # MUST build on the event loop: the Lighter SDK creates an aiohttp connector in its constructor
        # (asyncio.get_running_loop()); a worker thread has none. The constructor does no network.
        client = LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                               int(c.meta.get("api_key_index", 0)))
        self._clients.setdefault(owner, {})["lighter"] = client
        return client

    async def _drop_clients(self, owner) -> None:
        entry = self._clients.pop(owner, None)
        if entry and entry.get("lighter"):
            with contextlib.suppress(Exception):
                await entry["lighter"].close()

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

    async def _finish(self, sid: int, owner: int, cfg: dict | None = None) -> None:
        # Session over (STOP or duration): close whatever is still open the same way hedges normally
        # close — Entropy maker limit first, Lighter at market as it fills — then sweep EVERY venue at
        # market so nothing is ever left behind (maker timeout, positions from other coins, leftovers).
        with contextlib.suppress(Exception):
            ent = await self._entropy_client(owner)
            lit = await self._lighter_client(owner)
            try:
                if ent and lit:
                    open_pairs = []
                    for p in await self._guard(ent.positions(), default=[]) or []:
                        pair = next((x for x in PAIRS if x.entropy == p.get("coin")), None)
                        if pair and float(p.get("szi", 0) or 0) != 0:
                            open_pairs.append(pair)
                    if open_pairs:
                        t = float((cfg or {}).get("fill_timeout", 90))
                        await self._say(owner, "⏹ Закриваю відкриті хеджі ліміткою на Entropy "
                                               f"(до {self._fmt(t)}, далі — маркетом)…")
                        await asyncio.gather(*(self._guard(self._close_hedge(ent, lit, pair, owner=owner, maker=True,
                                                                             timeout=t))
                                               for pair in open_pairs))
                # 1) cancel all resting orders on both venues at once
                await asyncio.gather(
                    self._guard(ent.cancel_all()) if ent else self._noop(),
                    self._guard(lit.cancel_all()) if lit else self._noop(),
                )
                # 2) fetch Lighter market metadata + open positions on both venues (concurrently)
                lmarkets = {}
                if lit:
                    with contextlib.suppress(Exception):
                        timeout = aiohttp.ClientTimeout(total=8, connect=5)
                        async with aiohttp.ClientSession(timeout=timeout) as s:
                            byid = await md.lighter_markets(s, self._cfg.lighter_api_url)
                            lmarkets = {m.market_id: m for m in byid.values()}
                ent_pos, lit_pos = await asyncio.gather(
                    self._guard(ent.positions(), default=[]) if ent else self._noop([]),
                    self._guard(lit.open_positions(), default=[]) if lit else self._noop([]),
                )
                # 3) flatten every open position, all at once
                tasks = []
                for p in (ent_pos or []):
                    if float(p.get("szi", 0) or 0) != 0:
                        tasks.append(self._guard(ent.close_market(p["coin"])))
                for p in (lit_pos or []):
                    lmk = lmarkets.get(p["market_id"])
                    if lmk:
                        tasks.append(self._close_lighter(lit, lmk, p.get("entry")))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                await self._drop_clients(owner)   # session over — release cached clients
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
