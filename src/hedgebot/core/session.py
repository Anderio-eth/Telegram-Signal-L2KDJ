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
from .execution import FillResult, MakerExecutor, split_sizes
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
        # (owner, profile) -> {"entropy": client, "lighter": client}. Built once per session and reused
        # across cycles — rebuilding per cycle re-loaded Entropy market meta every time (seconds of
        # latency). Keyed by PROFILE too: profiles are different accounts behind different proxies, so
        # one owner's clients must never be handed to another of their profiles.
        self._clients: dict[tuple, dict] = {}
        self._exec = MakerExecutor(cfg)
        # (owner, profile, pair.key) -> watcher task for hedges opened by hand in the bot (sessions
        # guard their own hedges inline while holding).
        self._watchers: dict[tuple, asyncio.Task] = {}
        # (owner, profile, pair.key) being closed on purpose right now — the leg guard must not
        # "react" to a leg going flat because WE are closing it.
        self._closing: set[tuple] = set()

    async def start(self) -> None:
        """Resume RUNNING sessions after a restart — but only ONE per owner. Earlier double-taps can
        leave several RUNNING rows for the same user; resuming them all would recreate the margin
        fight, so keep the newest and retire the rest."""
        seen: set[tuple] = set()
        for s in await self._store.running_sessions():
            owner = (s.get("owner_id"), s.get("profile") or "")
            if owner in seen:
                with contextlib.suppress(Exception):
                    await self._store.set_session_status(s["id"], "STOPPED")  # retire the duplicate
                continue
            seen.add(owner)
            self._spawn(s, resumed=True)
        LOGGER.info("session engine started; resumed %d session(s)", len(self._tasks))

    async def start_session(self, owner_id: int, profile: str, config: dict) -> int:
        # Guard against stacking (double-tap / race): one running session per PROFILE. Two sessions on
        # the same profile fight over the same margin and flood "not enough margin"; two sessions on
        # different profiles are different accounts entirely and are exactly what this supports.
        existing = await self._store.active_session(owner_id, profile)
        if existing and existing.get("status") == "RUNNING":
            return existing["id"]
        sid = await self._store.create_session(owner_id, profile, config)
        self._spawn({"id": sid, "owner_id": owner_id, "profile": profile, "config": config})
        return sid

    async def stop_session(self, session_id: int) -> None:
        # The running task polls its status and winds down (closes the open hedge) when it sees this.
        await self._store.set_session_status(session_id, "STOPPING")

    def _spawn(self, s: dict, resumed: bool = False) -> None:
        if s["id"] in self._tasks:
            return
        self._tasks[s["id"]] = asyncio.create_task(self._run(s, resumed), name=f"session-{s['id']}")

    async def _say(self, owner_id: int, text: str, profile: str | None = None) -> None:
        if self._notify:
            if profile:
                text = f"<b>[{html.escape(profile)}]</b> {text}"
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

    async def _hedge_alert(self, cfg: dict, owner_id: int, text: str, profile: str | None = None) -> None:
        """Per-hedge open/close chatter. Off by default (stats go to the sheet instead of the chat);
        the user can flip `notify_each` on in the session config if they want the pings back."""
        if cfg.get("notify_each"):
            await self._say(owner_id, text, profile)

    async def _stat(self, owner_id: int, sid: int, profile: str, **row) -> None:
        if self._sheets:
            with contextlib.suppress(Exception):
                await self._sheets.append_hedge(owner_id, sid, profile, row)

    # ── the loop ─────────────────────────────────────────────────────────────────────────────────
    async def _run(self, s: dict, resumed: bool = False) -> None:
        sid, owner, cfg = s["id"], s["owner_id"], s["config"]
        profile = s.get("profile") or ""
        ends_at = time.time() + float(cfg.get("duration", 86400))
        mode = "DRY-RUN" if cfg.get("dry_run", True) else "LIVE"
        if self._sheets:
            with contextlib.suppress(Exception):
                await self._sheets.ensure_sheet(owner, sid, profile)
        verb = "🔄 Сесію відновлено після перезапуску" if resumed else "▶️ Сесію запущено"
        await self._say(owner, f"{verb} ({mode}). Триватиме ~{self._fmt(cfg.get('duration',86400))}.", profile)
        try:
            if resumed and not cfg.get("dry_run", True):
                await self._adopt_open_hedges(owner, profile, sid, cfg)   # finish hedges left by the restart
            while time.time() < ends_at:
                if await self._stopping(sid):
                    break
                try:
                    await self._one_cycle(owner, profile, sid, cfg)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 — one bad cycle must not end the whole session
                    LOGGER.exception("cycle failed in session %s", sid)
                    await self._say(owner, "⚠️ Цикл впав з помилкою — сесія триває, пробую далі.", profile)
                    await self._drop_clients(owner, profile)   # connector may be dead — rebuild next cycle
                    await self._interruptible_sleep(sid, 5)
                if await self._stopping(sid):
                    break
                await self._interruptible_sleep(sid, 5)   # small gap so a failing cycle can't tight-loop/spam
                if cfg.get("pause_on"):
                    pause = random.uniform(cfg.get("pause_min", 300), cfg.get("pause_max", 1800))
                    await self._hedge_alert(cfg, owner, f"⏸ Пауза {self._fmt(pause)} до наступного хеджа.", profile)
                    await self._interruptible_sleep(sid, pause)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a session must not crash the whole engine
            LOGGER.exception("session %s crashed", sid)
        finally:
            await self._finish(sid, owner, profile, cfg)
            self._tasks.pop(sid, None)

    async def _one_cycle(self, owner: int, profile: str, sid: int, cfg: dict) -> None:
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
            hid = await self._store.new_hedge(owner, profile, sid, pair.key, notional, side, "OPEN",
                                              {"mode": "dry", "hold": hold, "close_at": close_at.isoformat()})
            await self._hedge_alert(cfg, owner, f"🧪 [dry] Відкрив {pair.label} Entropy {side} ${notional:g}. "
                                                f"Закрию через {self._fmt(hold)}.", profile)
            await self._interruptible_sleep(sid, hold)
            await self._store.update_hedge(hid, status="CLOSED", realized_pnl=0.0, fees=0.0,
                                           entropy_vol=notional * 2, lighter_vol=notional * 2)
            await self._hedge_alert(cfg, owner, f"🧪 [dry] Закрив {pair.label}. (симуляція)", profile)
            await self._stat(owner, sid, profile, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED (dry)",
                             pnl=0.0, fees=0.0, lighter_vol=notional * 2, entropy_vol=notional * 2)
            return

        # ── live ──────────────────────────────────────────────────────────────────────────────────
        # Live: the side comes from the prices (long where it's cheaper) — see plan_hedge.
        await self._live_cycle(owner, profile, sid, pair, leverage, margin, None, hold, cfg)

    async def _live_cycle(self, owner, profile, sid, pair, leverage, margin, entropy_long, hold, cfg) -> None:
        # Split mode: build the position as several maker orders instead of one, to keep each Lighter
        # hedge small enough not to move the book. Off by default -- classic is one order.
        parts = int(cfg.get("split_parts", 1)) if cfg.get("split_on") else 1
        gap_s = float(cfg.get("split_gap_s", 0)) if cfg.get("split_on") else 0.0
        opened_at = datetime.now(timezone.utc)
        opened_ms = int(opened_at.timestamp() * 1000)
        # Clamp leverage to what BOTH venues allow for this coin (Lighter caps OAI/ANTH at 5x); the
        # notional follows the effective leverage so the margin used stays the user's chosen amount.
        plan, leverage = await self._build_plan(pair, margin, leverage, entropy_long)
        if plan is None or not plan.ok:
            why = plan.errors[0] if (plan and plan.errors) else "не вдалось скласти план"
            await self._say(owner, f"⚠️ {pair.label}: {html.escape(str(why))} — пропускаю цикл.", profile)
            return
        notional = plan.notional_usd
        entropy_long = plan.entropy.is_buy
        side = "LONG" if entropy_long else "SHORT"
        if int(leverage) < int(cfg["leverage"]):
            await self._say(owner, f"ℹ️ {pair.label}: плече знижено до {leverage}x (макс для цієї монети).", profile)
        # started_at, NOT opened_at: the maker order can work for a long time (until_complete has no
        # deadline), and the hold clock only starts once the whole size is on. Writing a close_at here
        # produced a countdown that had not started yet and could never come true.
        detail = {"hold": hold, "started_at": opened_at.isoformat(),
                  "entropy_px": plan.entropy_price, "lighter_px": plan.lighter_price}
        hid = await self._store.new_hedge(owner, profile, sid, pair.key, notional, side, "OPENING",
                                          detail)
        ent = await self._entropy_client(owner, profile)
        lit = await self._lighter_client(owner, profile)
        if not ent or not lit:
            await self._store.update_hedge(hid, status="FAILED")
            await self._say(owner, "⚠️ Немає ключів — зупиняю цикл.", profile)
            return
        # Never stack a new hedge on top of leftovers from a previous one (a leg that didn't close,
        # a resting order): flatten whatever is still open first.
        if not await self._ensure_flat(owner, profile, ent, lit, pair):
            await self._store.update_hedge(hid, status="FAILED")
            await self._say(owner, f"⚠️ {pair.label}: попередня позиція ще відкрита — пропускаю цикл.", profile)
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
                                       f"{html.escape(str(lev_err))}", profile)
            # Maker-first: Entropy post-only limit, Lighter at market as it fills (see core/execution).
            # until_complete: a hedge is either opened at the size the user asked for, or not opened.
            # Sessions never move on to the next cycle with a half-filled leg.
            fr = await self.open_maker_first(ent, lit, plan, timeout=cfg.get("fill_timeout", 90), sid=sid,
                                             until_complete=True, owner=owner, pair=pair,
                                             reprice_s=cfg.get("reprice_s"), profile=profile,
                                             parts=parts, gap_s=gap_s)
            if fr.error or fr.unhedged > 0:
                # A leg failed, or a partial fill is too small to hedge on Lighter — flatten both.
                res = await self._cancel_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                               lit_equity_before=lit_equity_before, profile=profile)
                await self._store.update_hedge(hid, status="FAILED", realized_pnl=res["pnl"], fees=res["fees"])
                why = fr.error or "частковий філ менший за мінімум Lighter"
                await self._say(owner, f"⛔ {pair.label}: {html.escape(str(why))} — закрив обидві ноги.", profile)
                await self._stat(owner, sid, profile, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="FAILED", status="FAILED",
                                 pnl=res["pnl"], fees=res["fees"], lighter_vol=0.0, entropy_vol=0.0)
                return
            if fr.e_filled <= 0:
                # The limit never filled: no position anywhere, nothing to unwind. Skip this cycle.
                await self._store.update_hedge(hid, status="CANCELLED")
                await self._hedge_alert(cfg, owner, f"⏭ {pair.label}: лімітка на Entropy не заповнилась за "
                                                    f"{self._fmt(cfg.get('fill_timeout', 90))} — пропускаю цикл.",
                                        profile)
                await self._stat(owner, sid, profile, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
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
                                       f"${notional:g} з ${planned:g}/ногу (обидві ноги захеджовані).", profile)
            # The position exists only now, so this is the time worth recording -- and the time the
            # hold is measured from. `opened_ms` deliberately stays at the cycle start: the opening
            # fills happened before this point and the PnL window must still cover them.
            opened_at = datetime.now(timezone.utc)
            detail = {**detail, "close_at": (opened_at + timedelta(seconds=hold)).isoformat()}
            await self._store.update_hedge(hid, status="OPEN", opened_at=opened_at, detail=detail)
            l_side = "SHORT" if entropy_long else "LONG"
            stops = await self.place_stops(ent, lit, pair, owner=owner, profile=profile)
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} відкрито (Entropy {side} ✓ / Lighter {l_side} ✓). "
                                                f"{self.stops_text(stops)}Закрию через {self._fmt(hold)}.", profile)
            hit = await self.guard_legs(owner, ent, lit, pair, profile=profile, seconds=hold, sid=sid)
            if hit:
                # A stop (or a liquidation) took one leg out; guard_legs already flattened the other.
                res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                              lit_equity_before=lit_equity_before, profile=profile)
                await self._store.update_hedge(hid, status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"],
                                               entropy_vol=notional * 2, lighter_vol=notional * 2)
                await self._stat(owner, sid, profile, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                                 coin=pair.label, side=side, open_status="OK", status=f"STOP ({hit})",
                                 pnl=res["pnl"], fees=res["fees"], lighter_vol=notional * 2, entropy_vol=notional * 2)
                return
            res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=opened_ms,
                                          lit_equity_before=lit_equity_before, maker=True, sid=sid,
                                          timeout=cfg.get("fill_timeout", 90), reprice_s=cfg.get("reprice_s"),
                                          profile=profile, parts=parts, gap_s=gap_s)
            await self._store.update_hedge(hid, status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"],
                                           entropy_vol=notional * 2, lighter_vol=notional * 2)
            lit_mark = "✓" if not res.get("errors") else "✗"
            await self._hedge_alert(cfg, owner, f"✅ {pair.label} закрито (Entropy ✓ / Lighter {lit_mark}). "
                                                f"PnL ≈ ${res['pnl']:g}, комісія ${res['fees']:g}.", profile)
            await self._stat(owner, sid, profile, opened_at=opened_at, closed_at=datetime.now(timezone.utc),
                             coin=pair.label, side=side, open_status="OK", status="CLOSED",
                             pnl=res["pnl"], fees=res["fees"], lighter_vol=notional * 2, entropy_vol=notional * 2)
        finally:
            pass  # clients are cached for the whole session; closed in _finish

    async def _ensure_flat(self, owner, profile, ent, lit, pair) -> bool:
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
        await self._say(owner, f"🧹 {pair.label}: лишилась стара позиція ({', '.join(leftovers)}) — "
                               f"закриваю перед новим хеджем.", profile)
        await self._close_hedge(ent, lit, pair, owner=owner, profile=profile)
        with contextlib.suppress(Exception):
            if await self._exec.entropy_szi(ent, pair.entropy):
                return False
            if lmk:
                lp = await lit.position(lmk.market_id)
                if lp and lp.get("abs"):
                    return False
        return True

    async def _adopt_open_hedges(self, owner, profile, sid, cfg) -> None:
        """After a restart, finish hedges that were left OPEN — wait out the rest of their planned hold
        (from detail.close_at) then close them, instead of abandoning the position and opening a new
        one on top (which fought for margin)."""
        hedges = await self._store.session_hedges(sid)
        openh = [h for h in hedges if h["status"] in ("OPEN", "OPENING")]
        if not openh:
            return
        ent = await self._entropy_client(owner, profile)
        lit = await self._lighter_client(owner, profile)
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
                                   f"{('закрию за ' + self._fmt(remaining)) if remaining > 0 else 'закриваю зараз'}.",
                            profile)
            # Fills from the opening happen BEFORE opened_at now, so the PnL window starts at the
            # cycle's own start when we have it; opened_at is only the fallback for older rows.
            since = None
            with contextlib.suppress(Exception):
                if det.get("started_at"):
                    since = datetime.fromisoformat(det["started_at"])
            since = since or h.get("opened_at")
            since_ms = int(since.timestamp() * 1000) if since else None
            if remaining > 0:
                await self.place_stops(ent, lit, pair, owner=owner, profile=profile)
                hit = await self.guard_legs(owner, ent, lit, pair, profile=profile, seconds=remaining, sid=sid)
                if hit:
                    res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms, profile=profile)
                    await self._store.update_hedge(h["id"], status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"])
                    continue
            res = await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms, maker=True, sid=sid,
                                          timeout=cfg.get("fill_timeout", 90), reprice_s=cfg.get("reprice_s"),
                                          profile=profile)
            await self._store.update_hedge(h["id"], status="CLOSED", realized_pnl=res["pnl"], fees=res["fees"])
            vol = float(h.get("notional_usd", 0) or 0) * 2
            await self._stat(owner, sid, profile, opened_at=h.get("opened_at"), closed_at=datetime.now(timezone.utc),
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

    async def place_stops(self, ent, lit, pair, owner=None, profile: str = "") -> dict:
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
            await self._say(owner, f"⚠️ {pair.label}: стоп не виставлено — "
                                   + "; ".join(html.escape(f) for f in failed), profile)
        return out

    @staticmethod
    def stops_text(stops: dict) -> str:
        parts = []
        for name, key in (("Entropy", "entropy"), ("Lighter", "lighter")):
            v = stops.get(key) or {}
            if v.get("stop"):
                parts.append(f"{name} стоп {v['stop']:g} (ліквідація {v['liq']:g})")
        return ("🛡 " + ", ".join(parts) + ". ") if parts else ""

    async def guard_legs(self, owner, ent, lit, pair, *, profile: str = "", seconds: float,
                         sid: int | None = None, poll: float = 5.0) -> str | None:
        """Hold for `seconds` (or until STOP) while watching both legs. If one leg disappears — its stop
        fired or it was liquidated — the other is closed at market at once, so the hedge never sits as
        naked delta. Returns "entropy"/"lighter" (the leg that went first) or None if the hold ended
        normally. Reads that fail are skipped, never mistaken for a flat position."""
        key = (owner, profile, pair.key)
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
                            await self._say(owner, f"🛑 {pair.label}: нога на {'Entropy' if e_gone else 'Lighter'} "
                                                   f"закрилась (стоп або ліквідація) — закрив {other} "
                                                   f"маркетом.{tail}", profile)
                            return first
            await asyncio.sleep(min(poll, max(0.2, end - time.time())))
        return None

    def watch_manual(self, owner, ent, lit, pair, profile: str = "") -> None:
        """Guard a hedge opened by hand in the bot until it is closed (no hold timer)."""
        key = (owner, profile, pair.key)
        old = self._watchers.pop(key, None)
        if old:
            old.cancel()

        async def run():
            with contextlib.suppress(asyncio.CancelledError):
                await self.guard_legs(owner, ent, lit, pair, profile=profile, seconds=30 * 86400)
            self._watchers.pop(key, None)

        self._watchers[key] = asyncio.create_task(run(), name=f"guard-{owner}-{profile}-{pair.key}")

    async def open_maker_first(self, ent, lit, plan, *, timeout: float, sid: int | None = None,
                               until_complete: bool = False, owner=None, pair=None,
                               reprice_s: float | None = None, profile: str = "",
                               parts: int = 1, gap_s: float = 0.0):
        """Open a planned hedge maker-first and verify the Lighter leg actually landed.

        With `parts` > 1 the size is opened as that many maker orders, `gap_s` apart. Each chunk is
        mirrored on Lighter the moment IT fills, so the taker side reaches the book in slices instead
        of one lump — which is the whole point: a single large market order walks the book and pays
        for every level it eats.

        A chunk that fails or is stopped ends the ladder and returns what is on so far; the caller
        flattens both venues, exactly as it does for a failed single open. Returns a combined
        core.execution.FillResult."""
        e, l = plan.entropy, plan.lighter
        chunks = split_sizes(e.size, l.size, plan.entropy_market, plan.lighter_market,
                             plan.entropy_price, plan.lighter_price, parts)
        if owner and parts > 1 and len(chunks) < parts:
            await self._say(owner, f"ℹ️ {pair.label if pair else ''}: розбивку зменшено до "
                                   f"{len(chunks)} част. — менші за мінімум біржі.", profile)
        total = FillResult()
        done = 0
        for i, (e_sz, l_sz) in enumerate(chunks, start=1):
            if i > 1:
                if gap_s > 0:
                    if sid is not None:
                        await self._interruptible_sleep(sid, gap_s)
                    else:
                        await asyncio.sleep(gap_s)
                if sid is not None and await self._stopping(sid):
                    break
            fr = await self._maker_pass(ent, lit, plan, e_sz, l_sz, timeout=timeout, sid=sid,
                                        until_complete=until_complete, owner=owner, pair=pair,
                                        reprice_s=reprice_s, profile=profile,
                                        part=(i, len(chunks)) if len(chunks) > 1 else None)
            total.e_filled += fr.e_filled
            total.l_done += fr.l_done
            total.unhedged = fr.unhedged
            total.notes += fr.notes
            if fr.error or fr.unhedged > 0 or not fr.complete:
                total.error = fr.error
                break
            done = i
        total.complete = done == len(chunks)
        return total

    async def _maker_pass(self, ent, lit, plan, e_size: float, l_size: float, *, timeout: float,
                          sid, until_complete: bool, owner, pair, reprice_s, profile: str,
                          part: tuple[int, int] | None):
        """One maker order for `e_size`, hedged on Lighter as it fills."""
        e, l = plan.entropy, plan.lighter
        tag = f"{pair.label if pair else ''}{f' (частина {part[0]}/{part[1]})' if part else ''}"
        stopping = (lambda: self._stopping(sid)) if sid is not None else None

        async def ping(filled: float, target: float) -> None:
            if owner:
                await self._say(owner, f"⏳ {tag}: лімітка на Entropy заповнена на "
                                       f"{filled / target * 100:.0f}% ({filled:g} з {target:g}) — "
                                       f"дотягую до повного розміру.", profile)

        # Measured as a DELTA: with a ladder the Lighter leg is already non-zero from earlier chunks,
        # so comparing against the absolute position would read every previous chunk as this one's.
        before = await self._lighter_abs(lit, l.market_index)
        fr = await self._exec.fill(ent, lit, plan.entropy_market, plan.lighter_market,
                                   e_is_buy=e.is_buy, e_target=e_size, l_is_ask=l.is_ask,
                                   l_target=l_size, closing=False, timeout=timeout,
                                   stopping=stopping, until_complete=until_complete,
                                   on_progress=ping, reprice_s=reprice_s)
        if fr.error or fr.l_done <= 0:
            return fr
        # IOC can fill short on a thin book — top the Lighter leg up once if it's missing a real chunk.
        await asyncio.sleep(0.5)
        after = await self._lighter_abs(lit, l.market_index)
        short = round(fr.l_done - (after - before), plan.lighter_market.size_decimals)
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

    @staticmethod
    async def _lighter_abs(lit, market_index: int) -> float:
        try:
            lp = await lit.position(market_index)
            return float(lp.get("abs", 0) or 0) if lp else 0.0
        except Exception:  # noqa: BLE001 — treated as "unchanged", the top-up check just skips
            return 0.0

    async def _maker_close(self, ent, lit, pair, lmk, *, timeout: float, sid: int | None,
                           reprice_s: float | None = None, parts: int = 1, gap_s: float = 0.0) -> None:
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
        # Same ladder on the way out: a big reduce-only market order walks Lighter's book just as
        # hard as an entry does.
        lpx = await self._exec.lighter_mid(lmk.market_id) or 0.0
        chunks = split_sizes(abs(szi), l_abs, emk, lmk, lpx, lpx, parts)
        # A hard ceiling on the whole ladder: the caller's market pass flattens whatever is left, so
        # overrunning costs nothing but time — and a slow book must not leave legs open for minutes.
        budget = time.monotonic() + max(60.0, len(chunks) * (gap_s + 30.0))
        # No `stopping` here: STOP is usually what asked for this close, so it must not abort it.
        # Each order gets its timeout, then the caller's market pass finishes the job.
        for i, (e_sz, l_sz) in enumerate(chunks, start=1):
            if i > 1:
                if gap_s > 0:
                    await asyncio.sleep(gap_s)
                if time.monotonic() > budget:
                    LOGGER.info("maker close %s: ladder out of time at %d/%d — market pass takes the rest",
                                pair.label, i, len(chunks))
                    return
            fr = await self._exec.fill(ent, lit, emk, lmk, e_is_buy=szi < 0, e_target=e_sz,
                                       l_is_ask=l_is_ask, l_target=l_sz, closing=True,
                                       timeout=timeout, stopping=None, reprice_s=reprice_s)
            if fr.error:
                LOGGER.warning("maker close %s: %s — falling back to market", pair.label, fr.error)
                return
            if not fr.complete:
                return              # timed out mid-chunk; the market pass finishes it

    async def _cancel_hedge(self, ent, lit, pair, owner=None, since_ms=None, lit_equity_before=None,
                            profile: str = "") -> dict:
        # Same flatten path as a normal close: flatten whatever filled on both venues and pull the
        # unfilled maker legs, so a one-sided fill can never be left as naked delta.
        return await self._close_hedge(ent, lit, pair, owner=owner, since_ms=since_ms,
                                       lit_equity_before=lit_equity_before, profile=profile)

    async def _close_hedge(self, ent, lit, pair, owner=None, since_ms=None, lit_equity_before=None,
                           maker=False, sid=None, timeout: float = 60, reprice_s: float | None = None,
                           profile: str = "", parts: int = 1, gap_s: float = 0.0) -> dict:
        """Flatten BOTH legs simultaneously and return {pnl, fees, errors}.

        PnL/fees: the Entropy leg's realized PnL and fee are read from the exchange's own fills since
        `since_ms` (accurate, includes the close); the Lighter leg's directional PnL is its unrealised
        PnL read just before flattening (RH-Lighter fee is 0). A failed close is reported, not swallowed
        — a leg left open ties up margin and blocks the next hedge."""
        errors: list[str] = []
        key = (owner, profile, pair.key)
        self._closing.add(key)
        try:
            return await self._close_hedge_inner(ent, lit, pair, owner, since_ms, lit_equity_before, maker, sid, timeout,
                                                 errors, reprice_s, profile, parts, gap_s)
        finally:
            self._closing.discard(key)
            w = self._watchers.pop(key, None)
            if w:
                w.cancel()

    async def _close_hedge_inner(self, ent, lit, pair, owner, since_ms, lit_equity_before, maker, sid, timeout,
                                 errors, reprice_s=None, profile="", parts=1, gap_s=0.0) -> dict:
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
            await self._maker_close(ent, lit, pair, lmk, timeout=timeout, sid=sid, reprice_s=reprice_s,
                                    parts=parts, gap_s=gap_s)
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
            await self._say(owner, f"⚠️ {pair.label}: " + "; ".join(html.escape(str(e)) for e in errors), profile)
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

    async def _entropy_client(self, owner, profile: str = ""):
        cached = self._clients.get((owner, profile), {}).get("entropy")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "entropy", profile)
        if not c:
            return None
        proxy = await self._store.get_proxy(owner, profile)
        client = await asyncio.to_thread(
            EntropyClient, self._cfg.hyperliquid_api_url, c.meta["wallet_address"], c.secret,
            self._cfg.entropy_dex, proxy)
        self._clients.setdefault((owner, profile), {})["entropy"] = client
        return client

    async def _lighter_client(self, owner, profile: str = ""):
        cached = self._clients.get((owner, profile), {}).get("lighter")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "lighter", profile)
        if not c:
            return None
        proxy = await self._store.get_proxy(owner, profile)
        # MUST build on the event loop: the Lighter SDK creates an aiohttp connector in its constructor
        # (asyncio.get_running_loop()); a worker thread has none. The constructor does no network.
        client = LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                               int(c.meta.get("api_key_index", 0)), proxy)
        self._clients.setdefault((owner, profile), {})["lighter"] = client
        return client

    async def _drop_clients(self, owner, profile: str = "") -> None:
        entry = self._clients.pop((owner, profile), None)
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

    async def _finish(self, sid: int, owner: int, profile: str = "", cfg: dict | None = None) -> None:
        # Session over (STOP or duration): close whatever is still open the same way hedges normally
        # close — Entropy maker limit first, Lighter at market as it fills — then sweep EVERY venue at
        # market so nothing is ever left behind (maker timeout, positions from other coins, leftovers).
        with contextlib.suppress(Exception):
            ent = await self._entropy_client(owner, profile)
            lit = await self._lighter_client(owner, profile)
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
                                               f"(до {self._fmt(t)}, далі — маркетом)…", profile)
                        await asyncio.gather(*(self._guard(self._close_hedge(ent, lit, pair, owner=owner, maker=True,
                                                                             timeout=t,
                                                                             reprice_s=(cfg or {}).get("reprice_s"),
                                                                             profile=profile))
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
                await self._drop_clients(owner, profile)   # session over — release cached clients
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
                await self._sheets.append_totals(owner, sid, profile, rows)
        await self._say(owner, await self._summary(sid), profile)

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
