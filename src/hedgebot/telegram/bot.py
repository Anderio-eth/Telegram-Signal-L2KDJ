"""The Telegram control surface: store keys, open a hedge, close it, see status.

Inline-menu driven (like the copy bot). Multi-step inputs (key fields, notional) run through one
ConversationHandler that reads what it's waiting for from user_data. Exchange clients are built from
stored credentials at the moment of a trade and torn down after, so nothing holds a live signer
between actions.

Deliberately thin on polish — the plan is to run it and fix ergonomics against real use.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import re
import time
from datetime import datetime, timezone

import aiohttp
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters,
)

from ..config import Config
from ..core.hedge import gap_text, plan_hedge
from ..core.sheets import ACCOUNT_SCOPE
from ..db.store import Store
from ..exchanges import market_data as md
from ..exchanges.hyperliquid_entropy import EntropyClient
from ..exchanges.lighter_client import LighterClient
from ..pairs import PAIRS, get as get_pair

LOGGER = logging.getLogger(__name__)
ASK = 1  # single conversation state for all free-text prompts
NL = "\n"
MENU_BTN = "☰ Меню"  # the one persistent reply-keyboard button, always at the bottom of the chat


# Every callback that opens the "send me a value" conversation, and the entry-point pattern BUILT
# from it. These must not be maintained separately: a button whose callback is missing from the
# pattern falls through to the plain router, which has no case for it, so pressing it does nothing
# at all -- silently, with no error anywhere. Add the callback here and the button works.
INPUT_CALLBACKS = (
    "key:lighter", "key:entropy", "key:gsheets", "cfg:margin",
    "sess:hold", "sess:pause", "sess:margin", "sess:reprice", "sess:split",
    "prof:add", "prof:proxy",
)
# Dynamic callbacks: a row index is appended (prof:ren:0). Same list, same rules as above.
INPUT_PREFIXES = ("prof:ren:",)
INPUT_PATTERN = "^(?:" + "|".join(
    [re.escape(c) for c in INPUT_CALLBACKS] + [re.escape(x) + r"\d+" for x in INPUT_PREFIXES]
) + ")$"
MAX_RANGE_MIN = 1440           # 24h -- the longest hold/pause the session config accepts
SESS_INPUTS = tuple(c for c in INPUT_CALLBACKS if c.startswith("sess:"))
SESS_INPUT_FLOWS = tuple(c.replace(":", "_") for c in SESS_INPUTS)

SESS_DEFAULT = {
    "coins": [PAIRS[0].key], "leverage": 3, "margin": 5.0,
    "hold_min": 1800, "hold_max": 7200, "pause_on": False, "pause_min": 300, "pause_max": 1800,
    "duration": 86400, "reprice_s": 8, "dry_run": True, "notify_each": False,
    # Split mode: набирати позицію кількома лімітками замість однієї, щоб кожен хедж на Lighter був
    # дрібним і не рухав стакан. Вимкнено — класика лишається поведінкою за замовчуванням.
    "split_on": False, "split_parts": 10, "split_gap_s": 7,
}


# Per-coin maxima (both venues): OpenAI / Anthropic 6x on io (5x on RH-Lighter), SanDisk 10x on both.
# Anything above a coin's max is lowered to it automatically — in sessions and manual opens alike.
LEV_HINT = ("⚙️ <b>Плече</b>\n"
            "Макс: SanDisk 10x · OpenAI / Anthropic 5x (ліміт Lighter).\n"
            "Вище за максимум монети — бот сам знизить до максимуму.")


class HedgeBot:
    BAL_TTL = 60.0     # seconds a cached balance line stays fresh before the menu refetches
    CLIENT_TTL = 600.0  # reuse a built exchange client for this long before rebuilding it

    def __init__(self, cfg: Config, store: Store, engine=None, sheets=None, feed=None) -> None:
        self._cfg = cfg
        self._store = store
        self._engine = engine
        self._sheets = sheets
        self._feed = feed
        # owner_id -> {"ts": monotonic, "at": "HH:MM:SS UTC", "text": str}. Keeps the main menu from
        # hitting both exchanges on every navigation; the 🔄 button forces a refetch.
        self._bal_cache: dict[tuple, dict] = {}
        # owner_id -> {"entropy": client|None, "lighter": client|None, "ts": monotonic}. Building a
        # client is a network round-trip (Entropy loads meta); reusing it is what makes the menu snappy.
        self._client_cache: dict[tuple, dict] = {}
        # chat_id -> current anchor message id. Single source of truth for "the menu message", shared
        # by handlers and by the notification re-anchor (push_menu), so they never fight.
        self._anchor: dict[int, int] = {}

    async def _sess_draft(self, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> dict:
        """The session-config draft belonging to the CURRENT profile.

        chat_data is per chat, not per profile. Holding one draft there meant that after switching
        profiles the screen still showed the previous account's settings — and the next save wrote
        them onto the new profile, silently overwriting it. The draft carries the profile it was
        loaded for and is reloaded the moment that changes."""
        profile = await self._prof(owner)
        if "sess" not in ctx.chat_data or ctx.chat_data.get("sess_profile") != profile:
            saved = (await self._store.load_settings(owner, profile)).get("session_cfg") or {}
            ctx.chat_data["sess"] = {**SESS_DEFAULT, **saved,
                                     "coins": list(saved.get("coins") or SESS_DEFAULT["coins"])}
            ctx.chat_data["sess_profile"] = profile
        return ctx.chat_data["sess"]

    async def _open_draft(self, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> dict:
        """The manual-open draft for the current profile — same reasoning as _sess_draft."""
        profile = await self._prof(owner)
        if "draft" not in ctx.chat_data or ctx.chat_data.get("draft_profile") != profile:
            saved = (await self._store.load_settings(owner, profile)).get("draft") or {}
            if not saved.get("side_manual"):
                saved.pop("entropy_long", None)   # drafts saved before auto-side: go auto
            ctx.chat_data["draft"] = {"pair": PAIRS[0].key, "leverage": 3, "margin": 5.0,
                                      "entropy_long": None, **saved}
            ctx.chat_data["draft_profile"] = profile
        return ctx.chat_data["draft"]

    async def _prof(self, owner: int) -> str:
        """The profile every screen is acting on. Resolved inside the helpers instead of being
        threaded through each handler, so a screen that forgets to ask still acts on the account the
        user is looking at rather than a stale one."""
        return await self._store.selected_profile(owner)

    async def _drop_clients(self, owner: int, profile: str | None = None) -> None:
        """Close and forget cached clients for one profile, or for all of the owner's profiles when
        none is named (a key change, a profile switch, or after an error)."""
        keys = [(owner, profile)] if profile is not None else [
            k for k in list(self._client_cache) if k[0] == owner]
        for k in keys:
            entry = self._client_cache.pop(k, None)
            self._bal_cache.pop(k, None)
            if entry and entry.get("lighter"):
                with contextlib.suppress(Exception):
                    await entry["lighter"].close()

    # ── wiring ───────────────────────────────────────────────────────────────────────────────────
    def register(self, app: Application) -> None:
        app.add_handler(CommandHandler("start", self._start))
        app.add_handler(ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_input, pattern=INPUT_PATTERN)],
            states={ASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_input)]},
            # ANY navigation button (Назад/Меню) ENDS the input flow and routes normally — otherwise a
            # prompt whose Назад points to open/sess/stats left the conversation stuck in ASK, and the
            # margin/hold/pause buttons then did nothing on the next tap (entry can't re-fire).
            fallbacks=[CallbackQueryHandler(self._conv_exit, pattern=r"^[a-z]")],
            per_message=False,
        ))
        app.add_handler(CallbackQueryHandler(self._router))
        # Catch-all for text typed OUTSIDE an active step flow (e.g. after a restart wiped the
        # in-memory conversation state): delete it so nothing lingers, and re-show the menu. When a
        # flow IS active the ConversationHandler above consumes the text first, so this won't fire.
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._stray_text))

    async def _stray_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._guard(update) is None:
            return
        text = (update.message.text or "").strip()
        with contextlib.suppress(Exception):
            await update.message.delete()
        if text == MENU_BTN:
            await self._reanchor_menu(update, ctx, update.effective_user.id)
            return
        await self._refresh_menu(update, ctx, update.effective_user.id,
                                 "⌨️ Керуй кнопками. Щоб відкрити хедж — тисни «📈 Відкрити хедж».")

    @staticmethod
    def _reply_kb() -> ReplyKeyboardMarkup:
        # One button that lives at the bottom of the chat for good, so the menu is always one tap away
        # no matter how far the anchor has scrolled up.
        return ReplyKeyboardMarkup([[KeyboardButton(MENU_BTN)]], resize_keyboard=True, is_persistent=True)

    async def _reanchor_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> None:
        """Drop the old anchor and post a fresh main menu at the bottom of the chat."""
        old = self._anchor.get(update.effective_chat.id)
        if old:
            with contextlib.suppress(Exception):
                await ctx.bot.delete_message(update.effective_chat.id, old)
        m = await update.effective_chat.send_message(**await self._main_menu(owner))
        self._anchor[update.effective_chat.id] = m.message_id

    def _guard(self, update: Update) -> int | None:
        uid = update.effective_user.id if update.effective_user else None
        if self._cfg.allowed_user_ids and uid not in self._cfg.allowed_user_ids:
            return None
        return uid

    # ── menus ────────────────────────────────────────────────────────────────────────────────────
    async def _start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._guard(update) is None:
            uid = update.effective_user.id if update.effective_user else "?"
            await update.message.reply_text(
                f"⛔ Доступ обмежено.\nТвій Telegram ID: <code>{uid}</code>\n"
                "Надішли його власнику бота, щоб він додав тебе в список дозволених.",
                parse_mode=ParseMode.HTML)
            return
        # Establish the persistent bottom keyboard, then delete its carrier — the keyboard stays put
        # even without the message, so nothing lingers in the chat.
        with contextlib.suppress(Exception):
            carrier = await update.message.reply_text("⌨️", reply_markup=self._reply_kb())
            await carrier.delete()
        m = await update.message.reply_text(**await self._main_menu(update.effective_user.id))
        self._anchor[update.effective_chat.id] = m.message_id   # the single message we keep and edit
        with contextlib.suppress(Exception):
            await update.message.delete()                # drop the /start command too

    _ENTRY_RE = re.compile(INPUT_PATTERN)   # same list as the entry points: see INPUT_CALLBACKS

    async def _conv_exit(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        """Fallback for any button pressed while awaiting text input. If it's another input button,
        switch to it (stay in the flow); otherwise end the flow and route the navigation normally —
        so a Назад never leaves the conversation stuck (which had killed the margin/hold/pause buttons)."""
        data = (update.callback_query.data or "") if update.callback_query else ""
        if self._ENTRY_RE.match(data):
            return await self._begin_input(update, ctx)     # re-enter with the new prompt
        await self._router(update, ctx)                      # do the navigation
        return ConversationHandler.END

    async def _menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(**await self._main_menu(update.effective_user.id))
        self._anchor[update.effective_chat.id] = update.callback_query.message.message_id
        return ConversationHandler.END

    @staticmethod
    def _cancel_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Скасувати", callback_data="menu")]])

    async def _edit_anchor(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str, markup) -> None:
        """Edit the one persistent menu message in place. Falls back to sending a new one (and
        remembering it) only if the old message is gone. This is what keeps the chat to a single
        message — every step of a flow edits this same message instead of sending new ones."""
        mid = self._anchor.get(update.effective_chat.id)
        if mid:
            try:
                await ctx.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=mid,
                                                text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
                return
            except Exception:  # noqa: BLE001 — message gone/identical; fall through to a fresh one
                pass
        m = await update.effective_chat.send_message(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        self._anchor[update.effective_chat.id] = m.message_id

    async def _refresh_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int, prefix: str = "") -> None:
        menu = await self._main_menu(owner)
        text = (prefix + "\n\n" + menu["text"]) if prefix else menu["text"]
        await self._edit_anchor(update, ctx, text, menu["reply_markup"])

    async def _main_menu(self, owner: int, force_bal: bool = False) -> dict:
        profile = await self._prof(owner)
        venues = await self._store.venues_set(owner, profile)
        l = "✅" if "lighter" in venues else "❌"
        e = "✅" if "entropy" in venues else "❌"
        bal = await self._balances_cached(owner, force=force_bal)
        # How many OTHER profiles are trading right now: with parallel sessions the menu must not
        # look idle just because the profile you happen to be looking at is.
        running = [x for x in await self._store.active_sessions(owner)]
        others = sum(1 for x in running if (x.get("profile") or "") != profile)
        run_line = ""
        if running:
            mine = any((x.get("profile") or "") == profile for x in running)
            run_line = (f"🟢 Сесія: {'йде' if mine else '—'}"
                        + (f"  ·  ще {others} на інших профілях" if others else "") + f"{NL}{NL}")
        text = (f"🤖 <b>Delta-Points</b>{NL}{NL}"
                f"👤 Профіль: <b>{html.escape(profile)}</b>{NL}"
                f"Ключі: Lighter {l}  ·  Entropy {e}{NL}{NL}"
                f"{run_line}{bal}{NL}{NL}"
                f"Хедж відкривається лімітками по спільних монетах (лонг на одній біржі, шорт на іншій).")
        rows = [
            [InlineKeyboardButton(f"👤 Профіль: {profile}", callback_data="profs")],
            [InlineKeyboardButton("🔄 Оновити баланси", callback_data="refresh")],
            [InlineKeyboardButton("🔑 Ключі", callback_data="keys")],
            [InlineKeyboardButton("📈 Відкрити хедж (ручний)", callback_data="open")],
            [InlineKeyboardButton("🤖 Авто-сесія", callback_data="sess")],
            [InlineKeyboardButton("📊 Статистика (Google Sheets)", callback_data="stats")],
            [InlineKeyboardButton("📂 Позиції / Закрити", callback_data="positions")],
        ]
        return {"text": text, "reply_markup": InlineKeyboardMarkup(rows), "parse_mode": ParseMode.HTML}

    async def _balances_cached(self, owner: int, force: bool = False) -> str:
        """Formatted balance block for the main menu, cached for BAL_TTL so ordinary navigation is
        instant; `force` (the 🔄 button) refetches now."""
        key = (owner, await self._prof(owner))
        hit = self._bal_cache.get(key)
        if hit and not force and (time.monotonic() - hit["ts"]) < self.BAL_TTL:
            return hit["text"]
        text = await self._fetch_balances_text(owner)
        at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        self._bal_cache[key] = {"ts": time.monotonic(), "at": at, "text": f"{text}{NL}<i>оновлено {at}</i>"}
        return self._bal_cache[key]["text"]

    async def _fetch_balances_text(self, owner: int) -> str:
        """Read both venues' balances into one compact block. The two venues are read CONCURRENTLY
        (building a client is a network round-trip each) and guarded independently, so the refresh
        takes as long as the slower one, not their sum."""
        fmt = lambda v: f"${v:,.2f}" if isinstance(v, (int, float)) else "—"

        async def entropy_line() -> str:
            try:
                ent = await self._entropy_client(owner)
                if not ent:
                    return "🟩 Entropy: ключ не заведено"
                b = await ent.balance()
                io, spot = float(b.get("io") or 0), float(b.get("spot") or 0)
                # `spot` is only the FREE spot USDC — the part backing io is already inside io.
                return f"🟩 Entropy: {fmt(b.get('total'))} (io {fmt(io)} + вільно spot {fmt(spot)})"
            except Exception as err:  # noqa: BLE001
                LOGGER.exception("entropy balance failed")
                await self._drop_clients(owner)   # cached client may be dead — rebuild next time
                return f"🟩 Entropy: помилка — {html.escape(str(err)[:120])}"

        async def lighter_line() -> str:
            try:
                lit = await self._lighter_client(owner)
                if not lit:
                    return "🟦 Lighter: ключ не заведено"
                b = await lit.balance()
                return f"🟦 Lighter: {fmt(b.get('total'))} · вільно {fmt(b.get('available'))}"
            except Exception as err:  # noqa: BLE001
                LOGGER.exception("lighter balance failed")
                await self._drop_clients(owner)   # cached client may be dead — rebuild next time
                return f"🟦 Lighter: помилка — {html.escape(str(err)[:120])}"

        e_line, l_line = await asyncio.gather(entropy_line(), lighter_line())
        return NL.join(["💰 <b>Баланси</b>", e_line, l_line])

    async def _router(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._guard(update) is None:
            await update.callback_query.answer("⛔", show_alert=True)
            return
        q = update.callback_query
        data = q.data
        await q.answer()
        owner = update.effective_user.id
        self._anchor[update.effective_chat.id] = q.message.message_id   # this message is the anchor we keep
        if data == "menu":
            await q.edit_message_text(**await self._main_menu(owner))
        elif data == "keys":
            await self._keys_menu(update, owner)
        elif data == "profs":
            await self._profs_menu(update, owner)
        elif data == "proxy":
            await self._proxy_menu(update, owner)
        elif data == "prof:proxyoff":
            await self._store.set_proxy(owner, await self._prof(owner), None)
            await self._forget(owner, await self._prof(owner))
            await self._proxy_menu(update, owner, "🗑 Проксі прибрано — трафік піде напряму.")
        elif data.startswith("prof:"):
            await self._prof_action(update, ctx, owner, data)
        elif data.startswith("unkey:") and data != "unkey:gsheets":
            venue = data.split(":", 1)[1]
            await self._store.delete_credentials(owner, venue, await self._prof(owner))
            await self._forget(owner, await self._prof(owner))
            await self._drop_clients(owner)
            await self._keys_menu(update, owner)   # already answered above; refreshed menu shows ❌
        elif data == "refresh":
            await q.edit_message_text(**await self._main_menu(owner, force_bal=True))
        elif data == "open":
            await self._open_draft(ctx, owner)
            await self._open_config(update, ctx)
        elif data == "cfg:pair":
            d = await self._open_draft(ctx, owner)
            keys = [p.key for p in PAIRS]
            d["pair"] = keys[(keys.index(d["pair"]) + 1) % len(keys)]
            await self._open_config(update, ctx)
        elif data == "cfg:side":
            # Auto -> Entropy LONG -> Entropy SHORT -> Auto
            d = await self._open_draft(ctx, owner)
            nxt = {None: True, True: False, False: None}[d.get("entropy_long")]
            d["entropy_long"], d["side_manual"] = nxt, nxt is not None
            await self._open_config(update, ctx)
        elif data == "cfg:lev":
            await self._lev_menu(update, ctx)
        elif data.startswith("setlev:"):
            (await self._open_draft(ctx, owner))["leverage"] = int(data.split(":", 1)[1])
            await self._open_config(update, ctx)
        elif data == "cfg:preview":
            await self._preview(update, ctx, owner)
        elif data == "confirm":
            await self._execute(update, ctx, owner)
        elif data == "sess":
            await self._sess_open(update, ctx, owner)
        elif data == "sess:coins":
            await self._sess_coins(update, ctx)
        elif data.startswith("scoin:"):
            key = data.split(":", 1)[1]
            coins = (await self._sess_draft(ctx, owner))["coins"]
            if key in coins:
                coins.remove(key)
            else:
                coins.append(key)
            await self._sess_coins(update, ctx)
        elif data == "sess:lev":
            await self._sess_lev(update, ctx)
        elif data.startswith("sslev:"):
            (await self._sess_draft(ctx, owner))["leverage"] = int(data.split(":", 1)[1])
            await self._sess_config(update, ctx)
        elif data == "sess:dur":
            await self._sess_dur(update, ctx)
        elif data.startswith("ssdur:"):
            (await self._sess_draft(ctx, owner))["duration"] = int(data.split(":", 1)[1])
            await self._sess_config(update, ctx)
        elif data == "sess:pausetoggle":
            sess = await self._sess_draft(ctx, owner)
            sess["pause_on"] = not sess.get("pause_on")
            await self._sess_config(update, ctx)
        elif data == "sess:splittoggle":
            sess = await self._sess_draft(ctx, owner)
            sess["split_on"] = not sess.get("split_on")
            await self._sess_config(update, ctx)
        elif data == "sess:dry":
            sess = await self._sess_draft(ctx, owner)
            sess["dry_run"] = not sess.get("dry_run")
            await self._sess_config(update, ctx)
        elif data == "sess:notify":
            s = await self._sess_draft(ctx, owner)
            s["notify_each"] = not s.get("notify_each", False)
            await self._sess_config(update, ctx)
        elif data == "sess:start":
            await self._sess_start(update, ctx, owner)
        elif data == "sess:stop":
            await self._sess_stop(update, ctx, owner)
        elif data == "stats":
            await self._stats_menu(update, owner)
        elif data == "stats:instr":
            await self._stats_instructions(update)
        elif data == "stats:test":
            await self._stats_test(update, owner)
        elif data == "unkey:gsheets":
            await self._store.delete_credentials(owner, "gsheets", ACCOUNT_SCOPE)
            await self._stats_menu(update, owner)
        elif data == "positions":
            await self._positions(update, owner)
        elif data.startswith("close:"):
            await self._close(update, owner, int(data.split(":", 1)[1]))

    async def _keys_menu(self, update: Update, owner: int) -> None:
        profile = await self._prof(owner)
        venues = await self._store.venues_set(owner, profile)
        has_l, has_e = "lighter" in venues, "entropy" in venues
        proxy = await self._store.get_proxy(owner, profile)
        text = (
            f"🔑 <b>Ключі — профіль «{html.escape(profile)}»</b>\n\n"
            f"🟦 Lighter: {'✅ заведено' if has_l else '❌ нема'}\n"
            f"🟩 Entropy: {'✅ заведено' if has_e else '❌ нема'}\n"
            f"🌐 Проксі: {('<code>' + html.escape(self._mask_proxy(proxy)) + '</code>') if proxy else '— нема'}\n\n"
            "<i>Ключі й проксі належать цьому профілю. Інші профілі — інші акаунти.</i>"
        )
        rows = [
            [InlineKeyboardButton("🗑 Відв'язати Lighter" if has_l else "➕ Завести Lighter",
                                  callback_data="unkey:lighter" if has_l else "key:lighter")],
            [InlineKeyboardButton("🗑 Відв'язати Entropy" if has_e else "➕ Завести Entropy",
                                  callback_data="unkey:entropy" if has_e else "key:entropy")],
            [InlineKeyboardButton("🌐 Проксі", callback_data="proxy")],
            [InlineKeyboardButton("👤 Профілі", callback_data="profs")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
        ]
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    # ── profiles (one Entropy + one Lighter + their shared proxy) ────────────────────────────────
    async def _keys_locked(self, owner: int, profile: str) -> str:
        """Why this profile's keys must not change right now -- "" when they may.

        Swapping accounts under a live hedge is unrecoverable: the position stays on the account that
        opened it, and the new keys cannot see, guard or close it."""
        if await self._store.active_session(owner, profile):
            return "На цьому профілі йде авто-сесія — зупини її спершу."
        if await self._store.open_hedges(owner, profile):
            return "На цьому профілі є відкриті хеджі — позицію іншим акаунтом не закрити."
        return ""

    async def _forget(self, owner: int, profile: str | None = None) -> None:
        """Drop cached clients and balances after keys, proxy or the current profile changed. The
        engine keeps its own cache, and a stale one would keep trading the previous account."""
        await self._drop_clients(owner, profile)
        if self._engine and profile is not None:
            with contextlib.suppress(Exception):
                await self._engine._drop_clients(owner, profile)

    async def _profs_menu(self, update: Update, owner: int, note: str = "") -> None:
        profs = await self._store.profiles(owner)
        running = {(x.get("profile") or "") for x in await self._store.active_sessions(owner)}
        lines = ["👤 <b>Профілі</b>", "",
                 "Профіль — це один Entropy + один Lighter + спільний проксі, зі своїми "
                 "налаштуваннями. Профілі торгують паралельно, кожен сам по собі.", ""]
        for x in profs:
            marks = (f"🟦{'✅' if x['has_lighter'] else '❌'} 🟩{'✅' if x['has_entropy'] else '❌'} "
                     f"🌐{'✅' if x['has_proxy'] else '—'}")
            run = "  ·  🟢 сесія" if x["name"] in running else ""
            lines.append(f"{'✅' if x['selected'] else '○'} <b>{html.escape(x['name'])}</b> — {marks}{run}")
        if note:
            lines += ["", note]
        rows = [[InlineKeyboardButton(f"{'✅' if x['selected'] else '○'} {x['name']}",
                                      callback_data=f"prof:use:{i}"),
                 InlineKeyboardButton("✏️", callback_data=f"prof:ren:{i}"),
                 InlineKeyboardButton("🗑", callback_data=f"prof:del:{i}")]
                for i, x in enumerate(profs)]
        rows.append([InlineKeyboardButton("➕ Додати профіль", callback_data="prof:add")])
        rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="menu")])
        await update.callback_query.edit_message_text(
            "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _prof_action(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                           owner: int, data: str) -> None:
        # Buttons carry the row index, not the name: names are free text and would not survive
        # Telegram's 64-byte callback budget.
        _, action, raw = data.split(":", 2)
        profs = await self._store.profiles(owner)
        i = int(raw) if raw.isdigit() else -1
        if not 0 <= i < len(profs):
            await self._profs_menu(update, owner, "⚠️ Список змінився — оновив.")
            return
        prof = profs[i]
        if action == "use":
            if not prof["selected"]:
                await self._store.select_profile(owner, prof["name"])
                await self._forget(owner)          # every screen now speaks to the other account
                for k in ("sess", "sess_profile", "draft", "draft_profile", "plan_ready"):
                    ctx.chat_data.pop(k, None)     # drafts belong to the profile they were loaded for
            await self._refresh_menu(update, ctx, owner, f"👤 Профіль: <b>{html.escape(prof['name'])}</b>")
        elif action == "del":
            lock = await self._keys_locked(owner, prof["name"])
            if lock:
                await self._profs_menu(update, owner, f"⛔ {lock}")
            elif len(profs) == 1:
                await self._profs_menu(update, owner, "⛔ Це єдиний профіль — його не можна видалити.")
            else:
                await update.callback_query.edit_message_text(
                    f"🗑 Видалити профіль «{html.escape(prof['name'])}»?\n\n"
                    "Разом з ним зникнуть його ключі Entropy і Lighter та проксі — вводити доведеться "
                    "заново. Історія хеджів залишиться.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🗑 Так, видалити", callback_data=f"prof:delok:{i}")],
                        [InlineKeyboardButton("⬅️ Ні", callback_data="profs")]]),
                    parse_mode=ParseMode.HTML)
        elif action == "delok":
            lock = await self._keys_locked(owner, prof["name"])
            if lock or len(profs) == 1:
                await self._profs_menu(update, owner, f"⛔ {lock or 'Це єдиний профіль.'}")
                return
            await self._store.delete_profile(owner, prof["name"])
            await self._forget(owner)
            await self._store.selected_profile(owner)      # promotes a survivor if this was selected
            await self._profs_menu(update, owner, f"🗑 «{html.escape(prof['name'])}» видалено.")

    async def _proxy_menu(self, update: Update, owner: int, note: str = "") -> None:
        profile = await self._prof(owner)
        proxy = await self._store.get_proxy(owner, profile)
        lines = [f"🌐 <b>Проксі — профіль «{html.escape(profile)}»</b>", ""]
        lines.append(f"Зараз: <code>{html.escape(self._mask_proxy(proxy))}</code>" if proxy
                     else "Зараз: <b>без проксі</b> — обидві біржі бачать IP сервера.")
        lines += ["", "Проксі спільний для Entropy і Lighter цього профілю. Підтримуються "
                      "<b>HTTP</b>-проксі (<code>http://user:pass@host:port</code>): трафік до бірж "
                      "усе одно йде через TLS наскрізь, тож проксі бачить лише адресу, а не запити."]
        if note:
            lines += ["", note]
        rows = [[InlineKeyboardButton("✏️ Задати / змінити", callback_data="prof:proxy")]]
        if proxy:
            rows.append([InlineKeyboardButton("🗑 Прибрати проксі", callback_data="prof:proxyoff")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="keys")])
        await update.callback_query.edit_message_text(
            "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    @staticmethod
    def _mask_proxy(url: str | None) -> str:
        """Show the proxy without its password -- the screen is a chat message that stays in history."""
        return re.sub(r"://([^:/@]+):[^@]*@", r"://\1:***@", url or "")

    @staticmethod
    async def _proxy_check(proxy: str) -> tuple[bool, str]:
        """Prove the proxy actually carries traffic and say which IP the world sees through it. A dead
        proxy otherwise shows up as every order failing, minutes later and with a confusing error."""
        try:
            timeout = aiohttp.ClientTimeout(total=12, connect=8)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get("https://api.ipify.org?format=json", proxy=proxy) as r:
                    if r.status != 200:
                        return False, f"HTTP {r.status}"
                    return True, str((await r.json()).get("ip") or "?")
        except Exception as e:  # noqa: BLE001 -- any failure here means "do not save this"
            return False, f"{type(e).__name__}: {e}"[:160]

    # ── stats / Google Sheets ──────────────────────────────────────────────────────────────────────
    async def _stats_menu(self, update: Update, owner: int) -> None:
        creds = await self._store.get_credentials(owner, "gsheets", ACCOUNT_SCOPE)
        if creds:
            email = creds.meta.get("client_email", "—")
            text = (
                "📊 <b>Статистика — Google Sheets</b>\n\n"
                "✅ Таблицю підключено.\n"
                f"Сервісний акаунт: <code>{html.escape(str(email))}</code>\n\n"
                "На кожну авто-сесію бот створює вкладку «Сесія N» і пише туди по рядку на кожен хедж "
                "(час, статус, PnL, комісії, обсяги) + підсумковий рядок.\n\n"
                "<i>Саме тому в чат не сиплються алерти на кожен вхід/вихід — усе йде в таблицю. "
                "Хочеш пінги назад — увімкни «🔔 Алерти» в налаштуваннях сесії.</i>"
            )
            rows = [
                [InlineKeyboardButton("🧪 Перевірити доступ", callback_data="stats:test")],
                [InlineKeyboardButton("🗑 Відключити таблицю", callback_data="unkey:gsheets")],
                [InlineKeyboardButton("ℹ️ Інструкція", callback_data="stats:instr")],
                [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
            ]
        else:
            text = (
                "📊 <b>Статистика — Google Sheets</b>\n\n"
                "❌ Таблицю ще не підключено.\n\n"
                "Підключиш свою Google-таблицю — і бот вестиме туди повну статистику сесій "
                "(окрема вкладка на сесію, рядок на кожен хедж + підсумки), а в чат не спамитиме.\n\n"
                "Тисни «ℹ️ Інструкція» — там покроково, де взяти ключ."
            )
            rows = [
                [InlineKeyboardButton("➕ Підключити таблицю", callback_data="key:gsheets")],
                [InlineKeyboardButton("ℹ️ Інструкція", callback_data="stats:instr")],
                [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
            ]
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    @staticmethod
    def _gsheets_hint(detail: str) -> str:
        d = (detail or "").lower()
        if "no module" in d or "modulenotfound" in d:
            return ("Схоже, бібліотека для Google Sheets не встановлена на сервері — це на моїй "
                    "стороні, напиши мені, я переставлю.")
        if "has not been used" in d or "service_disabled" in d or "disabled" in d:
            return ("Не ввімкнено <b>Google Sheets API</b>. Відкрий console.cloud.google.com → "
                    "APIs &amp; Services → Enabled APIs → Enable APIs → знайди <b>Google Sheets API</b> → Enable.")
        if "permission" in d or "permissiondenied" in d or "403" in d:
            return ("Найімовірніше — <b>таблицею не поділено з сервіс-акаунтом</b>. Відкрий таблицю → "
                    "Share → додай його email як <b>Editor</b>.")
        if "not found" in d or "404" in d:
            return ("Не той <b>ID таблиці</b>. Скопіюй посилання прямо з адресного рядка відкритої таблиці "
                    "(…/spreadsheets/d/<b>ID</b>/…).")
        if "invalid_grant" in d or "jwt" in d or "signature" in d:
            return ("Проблема з ключем (можливо, ключ вимкнено або зіпсовано). Створи новий JSON-ключ "
                    "сервіс-акаунта і додай його знову.")
        return ("Перевір по черзі: (1) таблицею поділено з сервіс-акаунтом як Editor; "
                "(2) ввімкнено Google Sheets API; (3) правильний лінк на таблицю.")

    async def _stats_test(self, update: Update, owner: int) -> None:
        await update.callback_query.edit_message_text("⏳ Перевіряю доступ до таблиці…")
        ok, detail = (False, "рушій статистики недоступний")
        if self._sheets:
            ok, detail = await self._sheets.test_connection(owner)
        if ok:
            text = f"✅ Доступ є. Таблиця: «{html.escape(detail)}».\n\nСтатистика сесій піде туди."
        else:
            text = (f"❌ Немає доступу до таблиці.\n\n<b>Помилка:</b>\n<code>{html.escape(detail)}</code>\n\n"
                    f"{self._gsheets_hint(detail)}")
        rows = [[InlineKeyboardButton("🔁 Ще раз", callback_data="stats:test")],
                [InlineKeyboardButton("ℹ️ Інструкція", callback_data="stats:instr")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="stats")]]
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _stats_instructions(self, update: Update) -> None:
        text = (
            "ℹ️ <b>Як підключити Google-таблицю (5 хв, безкоштовно)</b>\n\n"
            "1️⃣ Зайди на <b>console.cloud.google.com</b> → створи проєкт (будь-яку назву).\n"
            "2️⃣ У пошуку зверху знайди <b>Google Sheets API</b> → <b>Enable</b>.\n"
            "3️⃣ Меню → <b>APIs &amp; Services → Credentials → Create credentials → "
            "Service account</b>. Назву будь-яку, ролі можна пропустити → <b>Done</b>.\n"
            "4️⃣ Відкрий створений сервіс-акаунт → вкладка <b>Keys → Add key → Create new key → "
            "JSON</b>. Завантажиться <b>.json</b> файл — це і є ключ.\n"
            "5️⃣ Створи звичайну <b>Google-таблицю</b> (sheets.new). Скопіюй <b>email сервіс-акаунта</b> "
            "(вигляд <code>...@...gserviceaccount.com</code>, він у тому ж json, поле "
            "<code>client_email</code>) і <b>поділись</b> таблицею з ним як <b>Editor</b> "
            "(кнопка Share).\n\n"
            "6️⃣ Повертайся сюди → «➕ Підключити таблицю»:\n"
            "  • спершу надішли <b>вміст .json файлу</b> (відкрий його блокнотом, скопіюй усе, встав "
            "одним повідомленням);\n"
            "  • потім надішли <b>посилання на таблицю</b> (або її ID).\n\n"
            "⚠️ JSON — це секрет; бот його шифрує. Після додавання я одразу видаляю твоє повідомлення."
        )
        rows = [[InlineKeyboardButton("➕ Підключити таблицю", callback_data="key:gsheets")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="stats")]]
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    # ── free-text input (keys + notional) ──────────────────────────────────────────────────────────
    async def _begin_input(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        if self._guard(update) is None:
            return ConversationHandler.END
        await update.callback_query.answer()
        data = update.callback_query.data
        # This button lives on the persistent menu message — make it the anchor we keep editing.
        self._anchor[update.effective_chat.id] = update.callback_query.message.message_id
        if data == "key:lighter":
            ctx.user_data.clear()
            ctx.user_data.update(flow="key_lighter", step=0)
            await update.callback_query.edit_message_text(
                "🟦 <b>Lighter — крок 1/3</b>\n\n"
                "На <b>robinhoodchain.lighter.xyz</b> відкрий розділ <b>API</b>, створи/візьми "
                "API-ключ і надішли <b>приватний ключ API-ключа</b> (0x…).\n\n"
                "⚠️ Це НЕ ключ гаманця — це згенерований API-ключ. І саме RH-деплой.",
                reply_markup=self._cancel_kb(), parse_mode=ParseMode.HTML)
        elif data == "prof:add":
            ctx.user_data.clear()
            ctx.user_data.update(flow="prof_add")
            await update.callback_query.edit_message_text(
                "➕ <b>Новий профіль</b>\n\nНадішли <b>назву</b> — щоб упізнавати його у списку "
                "(напр. <code>Акк 2</code>).\n\nКлючі Entropy/Lighter і проксі заведеш йому "
                "окремо, після створення.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="profs")]]),
                parse_mode=ParseMode.HTML)
        elif data == "prof:proxy":
            ctx.user_data.clear()
            ctx.user_data.update(flow="prof_proxy")
            await update.callback_query.edit_message_text(
                "🌐 <b>Проксі профілю</b>\n\nНадішли адресу у вигляді\n"
                "<code>http://user:pass@host:port</code>  (або без логіна: <code>http://host:port</code>).\n\n"
                "Тільки <b>HTTP</b>-проксі. Перевірю його одразу й покажу, який IP видно через нього.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="proxy")]]),
                parse_mode=ParseMode.HTML)
        elif data.startswith("prof:ren:"):
            i = int(data.rsplit(":", 1)[1])
            profs = await self._store.profiles(update.effective_user.id)
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="profs")]])
            if not 0 <= i < len(profs):
                await update.callback_query.edit_message_text("⚠️ Список змінився.", reply_markup=back)
                return ConversationHandler.END
            ctx.user_data.clear()
            ctx.user_data.update(flow="prof_rename", old_name=profs[i]["name"])
            await update.callback_query.edit_message_text(
                f"✏️ Нова назва для «{html.escape(profs[i]['name'])}»:",
                reply_markup=back, parse_mode=ParseMode.HTML)
        elif data == "key:entropy":
            ctx.user_data.clear()
            ctx.user_data.update(flow="key_entropy", step=0)
            await update.callback_query.edit_message_text(
                "🟩 <b>Entropy — крок 1/2</b>\n\n"
                "Надішли <b>адресу свого ОСНОВНОГО гаманця</b> (0x…) — того, яким депозитив USDC на "
                "entropy.io. Просто адреса, не ключ.",
                reply_markup=self._cancel_kb(), parse_mode=ParseMode.HTML)
        elif data == "key:gsheets":
            ctx.user_data.clear()
            ctx.user_data.update(flow="key_gsheets", step=0)
            await update.callback_query.edit_message_text(
                "📊 <b>Google Sheets — крок 1/2</b>\n\nНадішли <b>весь вміст .json файлу</b> сервіс-акаунта "
                "(відкрий його блокнотом, скопіюй усе й встав одним повідомленням).",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="stats")]]),
                parse_mode=ParseMode.HTML)
        elif data == "cfg:margin":
            ctx.user_data.clear()
            ctx.user_data["flow"] = "cfg_margin"
            await update.callback_query.edit_message_text(
                "💵 Надішли <b>маржу в USD на ногу</b> числом (розмір позиції = маржа × плече):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="open")]]),
                parse_mode=ParseMode.HTML)
        elif data in SESS_INPUTS:
            ctx.user_data.clear()
            ctx.user_data["flow"] = "sess_" + data.split(":", 1)[1]
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="sess")]])
            if data == "sess:margin":
                msg = "💵 Надішли <b>маржу в USD на ногу</b> числом (розмір = маржа × плече):"
            elif data == "sess:split":
                msg = ("🧩 Надішли <b>кількість частин і паузу в секундах</b> — два числа через "
                       "пробіл, напр. <code>10 7</code>.\n\n"
                       "Позиція набиратиметься стількома лімітками поспіль: кожна частина "
                       "відкривається на Entropy і одразу хеджується на Lighter, потім пауза — і "
                       "наступна. Дрібніші маркет-ордери менше рухають стакан Lighter.\n\n"
                       "Закриття йде так само. Якщо частина виходить меншою за мінімум біржі, "
                       "бот сам зменшить їх кількість і скаже про це.")
            elif data == "sess:reprice":
                msg = ("🎯 Надішли <b>час життя лімітки в секундах</b> (напр. 8).\n\n"
                       "Лімітка ставиться за 2 тіки від ціни і стоїть рівно стільки — не менше і не "
                       "більше. Потім бот її знімає, хеджує на Lighter те, що встигло залитись, і "
                       "ставить нову лімітку на залишок. І так, поки не набереться весь розмір.")

            elif data == "sess:hold":
                msg = ("⏱ Надішли <b>діапазон утримання у хвилинах</b> — два числа через пробіл.\n\n"
                       "Напр. <code>30 120</code> (30хв–2г) або <code>60 1440</code> (1г–24г).\n"
                       "Максимум — <b>1440 хв (24 години)</b>.")
            else:
                msg = ("⏸ Надішли <b>діапазон паузи у хвилинах</b> — два числа через пробіл, напр. "
                       "<code>5 30</code>. Максимум — 1440 хв (24 години).")
            await update.callback_query.edit_message_text(msg, reply_markup=back, parse_mode=ParseMode.HTML)
        return ASK

    async def _got_input(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        if self._guard(update) is None:
            return ConversationHandler.END
        text = (update.message.text or "").strip()
        # Delete the user's reply at once — these carry keys — and drive the whole flow by editing the
        # single anchor message, so nothing new is ever left in the chat.
        with contextlib.suppress(Exception):
            await update.message.delete()
        owner = update.effective_user.id
        if text == MENU_BTN:            # bottom keyboard tapped mid-flow — bail out to the menu
            ctx.user_data.clear()
            await self._reanchor_menu(update, ctx, owner)
            return ConversationHandler.END
        flow = ctx.user_data.get("flow")

        if flow in ("prof_add", "prof_rename"):
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ До профілів", callback_data="profs")]])
            name = text.strip()[:32]
            if not name or name == ACCOUNT_SCOPE:
                await self._edit_anchor(update, ctx, "Так назвати не можна. Надішли іншу назву:", back)
                return ASK
            if flow == "prof_add":
                ok = await self._store.create_profile(owner, name)
                msg = (f"➕ Профіль «{html.escape(name)}» створено. Тепер заведи йому ключі й проксі."
                       if ok else f"⚠️ Профіль «{html.escape(name)}» уже є.")
            else:
                ok = await self._store.rename_profile(owner, ctx.user_data["old_name"], name)
                # The name is the join key for keys, sessions and hedges, so cached clients are stale.
                await self._forget(owner)
                msg = (f"✏️ Перейменовано на «{html.escape(name)}»." if ok
                       else f"⚠️ Назва «{html.escape(name)}» вже зайнята — нічого не змінив.")
            ctx.user_data.clear()
            await self._edit_anchor(update, ctx, msg, back)
            return ConversationHandler.END

        if flow == "prof_proxy":
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ До проксі", callback_data="proxy")]])
            url = text.strip()
            if not re.match(r"^https?://[^\s]+:\d+/?$", url) and not re.match(r"^http://[^\s@]+@[^\s]+:\d+/?$", url):
                if not re.match(r"^https?://", url):
                    await self._edit_anchor(update, ctx,
                        "Не схоже на адресу проксі. Потрібно <code>http://host:port</code> або "
                        "<code>http://user:pass@host:port</code>:", back)
                    return ASK
            profile = await self._prof(owner)
            await self._edit_anchor(update, ctx, "⏳ Перевіряю проксі…", None)
            ok, detail = await self._proxy_check(url)
            if not ok:
                # Never save a proxy that does not work: it would fail at order time instead, minutes
                # later and with an error that says nothing about the proxy.
                await self._edit_anchor(update, ctx,
                    f"✗ Проксі не працює: <code>{html.escape(detail)}</code>\n\nНічого не зберіг. "
                    f"Перевір адресу/логін і надішли ще раз:", back)
                return ASK
            await self._store.set_proxy(owner, profile, url)
            await self._forget(owner, profile)
            ctx.user_data.clear()
            await self._edit_anchor(update, ctx,
                f"✅ Проксі збережено для «{html.escape(profile)}».\nЗовнішній IP через нього: "
                f"<code>{html.escape(detail)}</code>", back)
            return ConversationHandler.END

        if flow == "key_lighter":
            step = ctx.user_data["step"]
            if step == 0:
                ctx.user_data.update(priv=text, step=1)
                await self._edit_anchor(update, ctx,
                    "🟦 <b>Lighter — крок 2/3</b>\n\nНадішли <b>Account Index</b> — число зі сторінки "
                    "API на robinhoodchain.lighter.xyz.", self._cancel_kb())
                return ASK
            if step == 1:
                ctx.user_data.update(account_index=int(text), step=2)
                await self._edit_anchor(update, ctx,
                    "🟦 <b>Lighter — крок 3/3</b>\n\nНадішли <b>API Key Index</b> — номер слота ключа "
                    "зі сторінки API (число).", self._cancel_kb())
                return ASK
            api_key_index = int(text) if text.isdigit() else 0
            profile = await self._prof(owner)
            await self._store.set_credentials(
                owner, "lighter", ctx.user_data["priv"],
                {"account_index": ctx.user_data["account_index"], "api_key_index": api_key_index},
                profile)
            ctx.user_data.clear()
            await self._forget(owner, profile)
            await self._refresh_menu(update, ctx, owner,
                                     f"✅ Lighter збережено для «{html.escape(profile)}».")
            return ConversationHandler.END

        if flow == "key_entropy":
            step = ctx.user_data["step"]
            if step == 0:
                ctx.user_data.update(wallet=text, step=1)
                await self._edit_anchor(update, ctx,
                    "🟩 <b>Entropy — крок 2/2</b>\n\nНадішли <b>приватний ключ AGENT-ключа</b> (0x…) — "
                    "з app.hyperliquid.xyz/API (Generate → Authorize).\n\n"
                    "⚠️ Це ключ agent-а, а НЕ приватний ключ основного гаманця.", self._cancel_kb())
                return ASK
            profile = await self._prof(owner)
            await self._store.set_credentials(
                owner, "entropy", text, {"wallet_address": ctx.user_data["wallet"]}, profile)
            ctx.user_data.clear()
            await self._forget(owner, profile)
            await self._refresh_menu(update, ctx, owner,
                                     f"✅ Entropy збережено для «{html.escape(profile)}».")
            return ConversationHandler.END

        if flow == "key_gsheets":
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="stats")]])
            if ctx.user_data["step"] == 0:
                try:
                    info = json.loads(text)
                    email = info.get("client_email")
                    if not email or "private_key" not in info:
                        raise ValueError
                except (ValueError, TypeError):
                    await self._edit_anchor(update, ctx,
                        "Це не схоже на JSON сервіс-акаунта (має бути з полями <code>client_email</code> "
                        "та <code>private_key</code>). Скопіюй увесь файл і надішли ще раз:", back)
                    return ASK
                ctx.user_data.update(gs_json=text, gs_email=email, step=1)
                await self._edit_anchor(update, ctx,
                    "📊 <b>Google Sheets — крок 2/2</b>\n\n✅ Ключ прийнято.\n"
                    f"Не забудь <b>поділитись таблицею</b> з <code>{html.escape(email)}</code> (Editor).\n\n"
                    "Тепер надішли <b>посилання на таблицю</b> (або її ID):", back)
                return ASK
            # step 1 — extract the spreadsheet id from a full URL or a bare id
            m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", text)
            spreadsheet_id = m.group(1) if m else text.strip()
            if not re.fullmatch(r"[a-zA-Z0-9_-]{20,}", spreadsheet_id):
                await self._edit_anchor(update, ctx,
                    "Не розібрав ID таблиці. Надішли повне посилання (…/spreadsheets/d/<b>ID</b>/…) "
                    "або сам ID:", back)
                return ASK
            email = ctx.user_data["gs_email"]
            # The sheet is account-level: it must survive renaming or deleting any profile.
            await self._store.set_credentials(
                owner, "gsheets", ctx.user_data["gs_json"],
                {"spreadsheet_id": spreadsheet_id, "client_email": email}, ACCOUNT_SCOPE)
            ctx.user_data.clear()
            # Verify access right away so a missing "share with the service account" is caught now.
            ok, detail = (True, "")
            if self._sheets:
                await self._edit_anchor(update, ctx, "⏳ Перевіряю доступ до таблиці…", None)
                ok, detail = await self._sheets.test_connection(owner)
            if ok:
                msg = (f"✅ Google-таблицю «{html.escape(detail)}» підключено. Статистика сесій піде туди."
                       if detail else "✅ Google-таблицю підключено. Статистика сесій піде туди.")
                await self._refresh_menu(update, ctx, owner, msg)
            else:
                # Own screen, not folded into the menu, so the error and the fix are impossible to miss.
                text = (f"❌ Ключ збережено, але доступу до таблиці немає.\n\n"
                        f"<b>Помилка:</b>\n<code>{html.escape(detail)}</code>\n\n"
                        f"{self._gsheets_hint(detail)}\n\n"
                        f"Email сервіс-акаунта: <code>{html.escape(email)}</code>")
                rows = [[InlineKeyboardButton("🔁 Перевірити ще раз", callback_data="stats:test")],
                        [InlineKeyboardButton("ℹ️ Інструкція", callback_data="stats:instr")],
                        [InlineKeyboardButton("⬅️ Меню", callback_data="menu")]]
                await self._edit_anchor(update, ctx, text, InlineKeyboardMarkup(rows))
            return ConversationHandler.END

        if flow == "cfg_margin":
            try:
                value = float(text.replace(",", "."))
                if value <= 0:
                    raise ValueError
            except ValueError:
                await self._edit_anchor(update, ctx, "Не зрозумів число, надішли ще раз:",
                                        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="open")]]))
                return ASK
            ctx.chat_data.setdefault(
                "draft", {"pair": PAIRS[0].key, "leverage": 3, "margin": 5.0, "entropy_long": None})["margin"] = value
            ctx.user_data.clear()
            await self._open_config(update, ctx)
            return ConversationHandler.END

        if flow in SESS_INPUT_FLOWS:
            s = await self._sess_draft(ctx, owner)
            back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="sess")]])
            try:
                if flow == "sess_margin":
                    value = float(text.replace(",", "."))
                    if value <= 0:
                        raise ValueError
                    s["margin"] = value
                elif flow == "sess_reprice":
                    s["reprice_s"] = max(1, int(float(text)))
                elif flow == "sess_split":
                    a, b = (float(x) for x in text.replace(",", ".").split()[:2])
                    s["split_parts"] = max(1, min(int(a), 100))
                    s["split_gap_s"] = max(0, min(int(b), 600))
                    s["split_on"] = s["split_parts"] > 1
                else:
                    a, b = (float(x) for x in text.replace(",", ".").split()[:2])
                    lo, hi = sorted((a, b))
                    if lo <= 0:
                        raise ValueError
                    lo, hi = min(lo, MAX_RANGE_MIN), min(hi, MAX_RANGE_MIN)
                    if flow == "sess_hold":
                        s["hold_min"], s["hold_max"] = lo * 60, hi * 60
                    else:
                        s["pause_min"], s["pause_max"] = lo * 60, hi * 60
                        s["pause_on"] = True
            except (ValueError, IndexError):
                await self._edit_anchor(update, ctx, "Не зрозумів. Для діапазону — два числа через пробіл "
                                        "(напр. <code>60 1440</code>, максимум 1440); для таймауту — "
                                        "одне число:", back)
                return ASK
            ctx.user_data.clear()
            await self._sess_config(update, ctx)
            return ConversationHandler.END

        return ConversationHandler.END

    # ── open hedge (config screen) ───────────────────────────────────────────────────────────────
    async def _open_config(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        d = await self._open_draft(ctx, update.effective_user.id)
        with contextlib.suppress(Exception):
            oid = update.effective_user.id
            await self._store.save_draft(oid, await self._prof(oid), d)  # remember last settings
        pair = get_pair(d["pair"])
        notional = d["margin"] * d["leverage"]
        side_mode = d.get("entropy_long")
        if side_mode is None:
            side_line = "Напрям: <b>Авто</b> — лонг там, де монета дешевша, шорт там, де дорожча"
            side_btn = "🔄 Напрям: Авто (по ціні)"
        else:
            e_side, l_side = ("LONG", "SHORT") if side_mode else ("SHORT", "LONG")
            side_line = f"Напрям: Entropy <b>{e_side}</b> / Lighter <b>{l_side}</b> (вручну)"
            side_btn = f"🔄 Напрям: Entropy {e_side}"
        text = (
            "📈 <b>Новий хедж</b>\n\n"
            f"Монета: <b>{pair.label}</b>\n"
            f"Плече: <b>{d['leverage']}x</b>\n"
            f"Маржа: <b>${d['margin']:g}</b>/ногу\n"
            f"→ Розмір позиції: <b>${notional:g}</b>/ногу\n"
            f"{side_line}\n\n"
            "<i>Entropy — лімітка-мейкер, Lighter — маркет після її заповнення.</i>"
        )
        rows = [
            [InlineKeyboardButton(f"🪙 Монета: {pair.label}", callback_data="cfg:pair")],
            [InlineKeyboardButton(f"⚙️ Плече: {d['leverage']}x", callback_data="cfg:lev"),
             InlineKeyboardButton(f"💵 Маржа: ${d['margin']:g}", callback_data="cfg:margin")],
            [InlineKeyboardButton(side_btn, callback_data="cfg:side")],
            [InlineKeyboardButton("👁 Прев'ю / Відкрити", callback_data="cfg:preview")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
        ]
        # _edit_anchor (not callback edit) so it works after a text step too.
        await self._edit_anchor(update, ctx, text, InlineKeyboardMarkup(rows))

    async def _lev_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        cur = (await self._open_draft(ctx, update.effective_user.id))["leverage"]
        def b(n): return InlineKeyboardButton(f"{'✅ ' if n == cur else ''}{n}x", callback_data=f"setlev:{n}")
        rows = [[b(1), b(2), b(3), b(4), b(5)], [b(6), b(7), b(8), b(9), b(10)],
                [InlineKeyboardButton("⬅️ Назад", callback_data="open")]]
        await self._edit_anchor(update, ctx, LEV_HINT, InlineKeyboardMarkup(rows))

    # ── auto-session ───────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _hhmm(v) -> str:
        """HH:MM:SS (UTC) from a datetime (asyncpg) or an ISO string; '—' if missing/unparsable."""
        if v is None:
            return "—"
        try:
            if isinstance(v, str):
                v = datetime.fromisoformat(v)
            return v.astimezone(timezone.utc).strftime("%H:%M:%S")
        except (ValueError, TypeError):
            return "—"

    def _fmt_secs(self, s: float) -> str:
        s = int(s)
        return f"{s/86400:.0f}д" if s >= 86400 else (f"{s/3600:.0f}г" if s >= 3600 else f"{s/60:.0f}хв")

    async def _sess_open(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> None:
        active = await self._store.active_session(owner, await self._prof(owner))
        if active:
            await self._sess_active(update, ctx, active)
            return
        await self._sess_draft(ctx, owner)
        await self._sess_config(update, ctx)

    async def _reprice_s(self, owner: int):
        """The limit lifetime the user configured for sessions. Manual opens/closes read the same
        setting, so both paths quote and re-quote on the identical clock."""
        with contextlib.suppress(Exception):
            return ((await self._store.load_settings(owner, await self._prof(owner)))
                    .get("session_cfg") or {}).get("reprice_s")
        return None

    async def _sess_config(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        s = await self._sess_draft(ctx, update.effective_user.id)
        with contextlib.suppress(Exception):
            oid = update.effective_user.id
            await self._store.save_session_cfg(oid, await self._prof(oid), s)
        coins = ", ".join(get_pair(k).label for k in s["coins"]) or "—"
        hold = f"{self._fmt_secs(s['hold_min'])}–{self._fmt_secs(s['hold_max'])}"
        pause = (f"увімк {self._fmt_secs(s['pause_min'])}–{self._fmt_secs(s['pause_max'])}"
                 if s["pause_on"] else "вимкнено")
        text = (
            "🤖 <b>Авто-сесія — налаштування</b>\n\n"
            f"🪙 Монети: <b>{coins}</b>\n"
            f"⚙️ Плече: <b>{s['leverage']}x</b>   💵 Маржа: <b>${s['margin']:g}</b>\n"
            f"⏱ Утримання: <b>{hold}</b>\n"
            f"⏸ Пауза: <b>{pause}</b>\n"
            f"🗓 Тривалість: <b>{self._fmt_secs(s['duration'])}</b>\n"
            f"🎯 Крок лімітки: <b>{s.get('reprice_s', 8)}с</b>\n"
            f"🧩 Набір: <b>{('розбивка ' + str(s.get('split_parts', 10)) + ' × ' + str(s.get('split_gap_s', 7)) + 'с') if s.get('split_on') else 'класика (одна лімітка)'}</b>\n"
            f"🔔 Алерти по хеджах: <b>{'увімк' if s.get('notify_each') else 'вимк (у таблицю)'}</b>\n"
            f"🧪 Режим: <b>{'DRY-RUN (тест)' if s['dry_run'] else 'LIVE (реальні ордери)'}</b>\n\n"
            "<i>Хеджі відкриваються/закриваються самі, час — рандом у межах утримання.</i>"
        )
        rows = [
            [InlineKeyboardButton("🪙 Монети", callback_data="sess:coins")],
            [InlineKeyboardButton(f"⚙️ Плече {s['leverage']}x", callback_data="sess:lev"),
             InlineKeyboardButton(f"💵 Маржа ${s['margin']:g}", callback_data="sess:margin")],
            [InlineKeyboardButton("⏱ Утримання", callback_data="sess:hold"),
             InlineKeyboardButton("⏸ Пауза", callback_data="sess:pause")],
            [InlineKeyboardButton(f"⏸ Пауза: {'увімк' if s['pause_on'] else 'вимк'}", callback_data="sess:pausetoggle")],
            [InlineKeyboardButton("🗓 Тривалість", callback_data="sess:dur")],
            [InlineKeyboardButton(f"🎯 Крок лімітки {s.get('reprice_s', 8)}с", callback_data="sess:reprice")],
            [InlineKeyboardButton(f"🧩 Розбивка: {'увімк' if s.get('split_on') else 'вимк'}",
                                  callback_data="sess:splittoggle"),
             InlineKeyboardButton(f"{s.get('split_parts', 10)} × {s.get('split_gap_s', 7)}с",
                                  callback_data="sess:split")],
            [InlineKeyboardButton(f"🔔 Алерти: {'увімк' if s.get('notify_each') else 'вимк'}", callback_data="sess:notify"),
             InlineKeyboardButton(f"🧪 {'DRY-RUN' if s['dry_run'] else 'LIVE'}", callback_data="sess:dry")],
            [InlineKeyboardButton("▶️ Старт", callback_data="sess:start")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
        ]
        await self._edit_anchor(update, ctx, text, InlineKeyboardMarkup(rows))

    async def _sess_active_view(self, sess_row: dict) -> dict:
        """Render the active-session status (text + buttons) — usable both from a handler and from the
        auto re-anchor after a notification."""
        cfg = sess_row["config"]
        hedges = await self._store.session_hedges(sess_row["id"])
        # OPENING is NOT open: the maker order is still working and no hold has started. Counting
        # the two together is what made a hedge look overdue when its clock had not begun.
        filling = [h for h in hedges if h["status"] == "OPENING"]
        opened = [h for h in hedges if h["status"] == "OPEN"]
        closed = [h for h in hedges if h["status"] == "CLOSED"]
        text = (
            f"🤖 <b>Авто-сесія — {'⏹ зупиняється…' if sess_row['status']=='STOPPING' else '🟢 активна'}</b>\n\n"
            f"Режим: {'DRY-RUN' if cfg.get('dry_run') else 'LIVE'}\n"
            f"Хеджів: {len(hedges)} (відкрито {len(opened)}, закрито {len(closed)}"
            + (f", набирається {len(filling)}" if filling else "") + ")\n"
            f"Монети: {', '.join(get_pair(k).label for k in cfg.get('coins', []))}\n"
        )
        if opened or filling:
            text += "\n<b>Відкриті зараз:</b>\n"
            for h in opened + filling:
                pair = get_pair(h["pair_key"])
                label = pair.label if pair else h["pair_key"]
                det = h.get("detail")
                if isinstance(det, str):
                    with contextlib.suppress(Exception):
                        det = json.loads(det or "{}")
                det = det if isinstance(det, dict) else {}
                if h["status"] == "OPENING":
                    text += (f"• {label} {h.get('entropy_side','')}: ⏳ набирається з "
                             f"{self._hhmm(det.get('started_at'))} — лімітка ще не заповнилась, "
                             f"відлік утримання не почався\n")
                else:
                    text += (f"• {label} {h.get('entropy_side','')}: відкрито "
                             f"{self._hhmm(h.get('opened_at'))} "
                             f"→ закриється ≈ {self._hhmm(det.get('close_at'))}\n")
            text += "<i>Час у UTC.</i>\n"
        rows = [
            [InlineKeyboardButton("⏹ Стоп (закрити все)", callback_data="sess:stop")],
            [InlineKeyboardButton("🔄 Оновити", callback_data="sess")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
        ]
        return {"text": text, "reply_markup": InlineKeyboardMarkup(rows), "parse_mode": ParseMode.HTML}

    async def _sess_active(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, sess_row: dict) -> None:
        view = await self._sess_active_view(sess_row)
        await self._edit_anchor(update, ctx, view["text"], view["reply_markup"])

    async def push_menu(self, app, chat_id: int) -> None:
        """Re-post the live anchor (active-session view, else main menu) at the BOTTOM of the chat and
        delete the previous one — so notifications scroll up above a menu that stays pinned to the
        bottom. Called after every bot-sent notification."""
        try:
            active = await self._store.active_session(chat_id, await self._prof(chat_id))
            view = await self._sess_active_view(active) if active else await self._main_menu(chat_id)
            old = self._anchor.get(chat_id)
            m = await app.bot.send_message(chat_id=chat_id, **view)
            self._anchor[chat_id] = m.message_id
            if old and old != m.message_id:
                with contextlib.suppress(Exception):
                    await app.bot.delete_message(chat_id, old)
        except Exception:  # noqa: BLE001 — re-anchoring must never break the notification path
            LOGGER.debug("push_menu failed", exc_info=True)

    async def _sess_coins(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chosen = (await self._sess_draft(ctx, update.effective_user.id))["coins"]
        rows = [[InlineKeyboardButton(f"{'✅ ' if p.key in chosen else '⬜ '}{p.label}", callback_data=f"scoin:{p.key}")]
                for p in PAIRS]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="sess")])
        await self._edit_anchor(update, ctx, "🪙 Обери монети (одну або кілька). Якщо кілька — наступний хедж на "
                                "рандомній з обраних:", InlineKeyboardMarkup(rows))

    async def _sess_lev(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        cur = (await self._sess_draft(ctx, update.effective_user.id))["leverage"]
        def b(n): return InlineKeyboardButton(f"{'✅ ' if n == cur else ''}{n}x", callback_data=f"sslev:{n}")
        rows = [[b(1), b(2), b(3), b(4), b(5)], [b(6), b(7), b(8), b(9), b(10)],
                [InlineKeyboardButton("⬅️ Назад", callback_data="sess")]]
        await self._edit_anchor(update, ctx, LEV_HINT, InlineKeyboardMarkup(rows))

    async def _sess_dur(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        cur = (await self._sess_draft(ctx, update.effective_user.id))["duration"]
        opts = [("12г", 43200), ("1д", 86400), ("3д", 259200), ("7д", 604800)]
        rows = [[InlineKeyboardButton(f"{'✅ ' if v == cur else ''}{lbl}", callback_data=f"ssdur:{v}") for lbl, v in opts],
                [InlineKeyboardButton("⬅️ Назад", callback_data="sess")]]
        await self._edit_anchor(update, ctx, "🗓 <b>Тривалість сесії</b>:", InlineKeyboardMarkup(rows))

    async def _sess_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> None:
        s = await self._sess_draft(ctx, owner)
        # Never stack sessions — two sessions fight over the same margin (the "not enough margin"
        # flood). If one is already running, just show it.
        active = await self._store.active_session(owner, await self._prof(owner))
        if active:
            await update.callback_query.answer("Сесія вже активна — спершу зупини її.", show_alert=True)
            await self._sess_active(update, ctx, active)
            return
        if not s["coins"]:
            await update.callback_query.answer("Обери хоча б одну монету", show_alert=True)
            return
        if not self._engine:
            await update.callback_query.answer("Рушій недоступний", show_alert=True)
            return
        sid = await self._engine.start_session(owner, await self._prof(owner), dict(s))
        await update.callback_query.answer(f"▶️ Сесію #{sid} запущено", show_alert=True)
        await self._sess_open(update, ctx, owner)

    async def _sess_stop(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int) -> None:
        # ONLY the selected profile's session. Profiles are separate accounts trading in parallel, so
        # stopping the one on screen must never wind down another account's session and flatten its
        # positions. (Stacked duplicates within one profile are still swept up: the loop stops every
        # running row of THAT profile.)
        profile = await self._prof(owner)
        stopped = 0
        if self._engine:
            for srow in await self._store.running_sessions():
                if srow.get("owner_id") == owner and (srow.get("profile") or "") == profile:
                    await self._engine.stop_session(srow["id"])
                    stopped += 1
        await update.callback_query.answer(
            f"⏹ Зупиняю сесію «{profile}», закриваю її позиції…" if stopped
            else f"На профілі «{profile}» немає активної сесії", show_alert=True)
        await self._sess_open(update, ctx, owner)

    async def _preview(self, update: Update, ctx, owner: int) -> None:
        d = ctx.chat_data.get("draft")
        pair = get_pair(d["pair"]) if d else None
        if not pair:
            await update.callback_query.edit_message_text(
                "Сесія скинулась. Почни заново.", reply_markup=self._back())
            return
        entropy_long = d.get("entropy_long")   # None = auto (cheaper venue long)
        await update.callback_query.edit_message_text("⏳ Рахую план…")
        try:
            plan, reason, eff = await asyncio.wait_for(
                self._build_plan(owner, pair, d["margin"], d["leverage"], entropy_long), timeout=25)
        except asyncio.TimeoutError:
            LOGGER.error("build_plan timed out")
            await update.callback_query.edit_message_text(
                "✗ Біржа не відповіла вчасно (таймаут). Спробуй ще раз.", reply_markup=self._back())
            return
        except Exception as err:  # noqa: BLE001 — surface it instead of a silent dead button
            LOGGER.exception("build_plan failed")
            await update.callback_query.edit_message_text(
                f"✗ Помилка розрахунку: {html.escape(str(err)[:200])}", reply_markup=self._back())
            return
        if plan is None:
            await update.callback_query.edit_message_text(f"✗ {html.escape(reason)}", reply_markup=self._back())
            return
        ctx.chat_data["plan_ready"] = True
        notional = plan.notional_usd
        e, l = plan.entropy, plan.lighter
        e_dir = "ЛОНГ (BUY)" if e.is_buy else "ШОРТ (SELL)"
        l_dir = "ШОРТ (SELL)" if l.is_ask else "ЛОНГ (BUY)"
        lowered = f" (знижено з {d['leverage']}x — макс для {pair.label})" if eff < int(d["leverage"]) else ""
        text = (
            f"👁 <b>ПЛАН — {pair.label}</b>\n"
            f"Плече {eff}x{lowered} · маржа ${d['margin']:g} → розмір <b>${notional:g}</b>/ногу\n"
            f"Ціни: {gap_text(plan)}"
            f"{' → лонг на дешевшій' if plan.auto_side else ' (напрям вручну)'}\n\n"
            f"🟩 <b>Entropy</b> {e.market} — {e_dir} {e.size:g}\n"
            f"   лімітка post-only на 2 тіки від ціни (мейкер, без тейкер-комісії)\n\n"
            f"🟦 <b>Lighter</b> {pair.lighter} — {l_dir} {l.size:g}\n"
            f"   маркет — одразу, як заповниться лімітка на Entropy\n\n"
            "<i>Не заповнилась за 90с — позиція не відкривається.</i>"
        )
        if plan.errors:
            # Errors carry a "<" ("size < min") which HTML parse_mode reads as a tag — escape them.
            text += "\n\n" + "\n".join("✗ " + html.escape(e) for e in plan.errors)
        rows = [[InlineKeyboardButton("✅ Відкрити", callback_data="confirm")]] if plan.ok else []
        rows.append([InlineKeyboardButton("⬅️ Скасувати", callback_data="menu")])
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _build_plan(self, owner: int, pair, margin: float, leverage: int, entropy_long: bool):
        """Returns (plan, reason, effective_leverage). Leverage is clamped to the lower of the two
        venues' per-coin maxima (same rule as sessions), and notional = margin × that leverage."""
        # Per-request timeout so one slow venue call can't wall the whole plan; each step is logged so
        # a stall shows up in the logs as the last line printed.
        timeout = aiohttp.ClientTimeout(total=8, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            LOGGER.info("build_plan: entropy_markets…")
            emk = (await md.entropy_markets(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
            LOGGER.info("build_plan: lighter_markets…")
            lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
            if not emk or not lmk:
                return None, "ринок не знайдено на одній із бірж", int(leverage)
            # Realtime WS mid first; REST mark only if the feed is cold/stale.
            eprice = self._feed.mid(pair.entropy) if self._feed else None
            if not eprice:
                LOGGER.info("build_plan: entropy_marks (REST)…")
                eprice = (await md.entropy_marks(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
            LOGGER.info("build_plan: lighter_mark…")
            lprice = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)
            LOGGER.info("build_plan: prices e=%s l=%s", eprice, lprice)
            if not eprice or not lprice:
                return None, "немає ціни для однієї з бірж", int(leverage)
        eff = max(1, min(int(leverage), int(emk.max_leverage or leverage), int(lmk.max_leverage or leverage)))
        plan = plan_hedge(pair, float(margin) * eff, entropy_long=entropy_long,
                          entropy_price=eprice, lighter_price=lprice,
                          entropy_market=emk, lighter_market=lmk)
        return plan, None, eff

    async def _execute(self, update: Update, ctx, owner: int) -> None:
        d = ctx.chat_data.get("draft")
        if not ctx.chat_data.get("plan_ready") or not d:
            await update.callback_query.answer("Спершу прев'ю", show_alert=True)
            return
        pair = get_pair(d["pair"])
        entropy_long = d.get("entropy_long")
        await update.callback_query.edit_message_text("⏳ Рахую план…")
        try:
            plan, reason, leverage = await self._build_plan(owner, pair, d["margin"], d["leverage"], entropy_long)
        except Exception as err:  # noqa: BLE001
            LOGGER.exception("build_plan (execute) failed")
            await update.callback_query.edit_message_text(f"✗ Помилка: {html.escape(str(err)[:200])}", reply_markup=self._back())
            return
        if plan is None or not plan.ok:
            await update.callback_query.edit_message_text(
                f"✗ {html.escape(reason or (plan.errors[0] if plan else ''))}", reply_markup=self._back())
            return
        notional = plan.notional_usd
        entropy_long = plan.entropy.is_buy   # resolved (auto side picks it from the prices)

        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        if not ent or not lit:
            await update.callback_query.edit_message_text("✗ Немає ключів для однієї з бірж.", reply_markup=self._back())
            return
        e, l = plan.entropy, plan.lighter
        with contextlib.suppress(Exception):
            await ent.set_leverage(e.market, leverage)
        with contextlib.suppress(Exception):
            await lit.set_leverage(l.market_index, leverage)
        side = "LONG" if entropy_long else "SHORT"
        if not self._engine:
            await update.callback_query.edit_message_text("✗ Движок не запущено.", reply_markup=self._back())
            return
        # Maker-first, same as sessions: Entropy post-only limit, Lighter at market once it fills.
        await update.callback_query.edit_message_text(
            f"⏳ {pair.label}: лімітка на Entropy ({side}) виставлена, чекаю заповнення…")
        fr = await self._engine.open_maker_first(ent, lit, plan, timeout=90,
                                                 reprice_s=await self._reprice_s(owner))
        self._bal_cache.pop(owner, None)  # balance changed — next menu refetches
        if fr.error or fr.unhedged > 0:
            await self._engine._cancel_hedge(ent, lit, pair, owner=owner)
            why = fr.error or "частковий філ менший за мінімум Lighter"
            await self._store.record_hedge(owner, await self._prof(owner), pair.key, notional, side,
                                           "FAILED", {"error": str(why)})
            await update.callback_query.edit_message_text(
                f"🔴 <b>{pair.label}</b> — не відкрито: {html.escape(str(why))}\nОбидві ноги закрито.",
                reply_markup=self._back(), parse_mode=ParseMode.HTML)
            return
        if fr.e_filled <= 0:
            await update.callback_query.edit_message_text(
                f"⏭ <b>{pair.label}</b> — лімітка на Entropy не заповнилась за 90с, позиції немає.",
                reply_markup=self._back(), parse_mode=ParseMode.HTML)
            return
        if not fr.complete:
            notional = round(notional * fr.e_filled / e.size, 2)
        # Stops 1% before liquidation on both legs, then a watcher that closes the other leg if one
        # of them is stopped out or liquidated.
        stops = await self._engine.place_stops(ent, lit, pair, owner=owner)
        self._engine.watch_manual(owner, ent, lit, pair, await self._prof(owner))
        await self._store.record_hedge(owner, await self._prof(owner), pair.key, notional, side, "OPEN",
                                       {"entropy_filled": fr.e_filled, "lighter_filled": fr.l_done, "stops": stops})
        part = "" if fr.complete else " (частково)"
        stop_line = html.escape(self._engine.stops_text(stops)) or "⚠️ стопи не виставлені (див. повідомлення вище)"
        await update.callback_query.edit_message_text(
            f"✅ <b>{pair.label}</b> — OPEN{part}, ${notional:g}\n"
            f"Entropy {side} (лімітка) ✓ / Lighter {'SHORT' if entropy_long else 'LONG'} (маркет) ✓\n{stop_line}",
            reply_markup=self._back(), parse_mode=ParseMode.HTML)

    # ── positions / close ──────────────────────────────────────────────────────────────────────────
    async def _positions(self, update: Update, owner: int) -> None:
        hedges = await self._store.open_hedges(owner, await self._prof(owner))
        if not hedges:
            await update.callback_query.edit_message_text("Немає відкритих хеджів.", reply_markup=self._back())
            return
        rows, lines = [], ["📂 <b>Відкриті хеджі</b>", ""]
        for h in hedges:
            pair = get_pair(h["pair_key"])
            label = pair.label if pair else h["pair_key"]
            lines.append(f"#{h['id']} {label} ${h['notional_usd']:g} Entropy {h['entropy_side']} — {h['status']}")
            rows.append([InlineKeyboardButton(f"❌ Закрити #{h['id']} {label}", callback_data=f"close:{h['id']}")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu")])
        await update.callback_query.edit_message_text(NL.join(lines), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _close(self, update: Update, owner: int, hedge_id: int) -> None:
        await update.callback_query.edit_message_text("⏳ Закриваю…")
        hedges = {h["id"]: h for h in await self._store.open_hedges(owner, await self._prof(owner))}
        h = hedges.get(hedge_id)
        if not h:
            await update.callback_query.edit_message_text("Не знайшов хедж.", reply_markup=self._back())
            return
        pair = get_pair(h["pair_key"])
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        res = {"pnl": None, "fees": None}
        if ent and lit and pair and self._engine:
            # Engine's close: maker-first (Entropy reduce-only limit, then Lighter), then a market pass
            # that flattens anything left on BOTH venues.
            res = await self._engine._close_hedge(ent, lit, pair, owner=owner, maker=True, timeout=60,
                                                  reprice_s=await self._reprice_s(owner))
        elif ent and pair:
            with contextlib.suppress(Exception):
                await ent.close_market(pair.entropy)
        # ent/lit are cached and reused — not closed here.
        await self._store.mark_hedge(hedge_id, "CLOSED")
        self._bal_cache.pop(owner, None)  # balance changed — next menu refetches
        pnl_note = f"\nPnL ≈ ${res['pnl']:g}" if res.get("pnl") is not None else ""
        await update.callback_query.edit_message_text(
            f"✅ Хедж #{hedge_id} {pair.label if pair else ''} закрито (обидві ноги).{pnl_note}",
            reply_markup=self._back(), parse_mode=ParseMode.HTML)

    # ── helpers ──────────────────────────────────────────────────────────────────────────────────
    def _back(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Меню", callback_data="menu")]])

    def _cache_get(self, owner: int, profile: str, venue: str):
        entry = self._client_cache.get((owner, profile))
        if entry and (time.monotonic() - entry["ts"]) < self.CLIENT_TTL:
            return entry.get(venue)
        return None

    def _cache_put(self, owner: int, profile: str, venue: str, client) -> None:
        entry = self._client_cache.setdefault((owner, profile), {"ts": time.monotonic()})
        entry[venue] = client
        entry["ts"] = time.monotonic()

    async def _entropy_client(self, owner: int, profile: str | None = None) -> EntropyClient | None:
        profile = profile if profile is not None else await self._prof(owner)
        cached = self._cache_get(owner, profile, "entropy")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "entropy", profile)
        if not c:
            return None
        proxy = await self._store.get_proxy(owner, profile)
        client = await asyncio.to_thread(
            EntropyClient, self._cfg.hyperliquid_api_url, c.meta["wallet_address"], c.secret,
            self._cfg.entropy_dex, proxy)
        self._cache_put(owner, profile, "entropy", client)
        return client

    async def _lighter_client(self, owner: int, profile: str | None = None) -> LighterClient | None:
        profile = profile if profile is not None else await self._prof(owner)
        cached = self._cache_get(owner, profile, "lighter")
        if cached is not None:
            return cached
        c = await self._store.get_credentials(owner, "lighter", profile)
        if not c:
            return None
        proxy = await self._store.get_proxy(owner, profile)
        # MUST build on the event loop: the Lighter SDK creates an aiohttp connector in its
        # constructor (asyncio.get_running_loop()), so a worker thread raised "no running event loop".
        # The constructor does no network, and clients are cached, so building inline is fine.
        client = LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                               int(c.meta.get("api_key_index", 0)), proxy)
        self._cache_put(owner, profile, "lighter", client)
        return client
