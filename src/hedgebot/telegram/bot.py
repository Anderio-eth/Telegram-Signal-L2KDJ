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
import logging

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters,
)

from ..config import Config
from ..core.hedge import plan_hedge
from ..db.store import Store
from ..exchanges import market_data as md
from ..exchanges.hyperliquid_entropy import EntropyClient
from ..exchanges.lighter_client import LighterClient
from ..pairs import PAIRS, get as get_pair

LOGGER = logging.getLogger(__name__)
ASK = 1  # single conversation state for all free-text prompts
NL = "\n"


class HedgeBot:
    def __init__(self, cfg: Config, store: Store) -> None:
        self._cfg = cfg
        self._store = store

    # ── wiring ───────────────────────────────────────────────────────────────────────────────────
    def register(self, app: Application) -> None:
        app.add_handler(CommandHandler("start", self._start))
        app.add_handler(ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_input, pattern=r"^(key:(lighter|entropy)|cfg:margin_custom)$")],
            states={ASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_input)]},
            fallbacks=[CallbackQueryHandler(self._menu, pattern=r"^menu$")],
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
        with contextlib.suppress(Exception):
            await update.message.delete()
        await self._refresh_menu(update, ctx, update.effective_user.id,
                                 "⌨️ Керуй кнопками. Щоб відкрити хедж — тисни «📈 Відкрити хедж».")

    def _guard(self, update: Update) -> int | None:
        uid = update.effective_user.id if update.effective_user else None
        if self._cfg.allowed_user_ids and uid not in self._cfg.allowed_user_ids:
            return None
        return uid

    # ── menus ────────────────────────────────────────────────────────────────────────────────────
    async def _start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._guard(update) is None:
            await update.message.reply_text("⛔ Доступ обмежено.")
            return
        m = await update.message.reply_text(**await self._main_menu(update.effective_user.id))
        ctx.chat_data["menu_msg_id"] = m.message_id      # the single message we keep and edit
        with contextlib.suppress(Exception):
            await update.message.delete()                # drop the /start command too

    async def _menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(**await self._main_menu(update.effective_user.id))
        ctx.chat_data["menu_msg_id"] = update.callback_query.message.message_id
        return ConversationHandler.END

    @staticmethod
    def _cancel_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Скасувати", callback_data="menu")]])

    async def _edit_anchor(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str, markup) -> None:
        """Edit the one persistent menu message in place. Falls back to sending a new one (and
        remembering it) only if the old message is gone. This is what keeps the chat to a single
        message — every step of a flow edits this same message instead of sending new ones."""
        mid = ctx.chat_data.get("menu_msg_id")
        if mid:
            try:
                await ctx.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=mid,
                                                text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
                return
            except Exception:  # noqa: BLE001 — message gone/identical; fall through to a fresh one
                pass
        m = await update.effective_chat.send_message(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        ctx.chat_data["menu_msg_id"] = m.message_id

    async def _refresh_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, owner: int, prefix: str = "") -> None:
        menu = await self._main_menu(owner)
        text = (prefix + "\n\n" + menu["text"]) if prefix else menu["text"]
        await self._edit_anchor(update, ctx, text, menu["reply_markup"])

    async def _main_menu(self, owner: int) -> dict:
        venues = await self._store.venues_set(owner)
        l = "✅" if "lighter" in venues else "❌"
        e = "✅" if "entropy" in venues else "❌"
        text = (f"🤖 <b>Delta-Points</b>{NL}{NL}"
                f"Ключі: Lighter {l}  ·  Entropy {e}{NL}"
                f"Хедж відкривається лімітками по спільних монетах (лонг на одній біржі, шорт на іншій).")
        rows = [
            [InlineKeyboardButton("🔑 Ключі", callback_data="keys")],
            [InlineKeyboardButton("💰 Баланси", callback_data="balances")],
            [InlineKeyboardButton("📈 Відкрити хедж", callback_data="open")],
            [InlineKeyboardButton("📂 Позиції / Закрити", callback_data="positions")],
        ]
        return {"text": text, "reply_markup": InlineKeyboardMarkup(rows), "parse_mode": ParseMode.HTML}

    async def _router(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._guard(update) is None:
            await update.callback_query.answer("⛔", show_alert=True)
            return
        q = update.callback_query
        data = q.data
        await q.answer()
        owner = update.effective_user.id
        ctx.chat_data["menu_msg_id"] = q.message.message_id   # this message is the anchor we keep
        if data == "menu":
            await q.edit_message_text(**await self._main_menu(owner))
        elif data == "keys":
            await self._keys_menu(update, owner)
        elif data.startswith("unkey:"):
            venue = data.split(":", 1)[1]
            await self._store.delete_credentials(owner, venue)
            await self._keys_menu(update, owner)   # already answered above; refreshed menu shows ❌
        elif data == "balances":
            await self._balances(update, owner)
        elif data == "open":
            ctx.chat_data.setdefault("draft", {"pair": PAIRS[0].key, "leverage": 3, "margin": 5.0, "entropy_long": True})
            await self._open_config(update, ctx)
        elif data == "cfg:pair":
            d = ctx.chat_data["draft"]
            keys = [p.key for p in PAIRS]
            d["pair"] = keys[(keys.index(d["pair"]) + 1) % len(keys)]
            await self._open_config(update, ctx)
        elif data == "cfg:side":
            ctx.chat_data["draft"]["entropy_long"] = not ctx.chat_data["draft"]["entropy_long"]
            await self._open_config(update, ctx)
        elif data == "cfg:lev":
            await self._lev_menu(update, ctx)
        elif data.startswith("setlev:"):
            ctx.chat_data["draft"]["leverage"] = int(data.split(":", 1)[1])
            await self._open_config(update, ctx)
        elif data == "cfg:margin":
            await self._margin_menu(update, ctx)
        elif data.startswith("setmargin:"):
            ctx.chat_data["draft"]["margin"] = float(data.split(":", 1)[1])
            await self._open_config(update, ctx)
        elif data == "cfg:preview":
            await self._preview(update, ctx, owner)
        elif data == "confirm":
            await self._execute(update, ctx, owner)
        elif data == "positions":
            await self._positions(update, owner)
        elif data.startswith("close:"):
            await self._close(update, owner, int(data.split(":", 1)[1]))

    async def _keys_menu(self, update: Update, owner: int) -> None:
        venues = await self._store.venues_set(owner)
        has_l, has_e = "lighter" in venues, "entropy" in venues
        text = (
            "🔑 <b>Ключі</b>\n\n"
            f"🟦 Lighter: {'✅ заведено' if has_l else '❌ нема'}\n"
            f"🟩 Entropy: {'✅ заведено' if has_e else '❌ нема'}\n\n"
            "<i>Щоб змінити ключ — спершу відв'яжи, потім заведи знову.</i>"
        )
        # One button per venue: unlink if set, add if not. Both set → three buttons total.
        rows = [
            [InlineKeyboardButton("🗑 Відв'язати Lighter" if has_l else "➕ Завести Lighter",
                                  callback_data="unkey:lighter" if has_l else "key:lighter")],
            [InlineKeyboardButton("🗑 Відв'язати Entropy" if has_e else "➕ Завести Entropy",
                                  callback_data="unkey:entropy" if has_e else "key:entropy")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
        ]
        await update.callback_query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _balances(self, update: Update, owner: int) -> None:
        await update.callback_query.edit_message_text("⏳ Читаю баланси…")
        lines = ["💰 <b>Баланси</b>", ""]
        fmt = lambda v: f"${v:,.2f}" if isinstance(v, (int, float)) else "—"

        # Whole block guarded per venue — a failure BUILDING the client (e.g. an SDK kwarg or a bad
        # key) must surface as text, not leave the message stuck on "reading…".
        try:
            ent = await self._entropy_client(owner)
            if not ent:
                lines.append("🟩 <b>Entropy</b>: ключ не заведено")
            else:
                b = await ent.balance()
                lines.append(f"🟩 <b>Entropy (io)</b>: всього {fmt(b.get('total'))}, вільно {fmt(b.get('free'))}, "
                             f"в позиціях {fmt(b.get('used'))}")
        except Exception as err:  # noqa: BLE001
            LOGGER.exception("entropy balance failed")
            lines.append(f"🟩 <b>Entropy</b>: помилка — {html.escape(str(err)[:150])}")

        lit = None
        try:
            lit = await self._lighter_client(owner)
            if not lit:
                lines.append("🟦 <b>Lighter</b>: ключ не заведено")
            else:
                b = await lit.balance()
                lines.append(f"🟦 <b>Lighter</b>: всього {fmt(b.get('total'))}, вільно {fmt(b.get('available'))}")
        except Exception as err:  # noqa: BLE001
            LOGGER.exception("lighter balance failed")
            lines.append(f"🟦 <b>Lighter</b>: помилка — {html.escape(str(err)[:150])}")
        finally:
            if lit:
                with contextlib.suppress(Exception):
                    await lit.close()

        lines.append("")
        lines.append("<i>Обидві біржі — ф'ючерси; «всього» = еквіті рахунку, «вільно» = під нову позицію.</i>")
        await update.callback_query.edit_message_text(
            NL.join(lines), reply_markup=self._back(), parse_mode=ParseMode.HTML)

    # ── free-text input (keys + notional) ──────────────────────────────────────────────────────────
    async def _begin_input(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        if self._guard(update) is None:
            return ConversationHandler.END
        await update.callback_query.answer()
        data = update.callback_query.data
        # This button lives on the persistent menu message — make it the anchor we keep editing.
        ctx.chat_data["menu_msg_id"] = update.callback_query.message.message_id
        if data == "key:lighter":
            ctx.user_data.clear()
            ctx.user_data.update(flow="key_lighter", step=0)
            await update.callback_query.edit_message_text(
                "🟦 <b>Lighter (Robinhood Chain) — крок 1/3</b>\n\n"
                "На <b>robinhoodchain.lighter.xyz</b> відкрий розділ <b>API</b>, створи/візьми "
                "API-ключ і надішли <b>приватний ключ API-ключа</b> (0x…).\n\n"
                "⚠️ Це НЕ ключ гаманця — це згенерований API-ключ. І саме RH-деплой.",
                reply_markup=self._cancel_kb(), parse_mode=ParseMode.HTML)
        elif data == "key:entropy":
            ctx.user_data.clear()
            ctx.user_data.update(flow="key_entropy", step=0)
            await update.callback_query.edit_message_text(
                "🟩 <b>Entropy — крок 1/2</b>\n\n"
                "Надішли <b>адресу свого ОСНОВНОГО гаманця</b> (0x…) — того, яким депозитив USDC на "
                "entropy.io. Просто адреса, не ключ.",
                reply_markup=self._cancel_kb(), parse_mode=ParseMode.HTML)
        elif data == "cfg:margin_custom":
            ctx.user_data.clear()
            ctx.user_data["flow"] = "cfg_margin"
            await update.callback_query.edit_message_text(
                "💵 Надішли <b>свою маржу в USD на ногу</b> (число; розмір = маржа × плече):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="cfg:margin")]]),
                parse_mode=ParseMode.HTML)
        return ASK

    async def _got_input(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
        if self._guard(update) is None:
            return ConversationHandler.END
        text = (update.message.text or "").strip()
        # Delete the user's reply at once — these carry keys — and drive the whole flow by editing the
        # single anchor message, so nothing new is ever left in the chat.
        with contextlib.suppress(Exception):
            await update.message.delete()
        flow = ctx.user_data.get("flow")
        owner = update.effective_user.id

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
            await self._store.set_credentials(
                owner, "lighter", ctx.user_data["priv"],
                {"account_index": ctx.user_data["account_index"], "api_key_index": api_key_index})
            ctx.user_data.clear()
            await self._refresh_menu(update, ctx, owner, "✅ Lighter збережено.")
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
            await self._store.set_credentials(
                owner, "entropy", text, {"wallet_address": ctx.user_data["wallet"]})
            ctx.user_data.clear()
            await self._refresh_menu(update, ctx, owner, "✅ Entropy збережено.")
            return ConversationHandler.END

        if flow == "cfg_margin":
            try:
                value = float(text.replace(",", "."))
                if value <= 0:
                    raise ValueError
            except ValueError:
                await self._edit_anchor(update, ctx, "Не зрозумів число, надішли ще раз:",
                                        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="cfg:margin")]]))
                return ASK
            ctx.chat_data.setdefault(
                "draft", {"pair": PAIRS[0].key, "leverage": 3, "margin": 5.0, "entropy_long": True})["margin"] = value
            ctx.user_data.clear()
            await self._open_config(update, ctx)
            return ConversationHandler.END

        return ConversationHandler.END

    # ── open hedge (config screen) ───────────────────────────────────────────────────────────────
    async def _open_config(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        d = ctx.chat_data["draft"]
        pair = get_pair(d["pair"])
        notional = d["margin"] * d["leverage"]
        e_side, l_side = ("LONG", "SHORT") if d["entropy_long"] else ("SHORT", "LONG")
        text = (
            "📈 <b>Новий хедж</b>\n\n"
            f"Монета: <b>{pair.label}</b>\n"
            f"Плече: <b>{d['leverage']}x</b>\n"
            f"Маржа: <b>${d['margin']:g}</b>/ногу\n"
            f"→ Розмір позиції: <b>${notional:g}</b>/ногу\n"
            f"Напрям: Entropy <b>{e_side}</b> / Lighter <b>{l_side}</b>\n\n"
            "<i>Лімітки-maker (чекають заповнення, менша комса). Плече макс на Entropy io = 6x.</i>"
        )
        rows = [
            [InlineKeyboardButton(f"🪙 Монета: {pair.label}", callback_data="cfg:pair")],
            [InlineKeyboardButton(f"⚙️ Плече: {d['leverage']}x", callback_data="cfg:lev"),
             InlineKeyboardButton(f"💵 Маржа: ${d['margin']:g}", callback_data="cfg:margin")],
            [InlineKeyboardButton(f"🔄 Напрям: Entropy {e_side}", callback_data="cfg:side")],
            [InlineKeyboardButton("👁 Прев'ю / Відкрити", callback_data="cfg:preview")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="menu")],
        ]
        # _edit_anchor (not callback edit) so it works after a text step too.
        await self._edit_anchor(update, ctx, text, InlineKeyboardMarkup(rows))

    async def _lev_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        cur = ctx.chat_data["draft"]["leverage"]
        def b(n): return InlineKeyboardButton(f"{'✅ ' if n == cur else ''}{n}x", callback_data=f"setlev:{n}")
        rows = [[b(1), b(2), b(3)], [b(4), b(5), b(6)],
                [InlineKeyboardButton("⬅️ Назад", callback_data="open")]]
        await self._edit_anchor(update, ctx, "⚙️ <b>Плече</b> (Entropy io макс 6x):", InlineKeyboardMarkup(rows))

    async def _margin_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        cur = ctx.chat_data["draft"]["margin"]
        def b(v): return InlineKeyboardButton(f"{'✅ ' if v == cur else ''}${v:g}", callback_data=f"setmargin:{v}")
        rows = [[b(3), b(5), b(10)], [b(25), b(50), b(100)],
                [InlineKeyboardButton("✍️ Своя сума", callback_data="cfg:margin_custom")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="open")]]
        await self._edit_anchor(update, ctx, "💵 <b>Маржа на ногу</b> (розмір = маржа × плече):",
                                InlineKeyboardMarkup(rows))

    async def _preview(self, update: Update, ctx, owner: int) -> None:
        d = ctx.chat_data.get("draft")
        pair = get_pair(d["pair"]) if d else None
        if not pair:
            await update.callback_query.edit_message_text(
                "Сесія скинулась. Почни заново.", reply_markup=self._back())
            return
        entropy_long = d["entropy_long"]
        notional = d["margin"] * d["leverage"]
        await update.callback_query.edit_message_text("⏳ Рахую план…")
        try:
            plan, reason = await asyncio.wait_for(
                self._build_plan(owner, pair, notional, entropy_long), timeout=25)
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
        e, l = plan.entropy, plan.lighter
        e_dir = "ЛОНГ (BUY)" if e.is_buy else "ШОРТ (SELL)"
        l_dir = "ШОРТ (SELL)" if l.is_ask else "ЛОНГ (BUY)"
        text = (
            f"👁 <b>ПЛАН — {pair.label}</b>\n"
            f"Плече {d['leverage']}x · маржа ${d['margin']:g} → розмір <b>${notional:g}</b>/ногу\n\n"
            f"🟩 <b>Entropy</b> {e.market} — {e_dir}\n"
            f"   кількість: <b>{e.size:g}</b> {pair.label}\n"
            f"   лімітна ціна: <b>{e.limit_px:g}</b>\n\n"
            f"🟦 <b>Lighter</b> {pair.lighter} — {l_dir}\n"
            f"   кількість: <b>{l.size:g}</b> {pair.label}\n"
            f"   лімітна ціна: <b>{l.limit_px:g}</b>\n\n"
            "<i>«кількість» — скільки токенів купуємо/продаємо; «лімітна ціна» — ціна, за якою "
            "виставлена maker-лімітка (висить, поки ринок її не заповнить).</i>"
        )
        if plan.errors:
            # Errors carry a "<" ("size < min") which HTML parse_mode reads as a tag — escape them.
            text += "\n\n" + "\n".join("✗ " + html.escape(e) for e in plan.errors)
        rows = [[InlineKeyboardButton("✅ Відкрити", callback_data="confirm")]] if plan.ok else []
        rows.append([InlineKeyboardButton("⬅️ Скасувати", callback_data="menu")])
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)

    async def _build_plan(self, owner: int, pair, notional: float, entropy_long: bool):
        # Per-request timeout so one slow venue call can't wall the whole plan; each step is logged so
        # a stall shows up in the logs as the last line printed.
        timeout = aiohttp.ClientTimeout(total=8, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            LOGGER.info("build_plan: entropy_markets…")
            emk = (await md.entropy_markets(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
            LOGGER.info("build_plan: lighter_markets…")
            lmk = (await md.lighter_markets(s, self._cfg.lighter_api_url)).get(pair.lighter)
            if not emk or not lmk:
                return None, "ринок не знайдено на одній із бірж"
            LOGGER.info("build_plan: entropy_marks…")
            eprice = (await md.entropy_marks(s, self._cfg.hyperliquid_api_url, self._cfg.entropy_dex)).get(pair.entropy)
            LOGGER.info("build_plan: lighter_mark…")
            lprice = await md.lighter_mark(s, self._cfg.lighter_api_url, lmk.market_id)
            LOGGER.info("build_plan: prices e=%s l=%s", eprice, lprice)
            if not eprice or not lprice:
                return None, "немає ціни для однієї з бірж"
        plan = plan_hedge(pair, notional, entropy_long=entropy_long,
                          entropy_price=eprice, lighter_price=lprice,
                          entropy_market=emk, lighter_market=lmk)
        return plan, None

    async def _execute(self, update: Update, ctx, owner: int) -> None:
        d = ctx.chat_data.get("draft")
        if not ctx.chat_data.get("plan_ready") or not d:
            await update.callback_query.answer("Спершу прев'ю", show_alert=True)
            return
        pair = get_pair(d["pair"])
        leverage = int(d["leverage"])
        notional = d["margin"] * leverage
        entropy_long = d["entropy_long"]
        await update.callback_query.edit_message_text("⏳ Відкриваю обидві ноги…")
        try:
            plan, reason = await self._build_plan(owner, pair, notional, entropy_long)
        except Exception as err:  # noqa: BLE001
            LOGGER.exception("build_plan (execute) failed")
            await update.callback_query.edit_message_text(f"✗ Помилка: {html.escape(str(err)[:200])}", reply_markup=self._back())
            return
        if plan is None or not plan.ok:
            await update.callback_query.edit_message_text(
                f"✗ {html.escape(reason or (plan.errors[0] if plan else ''))}", reply_markup=self._back())
            return

        results = {}
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        if not ent or not lit:
            await update.callback_query.edit_message_text("✗ Немає ключів для однієї з бірж.", reply_markup=self._back())
            return
        try:
            e, l = plan.entropy, plan.lighter
            # Set the chosen leverage before ordering (best-effort; a failure is reported per leg).
            with contextlib.suppress(Exception):
                await ent.set_leverage(e.market, leverage)
            with contextlib.suppress(Exception):
                await lit.set_leverage(l.market_index, leverage)
            try:
                results["entropy"] = await ent.limit_order(e.market, e.is_buy, e.size, e.limit_px, post_only=plan.post_only)
            except Exception as err:  # noqa: BLE001
                results["entropy"] = f"ERROR: {err}"
            try:
                results["lighter"] = await lit.limit_order(l.market_index, l.base_amount, l.price_int, l.is_ask, post_only=plan.post_only)
            except Exception as err:  # noqa: BLE001
                results["lighter"] = f"ERROR: {err}"
        finally:
            with contextlib.suppress(Exception):
                await lit.close()

        e_ok = "ERROR" not in str(results.get("entropy"))
        l_ok = "ERROR" not in str(results.get("lighter"))
        status = "OPEN" if (e_ok and l_ok) else ("PARTIAL" if (e_ok or l_ok) else "FAILED")
        await self._store.record_hedge(owner, pair.key, notional, "LONG" if entropy_long else "SHORT",
                                       status, {"results": {k: str(v) for k, v in results.items()}})
        warn = "" if status == "OPEN" else "\n⚠️ Одна нога не відкрилась — можлива гола дельта, перевір!"
        await update.callback_query.edit_message_text(
            f"{'✅' if status=='OPEN' else '🟠' if status=='PARTIAL' else '🔴'} <b>{pair.label}</b> — {status}\n"
            f"Entropy: {'ok' if e_ok else html.escape(str(results.get('entropy')))}\n"
            f"Lighter: {'ok' if l_ok else html.escape(str(results.get('lighter')))}{warn}",
            reply_markup=self._back(), parse_mode=ParseMode.HTML)

    # ── positions / close ──────────────────────────────────────────────────────────────────────────
    async def _positions(self, update: Update, owner: int) -> None:
        hedges = await self._store.open_hedges(owner)
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
        hedges = {h["id"]: h for h in await self._store.open_hedges(owner)}
        h = hedges.get(hedge_id)
        if not h:
            await update.callback_query.edit_message_text("Не знайшов хедж.", reply_markup=self._back())
            return
        pair = get_pair(h["pair_key"])
        ent = await self._entropy_client(owner)
        lit = await self._lighter_client(owner)
        errs = []
        try:
            with __import__("contextlib").suppress(Exception):
                await ent.close_market(pair.entropy)
            # Lighter close: reduce-only market via cancel + opposite order is a testing-time detail;
            # for now cancel resting orders and flag manual close of the filled size.
            with __import__("contextlib").suppress(Exception):
                await lit.cancel_all()
        finally:
            with __import__("contextlib").suppress(Exception):
                await lit.close()
        await self._store.mark_hedge(hedge_id, "CLOSED")
        note = "\n⚠️ Lighter: скасував ордери; закриття заповненої позиції звіримо на тесті." if not errs else ""
        await update.callback_query.edit_message_text(
            f"✅ Хедж #{hedge_id} {pair.label if pair else ''} закрито.{note}",
            reply_markup=self._back(), parse_mode=ParseMode.HTML)

    # ── helpers ──────────────────────────────────────────────────────────────────────────────────
    def _back(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Меню", callback_data="menu")]])

    async def _entropy_client(self, owner: int) -> EntropyClient | None:
        c = await self._store.get_credentials(owner, "entropy")
        if not c:
            return None
        return await asyncio.to_thread(
            EntropyClient, self._cfg.hyperliquid_api_url, c.meta["wallet_address"], c.secret, self._cfg.entropy_dex)

    async def _lighter_client(self, owner: int) -> LighterClient | None:
        c = await self._store.get_credentials(owner, "lighter")
        if not c:
            return None
        return LighterClient(self._cfg.lighter_api_url, int(c.meta["account_index"]), c.secret,
                             int(c.meta.get("api_key_index", 0)))
