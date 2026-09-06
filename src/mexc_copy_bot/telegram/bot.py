"""Telegram control surface.

Contains no trading logic (spec §3, §34): every button calls into CopyService or Store. That
boundary is what keeps a Telegram outage from affecting copying, and lets the trading path be
reasoned about without reading UI code.

Access is a hard whitelist of Telegram user ids (COPY_BOT_ALLOWED_USER_ID, comma-separated).
Anyone not on it gets a refusal — this bot can move real money on ten accounts, so an unknown chat
must never reach a keyboard. Everyone on the list has full control, including Emergency Stop.
"""

from __future__ import annotations

import logging

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from ..config import Settings
from ..core.copy_engine import FollowerResult
from ..core.events import MasterEvent
from ..core.service import CopyService
from ..db.store import FOLLOWER, MASTER, Store
from ..mexc.rest import MexcError, MexcRestClient, get_contract_specs, get_ticker_price
from . import messages

LOGGER = logging.getLogger(__name__)

# Conversation states for adding an account.
ASK_KEY, ASK_SECRET = range(2)


def _menu_keyboard(running: bool) -> InlineKeyboardMarkup:
    control = (
        InlineKeyboardButton("⏹ STOP", callback_data="stop")
        if running
        else InlineKeyboardButton("▶️ START", callback_data="start")
    )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Positions", callback_data="positions"),
             InlineKeyboardButton("👥 Accounts", callback_data="accounts")],
            [InlineKeyboardButton("📜 History", callback_data="history")],
            [control],
            [InlineKeyboardButton("🛑 EMERGENCY STOP", callback_data="emergency")],
        ]
    )


def _accounts_keyboard(has_master: bool, can_add_follower: bool) -> InlineKeyboardMarkup:
    rows = []
    if not has_master:
        rows.append([InlineKeyboardButton("👤 Add Master Account", callback_data="add_master")])
    if can_add_follower:
        rows.append([InlineKeyboardButton("➕ Add Follower Account", callback_data="add_follower")])
    rows.append([InlineKeyboardButton("🗑 Remove account", callback_data="remove_menu")])
    rows.append([InlineKeyboardButton("« Back", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


class CopyBot:
    def __init__(self, settings: Settings, store: Store, service: CopyService) -> None:
        self._settings = settings
        self._store = store
        self._service = service
        self._app: Application | None = None
        self._chat_id: int | None = None

        service.on_report = self._report_event
        service.on_notice = self._send_notice

    # ── plumbing ────────────────────────────────────────────────────────────────────────────
    def build(self) -> Application:
        app = Application.builder().token(self._settings.bot_token).build()

        add_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_add, pattern="^add_(master|follower)$")],
            states={
                ASK_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_key)],
                ASK_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_secret)],
            },
            fallbacks=[CommandHandler("cancel", self._cancel_add)],
            per_message=False,
        )

        app.add_handler(CommandHandler("start", self._cmd_start))
        app.add_handler(add_conversation)
        app.add_handler(CallbackQueryHandler(self._on_button))
        self._app = app
        return app

    def _authorized(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id in self._settings.allowed_user_ids)

    async def _guard(self, update: Update) -> bool:
        if self._authorized(update):
            if update.effective_chat:
                self._chat_id = update.effective_chat.id
            return True
        LOGGER.warning("refused telegram user %s", update.effective_user.id if update.effective_user else "?")
        if update.callback_query:
            await update.callback_query.answer("Not authorized", show_alert=True)
        elif update.message:
            await update.message.reply_text("Not authorized.")
        return False

    # ── screens ─────────────────────────────────────────────────────────────────────────────
    async def _menu_text(self) -> str:
        master = await self._store.get_master()
        followers = await self._store.list_accounts(FOLLOWER)
        return messages.main_menu(
            running=self._service.running,
            master=master,
            follower_count=len(followers),
            max_followers=self._settings.max_followers,
            master_connected=self._service.master_connected,
        )

    async def _show_menu(self, update: Update) -> None:
        text = await self._menu_text()
        keyboard = _menu_keyboard(self._service.running)
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        elif update.message:
            await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

    async def _cmd_start(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        await self._show_menu(update)

    async def _on_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._guard(update):
            return
        query = update.callback_query
        await query.answer()
        action = query.data or ""

        if action == "menu":
            await self._show_menu(update)
        elif action == "start":
            await query.edit_message_text(await self._service.start())
            await self._show_menu_message()
        elif action == "stop":
            await query.edit_message_text(await self._service.stop())
            await self._show_menu_message()
        elif action == "accounts":
            await self._show_accounts(update)
        elif action == "positions":
            await self._show_positions(update)
        elif action == "history":
            events = await self._store.recent_events(10)
            await query.edit_message_text(
                messages.history(events),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "emergency":
            await query.edit_message_text(
                "⚠️ <b>EMERGENCY STOP</b>\n\nThis stops copying AND closes every position on all "
                "follower accounts. It cannot be undone.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("YES, CLOSE ALL", callback_data="emergency_confirm")],
                        [InlineKeyboardButton("Cancel", callback_data="menu")],
                    ]
                ),
                parse_mode=ParseMode.HTML,
            )
        elif action == "emergency_confirm":
            await query.edit_message_text("Closing all follower positions…")
            await self._service.stop()
            outcomes = await self._service.emergency_close_all()
            lines = ["🛑 <b>EMERGENCY STOP COMPLETE</b>", ""]
            lines += [
                f"{'✅' if result == 'closed' else '❌'} {account.label} — {result}" for account, result in outcomes
            ]
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "remove_menu":
            await self._show_remove_menu(update)
        elif action.startswith("remove:"):
            account_id = int(action.split(":", 1)[1])
            await self._store.remove_account(account_id)
            await self._show_accounts(update)

    async def _show_menu_message(self) -> None:
        if self._app and self._chat_id:
            await self._app.bot.send_message(
                self._chat_id,
                await self._menu_text(),
                reply_markup=_menu_keyboard(self._service.running),
                parse_mode=ParseMode.HTML,
            )

    async def _show_accounts(self, update: Update) -> None:
        master = await self._store.get_master()
        followers = await self._store.list_accounts(FOLLOWER)
        await update.callback_query.edit_message_text(
            messages.accounts_list(master, followers),
            reply_markup=_accounts_keyboard(
                has_master=master is not None,
                can_add_follower=len(followers) < self._settings.max_followers,
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _show_remove_menu(self, update: Update) -> None:
        accounts = await self._store.list_accounts()
        if not accounts:
            await update.callback_query.edit_message_text(
                "No accounts.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="accounts")]])
            )
            return
        rows = [
            [InlineKeyboardButton(f"🗑 {a.label} (…{a.api_key_hint})", callback_data=f"remove:{a.id}")]
            for a in accounts
        ]
        rows.append([InlineKeyboardButton("« Back", callback_data="accounts")])
        await update.callback_query.edit_message_text(
            "Select an account to remove:", reply_markup=InlineKeyboardMarkup(rows)
        )

    async def _show_positions(self, update: Update) -> None:
        lines = ["📊 <b>POSITIONS</b>", ""]
        async with aiohttp.ClientSession() as session:
            for account in await self._store.list_accounts():
                credentials = await self._store.get_credentials(account.id)
                if not credentials:
                    continue
                client = MexcRestClient(*credentials, session=session)
                marker = "👑" if account.is_master else "•"
                try:
                    positions = [p for p in await client.get_open_positions() if p.hold_vol > 0]
                except MexcError as err:
                    lines.append(f"{marker} <b>{account.label}</b> — ❌ {err.message}")
                    continue
                if not positions:
                    lines.append(f"{marker} <b>{account.label}</b> — no position")
                else:
                    lines.append(f"{marker} <b>{account.label}</b>")
                    for p in positions:
                        lines.append(
                            f"    {p.symbol} {messages.side_name(p.position_type)} "
                            f"{p.hold_vol:g} @ {p.leverage}x"
                        )
                lines.append("")
        await update.callback_query.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu")]]),
            parse_mode=ParseMode.HTML,
        )

    # ── adding accounts ─────────────────────────────────────────────────────────────────────
    async def _begin_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._guard(update):
            return ConversationHandler.END
        kind = MASTER if update.callback_query.data == "add_master" else FOLLOWER
        context.user_data["kind"] = kind
        await update.callback_query.edit_message_text(
            f"Adding {kind.title()} account.\n\nSend the MEXC <b>API Key</b>:\n\n/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return ASK_KEY

    async def _got_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._guard(update):
            return ConversationHandler.END
        context.user_data["api_key"] = update.message.text.strip()
        # Delete the message so the key does not sit in chat history.
        with_suppress = getattr(update.message, "delete", None)
        if with_suppress:
            try:
                await update.message.delete()
            except Exception:  # noqa: BLE001 — deletion is best-effort, not a reason to abort
                pass
        await update.effective_chat.send_message("Now send the <b>Secret Key</b>:", parse_mode=ParseMode.HTML)
        return ASK_SECRET

    async def _got_secret(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self._guard(update):
            return ConversationHandler.END
        secret = update.message.text.strip()
        api_key = context.user_data.get("api_key", "")
        kind = context.user_data.get("kind", FOLLOWER)
        try:
            await update.message.delete()
        except Exception:  # noqa: BLE001
            pass

        status = await update.effective_chat.send_message("Validating with MEXC…")

        # Validate before storing: an account that cannot read its own balance will fail on the
        # first real trade, and finding that out now is far cheaper (spec §4).
        async with aiohttp.ClientSession() as session:
            client = MexcRestClient(api_key, secret, session=session)
            try:
                equity, available = await client.get_usdt_balance()
                mode = await client.get_position_mode()
            except MexcError as err:
                await status.edit_text(f"❌ Could not connect: {err.message}")
                context.user_data.clear()
                return ConversationHandler.END

        master = await self._store.get_master()
        warning = ""
        if kind == FOLLOWER and master and master.position_mode and mode != master.position_mode:
            # Hedge vs one-way changes what a side means; copying across a mismatch mirrors the
            # wrong direction. Verified the hard way during testing.
            warning = (
                f"\n\n⚠️ Position mode is {'hedge' if mode == 1 else 'one-way'}, but the master uses "
                f"{'hedge' if master.position_mode == 1 else 'one-way'}. Make them match before trading."
            )

        label = f"Master" if kind == MASTER else f"Follower #{len(await self._store.list_accounts(FOLLOWER)) + 1}"
        try:
            await self._store.add_account(
                label=label, kind=kind, api_key=api_key, api_secret=secret, position_mode=mode
            )
        except Exception as err:  # noqa: BLE001 — most likely the single-master constraint
            await status.edit_text(f"❌ Could not save: {err}")
            context.user_data.clear()
            return ConversationHandler.END

        await status.edit_text(
            f"✅ <b>{label} added</b>\n\n"
            f"Status: Connected\n"
            f"Balance: {messages.money(equity)} (available {messages.money(available)})\n"
            f"Position mode: {'hedge' if mode == 1 else 'one-way'}"
            f"{warning}",
            parse_mode=ParseMode.HTML,
        )
        context.user_data.clear()
        await self._show_menu_message()
        return ConversationHandler.END

    async def _cancel_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.clear()
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END

    # ── outbound ────────────────────────────────────────────────────────────────────────────
    async def _report_event(self, event: MasterEvent, results: list[FollowerResult]) -> None:
        if not (self._app and self._chat_id):
            return
        notional = None
        try:
            async with aiohttp.ClientSession() as session:
                specs = await get_contract_specs(session, event.symbol)
                price = await get_ticker_price(session, event.symbol)
            spec = specs.get(event.symbol)
            if spec and price:
                notional = spec.notional(event.delta_vol, price)
        except Exception:  # noqa: BLE001 — a missing price must not suppress the trade report
            LOGGER.debug("could not compute notional for %s", event.symbol)

        await self._app.bot.send_message(
            self._chat_id, messages.event_report(event, results, notional), parse_mode=ParseMode.HTML
        )

    async def _send_notice(self, text: str) -> None:
        if self._app and self._chat_id:
            await self._app.bot.send_message(self._chat_id, text)
