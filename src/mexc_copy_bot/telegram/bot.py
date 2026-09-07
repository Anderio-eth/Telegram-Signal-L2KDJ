"""Telegram control surface.

Contains no trading logic (spec §3, §34): every button calls into CopyService or Store. That
boundary is what keeps a Telegram outage from affecting copying, and lets the trading path be
reasoned about without reading UI code.

Access is a hard whitelist of Telegram user ids (COPY_BOT_ALLOWED_USER_ID, comma-separated).
Anyone not on it gets a refusal — this bot can move real money on ten accounts, so an unknown chat
must never reach a keyboard.

Everyone on the whitelist gets their OWN world: their own master, their own followers, their own
START/STOP and their own Emergency Stop. There is no shared view and no admin — the two brothers
running this see only what they added themselves. Every handler derives `owner_id` from
`update.effective_user.id` and passes it down; nothing here can address an account by id alone,
because the store requires the owner too.
"""

from __future__ import annotations

import asyncio
import logging
import time

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
from ..core.registry import ServiceRegistry
from ..db.store import FOLLOWER, MASTER, MODE_COPY, MODE_REVERSE, Account, Store
from ..mexc.rest import MexcError, MexcRestClient, get_contract_specs, get_ticker_price
from . import messages

LOGGER = logging.getLogger(__name__)

# Conversation states for adding an account.
ASK_KEY, ASK_SECRET = range(2)

# How long a balance reading stays good enough to reuse. Tapping around the menu should not fire
# ten exchange calls per screen; five seconds is under the time it takes to read the menu, so what
# you see is still effectively live, while a burst of taps costs one round of calls instead of one
# per tap.
BALANCE_TTL_SECONDS = 5.0


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
            [InlineKeyboardButton("📜 History", callback_data="history"),
             InlineKeyboardButton("⚙️ Mode", callback_data="mode")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="menu")],
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
    def __init__(self, settings: Settings, store: Store, registry: ServiceRegistry) -> None:
        self._settings = settings
        self._store = store
        self._registry = registry
        self._app: Application | None = None
        # Where to send each owner's unsolicited messages (trade reports, drift warnings). One
        # chat per owner, learned from their last interaction — reports must never land in the
        # other brother's chat.
        self._chat_ids: dict[int, int] = {}
        # owner id -> (fetched at, balances). Per owner, so one person's cached numbers can never
        # be served to the other.
        self._balance_cache: dict[int, tuple[float, dict[int, messages.Balance]]] = {}

        registry.configure_callbacks(on_report=self._report_for, on_notice=self._notice_for)

    # ── plumbing ────────────────────────────────────────────────────────────────────────────
    def build(self) -> Application:
        app = Application.builder().token(self._settings.bot_token).build()

        add_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_add, pattern="^add_(master|follower)$")],
            states={
                ASK_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_key)],
                ASK_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_secret)],
            },
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                CommandHandler("start", self._restart_from_conversation),
                # Any other button pressed mid-flow abandons the flow and does what was asked.
                # Without this the conversation is a trap: its states only accept text, so every
                # button silently falls through to a handler with no branch for it and the bot
                # appears dead until someone happens to type /cancel.
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            # Pressing "Add Master" again restarts the flow instead of being ignored because a
            # previous attempt was never finished.
            allow_reentry=True,
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

    async def _guard(self, update: Update) -> int | None:
        """Returns the owner id to act as, or None when the user is refused."""
        if self._authorized(update):
            owner_id = update.effective_user.id
            if update.effective_chat:
                self._chat_ids[owner_id] = update.effective_chat.id
            return owner_id
        LOGGER.warning("refused telegram user %s", update.effective_user.id if update.effective_user else "?")
        if update.callback_query:
            await update.callback_query.answer("Not authorized", show_alert=True)
        elif update.message:
            await update.message.reply_text("Not authorized.")
        return None

    # ── screens ─────────────────────────────────────────────────────────────────────────────
    async def _menu_text(self, owner_id: int) -> str:
        service = await self._registry.get(owner_id)
        master = await self._store.get_master(owner_id)
        followers = await self._store.list_accounts(owner_id, FOLLOWER)
        mode, reverse_id = await self._store.get_mode(owner_id)

        accounts = ([master] if master else []) + followers
        return messages.main_menu(
            running=service.running,
            master=master,
            followers=followers,
            max_followers=self._settings.max_followers,
            master_connected=service.master_connected,
            balances=await self._balances(owner_id, accounts),
            mode=mode,
            reverse_account=next((f for f in followers if f.id == reverse_id), None),
        )

    async def _balances(self, owner_id: int, accounts: list[Account]) -> dict[int, messages.Balance]:
        """Live USDT balance for each account, fetched concurrently.

        Live rather than cached: a stale number on the screen you check before pressing START is
        worse than none. Concurrently because with a master and nine followers, doing this in
        sequence would put ten round trips between a button press and the menu appearing — as one
        batch it costs about the same as the slowest single call.

        Failures are per account: one revoked key shows on its own line, and the rest of the menu
        still renders. You still need the buttons when MEXC is down.
        """
        if not accounts:
            return {}

        cached_at, cached = self._balance_cache.get(owner_id, (0.0, {}))
        # Reused only when it covers every account being shown: a follower added moments ago must
        # not sit there blank until the cache expires.
        if (
            time.monotonic() - cached_at < BALANCE_TTL_SECONDS
            and all(account.id in cached for account in accounts)
        ):
            return cached

        credentials_by_id = await self._store.get_credentials_for(owner_id)

        async with aiohttp.ClientSession() as session:
            async def fetch(account: Account) -> messages.Balance:
                credentials = credentials_by_id.get(account.id)
                if not credentials:
                    return messages.Balance(error="credentials could not be read")
                try:
                    equity, available = await MexcRestClient(*credentials, session=session).get_usdt_balance()
                    return messages.Balance(equity=equity, available=available)
                except MexcError as err:
                    return messages.Balance(error=err.message or "unavailable")
                except Exception:  # noqa: BLE001 — a network blip must not hide the menu
                    LOGGER.debug("balance lookup failed for account %s", account.id)
                    return messages.Balance(error="balance unavailable")

            results = await asyncio.gather(*(fetch(a) for a in accounts))

        balances = dict(zip((a.id for a in accounts), results))
        self._balance_cache[owner_id] = (time.monotonic(), balances)
        return balances

    def _invalidate_balances(self, owner_id: int) -> None:
        """After adding or removing an account, showing the previous line-up for five seconds
        would look like the change had not taken."""
        self._balance_cache.pop(owner_id, None)

    async def _show_menu(self, update: Update, owner_id: int) -> None:
        service = await self._registry.get(owner_id)
        text = await self._menu_text(owner_id)
        keyboard = _menu_keyboard(service.running)
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        elif update.message:
            await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

    async def _cmd_start(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        owner_id = await self._guard(update)
        if owner_id is None:
            return
        await self._show_menu(update, owner_id)

    async def _on_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        owner_id = await self._guard(update)
        if owner_id is None:
            return
        query = update.callback_query
        await query.answer()
        action = query.data or ""
        LOGGER.info("button %r from %s", action, owner_id)
        service = await self._registry.get(owner_id)

        if action == "menu":
            await self._show_menu(update, owner_id)
        elif action == "start":
            await query.edit_message_text(await service.start())
            await self._show_menu_message(owner_id)
        elif action == "stop":
            await query.edit_message_text(await service.stop())
            await self._show_menu_message(owner_id)
        elif action == "accounts":
            await self._show_accounts(update, owner_id)
        elif action == "positions":
            await self._show_positions(update, owner_id)
        elif action == "history":
            events = await self._store.recent_events(owner_id, 10)
            await query.edit_message_text(
                messages.history(events),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "emergency":
            await query.edit_message_text(
                "⚠️ <b>EMERGENCY STOP</b>\n\nThis stops copying AND closes every position on <b>your</b> "
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
            await service.stop()
            outcomes = await service.emergency_close_all()
            lines = ["🛑 <b>EMERGENCY STOP COMPLETE</b>", ""]
            lines += [
                f"{'✅' if result == 'closed' else '❌'} {account.label} — {result}" for account, result in outcomes
            ]
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "mode":
            await self._show_mode(update, owner_id)
        elif action == "mode_copy":
            if await self._refuse_while_running(update, owner_id):
                return
            await self._store.set_mode(owner_id, MODE_COPY, None)
            await self._show_mode(update, owner_id)
        elif action == "mode_reverse":
            if await self._refuse_while_running(update, owner_id):
                return
            await self._show_reverse_picker(update, owner_id)
        elif action.startswith("reverse_pick:"):
            if await self._refuse_while_running(update, owner_id):
                return
            account_id = int(action.split(":", 1)[1])
            # Verified against this owner's own followers: a stale callback must not be able to
            # point the hedge at an account that is not theirs, or no longer exists.
            followers = await self._store.list_accounts(owner_id, FOLLOWER)
            if any(f.id == account_id for f in followers):
                await self._store.set_mode(owner_id, MODE_REVERSE, account_id)
            await self._show_mode(update, owner_id)
        elif action == "remove_menu":
            await self._show_remove_menu(update, owner_id)
        elif action.startswith("remove:"):
            account_id = int(action.split(":", 1)[1])
            # Scoped delete: a callback id from someone else's keyboard simply matches no row.
            await self._store.remove_account(account_id, owner_id)
            self._invalidate_balances(owner_id)
            await self._show_accounts(update, owner_id)
        else:
            # Reached only if a keyboard offers something this method does not handle. Silence
            # here reads to the user as a dead bot, so it is logged and acknowledged instead.
            LOGGER.warning("unhandled button %r from %s", action, owner_id)
            await self._show_menu(update, owner_id)

    def _chat_for(self, owner_id: int) -> int:
        """Where to message this owner.

        Falls back to the owner id itself because a private chat with a bot has chat.id ==
        user.id. Without the fallback, everything resumed on boot (spec §31) would trade with
        no report reaching anyone until that person happened to press a button.
        """
        return self._chat_ids.get(owner_id, owner_id)

    async def _show_menu_message(self, owner_id: int) -> None:
        chat_id = self._chat_for(owner_id)
        if not (self._app and chat_id):
            return
        service = await self._registry.get(owner_id)
        await self._app.bot.send_message(
            chat_id,
            await self._menu_text(owner_id),
            reply_markup=_menu_keyboard(service.running),
            parse_mode=ParseMode.HTML,
        )

    async def _refuse_while_running(self, update: Update, owner_id: int) -> bool:
        """Mode changes are refused while copying is on.

        Switching sides mid-flight would leave whatever is already open on the wrong side of the
        master, with nothing to close it: the expected-position rows are keyed by side, so the old
        ones would simply be orphaned. Stopping first makes the change deliberate.
        """
        service = await self._registry.get(owner_id)
        if not service.running:
            return False
        await update.callback_query.answer(
            "Stop copying before changing mode — open positions would be left on the wrong side.",
            show_alert=True,
        )
        return True

    async def _show_mode(self, update: Update, owner_id: int) -> None:
        mode, reverse_id = await self._store.get_mode(owner_id)
        followers = await self._store.list_accounts(owner_id, FOLLOWER)
        chosen = next((f for f in followers if f.id == reverse_id), None)

        rows = []
        if mode != MODE_COPY:
            rows.append([InlineKeyboardButton("📋 Switch to COPY", callback_data="mode_copy")])
        rows.append(
            [InlineKeyboardButton(
                "🔁 Choose hedge account" if mode == MODE_REVERSE else "🔁 Switch to REVERSE",
                callback_data="mode_reverse",
            )]
        )
        rows.append([InlineKeyboardButton("« Back", callback_data="menu")])

        await update.callback_query.edit_message_text(
            messages.mode_screen(mode, chosen, followers),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _show_reverse_picker(self, update: Update, owner_id: int) -> None:
        followers = await self._store.list_accounts(owner_id, FOLLOWER)
        if not followers:
            await update.callback_query.edit_message_text(
                "Reverse mode needs a second account to hedge on. Add a follower first.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="mode")]]),
            )
            return
        rows = [
            [InlineKeyboardButton(
                f"🔁 {f.label} (…{f.api_key_hint})", callback_data=f"reverse_pick:{f.id}"
            )]
            for f in followers
        ]
        rows.append([InlineKeyboardButton("« Back", callback_data="mode")])
        await update.callback_query.edit_message_text(
            "Which account should take the <b>opposite</b> side of the master?",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _show_accounts(self, update: Update, owner_id: int) -> None:
        master = await self._store.get_master(owner_id)
        followers = await self._store.list_accounts(owner_id, FOLLOWER)
        await update.callback_query.edit_message_text(
            messages.accounts_list(master, followers),
            reply_markup=_accounts_keyboard(
                has_master=master is not None,
                can_add_follower=len(followers) < self._settings.max_followers,
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _show_remove_menu(self, update: Update, owner_id: int) -> None:
        accounts = await self._store.list_accounts(owner_id)
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

    async def _show_positions(self, update: Update, owner_id: int) -> None:
        lines = ["📊 <b>POSITIONS</b>", ""]
        async with aiohttp.ClientSession() as session:
            for account in await self._store.list_accounts(owner_id):
                credentials = await self._store.get_credentials(account.id, owner_id)
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
        if await self._guard(update) is None:
            return ConversationHandler.END
        # Telegram spins the button until the callback is answered; forgetting this is
        # indistinguishable from the bot being broken, even when the flow behind it works.
        await update.callback_query.answer()
        kind = MASTER if update.callback_query.data == "add_master" else FOLLOWER
        context.user_data["kind"] = kind
        prompt = await update.callback_query.edit_message_text(
            f"Adding {kind.title()} account.\n\nSend the MEXC <b>API Key</b> as a message:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖ Cancel", callback_data="menu")]]),
        )
        # Tracked so the whole exchange can be swept away once the account is connected: these
        # prompts are scaffolding, and what they were collecting now lives in the menu instead.
        context.user_data["cleanup"] = [prompt.message_id] if hasattr(prompt, "message_id") else []
        return ASK_KEY

    async def _got_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if await self._guard(update) is None:
            return ConversationHandler.END
        context.user_data["api_key"] = update.message.text.strip()
        # Delete the message so the key does not sit in chat history.
        with_suppress = getattr(update.message, "delete", None)
        if with_suppress:
            try:
                await update.message.delete()
            except Exception:  # noqa: BLE001 — deletion is best-effort, not a reason to abort
                pass
        prompt = await update.effective_chat.send_message(
            "Now send the <b>Secret Key</b>:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖ Cancel", callback_data="menu")]]),
        )
        context.user_data.setdefault("cleanup", []).append(prompt.message_id)
        return ASK_SECRET

    async def _got_secret(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
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

        master = await self._store.get_master(owner_id)
        warning = ""
        if kind == FOLLOWER and master and master.position_mode and mode != master.position_mode:
            # Hedge vs one-way changes what a side means; copying across a mismatch mirrors the
            # wrong direction. Verified the hard way during testing.
            warning = (
                f"\n\n⚠️ Position mode is {'hedge' if mode == 1 else 'one-way'}, but the master uses "
                f"{'hedge' if master.position_mode == 1 else 'one-way'}. Make them match before trading."
            )

        followers = await self._store.list_accounts(owner_id, FOLLOWER)
        label = "Master" if kind == MASTER else f"Follower #{len(followers) + 1}"
        try:
            await self._store.add_account(
                owner_id=owner_id,
                label=label,
                kind=kind,
                api_key=api_key,
                api_secret=secret,
                position_mode=mode,
            )
        except Exception as err:  # noqa: BLE001 — most likely the one-master-per-owner constraint
            await status.edit_text(f"❌ Could not save: {err}")
            context.user_data.clear()
            return ConversationHandler.END

        self._invalidate_balances(owner_id)

        # The account is connected, so the add-flow messages have served their purpose. Everything
        # they showed (balance, mode) is now on the menu, which stays put instead of scrolling off.
        cleanup = list(context.user_data.get("cleanup", []))
        cleanup.append(status.message_id)
        if warning:
            # A position-mode mismatch would silently mirror the wrong direction, so that one
            # message survives the sweep rather than being replaced by a tidy menu.
            await status.edit_text(f"✅ <b>{label} added</b>{warning}", parse_mode=ParseMode.HTML)
            cleanup.remove(status.message_id)
        await self._delete_messages(update.effective_chat.id, cleanup)

        context.user_data.clear()
        await self._show_menu_message(owner_id)
        return ConversationHandler.END

    async def _abandon_and_dispatch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """A button pressed while the add flow is waiting for a key: drop the flow, honour the
        button. The half-entered credentials are discarded rather than carried into the next
        attempt, where they would be paired with a secret meant for a different key."""
        context.user_data.clear()
        await self._on_button(update, context)
        return ConversationHandler.END

    async def _restart_from_conversation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """/start while mid-flow. Same reasoning as above: it is how someone gets unstuck."""
        context.user_data.clear()
        await self._cmd_start(update, context)
        return ConversationHandler.END

    async def _cancel_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await self._delete_messages(update.effective_chat.id, context.user_data.get("cleanup", []))
        context.user_data.clear()
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END

    async def _delete_messages(self, chat_id: int, message_ids: list[int]) -> None:
        """Best-effort tidy-up. Telegram refuses to delete messages older than 48h and returns an
        error for one already gone; neither is worth failing an otherwise successful add over."""
        if not self._app:
            return
        for message_id in message_ids:
            try:
                await self._app.bot.delete_message(chat_id, message_id)
            except Exception:  # noqa: BLE001
                LOGGER.debug("could not delete message %s", message_id)

    # ── outbound ────────────────────────────────────────────────────────────────────────────
    # The registry asks for a callback per owner, so a service physically cannot report into a
    # chat that is not its owner's.
    def _report_for(self, owner_id: int):
        async def report(event: MasterEvent, results: list[FollowerResult]) -> None:
            await self._report_event(owner_id, event, results)

        return report

    def _notice_for(self, owner_id: int):
        async def notice(text: str) -> None:
            chat_id = self._chat_for(owner_id)
            if self._app and chat_id:
                await self._app.bot.send_message(chat_id, text)

        return notice

    async def _report_event(self, owner_id: int, event: MasterEvent, results: list[FollowerResult]) -> None:
        chat_id = self._chat_for(owner_id)
        if not (self._app and chat_id):
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
            chat_id, messages.event_report(event, results, notional), parse_mode=ParseMode.HTML
        )
