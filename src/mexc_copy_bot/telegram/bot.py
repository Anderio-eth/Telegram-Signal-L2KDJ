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
from telegram.error import BadRequest
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
from ..core.stuck import StuckManager
from ..db.store import (
    DIRECTION_COPY,
    DIRECTION_REVERSE,
    FOLLOWER,
    MASTER,
    MODE_COPY,
    MODE_REVERSE,
    Account,
    Store,
)
from ..mexc.rest import MexcError, MexcRestClient, get_contract_specs, get_ticker_price
from . import messages
from .i18n import EN, UK, t

LOGGER = logging.getLogger(__name__)

# Conversation states for adding an account.
ASK_KEY, ASK_SECRET = range(2)

# Its own conversation: moving a limit asks for a number, and it must not be confused with the
# key/secret flow, which is waiting for text of a completely different kind.
ASK_PRICE = 100
ASK_FOLDER_NAME = 101

# Written out rather than inlined: patching this file has twice turned an escaped newline into
# a real one, which is a syntax error that only shows up at import time.
NEWLINE = chr(10)

# How long a balance reading stays good enough to reuse. Tapping around the menu should not fire
# ten exchange calls per screen; five seconds is under the time it takes to read the menu, so what
# you see is still effectively live, while a burst of taps costs one round of calls instead of one
# per tap.
BALANCE_TTL_SECONDS = 5.0


def _menu_keyboard(running: bool, lang: str, stuck: int = 0, folder: str = "") -> InlineKeyboardMarkup:
    control = (
        InlineKeyboardButton(t(lang, "btn_stop"), callback_data="stop")
        if running
        else InlineKeyboardButton(t(lang, "btn_start"), callback_data="start")
    )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t(lang, "btn_positions"), callback_data="positions"),
             InlineKeyboardButton(t(lang, "btn_accounts"), callback_data="accounts")],
            [InlineKeyboardButton(t(lang, "btn_history"), callback_data="history"),
             InlineKeyboardButton(t(lang, "btn_mode"), callback_data="mode")],
            [InlineKeyboardButton(t(lang, "btn_folder", name=folder), callback_data="folders")],
            [InlineKeyboardButton(t(lang, "btn_refresh"), callback_data="menu"),
             InlineKeyboardButton(t(lang, "btn_lang"), callback_data="lang")],
            [control],
            [InlineKeyboardButton(t(lang, "btn_emergency"), callback_data="emergency")],
        ]
        # Shown only when it means something. A permanent "Stuck (0)" is noise that teaches
        # people to stop reading the row it sits in.
        + ([[InlineKeyboardButton(t(lang, "btn_stuck", n=stuck), callback_data="stuck")]] if stuck else [])
    )


def _accounts_keyboard(has_master: bool, can_add_follower: bool, lang: str) -> InlineKeyboardMarkup:
    rows = []
    if not has_master:
        rows.append([InlineKeyboardButton(t(lang, "btn_add_master"), callback_data="add_master")])
    else:
        rows.append([InlineKeyboardButton(t(lang, "btn_promote"), callback_data="promote")])
        rows.append([InlineKeyboardButton(t(lang, "btn_change_master"), callback_data="change_master")])
    if can_add_follower:
        rows.append([InlineKeyboardButton(t(lang, "btn_add_follower"), callback_data="add_follower")])
    rows.append([InlineKeyboardButton(t(lang, "btn_remove"), callback_data="remove_menu")])
    rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu")])
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
        # Language per owner, read once and kept: it is needed by every screen and every
        # notice, and a database round trip per line of text would be absurd.
        self._lang_cache: dict[int, str] = {}
        # Which folder each owner is looking at. Resolved in _guard, so every handler can
        # reach it without another round trip — the alternative was threading a folder id
        # through every screen by hand, which is the sort of change one place gets forgotten
        # in, and a forgotten one mixes two setups' accounts together.
        self._folder_cache: dict[int, int] = {}
        # Sessions opened for stuck-account work while copying is stopped, closed on shutdown.
        self._own_sessions: list[aiohttp.ClientSession] = []

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

        price_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_price, pattern=r"^sme:\d+$")],
            states={ASK_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_price)]},
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            allow_reentry=True,
            per_message=False,
        )

        folder_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_folder_name, pattern="^f(new|ren)$")],
            states={
                ASK_FOLDER_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._got_folder_name)
                ]
            },
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            allow_reentry=True,
            per_message=False,
        )

        app.add_handler(CommandHandler("start", self._cmd_start))
        app.add_handler(add_conversation)
        app.add_handler(price_conversation)
        app.add_handler(folder_conversation)
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
            await self._resolve_folder(owner_id)
            return owner_id
        LOGGER.warning("refused telegram user %s", update.effective_user.id if update.effective_user else "?")
        if update.callback_query:
            await update.callback_query.answer(t(UK, "not_authorized"), show_alert=True)
        elif update.message:
            await update.message.reply_text(t(UK, "not_authorized") + ".")
        return None

    async def _resolve_folder(self, owner_id: int) -> int:
        """The folder this owner is working in, creating their first one if they have none.

        A brand new owner has never chosen a folder and has none to choose. Making one beats
        showing an empty screen that reads as though their accounts had gone missing.
        """
        folder_id = await self._store.active_folder_id(owner_id)
        if folder_id is None:
            folder_id = await self._store.create_folder(owner_id, "MEXC")
            await self._store.set_active_folder(owner_id, folder_id)
            LOGGER.info("created first folder %s for owner %s", folder_id, owner_id)
        self._folder_cache[owner_id] = folder_id
        return folder_id

    def _folder(self, owner_id: int) -> int:
        """The cached folder. Warm by the time any handler runs, because _guard fills it."""
        return self._folder_cache[owner_id]

    def _lang_now(self, owner_id: int) -> str:
        """The cached language. Every handler calls _lang() early, so this is warm by the time a
        keyboard is built; the default only applies before anyone has interacted."""
        return self._lang_cache.get(owner_id, UK)

    async def _lang(self, owner_id: int) -> str:
        cached = self._lang_cache.get(owner_id)
        if cached is None:
            cached = await self._store.get_language(owner_id)
            self._lang_cache[owner_id] = cached
        return cached

    # ── screens ─────────────────────────────────────────────────────────────────────────────
    async def _menu_text(self, owner_id: int) -> str:
        service = await self._registry.get(self._folder(owner_id), owner_id)
        master = await self._store.get_master(self._folder(owner_id))
        followers = await self._store.list_accounts(self._folder(owner_id), FOLLOWER)
        mode, reverse_id = await self._store.get_mode(self._folder(owner_id))
        lang = await self._lang(owner_id)

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
            lang=lang,
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

        credentials_by_id = await self._store.get_credentials_for(self._folder(owner_id))

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
        service = await self._registry.get(self._folder(owner_id), owner_id)
        text = await self._menu_text(owner_id)
        keyboard = _menu_keyboard(
            service.running,
            await self._lang(owner_id),
            await self._stuck_count(owner_id),
            await self._folder_name(owner_id),
        )
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
        service = await self._registry.get(self._folder(owner_id), owner_id)

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
                messages.history(events, await self._lang(owner_id)),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "emergency":
            await query.edit_message_text(
                t(await self._lang(owner_id), "emergency_confirm"),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(t(await self._lang(owner_id), "btn_close_all"),
                                              callback_data="emergency_confirm")],
                        [InlineKeyboardButton(t(await self._lang(owner_id), "btn_cancel"),
                                              callback_data="menu")],
                    ]
                ),
                parse_mode=ParseMode.HTML,
            )
        elif action == "emergency_confirm":
            await query.edit_message_text(t(await self._lang(owner_id), "emergency_working"))
            await service.stop()
            outcomes = await service.emergency_close_all()
            lines = [t(await self._lang(owner_id), "emergency_done"), ""]
            lines += [
                f"{'✅' if result == 'closed' else '❌'} {account.label} — {result}" for account, result in outcomes
            ]
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="menu")]]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "promote":
            if await self._refuse_while_running(update, owner_id):
                return
            await self._show_promote(update, owner_id)
        elif action.startswith("promote:"):
            if await self._refuse_while_running(update, owner_id):
                return
            await self._do_promote(update, owner_id, int(action.split(":", 1)[1]))
        elif action == "change_master":
            if await self._refuse_while_running(update, owner_id):
                return
            master = await self._store.get_master(self._folder(owner_id))
            if not master:
                await self._show_accounts(update, owner_id)
                return
            await update.callback_query.edit_message_text(
                t(lang_now := await self._lang(owner_id), "change_master_warn", hint=master.api_key_hint),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(t(lang_now, "btn_confirm_change"), callback_data="add_master")],
                    [InlineKeyboardButton(t(lang_now, "btn_back"), callback_data="accounts")],
                ]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "folders":
            await self._show_folders(update, owner_id)
        elif action.startswith("fsel:"):
            folder_id = int(action.split(":", 1)[1])
            folder = await self._store.get_folder(folder_id, owner_id)
            if folder:
                await self._store.set_active_folder(owner_id, folder_id)
                self._folder_cache[owner_id] = folder_id
                self._invalidate_balances(owner_id)
                await update.callback_query.answer(
                    t(await self._lang(owner_id), "folder_switched", name=folder.name).replace("<b>", "").replace("</b>", "")
                )
            await self._show_menu(update, owner_id)
        elif action == "fdel":
            await self._confirm_delete_folder(update, owner_id)
        elif action == "fdel_ok":
            await self._delete_folder(update, owner_id)
        elif action == "stuck":
            await self._show_stuck(update, owner_id)
        elif action.startswith("sg:"):
            _, gid, what = action.split(":")
            context.user_data["picked"] = None
            await self._show_picker(update, owner_id, int(gid), what, context)
        elif action.startswith("sp:"):
            _, gid, what, acc = action.split(":")
            picked = context.user_data.get("picked")
            group = await self._store.get_stuck_group(owner_id, int(gid))
            if group:
                if picked is None:
                    picked = {m.account_id for m in group.members}
                picked = set(picked)
                picked.symmetric_difference_update({int(acc)})
                context.user_data["picked"] = picked
            await self._show_picker(update, owner_id, int(gid), what, context)
        elif action.startswith("sx:"):
            _, gid, what = action.split(":")
            await self._run_stuck_action(update, owner_id, int(gid), what, context)
        elif action.startswith("sm:"):
            await self._show_move_limit(update, owner_id, int(action.split(":")[1]))
        elif action == "lang":
            current = await self._lang(owner_id)
            new = EN if current == UK else UK
            await self._store.set_language(owner_id, new)
            self._lang_cache[owner_id] = new
            await self._show_menu(update, owner_id)
        elif action == "mode":
            await self._show_mode(update, owner_id)
        elif action == "mode_copy":
            if await self._refuse_while_running(update, owner_id):
                return
            await self._store.set_mode(self._folder(owner_id), MODE_COPY, None)
            await self._show_mode(update, owner_id)
        elif action.startswith("dir:"):
            if await self._refuse_while_running(update, owner_id):
                return
            account_id = int(action.split(":", 1)[1])
            folder_id = self._folder(owner_id)
            current = {a.id: a for a in await self._store.list_accounts(folder_id, FOLLOWER)}
            account = current.get(account_id)
            if account:
                await self._store.set_direction(
                    account_id,
                    folder_id,
                    DIRECTION_COPY if account.is_reversed else DIRECTION_REVERSE,
                )
            await self._show_mode(update, owner_id)
        elif action == "mode_reverse":
            if await self._refuse_while_running(update, owner_id):
                return
            await self._store.set_mode(self._folder(owner_id), MODE_REVERSE, None)
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
        service = await self._registry.get(self._folder(owner_id), owner_id)
        await self._app.bot.send_message(
            chat_id,
            await self._menu_text(owner_id),
            reply_markup=_menu_keyboard(
                service.running,
                await self._lang(owner_id),
                await self._stuck_count(owner_id),
                await self._folder_name(owner_id),
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _refuse_while_running(self, update: Update, owner_id: int) -> bool:
        """Mode changes are refused while copying is on.

        Switching sides mid-flight would leave whatever is already open on the wrong side of the
        master, with nothing to close it: the expected-position rows are keyed by side, so the old
        ones would simply be orphaned. Stopping first makes the change deliberate.
        """
        service = await self._registry.get(self._folder(owner_id), owner_id)
        if not service.running:
            return False
        await update.callback_query.answer(
            t(await self._lang(owner_id), "stop_before_mode"),
            show_alert=True,
        )
        return True

    async def _show_mode(self, update: Update, owner_id: int) -> None:
        mode, reverse_id = await self._store.get_mode(self._folder(owner_id))
        followers = await self._store.list_accounts(self._folder(owner_id), FOLLOWER)
        chosen = next((f for f in followers if f.id == reverse_id), None)
        lang = await self._lang(owner_id)
        rows = []
        if mode == MODE_REVERSE:
            # One button per account, showing what it does now — tapping flips it.
            for follower in followers:
                arrow = t(lang, "btn_dir_reverse") if follower.is_reversed else t(lang, "btn_dir_copy")
                rows.append([InlineKeyboardButton(
                    f"{follower.label} — {arrow}", callback_data=f"dir:{follower.id}"
                )])
            rows.append([InlineKeyboardButton(t(lang, "btn_to_copy"), callback_data="mode_copy")])
        else:
            # Only offered when it would change something. In REVERSE the accounts are already
            # listed above, and a button that re-selects the mode you are in is a dead tap.
            rows.append(
                [InlineKeyboardButton(t(lang, "btn_to_reverse"), callback_data="mode_reverse")]
            )
        rows.append([InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="menu")])

        await update.callback_query.edit_message_text(
            messages.mode_screen(mode, chosen, followers, lang),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _show_promote(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        master = await self._store.get_master(self._folder(owner_id))
        followers = await self._store.list_accounts(self._folder(owner_id), FOLLOWER)
        if not master or not followers:
            await self._show_accounts(update, owner_id)
            return
        rows = [
            [InlineKeyboardButton(
                f"⬆️ {f.label} (…{f.api_key_hint})", callback_data=f"promote:{f.id}"
            )]
            for f in followers
        ]
        rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="accounts")])
        await update.callback_query.edit_message_text(
            t(lang, "promote_pick", hint=master.api_key_hint),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _do_promote(self, update: Update, owner_id: int, account_id: int) -> None:
        lang = await self._lang(owner_id)
        followers = await self._store.list_accounts(self._folder(owner_id), FOLLOWER)
        chosen = next((f for f in followers if f.id == account_id), None)
        # Checked against this owner's own followers: a stale callback must not be able to hand
        # the master role to an account that is not theirs, or no longer exists.
        if not chosen or not await self._store.promote_follower(self._folder(owner_id), account_id):
            await self._show_accounts(update, owner_id)
            return

        # The service is holding the previous master's socket and position baseline, both of which
        # now describe a follower. START must build them again from the new master.
        service = await self._registry.get(self._folder(owner_id), owner_id)
        if service.running:
            await service.stop()
        self._invalidate_balances(owner_id)

        await update.callback_query.edit_message_text(
            t(lang, "promoted", new=chosen.label),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(lang, "btn_back"), callback_data="accounts")]]),
            parse_mode=ParseMode.HTML,
        )

    async def _folder_name(self, owner_id: int) -> str:
        folder = await self._store.get_folder(self._folder(owner_id), owner_id)
        return folder.name if folder else "—"

    async def _show_folders(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        folders = await self._store.list_folders(owner_id)
        active = self._folder(owner_id)
        counts = {f.id: len(await self._store.list_accounts(f.id)) for f in folders}

        rows = [
            [InlineKeyboardButton(
                f"{'▶️' if f.id == active else '📁'} {f.name}", callback_data=f"fsel:{f.id}"
            )]
            for f in folders
        ]
        rows.append([
            InlineKeyboardButton(t(lang, "btn_new_folder"), callback_data="fnew"),
            InlineKeyboardButton(t(lang, "btn_rename_folder"), callback_data="fren"),
        ])
        rows.append([InlineKeyboardButton(t(lang, "btn_delete_folder"), callback_data="fdel")])
        rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu")])

        await update.callback_query.edit_message_text(
            messages.folders_screen(folders, active, counts, lang),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _confirm_delete_folder(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        folders = await self._store.list_folders(owner_id)
        if len(folders) < 2:
            # Deleting the only folder would leave nothing to switch to and nowhere to add an
            # account, so the bot would have to invent one on the next tap anyway.
            await update.callback_query.answer(t(lang, "folder_last_one"), show_alert=True)
            return
        folder_id = self._folder(owner_id)
        service = await self._registry.get(folder_id, owner_id)
        if service.running:
            await update.callback_query.answer(t(lang, "folder_stop_first"), show_alert=True)
            return
        folder = await self._store.get_folder(folder_id, owner_id)
        accounts = len(await self._store.list_accounts(folder_id))
        await update.callback_query.edit_message_text(
            t(lang, "folder_delete_warn", name=folder.name, accounts=accounts),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(t(lang, "btn_confirm_delete"), callback_data="fdel_ok")],
                [InlineKeyboardButton(t(lang, "btn_back"), callback_data="folders")],
            ]),
            parse_mode=ParseMode.HTML,
        )

    async def _delete_folder(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        folder_id = self._folder(owner_id)
        await self._store.delete_folder(folder_id, owner_id)
        # Point the owner at whatever is left before anything tries to read the deleted one.
        remaining = await self._store.list_folders(owner_id)
        if remaining:
            await self._store.set_active_folder(owner_id, remaining[0].id)
        await self._resolve_folder(owner_id)
        self._invalidate_balances(owner_id)
        await update.callback_query.edit_message_text(
            t(lang, "folder_deleted"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(lang, "btn_back"), callback_data="folders")]]),
            parse_mode=ParseMode.HTML,
        )

    async def _begin_folder_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        await update.callback_query.answer()
        context.user_data["folder_action"] = update.callback_query.data  # fnew or fren
        await update.callback_query.edit_message_text(
            t(await self._lang(owner_id), "folder_name_ask"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(self._lang_now(owner_id), "btn_cancel_x"), callback_data="folders")]]),
        )
        return ASK_FOLDER_NAME

    async def _got_folder_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        lang = await self._lang(owner_id)
        name = (update.message.text or "").strip()[:40]
        if not name:
            await update.message.reply_text(t(lang, "folder_name_ask"))
            return ASK_FOLDER_NAME

        action = context.user_data.pop("folder_action", "fnew")
        if action == "fren":
            await self._store.rename_folder(self._folder(owner_id), owner_id, name)
            text = t(lang, "folder_renamed", name=name)
        else:
            folder_id = await self._store.create_folder(owner_id, name)
            # A new folder is empty, so switching to it immediately is what anyone creating one
            # was about to do anyway.
            await self._store.set_active_folder(owner_id, folder_id)
            self._folder_cache[owner_id] = folder_id
            self._invalidate_balances(owner_id)
            text = t(lang, "folder_created", name=name)

        await update.effective_chat.send_message(
            text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(lang, "btn_back"), callback_data="folders")]]),
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    async def _stuck_count(self, owner_id: int) -> int:
        return sum(len(g.members) for g in await self._store.list_stuck_groups(owner_id))

    async def _show_stuck(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        groups = await self._store.list_stuck_groups(owner_id)
        rows = []
        for group in groups:
            if group.is_entry:
                rows.append([
                    InlineKeyboardButton(t(lang, "btn_enter_market"), callback_data=f"sg:{group.id}:enter"),
                    InlineKeyboardButton(t(lang, "btn_drop_entry"), callback_data=f"sg:{group.id}:drop"),
                ])
            else:
                rows.append([
                    InlineKeyboardButton(t(lang, "btn_close_market"), callback_data=f"sg:{group.id}:close"),
                ])
            rows.append([InlineKeyboardButton(t(lang, "btn_move_limit"), callback_data=f"sm:{group.id}")])
        rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu")])
        await update.callback_query.edit_message_text(
            messages.stuck_screen(groups, lang),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _show_picker(
        self, update: Update, owner_id: int, group_id: int, what: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Tick-list of the group's accounts. Everyone is ticked to begin with, because acting on
        all of them is the common case and un-ticking is the exception."""
        lang = await self._lang(owner_id)
        group = await self._store.get_stuck_group(owner_id, group_id)
        if not group:
            await self._show_stuck(update, owner_id)
            return
        picked = self._picked(context, group)

        rows = [
            [InlineKeyboardButton(
                f"{'☑️' if m.account_id in picked else '⬜️'} {m.label} — {m.vol:g}",
                callback_data=f"sp:{group_id}:{what}:{m.account_id}",
            )]
            for m in group.members
        ]
        rows.append([InlineKeyboardButton(
            t(lang, "btn_do_it", n=len(picked)), callback_data=f"sx:{group_id}:{what}"
        )])
        rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="stuck")])
        await update.callback_query.edit_message_text(
            t(lang, "pick_accounts"), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML
        )

    @staticmethod
    def _picked(context: ContextTypes.DEFAULT_TYPE, group) -> set[int]:
        """Who is currently ticked. Absent means "nobody has touched it", which is everyone."""
        picked = context.user_data.get("picked")
        if picked is None:
            return {m.account_id for m in group.members}
        return set(picked)

    async def _stuck_manager(self, owner_id: int) -> StuckManager | None:
        """Actions on stranded accounts need an HTTP session. Borrow the running service's when
        there is one; otherwise these buttons must still work while copying is stopped, which is
        exactly when someone is most likely to be sorting a mess out."""
        service = await self._registry.get(self._folder(owner_id), owner_id)
        session = getattr(service, "_session", None)
        if session is None or session.closed:
            session = aiohttp.ClientSession()
            self._own_sessions.append(session)
        return StuckManager(self._store, session, owner_id)

    async def _run_stuck_action(
        self, update: Update, owner_id: int, group_id: int, what: str,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        lang = await self._lang(owner_id)
        group = await self._store.get_stuck_group(owner_id, group_id)
        if not group:
            await self._show_stuck(update, owner_id)
            return
        picked = sorted(self._picked(context, group))
        if not picked:
            await update.callback_query.answer(t(lang, "nothing_selected"), show_alert=True)
            return

        await update.callback_query.edit_message_text(t(lang, "emergency_working"))
        manager = await self._stuck_manager(owner_id)
        if what == "close":
            outcomes = await manager.close_at_market(group, picked)
        elif what == "enter":
            outcomes = await manager.enter_at_market(group, picked)
        else:
            outcomes = await manager.cancel_entry(group, picked)

        context.user_data.pop("picked", None)
        lines = [t(lang, "done_title"), ""]
        lines += [f"{label} — {outcome}" for label, outcome in outcomes]
        lines += ["", t(lang, "back_under_master")]
        await update.callback_query.edit_message_text(
            NEWLINE.join(lines),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(lang, "btn_back"), callback_data="stuck")]]),
            parse_mode=ParseMode.HTML,
        )

    async def _show_move_limit(self, update: Update, owner_id: int, group_id: int) -> None:
        lang = await self._lang(owner_id)
        group = await self._store.get_stuck_group(owner_id, group_id)
        if not group:
            await self._show_stuck(update, owner_id)
            return

        market = None
        try:
            async with aiohttp.ClientSession() as session:
                market = await get_ticker_price(session, group.symbol)
        except Exception:  # noqa: BLE001 — a missing quote must not block moving the order
            LOGGER.debug("no ticker for %s", group.symbol)

        lines = [
            t(lang, "move_limit_title"),
            "",
            f"<b>{group.symbol}</b> {messages.side_name(group.position_type)} · "
            f"{len(group.members)}",
            "",
            t(lang, "move_limit_market", price=f"{market:g}" if market else "—"),
            t(lang, "move_limit_current", price=f"{group.limit_price:g}" if group.limit_price else "—"),
            "",
            t(lang, "move_limit_ask"),
        ]
        await update.callback_query.edit_message_text(
            NEWLINE.join(lines),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(t(lang, "btn_refresh"), callback_data=f"sm:{group_id}"),
                 InlineKeyboardButton(t(lang, "btn_move_limit"), callback_data=f"sme:{group_id}")],
                [InlineKeyboardButton(t(lang, "btn_back"), callback_data="stuck")],
            ]),
            parse_mode=ParseMode.HTML,
        )

    async def _begin_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        await update.callback_query.answer()
        context.user_data["price_group"] = int(update.callback_query.data.split(":")[1])
        await update.callback_query.edit_message_text(
            t(await self._lang(owner_id), "move_limit_ask"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(self._lang_now(owner_id), "btn_cancel_x"), callback_data="stuck")]]),
        )
        return ASK_PRICE

    async def _got_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        lang = await self._lang(owner_id)
        raw = (update.message.text or "").strip().replace(",", ".")
        try:
            price = float(raw)
            if price <= 0:
                raise ValueError
        except ValueError:
            # Stay in the conversation: a typo should cost one more message, not the whole flow.
            await update.message.reply_text(t(lang, "move_limit_bad"))
            return ASK_PRICE

        group_id = context.user_data.pop("price_group", None)
        group = await self._store.get_stuck_group(owner_id, group_id) if group_id else None
        if not group:
            await update.message.reply_text(t(lang, "stuck_none"), parse_mode=ParseMode.HTML)
            return ConversationHandler.END

        manager = await self._stuck_manager(owner_id)
        outcomes = await manager.move_limit(group, price)
        lines = [t(lang, "done_title"), "", f"<b>{group.symbol}</b> → {price:g}", ""]
        lines += [f"{label} — {outcome}" for label, outcome in outcomes]
        await update.effective_chat.send_message(
            NEWLINE.join(lines),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                t(lang, "btn_back"), callback_data="stuck")]]),
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    async def _show_accounts(self, update: Update, owner_id: int) -> None:
        master = await self._store.get_master(self._folder(owner_id))
        followers = await self._store.list_accounts(self._folder(owner_id), FOLLOWER)
        await update.callback_query.edit_message_text(
            messages.accounts_list(master, followers, await self._lang(owner_id)),
            reply_markup=_accounts_keyboard(
                has_master=master is not None,
                can_add_follower=len(followers) < self._settings.max_followers,
                lang=await self._lang(owner_id),
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _show_remove_menu(self, update: Update, owner_id: int) -> None:
        accounts = await self._store.list_accounts(owner_id)
        if not accounts:
            await update.callback_query.edit_message_text(
                t(await self._lang(owner_id), "acc_none"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="accounts")]])
            )
            return
        rows = [
            [InlineKeyboardButton(f"🗑 {a.label} (…{a.api_key_hint})", callback_data=f"remove:{a.id}")]
            for a in accounts
        ]
        rows.append([InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="accounts")])
        await update.callback_query.edit_message_text(
            t(await self._lang(owner_id), "acc_pick_remove"), reply_markup=InlineKeyboardMarkup(rows)
        )

    async def _show_positions(self, update: Update, owner_id: int) -> None:
        lines = [t(await self._lang(owner_id), "positions_title"), ""]
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
                    lines.append(f"{marker} <b>{account.label}</b> — " + t(await self._lang(owner_id), "no_position"))
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
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_back"), callback_data="menu")]]),
            parse_mode=ParseMode.HTML,
        )

    # ── adding accounts ─────────────────────────────────────────────────────────────────────
    async def _begin_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        # Telegram spins the button until the callback is answered; forgetting this is
        # indistinguishable from the bot being broken, even when the flow behind it works.
        await update.callback_query.answer()
        kind = MASTER if update.callback_query.data == "add_master" else FOLLOWER
        context.user_data["kind"] = kind
        prompt = await update.callback_query.edit_message_text(
            t(await self._lang(owner_id), "add_send_key", kind=kind.title()),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_cancel_x"), callback_data="menu")]]),
        )
        # Tracked so the whole exchange can be swept away once the account is connected: these
        # prompts are scaffolding, and what they were collecting now lives in the menu instead.
        context.user_data["cleanup"] = [prompt.message_id] if hasattr(prompt, "message_id") else []
        return ASK_KEY

    async def _got_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
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
            t(await self._lang(owner_id), "add_send_secret"),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_cancel_x"), callback_data="menu")]]),
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

        folder_id = self._folder(owner_id)
        status = await update.effective_chat.send_message(t(lang, "add_validating"))

        # Validate before storing: an account that cannot read its own balance will fail on the
        # first real trade, and finding that out now is far cheaper (spec §4).
        async with aiohttp.ClientSession() as session:
            client = MexcRestClient(api_key, secret, session=session)
            try:
                equity, available = await client.get_usdt_balance()
                mode = await client.get_position_mode()
            except MexcError as err:
                await status.edit_text(t(lang, "add_cannot_connect", error=err.message))
                context.user_data.clear()
                return ConversationHandler.END

        master = await self._store.get_master(self._folder(owner_id))
        warning = ""
        if kind == FOLLOWER and master and master.position_mode and mode != master.position_mode:
            # Hedge vs one-way changes what a side means; copying across a mismatch mirrors the
            # wrong direction. Verified the hard way during testing.
            warning = (
                t(lang, "add_mode_mismatch",
                  theirs="hedge" if mode == 1 else "one-way",
                  masters="hedge" if master.position_mode == 1 else "one-way")
            )

        followers = await self._store.list_accounts(folder_id, FOLLOWER)
        label = "Master" if kind == MASTER else f"Follower #{len(followers) + 1}"
        replacing = kind == MASTER and master is not None
        try:
            if replacing:
                # Swap rather than add: the schema permits one master per folder, so adding on top
                # would simply be rejected. Done in a single transaction inside the store, because
                # a delete that succeeded without its insert leaves the folder with no master and
                # the old keys already gone.
                await self._store.replace_master(
                    owner_id=owner_id,
                    folder_id=folder_id,
                    api_key=api_key,
                    api_secret=secret,
                    position_mode=mode,
                )
            else:
                await self._store.add_account(
                    owner_id=owner_id,
                    folder_id=folder_id,
                    label=label,
                    kind=kind,
                    api_key=api_key,
                    api_secret=secret,
                    position_mode=mode,
                )
        except Exception as err:  # noqa: BLE001 — most likely the one-master-per-folder constraint
            await status.edit_text(t(lang, "add_cannot_save", error=err))
            context.user_data.clear()
            return ConversationHandler.END

        if replacing:
            # The service holds the previous master's socket and its position baseline, both of
            # which describe an account that is no longer the master. START must rebuild them.
            service = await self._registry.get(folder_id, owner_id)
            if service.running:
                await service.stop()

        self._invalidate_balances(owner_id)

        # The account is connected, so the add-flow messages have served their purpose. Everything
        # they showed (balance, mode) is now on the menu, which stays put instead of scrolling off.
        cleanup = list(context.user_data.get("cleanup", []))
        cleanup.append(status.message_id)
        if replacing:
            # A swap deleted an account and its keys. That deserves a line that stays, not a menu
            # quietly redrawn with a different key hint.
            await status.edit_text(
                t(lang, "master_changed", hint=api_key[-4:]), parse_mode=ParseMode.HTML
            )
            cleanup.remove(status.message_id)
        elif warning:
            # A position-mode mismatch would silently mirror the wrong direction, so that one
            # message survives the sweep rather than being replaced by a tidy menu.
            await status.edit_text(t(lang, "add_done", label=label, warning=warning), parse_mode=ParseMode.HTML)
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
        await update.message.reply_text(t(UK, "cancelled"))
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
            if not (self._app and chat_id):
                return
            try:
                await self._app.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
            except BadRequest:
                # Notices carry text straight from the exchange — a symbol or an error message
                # can contain a stray "<" that makes Telegram reject the whole message as invalid
                # HTML. Losing a trade report to a formatting character is the worse outcome, so
                # it goes out unformatted rather than not at all.
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
