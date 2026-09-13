"""Telegram control surface.

Contains no trading logic (spec §3, §34): every button calls into CopyService or Store. That
boundary is what keeps a Telegram outage from affecting copying, and lets the trading path be
reasoned about without reading UI code.

Access is a hard whitelist of Telegram user ids (COPY_BOT_ALLOWED_USER_ID, comma-separated).
Anyone not on it gets a refusal — this bot can move real money on ten accounts, so an unknown chat
must never reach a keyboard.

Everyone on the whitelist gets their OWN world: their own master, their own followers, their own
START/STOP and their own Emergency Stop. There is no admin — the two brothers running this control
only what they added themselves. Every handler derives `owner_id` from `update.effective_user.id`
and passes it down; nothing here can address an account by id alone, because the store requires
the owner too.

It runs in two kinds of place. In a private chat it is what it always was: every folder in one
list. In a forum group each topic is one exchange (see telegram/topics.py) — the MEXC topic shows
only MEXC folders, the HIBT topic only HIBT ones. Control stays per person there, but VISIBILITY
does not: a group shows every member every message the bot posts, menus and reports included.
What the bot does about that is keep each menu answering only to its owner, and never ask for
API keys anywhere but a private chat.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import time

import aiohttp
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatMemberStatus, ChatType, ParseMode
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
from ..exchange import (
    DEFAULT_EXCHANGE,
    EXCHANGE_HIBT,
    EXCHANGE_MEXC,
    Credentials,
    contract_specs,
    exchange_name,
    make_rest_client,
    ticker_price,
)
from ..mexc.rest import MexcError
from . import messages
from .i18n import EN, UK, t
from .topics import (
    ADD_LINK_COMMAND,
    CURRENT_VIEW,
    Place,
    View,
    add_link,
    exchange_from_title,
    parse_add_payload,
    parse_exchange,
)

LOGGER = logging.getLogger(__name__)

# Conversation states for adding an account.
ASK_KEY, ASK_SECRET = range(2)

# Its own conversation: moving a limit asks for a number, and it must not be confused with the
# key/secret flow, which is waiting for text of a completely different kind.
ASK_PRICE = 100
ASK_FOLDER_NAME = 101
ASK_RENAME = 102

# Written out rather than inlined: patching this file has twice turned an escaped newline into
# a real one, which is a syntax error that only shows up at import time.
NEWLINE = chr(10)

# The one button that lives above the message box, so the menu is always one press away instead of
# something to scroll back and find. Its text is fixed rather than translated: it is matched
# against what Telegram sends back, and a menu opened in one language must still work after the
# language is switched.
# A trade can produce a report and a notice in the same breath; moving the menu for each would
# leave a trail of dead menus up the chat.
MENU_MOVE_COOLDOWN = 3.0

MENU_BUTTON = "☰ Menu"
MENU_FILTER = filters.Regex(f"^{MENU_BUTTON}$")

# Attached to a SENT message only — Telegram cannot add one while editing — so it goes on whatever
# the bot sends first and then simply stays there.
PERSISTENT_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(MENU_BUTTON)]], resize_keyboard=True, is_persistent=True
)
# In a group the same button, shown only to the person it was sent in reply to. A plain one would
# appear above everybody's message box, including people the bot refuses.
GROUP_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton(MENU_BUTTON)]], resize_keyboard=True, is_persistent=True, selective=True
)

# How long a balance reading stays good enough to reuse. Tapping around the menu should not fire
# ten exchange calls per screen; five seconds is under the time it takes to read the menu, so what
# you see is still effectively live, while a burst of taps costs one round of calls instead of one
# per tap.
BALANCE_TTL_SECONDS = 5.0


def _column_rows(
    left: list, right: list, lang: str, titles: tuple[str, str] | None = None
) -> list[list[InlineKeyboardButton]]:
    """The two legs, side by side, one account per cell.

    Buttons rather than text because a keyboard aligns its columns and a message does not: the
    balances and the lights line up under each other, so the whole folder reads at a glance
    instead of being counted. The cells do nothing when pressed — they are a display that happens
    to be made of buttons.

    The shorter column is padded with blanks, or Telegram would slide the remaining cells of the
    longer one across into the wrong leg.
    """
    rows: list[list[InlineKeyboardButton]] = []
    blank = InlineKeyboardButton(" ", callback_data="noop")
    if titles:
        rows.append([InlineKeyboardButton(title, callback_data="noop") for title in titles])
    for index in range(max(len(left), len(right))):
        row = []
        row.append(
            InlineKeyboardButton(left[index].text(), callback_data="noop") if index < len(left) else blank
        )
        row.append(
            InlineKeyboardButton(right[index].text(), callback_data="noop") if index < len(right) else blank
        )
        rows.append(row)

    if left or right:
        rows.append([
            InlineKeyboardButton(t(lang, "btn_close_column"), callback_data="closecol:master")
            if left else blank,
            InlineKeyboardButton(t(lang, "btn_close_column"), callback_data="closecol:opposite")
            if right else blank,
        ])
    return rows


def _topic_title(message) -> str | None:
    """A forum topic's name, as Telegram attaches it to messages posted inside the topic."""
    root = getattr(message, "reply_to_message", None)
    created = getattr(root, "forum_topic_created", None) if root else None
    return created.name if created else None


def _menu_keyboard(
    running: bool,
    lang: str,
    stuck: int = 0,
    folder: str = "",
    columns: tuple[list, list, tuple[str, str]] | None = None,
) -> InlineKeyboardMarkup:
    control = (
        InlineKeyboardButton(t(lang, "btn_stop"), callback_data="stop")
        if running
        else InlineKeyboardButton(t(lang, "btn_start"), callback_data="start")
    )
    # The legs sit directly under Refresh, because Refresh is what updates the balances in them.
    legs = _column_rows(columns[0], columns[1], lang, columns[2]) if columns else []
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t(lang, "btn_positions"), callback_data="positions"),
             InlineKeyboardButton(t(lang, "btn_accounts"), callback_data="accounts")],
            [InlineKeyboardButton(t(lang, "btn_history"), callback_data="history"),
             InlineKeyboardButton(t(lang, "btn_mode"), callback_data="mode")],
            [InlineKeyboardButton(t(lang, "btn_folder", name=folder), callback_data="folders")],
            [InlineKeyboardButton(t(lang, "btn_refresh"), callback_data="menu"),
             InlineKeyboardButton(t(lang, "btn_lang"), callback_data="lang")],
        ]
        + legs
        + [
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
        # Which folder each owner is looking at is NOT kept here any more. It depends on where
        # they are looking from — the MEXC topic and the HIBT topic show different folders — and
        # a single slot per owner, shared by every handler and every background report, is exactly
        # how a START pressed in one topic ends up starting the other one's folder. See
        # telegram/topics.py: it lives in the View, one per running task.
        #
        # Forum topics, cached because every button press in a group asks: (chat, thread) ->
        # exchange, and (chat, message) -> the owner of that menu.
        self._topic_cache: dict[tuple[int, int], str] = {}
        self._screen_owners: dict[tuple[int, int], int] = {}
        # (owner, exchange) -> the place last written to the database, so an unchanged place
        # costs nothing on the next press.
        self._places: dict[tuple[int, str], tuple[Place, str]] = {}
        # Display names, so a report in a shared topic can say whose trade it was.
        self._names: dict[int, str] = {}
        # Sessions opened for stuck-account work while copying is stopped, closed on shutdown.
        self._own_sessions: list[aiohttp.ClientSession] = []
        # (owner, exchange or None) -> (chat, message) of the menu currently on screen. Kept so a
        # trade report can bring the lights and balances up to date without waiting for someone to
        # press Refresh — which is the whole point of a light. Per exchange because in a forum
        # group one owner has a menu in each topic.
        self._menu_message: dict[tuple[int, str | None], tuple[int, int]] = {}
        # (owner, chat, thread) that already have the Menu button above their message box. Sent
        # once per place per process rather than on every menu, which would be a stray message.
        self._menu_button_shown: set[tuple[int, int, int | None]] = set()
        # When the menu was last moved to the bottom, per owner per exchange.
        self._menu_moved: dict[tuple[int, str | None], float] = {}

        registry.configure_callbacks(on_report=self._report_for, on_notice=self._notice_for)

    # ── plumbing ────────────────────────────────────────────────────────────────────────────
    def build(self) -> Application:
        app = Application.builder().token(self._settings.bot_token).build()

        add_conversation = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self._begin_add, pattern="^add_(master|follower)$"),
                # Arriving from a topic's "add account" link. Private chats only: the keys that
                # follow must never be typed where a group can read them.
                CommandHandler(
                    "start", self._begin_add_from_link,
                    filters=filters.ChatType.PRIVATE & filters.Regex(ADD_LINK_COMMAND),
                ),
            ],
            states={
                ASK_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND & ~MENU_FILTER, self._got_key)],
                ASK_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND & ~MENU_FILTER, self._got_secret)],
            },
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                CommandHandler("start", self._restart_from_conversation),
                # Pressing Menu mid-flow means "get me out of here", exactly as pressing any other
                # button does. Without it the flow would read the word "Menu" as an API key.
                MessageHandler(MENU_FILTER, self._restart_from_conversation),
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
            states={ASK_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND & ~MENU_FILTER, self._got_price)]},
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                MessageHandler(MENU_FILTER, self._restart_from_conversation),
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            allow_reentry=True,
            per_message=False,
        )

        rename_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_rename, pattern="^ren:[ga]:[0-9]+$")],
            states={
                ASK_RENAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~MENU_FILTER, self._got_rename)
                ]
            },
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                MessageHandler(MENU_FILTER, self._restart_from_conversation),
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            allow_reentry=True,
            per_message=False,
        )

        folder_conversation = ConversationHandler(
            entry_points=[CallbackQueryHandler(self._begin_folder_name, pattern="^f(new|ren)$")],
            states={
                ASK_FOLDER_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~MENU_FILTER, self._got_folder_name)
                ]
            },
            fallbacks=[
                CommandHandler("cancel", self._cancel_add),
                MessageHandler(MENU_FILTER, self._restart_from_conversation),
                CallbackQueryHandler(self._abandon_and_dispatch),
            ],
            allow_reentry=True,
            per_message=False,
        )

        # A /start carrying an add-account link is left to the add conversation below; this
        # handler comes first, and taking it here would open the menu instead of asking for keys.
        app.add_handler(CommandHandler("start", self._cmd_start, filters=~filters.Regex(ADD_LINK_COMMAND)))
        app.add_handler(CommandHandler("menu", self._cmd_start))
        app.add_handler(MessageHandler(MENU_FILTER, self._cmd_start))
        app.add_handler(CommandHandler("bind", self._cmd_bind))
        app.add_handler(CommandHandler("unbind", self._cmd_unbind))
        app.add_handler(CommandHandler("topics", self._cmd_topics))
        app.add_handler(MessageHandler(
            filters.StatusUpdate.FORUM_TOPIC_CREATED | filters.StatusUpdate.FORUM_TOPIC_EDITED,
            self._on_topic_event,
        ))
        app.add_handler(add_conversation)
        app.add_handler(price_conversation)
        app.add_handler(folder_conversation)
        app.add_handler(rename_conversation)
        app.add_handler(CallbackQueryHandler(self._on_button))
        self._app = app
        return app

    def _authorized(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id in self._settings.allowed_user_ids)

    async def _guard(self, update: Update) -> int | None:
        """Returns the owner id to act as, or None when the user is refused.

        Also settles where they are acting, and so what they see. In a private chat that is every
        folder they own, as it always was. In a forum group it is the topic: the topic names an
        exchange, only that exchange's folders exist there, and a button belongs to whoever the
        menu was opened for.
        """
        if not self._authorized(update):
            return await self._refuse(update)
        user = update.effective_user
        owner_id = user.id
        self._names[owner_id] = user.full_name or (f"@{user.username}" if user.username else str(owner_id))
        chat = update.effective_chat
        if chat is None or chat.type == ChatType.PRIVATE:
            chat_id = chat.id if chat else owner_id
            self._chat_ids[owner_id] = chat_id
            folder_id = await self._resolve_folder(owner_id, None)
            CURRENT_VIEW.set(View(owner_id, None, Place(chat_id), folder_id))
            return owner_id
        return await self._guard_group(update, owner_id, chat.id)

    async def _guard_group(self, update: Update, owner_id: int, chat_id: int) -> int | None:
        query = update.callback_query
        message = query.message if query else update.message
        lang = await self._lang(owner_id)
        if not isinstance(message, Message):
            # A button on a message too old for Telegram to hand back. Which topic it was in cannot
            # be read, so it cannot be acted on safely.
            if query:
                await query.answer(t(lang, "screen_stale"), show_alert=True)
            return None
        thread_id = message.message_thread_id if message.is_topic_message else None
        if thread_id is None:
            await self._hint(update, t(lang, "topic_use_a_topic"))
            return None
        exchange = await self._topic_exchange(chat_id, thread_id, message)
        if exchange is None:
            await self._hint(update, t(lang, "topic_not_bound"))
            return None
        if query and await self._screen_owner(chat_id, message.message_id) != owner_id:
            # Somebody else's menu. Acting on it would redraw their screen with this person's
            # accounts, in front of the whole topic.
            await query.answer(t(lang, "screen_not_yours"), show_alert=True)
            return None
        place = Place(chat_id, thread_id)
        await self._remember_place(owner_id, exchange, place)
        folder_id = await self._resolve_folder(owner_id, exchange)
        CURRENT_VIEW.set(View(owner_id, exchange, place, folder_id))
        return owner_id

    async def _refuse(self, update: Update) -> None:
        LOGGER.warning("refused telegram user %s", update.effective_user.id if update.effective_user else "?")
        if update.callback_query:
            await update.callback_query.answer(t(UK, "not_authorized"), show_alert=True)
        elif update.message:
            await update.message.reply_text(t(UK, "not_authorized") + ".")
        return None

    async def _hint(self, update: Update, text: str) -> None:
        """Say why nothing happened, in whichever way this update can be answered."""
        if update.callback_query:
            await update.callback_query.answer(text, show_alert=True)
        elif update.message:
            with contextlib.suppress(Exception):
                await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _resolve_folder(self, owner_id: int, exchange: str | None) -> int:
        """The folder this owner is working in here, creating their first one if they have none.

        A brand new owner has never chosen a folder and has none to choose. Making one beats
        showing an empty screen that reads as though their accounts had gone missing.

        Read from the database on every press rather than cached: a folder deleted in one place
        must not survive as a cached id in another.
        """
        if exchange is None:
            folder_id = await self._store.active_folder_id(owner_id)
            if folder_id is None:
                folder_id = await self._store.create_folder(owner_id, "MEXC")
                await self._store.set_active_folder(owner_id, folder_id)
                LOGGER.info("created first folder %s for owner %s", folder_id, owner_id)
            return folder_id

        remembered = await self._store.topic_view(owner_id, exchange)
        folder = None
        if remembered and remembered.get("folder_id"):
            folder = await self._store.get_folder(remembered["folder_id"], owner_id)
            if folder and folder.exchange != exchange:
                folder = None
        if folder is None:
            same_venue = await self._store.list_folders(owner_id, exchange)
            if same_venue:
                folder = same_venue[0]
        if folder is None:
            folder_id = await self._store.create_folder(owner_id, exchange_name(exchange), exchange)
            LOGGER.info("created first %s folder %s for owner %s", exchange, folder_id, owner_id)
        else:
            folder_id = folder.id
        if not remembered or remembered.get("folder_id") != folder_id:
            await self._store.set_view_folder(owner_id, exchange, folder_id)
        return folder_id

    def _view(self, owner_id: int) -> View:
        """Where this owner is acting right now. Set by _guard, or by _enter_folder_view for a
        background message. Never guessed: acting on a folder nobody chose is how accounts from two
        setups get mixed, so a missing view is an error rather than a default."""
        view = CURRENT_VIEW.get()
        if view is None or view.owner_id != owner_id or view.folder_id is None:
            raise RuntimeError(f"no view for owner {owner_id}")
        return view

    def _folder(self, owner_id: int) -> int:
        """The folder in front of this owner right now."""
        return self._view(owner_id).folder_id

    async def _switch_folder(self, owner_id: int, folder_id: int) -> None:
        """Open another folder here, and remember that it is the one open here."""
        view = self._view(owner_id)
        if view.in_topic:
            await self._store.set_view_folder(owner_id, view.exchange, folder_id)
        else:
            await self._store.set_active_folder(owner_id, folder_id)
        CURRENT_VIEW.set(view.with_folder(folder_id))
        self._invalidate_balances(owner_id)

    # ── forum topics ────────────────────────────────────────────────────────────────────────
    async def _topic_exchange(self, chat_id: int, thread_id: int, message: Message | None) -> str | None:
        """The exchange a topic is for — bound earlier, or recognised now by its title."""
        key = (chat_id, thread_id)
        if key in self._topic_cache:
            return self._topic_cache[key]
        exchange = await self._store.topic_exchange(chat_id, thread_id)
        if exchange is None:
            title = _topic_title(message)
            exchange = exchange_from_title(title)
            if exchange:
                await self._store.bind_topic(chat_id, thread_id, exchange, title, None)
                LOGGER.info("bound topic %s/%s (%r) to %s by its title", chat_id, thread_id, title, exchange)
        if exchange:
            self._topic_cache[key] = exchange
        return exchange

    async def _screen_owner(self, chat_id: int, message_id: int) -> int | None:
        key = (chat_id, message_id)
        if key not in self._screen_owners:
            owner = await self._store.screen_owner(chat_id, message_id)
            if owner is None:
                return None
            self._screen_owners[key] = owner
        return self._screen_owners[key]

    async def _own_screen(self, message, owner_id: int, markup) -> None:
        """Write down whose menu a message in a group is. Only keyboards need an owner: a message
        with nothing to press cannot be acted on by the wrong person."""
        if not isinstance(message, Message) or message.chat_id >= 0:
            return
        if not isinstance(markup, InlineKeyboardMarkup):
            return
        self._screen_owners[(message.chat_id, message.message_id)] = owner_id
        with contextlib.suppress(Exception):
            await self._store.record_screen_owner(message.chat_id, message.message_id, owner_id)

    async def _remember_place(self, owner_id: int, exchange: str, place: Place) -> None:
        name = self._names.get(owner_id, "")
        if self._places.get((owner_id, exchange)) == (place, name):
            return
        await self._store.remember_view(owner_id, exchange, place.chat_id, place.thread_id, name or None)
        self._places[(owner_id, exchange)] = (place, name)

    async def _enter_folder_view(self, owner_id: int, folder_id: int | None):
        """Set up the view a background message about `folder_id` is shown in.

        It goes to that folder's exchange topic, if the owner has ever used one, and otherwise to
        their private chat. The menu redrawn there is the one open in THAT place — not necessarily
        `folder_id`: a report about a folder running in the background must not swap what the
        owner is looking at. Returns (the folder, the view).
        """
        folder = await self._store.get_folder(folder_id, owner_id) if folder_id is not None else None
        exchange = folder.exchange if folder else DEFAULT_EXCHANGE
        remembered = await self._store.topic_view(owner_id, exchange)
        if remembered:
            if remembered.get("display_name"):
                self._names.setdefault(owner_id, remembered["display_name"])
            place, key_exchange = Place(remembered["chat_id"], remembered["thread_id"]), exchange
        else:
            place, key_exchange = Place(self._chat_ids.get(owner_id, owner_id)), None
        shown = await self._resolve_folder(owner_id, key_exchange)
        view = View(owner_id, key_exchange, place, shown)
        CURRENT_VIEW.set(view)
        return folder, view

    def _whose(self, owner_id: int, folder=None) -> str:
        name = html.escape(self._names.get(owner_id) or str(owner_id))
        suffix = f" · {html.escape(folder.name)}" if folder else ""
        return f"👤 <b>{name}</b>{suffix}"

    async def _send(self, place: Place, owner_id: int, text: str, **kwargs):
        """Send into a place — its topic included — and remember whose screen it is.

        `send_message` knows nothing about topics: without message_thread_id, a message meant for
        the HIBT topic lands in General. Everything the bot sends on its own goes through here, so
        that cannot be forgotten one call at a time.
        """
        if not self._app:
            return None
        if place.thread_id is not None:
            kwargs["message_thread_id"] = place.thread_id
        try:
            sent = await self._app.bot.send_message(place.chat_id, text, **kwargs)
        except BadRequest as err:
            reason = str(err).lower()
            if place.thread_id is not None and "thread" in reason:
                # The topic was deleted or closed. The owner still has to hear about their trade.
                kwargs.pop("message_thread_id", None)
                LOGGER.warning("topic %s/%s is gone; writing to %s privately", place.chat_id, place.thread_id, owner_id)
                return await self._send(Place(owner_id), owner_id, text, **kwargs)
            if not kwargs.get("parse_mode"):
                raise
            # Exchange text can carry a stray "<" that Telegram rejects as HTML. Losing a report to
            # a formatting character is the worse outcome, so it goes out unformatted instead.
            kwargs.pop("parse_mode")
            sent = await self._app.bot.send_message(place.chat_id, text, **kwargs)
        await self._own_screen(sent, owner_id, kwargs.get("reply_markup"))
        return sent

    async def _send_here(self, owner_id: int, text: str, **kwargs):
        return await self._send(self._view(owner_id).place, owner_id, text, **kwargs)

    def _in_flow_place(self, owner_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Is this reply typed where the question was asked?

        Telegram keys a conversation by chat and user, and every topic of a forum is the same chat.
        Without this, a name typed in the HIBT topic would answer a rename started in the MEXC topic
        — and rename the MEXC folder, because the question was about that one.
        """
        asked = context.user_data.get("flow_place")
        return asked is None or asked == self._view(owner_id).place

    async def _cmd_bind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """/bind mexc | /bind hibt — make this topic that exchange's."""
        if not self._authorized(update):
            await self._refuse(update)
            return
        message, chat = update.message, update.effective_chat
        lang = await self._lang(update.effective_user.id)
        if chat is None or chat.type == ChatType.PRIVATE or not message.is_topic_message:
            await message.reply_text(t(lang, "bind_only_in_topic"), parse_mode=ParseMode.HTML)
            return
        title = _topic_title(message)
        exchange = parse_exchange(context.args[0] if context.args else None) or exchange_from_title(title)
        if not exchange:
            await message.reply_text(t(lang, "bind_usage"), parse_mode=ParseMode.HTML)
            return
        await self._store.bind_topic(chat.id, message.message_thread_id, exchange, title, update.effective_user.id)
        self._topic_cache[(chat.id, message.message_thread_id)] = exchange
        text = t(lang, "bind_done", exchange=exchange_name(exchange))
        if not await self._bot_is_admin(chat):
            text += NEWLINE + NEWLINE + t(lang, "bot_not_admin")
        await message.reply_text(text, parse_mode=ParseMode.HTML)
        await self._cmd_start(update, context)

    async def _cmd_unbind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            await self._refuse(update)
            return
        message, chat = update.message, update.effective_chat
        lang = await self._lang(update.effective_user.id)
        if chat is None or chat.type == ChatType.PRIVATE or not message.is_topic_message:
            await message.reply_text(t(lang, "bind_only_in_topic"), parse_mode=ParseMode.HTML)
            return
        await self._store.unbind_topic(chat.id, message.message_thread_id)
        self._topic_cache.pop((chat.id, message.message_thread_id), None)
        await message.reply_text(t(lang, "unbind_done"), parse_mode=ParseMode.HTML)

    async def _cmd_topics(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            await self._refuse(update)
            return
        chat = update.effective_chat
        lang = await self._lang(update.effective_user.id)
        if chat is None or chat.type == ChatType.PRIVATE:
            await update.message.reply_text(t(lang, "bind_only_in_topic"), parse_mode=ParseMode.HTML)
            return
        bound = await self._store.list_topics(chat.id)
        lines = [t(lang, "topics_title"), ""]
        lines += [
            f"• <b>{exchange_name(exchange)}</b> — {html.escape(title or '#' + str(thread))}"
            for thread, exchange, title in bound
        ] or [t(lang, "topics_none")]
        if not await self._bot_is_admin(chat):
            lines += ["", t(lang, "bot_not_admin")]
        await update.message.reply_text(NEWLINE.join(lines), parse_mode=ParseMode.HTML)

    async def _on_topic_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """A topic was created or renamed. One called MEXC or HIBT is bound on the spot.

        Only ever binds, never unbinds: a topic bound by hand with /bind keeps its exchange when
        someone renames it to something the title matcher does not recognise.
        """
        message = update.message
        if not message or not message.is_topic_message:
            return
        created, edited = message.forum_topic_created, message.forum_topic_edited
        title = (created.name if created else None) or (edited.name if edited else None)
        exchange = exchange_from_title(title)
        if not exchange:
            return
        await self._store.bind_topic(message.chat_id, message.message_thread_id, exchange, title, None)
        self._topic_cache[(message.chat_id, message.message_thread_id)] = exchange
        with contextlib.suppress(Exception):
            await message.reply_text(t(UK, "bind_auto", exchange=exchange_name(exchange)), parse_mode=ParseMode.HTML)

    async def _bot_is_admin(self, chat) -> bool:
        """Without admin rights the bot does not see the names and prices people type in answer
        to its questions, and cannot tidy away old menus."""
        try:
            member = await chat.get_member(self._app.bot.id)
        except Exception:  # noqa: BLE001 - unknown is reported as "not admin", which is the safe advice
            return False
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

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
        mode, _ = await self._store.get_mode(self._folder(owner_id))
        lang = await self._lang(owner_id)

        accounts = ([master] if master else []) + followers
        view = self._view(owner_id)
        header = (
            f"{self._whose(owner_id)} · {exchange_name(view.exchange)}{NEWLINE}{NEWLINE}"
            if view.place.is_group
            else ""
        )
        return header + messages.main_menu(
            running=service.running,
            master=master,
            followers=followers,
            max_followers=self._settings.max_followers,
            master_connected=service.master_connected,
            balances=await self._balances(owner_id, accounts),
            mode=mode,
            lang=lang,
            listed_in_columns=mode == MODE_REVERSE and bool(accounts),
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
                    snapshot = await make_rest_client(credentials, session=session).get_usdt_snapshot()
                    return messages.Balance(
                        equity=snapshot.equity,
                        available=snapshot.openable,
                        wallet=snapshot.available,
                    )
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

    async def _group_names(self, owner_id: int) -> tuple[str, str]:
        """What the two legs are called here, falling back to a translated default.

        The default is not stored, so a folder that has never been renamed follows the language
        switch instead of being stuck with whichever language it was created in.
        """
        lang = await self._lang(owner_id)
        folder = await self._store.get_folder(self._folder(owner_id), owner_id)
        return (
            (folder.group_one_name if folder and folder.group_one_name else t(lang, "group_one")),
            (folder.group_two_name if folder and folder.group_two_name else t(lang, "group_two")),
        )

    async def _ensure_menu_button(self, update: Update, owner_id: int) -> None:
        """Put the Menu button above the message box, once per place.

        Telegram attaches a reply keyboard to a message being SENT and has no way to add one while
        editing, so it cannot ride along with the menu itself — the menu is usually an edit. It is
        sent once, on its own, and then stays until someone removes it.

        In a group it is sent as a reply and marked selective, so it appears for the person who
        asked and nobody else.
        """
        place = self._view(owner_id).place
        key = (owner_id, place.chat_id, place.thread_id)
        if key in self._menu_button_shown or not self._app:
            return
        self._menu_button_shown.add(key)
        hint = t(await self._lang(owner_id), "menu_button_hint")
        with contextlib.suppress(Exception):
            if place.is_group and update.message:
                await update.message.reply_text(hint, reply_markup=GROUP_KEYBOARD)
            else:
                await self._send(place, owner_id, hint, reply_markup=PERSISTENT_KEYBOARD)

    async def _menu_columns(self, owner_id: int) -> tuple[list, list, tuple[str, str]] | None:
        """The two legs, or None when this folder is not in REVERSE.

        Only REVERSE has legs to show. In COPY every account does the same thing, so a column per
        direction would be one column and a screenful of cells saying so.
        """
        folder_id = self._folder(owner_id)
        mode, _ = await self._store.get_mode(folder_id)
        if mode != MODE_REVERSE:
            return None
        master = await self._store.get_master(folder_id)
        followers = await self._store.list_accounts(folder_id, FOLLOWER)
        if not master and not followers:
            return None
        service = await self._registry.get(folder_id, owner_id)
        accounts = ([master] if master else []) + followers
        left, right = messages.reverse_columns(
            master, followers, await self._balances(owner_id, accounts), service.account_status
        )
        one, two = await self._group_names(owner_id)
        sides = service.account_sides
        return left, right, (
            messages.group_title(left, sides, one),
            messages.group_title(right, sides, two),
        )

    async def _menu_view(self, owner_id: int) -> tuple[str, InlineKeyboardMarkup]:
        service = await self._registry.get(self._folder(owner_id), owner_id)
        return (
            await self._menu_text(owner_id),
            _menu_keyboard(
                service.running,
                await self._lang(owner_id),
                await self._stuck_count(owner_id),
                await self._folder_name(owner_id),
                await self._menu_columns(owner_id),
            ),
        )

    async def _show_menu(self, update: Update, owner_id: int) -> None:
        text, keyboard = await self._menu_view(owner_id)
        key = (owner_id, self._view(owner_id).exchange)
        if update.callback_query:
            message = update.callback_query.message
            self._menu_message[key] = (message.chat_id, message.message_id)
            await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        elif update.message:
            # The inline keyboard belongs to the message; the Menu button belongs to the chat. Both
            # cannot ride on one send, so the persistent one goes out first and then stays put.
            await self._ensure_menu_button(update, owner_id)
            sent = await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            await self._own_screen(sent, owner_id, keyboard)
            self._menu_message[key] = (sent.chat_id, sent.message_id)

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
            # Refresh is the button pressed to find out what is true now, so it asks rather than
            # redrawing what was already on screen.
            self._invalidate_balances(owner_id)
            with contextlib.suppress(Exception):
                await service.refresh_account_status()
            await self._show_menu(update, owner_id)
        elif action == "noop":
            # The cells in the two legs are a display made of buttons; pressing one does nothing,
            # and the answer above already cleared Telegram's spinner.
            pass
        elif action.startswith("closecol:"):
            await self._close_column(update, owner_id, action.split(":", 1)[1])
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
            events = await self._store.recent_events(owner_id, 10, self._view(owner_id).exchange)
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
            view = self._view(owner_id)
            # In a topic only that exchange's folders can be opened, whatever an old keyboard offers.
            if folder and (not view.in_topic or folder.exchange == view.exchange):
                await self._switch_folder(owner_id, folder_id)
                await update.callback_query.answer(
                    t(await self._lang(owner_id), "folder_switched", name=folder.name).replace("<b>", "").replace("</b>", "")
                )
            await self._show_menu(update, owner_id)
        elif action.startswith("fexch:"):
            await self._switch_folder_exchange(update, owner_id, action.split(":", 1)[1])
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
            master = await self._store.get_master(folder_id)
            everyone = ([master] if master else []) + await self._store.list_accounts(folder_id, FOLLOWER)
            account = next((a for a in everyone if a.id == account_id), None)
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

    async def _close_column(self, update: Update, owner_id: int, leg: str) -> None:
        """Close one leg at market, touching only the accounts that actually hold something.

        The skip is not politeness, it is the whole safety of the button. On MEXC an order on the
        opposite side of a flat account opens a position rather than closing one, so sending a
        close to an account already closed by hand would put it right back in, facing the other
        way. Accounts are read first and the flat ones are named, not silently passed over.
        """
        lang = await self._lang(owner_id)
        folder_id = self._folder(owner_id)
        service = await self._registry.get(folder_id, owner_id)

        master = await self._store.get_master(folder_id)
        followers = await self._store.list_accounts(folder_id, FOLLOWER)
        wanted = (
            [a for a in followers if a.is_reversed]
            if leg == "opposite"
            else ([master] if master else []) + [a for a in followers if not a.is_reversed]
        )
        one, two = await self._group_names(owner_id)
        side = two if leg == "opposite" else one

        closed, failed, skipped = await service.close_accounts([a.id for a in wanted])

        lines = [t(lang, "closed_column", side=side), ""]
        if not closed and not failed:
            lines.append(t(lang, "column_nothing_open"))
        for label, pnl in closed:
            # An unread settlement says so rather than printing a zero, which would read as
            # "this one broke even".
            amount = messages.signed_money(pnl) if pnl is not None else t(lang, "pnl_pending")
            lines.append(f"✅ {label}  {amount}")
        lines += [f"❌ {label} — {error}" for label, error in failed]

        known = [pnl for _, pnl in closed if pnl is not None]
        if known:
            suffix = (
                ""
                if len(known) == len(closed)
                else t(lang, "counted_suffix", n=len(known), total=len(closed))
            )
            lines.append("")
            lines.append(t(lang, "total_pnl", amount=messages.signed_money(sum(known)), suffix=suffix))

        if skipped:
            lines.append("")
            lines.append(t(lang, "column_skipped", names=", ".join(skipped)))

        await update.callback_query.edit_message_text(
            NEWLINE.join(lines),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu")]]
            ),
            parse_mode=ParseMode.HTML,
        )
        self._invalidate_balances(owner_id)

    async def _show_menu_message(self, owner_id: int) -> None:
        """A fresh menu, sent to wherever this owner is acting right now."""
        if not self._app:
            return
        view = self._view(owner_id)
        text, keyboard = await self._menu_view(owner_id)
        sent = await self._send(view.place, owner_id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        if sent:
            self._menu_message[(owner_id, view.exchange)] = (sent.chat_id, sent.message_id)

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
            # Every account, master included: in this mode it is one more member of a leg, and
            # leaving it off would make the one account you cannot move the invisible one.
            master = await self._store.get_master(self._folder(owner_id))
            for account in ([master] if master else []) + followers:
                group = t(lang, "btn_dir_reverse") if account.is_reversed else t(lang, "btn_dir_copy")
                rows.append([
                    InlineKeyboardButton(f"{account.label} — {group}", callback_data=f"dir:{account.id}"),
                    InlineKeyboardButton("✏️", callback_data=f"ren:a:{account.id}"),
                ])
            folder = await self._store.get_folder(self._folder(owner_id), owner_id)
            rows.append([
                InlineKeyboardButton(
                    "✏️ " + (folder.group_one_name if folder and folder.group_one_name else t(lang, "group_one")),
                    callback_data="ren:g:1",
                ),
                InlineKeyboardButton(
                    "✏️ " + (folder.group_two_name if folder and folder.group_two_name else t(lang, "group_two")),
                    callback_data="ren:g:2",
                ),
            ])
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

    async def _begin_rename(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Ask for a new name — for one of the two legs, or for a single account."""
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        query = update.callback_query
        await query.answer()
        _, kind, target = (query.data or "").split(":")
        context.user_data["rename"] = (kind, int(target))
        context.user_data["flow_place"] = self._view(owner_id).place

        lang = await self._lang(owner_id)
        if kind == "g":
            prompt = t(lang, "rename_group_prompt", n=target)
        else:
            folder_id = self._folder(owner_id)
            master = await self._store.get_master(folder_id)
            everyone = ([master] if master else []) + await self._store.list_accounts(folder_id, FOLLOWER)
            current = next((a for a in everyone if a.id == int(target)), None)
            prompt = t(lang, "rename_account_prompt", name=current.label if current else "?")
        await query.edit_message_text(prompt, parse_mode=ParseMode.HTML)
        return ASK_RENAME

    async def _got_rename(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        if not self._in_flow_place(owner_id, context):
            return ASK_RENAME
        context.user_data.pop("flow_place", None)
        target = context.user_data.pop("rename", None)
        name = (update.message.text or "").strip()
        if target and name:
            kind, target_id = target
            if kind == "g":
                await self._store.rename_group(self._folder(owner_id), owner_id, target_id, name)
            else:
                # Scoped by owner in SQL, so a stale keyboard from somebody else matches no row.
                await self._store.rename_account(target_id, owner_id, name)
        await self._show_menu_message(owner_id)
        return ConversationHandler.END

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
        if not folder:
            return "—"
        venue = exchange_name(folder.exchange)
        # "MEXC · MEXC" says nothing twice; the first folder is literally named after its venue.
        return folder.name if venue.lower() in folder.name.lower() else f"{folder.name} · {venue}"

    async def _show_folders(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        view = self._view(owner_id)
        folders = await self._store.list_folders(owner_id, view.exchange)
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
        if not view.in_topic:
            # A topic IS its exchange, so there is nothing to switch there. Only a private chat,
            # which lists every folder, offers to move an empty one between venues.
            current = next((f for f in folders if f.id == active), None)
            other = EXCHANGE_HIBT if (current and current.exchange == EXCHANGE_MEXC) else EXCHANGE_MEXC
            rows.append([InlineKeyboardButton(
                t(lang, "btn_folder_exchange", current=exchange_name(current.exchange if current else None),
                  other=exchange_name(other)),
                callback_data=f"fexch:{other}",
            )])
        rows.append([InlineKeyboardButton(t(lang, "btn_delete_folder"), callback_data="fdel")])
        rows.append([InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu")])

        await update.callback_query.edit_message_text(
            messages.folders_screen(folders, active, counts, lang),
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )

    async def _switch_folder_exchange(self, update: Update, owner_id: int, exchange: str) -> None:
        """Move the folder on screen to another venue - only while it is empty and stopped.

        Empty, because stored keys belong to the venue they came from. Stopped, because a running
        folder is trading on them. The store enforces both in the UPDATE itself; the checks here
        only decide which explanation to show.
        """
        lang = await self._lang(owner_id)
        if self._view(owner_id).in_topic:
            await update.callback_query.answer(t(lang, "folder_exchange_in_topic"), show_alert=True)
            return
        folder_id = self._folder(owner_id)
        service = await self._registry.get(folder_id, owner_id)
        if service.running:
            await update.callback_query.answer(t(lang, "folder_stop_first"), show_alert=True)
            return
        if await self._store.list_accounts(folder_id):
            await update.callback_query.answer(t(lang, "folder_exchange_not_empty"), show_alert=True)
            return
        if not await self._store.set_folder_exchange(folder_id, owner_id, exchange):
            await update.callback_query.answer(t(lang, "folder_exchange_refused"), show_alert=True)
            return
        await update.callback_query.answer(
            t(lang, "folder_exchange_set", exchange=exchange_name(exchange)), show_alert=True
        )
        await self._show_folders(update, owner_id)

    async def _confirm_delete_folder(self, update: Update, owner_id: int) -> None:
        lang = await self._lang(owner_id)
        folders = await self._store.list_folders(owner_id, self._view(owner_id).exchange)
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
        view = self._view(owner_id)
        remaining = await self._store.list_folders(owner_id, view.exchange)
        if remaining:
            await self._switch_folder(owner_id, remaining[0].id)
        else:
            CURRENT_VIEW.set(view.with_folder(await self._resolve_folder(owner_id, view.exchange)))
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
        context.user_data["flow_place"] = self._view(owner_id).place
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
        if not self._in_flow_place(owner_id, context):
            return ASK_FOLDER_NAME
        name = (update.message.text or "").strip()[:40]
        if not name:
            await update.message.reply_text(t(lang, "folder_name_ask"))
            return ASK_FOLDER_NAME

        context.user_data.pop("flow_place", None)
        action = context.user_data.pop("folder_action", "fnew")
        if action == "fren":
            await self._store.rename_folder(self._folder(owner_id), owner_id, name)
            text = t(lang, "folder_renamed", name=name)
        else:
            # Created on the exchange of the topic it was asked for in; in a private chat, MEXC
            # until switched.
            folder_id = await self._store.create_folder(
                owner_id, name, self._view(owner_id).exchange or DEFAULT_EXCHANGE
            )
            # A new folder is empty, so switching to it immediately is what anyone creating one
            # was about to do anyway.
            await self._switch_folder(owner_id, folder_id)
            text = t(lang, "folder_created", name=name)

        await self._send_here(
            owner_id,
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
            exchange = await self._exchange_for(owner_id)
            async with aiohttp.ClientSession() as session:
                market = await ticker_price(session, exchange, group.symbol)
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
        context.user_data["flow_place"] = self._view(owner_id).place
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
        if not self._in_flow_place(owner_id, context):
            return ASK_PRICE
        raw = (update.message.text or "").strip().replace(",", ".")
        try:
            price = float(raw)
            if price <= 0:
                raise ValueError
        except ValueError:
            # Stay in the conversation: a typo should cost one more message, not the whole flow.
            await update.message.reply_text(t(lang, "move_limit_bad"))
            return ASK_PRICE

        context.user_data.pop("flow_place", None)
        group_id = context.user_data.pop("price_group", None)
        group = await self._store.get_stuck_group(owner_id, group_id) if group_id else None
        if not group:
            await update.message.reply_text(t(lang, "stuck_none"), parse_mode=ParseMode.HTML)
            return ConversationHandler.END

        manager = await self._stuck_manager(owner_id)
        outcomes = await manager.move_limit(group, price)
        lines = [t(lang, "done_title"), "", f"<b>{group.symbol}</b> → {price:g}", ""]
        lines += [f"{label} — {outcome}" for label, outcome in outcomes]
        await self._send_here(
            owner_id,
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
            for account in await self._store.list_accounts(self._folder(owner_id)):
                credentials = await self._store.get_credentials(account.id, owner_id)
                if not credentials:
                    continue
                client = make_rest_client(credentials, session=session)
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
        view = self._view(owner_id)
        if view.place.is_group:
            # Keys are never typed into a group. The whole topic would read them, and deleting the
            # message afterwards is too late: members have already had the notification.
            lang = await self._lang(owner_id)
            link = add_link(self._app.bot.username, view.exchange, kind, view.folder_id)
            await update.callback_query.edit_message_text(
                t(lang, "add_in_private", exchange=exchange_name(view.exchange)),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(t(lang, "btn_open_private"), url=link)],
                    [InlineKeyboardButton(t(lang, "btn_back"), callback_data="accounts")],
                ]),
                parse_mode=ParseMode.HTML,
            )
            return ConversationHandler.END
        context.user_data["kind"] = kind
        folder = await self._store.get_folder(view.folder_id, owner_id)
        prompt = await update.callback_query.edit_message_text(
            t(await self._lang(owner_id), "add_send_key", kind=kind.title(),
              exchange=exchange_name(folder.exchange if folder else None)),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(self._lang_now(owner_id), "btn_cancel_x"), callback_data="menu")]]),
        )
        # Tracked so the whole exchange can be swept away once the account is connected: these
        # prompts are scaffolding, and what they were collecting now lives in the menu instead.
        context.user_data["cleanup"] = [prompt.message_id] if hasattr(prompt, "message_id") else []
        return ASK_KEY

    async def _begin_add_from_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Keys for a folder of a topic, entered here in private.

        The link names a folder, but only points at it: it has to belong to whoever followed the link
        and be on the exchange the link says, or the flow does not start. A link forwarded to someone
        else, or edited by hand, reaches nobody's folder.
        """
        owner_id = await self._guard(update)
        if owner_id is None:
            return ConversationHandler.END
        lang = await self._lang(owner_id)
        parsed = parse_add_payload(context.args[0] if context.args else None)
        folder = await self._store.get_folder(parsed[2], owner_id) if parsed else None
        if not parsed or not folder or folder.exchange != parsed[0]:
            await update.message.reply_text(t(lang, "add_link_invalid"), parse_mode=ParseMode.HTML)
            return ConversationHandler.END
        exchange, kind, folder_id = parsed
        context.user_data.clear()
        context.user_data.update(kind=kind, add_folder_id=folder_id)
        prompt = await update.message.reply_text(
            t(lang, "add_send_key_for", kind=kind.title(), exchange=exchange_name(exchange),
              folder=html.escape(folder.name)),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "btn_cancel_x"), callback_data="menu")]]),
        )
        context.user_data["cleanup"] = [prompt.message_id]
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
        prompt = await self._send_here(
            owner_id,
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

        lang = await self._lang(owner_id)
        # From a topic's link, the folder the link named; otherwise the one on screen.
        linked_folder = context.user_data.get("add_folder_id")
        folder_id = linked_folder or self._folder(owner_id)
        exchange = await self._exchange_for(owner_id, folder_id)
        status = await self._send_here(owner_id, t(lang, "add_validating", exchange=exchange_name(exchange)))

        # Validate before storing: an account that cannot read its own balance will fail on the
        # first real trade, and finding that out now is far cheaper (spec §4).
        # Checked against the folder's own exchange. Keys belong to one venue, and a folder trades on
        # one: HIBT keys validated against MEXC would be refused as invalid when they are perfectly fine.
        async with aiohttp.ClientSession() as session:
            client = make_rest_client(Credentials(api_key, secret, exchange), session=session)
            try:
                equity, available = await client.get_usdt_balance()
                mode = await client.get_position_mode()
            except MexcError as err:
                await status.edit_text(t(lang, "add_cannot_connect", error=err.message))
                context.user_data.clear()
                return ConversationHandler.END

        master = await self._store.get_master(folder_id)
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
        if linked_folder:
            # Added from a topic: say so here, then move the updated menu to the bottom of the
            # topic, which is where the owner is going to look. Moved rather than sent again, so
            # the "open private chat" screen left behind there goes away with it.
            await update.effective_chat.send_message(t(lang, "add_done_go_back"), parse_mode=ParseMode.HTML)
            await self._enter_folder_view(owner_id, linked_folder)
            await self._refresh_menu(owner_id, force=True)
        else:
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
    def _report_for(self, owner_id: int, folder_id: int | None = None):
        async def report(event: MasterEvent, results: list[FollowerResult]) -> None:
            await self._report_event(owner_id, event, results, folder_id)

        return report

    async def _exchange_for(self, owner_id: int, folder_id: int | None = None) -> str:
        """The venue of a given folder, or of the one on screen when none is named."""
        folder = await self._store.get_folder(
            folder_id if folder_id is not None else self._folder(owner_id), owner_id
        )
        return folder.exchange if folder else EXCHANGE_MEXC

    def _notice_for(self, owner_id: int, folder_id: int | None = None):
        async def notice(text: str) -> None:
            if not self._app:
                return
            folder, view = await self._enter_folder_view(owner_id, folder_id)
            if view.place.is_group:
                text = self._whose(owner_id, folder) + NEWLINE + text
            try:
                # _send falls back to plain text when exchange text breaks the HTML.
                await self._send(view.place, owner_id, text, parse_mode=ParseMode.HTML)
            except Exception:  # noqa: BLE001 - a notice must never take the service down with it
                LOGGER.warning("could not deliver a notice to %s", owner_id, exc_info=True)
            await self._refresh_menu(owner_id)

        return notice

    async def _report_event(
        self, owner_id: int, event: MasterEvent, results: list[FollowerResult], folder_id: int | None = None
    ) -> None:
        if not self._app:
            return
        folder, view = await self._enter_folder_view(owner_id, folder_id)
        notional = None
        try:
            # The folder the event came from, not the one on screen: a folder on another venue
            # keeps copying in the background. Its size units matter here - a HIBT size read against
            # MEXC's contract size (0.01 ETH) would report a $450 trade as $4.50.
            exchange = folder.exchange if folder else EXCHANGE_MEXC
            async with aiohttp.ClientSession() as session:
                specs = await contract_specs(session, exchange, event.symbol)
                price = await ticker_price(session, exchange, event.symbol)
            spec = specs.get(event.symbol)
            if spec and price:
                notional = spec.notional(event.delta_vol, price)
        except Exception:  # noqa: BLE001 - a missing price must not suppress the trade report
            LOGGER.debug("could not compute notional for %s", event.symbol)

        text = messages.event_report(event, results, notional, await self._lang(owner_id))
        if view.place.is_group:
            # Everyone in the topic reads this. Whose trade it was is the first thing they need.
            text = self._whose(owner_id, folder) + NEWLINE + text
        try:
            await self._send(view.place, owner_id, text, parse_mode=ParseMode.HTML)
        except Exception:  # noqa: BLE001 - logged; the trade itself already happened
            LOGGER.warning("could not deliver a trade report to %s", owner_id, exc_info=True)
        await self._refresh_menu(owner_id)

    async def _refresh_menu(self, owner_id: int, *, force: bool = False) -> None:
        """Move the menu to the bottom of the place it lives in, brought up to date.

        Telegram has no way to pin a message to the bottom, so the only way to keep the menu in
        front of you is to send it again below whatever just arrived and remove the old copy. The
        new one goes out BEFORE the old one is deleted, so there is never an instant with no menu
        on screen.

        One menu per owner per place: in a forum group the same person keeps a MEXC menu in one
        topic and a HIBT menu in another, and a report for one must not move the other.

        Everything here is best-effort: the trade has already happened, and failing to redraw a
        keyboard must never surface as an error about it.
        """
        if not self._app:
            return
        try:
            view = self._view(owner_id)
        except RuntimeError:
            return
        key = (owner_id, view.exchange)

        now = time.monotonic()
        if not force and now - self._menu_moved.get(key, 0.0) < MENU_MOVE_COOLDOWN:
            # A trade can produce a report and a notice within the same second. Moving the menu
            # for each would leave a trail of them up the chat.
            return
        self._menu_moved[key] = now

        previous = self._menu_message.get(key)
        try:
            service = await self._registry.get(view.folder_id, owner_id)
            await service.refresh_account_status()
            self._invalidate_balances(owner_id)
            text, keyboard = await self._menu_view(owner_id)
            sent = await self._send(view.place, owner_id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            if sent:
                self._menu_message[key] = (sent.chat_id, sent.message_id)
        except Exception:  # noqa: BLE001 - a redraw must not report itself as a trade failure
            LOGGER.info("could not move the menu for %s", owner_id, exc_info=True)
            return

        if previous:
            with contextlib.suppress(Exception):
                await self._app.bot.delete_message(chat_id=previous[0], message_id=previous[1])
