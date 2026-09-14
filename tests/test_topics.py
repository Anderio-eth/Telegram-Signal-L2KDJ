"""The bot inside a forum group: MEXC in one topic, HIBT in another, every member on their own.

These drive the real handlers with real telegram objects. Only the two things at the edges are
fakes — the database, and Telegram's servers — so what is under test is exactly the wiring that
decides where a person is, what they may see there, and where the bot's own messages land.

Each user runs the bot in a forum group of their own. The failures guarded against here are all quiet
ones: somebody acting in a group that is not theirs, a report that lands in General or at a group the
bot was removed from, a folder of the wrong exchange opening in a topic, API keys typed into a group,
a rename answered in the wrong topic renaming the wrong folder.
"""

from __future__ import annotations

import asyncio
import itertools
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telegram import (
    CallbackQuery,
    Chat,
    ChatMemberAdministrator,
    ChatMemberLeft,
    ChatMemberMember,
    ChatMemberUpdated,
    ForumTopic,
    ForumTopicCreated,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
    Update,
    User,
)
from telegram.error import BadRequest, Forbidden
from telegram.ext import ConversationHandler

import mexc_copy_bot.telegram.bot as bot_module
from mexc_copy_bot.core.events import Action, MasterEvent
from mexc_copy_bot.db.store import FOLLOWER, MASTER, Folder
from mexc_copy_bot.exchange import EXCHANGE_HIBT, EXCHANGE_MEXC
from mexc_copy_bot.telegram import topics
from mexc_copy_bot.telegram.bot import ASK_FOLDER_NAME, ASK_KEY, CopyBot
from mexc_copy_bot.telegram.i18n import t
from mexc_copy_bot.telegram.topics import CURRENT_VIEW, Place, View

ANNA, BORYS, STRANGER = 111, 222, 333
GROUP = -100500
MEXC_TOPIC, HIBT_TOPIC, CHAT_TOPIC = 11, 22, 33
NOW = datetime.now(timezone.utc)


# ── pure parts ───────────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("MEXC", EXCHANGE_MEXC), ("мекс", EXCHANGE_MEXC), ("🟢 Mexc copy", EXCHANGE_MEXC),
        ("HIBT", EXCHANGE_HIBT), ("Хібт", EXCHANGE_HIBT), ("хiбт", EXCHANGE_HIBT),
        ("MEXC vs HIBT", None), ("mexcoin", None), ("Chat", None), (None, None),
    ],
)
def test_topic_titles(title, expected):
    assert topics.exchange_from_title(title) == expected


def test_add_link_round_trips_and_rejects_anything_else():
    payload = topics.add_payload(EXCHANGE_HIBT, FOLLOWER, 42)
    assert payload == "add_hibt_f_42"
    assert topics.parse_add_payload(payload) == (EXCHANGE_HIBT, FOLLOWER, 42)
    assert topics.parse_add_payload("add_mexc_m_7") == (EXCHANGE_MEXC, MASTER, 7)
    for junk in ("", "add_binance_f_1", "add_hibt_x_1", "add_hibt_f_", "add_hibt_f_1;drop"):
        assert topics.parse_add_payload(junk) is None


# ── fakes at the edges ───────────────────────────────────────────────────────────────────────────
class FakeStore:
    def __init__(self):
        self.folders: dict[int, Folder] = {}
        self.ids = itertools.count(1)
        self.active: dict[int, int] = {}
        self.topics: dict[tuple[int, int], tuple[str, str | None]] = {}
        self.views: dict[tuple[int, str], dict] = {}
        self.screens: dict[tuple[int, int], int] = {}
        self.groups: dict[int, int] = {}

    async def group_owner(self, chat_id):
        return self.groups.get(chat_id)

    async def claim_group(self, chat_id, owner_id, title):
        return self.groups.setdefault(chat_id, owner_id)

    async def release_group(self, chat_id):
        self.groups.pop(chat_id, None)
        self.topics = {k: v for k, v in self.topics.items() if k[0] != chat_id}
        self.views = {k: v for k, v in self.views.items() if v.get("chat_id") != chat_id}

    async def get_language(self, owner_id):
        return "uk"

    async def active_folder_id(self, owner_id):
        return self.active.get(owner_id)

    async def set_active_folder(self, owner_id, folder_id):
        self.active[owner_id] = folder_id

    async def create_folder(self, owner_id, name, exchange=EXCHANGE_MEXC):
        folder_id = next(self.ids)
        self.folders[folder_id] = Folder(folder_id, owner_id, name, False, "COPY", None, exchange=exchange)
        return folder_id

    async def get_folder(self, folder_id, owner_id):
        folder = self.folders.get(folder_id)
        return folder if folder and folder.owner_id == owner_id else None

    async def list_folders(self, owner_id, exchange=None):
        return [f for f in self.folders.values() if f.owner_id == owner_id and exchange in (None, f.exchange)]

    async def rename_folder(self, folder_id, owner_id, name):
        raise AssertionError("a rename must not happen in these tests")

    async def list_accounts(self, folder_id, kind=None):
        return []

    async def get_master(self, folder_id):
        return None

    async def get_mode(self, folder_id):
        return "COPY", None

    async def list_stuck_groups(self, owner_id):
        return []

    async def recent_events(self, owner_id, limit=10, exchange=None):
        return []

    async def topic_exchange(self, chat_id, thread_id):
        entry = self.topics.get((chat_id, thread_id))
        return entry[0] if entry else None

    async def bind_topic(self, chat_id, thread_id, exchange, title, bound_by):
        self.topics[(chat_id, thread_id)] = (exchange, title)

    async def unbind_topic(self, chat_id, thread_id):
        return self.topics.pop((chat_id, thread_id), None) is not None

    async def list_topics(self, chat_id):
        return [(thread, ex, title) for (chat, thread), (ex, title) in self.topics.items() if chat == chat_id]

    async def remember_view(self, owner_id, exchange, chat_id, thread_id, display_name):
        row = self.views.setdefault((owner_id, exchange), {"folder_id": None})
        row.update(chat_id=chat_id, thread_id=thread_id, display_name=display_name)

    async def topic_view(self, owner_id, exchange):
        row = self.views.get((owner_id, exchange))
        return dict(row) if row else None

    async def set_view_folder(self, owner_id, exchange, folder_id):
        self.views[(owner_id, exchange)]["folder_id"] = folder_id

    async def record_screen_owner(self, chat_id, message_id, owner_id):
        self.screens[(chat_id, message_id)] = owner_id

    async def screen_owner(self, chat_id, message_id):
        return self.screens.get((chat_id, message_id))


class FakeService:
    running = False
    master_connected = False
    account_status: dict = {}
    account_sides: dict = {}

    async def refresh_account_status(self):
        return None


class FakeRegistry:
    def configure_callbacks(self, **_kwargs):
        pass

    async def get(self, folder_id, owner_id):
        return FakeService()


class FakeTelegram:
    """Telegram's servers: records what the bot sends, edits, deletes and answers."""

    username = "copybot"
    id = 999

    def __init__(self):
        self.sent: list[Message] = []
        self.edits: list[SimpleNamespace] = []
        self.deleted: list[tuple[int, int]] = []
        self.alerts: list[str | None] = []
        self.ids = itertools.count(1000)
        self.missing_threads: set[int] = set()
        self.kicked_from: set[int] = set()
        self.left: list[int] = []
        self.created_topics: list[str] = []

    async def send_message(self, chat_id, text, message_thread_id=None, reply_markup=None, **_kw):
        if chat_id in self.kicked_from:
            raise Forbidden("Forbidden: bot was kicked from the supergroup chat")
        if message_thread_id in self.missing_threads:
            raise BadRequest("Message thread not found")
        chat = Chat(id=chat_id, type=Chat.SUPERGROUP if chat_id < 0 else Chat.PRIVATE, is_forum=chat_id < 0)
        message = Message(
            message_id=next(self.ids), date=NOW, chat=chat, text=text, reply_markup=reply_markup,
            message_thread_id=message_thread_id, is_topic_message=message_thread_id is not None,
        )
        message.set_bot(self)
        self.sent.append(message)
        return message

    async def edit_message_text(self, text, chat_id=None, message_id=None, reply_markup=None, **_kw):
        self.edits.append(SimpleNamespace(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup))
        return True

    async def answer_callback_query(self, callback_query_id, text=None, show_alert=None, **_kw):
        self.alerts.append(text)
        return True

    async def delete_message(self, chat_id, message_id, **_kw):
        self.deleted.append((chat_id, message_id))
        return True

    async def create_forum_topic(self, chat_id, name, **_kw):
        self.created_topics.append(name)
        return ForumTopic(message_thread_id=500 + len(self.created_topics), name=name, icon_color=0)

    async def leave_chat(self, chat_id, **_kw):
        self.left.append(chat_id)
        return True


class _Settings:
    bot_token = "123456:AAHfake-token-for-dispatch-tests"
    allowed_user_ids = frozenset({ANNA, BORYS})
    max_followers = 9


@pytest.fixture
def world(monkeypatch):
    store, telegram = FakeStore(), FakeTelegram()
    copybot = CopyBot(_Settings(), store=store, registry=FakeRegistry())
    copybot._app = SimpleNamespace(bot=telegram)

    rights = SimpleNamespace(admin=True, manage_topics=True)

    async def bot_rights(_chat):
        return rights
    monkeypatch.setattr(copybot, "_bot_rights", bot_rights)

    # Reports must not reach a real exchange for prices.
    async def no_price(*_a, **_kw):
        raise RuntimeError("offline")
    monkeypatch.setattr(bot_module, "contract_specs", no_price)
    monkeypatch.setattr(bot_module, "ticker_price", no_price)
    return SimpleNamespace(bot=copybot, store=store, telegram=telegram, rights=rights)


def command_entities(text):
    """Telegram marks a command with an entity; handlers match on that, not on the leading slash."""
    if not text or not text.startswith("/"):
        return None
    return [MessageEntity(type=MessageEntity.BOT_COMMAND, offset=0, length=len(text.split()[0]))]


def user(user_id):
    return User(id=user_id, first_name={ANNA: "Anna", BORYS: "Borys"}.get(user_id, "Stranger"), is_bot=False)


def topic_message(telegram, who, text, thread, title=None, chat_id=GROUP, forum=True):
    chat = Chat(id=chat_id, type=Chat.SUPERGROUP, is_forum=forum, title="Anna's desk")
    root = None
    if title is not None:
        # Telegram attaches the topic's creation message to posts inside it; that is where the
        # title comes from.
        root = Message(message_id=thread, date=NOW, chat=chat, forum_topic_created=ForumTopicCreated(title, 0))
    message = Message(
        message_id=next(telegram.ids), date=NOW, chat=chat, from_user=user(who), text=text,
        message_thread_id=thread, is_topic_message=thread is not None, reply_to_message=root,
        entities=command_entities(text),
    )
    message.set_bot(telegram)
    return Update(update_id=next(telegram.ids), message=message)


def private_message(telegram, who, text):
    chat = Chat(id=who, type=Chat.PRIVATE)
    message = Message(
        message_id=next(telegram.ids), date=NOW, chat=chat, from_user=user(who), text=text,
        entities=command_entities(text),
    )
    message.set_bot(telegram)
    return Update(update_id=next(telegram.ids), message=message)


def press(telegram, who, data, on: Message):
    query = CallbackQuery(id=str(next(telegram.ids)), from_user=user(who), chat_instance="x", data=data, message=on)
    query.set_bot(telegram)
    return Update(update_id=next(telegram.ids), callback_query=query)


def context(args=None):
    return SimpleNamespace(args=args or [], user_data={})


def run(coro):
    return asyncio.run(coro)


def menu_in(world, owner, thread):
    """The menu the bot last sent this owner in a topic."""
    return next(
        m for m in reversed(world.telegram.sent)
        if m.message_thread_id == thread and isinstance(m.reply_markup, InlineKeyboardMarkup)
        and world.store.screens.get((m.chat_id, m.message_id)) == owner
    )


def buttons(markup):
    return [b for row in markup.inline_keyboard for b in row]


# ── where the bot works ──────────────────────────────────────────────────────────────────────────
def test_a_private_chat_works_as_it_always_did(world):
    run(world.bot._cmd_start(private_message(world.telegram, ANNA, "/menu"), context()))

    menu = world.telegram.sent[-1]
    assert menu.chat_id == ANNA and menu.message_thread_id is None
    assert [f.name for f in world.store.folders.values()] == ["MEXC"]
    assert world.store.views == {}  # no topic involved


def test_general_is_not_a_place_to_trade(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", thread=None), context()))
    assert not world.store.folders
    assert "гілку" in world.telegram.sent[-1].text


def test_a_topic_named_after_an_exchange_binds_itself(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))

    assert world.store.topics[(GROUP, HIBT_TOPIC)][0] == EXCHANGE_HIBT
    menu = menu_in(world, ANNA, HIBT_TOPIC)
    assert "Anna" in menu.text and "HIBT" in menu.text
    [folder] = world.store.folders.values()
    assert folder.exchange == EXCHANGE_HIBT


def test_an_unrecognised_topic_asks_to_be_bound_and_bind_does_it(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", CHAT_TOPIC, title="Chat"), context()))
    assert not world.store.folders
    assert "/bind" in world.telegram.sent[-1].text

    run(world.bot._cmd_bind(topic_message(world.telegram, ANNA, "/bind mexc", CHAT_TOPIC, title="Chat"), context(["mexc"])))
    assert world.store.topics[(GROUP, CHAT_TOPIC)][0] == EXCHANGE_MEXC
    assert menu_in(world, ANNA, CHAT_TOPIC)


def test_bind_is_refused_outside_a_topic(world):
    run(world.bot._cmd_bind(private_message(world.telegram, ANNA, "/bind hibt"), context(["hibt"])))
    assert not world.store.topics


def test_each_topic_shows_only_its_own_exchange(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", MEXC_TOPIC, title="MEXC"), context()))
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))

    mexc_folder = world.store.views[(ANNA, EXCHANGE_MEXC)]["folder_id"]
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]
    assert world.store.folders[mexc_folder].exchange == EXCHANGE_MEXC
    assert world.store.folders[hibt_folder].exchange == EXCHANGE_HIBT

    # The folders screen in the HIBT topic lists HIBT folders only, and offers no venue switch.
    hibt_menu = menu_in(world, ANNA, HIBT_TOPIC)
    run(world.bot._on_button(press(world.telegram, ANNA, "folders", on=hibt_menu), context()))
    screen = world.telegram.edits[-1]
    labels = [b.text for b in buttons(screen.reply_markup)]
    assert any("HIBT" in label for label in labels)
    assert not any("MEXC" in label for label in labels)
    assert not any(b.callback_data and b.callback_data.startswith("fexch:") for b in buttons(screen.reply_markup))


def test_a_folder_from_another_exchange_cannot_be_opened_in_a_topic(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", MEXC_TOPIC, title="MEXC"), context()))
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    mexc_folder = world.store.views[(ANNA, EXCHANGE_MEXC)]["folder_id"]
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    # A hand-made or stale callback naming the MEXC folder, pressed in the HIBT topic.
    run(world.bot._on_button(press(world.telegram, ANNA, f"fsel:{mexc_folder}", on=menu_in(world, ANNA, HIBT_TOPIC)), context()))
    assert world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"] == hibt_folder


# ── one group, one owner ─────────────────────────────────────────────────────────────────────────
def test_a_group_belongs_to_whoever_set_it_up_first(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    run(world.bot._cmd_start(topic_message(world.telegram, BORYS, "/menu", HIBT_TOPIC, title="HIBT"), context()))

    assert world.store.groups[GROUP] == ANNA
    assert {f.owner_id for f in world.store.folders.values()} == {ANNA}  # nothing was made for Borys
    assert world.telegram.sent[-1].text == t("uk", "group_not_yours")


def test_nobody_else_can_press_anything_in_an_owned_group(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    annas_menu = menu_in(world, ANNA, HIBT_TOPIC)
    edits_before = len(world.telegram.edits)

    run(world.bot._on_button(press(world.telegram, BORYS, "positions", on=annas_menu), context()))

    assert len(world.telegram.edits) == edits_before
    assert world.telegram.alerts[-1] == t("uk", "group_not_yours")


def test_nobody_else_can_rebind_an_owned_groups_topics(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    run(world.bot._cmd_bind(topic_message(world.telegram, BORYS, "/bind mexc", HIBT_TOPIC, title="HIBT"), context(["mexc"])))
    assert world.store.topics[(GROUP, HIBT_TOPIC)][0] == EXCHANGE_HIBT


def test_a_button_left_in_an_unowned_group_does_not_claim_it(world):
    stale = Message(message_id=1, date=NOW, chat=Chat(id=GROUP, type=Chat.SUPERGROUP, is_forum=True),
                    message_thread_id=HIBT_TOPIC, is_topic_message=True)
    stale.set_bot(world.telegram)
    run(world.bot._on_button(press(world.telegram, BORYS, "menu", on=stale), context()))
    assert GROUP not in world.store.groups


def test_someone_not_on_the_whitelist_is_refused_in_a_topic_too(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    run(world.bot._on_button(press(world.telegram, STRANGER, "menu", on=menu_in(world, ANNA, HIBT_TOPIC)), context()))
    assert world.telegram.alerts[-1] == "Немає доступу"


# ── adding the bot to a group, /setup, removing it ───────────────────────────────────────────────
def membership(telegram, who, old, new, chat_id=GROUP):
    bot_user = User(id=telegram.id, first_name="copybot", is_bot=True)
    statuses = {"left": ChatMemberLeft(bot_user), "member": ChatMemberMember(bot_user)}
    admin = ChatMemberAdministrator(
        bot_user, can_be_edited=False, is_anonymous=False, can_manage_chat=True, can_delete_messages=True,
        can_manage_video_chats=False, can_restrict_members=False, can_promote_members=False,
        can_change_info=False, can_invite_users=False, can_post_stories=False, can_edit_stories=False,
        can_delete_stories=False, can_manage_topics=True,
    )
    statuses["administrator"] = admin
    change = ChatMemberUpdated(
        chat=Chat(id=chat_id, type=Chat.SUPERGROUP, is_forum=True, title="Anna's desk"),
        from_user=user(who), date=NOW, old_chat_member=statuses[old], new_chat_member=statuses[new],
    )
    return Update(update_id=next(telegram.ids), my_chat_member=change)


def test_adding_the_bot_makes_the_group_yours_and_says_how_to_set_it_up(world):
    run(world.bot._on_membership(membership(world.telegram, ANNA, "left", "member"), context()))
    assert world.store.groups[GROUP] == ANNA
    assert "/setup" in world.telegram.sent[-1].text and "Anna" in world.telegram.sent[-1].text


def test_the_bot_leaves_a_group_it_was_added_to_by_someone_without_access(world):
    run(world.bot._on_membership(membership(world.telegram, STRANGER, "left", "member"), context()))
    assert world.telegram.left == [GROUP]
    assert GROUP not in world.store.groups


def test_being_made_admin_prompts_setup(world):
    run(world.bot._on_membership(membership(world.telegram, ANNA, "left", "member"), context()))
    run(world.bot._on_membership(membership(world.telegram, ANNA, "member", "administrator"), context()))
    assert world.telegram.sent[-1].text == t("uk", "bot_promoted")


def test_setup_creates_both_topics_and_binds_them(world):
    run(world.bot._cmd_setup(topic_message(world.telegram, ANNA, "/setup", thread=None), context()))

    assert sorted(world.telegram.created_topics) == ["HIBT", "MEXC"]
    assert sorted(ex for ex, _ in world.store.topics.values()) == [EXCHANGE_HIBT, EXCHANGE_MEXC]
    assert world.store.groups[GROUP] == ANNA
    # Run again: nothing is created twice.
    run(world.bot._cmd_setup(topic_message(world.telegram, ANNA, "/setup", thread=None), context()))
    assert len(world.telegram.created_topics) == 2


def test_setup_only_creates_what_is_missing(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", MEXC_TOPIC, title="MEXC"), context()))
    run(world.bot._cmd_setup(topic_message(world.telegram, ANNA, "/setup", thread=None), context()))
    assert world.telegram.created_topics == ["HIBT"]


def test_a_topic_created_by_setup_is_not_announced_twice(world):
    run(world.bot._cmd_setup(topic_message(world.telegram, ANNA, "/setup", thread=None), context()))
    thread = next(th for (chat, th), (ex, _) in world.store.topics.items() if ex == EXCHANGE_HIBT)
    sent_before = len(world.telegram.sent)

    event = topic_message(world.telegram, ANNA, None, thread)
    event.message._unfreeze()
    event.message.forum_topic_created = ForumTopicCreated("HIBT", 0)
    run(world.bot._on_topic_event(event, context()))

    assert len(world.telegram.sent) == sent_before


@pytest.mark.parametrize(
    ("forum", "admin", "manage_topics", "expected"),
    [
        (False, True, True, "setup_not_forum"),
        (True, False, False, "setup_need_admin"),
        (True, True, False, "setup_need_topics_right"),
    ],
)
def test_setup_says_which_switch_is_still_off(world, forum, admin, manage_topics, expected):
    world.rights.admin, world.rights.manage_topics = admin, manage_topics
    run(world.bot._cmd_setup(topic_message(world.telegram, ANNA, "/setup", thread=None, forum=forum), context()))
    assert world.telegram.created_topics == []
    assert world.telegram.sent[-1].text == t("uk", expected, names="MEXC, HIBT")


def test_removing_the_bot_releases_the_group_and_reports_go_privately(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    run(world.bot._on_membership(membership(world.telegram, ANNA, "member", "left"), context()))
    assert GROUP not in world.store.groups and not world.store.topics

    async def background():
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())
    report = next(m for m in world.telegram.sent if "ETH_USDT" in (m.text or ""))
    assert (report.chat_id, report.message_thread_id) == (ANNA, None)


def test_a_group_that_throws_the_bot_out_without_notice_does_not_swallow_reports(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]
    world.telegram.kicked_from.add(GROUP)

    async def background():
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())
    report = next(m for m in world.telegram.sent if "ETH_USDT" in (m.text or ""))
    assert (report.chat_id, report.message_thread_id) == (ANNA, None)


# ── keys never go into the group ─────────────────────────────────────────────────────────────────
def test_adding_an_account_in_a_topic_hands_over_to_a_private_chat(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    folder_id = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    state = run(world.bot._begin_add(press(world.telegram, ANNA, "add_follower", on=menu_in(world, ANNA, HIBT_TOPIC)), context()))

    assert state == ConversationHandler.END  # nothing is waiting for a key in the group
    link = next(b.url for b in buttons(world.telegram.edits[-1].reply_markup) if b.url)
    assert link == f"https://t.me/copybot?start=add_hibt_f_{folder_id}"


def test_the_private_link_opens_the_key_flow_for_that_folder(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    folder_id = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]
    ctx = context([f"add_hibt_f_{folder_id}"])

    state = run(world.bot._begin_add_from_link(private_message(world.telegram, ANNA, f"/start add_hibt_f_{folder_id}"), ctx))

    assert state == ASK_KEY
    assert ctx.user_data["add_folder_id"] == folder_id and ctx.user_data["kind"] == FOLLOWER
    assert "HIBT" in world.telegram.sent[-1].text


@pytest.mark.parametrize("who, payload_for", [(BORYS, "annas folder"), (ANNA, "wrong exchange")])
def test_a_link_to_a_folder_that_is_not_yours_or_not_that_exchange_goes_nowhere(world, who, payload_for):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    folder_id = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]
    payload = f"add_hibt_f_{folder_id}" if payload_for == "annas folder" else f"add_mexc_f_{folder_id}"
    ctx = context([payload])

    state = run(world.bot._begin_add_from_link(private_message(world.telegram, who, f"/start {payload}"), ctx))

    assert state == ConversationHandler.END
    assert "add_folder_id" not in ctx.user_data


def test_a_start_link_reaches_the_add_flow_not_the_menu():
    """Dispatch order: the plain /start handler comes first and must let an add link through."""
    copybot = CopyBot(_Settings(), store=None, registry=FakeRegistry())
    app = copybot.build()
    telegram = FakeTelegram()
    update = private_message(telegram, ANNA, "/start add_hibt_f_5")

    first_start = next(h for h in app.handlers[0] if getattr(h, "commands", None) == frozenset({"start"}))
    conversation = next(h for h in app.handlers[0] if hasattr(h, "entry_points"))
    assert not first_start.check_update(update)
    assert conversation.check_update(update)
    # An ordinary /start still opens the menu.
    assert first_start.check_update(private_message(telegram, ANNA, "/start"))


# ── answers typed in the wrong topic ─────────────────────────────────────────────────────────────
def test_a_name_typed_in_another_topic_does_not_answer_the_question(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", MEXC_TOPIC, title="MEXC"), context()))
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    ctx = context()
    run(world.bot._begin_folder_name(press(world.telegram, ANNA, "fren", on=menu_in(world, ANNA, MEXC_TOPIC)), ctx))

    # Typed in the HIBT topic: still waiting, and FakeStore.rename_folder would have raised.
    state = run(world.bot._got_folder_name(topic_message(world.telegram, ANNA, "New name", HIBT_TOPIC, title="HIBT"), ctx))
    assert state == ASK_FOLDER_NAME


# ── reports and the menu ─────────────────────────────────────────────────────────────────────────
def _event():
    return MasterEvent(Action.OPEN, "ETH_USDT", 1, 0.36, 0.36, 10, 2, "k1")


def test_a_report_goes_to_its_exchanges_topic_and_names_the_owner(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    async def background():  # a service task: no handler, no view of its own
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())

    report = next(m for m in world.telegram.sent if "ETH_USDT" in (m.text or ""))
    assert (report.chat_id, report.message_thread_id) == (GROUP, HIBT_TOPIC)
    assert "Anna" in report.text


def test_a_report_for_an_exchange_never_used_in_a_topic_goes_privately(world):
    folder_id = run(world.store.create_folder(ANNA, "MEXC", EXCHANGE_MEXC))

    async def background():
        await world.bot._report_for(ANNA, folder_id)(_event(), [])
    run(background())

    report = next(m for m in world.telegram.sent if "ETH_USDT" in (m.text or ""))
    assert (report.chat_id, report.message_thread_id) == (ANNA, None)


def test_a_deleted_topic_does_not_swallow_the_report(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]
    world.telegram.missing_threads.add(HIBT_TOPIC)

    async def background():
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())

    report = next(m for m in world.telegram.sent if "ETH_USDT" in (m.text or ""))
    assert (report.chat_id, report.message_thread_id) == (ANNA, None)


def test_the_menu_moves_to_the_bottom_after_a_report(world):
    """Asked for, committed, and never live: an older copy of _refresh_menu below it won."""
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    old_menu = menu_in(world, ANNA, HIBT_TOPIC)
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    async def background():
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())

    new_menu = menu_in(world, ANNA, HIBT_TOPIC)
    assert new_menu.message_id != old_menu.message_id
    assert world.telegram.sent.index(new_menu) > next(
        i for i, m in enumerate(world.telegram.sent) if "ETH_USDT" in (m.text or "")
    )
    assert (GROUP, old_menu.message_id) in world.telegram.deleted


def test_a_hibt_report_does_not_move_the_mexc_menu(world):
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", MEXC_TOPIC, title="MEXC"), context()))
    run(world.bot._cmd_start(topic_message(world.telegram, ANNA, "/menu", HIBT_TOPIC, title="HIBT"), context()))
    mexc_menu = menu_in(world, ANNA, MEXC_TOPIC)
    hibt_folder = world.store.views[(ANNA, EXCHANGE_HIBT)]["folder_id"]

    async def background():
        await world.bot._report_for(ANNA, hibt_folder)(_event(), [])
    run(background())

    assert (GROUP, mexc_menu.message_id) not in world.telegram.deleted
    assert menu_in(world, ANNA, MEXC_TOPIC).message_id == mexc_menu.message_id


def test_only_one_refresh_and_one_report_method_exist():
    from pathlib import Path

    source = Path(bot_module.__file__).read_text(encoding="utf-8")
    assert source.count("    async def _refresh_menu(") == 1
    assert source.count("    async def _report_event(") == 1


def test_a_background_report_cannot_change_the_folder_a_handler_is_acting_on(world):
    """The reason the view is a ContextVar and not a field on the bot."""
    handler_view = View(ANNA, EXCHANGE_MEXC, Place(GROUP, MEXC_TOPIC), folder_id=1)
    other_folder = run(world.store.create_folder(ANNA, "HIBT", EXCHANGE_HIBT))

    async def handler():
        CURRENT_VIEW.set(handler_view)
        # A report for another folder runs as its own task in the middle of this handler.
        await asyncio.create_task(world.bot._enter_folder_view(ANNA, other_folder))
        return CURRENT_VIEW.get()

    assert run(handler()) == handler_view


def test_a_handler_without_a_view_refuses_rather_than_guessing(world):
    async def orphan():
        world.bot._folder(ANNA)

    with pytest.raises(RuntimeError):
        run(orphan())
