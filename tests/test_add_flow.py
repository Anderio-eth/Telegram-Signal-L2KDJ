"""Tests for the add-account conversation being escapable.

The bug these cover was reported as "presses the button and nothing happens". A ConversationHandler
whose states only accept text is a trap: once someone starts adding an account and wanders off,
every button they press matches nothing, falls through to a handler with no branch for it, and the
bot looks dead. The only escape was typing /cancel, which nobody knows about.

These run the real handler wiring — no network, because check_update() is pure dispatch logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest  # noqa: E402
from telegram import CallbackQuery, Chat, Message, Update, User  # noqa: E402
from telegram.ext import CallbackQueryHandler, CommandHandler  # noqa: E402

from mexc_copy_bot.telegram.bot import ASK_KEY, CopyBot  # noqa: E402

USER_ID = 4242
CHAT_ID = 4242


class _Settings:
    """Only what build() reads. A real Settings would demand a database and an encryption key."""

    bot_token = "123456:AAHfake-token-for-dispatch-tests"
    allowed_user_ids = frozenset({USER_ID})
    max_followers = 9


class _Registry:
    def configure_callbacks(self, **_kwargs) -> None:
        pass


@pytest.fixture
def conversation():
    bot = CopyBot(_Settings(), store=None, registry=_Registry())
    app = bot.build()
    # The ConversationHandler is the second handler registered (after /start).
    return next(h for h in app.handlers[0] if hasattr(h, "entry_points"))


def _button(data: str) -> Update:
    user = User(id=USER_ID, first_name="Tester", is_bot=False)
    chat = Chat(id=CHAT_ID, type=Chat.PRIVATE)
    message = Message(message_id=1, date=None, chat=chat, from_user=user)
    query = CallbackQuery(id="1", from_user=user, chat_instance="x", data=data, message=message)
    return Update(update_id=1, callback_query=query)


def test_add_master_button_is_an_entry_point(conversation):
    assert conversation.check_update(_button("add_master")) is not None


def test_pressing_add_master_again_while_stuck_restarts_the_flow(conversation):
    """The reported symptom: a half-finished attempt made the button permanently inert."""
    conversation._conversations[(CHAT_ID, USER_ID)] = ASK_KEY
    assert conversation.allow_reentry is True
    assert conversation.check_update(_button("add_master")) is not None


def test_any_other_button_escapes_a_half_finished_add(conversation):
    """Menu, Accounts, Cancel — all must get out, not fall into silence."""
    conversation._conversations[(CHAT_ID, USER_ID)] = ASK_KEY
    for data in ("menu", "accounts", "positions", "emergency"):
        assert conversation.check_update(_button(data)) is not None, data


def test_a_button_press_is_always_acknowledged():
    """An unanswered callback leaves Telegram spinning, which reads as a broken bot."""
    source = (Path(__file__).resolve().parents[1] / "src/mexc_copy_bot/telegram/bot.py").read_text(
        encoding="utf-8"
    )
    begin_add = source.split("async def _begin_add")[1].split("async def ")[0]
    assert "callback_query.answer()" in begin_add


def test_start_and_cancel_both_break_out(conversation):
    handlers = conversation.fallbacks
    commands = {c for h in handlers if isinstance(h, CommandHandler) for c in h.commands}
    assert {"cancel", "start"} <= commands
    assert any(isinstance(h, CallbackQueryHandler) for h in handlers)
