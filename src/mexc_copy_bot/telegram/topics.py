"""Running inside a forum group: which exchange a topic is, and whose screen a message is.

The bot started life in private chats, where "the chat" and "the person" are the same thing: one
owner, one menu, one place for their reports. A forum group breaks that in two ways at once.

  · One person has several places. The same owner can have a menu open in the MEXC topic and
    another in the HIBT topic, and each must show that exchange's folders — so "which folder is this
    person looking at" is no longer a property of the person, but of the person IN a place.

  · One place has several people. Everyone in the group sees every menu the bot posts. A button is
    pressed by whoever taps it, so a menu has to know who it belongs to, or one member pressing
    Positions on another's menu would replace that menu with their own accounts.

`View` is the answer to the first: owner + exchange + where. It lives in a ContextVar rather than in
a field on the bot because the bot is asynchronous — a trade report arriving mid-handler must not be
able to swap which folder a START button acts on. Each asyncio task gets its own copy of the
variable, so a handler and a background report can never see each other's view.

Nothing here talks to Telegram or the database, so all of it is tested directly.
"""

from __future__ import annotations

import re
from contextvars import ContextVar
from dataclasses import dataclass, replace

from ..db.store import FOLLOWER, MASTER
from ..exchange import EXCHANGE_HIBT, EXCHANGE_MEXC, EXCHANGES

# Words a topic title is recognised by, written the way people actually name them — Latin or
# Cyrillic, any case. A title is split into words first, so "🟢 MEXC copy" matches and "mexcoin"
# does not.
TITLE_WORDS: dict[str, str] = {
    "mexc": EXCHANGE_MEXC,
    "мекс": EXCHANGE_MEXC,
    "мэкс": EXCHANGE_MEXC,
    "hibt": EXCHANGE_HIBT,
    "хібт": EXCHANGE_HIBT,
    "хибт": EXCHANGE_HIBT,
    "хiбт": EXCHANGE_HIBT,  # Latin "i" inside a Cyrillic word, which a phone keyboard produces
}


def exchange_from_title(title: str | None) -> str | None:
    """The exchange a topic title names, or None when it names none — or both.

    Both is refused rather than resolved: a topic called "MEXC vs HIBT" is a conversation about the
    two, and quietly binding it to whichever word came first would put one exchange's trades in it.
    """
    words = set(re.split(r"[^\w]+", (title or "").casefold()))
    found = {TITLE_WORDS[word] for word in words if word in TITLE_WORDS}
    return found.pop() if len(found) == 1 else None


def parse_exchange(text: str | None) -> str | None:
    """An exchange as typed after /bind: `mexc`, `HIBT`, `мекс`."""
    value = (text or "").strip().casefold()
    if value in EXCHANGES:
        return value
    return TITLE_WORDS.get(value)


@dataclass(frozen=True)
class Place:
    """Where a message goes: a chat, and a topic inside it when there is one."""

    chat_id: int
    thread_id: int | None = None

    @property
    def is_group(self) -> bool:
        # Telegram gives groups negative ids and people positive ones.
        return self.chat_id < 0


@dataclass(frozen=True)
class View:
    """One owner, looking at one exchange's folders, in one place.

    `exchange` is None in a private chat: there the bot works as it always has, every folder in one
    list. In a topic it is fixed by the topic, and only that exchange's folders exist.
    """

    owner_id: int
    exchange: str | None
    place: Place
    folder_id: int | None = None

    @property
    def in_topic(self) -> bool:
        return self.exchange is not None

    def with_folder(self, folder_id: int) -> "View":
        return replace(self, folder_id=folder_id)


CURRENT_VIEW: ContextVar[View | None] = ContextVar("copy_bot_view", default=None)


# ── entering keys privately ──────────────────────────────────────────────────────────────────────
# A secret typed into a group is read by the whole group, and deleting it a second later does not
# help: members get a notification preview, and deleting someone else's message needs admin rights
# the bot may not have. So in a topic, "add account" hands over to a private chat through a t.me
# deep link, which carries exactly what the private conversation needs to know and nothing secret.
ADD_LINK_PATTERN = re.compile(r"^add_(mexc|hibt)_(m|f)_(\d+)$")
ADD_LINK_COMMAND = re.compile(r"^/start add_(mexc|hibt)_(m|f)_\d+$")


def add_payload(exchange: str, kind: str, folder_id: int) -> str:
    """The deep-link payload for adding an account. Telegram allows [A-Za-z0-9_-], 64 characters."""
    return f"add_{exchange}_{'m' if kind == MASTER else 'f'}_{int(folder_id)}"


def parse_add_payload(payload: str | None) -> tuple[str, str, int] | None:
    """(exchange, kind, folder id) from a payload, or None if it is not one of ours.

    The folder id is only a pointer. Whoever follows the link is still checked against it: the
    folder must belong to them, and be on the exchange the link names.
    """
    match = ADD_LINK_PATTERN.match((payload or "").strip())
    if not match:
        return None
    exchange, kind, folder_id = match.groups()
    return exchange, (MASTER if kind == "m" else FOLLOWER), int(folder_id)


def add_link(bot_username: str, exchange: str, kind: str, folder_id: int) -> str:
    return f"https://t.me/{bot_username}?start={add_payload(exchange, kind, folder_id)}"
