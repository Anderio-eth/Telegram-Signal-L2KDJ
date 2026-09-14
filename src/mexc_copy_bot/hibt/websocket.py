"""HIBT private websocket — real-time pushes of an account's positions, orders and balance.

Verified against the live venue, 2026-09-14:

    connect  wss://fapi.hibt0.com/v2/ws
    <-  {"type":"hello","data":"success"}
    ->  {"event":"auth","accessKey":KEY,"timestamp":MS,"signature":HMAC_SHA256(secret, MS)}
    <-  {"type":"auth","data":"ok"}
    ->  {"event":"sub","topic":"user.position"}          (also user.order, user.balance, user.entrust)
    <-  {"type":"sub","data":{"topic":"user.position","status":"ok"}}
    <-  {"type":"user.position","ts":...,"data":[ {positionID,symbol,side,leverage,price,amount,...} ]}

A position frame carries the symbol, which the REST position read does not give without being asked
per symbol — so this is how the bot learns what an account opened without polling every symbol. An
empty `data:[]` on user.position means that account is now flat.

The signature is just HMAC-SHA256 of the timestamp string; nothing else is signed. No ping is
needed — aiohttp's own heartbeat keeps the socket alive — and the stream reconnects on its own,
re-authenticating and re-subscribing, because a rented box loses its network sometimes and a
position feed that quietly stopped would be worse than one that says it dropped.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import time
from typing import Any, Awaitable, Callable

import aiohttp

LOGGER = logging.getLogger(__name__)

WS_URL = "wss://fapi.hibt0.com/v2/ws"
TOPICS = ("user.position", "user.order", "user.balance")
FrameHandler = Callable[[str, dict[str, Any]], Awaitable[None] | None]


def _sign(secret: str, timestamp: str) -> str:
    return hmac.new(secret.encode(), timestamp.encode(), hashlib.sha256).hexdigest()


class HibtUserStream:
    """One account's private stream. `on_frame(topic, frame)` is called for every data push."""

    def __init__(
        self,
        api_key: str,
        secret: str,
        *,
        session: aiohttp.ClientSession,
        on_frame: FrameHandler,
        topics: tuple[str, ...] = TOPICS,
        reconnect_max_seconds: float = 30.0,
    ) -> None:
        self._api_key = api_key
        self._secret = secret
        self._session = session
        self._on_frame = on_frame
        self._topics = topics
        self._reconnect_max = reconnect_max_seconds
        self._task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="hibt-user-stream")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def wait_connected(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._session_once()
                backoff = 1.0  # a session that actually ran resets the penalty
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — any drop is a reason to wait and retry
                LOGGER.info("hibt stream dropped: %s", err)
            finally:
                self._connected.clear()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._reconnect_max)

    async def _session_once(self) -> None:
        async with self._session.ws_connect(WS_URL, heartbeat=20) as ws:
            timestamp = str(int(time.time() * 1000))
            await ws.send_json({
                "event": "auth", "accessKey": self._api_key,
                "timestamp": timestamp, "signature": _sign(self._secret, timestamp),
            })
            authed = False
            async for message in ws:
                if message.type is not aiohttp.WSMsgType.TEXT:
                    if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    continue
                frame = message.json()
                kind = frame.get("type")
                if kind == "auth":
                    if frame.get("data") != "ok":
                        raise RuntimeError(f"auth refused: {frame}")
                    authed = True
                    for topic in self._topics:
                        await ws.send_json({"event": "sub", "topic": topic})
                    self._connected.set()
                elif kind in self._topics:
                    await self._deliver(kind, frame)
                # "hello", "sub" acknowledgements and anything else are not data; ignored.
            if not authed:
                raise RuntimeError("socket closed before auth")

    async def _deliver(self, topic: str, frame: dict[str, Any]) -> None:
        try:
            result = self._on_frame(topic, frame)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 — a handler bug must not take the stream down
            LOGGER.exception("hibt frame handler failed on %s", topic)
