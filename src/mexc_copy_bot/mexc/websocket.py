"""Private websocket for the master account.

Connects to wss://contract.mexc.com/edge, authenticates, and forwards `push.personal.position`
frames. That channel carries the position STATE (holdVol, leverage, positionType, state), which is
what makes correct copying possible: three separate master orders that add up to one position
produce position pushes we can compare against the last known size, rather than three independent
"open" instructions (spec §11).

Note the host: reads and websockets live on contract.mexc.com, but ORDER PLACEMENT does not —
see rest.py's BASE_URL comment. The two hosts are not interchangeable.

This object does one thing: keep a socket alive and hand out frames. It never decides what a
frame means and never places an order — that separation is what lets the copy engine be tested
without a socket, and lets a socket failure never turn into a half-executed trade.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from .auth import sign_ws_login

LOGGER = logging.getLogger(__name__)

WS_URL = "wss://contract.mexc.com/edge"

# MEXC closes a connection that goes 60s without a ping; it asks for one every 10-20s.
PING_SECONDS = 15.0
LOGIN_TIMEOUT = 10.0


class MasterWebSocket:
    """Streams the master account's position changes.

    Reconnects on its own, forever, with backoff. After every reconnect it calls `on_resync`
    before resuming: a socket that was down may have missed changes entirely, and resuming from a
    stale idea of the master's position would copy the wrong delta (spec §27).
    """

    def __init__(
        self,
        api_key: str,
        secret: str,
        *,
        on_position: Callable[[dict[str, Any]], Awaitable[None]],
        on_resync: Callable[[], Awaitable[None]] | None = None,
        on_status: Callable[[str], Awaitable[None]] | None = None,
        reconnect_max_seconds: float = 30.0,
    ) -> None:
        self._api_key = api_key
        self._secret = secret
        self._on_position = on_position
        self._on_resync = on_resync
        self._on_status = on_status
        self._reconnect_max = reconnect_max_seconds
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="master-ws")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._connected = False

    async def _status(self, message: str) -> None:
        LOGGER.info("master ws: %s", message)
        if self._on_status:
            with contextlib.suppress(Exception):
                await self._on_status(message)

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._session_loop()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — any failure must lead to a reconnect, not a crash
                self._connected = False
                LOGGER.warning("master ws dropped: %s (retry in %.0fs)", err, backoff)
                await self._status(f"disconnected: {err}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_max)

    async def _session_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL, heartbeat=None, timeout=aiohttp.ClientWSTimeout(ws_close=10)) as ws:
                await ws.send_json(sign_ws_login(self._api_key, self._secret))

                # Wait for the login result before subscribing: a failed auth still leaves the
                # socket open, and silently streaming nothing looks identical to "master is idle".
                deadline = asyncio.get_running_loop().time() + LOGIN_TIMEOUT
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise RuntimeError("login timed out")
                    msg = await ws.receive(timeout=remaining)
                    if msg.type is not aiohttp.WSMsgType.TEXT:
                        raise RuntimeError(f"unexpected frame during login: {msg.type}")
                    payload = json.loads(msg.data)
                    channel = payload.get("channel")
                    if channel == "rs.login":
                        break
                    if channel == "rs.error":
                        raise RuntimeError(f"login rejected: {payload.get('data')}")

                self._connected = True
                await self._status("connected")

                # A reconnect may have missed changes; re-read reality before trusting deltas.
                if self._on_resync:
                    await self._on_resync()

                ping = asyncio.create_task(self._ping_loop(ws), name="master-ws-ping")
                try:
                    await self._read_loop(ws)
                finally:
                    ping.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping
                    self._connected = False

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not ws.closed:
            await asyncio.sleep(PING_SECONDS)
            with contextlib.suppress(Exception):
                await ws.send_json({"method": "ping"})

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type is aiohttp.WSMsgType.ERROR:
                raise RuntimeError(f"socket error: {ws.exception()}")
            if msg.type is not aiohttp.WSMsgType.TEXT:
                continue

            payload = json.loads(msg.data)
            channel = payload.get("channel")
            if channel == "push.personal.position":
                data = payload.get("data") or {}
                # One slow handler must not stall the socket into a server-side timeout, but the
                # handler here is a fast in-memory diff that enqueues work, so awaiting is correct:
                # it also guarantees events are processed in the order the venue sent them.
                await self._on_position(data)
            elif channel == "rs.error":
                LOGGER.warning("master ws error frame: %s", payload.get("data"))
        raise RuntimeError("socket closed by server")
