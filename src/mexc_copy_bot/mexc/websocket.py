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

# A session that stood up this long was working; whatever ended it is a fresh problem and gets a
# fresh short delay. Anything shorter is a socket that will not stay up, and backing off is the
# only thing that stops it becoming a hot loop.
HEALTHY_SESSION_SECONDS = 60.0

# A 403 on the handshake is not a dropped connection, it is this network being refused. MEXC
# blocks contract.mexc.com at the CDN for some address ranges — the REST client already works
# around the same block by using api.mexc.com, and the socket host has no alternative. Retrying is
# still right in case the block lifts, but saying so every thirty seconds fills the log with a
# line that carries no new information and buries the ones that do.
BLOCKED_MARKERS = ("403", "invalid response status")

# MEXC's own docs disagree with what the socket actually sends: the position channel is
# `push.personal.position`, while the docs name the stop channel `push.stop.order`. Both spellings
# are accepted rather than betting on either — the same class of documentation error already cost
# us once, when the documented order host returned 403 and a different one worked.
STOP_CHANNELS = frozenset(
    {"push.personal.stop.order", "push.stop.order", "push.personal.plan.order", "push.plan.order"}
)

# The master's own orders. This is what makes a resting limit visible: the position channel only
# speaks once an order has already filled, by which point mirroring can only be a market order.
ORDER_CHANNELS = frozenset({"push.personal.order", "push.order"})


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
        on_order: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        on_stop_order: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        on_resync: Callable[[], Awaitable[None]] | None = None,
        on_status: Callable[[bool, str], Awaitable[None]] | None = None,
        reconnect_max_seconds: float = 30.0,
    ) -> None:
        self._api_key = api_key
        self._secret = secret
        self._on_position = on_position
        self._on_order = on_order
        self._on_stop_order = on_stop_order
        self._on_resync = on_resync
        self._on_status = on_status
        self._reconnect_max = reconnect_max_seconds
        # Whether the "this network is blocked" explanation has already been given, so it is said
        # once rather than on every retry.
        self._reported_block = False
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._connected = False
        # Lets a caller wait for the socket to be genuinely up. Without it, start() returns
        # while the login is still in flight, and anything rendered straight afterwards
        # reports the master as disconnected — true for about a second, then stuck on screen
        # because a Telegram message does not redraw itself.
        self._connected_event = asyncio.Event()
        # Channels seen but not handled, so each is reported once instead of every frame.
        self._seen_channels: set[str] = set()

    @property
    def connected(self) -> bool:
        return self._connected

    async def wait_connected(self, timeout: float) -> bool:
        """Block until the socket is logged in. False if it did not make it in time."""
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True

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
        self._connected_event.clear()

    async def _status(self, connected: bool, detail: str = "") -> None:
        LOGGER.info(
            "master ws: %s%s",
            "connected" if connected else "disconnected",
            f" ({detail})" if detail else "",
        )
        if self._on_status:
            with contextlib.suppress(Exception):
                await self._on_status(connected, detail)

    async def _run(self) -> None:
        """Keep one master socket up, backing off when it will not stay up.

        The wait applies to EVERY reconnect, not only to one that failed with an exception. MEXC
        closes an idle socket after about a minute and aiohttp reports that as the iterator simply
        ending — no exception at all. This loop used to sleep only in the `except` branch, so a
        venue-side close came back round instantly, and each pass re-connects, re-logs in and calls
        on_resync, which reads positions over REST. A socket that would not stay up therefore
        turned into an unpaced loop of REST calls, which is one way to be told your requests are
        too frequent while doing nothing at all.

        Backoff resets on a session that actually lasted, not on one that merely connected.
        Otherwise connect-then-drop keeps resetting the delay to a second and the loop never slows.
        """
        backoff = 1.0
        while self._running:
            started = asyncio.get_running_loop().time()
            try:
                await self._session_loop()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — any failure must lead to a reconnect, not a crash
                detail = str(err)
            else:
                detail = "closed by the venue"
            self._connected = False
            self._connected_event.clear()

            if asyncio.get_running_loop().time() - started >= HEALTHY_SESSION_SECONDS:
                backoff = 1.0

            blocked = any(marker in detail.lower() for marker in BLOCKED_MARKERS)
            if blocked:
                # Nothing here will change on its own, so go straight to the longest wait rather
                # than climbing to it, and say it once. Positions come over REST regardless.
                backoff = self._reconnect_max
                if not self._reported_block:
                    self._reported_block = True
                    LOGGER.warning(
                        "master ws refused by the venue from this network (%s). "
                        "This is a CDN block on contract.mexc.com, not a credentials problem. "
                        "Copying continues over REST; retrying quietly every %.0fs.",
                        detail, backoff,
                    )
                else:
                    LOGGER.debug("master ws still refused (%s)", detail)
            else:
                self._reported_block = False
                LOGGER.warning("master ws dropped: %s (retry in %.0fs)", detail, backoff)

            await self._status(False, detail)
            await asyncio.sleep(backoff)
            if not blocked:
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
                self._connected_event.set()
                await self._status(True)

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
                    self._connected_event.clear()

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
            if channel in ORDER_CHANNELS:
                if self._on_order:
                    await self._on_order(payload.get("data") or {})
                continue

            if channel in STOP_CHANNELS:
                if self._on_stop_order:
                    await self._on_stop_order(channel, payload.get("data") or {})
                continue

            if channel == "push.personal.position":
                data = payload.get("data") or {}
                # One slow handler must not stall the socket into a server-side timeout, but the
                # handler here is a fast in-memory diff that enqueues work, so awaiting is correct:
                # it also guarantees events are processed in the order the venue sent them.
                await self._on_position(data)
            elif channel == "rs.error":
                LOGGER.warning("master ws error frame: %s", payload.get("data"))
            elif channel and channel not in self._seen_channels:
                # Logged once per channel: this is how we find out what MEXC really sends, rather
                # than trusting docs that have already been wrong about this API.
                self._seen_channels.add(channel)
                LOGGER.info("master ws: first frame on unhandled channel %r: %s", channel, payload)
        raise RuntimeError("socket closed by server")
