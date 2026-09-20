"""Realtime price feed over Hyperliquid's public websocket.

Subscribes to `allMids` for the io builder dex and keeps the latest mid per market in memory, so the
trading path can read a fresh price without a REST round-trip. Public data only — no keys.

Only Hyperliquid is on the socket: RH-Lighter's websocket sits behind CloudFront and refuses the
handshake from a server (400), so the Lighter mid stays on its (cheap) REST call. Every reader must
fall back to REST when `mid()` returns None (feed still warming up, stale, or disconnected), so the
feed is a pure latency optimisation and never a dependency.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time

import aiohttp

LOGGER = logging.getLogger(__name__)


class PriceFeed:
    STALE = 15.0  # seconds; a mid older than this is treated as absent so readers fall back to REST

    def __init__(self, api_url: str, dex: str = "io") -> None:
        # https://api.hyperliquid.xyz -> wss://api.hyperliquid.xyz/ws
        self._ws_url = api_url.replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/ws"
        self._dex = dex
        self._mids: dict[str, float] = {}
        self._ts: float = 0.0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="pricefeed")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def mid(self, market: str) -> float | None:
        """Latest mid for an io market (e.g. 'io:OAI'), or None if the feed is cold/stale/disconnected."""
        if time.time() - self._ts > self.STALE:
            return None
        return self._mids.get(market)

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(self._ws_url, heartbeat=20) as ws:
                        await ws.send_json({"method": "subscribe",
                                            "subscription": {"type": "allMids", "dex": self._dex}})
                        LOGGER.info("pricefeed connected (%s dex=%s)", self._ws_url, self._dex)
                        backoff = 1
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = json.loads(msg.data)
                            if data.get("channel") != "allMids":
                                continue
                            mids = data.get("data", {}).get("mids", {})
                            if not mids:
                                continue
                            for name, px in mids.items():
                                try:
                                    self._mids[name] = float(px)
                                except (TypeError, ValueError):
                                    continue
                            self._ts = time.time()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — reconnect with backoff, never die
                LOGGER.warning("pricefeed dropped, reconnecting in %ds", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
