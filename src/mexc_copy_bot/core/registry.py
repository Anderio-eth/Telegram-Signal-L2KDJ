"""One CopyService per owner, created on demand.

Two people run this bot with their own master and their own followers. Rather than teaching a
single service to juggle several masters — one tracker per master, one lock per master, one
reconcile loop per master, all inside one object — each owner simply gets their own service. The
isolation then comes for free: a crash, a stuck websocket or an emergency stop is confined to the
owner whose service it belongs to.

Services are created lazily and kept, so `get()` is cheap to call from every Telegram handler. An
idle service holds no socket and no HTTP session (start() opens them), so a registry entry for
someone who never pressed START costs almost nothing.
"""

from __future__ import annotations

import asyncio
import logging

from ..db.store import Store
from .service import CopyService

LOGGER = logging.getLogger(__name__)


class ServiceRegistry:
    def __init__(
        self,
        store: Store,
        *,
        retry_attempts: int,
        reconcile_seconds: int,
        ws_reconnect_max_seconds: float,
    ) -> None:
        self._store = store
        self._retry_attempts = retry_attempts
        self._reconcile_seconds = reconcile_seconds
        self._ws_reconnect_max = ws_reconnect_max_seconds
        self._services: dict[int, CopyService] = {}
        # Creation is guarded because two Telegram updates from the same person can be handled
        # concurrently; without this, both could build a service and one master socket would be
        # left running with nothing referencing it.
        self._lock = asyncio.Lock()

        # Assigned by the bot once, then copied onto every service it creates — the callbacks
        # need to know which chat to report into, which is exactly the owner id.
        self._on_report = None
        self._on_notice = None

    def configure_callbacks(self, *, on_report, on_notice) -> None:
        """`on_report`/`on_notice` are factories: owner_id -> callback."""
        self._on_report = on_report
        self._on_notice = on_notice

    async def get(self, owner_id: int) -> CopyService:
        existing = self._services.get(owner_id)
        if existing:
            return existing
        async with self._lock:
            existing = self._services.get(owner_id)
            if existing:
                return existing
            service = CopyService(
                self._store,
                owner_id,
                retry_attempts=self._retry_attempts,
                reconcile_seconds=self._reconcile_seconds,
                ws_reconnect_max_seconds=self._ws_reconnect_max,
            )
            if self._on_report:
                service.on_report = self._on_report(owner_id)
            if self._on_notice:
                service.on_notice = self._on_notice(owner_id)
            self._services[owner_id] = service
            return service

    def active(self) -> list[CopyService]:
        """Services that currently hold a master socket."""
        return [s for s in self._services.values() if s.running]

    async def resume_persisted(self) -> None:
        """Restart copying for every owner who had it ON before the restart (spec §31).

        Per owner rather than globally: one person's redeploy-time resume must not switch on
        someone else who had deliberately stopped.
        """
        for owner_id in await self._store.running_owners():
            service = await self.get(owner_id)
            try:
                LOGGER.info("owner %s was running before restart: %s", owner_id, await service.start())
            except Exception:  # noqa: BLE001 — one owner's bad credentials must not block the others
                LOGGER.exception("could not resume owner %s", owner_id)

    async def shutdown(self) -> None:
        """Tear down every socket without clearing anyone's persisted run flag, so a redeploy
        brings each owner back exactly as they were."""
        for service in list(self._services.values()):
            running = await self._store.is_running(service.owner_id)
            try:
                await service.stop()
            except Exception:  # noqa: BLE001 — keep shutting the rest down
                LOGGER.exception("error stopping owner %s", service.owner_id)
            await self._store.set_running(service.owner_id, running)
