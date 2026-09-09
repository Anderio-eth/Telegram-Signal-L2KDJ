"""One CopyService per FOLDER, created on demand.

A folder is one master with its followers. Rather than teaching a single service to juggle several
masters — one tracker, one lock, one reconcile loop each, all inside one object — every folder
gets its own. The isolation then comes for free: a crash, a stuck websocket or an emergency stop
is confined to the folder it belongs to.

Folders run independently of which one their owner is looking at, so switching the view never
touches a running service. That is deliberate: stopping a live setup as a side effect of glancing
at another one would leave real positions unattended by accident.

Services are created lazily and kept, so `get()` is cheap to call from every Telegram handler. An
idle service holds no socket and no HTTP session (start() opens them), so a registry entry for a
folder nobody has started costs almost nothing.
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
        # keyed by folder id
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

    async def get(self, folder_id: int, owner_id: int) -> CopyService:
        existing = self._services.get(folder_id)
        if existing:
            return existing
        async with self._lock:
            existing = self._services.get(folder_id)
            if existing:
                return existing
            service = CopyService(
                self._store,
                folder_id,
                owner_id,
                retry_attempts=self._retry_attempts,
                reconcile_seconds=self._reconcile_seconds,
                ws_reconnect_max_seconds=self._ws_reconnect_max,
            )
            if self._on_report:
                service.on_report = self._on_report(owner_id)
            if self._on_notice:
                service.on_notice = self._on_notice(owner_id)
            self._services[folder_id] = service
            return service

    def active(self) -> list[CopyService]:
        """Services that currently hold a master socket."""
        return [s for s in self._services.values() if s.running]

    async def resume_persisted(self) -> None:
        """Restart copying for every owner who had it ON before the restart (spec §31).

        Per folder rather than globally: a redeploy must not switch on a setup that had been
        deliberately stopped, and must not leave a running one dark.
        """
        for folder in await self._store.running_folders():
            service = await self.get(folder.id, folder.owner_id)
            try:
                LOGGER.info(
                    "folder %s (%s) was running before restart: %s",
                    folder.id, folder.name, await service.start(),
                )
            except Exception:  # noqa: BLE001 — one folder's bad keys must not block the others
                LOGGER.exception("could not resume folder %s", folder.id)

    async def shutdown(self) -> None:
        """Tear down every socket without clearing anyone's persisted run flag, so a redeploy
        brings each owner back exactly as they were."""
        for service in list(self._services.values()):
            running = await self._store.is_running(service.folder_id)
            try:
                await service.stop()
            except Exception:  # noqa: BLE001 — keep shutting the rest down
                LOGGER.exception("error stopping folder %s", service.folder_id)
            await self._store.set_running(service.folder_id, running)
