"""Database access — plain asyncpg, no ORM.

An ORM would add ~40MB of resident memory and a dependency tree for maybe fifteen queries; this
bot shares a 512MB instance with the signal bot, so the queries are written out.

Credentials are encrypted before they reach this layer and decrypted only where an API call needs
them — nothing here ever returns a plaintext secret by accident, because the row types simply
don't carry one.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg

from ..security.encryption import CredentialCipher

LOGGER = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

MASTER = "MASTER"
FOLLOWER = "FOLLOWER"


@dataclass(frozen=True)
class Account:
    id: int
    label: str
    kind: str
    api_key_hint: str
    size_multiplier: float
    active: bool
    position_mode: int | None
    last_error: str | None

    @property
    def is_master(self) -> bool:
        return self.kind == MASTER


@dataclass(frozen=True)
class PositionRow:
    account_id: int
    symbol: str
    position_type: int
    hold_vol: float
    leverage: int
    open_type: int


class Store:
    def __init__(self, pool: asyncpg.Pool, cipher: CredentialCipher) -> None:
        self._pool = pool
        self._cipher = cipher

    @classmethod
    async def connect(cls, dsn: str, cipher: CredentialCipher) -> "Store":
        # Small pool on purpose: nine followers acting at once is the peak, and every extra idle
        # connection is memory on both sides.
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6, command_timeout=20)
        store = cls(pool, cipher)
        await store._apply_schema()
        return store

    async def close(self) -> None:
        await self._pool.close()

    async def _apply_schema(self) -> None:
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        async with self._pool.acquire() as conn:
            await conn.execute(sql)
        LOGGER.info("schema applied")

    # ── accounts ────────────────────────────────────────────────────────────────────────────
    async def add_account(
        self, *, label: str, kind: str, api_key: str, api_secret: str, position_mode: int | None
    ) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO copy_accounts (label, kind, api_key_enc, api_secret_enc, api_key_hint, position_mode)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                label,
                kind,
                self._cipher.encrypt(api_key),
                self._cipher.encrypt(api_secret),
                api_key[-4:],
                position_mode,
            )

    async def list_accounts(self, kind: str | None = None) -> list[Account]:
        query = """
            SELECT id, label, kind, api_key_hint, size_multiplier, active, position_mode, last_error
            FROM copy_accounts
        """
        args: list[Any] = []
        if kind:
            query += " WHERE kind = $1"
            args.append(kind)
        query += " ORDER BY kind DESC, id ASC"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [Account(**dict(r)) for r in rows]

    async def get_master(self) -> Account | None:
        accounts = await self.list_accounts(MASTER)
        return accounts[0] if accounts else None

    async def get_credentials(self, account_id: int) -> tuple[str, str] | None:
        """Decrypted (api_key, secret). The only place plaintext exists, and only in memory."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT api_key_enc, api_secret_enc FROM copy_accounts WHERE id = $1", account_id
            )
        if not row:
            return None
        return self._cipher.decrypt(row["api_key_enc"]), self._cipher.decrypt(row["api_secret_enc"])

    async def remove_account(self, account_id: int) -> bool:
        async with self._pool.acquire() as conn:
            result = await conn.execute("DELETE FROM copy_accounts WHERE id = $1", account_id)
        return result.endswith("1")

    async def set_account_active(self, account_id: int, active: bool) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET active = $2, updated_at = now() WHERE id = $1", account_id, active
            )

    async def set_account_error(self, account_id: int, error: str | None) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET last_error = $2, updated_at = now() WHERE id = $1", account_id, error
            )

    async def set_size_multiplier(self, account_id: int, multiplier: float) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET size_multiplier = $2, updated_at = now() WHERE id = $1",
                account_id,
                multiplier,
            )

    # ── run state ───────────────────────────────────────────────────────────────────────────
    async def is_running(self) -> bool:
        async with self._pool.acquire() as conn:
            return bool(await conn.fetchval("SELECT running FROM copy_state WHERE id = 1"))

    async def set_running(self, running: bool) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("UPDATE copy_state SET running = $1, updated_at = now() WHERE id = 1", running)

    # ── positions (expected state) ──────────────────────────────────────────────────────────
    async def get_positions(self, account_id: int) -> dict[tuple[str, int], PositionRow]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT account_id, symbol, position_type, hold_vol, leverage, open_type"
                " FROM copy_positions WHERE account_id = $1",
                account_id,
            )
        return {(r["symbol"], r["position_type"]): PositionRow(**dict(r)) for r in rows}

    async def upsert_position(self, row: PositionRow) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO copy_positions (account_id, symbol, position_type, hold_vol, leverage, open_type, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (account_id, symbol, position_type) DO UPDATE
                SET hold_vol = EXCLUDED.hold_vol,
                    leverage = EXCLUDED.leverage,
                    open_type = EXCLUDED.open_type,
                    updated_at = now()
                """,
                row.account_id,
                row.symbol,
                row.position_type,
                row.hold_vol,
                row.leverage,
                row.open_type,
            )

    async def delete_position(self, account_id: int, symbol: str, position_type: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM copy_positions WHERE account_id = $1 AND symbol = $2 AND position_type = $3",
                account_id,
                symbol,
                position_type,
            )

    # ── events + tasks ──────────────────────────────────────────────────────────────────────
    async def record_event(
        self,
        *,
        dedupe_key: str,
        symbol: str,
        position_type: int,
        action: str,
        master_vol: float,
        delta_vol: float,
        leverage: int,
        open_type: int,
        raw: dict[str, Any] | None,
    ) -> int | None:
        """Returns the new event id, or None when this exact event was already recorded.

        The None case is the idempotency guard doing its job — a duplicate push after a reconnect
        must not become a second round of orders.
        """
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO copy_master_events
                    (dedupe_key, symbol, position_type, action, master_vol, delta_vol, leverage, open_type, raw)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (dedupe_key) DO NOTHING
                RETURNING id
                """,
                dedupe_key,
                symbol,
                position_type,
                action,
                master_vol,
                delta_vol,
                leverage,
                open_type,
                json.dumps(raw) if raw else None,
            )

    async def create_task(
        self,
        *,
        event_id: int,
        account_id: int,
        action: str,
        symbol: str,
        position_type: int,
        vol: float,
        leverage: int,
        open_type: int,
        external_oid: str,
    ) -> int | None:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO copy_tasks
                    (event_id, account_id, action, symbol, position_type, vol, leverage, open_type, external_oid)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (event_id, account_id) DO NOTHING
                RETURNING id
                """,
                event_id,
                account_id,
                action,
                symbol,
                position_type,
                vol,
                leverage,
                open_type,
                external_oid,
            )

    async def finish_task(self, task_id: int, *, status: str, attempts: int, error: str | None) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_tasks SET status = $2, attempts = $3, error = $4, updated_at = now() WHERE id = $1",
                task_id,
                status,
                attempts,
                error,
            )

    async def recent_events(self, limit: int = 10) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.id, e.symbol, e.action, e.position_type, e.master_vol, e.delta_vol,
                       e.leverage, e.observed_at,
                       count(t.id) FILTER (WHERE t.status = 'SUCCESS') AS ok,
                       count(t.id) FILTER (WHERE t.status = 'FAILED')  AS failed
                FROM copy_master_events e
                LEFT JOIN copy_tasks t ON t.event_id = e.id
                GROUP BY e.id
                ORDER BY e.observed_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]
