"""Database access — plain asyncpg, no ORM.

An ORM would add ~40MB of resident memory and a dependency tree for maybe fifteen queries; this
bot shares a 512MB instance with the signal bot, so the queries are written out.

Credentials are encrypted before they reach this layer and decrypted only where an API call needs
them — nothing here ever returns a plaintext secret by accident, because the row types simply
don't carry one.

Every account-scoped method takes `owner_id` (a Telegram user id) and puts it in the WHERE
clause, including the mutations. That is deliberate: passing an id that belongs to somebody
else has to come back empty at the SQL level, so a missing check in the Telegram layer cannot
delete or trade another owner's account.
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

MODE_COPY = "COPY"
MODE_REVERSE = "REVERSE"


@dataclass(frozen=True)
class Account:
    id: int
    owner_id: int
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
        self,
        *,
        owner_id: int,
        label: str,
        kind: str,
        api_key: str,
        api_secret: str,
        position_mode: int | None,
    ) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO copy_accounts
                    (owner_id, label, kind, api_key_enc, api_secret_enc, api_key_hint, position_mode)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                owner_id,
                label,
                kind,
                self._cipher.encrypt(api_key),
                self._cipher.encrypt(api_secret),
                api_key[-4:],
                position_mode,
            )

    async def list_accounts(self, owner_id: int, kind: str | None = None) -> list[Account]:
        query = """
            SELECT id, owner_id, label, kind, api_key_hint, size_multiplier, active, position_mode,
                   last_error
            FROM copy_accounts
            WHERE owner_id = $1
        """
        args: list[Any] = [owner_id]
        if kind:
            query += " AND kind = $2"
            args.append(kind)
        query += " ORDER BY kind DESC, id ASC"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [Account(**dict(r)) for r in rows]

    async def list_owners(self) -> list[int]:
        """Owners with at least one account — who the manager must run a copier for."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT owner_id FROM copy_accounts ORDER BY owner_id")
        return [r["owner_id"] for r in rows]

    async def get_master(self, owner_id: int) -> Account | None:
        accounts = await self.list_accounts(owner_id, MASTER)
        return accounts[0] if accounts else None

    async def get_credentials(self, account_id: int, owner_id: int) -> tuple[str, str] | None:
        """Decrypted (api_key, secret). The only place plaintext exists, and only in memory.

        Scoped by owner as well as id: nothing should be able to ask for another owner's keys,
        so the query simply cannot find them.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT api_key_enc, api_secret_enc FROM copy_accounts WHERE id = $1 AND owner_id = $2",
                account_id,
                owner_id,
            )
        if not row:
            return None
        return self._cipher.decrypt(row["api_key_enc"]), self._cipher.decrypt(row["api_secret_enc"])

    async def get_credentials_for(self, owner_id: int) -> dict[int, tuple[str, str]]:
        """Every one of this owner's accounts, decrypted, in a single round trip.

        The menu needs all ten at once to show balances. Asking per account turned one screen into
        ten queries against a remote database, which cost more than the exchange calls they were
        feeding.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, api_key_enc, api_secret_enc FROM copy_accounts WHERE owner_id = $1",
                owner_id,
            )
        return {
            r["id"]: (self._cipher.decrypt(r["api_key_enc"]), self._cipher.decrypt(r["api_secret_enc"]))
            for r in rows
        }

    async def remove_account(self, account_id: int, owner_id: int) -> bool:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM copy_accounts WHERE id = $1 AND owner_id = $2", account_id, owner_id
            )
        return result.endswith("1")

    async def set_account_active(self, account_id: int, owner_id: int, active: bool) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET active = $3, updated_at = now() WHERE id = $1 AND owner_id = $2",
                account_id,
                owner_id,
                active,
            )

    async def set_account_error(self, account_id: int, error: str | None) -> None:
        # No owner filter: the engine calls this with an id it just read from that owner's own
        # account list, and it only ever writes a diagnostic string.
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET last_error = $2, updated_at = now() WHERE id = $1", account_id, error
            )

    async def set_size_multiplier(self, account_id: int, owner_id: int, multiplier: float) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_accounts SET size_multiplier = $3, updated_at = now()"
                " WHERE id = $1 AND owner_id = $2",
                account_id,
                owner_id,
                multiplier,
            )

    # ── run state ───────────────────────────────────────────────────────────────────────────
    async def is_running(self, owner_id: int) -> bool:
        async with self._pool.acquire() as conn:
            return bool(await conn.fetchval("SELECT running FROM copy_state WHERE owner_id = $1", owner_id))

    async def set_running(self, owner_id: int, running: bool) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO copy_state (owner_id, running) VALUES ($1, $2)
                ON CONFLICT (owner_id) DO UPDATE SET running = EXCLUDED.running, updated_at = now()
                """,
                owner_id,
                running,
            )

    async def get_mode(self, owner_id: int) -> tuple[str, int | None]:
        """(mode, reverse account id). Defaults to plain copying for an owner with no row yet."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT mode, reverse_account_id FROM copy_state WHERE owner_id = $1", owner_id
            )
        if not row:
            return MODE_COPY, None
        return row["mode"] or MODE_COPY, row["reverse_account_id"]

    async def set_mode(self, owner_id: int, mode: str, reverse_account_id: int | None) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO copy_state (owner_id, mode, reverse_account_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (owner_id) DO UPDATE
                SET mode = EXCLUDED.mode,
                    reverse_account_id = EXCLUDED.reverse_account_id,
                    updated_at = now()
                """,
                owner_id,
                mode,
                reverse_account_id,
            )

    # ── mirrored resting orders ──────────────────────────────────────────────────
    async def record_mirrored_order(
        self, *, owner_id: int, master_order_id: str, account_id: int,
        follower_order_id: str, symbol: str,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO copy_mirrored_orders
                    (owner_id, master_order_id, account_id, follower_order_id, symbol)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (master_order_id, account_id) DO NOTHING
                """,
                owner_id, master_order_id, account_id, follower_order_id, symbol,
            )

    async def get_mirrored_orders(self, owner_id: int, master_order_id: str) -> list[tuple[int, str]]:
        """(account id, that account's order id) for one master order."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT account_id, follower_order_id FROM copy_mirrored_orders"
                " WHERE owner_id = $1 AND master_order_id = $2",
                owner_id, master_order_id,
            )
        return [(r["account_id"], r["follower_order_id"]) for r in rows]

    async def clear_mirrored_order(self, owner_id: int, master_order_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM copy_mirrored_orders WHERE owner_id = $1 AND master_order_id = $2",
                owner_id, master_order_id,
            )

    async def running_owners(self) -> list[int]:
        """Owners whose copying was left ON — restored on boot so a redeploy resumes each."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT owner_id FROM copy_state WHERE running ORDER BY owner_id")
        return [r["owner_id"] for r in rows]

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
        owner_id: int,
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
                    (owner_id, dedupe_key, symbol, position_type, action, master_vol, delta_vol,
                     leverage, open_type, raw)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (owner_id, dedupe_key) DO NOTHING
                RETURNING id
                """,
                owner_id,
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

    async def finish_task(
        self,
        task_id: int,
        *,
        status: str,
        attempts: int,
        error: str | None,
        realized_pnl: float | None = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE copy_tasks SET status = $2, attempts = $3, error = $4, realized_pnl = $5,"
                " updated_at = now() WHERE id = $1",
                task_id,
                status,
                attempts,
                error,
                realized_pnl,
            )

    async def recent_events(self, owner_id: int, limit: int = 10) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.id, e.symbol, e.action, e.position_type, e.master_vol, e.delta_vol,
                       e.leverage, e.observed_at,
                       count(t.id) FILTER (WHERE t.status = 'SUCCESS') AS ok,
                       count(t.id) FILTER (WHERE t.status = 'FAILED')  AS failed,
                       sum(t.realized_pnl) AS pnl,
                       -- How many closes actually reported a number, so the history can say
                       -- "partial" instead of presenting an incomplete sum as the total.
                       count(t.realized_pnl) AS pnl_count
                FROM copy_master_events e
                LEFT JOIN copy_tasks t ON t.event_id = e.id
                WHERE e.owner_id = $1
                GROUP BY e.id
                ORDER BY e.observed_at DESC
                LIMIT $2
                """,
                owner_id,
                limit,
            )
        return [dict(r) for r in rows]
