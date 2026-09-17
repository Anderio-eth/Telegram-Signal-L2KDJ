"""Persistence: encrypted per-user credentials for both venues, and a log of opened hedges.

One asyncpg pool, schema applied on connect. Secrets are encrypted by CredentialCipher before they
reach a column; `meta` holds only the non-secret bits (Lighter's account/api-key indices, the
Hyperliquid wallet address).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg

from ..core.crypto import CredentialCipher

SCHEMA = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")


@dataclass(frozen=True)
class Credentials:
    venue: str
    secret: str            # decrypted private key
    meta: dict[str, Any]   # e.g. {"account_index": 3, "api_key_index": 0} or {"wallet_address": "0x.."}


class Store:
    def __init__(self, dsn: str, cipher: CredentialCipher) -> None:
        self._dsn = dsn
        self._cipher = cipher
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    # ── credentials ──────────────────────────────────────────────────────────────────────────────
    async def set_credentials(self, owner_id: int, venue: str, secret: str, meta: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO hb_credentials (owner_id, venue, enc_secret, meta)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (owner_id, venue) DO UPDATE
                    SET enc_secret = EXCLUDED.enc_secret, meta = EXCLUDED.meta, updated_at = now()
                """,
                owner_id, venue, self._cipher.encrypt(secret), json.dumps(meta),
            )

    async def get_credentials(self, owner_id: int, venue: str) -> Credentials | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT enc_secret, meta FROM hb_credentials WHERE owner_id = $1 AND venue = $2",
                owner_id, venue,
            )
        if not row:
            return None
        return Credentials(venue, self._cipher.decrypt(row["enc_secret"]), dict(row["meta"]))

    async def venues_set(self, owner_id: int) -> set[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT venue FROM hb_credentials WHERE owner_id = $1", owner_id)
        return {r["venue"] for r in rows}

    async def delete_credentials(self, owner_id: int, venue: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM hb_credentials WHERE owner_id = $1 AND venue = $2", owner_id, venue)

    # ── hedges ───────────────────────────────────────────────────────────────────────────────────
    async def record_hedge(self, owner_id: int, pair_key: str, notional_usd: float,
                           entropy_side: str, status: str, detail: dict) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO hb_hedges (owner_id, pair_key, notional_usd, entropy_side, status, detail)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb) RETURNING id
                """,
                owner_id, pair_key, notional_usd, entropy_side, status, json.dumps(detail),
            )

    async def open_hedges(self, owner_id: int) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM hb_hedges WHERE owner_id = $1 AND status IN ('OPEN','PARTIAL')"
                " ORDER BY created_at DESC LIMIT 25",
                owner_id,
            )
        return [dict(r) for r in rows]

    async def mark_hedge(self, hedge_id: int, status: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE hb_hedges SET status = $2, closed_at = CASE WHEN $2 = 'CLOSED' THEN now() ELSE closed_at END"
                " WHERE id = $1",
                hedge_id, status,
            )
