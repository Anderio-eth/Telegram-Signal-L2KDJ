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


# The profile a brand-new user gets; also what the boot migration renames existing rows to.
DEFAULT_PROFILE = "Основний"


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
    async def set_credentials(self, owner_id: int, venue: str, secret: str, meta: dict,
                              profile: str) -> None:
        """Store this profile's key for a venue. `label` holds the profile name: a profile is just
        the two venue rows that share it, which is why nothing else needs a join table."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO hb_profiles (owner_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    owner_id, profile,
                )
                await conn.execute(
                    """
                    INSERT INTO hb_credentials (owner_id, venue, label, enc_secret, meta)
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    ON CONFLICT (owner_id, venue, label) DO UPDATE
                        SET enc_secret = EXCLUDED.enc_secret, meta = EXCLUDED.meta, updated_at = now()
                    """,
                    owner_id, venue, profile, self._cipher.encrypt(secret), json.dumps(meta),
                )

    async def get_credentials(self, owner_id: int, venue: str, profile: str) -> Credentials | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT enc_secret, meta FROM hb_credentials "
                "WHERE owner_id = $1 AND venue = $2 AND label = $3",
                owner_id, venue, profile,
            )
        if not row:
            return None
        # asyncpg returns JSONB as a str by default (no codec registered), so parse it.
        meta = row["meta"]
        if isinstance(meta, str):
            meta = json.loads(meta)
        return Credentials(venue, self._cipher.decrypt(row["enc_secret"]), dict(meta or {}))

    async def venues_set(self, owner_id: int, profile: str) -> set[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT venue FROM hb_credentials WHERE owner_id = $1 AND label = $2",
                owner_id, profile)
        return {r["venue"] for r in rows}

    async def delete_credentials(self, owner_id: int, venue: str, profile: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM hb_credentials WHERE owner_id = $1 AND venue = $2 AND label = $3",
                owner_id, venue, profile)

    # ── profiles ─────────────────────────────────────────────────────────────────────────────────
    async def profiles(self, owner_id: int) -> list[dict]:
        """Every profile with what it has wired up, oldest first."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT p.name, p.selected, (p.enc_proxy IS NOT NULL) AS has_proxy,
                       EXISTS (SELECT 1 FROM hb_credentials c WHERE c.owner_id = p.owner_id
                               AND c.label = p.name AND c.venue = 'entropy') AS has_entropy,
                       EXISTS (SELECT 1 FROM hb_credentials c WHERE c.owner_id = p.owner_id
                               AND c.label = p.name AND c.venue = 'lighter') AS has_lighter
                FROM hb_profiles p WHERE p.owner_id = $1 ORDER BY p.created_at, p.name
                """,
                owner_id)
        return [dict(r) for r in rows]

    async def selected_profile(self, owner_id: int) -> str:
        """The profile the menu is on. Creates a first one rather than ever returning nothing --
        every screen needs some profile to be current."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                name = await conn.fetchval(
                    "SELECT name FROM hb_profiles WHERE owner_id = $1 AND selected", owner_id)
                if name is not None:
                    return name
                name = await conn.fetchval(
                    "SELECT name FROM hb_profiles WHERE owner_id = $1 ORDER BY created_at, name LIMIT 1",
                    owner_id)
                if name is None:
                    name = DEFAULT_PROFILE
                    await conn.execute(
                        "INSERT INTO hb_profiles (owner_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        owner_id, name)
                await conn.execute(
                    "UPDATE hb_profiles SET selected = TRUE WHERE owner_id = $1 AND name = $2",
                    owner_id, name)
        return name

    async def select_profile(self, owner_id: int, name: str) -> bool:
        """Point the menu at `name`. Deactivate-then-activate keeps the one-selected index happy."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if not await conn.fetchval(
                        "SELECT 1 FROM hb_profiles WHERE owner_id = $1 AND name = $2", owner_id, name):
                    return False
                await conn.execute(
                    "UPDATE hb_profiles SET selected = FALSE WHERE owner_id = $1 AND selected", owner_id)
                await conn.execute(
                    "UPDATE hb_profiles SET selected = TRUE WHERE owner_id = $1 AND name = $2",
                    owner_id, name)
        return True

    async def create_profile(self, owner_id: int, name: str) -> bool:
        async with self._pool.acquire() as conn:
            res = await conn.execute(
                "INSERT INTO hb_profiles (owner_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                owner_id, name)
        return res.endswith(" 1")

    async def rename_profile(self, owner_id: int, old: str, new: str) -> bool:
        """The name is the join key, so it has to move in every table at once."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if await conn.fetchval(
                        "SELECT 1 FROM hb_profiles WHERE owner_id = $1 AND name = $2", owner_id, new):
                    return False
                res = await conn.execute(
                    "UPDATE hb_profiles SET name = $3 WHERE owner_id = $1 AND name = $2",
                    owner_id, old, new)
                if not res.endswith(" 1"):
                    return False
                await conn.execute(
                    "UPDATE hb_credentials SET label = $3 WHERE owner_id = $1 AND label = $2",
                    owner_id, old, new)
                for table in ("hb_sessions", "hb_hedges"):
                    await conn.execute(
                        f"UPDATE {table} SET profile = $3 WHERE owner_id = $1 AND profile = $2",
                        owner_id, old, new)
        return True

    async def delete_profile(self, owner_id: int, name: str) -> None:
        """Drop a profile and its keys. Hedge/session history is kept -- it is the trading record."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM hb_credentials WHERE owner_id = $1 AND label = $2", owner_id, name)
                await conn.execute(
                    "DELETE FROM hb_profiles WHERE owner_id = $1 AND name = $2", owner_id, name)

    async def set_proxy(self, owner_id: int, name: str, proxy: str | None) -> None:
        """The proxy URL carries a password, so it is encrypted exactly like an API key."""
        enc = self._cipher.encrypt(proxy) if proxy else None
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE hb_profiles SET enc_proxy = $3 WHERE owner_id = $1 AND name = $2",
                owner_id, name, enc)

    async def get_proxy(self, owner_id: int, name: str) -> str | None:
        async with self._pool.acquire() as conn:
            enc = await conn.fetchval(
                "SELECT enc_proxy FROM hb_profiles WHERE owner_id = $1 AND name = $2", owner_id, name)
        return self._cipher.decrypt(enc) if enc else None

    # ── remembered settings (per profile) ────────────────────────────────────────────────────────
    async def load_settings(self, owner_id: int, profile: str) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT draft, session_cfg FROM hb_profiles WHERE owner_id = $1 AND name = $2",
                owner_id, profile)
        if not row:
            return {"draft": {}, "session_cfg": {}}
        out = {}
        for k in ("draft", "session_cfg"):
            v = row[k]
            out[k] = json.loads(v) if isinstance(v, str) else dict(v or {})
        return out

    async def save_draft(self, owner_id: int, profile: str, draft: dict) -> None:
        await self._save_profile_json(owner_id, profile, "draft", draft)

    async def save_session_cfg(self, owner_id: int, profile: str, cfg: dict) -> None:
        await self._save_profile_json(owner_id, profile, "session_cfg", cfg)

    async def _save_profile_json(self, owner_id: int, profile: str, column: str, value: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO hb_profiles (owner_id, name, {column}) VALUES ($1, $2, $3::jsonb) "
                f"ON CONFLICT (owner_id, name) DO UPDATE SET {column} = EXCLUDED.{column}",
                owner_id, profile, json.dumps(value))

    # ── hedges ───────────────────────────────────────────────────────────────────────────────────
    async def record_hedge(self, owner_id: int, profile: str, pair_key: str, notional_usd: float,
                           entropy_side: str, status: str, detail: dict) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO hb_hedges (owner_id, profile, pair_key, notional_usd, entropy_side,
                                       status, detail)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) RETURNING id
                """,
                owner_id, profile, pair_key, notional_usd, entropy_side, status, json.dumps(detail),
            )

    async def open_hedges(self, owner_id: int, profile: str | None = None) -> list[dict]:
        """Open hedges of one profile, or of every profile when `profile` is None."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM hb_hedges WHERE owner_id = $1 AND status IN ('OPEN','PARTIAL')"
                " AND ($2::text IS NULL OR profile = $2) ORDER BY created_at DESC LIMIT 25",
                owner_id, profile,
            )
        return [dict(r) for r in rows]

    async def mark_hedge(self, hedge_id: int, status: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE hb_hedges SET status = $2, closed_at = CASE WHEN $2 = 'CLOSED' THEN now() ELSE closed_at END"
                " WHERE id = $1",
                hedge_id, status,
            )

    # ── sessions ─────────────────────────────────────────────────────────────────────────────────
    async def create_session(self, owner_id: int, profile: str, config: dict) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                "INSERT INTO hb_sessions (owner_id, profile, config) VALUES ($1, $2, $3::jsonb) "
                "RETURNING id",
                owner_id, profile, json.dumps(config),
            )

    @staticmethod
    def _session_row(row) -> dict:
        d = dict(row)
        if isinstance(d.get("config"), str):
            d["config"] = json.loads(d["config"])
        return d

    async def active_session(self, owner_id: int, profile: str) -> dict | None:
        """The running session of ONE profile. Profiles trade in parallel, so this must never
        return another profile's session -- that is what would cross the accounts."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM hb_sessions WHERE owner_id = $1 AND profile = $2"
                " AND status IN ('RUNNING','STOPPING') ORDER BY started_at DESC LIMIT 1",
                owner_id, profile,
            )
        return self._session_row(row) if row else None

    async def active_sessions(self, owner_id: int) -> list[dict]:
        """Every running session of this owner, across profiles -- for the "what is going on" view."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM hb_sessions WHERE owner_id = $1 AND status IN ('RUNNING','STOPPING')"
                " ORDER BY started_at",
                owner_id,
            )
        return [self._session_row(r) for r in rows]

    async def active_session_by_id(self, session_id: int) -> dict | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM hb_sessions WHERE id = $1", session_id)
        return self._session_row(row) if row else None

    async def running_sessions(self) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM hb_sessions WHERE status IN ('RUNNING','STOPPING')")
        return [self._session_row(r) for r in rows]

    async def set_session_status(self, session_id: int, status: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE hb_sessions SET status = $2, ended_at = CASE WHEN $2 IN ('STOPPED','DONE') THEN now() ELSE ended_at END"
                " WHERE id = $1",
                session_id, status,
            )

    # ── hedge lifecycle (session-aware) ──────────────────────────────────────────────────────────
    async def new_hedge(self, owner_id: int, profile: str, session_id: int | None, pair_key: str,
                        notional_usd: float, entropy_side: str, status: str, detail: dict) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO hb_hedges (owner_id, profile, session_id, pair_key, notional_usd,
                                       entropy_side, status, detail, opened_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now()) RETURNING id
                """,
                owner_id, profile, session_id, pair_key, notional_usd, entropy_side, status,
                json.dumps(detail),
            )

    async def update_hedge(self, hedge_id: int, **fields) -> None:
        cols = {"status", "realized_pnl", "fees", "entropy_vol", "lighter_vol", "notional_usd",
                "opened_at", "entropy_side"}
        sets, vals = [], []
        for k, v in fields.items():
            if k in cols:
                vals.append(v)
                sets.append(f"{k} = ${len(vals)+1}")
            elif k == "detail":
                vals.append(json.dumps(v))
                sets.append(f"detail = ${len(vals)+1}::jsonb")
        if not sets:
            return
        if fields.get("status") == "CLOSED":
            sets.append("closed_at = now()")
        async with self._pool.acquire() as conn:
            await conn.execute(f"UPDATE hb_hedges SET {', '.join(sets)} WHERE id = $1", hedge_id, *vals)

    async def session_hedges(self, session_id: int) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM hb_hedges WHERE session_id = $1 ORDER BY created_at", session_id)
        return [dict(r) for r in rows]
