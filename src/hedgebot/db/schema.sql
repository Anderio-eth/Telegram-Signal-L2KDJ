-- Applied on every boot; all statements are idempotent so the shared Postgres can be reused safely
-- alongside whatever else lives there (tables are prefixed hb_ to avoid collisions).

CREATE TABLE IF NOT EXISTS hb_credentials (
    owner_id    BIGINT      NOT NULL,          -- telegram user id
    venue       TEXT        NOT NULL,          -- 'lighter' | 'entropy'
    enc_secret  TEXT        NOT NULL,          -- AES-GCM of the private key
    meta        JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- non-secret fields (indices, wallet address)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, venue)
);

-- A log of hedges opened through the bot, for the "positions / close" screen and for later
-- daily-cycle automation. Legs and fills are kept as JSON so the shape can evolve during testing.
CREATE TABLE IF NOT EXISTS hb_hedges (
    id            BIGSERIAL   PRIMARY KEY,
    owner_id      BIGINT      NOT NULL,
    pair_key      TEXT        NOT NULL,
    notional_usd  DOUBLE PRECISION NOT NULL,
    entropy_side  TEXT        NOT NULL,         -- 'LONG' | 'SHORT' (Lighter takes the opposite)
    status        TEXT        NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED | PARTIAL | FAILED
    detail        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS hb_hedges_owner ON hb_hedges (owner_id, status);

-- Remembered per-user settings: the last hedge draft and the last auto-session config, so the
-- config screens come back pre-filled after a restart.
CREATE TABLE IF NOT EXISTS hb_settings (
    owner_id     BIGINT      PRIMARY KEY,
    draft        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    session_cfg  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-trading sessions. The engine (added next) reads RUNNING rows and drives hedges under them;
-- persisting here is what lets a session survive a redeploy instead of being abandoned mid-flight.
CREATE TABLE IF NOT EXISTS hb_sessions (
    id          BIGSERIAL   PRIMARY KEY,
    owner_id    BIGINT      NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'RUNNING',  -- RUNNING | STOPPING | STOPPED | DONE
    config      JSONB       NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS hb_sessions_owner ON hb_sessions (owner_id, status);

-- Per-hedge accounting for the stats/Sheets report (columns fill in over a hedge's life).
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS session_id    BIGINT;
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS opened_at     TIMESTAMPTZ;
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS realized_pnl  DOUBLE PRECISION;
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS fees          DOUBLE PRECISION;
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS entropy_vol   DOUBLE PRECISION;
ALTER TABLE hb_hedges ADD COLUMN IF NOT EXISTS lighter_vol   DOUBLE PRECISION;
