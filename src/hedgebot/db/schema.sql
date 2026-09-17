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
