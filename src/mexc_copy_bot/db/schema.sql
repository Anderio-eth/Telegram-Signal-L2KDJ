-- Schema for the MEXC copy-trading bot.
--
-- Tables are prefixed `copy_` because this database is shared with another project; the prefix is
-- what keeps the two from ever colliding on a name like "accounts" or "positions".
--
-- Applied idempotently at startup (see db/store.py), so a redeploy is safe and there is no
-- separate migration step to forget.

CREATE TABLE IF NOT EXISTS copy_accounts (
    id                BIGSERIAL PRIMARY KEY,
    label             TEXT        NOT NULL,
    -- 'MASTER' or 'FOLLOWER'. Exactly one master is enforced by the partial index below.
    kind              TEXT        NOT NULL CHECK (kind IN ('MASTER', 'FOLLOWER')),
    -- Encrypted with AES-256-GCM (security/encryption.py). Never stored or logged in plaintext.
    api_key_enc       TEXT        NOT NULL,
    api_secret_enc    TEXT        NOT NULL,
    -- Last 4 characters of the API key, for showing "which account is this" without decrypting.
    api_key_hint      TEXT        NOT NULL,
    -- Position size relative to the master: 1.0 = same size. Architecture supports it from day
    -- one even though the first version always copies 1:1 (spec §14).
    size_multiplier   DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    active            BOOLEAN     NOT NULL DEFAULT TRUE,
    -- Cached from the exchange so mismatches can be spotted without an API call per event:
    -- a follower in one-way mode cannot correctly mirror a hedge-mode master.
    position_mode     INTEGER,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One master, enforced by the database rather than by application checks that can race.
CREATE UNIQUE INDEX IF NOT EXISTS copy_accounts_single_master
    ON copy_accounts ((kind)) WHERE kind = 'MASTER';

-- What we believe each account's position is. The exchange remains the source of truth;
-- reconciliation compares this against reality and reports drift.
CREATE TABLE IF NOT EXISTS copy_positions (
    account_id     BIGINT      NOT NULL REFERENCES copy_accounts(id) ON DELETE CASCADE,
    symbol         TEXT        NOT NULL,
    -- 1 long, 2 short (MEXC's own enum). In hedge mode both can exist for one symbol at once,
    -- which is why it is part of the key.
    position_type  INTEGER     NOT NULL,
    hold_vol       DOUBLE PRECISION NOT NULL,
    leverage       INTEGER     NOT NULL,
    open_type      INTEGER     NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, symbol, position_type)
);

-- Every observed change on the master. `dedupe_key` is what makes replay safe: MEXC can deliver
-- the same position push twice (notably right after a reconnect), and re-processing it would
-- open a second position on every follower.
CREATE TABLE IF NOT EXISTS copy_master_events (
    id             BIGSERIAL   PRIMARY KEY,
    dedupe_key     TEXT        NOT NULL UNIQUE,
    symbol         TEXT        NOT NULL,
    position_type  INTEGER     NOT NULL,
    -- OPEN | INCREASE | DECREASE | CLOSE
    action         TEXT        NOT NULL,
    -- Absolute master size after the change, and the delta that caused it. Followers act on the
    -- delta for increases/decreases and on the absolute size for opens.
    master_vol     DOUBLE PRECISION NOT NULL,
    delta_vol      DOUBLE PRECISION NOT NULL,
    leverage       INTEGER     NOT NULL,
    open_type      INTEGER     NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw            JSONB
);

CREATE INDEX IF NOT EXISTS copy_master_events_time ON copy_master_events (observed_at DESC);

-- One row per (event, follower). The unique constraint is the second idempotency guard: even if
-- an event were somehow processed twice, a follower cannot receive the same instruction twice.
CREATE TABLE IF NOT EXISTS copy_tasks (
    id             BIGSERIAL   PRIMARY KEY,
    event_id       BIGINT      NOT NULL REFERENCES copy_master_events(id) ON DELETE CASCADE,
    account_id     BIGINT      NOT NULL REFERENCES copy_accounts(id) ON DELETE CASCADE,
    action         TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    position_type  INTEGER     NOT NULL,
    vol            DOUBLE PRECISION NOT NULL,
    leverage       INTEGER     NOT NULL,
    open_type      INTEGER     NOT NULL,
    -- PENDING | PROCESSING | SUCCESS | FAILED
    status         TEXT        NOT NULL DEFAULT 'PENDING',
    attempts       INTEGER     NOT NULL DEFAULT 0,
    error          TEXT,
    -- Sent to MEXC as externalOid: the venue rejects a duplicate, so a retry after a network
    -- timeout that actually succeeded cannot place a second order.
    external_oid   TEXT        NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, account_id)
);

CREATE INDEX IF NOT EXISTS copy_tasks_event ON copy_tasks (event_id);
CREATE INDEX IF NOT EXISTS copy_tasks_recent ON copy_tasks (created_at DESC);

-- Single-row control state, so START/STOP survives a restart (spec §31).
CREATE TABLE IF NOT EXISTS copy_state (
    id           INTEGER     PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    running      BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO copy_state (id, running) VALUES (1, FALSE) ON CONFLICT (id) DO NOTHING;
