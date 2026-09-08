-- Schema for the MEXC copy-trading bot.
--
-- Tables are prefixed `copy_` because this database is shared with another project; the prefix is
-- what keeps the two from ever colliding on a name like "accounts" or "positions".
--
-- Multi-tenant by Telegram user id: two brothers run their own master and their own followers in
-- the same bot, and must never see or touch each other's accounts. `owner_id` is on every table
-- that holds anything account-specific, and every query filters by it — the isolation is in the
-- data model rather than in UI checks that can be forgotten.
--
-- Applied idempotently at startup (see db/store.py), so a redeploy is safe and there is no
-- separate migration step to forget.

-- ── Upgrade in place, before anything below needs the new columns ───────────────────────────
-- Everything after this point is written for the multi-tenant shape, including indexes on
-- `owner_id`, so a database created by an older build has to be brought forward first. Each step
-- is conditional, so this is a no-op on a fresh database and on an already-migrated one.
DO $$
DECLARE
    con record;
BEGIN
    IF to_regclass('public.copy_accounts') IS NOT NULL THEN
        -- Pre-existing rows go to owner 0, which belongs to nobody: they stay invisible in the
        -- bot rather than silently becoming someone's accounts.
        ALTER TABLE copy_accounts ADD COLUMN IF NOT EXISTS owner_id BIGINT NOT NULL DEFAULT 0;
        -- The single-master index used to be global; it has to become per-owner.
        IF EXISTS (SELECT 1 FROM pg_indexes
                   WHERE indexname = 'copy_accounts_single_master' AND indexdef LIKE '%(kind)%') THEN
            DROP INDEX copy_accounts_single_master;
        END IF;
    END IF;

    IF to_regclass('public.copy_tasks') IS NOT NULL THEN
        ALTER TABLE copy_tasks ADD COLUMN IF NOT EXISTS realized_pnl DOUBLE PRECISION;
    END IF;

    IF to_regclass('public.copy_state') IS NOT NULL THEN
        ALTER TABLE copy_state ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'COPY';
        ALTER TABLE copy_state ADD COLUMN IF NOT EXISTS reverse_account_id BIGINT;
        ALTER TABLE copy_state ADD COLUMN IF NOT EXISTS mirror_limits BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE copy_state ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'uk';
    END IF;

    IF to_regclass('public.copy_master_events') IS NOT NULL THEN
        ALTER TABLE copy_master_events ADD COLUMN IF NOT EXISTS owner_id BIGINT NOT NULL DEFAULT 0;
        -- dedupe_key was globally unique, which would let one owner's event suppress another's
        -- identical one. Found by column rather than by name: Postgres named the old constraint
        -- itself, and guessing that name wrong leaves the bug silently in place.
        FOR con IN
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class rel ON rel.oid = c.conrelid
            WHERE rel.relname = 'copy_master_events'
              AND c.contype = 'u'
              AND (SELECT array_agg(a.attname::text)
                   FROM unnest(c.conkey) AS k
                   JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k) = ARRAY['dedupe_key']
        LOOP
            EXECUTE format('ALTER TABLE copy_master_events DROP CONSTRAINT %I', con.conname);
        END LOOP;
    END IF;

    -- copy_state was one global row keyed on id = 1; it is now one row per owner. Dropping it is
    -- safe: it holds nothing but a START/STOP flag, and losing it means "stopped".
    IF to_regclass('public.copy_state') IS NOT NULL
       AND EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'copy_state' AND column_name = 'id') THEN
        DROP TABLE copy_state;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS copy_accounts (
    id                BIGSERIAL PRIMARY KEY,
    -- Telegram user id of the owner. Accounts are private to them.
    owner_id          BIGINT      NOT NULL,
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

-- One master PER OWNER, enforced by the database rather than by application checks that can race.
CREATE UNIQUE INDEX IF NOT EXISTS copy_accounts_single_master
    ON copy_accounts (owner_id) WHERE kind = 'MASTER';

CREATE INDEX IF NOT EXISTS copy_accounts_owner ON copy_accounts (owner_id);

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
    owner_id       BIGINT      NOT NULL,
    -- Unique per owner, not globally: two masters can legitimately produce the same position
    -- version for the same symbol, and one owner's event must never suppress the other's.
    dedupe_key     TEXT        NOT NULL,
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

CREATE UNIQUE INDEX IF NOT EXISTS copy_master_events_dedupe ON copy_master_events (owner_id, dedupe_key);
CREATE INDEX IF NOT EXISTS copy_master_events_time ON copy_master_events (owner_id, observed_at DESC);

-- Which follower order was placed to mirror which master order. Needed to cancel the copies
-- when the master pulls theirs: without it, a cancelled master order leaves nine live orders
-- resting on the followers with nothing left to fill against.
CREATE TABLE IF NOT EXISTS copy_mirrored_orders (
    owner_id         BIGINT NOT NULL,
    master_order_id  TEXT   NOT NULL,
    account_id       BIGINT NOT NULL REFERENCES copy_accounts(id) ON DELETE CASCADE,
    follower_order_id TEXT  NOT NULL,
    symbol           TEXT   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (master_order_id, account_id)
);

CREATE INDEX IF NOT EXISTS copy_mirrored_orders_owner ON copy_mirrored_orders (owner_id, created_at DESC);

-- Accounts that failed to follow the master and are now managed by hand.
--
-- A follower whose limit did not fill is not merely late: it holds something the master does not,
-- or misses something the master has. Until that is resolved it must stop listening to the master
-- entirely — otherwise the next master action lands on an account in a completely different state.
--
-- Grouped by the master action that stranded them, so the user can see which accounts are stuck on
-- what. Several groups coexist: a later failure is its own group, never merged into an earlier one.
CREATE TABLE IF NOT EXISTS copy_stuck_groups (
    id             BIGSERIAL   PRIMARY KEY,
    owner_id       BIGINT      NOT NULL,
    symbol         TEXT        NOT NULL,
    position_type  INTEGER     NOT NULL,
    -- ENTRY: the buy never filled, so there is no position and a limit is still resting.
    -- EXIT:  the sell never filled (or filled partly), so the position is still held.
    kind           TEXT        NOT NULL CHECK (kind IN ('ENTRY', 'EXIT')),
    -- The price their resting limit currently sits at. The user can move it.
    limit_price    DOUBLE PRECISION,
    leverage       INTEGER     NOT NULL DEFAULT 0,
    open_type      INTEGER     NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS copy_stuck_groups_open
    ON copy_stuck_groups (owner_id) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS copy_stuck_accounts (
    group_id          BIGINT NOT NULL REFERENCES copy_stuck_groups(id) ON DELETE CASCADE,
    account_id        BIGINT NOT NULL REFERENCES copy_accounts(id) ON DELETE CASCADE,
    -- What is outstanding: still to close (EXIT) or still to enter (ENTRY). A partial close is
    -- not a close, so this is the remainder, not the original size.
    vol               DOUBLE PRECISION NOT NULL,
    follower_order_id TEXT,
    resolved_at       TIMESTAMPTZ,
    PRIMARY KEY (group_id, account_id)
);

-- Which accounts are currently deaf to the master. Read on every master action, so it is indexed.
CREATE INDEX IF NOT EXISTS copy_stuck_accounts_open
    ON copy_stuck_accounts (account_id) WHERE resolved_at IS NULL;

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
    -- Realised PnL for a CLOSE, as the exchange settled it. NULL for opens, and for a close whose
    -- settlement could not be read back — which must stay distinguishable from a genuine zero.
    realized_pnl   DOUBLE PRECISION,
    -- Sent to MEXC as externalOid: the venue rejects a duplicate, so a retry after a network
    -- timeout that actually succeeded cannot place a second order.
    external_oid   TEXT        NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, account_id)
);

CREATE INDEX IF NOT EXISTS copy_tasks_event ON copy_tasks (event_id);
CREATE INDEX IF NOT EXISTS copy_tasks_recent ON copy_tasks (created_at DESC);

-- Run state per owner, so START/STOP survives a restart (spec §31) and one owner stopping does
-- not stop the other's copying.
CREATE TABLE IF NOT EXISTS copy_state (
    owner_id     BIGINT      PRIMARY KEY,
    running      BOOLEAN     NOT NULL DEFAULT FALSE,
    -- 'COPY'    — every active follower mirrors the master on the same side.
    -- 'REVERSE' — exactly one chosen account takes the OPPOSITE side, an automatic hedge.
    -- One column rather than two flags: the modes are mutually exclusive by construction, so
    -- there is no state in which both could be on.
    mode         TEXT        NOT NULL DEFAULT 'COPY' CHECK (mode IN ('COPY', 'REVERSE')),
    -- Which follower takes the opposite side in REVERSE mode. Cleared if that account is
    -- deleted, which leaves the mode unconfigured rather than silently retargeting someone else.
    reverse_account_id BIGINT REFERENCES copy_accounts(id) ON DELETE SET NULL,
    -- Interface language for this owner: 'uk' or 'en'. Per owner rather than global, since the
    -- two brothers do not have to agree on one.
    language     TEXT        NOT NULL DEFAULT 'uk',
    -- Unused. Limit mirroring is unconditional; this column is kept only so an older database
    -- does not need a destructive migration to drop it.
    mirror_limits BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

