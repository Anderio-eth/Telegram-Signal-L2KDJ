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

-- Several named API keys per venue (Lighter accounts can hold more than one API-key slot, and a
-- user may run more than one account). `label` is the user's own name for a key. Exactly one row per
-- (owner, venue) is active, and that is the row get_credentials() resolves to -- so every existing
-- caller keeps asking for "the Lighter key" and never learns about labels.
ALTER TABLE hb_credentials ADD COLUMN IF NOT EXISTS label TEXT NOT NULL DEFAULT '';
DO $$
BEGIN
    -- Widen the primary key once; rows written before this migration keep label '' and stay active.
    -- Match the constraint by the TABLE, not by name: this Postgres is shared with the other bots,
    -- and a same-named constraint in any other schema made the name lookup return two rows and abort
    -- the whole migration (so the bot would not boot).
    IF (SELECT array_length(c.conkey, 1) FROM pg_constraint c
        WHERE c.conrelid = 'hb_credentials'::regclass AND c.contype = 'p') = 2 THEN
        ALTER TABLE hb_credentials DROP CONSTRAINT hb_credentials_pkey;
        ALTER TABLE hb_credentials ADD PRIMARY KEY (owner_id, venue, label);
    END IF;
END $$;

-- A PROFILE is one Entropy account + one Lighter account + the proxy they share + its own session
-- settings. hb_credentials.label holds the profile name, so a profile's two venue rows join to it.
-- Several profiles run side by side, each with its own session, hedges and settings.
CREATE TABLE IF NOT EXISTS hb_profiles (
    owner_id    BIGINT      NOT NULL,
    name        TEXT        NOT NULL,
    enc_proxy   TEXT,                                -- AES-GCM, same cipher as the API keys
    session_cfg JSONB       NOT NULL DEFAULT '{}'::jsonb,
    draft       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    selected    BOOLEAN     NOT NULL DEFAULT FALSE,  -- the profile the menu is currently showing
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, name)
);
CREATE UNIQUE INDEX IF NOT EXISTS hb_profiles_one_selected ON hb_profiles (owner_id) WHERE selected;

-- Sessions and hedges belong to a profile, so two profiles can trade at the same time without the
-- engine confusing whose position is whose.
ALTER TABLE hb_sessions ADD COLUMN IF NOT EXISTS profile TEXT NOT NULL DEFAULT '';
ALTER TABLE hb_hedges   ADD COLUMN IF NOT EXISTS profile TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS hb_sessions_profile ON hb_sessions (owner_id, profile, status);

-- Migration: whatever is already stored becomes the first profile, so nothing is re-entered by hand.
-- Guarded so a re-run (schema.sql executes on every boot) is a no-op.
UPDATE hb_credentials c SET label = 'Основний'
 WHERE c.label = '' AND NOT EXISTS (SELECT 1 FROM hb_credentials d
   WHERE d.owner_id = c.owner_id AND d.venue = c.venue AND d.label = 'Основний');
INSERT INTO hb_profiles (owner_id, name, session_cfg, draft, selected)
SELECT s.owner_id, 'Основний', s.session_cfg, s.draft, TRUE FROM hb_settings s
ON CONFLICT (owner_id, name) DO NOTHING;
-- Google Sheets is account-level (one table across every profile), so its row is parked under a
-- reserved label that can never be a profile name -- renaming or deleting a profile must not take
-- the sheet with it.
UPDATE hb_credentials SET label = '*' WHERE venue = 'gsheets' AND label <> '*';
DELETE FROM hb_profiles WHERE name = '*';
INSERT INTO hb_profiles (owner_id, name)
SELECT DISTINCT c.owner_id, c.label FROM hb_credentials c WHERE c.venue <> 'gsheets'
ON CONFLICT (owner_id, name) DO NOTHING;
UPDATE hb_sessions SET profile = 'Основний' WHERE profile = '';
UPDATE hb_hedges   SET profile = 'Основний' WHERE profile = '';
-- Exactly one selected profile per owner (the menu needs a current one to show).
UPDATE hb_profiles p SET selected = TRUE
 WHERE NOT EXISTS (SELECT 1 FROM hb_profiles q WHERE q.owner_id = p.owner_id AND q.selected)
   AND p.created_at = (SELECT min(created_at) FROM hb_profiles r WHERE r.owner_id = p.owner_id);

-- Superseded by profiles: "which key is live" is decided by which PROFILE is selected, so a single
-- active row per (owner, venue) is simply wrong -- every profile owns its own key and all of them are
-- live at once. Left in place, the index rejected the second profile's key outright.
DROP INDEX IF EXISTS hb_credentials_one_active;
ALTER TABLE hb_credentials DROP COLUMN IF EXISTS is_active;
