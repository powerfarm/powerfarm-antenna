-- Durable GitHub observation substrate. Raw receipt bytes remain in receipts;
-- every table below is semantic evidence or a replaceable projection.

CREATE TABLE IF NOT EXISTS github_deliveries (
    delivery_id          TEXT PRIMARY KEY,
    first_receipt_id     TEXT NOT NULL REFERENCES receipts(id),
    event_name           TEXT NOT NULL,
    action               TEXT,
    received_at          TEXT NOT NULL,
    last_received_at     TEXT NOT NULL,
    verification_status  TEXT NOT NULL,
    body_sha256          TEXT NOT NULL,
    body_size            INTEGER NOT NULL,
    installation_id      INTEGER,
    repository_id        INTEGER,
    repository_full_name TEXT,
    ref_name             TEXT,
    before_sha           TEXT,
    after_sha            TEXT,
    classification       TEXT NOT NULL CHECK (classification IN ('KNOWN','UNKNOWN')),
    processing_status    TEXT NOT NULL,
    duplicate_count      INTEGER NOT NULL DEFAULT 0,
    error                TEXT
);
CREATE INDEX IF NOT EXISTS github_deliveries_received_idx
    ON github_deliveries(received_at DESC);
CREATE INDEX IF NOT EXISTS github_deliveries_repo_idx
    ON github_deliveries(repository_id, received_at DESC);

CREATE TABLE IF NOT EXISTS github_delivery_receipts (
    receipt_id   TEXT PRIMARY KEY REFERENCES receipts(id),
    delivery_id  TEXT NOT NULL REFERENCES github_deliveries(delivery_id),
    duplicate    INTEGER NOT NULL CHECK (duplicate IN (0,1)),
    recorded_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS github_delivery_receipts_delivery_idx
    ON github_delivery_receipts(delivery_id, recorded_at);

CREATE TABLE IF NOT EXISTS observations (
    id                    TEXT PRIMARY KEY,
    evidence_key          TEXT NOT NULL UNIQUE,
    source                TEXT NOT NULL,
    source_identity       TEXT NOT NULL,
    source_receipt_id     TEXT REFERENCES receipts(id),
    github_delivery_id    TEXT REFERENCES github_deliveries(delivery_id),
    observed_at           TEXT NOT NULL,
    kind                  TEXT NOT NULL,
    classification        TEXT NOT NULL CHECK (classification IN ('KNOWN','UNKNOWN')),
    event_name            TEXT,
    action                TEXT,
    repository_id         INTEGER,
    repository_full_name  TEXT,
    body_sha256           TEXT,
    normalized_metadata   TEXT NOT NULL,
    reconciliation_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (reconciliation_status IN ('pending','reconciled','failed')),
    reconciliation_run_id TEXT,
    error                 TEXT
);
CREATE INDEX IF NOT EXISTS observations_pending_idx
    ON observations(reconciliation_status, observed_at);
CREATE INDEX IF NOT EXISTS observations_repo_idx
    ON observations(repository_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS repository_registry (
    repository_id          INTEGER PRIMARY KEY,
    full_name              TEXT NOT NULL,
    owner_login            TEXT,
    visibility             TEXT,
    archived               INTEGER,
    default_branch         TEXT,
    html_url               TEXT,
    github_updated_at      TEXT,
    observed_scope         TEXT NOT NULL,
    presence_state         TEXT NOT NULL,
    membership_treatment   TEXT NOT NULL DEFAULT 'unresolved',
    drift_state            TEXT NOT NULL,
    first_observed_at      TEXT NOT NULL,
    last_observed_at       TEXT NOT NULL,
    last_observation_id    TEXT NOT NULL REFERENCES observations(id),
    last_census_run_id     TEXT,
    last_event_name        TEXT,
    last_event_action      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS repository_registry_name_idx
    ON repository_registry(full_name);

CREATE TABLE IF NOT EXISTS census_runs (
    id                 TEXT PRIMARY KEY,
    organization       TEXT NOT NULL,
    installation_id    INTEGER,
    started_at         TEXT NOT NULL,
    completed_at       TEXT,
    status             TEXT NOT NULL CHECK (status IN ('running','completed','failed')),
    complete           INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0,1)),
    repository_count   INTEGER,
    page_count         INTEGER,
    api_rate_remaining INTEGER,
    api_rate_reset     TEXT,
    error              TEXT
);

CREATE TABLE IF NOT EXISTS census_repositories (
    census_run_id  TEXT NOT NULL REFERENCES census_runs(id),
    repository_id  INTEGER NOT NULL,
    full_name      TEXT NOT NULL,
    metadata_json  TEXT NOT NULL,
    PRIMARY KEY (census_run_id, repository_id)
);

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id                    TEXT PRIMARY KEY,
    trigger_kind          TEXT NOT NULL,
    trigger_identity      TEXT,
    started_at            TEXT NOT NULL,
    completed_at          TEXT,
    status                TEXT NOT NULL CHECK (status IN ('running','completed','failed')),
    observation_count     INTEGER NOT NULL DEFAULT 0,
    meaningful_count      INTEGER NOT NULL DEFAULT 0,
    unknown_count         INTEGER NOT NULL DEFAULT 0,
    projection_item_count INTEGER NOT NULL DEFAULT 0,
    result_json           TEXT,
    error                 TEXT
);

CREATE TABLE IF NOT EXISTS projection_records (
    record_id             TEXT PRIMARY KEY,
    desired_json          TEXT NOT NULL,
    desired_sha256        TEXT NOT NULL,
    reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(id),
    status                TEXT NOT NULL CHECK (status IN ('pending','in_flight','completed','uncertain','failed')),
    mutation_id           TEXT,
    updated_at            TEXT NOT NULL,
    completed_at          TEXT,
    last_error            TEXT
);
CREATE INDEX IF NOT EXISTS projection_records_status_idx
    ON projection_records(status, updated_at);

CREATE TABLE IF NOT EXISTS projection_mutations (
    mutation_id           TEXT PRIMARY KEY,
    reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(id),
    affected_record_ids   TEXT NOT NULL,
    payload_sha256        TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('in_flight','completed','uncertain','failed')),
    attempt               INTEGER NOT NULL DEFAULT 1,
    started_at            TEXT NOT NULL,
    completed_at          TEXT,
    effect_receipt_json   TEXT,
    error                 TEXT
);
