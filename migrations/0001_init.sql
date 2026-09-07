-- Antenna v0 — durable spine.
-- STATE / MEMORY / EXECUTION are kept distinct (spec §13).
-- Execution state (live sockets, in-flight requests) is deliberately NOT here.

CREATE TABLE IF NOT EXISTS receipts (
    id             TEXT PRIMARY KEY,
    kind           TEXT NOT NULL DEFAULT 'antenna:receipt',
    recorded_at    TEXT NOT NULL,

    scope          TEXT,
    actor          TEXT,
    principal      TEXT,
    provenance     TEXT,

    transport      TEXT NOT NULL,
    source         TEXT,
    content_type   TEXT,
    content_length INTEGER,

    raw_storage    TEXT NOT NULL CHECK (raw_storage IN ('sqlite','object','none')),
    raw_body       BLOB,
    raw_ref        TEXT,
    raw_digest     TEXT,

    interaction    TEXT NOT NULL CHECK (interaction IN ('event','blob','stream','call')),

    correlation_id TEXT,
    return_path    TEXT,
    route_hint     TEXT,

    relationships  TEXT,
    telemetry      TEXT,

    status         TEXT NOT NULL CHECK (status IN
                     ('accepted','routed','deferred','quarantined','failed','ignored'))
);
CREATE INDEX IF NOT EXISTS receipts_status_idx      ON receipts(status);
CREATE INDEX IF NOT EXISTS receipts_correlation_idx ON receipts(correlation_id);
CREATE INDEX IF NOT EXISTS receipts_recorded_idx    ON receipts(recorded_at DESC);

CREATE TABLE IF NOT EXISTS objects (
    digest       TEXT PRIMARY KEY,
    path         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    content_type TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id           TEXT PRIMARY KEY,
    receipt_id   TEXT NOT NULL REFERENCES receipts(id),
    capability   TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN
                   ('pending','running','completed','failed','interrupted')),
    input        TEXT,
    output       TEXT,
    error        TEXT,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    completed_at TEXT,
    telemetry    TEXT
);
CREATE INDEX IF NOT EXISTS runs_receipt_idx ON runs(receipt_id);
CREATE INDEX IF NOT EXISTS runs_status_idx  ON runs(status);

CREATE TABLE IF NOT EXISTS deliveries (
    id               TEXT PRIMARY KEY,
    receipt_id       TEXT REFERENCES receipts(id),
    run_id           TEXT REFERENCES runs(id),
    correlation_id   TEXT,

    destination      TEXT NOT NULL,
    transport        TEXT NOT NULL,

    -- Duplicate-delivery protection. One logical outward effect == one row.
    idempotency_key  TEXT NOT NULL UNIQUE,

    payload_ref      TEXT,
    payload_inline   BLOB,
    content_type     TEXT,

    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    status           TEXT NOT NULL CHECK (status IN
                       ('pending','in_flight','completed','failed','denied','orphaned')),
    next_attempt_at  TEXT,

    -- Set when a crash left this delivery in_flight: the outward effect is
    -- UNCERTAIN, not known-undone. Resent with the same idempotency key.
    resumed_uncertain INTEGER NOT NULL DEFAULT 0,

    created_at       TEXT NOT NULL,
    started_at       TEXT,
    completed_at     TEXT,
    last_error       TEXT,

    authority        TEXT,
    telemetry        TEXT
);
CREATE INDEX IF NOT EXISTS deliveries_pending_idx ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS deliveries_receipt_idx ON deliveries(receipt_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id  TEXT NOT NULL REFERENCES deliveries(id),
    attempt      INTEGER NOT NULL,
    started_at   TEXT NOT NULL,
    completed_at TEXT,
    outcome      TEXT,
    detail       TEXT
);
CREATE INDEX IF NOT EXISTS delivery_attempts_idx ON delivery_attempts(delivery_id);

CREATE TABLE IF NOT EXISTS route_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT NOT NULL REFERENCES receipts(id),
    decided_at TEXT NOT NULL,
    route_name TEXT,
    action     TEXT NOT NULL,
    target     TEXT,
    detail     TEXT
);
CREATE INDEX IF NOT EXISTS route_events_receipt_idx ON route_events(receipt_id);
