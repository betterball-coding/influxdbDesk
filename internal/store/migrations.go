package store

import (
	"context"
	"fmt"
)

type migration struct {
	version int
	sql     string
}

var migrations = []migration{
	{version: 1, sql: schemaV1},
	{version: 2, sql: schemaV2},
	{version: 3, sql: schemaV3},
	{version: 4, sql: schemaV4},
	{version: 5, sql: schemaV5},
	{version: 6, sql: schemaV6},
	{version: 7, sql: schemaV7},
	{version: 8, sql: schemaV8},
	{version: 9, sql: schemaV9},
	{version: 10, sql: schemaV10},
	{version: 11, sql: schemaV11},
	{version: 12, sql: schemaV12},
	{version: 13, sql: schemaV13},
	{version: 14, sql: schemaV14},
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	for _, m := range migrations {
		var count int
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", m.version,
		).Scan(&count); err != nil {
			return fmt.Errorf("read migration %d: %w", m.version, err)
		}
		if count != 0 {
			continue
		}
		if err := s.WithImmediate(ctx, func(tx Executor) error {
			if _, err := tx.ExecContext(ctx, m.sql); err != nil {
				return fmt.Errorf("apply migration %d: %w", m.version, err)
			}
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO schema_migrations(version, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
				m.version,
			); err != nil {
				return fmt.Errorf("record migration %d: %w", m.version, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

const schemaV1 = `
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    terminal INTEGER NOT NULL CHECK (terminal IN (0, 1)),
    snapshot_revision TEXT NOT NULL,
    state_revision TEXT NOT NULL,
    last_event_seq TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    terminal_at TEXT,
    public_error_code TEXT,
    public_safe_message TEXT,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    result_available INTEGER NOT NULL DEFAULT 1 CHECK (result_available IN (0, 1))
);

CREATE TABLE task_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    resource_revision TEXT NOT NULL,
    state TEXT NOT NULL,
    change_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY(resource_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX task_events_resource_idx ON task_events(resource_id, seq);

CREATE TABLE idempotency_ledger (
    scope TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX idempotency_resource_idx ON idempotency_ledger(resource_kind, resource_id);

CREATE TABLE command_ledger (
    scope TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    result_state TEXT NOT NULL,
    result_state_revision TEXT NOT NULL,
    public_error_code TEXT,
    committed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX command_resource_idx ON command_ledger(resource_kind, resource_id);

CREATE TABLE protection_snapshots (
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    profile_revision TEXT NOT NULL,
    protection_revision TEXT NOT NULL,
    mode TEXT NOT NULL,
    closed_at TEXT,
    purge_after TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(connection_id, connection_generation)
);
CREATE UNIQUE INDEX protection_one_active_generation_idx
    ON protection_snapshots(connection_id) WHERE closed_at IS NULL;

CREATE TABLE protection_command_ledger (
    scope TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    command_kind TEXT NOT NULL,
    command_request_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_code TEXT NOT NULL,
    applied_protection_revision TEXT,
    committed_at TEXT NOT NULL,
    UNIQUE(connection_id, connection_generation, command_kind, command_request_id)
);
CREATE INDEX protection_command_generation_idx
    ON protection_command_ledger(connection_id, connection_generation);

CREATE TABLE protection_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    protection_revision TEXT NOT NULL,
    mode TEXT NOT NULL,
    change_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX protection_events_generation_idx
    ON protection_events(connection_id, connection_generation, seq);

CREATE TABLE transfer_jobs (
    job_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('IMPORT', 'EXPORT')),
    state TEXT NOT NULL,
    retained INTEGER NOT NULL DEFAULT 1 CHECK (retained IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX transfer_jobs_profile_idx ON transfer_jobs(profile_id, retained);

CREATE TABLE transfer_reservations (
    reservation_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('PRIVATE', 'VOLUME')),
    scope_id TEXT NOT NULL,
    reserved_bytes TEXT NOT NULL,
    consumed_bytes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES transfer_jobs(job_id) ON DELETE CASCADE
);
CREATE INDEX transfer_reservation_scope_idx
    ON transfer_reservations(scope_kind, scope_id);

CREATE TABLE import_jobs (
    job_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    state_revision TEXT NOT NULL,
    snapshot_revision TEXT NOT NULL,
    checkpoint_digest TEXT NOT NULL,
    active_run_segment_id TEXT,
    pause_requested INTEGER NOT NULL DEFAULT 0 CHECK (pause_requested IN (0, 1)),
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES transfer_jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE import_checkpoints (
    job_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    sequence TEXT NOT NULL,
    parent_digest TEXT,
    logical_offset TEXT NOT NULL,
    last_settled_batch_id TEXT,
    last_disposition TEXT,
    adaptive_max_points TEXT NOT NULL,
    adaptive_max_bytes TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    staging_sha256 TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    target_digest TEXT NOT NULL,
    lossy INTEGER NOT NULL CHECK (lossy IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY(job_id, digest),
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE import_checkpoint_transitions (
    job_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    owner_run_segment_id TEXT NOT NULL,
    cause TEXT NOT NULL,
    parent_checkpoint_digest TEXT NOT NULL,
    child_checkpoint_digest TEXT NOT NULL,
    batch_id TEXT,
    attempt_id TEXT,
    incident_id TEXT,
    command_request_id TEXT,
    committed_at TEXT NOT NULL,
    PRIMARY KEY(job_id, child_checkpoint_digest),
    UNIQUE(job_id, parent_checkpoint_digest),
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE import_run_segments (
    run_segment_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    initial_checkpoint_digest TEXT NOT NULL,
    committed_checkpoint_digest TEXT NOT NULL,
    active_attempt_id TEXT,
    stop_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX import_one_live_segment_idx ON import_run_segments(job_id)
    WHERE state IN ('ACTIVE', 'STOPPING');

CREATE TABLE import_attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    run_segment_id TEXT NOT NULL,
    state TEXT NOT NULL,
    parent_checkpoint_digest TEXT NOT NULL,
    new_checkpoint_digest TEXT,
    start_offset TEXT NOT NULL,
    end_offset TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY(run_segment_id) REFERENCES import_run_segments(run_segment_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX import_one_live_attempt_idx ON import_attempts(job_id)
    WHERE state = 'SENDING';

CREATE TABLE import_incidents (
    incident_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('PARTIAL', 'UNKNOWN')),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED', 'ABORTED')),
    parent_checkpoint_digest TEXT NOT NULL,
    run_segment_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    start_offset TEXT NOT NULL,
    end_offset TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    replay_parent_incident_id TEXT,
    resolution TEXT,
    resolved_at TEXT,
    resolution_command_request_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX import_one_open_incident_idx ON import_incidents(job_id)
    WHERE status = 'OPEN';

CREATE TABLE audit_events (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    action TEXT NOT NULL,
    target_id TEXT NOT NULL,
    target_digest TEXT,
    outcome TEXT NOT NULL,
    correlation_id TEXT,
    occurred_at TEXT NOT NULL
);
`

const schemaV2 = `
CREATE TABLE profiles (
    id TEXT PRIMARY KEY,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('production', 'staging', 'development')),
    auth_mode TEXT NOT NULL CHECK (auth_mode IN ('NONE', 'BASIC', 'BEARER')),
    username TEXT,
    credential_kind TEXT,
    credential_ref TEXT,
    protection_mode TEXT NOT NULL CHECK (protection_mode IN ('PermanentReadOnly', 'ProtectedLocked')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (auth_mode = 'NONE' AND credential_kind IS NULL AND credential_ref IS NULL)
        OR (auth_mode = 'BASIC' AND username IS NOT NULL AND credential_kind = 'influx-password' AND credential_ref IS NOT NULL)
        OR (auth_mode = 'BEARER' AND credential_kind = 'jwt' AND credential_ref IS NOT NULL)
    )
);

CREATE TABLE connection_generation_counters (
    profile_id TEXT PRIMARY KEY,
    last_generation TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
`

const schemaV3 = `
CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    profile_revision TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    protection_revision TEXT NOT NULL,
    action_digest TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    dispatch_attempted INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_attempted IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX operations_connection_idx
    ON operations(connection_id, connection_generation, created_at);
`

const schemaV4 = `
ALTER TABLE operations ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0
    CHECK (cancel_requested IN (0, 1));
`

const schemaV5 = `
CREATE TABLE query_sessions (
    session_id TEXT PRIMARY KEY,
    profile_id TEXT,
    profile_revision TEXT,
    connection_id TEXT,
    connection_generation TEXT,
    state TEXT NOT NULL,
    terminal INTEGER NOT NULL CHECK (terminal IN (0, 1)),
    snapshot_revision TEXT NOT NULL,
    state_revision TEXT NOT NULL,
    last_event_seq TEXT NOT NULL,
    statement_count TEXT,
    series_count TEXT,
    row_count TEXT,
    has_statement_errors INTEGER NOT NULL DEFAULT 0 CHECK (has_statement_errors IN (0, 1)),
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    result_available INTEGER NOT NULL DEFAULT 0 CHECK (result_available IN (0, 1)),
    closed INTEGER NOT NULL DEFAULT 0 CHECK (closed IN (0, 1)),
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    terminal_at TEXT,
    FOREIGN KEY(session_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX query_sessions_connection_idx
    ON query_sessions(connection_id, connection_generation, created_at);
CREATE INDEX query_sessions_retention_idx
    ON query_sessions(terminal, archived, terminal_at);
`

const schemaV6 = `
ALTER TABLE import_run_segments ADD COLUMN after_resolution TEXT
    CHECK (after_resolution IS NULL OR after_resolution IN ('CONTINUE', 'PAUSE'));
ALTER TABLE import_run_segments ADD COLUMN resolution_command_request_id TEXT;
`

const schemaV7 = `
CREATE TABLE export_jobs (
    job_id TEXT PRIMARY KEY,
    profile_revision TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    target_digest TEXT NOT NULL,
    target_volume_id TEXT NOT NULL,
    target_reservation_id TEXT,
    state TEXT NOT NULL CHECK (state IN (
        'QUEUED', 'RUNNING', 'FINALIZING', 'PAUSED_RESTARTABLE',
        'CLEANING', 'SUCCEEDED', 'CANCELED', 'FAILED'
    )),
    state_revision TEXT NOT NULL,
    snapshot_revision TEXT NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    cleanup_final_state TEXT CHECK (cleanup_final_state IS NULL OR cleanup_final_state IN (
        'SUCCEEDED', 'CANCELED', 'FAILED'
    )),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES tasks(id) ON DELETE CASCADE,
    FOREIGN KEY(job_id) REFERENCES transfer_jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY(target_reservation_id) REFERENCES transfer_reservations(reservation_id) ON DELETE SET NULL
);
CREATE INDEX export_jobs_connection_idx
    ON export_jobs(connection_id, connection_generation, state, created_at);
CREATE INDEX export_jobs_state_idx ON export_jobs(state, updated_at);

CREATE TABLE export_fragments (
    fragment_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    ordinal TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('DATA', 'MANIFEST')),
    state TEXT NOT NULL CHECK (state IN ('WRITING', 'FINALIZING', 'COMPLETE', 'CORRUPT')),
    start_ns TEXT,
    end_ns TEXT,
    part_path TEXT,
    final_path TEXT,
    compressed_size TEXT,
    uncompressed_size TEXT,
    checksum_sha256 TEXT,
    validation_epoch TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(job_id, ordinal),
    FOREIGN KEY(job_id) REFERENCES export_jobs(job_id) ON DELETE CASCADE
);
CREATE INDEX export_fragments_job_state_idx
    ON export_fragments(job_id, state, ordinal);
`

const schemaV8 = `
CREATE TABLE import_preflight_details (
    job_id TEXT PRIMARY KEY,
    profile_revision TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    connection_generation TEXT NOT NULL,
    source_identity_digest TEXT NOT NULL,
    expected_source_sha256 TEXT NOT NULL,
    expected_source_size TEXT NOT NULL,
    import_format TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    target_digest TEXT NOT NULL,
    staging_file_name TEXT,
    source_sha256 TEXT,
    staging_sha256 TEXT,
    logical_bytes TEXT,
    point_count TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
);
CREATE INDEX import_preflight_connection_idx
    ON import_preflight_details(connection_id, connection_generation, updated_at);
`

const schemaV9 = `
DROP INDEX task_events_resource_idx;
ALTER TABLE task_events RENAME TO task_events_integer_seq;

CREATE TABLE task_events (
    seq TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    resource_revision TEXT NOT NULL,
    state TEXT NOT NULL,
    change_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY(resource_id) REFERENCES tasks(id) ON DELETE CASCADE
);
INSERT INTO task_events(
    seq,schema_version,kind,resource_id,resource_revision,state,change_type,occurred_at
)
SELECT CAST(seq AS TEXT),schema_version,kind,resource_id,resource_revision,state,change_type,occurred_at
FROM task_events_integer_seq;
DROP TABLE task_events_integer_seq;
CREATE INDEX task_events_resource_idx ON task_events(resource_id, length(seq), seq);

CREATE TABLE task_event_head (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    seq TEXT NOT NULL
);
INSERT INTO task_event_head(singleton,seq)
VALUES(1,COALESCE((
    SELECT seq FROM task_events ORDER BY length(seq) DESC,seq DESC LIMIT 1
),'0'));
`

const schemaV10 = `
CREATE TABLE export_artifact_reservations (
    artifact_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'SETTLED', 'ABORTED')),
    active_attempt_id TEXT NOT NULL,
    reserved_bytes TEXT NOT NULL,
    settled_bytes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(job_id, artifact_id),
    FOREIGN KEY(job_id) REFERENCES export_jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY(artifact_id) REFERENCES export_fragments(fragment_id) ON DELETE CASCADE
);
CREATE INDEX export_artifact_reservations_job_state_idx
    ON export_artifact_reservations(job_id, state, artifact_id);

CREATE TABLE export_artifact_reservation_extents (
    artifact_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    extent_ordinal TEXT NOT NULL,
    reservation_id TEXT NOT NULL UNIQUE,
    reserved_bytes TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(artifact_id, extent_ordinal),
    FOREIGN KEY(job_id, artifact_id)
        REFERENCES export_artifact_reservations(job_id, artifact_id) ON DELETE CASCADE,
    FOREIGN KEY(reservation_id)
        REFERENCES transfer_reservations(reservation_id) ON DELETE CASCADE
);
CREATE INDEX export_artifact_reservation_extents_job_idx
    ON export_artifact_reservation_extents(job_id, artifact_id, length(extent_ordinal), extent_ordinal);
`

const schemaV11 = `
CREATE TABLE export_plan_details (
    job_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    database_name TEXT NOT NULL,
    retention_policy TEXT NOT NULL,
    measurement TEXT NOT NULL,
    start_ns TEXT NOT NULL,
    end_ns TEXT NOT NULL,
    slice_width_ns TEXT NOT NULL,
    output_directory TEXT NOT NULL,
    type_preserving INTEGER NOT NULL CHECK (type_preserving IN (0, 1)),
    lossy INTEGER NOT NULL CHECK (lossy IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES export_jobs(job_id) ON DELETE CASCADE
);
`

const schemaV12 = `
ALTER TABLE import_preflight_details ADD COLUMN target_database TEXT;
ALTER TABLE import_preflight_details ADD COLUMN target_retention_policy TEXT;
`

const schemaV13 = `
ALTER TABLE profiles ADD COLUMN default_database TEXT NOT NULL DEFAULT '';
`

const schemaV14 = `
ALTER TABLE profiles ADD COLUMN allow_insecure_auth INTEGER NOT NULL DEFAULT 0
    CHECK (allow_insecure_auth IN (0, 1));
`
