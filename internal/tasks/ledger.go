package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
)

type IdempotencyEntry struct {
	Scope         string
	RequestDigest string
	ResourceKind  string
	ResourceID    string
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

func LookupIdempotencyInTx(ctx context.Context, tx store.Executor, scope string) (IdempotencyEntry, bool, error) {
	var entry IdempotencyEntry
	var createdAt, expiresAt string
	err := tx.QueryRowContext(ctx, `SELECT scope,request_digest,resource_kind,resource_id,
		created_at,expires_at FROM idempotency_ledger WHERE scope=?`, scope).Scan(
		&entry.Scope, &entry.RequestDigest, &entry.ResourceKind, &entry.ResourceID,
		&createdAt, &expiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return IdempotencyEntry{}, false, nil
	}
	if err != nil {
		return IdempotencyEntry{}, false, fmt.Errorf("read idempotency ledger: %w", err)
	}
	entry.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return IdempotencyEntry{}, false, fmt.Errorf("parse idempotency creation: %w", err)
	}
	entry.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return IdempotencyEntry{}, false, fmt.Errorf("parse idempotency expiry: %w", err)
	}
	return entry, true, nil
}

func InsertIdempotencyInTx(ctx context.Context, tx store.Executor, entry IdempotencyEntry) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO idempotency_ledger(
		scope,request_digest,resource_kind,resource_id,created_at,expires_at
	) VALUES(?,?,?,?,?,?)`, entry.Scope, entry.RequestDigest, entry.ResourceKind,
		entry.ResourceID, entry.CreatedAt.Format(time.RFC3339Nano), entry.ExpiresAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert idempotency ledger: %w", err)
	}
	return nil
}

type CommandEntry struct {
	Scope               string
	RequestDigest       string
	ResourceKind        string
	ResourceID          string
	ResultState         string
	ResultStateRevision string
	PublicErrorCode     *string
	CommittedAt         time.Time
	ExpiresAt           time.Time
}

func LookupCommandInTx(ctx context.Context, tx store.Executor, scope, digest string) (CommandEntry, bool, error) {
	var entry CommandEntry
	var code sql.NullString
	var committedAt, expiresAt string
	err := tx.QueryRowContext(ctx, `SELECT scope,request_digest,resource_kind,resource_id,
		result_state,result_state_revision,public_error_code,committed_at,expires_at
		FROM command_ledger WHERE scope=?`, scope).Scan(
		&entry.Scope, &entry.RequestDigest, &entry.ResourceKind, &entry.ResourceID,
		&entry.ResultState, &entry.ResultStateRevision, &code, &committedAt, &expiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return CommandEntry{}, false, nil
	}
	if err != nil {
		return CommandEntry{}, false, fmt.Errorf("read command ledger: %w", err)
	}
	if entry.RequestDigest != digest {
		return CommandEntry{}, false, ErrCommandIdempotencyConflict
	}
	if code.Valid {
		entry.PublicErrorCode = &code.String
	}
	entry.CommittedAt, err = time.Parse(time.RFC3339Nano, committedAt)
	if err != nil {
		return CommandEntry{}, false, err
	}
	entry.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return CommandEntry{}, false, err
	}
	return entry, true, nil
}

func InsertCommandInTx(ctx context.Context, tx store.Executor, entry CommandEntry) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO command_ledger(
		scope,request_digest,resource_kind,resource_id,result_state,result_state_revision,
		public_error_code,committed_at,expires_at
	) VALUES(?,?,?,?,?,?,?,?,?)`, entry.Scope, entry.RequestDigest, entry.ResourceKind,
		entry.ResourceID, entry.ResultState, entry.ResultStateRevision,
		ptrValue(entry.PublicErrorCode), entry.CommittedAt.Format(time.RFC3339Nano),
		entry.ExpiresAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert command ledger: %w", err)
	}
	return nil
}
