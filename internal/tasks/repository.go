package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
)

type Clock func() time.Time

type Repository struct {
	store *store.Store
	now   Clock
}

func NewRepository(s *store.Store, now Clock) *Repository {
	if now == nil {
		now = time.Now
	}
	return &Repository{store: s, now: now}
}

func (r *Repository) Create(ctx context.Context, req CreateRequest) (Meta, bool, error) {
	if req.ID == "" || req.Kind == "" || req.InitialState == "" || req.ClientScope == "" || req.RequestDigest == "" {
		return Meta{}, false, errors.New("invalid task create request")
	}
	if req.LedgerExpiresAt.IsZero() {
		return Meta{}, false, errors.New("idempotency expiry is required")
	}

	var out Meta
	replayed := false
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := LookupIdempotencyInTx(ctx, tx, req.ClientScope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != req.RequestDigest {
				return ErrIdempotencyConflict
			}
			out, err = GetInTx(ctx, tx, entry.ResourceID)
			replayed = true
			return err
		}

		out, err = r.CreateInTx(ctx, tx, req.ID, req.Kind, req.InitialState, req.ResultAvailable)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		return InsertIdempotencyInTx(ctx, tx, IdempotencyEntry{
			Scope: req.ClientScope, RequestDigest: req.RequestDigest,
			ResourceKind: req.Kind, ResourceID: req.ID,
			CreatedAt: now, ExpiresAt: req.LedgerExpiresAt.UTC(),
		})
	})
	return out, replayed, err
}

// CreateInTx creates the task snapshot and its initial event without opening
// a transaction. It is used by aggregate repositories that must create their
// own rows and the task atomically.
func (r *Repository) CreateInTx(ctx context.Context, tx store.Executor, id, kind, initialState string, resultAvailable bool) (Meta, error) {
	now := r.now().UTC()
	out := Meta{
		ID: id, Kind: kind, State: initialState,
		SnapshotRevision: "1", StateRevision: "1", LastEventSeq: "0",
		CreatedAt: now.Format(time.RFC3339Nano), UpdatedAt: now.Format(time.RFC3339Nano),
		ResultAvailable: resultAvailable,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(
		id,kind,state,terminal,snapshot_revision,state_revision,last_event_seq,
		created_at,updated_at,archived,result_available
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, out.ID, out.Kind, out.State, 0,
		out.SnapshotRevision, out.StateRevision, out.LastEventSeq,
		out.CreatedAt, out.UpdatedAt, 0, boolInt(out.ResultAvailable)); err != nil {
		return Meta{}, fmt.Errorf("insert task: %w", err)
	}
	if err := appendEventInTx(ctx, tx, &out, "CREATED", now); err != nil {
		return Meta{}, err
	}
	return out, nil
}

func (r *Repository) Get(ctx context.Context, id string) (Meta, error) {
	return GetInTx(ctx, r.store.DB(), id)
}

func GetInTx(ctx context.Context, tx store.Executor, id string) (Meta, error) {
	var m Meta
	var terminal, archived, resultAvailable int
	var terminalAt, errorCode, safeMessage sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT
		id,kind,state,terminal,snapshot_revision,state_revision,last_event_seq,
		created_at,updated_at,terminal_at,public_error_code,public_safe_message,
		archived,result_available
		FROM tasks WHERE id=?`, id).Scan(
		&m.ID, &m.Kind, &m.State, &terminal, &m.SnapshotRevision, &m.StateRevision,
		&m.LastEventSeq, &m.CreatedAt, &m.UpdatedAt, &terminalAt, &errorCode,
		&safeMessage, &archived, &resultAvailable,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Meta{}, ErrNotFound
	}
	if err != nil {
		return Meta{}, fmt.Errorf("read task: %w", err)
	}
	m.Terminal = terminal != 0
	m.Archived = archived != 0
	m.ResultAvailable = resultAvailable != 0
	if terminalAt.Valid {
		m.TerminalAt = &terminalAt.String
	}
	if errorCode.Valid {
		m.PublicErrorCode = &errorCode.String
	}
	if safeMessage.Valid {
		m.PublicSafeMessage = &safeMessage.String
	}
	return m, nil
}

func (r *Repository) Apply(ctx context.Context, id, expectedStateRevision string, change Change) (Meta, error) {
	var out Meta
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, err := GetInTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if expectedStateRevision != "" && current.StateRevision != expectedStateRevision {
			return ErrRevisionConflict
		}
		out, err = r.ApplyInTx(ctx, tx, current, change)
		return err
	})
	return out, err
}

func (r *Repository) ApplyInTx(ctx context.Context, tx store.Executor, current Meta, change Change) (Meta, error) {
	if change.ChangeType == "" {
		return Meta{}, errors.New("task change type is required")
	}
	if err := validatePublicText(change.PublicErrorCode, change.PublicSafeMessage); err != nil {
		return Meta{}, err
	}

	snapshotRevision, err := incrementDecimal(current.SnapshotRevision)
	if err != nil {
		return Meta{}, err
	}
	stateRevision := current.StateRevision
	if change.StateChanged {
		stateRevision, err = incrementDecimal(current.StateRevision)
		if err != nil {
			return Meta{}, err
		}
	}
	now := r.now().UTC()
	previousSnapshotRevision := current.SnapshotRevision
	current.SnapshotRevision = snapshotRevision
	current.StateRevision = stateRevision
	current.UpdatedAt = now.Format(time.RFC3339Nano)
	if change.State != "" {
		current.State = change.State
	}
	current.Terminal = change.Terminal
	if current.Terminal && current.TerminalAt == nil {
		terminalAt := current.UpdatedAt
		current.TerminalAt = &terminalAt
	}
	current.PublicErrorCode = change.PublicErrorCode
	current.PublicSafeMessage = change.PublicSafeMessage
	if change.Archived != nil {
		current.Archived = *change.Archived
	}
	if change.ResultAvailable != nil {
		current.ResultAvailable = *change.ResultAvailable
	}

	result, err := tx.ExecContext(ctx, `UPDATE tasks SET
		state=?,terminal=?,snapshot_revision=?,state_revision=?,updated_at=?,terminal_at=?,
		public_error_code=?,public_safe_message=?,archived=?,result_available=?
		WHERE id=? AND snapshot_revision=?`,
		current.State, boolInt(current.Terminal), current.SnapshotRevision, current.StateRevision,
		current.UpdatedAt, ptrValue(current.TerminalAt), ptrValue(current.PublicErrorCode),
		ptrValue(current.PublicSafeMessage), boolInt(current.Archived), boolInt(current.ResultAvailable),
		current.ID, previousSnapshotRevision,
	)
	if err != nil {
		return Meta{}, fmt.Errorf("update task: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Meta{}, fmt.Errorf("read updated task rows: %w", err)
	}
	if rows != 1 {
		return Meta{}, ErrRevisionConflict
	}
	if err := appendEventInTx(ctx, tx, &current, change.ChangeType, now); err != nil {
		return Meta{}, err
	}
	return current, nil
}

func appendEventInTx(ctx context.Context, tx store.Executor, meta *Meta, changeType string, at time.Time) error {
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT seq FROM task_event_head WHERE singleton=1`).Scan(&current); err != nil {
		return fmt.Errorf("read task event head: %w", err)
	}
	next, err := incrementDecimal(current)
	if err != nil {
		return fmt.Errorf("increment task event head: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO task_events(
		seq,schema_version,kind,resource_id,resource_revision,state,change_type,occurred_at
	) VALUES(?,1,?,?,?,?,?,?)`, next, meta.Kind, meta.ID, meta.SnapshotRevision, meta.State,
		changeType, at.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert task event: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE task_event_head SET seq=? WHERE singleton=1 AND seq=?`, next, current)
	if err != nil {
		return fmt.Errorf("update task event head: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrRevisionConflict
	}
	meta.LastEventSeq = next
	if _, err := tx.ExecContext(ctx,
		"UPDATE tasks SET last_event_seq=? WHERE id=?", meta.LastEventSeq, meta.ID,
	); err != nil {
		return fmt.Errorf("update task event head: %w", err)
	}
	return nil
}

func (r *Repository) EventHead(ctx context.Context) (string, error) {
	var head string
	if err := r.store.DB().QueryRowContext(ctx, `SELECT seq FROM task_event_head WHERE singleton=1`).Scan(&head); err != nil {
		return "", fmt.Errorf("read event head: %w", err)
	}
	if _, err := canonicalDecimal(head); err != nil {
		return "", err
	}
	return head, nil
}

func (r *Repository) Changes(ctx context.Context, after string, limit int) ([]Event, error) {
	canonical, err := canonicalDecimal(after)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := r.store.DB().QueryContext(ctx, `SELECT
		schema_version,seq,kind,resource_id,resource_revision,state,change_type,occurred_at
		FROM task_events
		WHERE length(seq)>length(?) OR (length(seq)=length(?) AND seq>?)
		ORDER BY length(seq),seq LIMIT ?`, canonical, canonical, canonical, limit)
	if err != nil {
		return nil, fmt.Errorf("read task changes: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.SchemaVersion, &event.Seq, &event.Kind, &event.ID,
			&event.ResourceRevision, &event.State, &event.ChangeType, &event.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan task event: %w", err)
		}
		if _, err := canonicalDecimal(event.Seq); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

// RecoverCore applies the process-restart state contract before network entry
// points are opened. Import recovery is owned by transfer.Repository because
// it must reconcile attempts and incidents in the same transaction.
func (r *Repository) RecoverCore(ctx context.Context) (int, error) {
	rows, err := r.store.DB().QueryContext(ctx, `SELECT id FROM tasks
		WHERE terminal=0 AND kind IN ('QUERY','OPERATION','EXPORT')`)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	changed := 0
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		for _, id := range ids {
			meta, err := GetInTx(ctx, tx, id)
			if err != nil {
				return err
			}
			var change Change
			switch meta.Kind {
			case "QUERY":
				available := false
				code, message := "LOST_ON_RESTART", "Query interrupted by application restart"
				change = Change{State: "INTERRUPTED", Terminal: true, StateChanged: true,
					ChangeType: "RECOVERED", ResultAvailable: &available,
					PublicErrorCode: &code, PublicSafeMessage: &message}
			case "OPERATION":
				switch meta.State {
				case "QUEUED":
					change = Change{State: "INTERRUPTED_NOT_SENT", Terminal: true, StateChanged: true, ChangeType: "RECOVERED"}
				case "DISPATCHING":
					change = Change{State: "OUTCOME_UNKNOWN", Terminal: true, StateChanged: true, ChangeType: "RECOVERED"}
				default:
					continue
				}
			case "EXPORT":
				change = Change{State: "PAUSED_RESTARTABLE", StateChanged: true, ChangeType: "RECOVERED"}
			default:
				continue
			}
			updated, err := r.ApplyInTx(ctx, tx, meta, change)
			if err != nil {
				return err
			}
			if meta.Kind == "QUERY" {
				result, err := tx.ExecContext(ctx, `UPDATE query_sessions SET
					state=?,terminal=?,snapshot_revision=?,state_revision=?,last_event_seq=?,
					result_available=?,archived=?,updated_at=?,terminal_at=?
					WHERE session_id=?`, updated.State, boolInt(updated.Terminal),
					updated.SnapshotRevision, updated.StateRevision, updated.LastEventSeq,
					boolInt(updated.ResultAvailable), boolInt(updated.Archived), updated.UpdatedAt,
					ptrValue(updated.TerminalAt), updated.ID)
				if err != nil {
					return err
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					if err != nil {
						return err
					}
					return errors.New("query recovery mirror is missing")
				}
			}
			changed++
		}
		return nil
	})
	return changed, err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func ptrValue(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}
