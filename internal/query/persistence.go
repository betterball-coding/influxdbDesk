package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	queryKind              = "QUERY"
	queryDetailRetention   = 24 * time.Hour
	queryResourceRetention = 90 * 24 * time.Hour
)

type RetentionSweep struct {
	Archived int
	Purged   int
}

type queryMirror struct {
	profileID, profileRevision            sql.NullString
	connectionID, connectionGeneration    sql.NullString
	state                                 string
	terminal                              int
	snapshotRevision, stateRevision       string
	lastEventSeq                          string
	statementCount, seriesCount, rowCount sql.NullString
	hasStatementErrors, complete          int
	resultAvailable, closed, archived     int
	createdAt, updatedAt                  string
	terminalAt                            sql.NullString
}

func GetStoredSession(ctx context.Context, database *store.Store, sessionID string) (QuerySession, error) {
	if database == nil {
		return QuerySession{}, requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	snapshot, err := readStoredSessionTx(ctx, database.DB(), sessionID)
	if errors.Is(err, tasks.ErrNotFound) {
		return QuerySession{}, requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	return snapshot, err
}

func ListStoredSessions(ctx context.Context, database *store.Store, limit int) ([]QuerySession, error) {
	if database == nil {
		return nil, nil
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := database.DB().QueryContext(ctx, `SELECT session_id
		FROM query_sessions ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]QuerySession, 0, len(ids))
	for _, id := range ids {
		snapshot, err := readStoredSessionTx(ctx, database.DB(), id)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func readStoredSessionTx(ctx context.Context, tx store.Executor, sessionID string) (QuerySession, error) {
	meta, err := tasks.GetInTx(ctx, tx, sessionID)
	if err != nil {
		return QuerySession{}, err
	}
	if meta.Kind != queryKind {
		return QuerySession{}, tasks.ErrNotFound
	}
	var row queryMirror
	err = tx.QueryRowContext(ctx, `SELECT
		profile_id,profile_revision,connection_id,connection_generation,
		state,terminal,snapshot_revision,state_revision,last_event_seq,
		statement_count,series_count,row_count,has_statement_errors,complete,
		result_available,closed,archived,created_at,updated_at,terminal_at
		FROM query_sessions WHERE session_id=?`, sessionID).Scan(
		&row.profileID, &row.profileRevision, &row.connectionID, &row.connectionGeneration,
		&row.state, &row.terminal, &row.snapshotRevision, &row.stateRevision, &row.lastEventSeq,
		&row.statementCount, &row.seriesCount, &row.rowCount, &row.hasStatementErrors,
		&row.complete, &row.resultAvailable, &row.closed, &row.archived,
		&row.createdAt, &row.updatedAt, &row.terminalAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return QuerySession{}, tasks.ErrNotFound
	}
	if err != nil {
		return QuerySession{}, fmt.Errorf("read query session: %w", err)
	}
	if row.state != meta.State || row.terminal != queryBoolInt(meta.Terminal) ||
		row.snapshotRevision != meta.SnapshotRevision || row.stateRevision != meta.StateRevision ||
		row.lastEventSeq != meta.LastEventSeq || row.resultAvailable != queryBoolInt(meta.ResultAvailable) ||
		row.archived != queryBoolInt(meta.Archived) || row.createdAt != meta.CreatedAt || row.updatedAt != meta.UpdatedAt ||
		nullableText(row.terminalAt) != pointerText(meta.TerminalAt) {
		return QuerySession{}, errors.New("QUERY_PERSISTENCE_MISMATCH")
	}
	for name, value := range map[string]string{
		"snapshotRevision": meta.SnapshotRevision,
		"stateRevision":    meta.StateRevision,
		"lastEventSeq":     meta.LastEventSeq,
	} {
		if !isCanonicalUint(value) {
			return QuerySession{}, fmt.Errorf("INVALID_EXACT_NUMBER: %s", name)
		}
	}
	if row.connectionGeneration.Valid && !isCanonicalUint(row.connectionGeneration.String) {
		return QuerySession{}, errors.New("INVALID_EXACT_NUMBER: connectionGeneration")
	}
	if row.profileRevision.Valid && !isCanonicalUint(row.profileRevision.String) {
		return QuerySession{}, errors.New("INVALID_EXACT_NUMBER: profileRevision")
	}
	for name, value := range map[string]sql.NullString{
		"statementCount": row.statementCount,
		"seriesCount":    row.seriesCount,
		"rowCount":       row.rowCount,
	} {
		if value.Valid && !isCanonicalUint(value.String) {
			return QuerySession{}, fmt.Errorf("INVALID_EXACT_NUMBER: %s", name)
		}
	}
	if !meta.Archived && (!row.profileID.Valid || !row.profileRevision.Valid || !row.connectionID.Valid ||
		!row.connectionGeneration.Valid || !row.statementCount.Valid || !row.seriesCount.Valid || !row.rowCount.Valid) {
		return QuerySession{}, errors.New("QUERY_PERSISTENCE_INCOMPLETE")
	}
	if !validSessionState(SessionState(meta.State)) {
		return QuerySession{}, errors.New("QUERY_STATE_INVALID")
	}
	if _, err := time.Parse(time.RFC3339Nano, meta.CreatedAt); err != nil {
		return QuerySession{}, errors.New("QUERY_CREATED_AT_INVALID")
	}
	if _, err := time.Parse(time.RFC3339Nano, meta.UpdatedAt); err != nil {
		return QuerySession{}, errors.New("QUERY_UPDATED_AT_INVALID")
	}
	if meta.TerminalAt != nil {
		if _, err := time.Parse(time.RFC3339Nano, *meta.TerminalAt); err != nil {
			return QuerySession{}, errors.New("QUERY_TERMINAL_AT_INVALID")
		}
	}
	if meta.Terminal != isTerminalSessionState(SessionState(meta.State)) {
		return QuerySession{}, errors.New("QUERY_TERMINAL_STATE_MISMATCH")
	}

	snapshot := QuerySession{
		ID: sessionID, State: SessionState(meta.State), Terminal: meta.Terminal,
		SnapshotRevision: meta.SnapshotRevision, StateRevision: meta.StateRevision,
		LastEventSeq: meta.LastEventSeq, ProfileID: nullableText(row.profileID),
		ProfileRevision: nullableText(row.profileRevision), ConnectionID: nullableText(row.connectionID),
		Generation: nullableText(row.connectionGeneration), CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt,
		TerminalAt: cloneTextPointer(meta.TerminalAt), PublicErrorCode: pointerText(meta.PublicErrorCode),
		PublicSafeMessage: pointerText(meta.PublicSafeMessage), StatementCount: nullableText(row.statementCount),
		SeriesCount: nullableText(row.seriesCount), RowCount: nullableText(row.rowCount),
		HasStatementErrors: row.hasStatementErrors != 0, Complete: row.complete != 0,
		ResultAvailable: meta.ResultAvailable, Closed: row.closed != 0, Archived: meta.Archived,
	}
	return snapshot, nil
}

func insertQuerySessionTx(ctx context.Context, tx store.Executor, snapshot QuerySession) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO query_sessions(
		session_id,profile_id,profile_revision,connection_id,connection_generation,
		state,terminal,snapshot_revision,state_revision,last_event_seq,
		statement_count,series_count,row_count,has_statement_errors,complete,
		result_available,closed,archived,created_at,updated_at,terminal_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		snapshot.ID, nullableValue(snapshot.ProfileID), nullableValue(snapshot.ProfileRevision),
		nullableValue(snapshot.ConnectionID), nullableValue(snapshot.Generation), string(snapshot.State),
		queryBoolInt(snapshot.Terminal), snapshot.SnapshotRevision, snapshot.StateRevision,
		snapshot.LastEventSeq, nullableValue(snapshot.StatementCount), nullableValue(snapshot.SeriesCount),
		nullableValue(snapshot.RowCount), queryBoolInt(snapshot.HasStatementErrors), queryBoolInt(snapshot.Complete),
		queryBoolInt(snapshot.ResultAvailable), queryBoolInt(snapshot.Closed), queryBoolInt(snapshot.Archived),
		snapshot.CreatedAt, snapshot.UpdatedAt, pointerValue(snapshot.TerminalAt))
	if err != nil {
		return fmt.Errorf("insert query session: %w", err)
	}
	return nil
}

func updateQuerySessionTx(ctx context.Context, tx store.Executor, snapshot QuerySession) error {
	result, err := tx.ExecContext(ctx, `UPDATE query_sessions SET
		profile_id=?,profile_revision=?,connection_id=?,connection_generation=?,
		state=?,terminal=?,snapshot_revision=?,state_revision=?,last_event_seq=?,
		statement_count=?,series_count=?,row_count=?,has_statement_errors=?,complete=?,
		result_available=?,closed=?,archived=?,created_at=?,updated_at=?,terminal_at=?
		WHERE session_id=?`, nullableValue(snapshot.ProfileID), nullableValue(snapshot.ProfileRevision),
		nullableValue(snapshot.ConnectionID), nullableValue(snapshot.Generation), string(snapshot.State),
		queryBoolInt(snapshot.Terminal), snapshot.SnapshotRevision, snapshot.StateRevision,
		snapshot.LastEventSeq, nullableValue(snapshot.StatementCount), nullableValue(snapshot.SeriesCount),
		nullableValue(snapshot.RowCount), queryBoolInt(snapshot.HasStatementErrors), queryBoolInt(snapshot.Complete),
		queryBoolInt(snapshot.ResultAvailable), queryBoolInt(snapshot.Closed), queryBoolInt(snapshot.Archived),
		snapshot.CreatedAt, snapshot.UpdatedAt, pointerValue(snapshot.TerminalAt), snapshot.ID)
	if err != nil {
		return fmt.Errorf("update query session: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return tasks.ErrNotFound
	}
	return nil
}

func applyDesiredQueryTx(
	ctx context.Context,
	tx store.Executor,
	taskRepository *tasks.Repository,
	desired QuerySession,
	stateChanged bool,
	changeType string,
) (QuerySession, error) {
	current, err := tasks.GetInTx(ctx, tx, desired.ID)
	if err != nil {
		return QuerySession{}, err
	}
	archived, available := desired.Archived, desired.ResultAvailable
	change := tasks.Change{
		State: string(desired.State), Terminal: desired.Terminal, StateChanged: stateChanged,
		ChangeType: changeType, PublicErrorCode: optionalText(desired.PublicErrorCode),
		PublicSafeMessage: optionalText(desired.PublicSafeMessage), Archived: &archived,
		ResultAvailable: &available,
	}
	updated, err := taskRepository.ApplyInTx(ctx, tx, current, change)
	if err != nil {
		return QuerySession{}, err
	}
	applyTaskMeta(&desired, updated)
	if err := updateQuerySessionTx(ctx, tx, desired); err != nil {
		return QuerySession{}, err
	}
	return desired, nil
}

func (s *Service) persistDesired(ctx context.Context, desired QuerySession, stateChanged bool, changeType string) (QuerySession, error) {
	if s.store == nil || s.tasks == nil {
		return desired, nil
	}
	var result QuerySession
	err := s.store.WithImmediate(ctx, func(tx store.Executor) error {
		var err error
		result, err = applyDesiredQueryTx(ctx, tx, s.tasks, desired, stateChanged, changeType)
		return err
	})
	return result, err
}

func SweepRetention(
	ctx context.Context,
	database *store.Store,
	taskRepository *tasks.Repository,
	now time.Time,
) (RetentionSweep, error) {
	if database == nil || taskRepository == nil {
		return RetentionSweep{}, errors.New("query retention persistence is required")
	}
	var result RetentionSweep
	err := database.WithImmediate(ctx, func(tx store.Executor) error {
		rows, err := tx.QueryContext(ctx, `SELECT session_id,terminal_at,archived
			FROM query_sessions WHERE terminal=1 AND terminal_at IS NOT NULL`)
		if err != nil {
			return err
		}
		type candidate struct {
			id       string
			terminal time.Time
			archived bool
		}
		var candidates []candidate
		for rows.Next() {
			var item candidate
			var terminalAt string
			var archived int
			if err := rows.Scan(&item.id, &terminalAt, &archived); err != nil {
				rows.Close()
				return err
			}
			item.terminal, err = time.Parse(time.RFC3339Nano, terminalAt)
			if err != nil {
				rows.Close()
				return errors.New("QUERY_TERMINAL_AT_INVALID")
			}
			item.archived = archived != 0
			candidates = append(candidates, item)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, item := range candidates {
			if !now.Before(item.terminal.Add(queryResourceRetention)) {
				if _, err := tx.ExecContext(ctx, `DELETE FROM idempotency_ledger
					WHERE resource_kind='QUERY' AND resource_id=?`, item.id); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM command_ledger
					WHERE resource_kind='QUERY' AND resource_id=?`, item.id); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id=? AND kind='QUERY'`, item.id); err != nil {
					return err
				}
				result.Purged++
				continue
			}
			if item.archived || now.Before(item.terminal.Add(queryDetailRetention)) {
				continue
			}
			snapshot, err := readStoredSessionTx(ctx, tx, item.id)
			if err != nil {
				return err
			}
			snapshot.ProfileID = ""
			snapshot.ProfileRevision = ""
			snapshot.ConnectionID = ""
			snapshot.Generation = ""
			snapshot.StatementCount = ""
			snapshot.SeriesCount = ""
			snapshot.RowCount = ""
			snapshot.HasStatementErrors = false
			snapshot.Complete = false
			snapshot.ResultAvailable = false
			snapshot.PublicErrorCode = ""
			snapshot.PublicSafeMessage = ""
			snapshot.Closed = true
			snapshot.Archived = true
			if _, err := applyDesiredQueryTx(ctx, tx, taskRepository, snapshot, false, "ARCHIVED"); err != nil {
				return err
			}
			result.Archived++
		}
		return nil
	})
	return result, err
}

func applyTaskMeta(snapshot *QuerySession, meta tasks.Meta) {
	snapshot.State = SessionState(meta.State)
	snapshot.Terminal = meta.Terminal
	snapshot.SnapshotRevision = meta.SnapshotRevision
	snapshot.StateRevision = meta.StateRevision
	snapshot.LastEventSeq = meta.LastEventSeq
	snapshot.CreatedAt = meta.CreatedAt
	snapshot.UpdatedAt = meta.UpdatedAt
	snapshot.TerminalAt = cloneTextPointer(meta.TerminalAt)
	snapshot.PublicErrorCode = pointerText(meta.PublicErrorCode)
	snapshot.PublicSafeMessage = pointerText(meta.PublicSafeMessage)
	snapshot.Archived = meta.Archived
	snapshot.ResultAvailable = meta.ResultAvailable
}

func validSessionState(state SessionState) bool {
	switch state {
	case SessionQueued, SessionRunning, SessionCancelRequested, SessionSucceeded,
		SessionSucceededWithErrors, SessionTruncated, SessionTimedOut, SessionCanceled,
		SessionFailed, SessionInterrupted:
		return true
	default:
		return false
	}
}

func isTerminalSessionState(state SessionState) bool {
	switch state {
	case SessionSucceeded, SessionSucceededWithErrors, SessionTruncated, SessionTimedOut,
		SessionCanceled, SessionFailed, SessionInterrupted:
		return true
	default:
		return false
	}
}

func optionalText(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func nullableValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableText(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func pointerText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func cloneTextPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func pointerValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func queryBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
