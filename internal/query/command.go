package query

import (
	"context"
	"errors"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	queryCommandCancel = "CANCEL"
	queryCommandClose  = "CLOSE"
)

func (s *Service) CancelQuery(ctx context.Context, id string, request CommandEnvelope) (QuerySession, error) {
	return s.applyCommand(ctx, id, queryCommandCancel, request)
}

func (s *Service) CloseQuery(ctx context.Context, id string, request CommandEnvelope) (QuerySession, error) {
	return s.applyCommand(ctx, id, queryCommandClose, request)
}

func CancelStoredQuery(
	ctx context.Context,
	database *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	id string,
	request CommandEnvelope,
) (QuerySession, error) {
	return applyStoredCommand(ctx, database, taskRepository, now, id, queryCommandCancel, request, false)
}

func CloseStoredQuery(
	ctx context.Context,
	database *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	id string,
	request CommandEnvelope,
) (QuerySession, error) {
	return applyStoredCommand(ctx, database, taskRepository, now, id, queryCommandClose, request, false)
}

func (s *Service) applyCommand(
	ctx context.Context,
	id, kind string,
	request CommandEnvelope,
) (QuerySession, error) {
	revision, scope, digest, err := validateQueryCommand(id, kind, request)
	if err != nil {
		return QuerySession{}, err
	}
	if s.store == nil || s.tasks == nil {
		return s.applyInMemoryCommand(id, kind, request, revision)
	}

	var (
		resultToClose *ResultSet
		cancel        context.CancelFunc
	)
	s.mu.Lock()
	snapshot, applied, err := applyStoredCommandValidated(ctx, s.store, s.tasks, s.options.Now,
		id, kind, request, revision, scope, digest, true)
	if err == nil {
		if record := s.sessions[id]; record != nil {
			record.snapshot = snapshot
			if applied && (kind == queryCommandCancel || kind == queryCommandClose) {
				cancel = record.cancel
			}
			if applied && snapshot.State == SessionCanceled {
				if lane := s.lanes[record.laneKey]; lane != nil {
					lane.queue = removeString(lane.queue, id)
				}
			}
			if kind == queryCommandClose && snapshot.Closed {
				record.closed = true
				resultToClose = record.result
				record.result = nil
				record.rawIssue = ""
				if record.resultExpiry != nil {
					record.resultExpiry.Stop()
					record.resultExpiry = nil
				}
			}
		}
	}
	s.mu.Unlock()
	if err != nil {
		return snapshot, queryCommandError(err)
	}
	if cancel != nil {
		cancel()
	}
	if err := closeResultSet(resultToClose); err != nil {
		return QuerySession{}, requestError("RESULT_CLEANUP_FAILED", "查询结果文件无法删除")
	}
	return snapshot, nil
}

func applyStoredCommand(
	ctx context.Context,
	database *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	id, kind string,
	request CommandEnvelope,
	allowActive bool,
) (QuerySession, error) {
	revision, scope, digest, err := validateQueryCommand(id, kind, request)
	if err != nil {
		return QuerySession{}, err
	}
	snapshot, _, err := applyStoredCommandValidated(ctx, database, taskRepository, now,
		id, kind, request, revision, scope, digest, allowActive)
	return snapshot, queryCommandError(err)
}

func applyStoredCommandValidated(
	ctx context.Context,
	database *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	id, kind string,
	request CommandEnvelope,
	revision, scope, digest string,
	allowActive bool,
) (snapshot QuerySession, applied bool, err error) {
	if now == nil {
		now = time.Now
	}
	err = database.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			if entry.ResourceKind != queryKind || entry.ResourceID != id {
				return tasks.ErrCommandIdempotencyConflict
			}
			snapshot, err = readStoredSessionTx(ctx, tx, id)
			snapshot.Replayed = true
			return err
		}
		current, err := readStoredSessionTx(ctx, tx, id)
		if err != nil {
			return err
		}
		snapshot = current
		targetSatisfied := current.Terminal || current.State == SessionCancelRequested
		if kind == queryCommandClose {
			targetSatisfied = current.Closed
		}
		committedAt := now().UTC()
		if targetSatisfied {
			if err := insertQueryCommand(ctx, tx, scope, digest, id, current, committedAt); err != nil {
				return err
			}
			snapshot = current
			return nil
		}
		if current.StateRevision != revision {
			snapshot = current
			return tasks.ErrRevisionConflict
		}
		if !allowActive && !current.Terminal {
			snapshot = current
			return errors.New("QUERY_STATE_CONFLICT")
		}

		desired := current
		switch kind {
		case queryCommandCancel:
			switch current.State {
			case SessionQueued:
				desired.State, desired.Terminal, desired.Complete = SessionCanceled, true, false
			case SessionRunning:
				desired.State, desired.Terminal = SessionCancelRequested, false
			default:
				return errors.New("QUERY_STATE_CONFLICT")
			}
		case queryCommandClose:
			desired.Closed = true
			desired.ResultAvailable = false
			switch current.State {
			case SessionQueued:
				desired.State, desired.Terminal, desired.Complete = SessionCanceled, true, false
			case SessionRunning:
				desired.State, desired.Terminal = SessionCancelRequested, false
			case SessionCancelRequested:
			default:
				if !current.Terminal {
					return errors.New("QUERY_STATE_CONFLICT")
				}
			}
		default:
			return errors.New("QUERY_COMMAND_INVALID")
		}
		changeType := kind + "_REQUESTED"
		if desired.State == SessionCanceled {
			changeType = "CANCELED"
		} else if kind == queryCommandClose && current.Terminal {
			changeType = "CLOSED"
		}
		snapshot, err = applyDesiredQueryTx(ctx, tx, taskRepository, desired, true, changeType)
		if err != nil {
			return err
		}
		if err := insertQueryCommand(ctx, tx, scope, digest, id, snapshot, committedAt); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return snapshot, applied, err
}

func insertQueryCommand(
	ctx context.Context,
	tx store.Executor,
	scope, digest, id string,
	snapshot QuerySession,
	committedAt time.Time,
) error {
	return tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
		Scope: scope, RequestDigest: digest, ResourceKind: queryKind, ResourceID: id,
		ResultState: string(snapshot.State), ResultStateRevision: snapshot.StateRevision,
		CommittedAt: committedAt, ExpiresAt: committedAt.Add(queryResourceRetention),
	})
}

func validateQueryCommand(id, kind string, request CommandEnvelope) (string, string, string, error) {
	if !isUUID(id) {
		return "", "", "", requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	if !isUUID(request.CommandRequestID) {
		return "", "", "", requestError("INVALID_REQUEST", "commandRequestId 无效")
	}
	if !isCanonicalUint(request.ExpectedStateRevision) {
		return "", "", "", requestError("INVALID_EXACT_NUMBER", "expectedStateRevision 无效")
	}
	if kind != queryCommandCancel && kind != queryCommandClose {
		return "", "", "", requestError("INVALID_REQUEST", "查询命令无效")
	}
	scope := queryKind + "\x1f" + id + "\x1f" + kind + "\x1f" + request.CommandRequestID
	digest := digestValues("QueryCommandV1", id, kind, request.ExpectedStateRevision)
	return request.ExpectedStateRevision, scope, digest, nil
}

func queryCommandError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, tasks.ErrNotFound):
		return requestError("QUERY_NOT_FOUND", "查询任务不存在")
	case errors.Is(err, tasks.ErrRevisionConflict):
		return requestError("REVISION_CONFLICT", "查询状态已变化")
	case errors.Is(err, tasks.ErrCommandIdempotencyConflict):
		return requestError("COMMAND_IDEMPOTENCY_CONFLICT", "commandRequestId 已绑定到不同命令")
	case err.Error() == "QUERY_STATE_CONFLICT":
		return requestError("QUERY_STATE_CONFLICT", "当前查询状态不允许该命令")
	default:
		return err
	}
}

func (s *Service) applyInMemoryCommand(
	id, kind string,
	request CommandEnvelope,
	revision string,
) (QuerySession, error) {
	_ = request
	now := s.options.Now()
	var resultToClose *ResultSet
	s.mu.Lock()
	record := s.sessions[id]
	if record == nil {
		s.mu.Unlock()
		return QuerySession{}, requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	targetSatisfied := record.snapshot.Terminal || record.snapshot.State == SessionCancelRequested
	if kind == queryCommandClose {
		targetSatisfied = record.closed
	}
	if targetSatisfied {
		snapshot := record.snapshot
		s.mu.Unlock()
		return snapshot, nil
	}
	if record.snapshot.StateRevision != revision {
		s.mu.Unlock()
		return QuerySession{}, requestError("REVISION_CONFLICT", "查询状态已变化")
	}
	if kind == queryCommandClose {
		record.closed = true
		record.snapshot.Closed = true
		record.snapshot.ResultAvailable = false
		resultToClose = record.result
		record.result = nil
		record.rawIssue = ""
		if record.resultExpiry != nil {
			record.resultExpiry.Stop()
			record.resultExpiry = nil
		}
	}
	if record.snapshot.State == SessionQueued {
		if lane := s.lanes[record.laneKey]; lane != nil {
			lane.queue = removeString(lane.queue, id)
		}
		transition(&record.snapshot, SessionCanceled, true, now)
		record.snapshot.Complete = false
	} else if record.snapshot.State == SessionRunning {
		transition(&record.snapshot, SessionCancelRequested, false, now)
	} else if kind == queryCommandClose && record.snapshot.State == SessionCancelRequested {
		bumpSnapshot(&record.snapshot, now)
		record.snapshot.StateRevision = incrementDecimal(record.snapshot.StateRevision)
	} else if kind == queryCommandClose && record.snapshot.Terminal {
		bumpSnapshot(&record.snapshot, now)
		record.snapshot.StateRevision = incrementDecimal(record.snapshot.StateRevision)
	} else {
		s.mu.Unlock()
		return QuerySession{}, requestError("QUERY_STATE_CONFLICT", "当前查询状态不允许该命令")
	}
	cancel := record.cancel
	snapshot := record.snapshot
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if err := closeResultSet(resultToClose); err != nil {
		return QuerySession{}, requestError("RESULT_CLEANUP_FAILED", "查询结果文件无法删除")
	}
	return snapshot, nil
}
