package operation

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const cancelCommandRetention = 90 * 24 * time.Hour

func (s *Service) Cancel(ctx context.Context, operationID string, request CommandEnvelope) (Operation, error) {
	normalizedRevision, scope, digest, err := validateCancelCommand(operationID, request)
	if err != nil {
		return Operation{}, err
	}
	if replay, found, err := replayCancelCommand(ctx, s.store.DB(), operationID, scope, digest); err != nil || found {
		if err != nil {
			return replay, err
		}
		if control := s.operationControl(operationID); control != nil {
			if err := s.protection.WithGenerationGate(ctx, s.connectionID, s.generation, func(context.Context) error { return nil }); err != nil {
				return Operation{}, err
			}
			replay, err = s.Get(ctx, operationID)
			replay.Replayed = true
		}
		return replay, err
	}

	current, err := s.Get(ctx, operationID)
	if err != nil {
		return Operation{}, err
	}
	if current.ConnectionID != s.connectionID || current.ConnectionGeneration != s.generation {
		return Operation{}, ErrNotFound
	}
	control := s.operationControl(operationID)
	if control == nil {
		return cancelStoredValidated(ctx, s.store, s.tasks, s.options.Now, operationID,
			request, normalizedRevision, scope, digest)
	}

	var out Operation
	err = s.protection.WithGenerationGate(ctx, s.connectionID, s.generation, func(ctx context.Context) error {
		control.mu.Lock()
		started := control.started
		control.mu.Unlock()
		var replayed, applied bool
		var err error
		out, replayed, applied, err = applyCancel(ctx, s.store, s.tasks, s.options.Now,
			operationID, request, normalizedRevision, scope, digest, started, true)
		if err != nil {
			return err
		}
		out.Replayed = replayed
		if applied {
			control.cancel()
		}
		return nil
	})
	return out, err
}

func CancelStored(
	ctx context.Context,
	s *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	operationID string,
	request CommandEnvelope,
) (Operation, error) {
	normalizedRevision, scope, digest, err := validateCancelCommand(operationID, request)
	if err != nil {
		return Operation{}, err
	}
	return cancelStoredValidated(ctx, s, taskRepository, now, operationID, request,
		normalizedRevision, scope, digest)
}

func cancelStoredValidated(
	ctx context.Context,
	s *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	operationID string,
	request CommandEnvelope,
	normalizedRevision, scope, digest string,
) (Operation, error) {
	if replay, found, err := replayCancelCommand(ctx, s.DB(), operationID, scope, digest); err != nil || found {
		return replay, err
	}
	out, replayed, _, err := applyCancel(ctx, s, taskRepository, now, operationID, request,
		normalizedRevision, scope, digest, false, false)
	out.Replayed = replayed
	return out, err
}

func applyCancel(
	ctx context.Context,
	s *store.Store,
	taskRepository *tasks.Repository,
	now func() time.Time,
	operationID string,
	request CommandEnvelope,
	normalizedRevision, scope, digest string,
	started, allowActive bool,
) (out Operation, replayed, applied bool, err error) {
	err = s.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			if entry.ResourceKind != "OPERATION" || entry.ResourceID != operationID {
				return tasks.ErrCommandIdempotencyConflict
			}
			out, err = getInTx(ctx, tx, operationID)
			replayed = true
			return err
		}

		current, err := getInTx(ctx, tx, operationID)
		if err != nil {
			return err
		}
		committedAt := now().UTC()
		if current.Task.State == StateCanceledNotSent {
			if err := insertCancelCommand(ctx, tx, scope, digest, operationID, current.Task, committedAt); err != nil {
				return err
			}
			out = current
			return nil
		}
		if current.CancelRequested {
			if err := insertCancelCommand(ctx, tx, scope, digest, operationID, current.Task, committedAt); err != nil {
				return err
			}
			out = current
			return nil
		}
		if current.Task.StateRevision != normalizedRevision {
			return tasks.ErrRevisionConflict
		}
		if current.Task.Terminal || !allowActive {
			return ErrStateConflict
		}

		previousState := current.Task.State
		change := tasks.Change{StateChanged: true}
		changeType := "CANCEL_REQUESTED"
		auditOutcome := "REQUESTED"
		if started {
			if previousState != StateDispatching {
				return ErrStateConflict
			}
			change.State = StateDispatching
		} else {
			if previousState != StateQueued && previousState != StateDispatching {
				return ErrStateConflict
			}
			change.State = StateCanceledNotSent
			change.Terminal = true
			changeType = "CANCELED_NOT_SENT"
			auditOutcome = "CANCELED_NOT_SENT"
		}
		change.ChangeType = changeType
		updated, err := taskRepository.ApplyInTx(ctx, tx, current.Task, change)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,cancel_requested=1,updated_at=?
			WHERE operation_id=? AND state=?`, change.State, committedAt.Format(time.RFC3339Nano),
			operationID, previousState)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return ErrStateConflict
		}
		if err := tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "OPERATION", ResourceID: operationID,
			ResultState: updated.State, ResultStateRevision: updated.StateRevision,
			CommittedAt: committedAt, ExpiresAt: committedAt.Add(cancelCommandRetention),
		}); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) SELECT 'MUTATION','CANCEL',operation_id,action_digest,?,?,?
			FROM operations WHERE operation_id=?`, auditOutcome, request.CommandRequestID,
			committedAt.Format(time.RFC3339Nano), operationID); err != nil {
			return err
		}
		out, err = getInTx(ctx, tx, operationID)
		applied = err == nil
		return err
	})
	return out, replayed, applied, err
}

func insertCancelCommand(
	ctx context.Context,
	tx store.Executor,
	scope, digest, operationID string,
	meta tasks.Meta,
	committedAt time.Time,
) error {
	return tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
		Scope: scope, RequestDigest: digest, ResourceKind: "OPERATION", ResourceID: operationID,
		ResultState: meta.State, ResultStateRevision: meta.StateRevision,
		CommittedAt: committedAt, ExpiresAt: committedAt.Add(cancelCommandRetention),
	})
}

func replayCancelCommand(
	ctx context.Context,
	tx store.Executor,
	operationID, scope, digest string,
) (Operation, bool, error) {
	entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
	if err != nil || !found {
		return Operation{}, found, err
	}
	if entry.ResourceKind != "OPERATION" || entry.ResourceID != operationID {
		return Operation{}, true, tasks.ErrCommandIdempotencyConflict
	}
	out, err := getInTx(ctx, tx, operationID)
	out.Replayed = true
	return out, true, err
}

func validateCancelCommand(operationID string, request CommandEnvelope) (string, string, string, error) {
	if _, err := uuid.Parse(operationID); err != nil {
		return "", "", "", ErrNotFound
	}
	if _, err := uuid.Parse(request.CommandRequestID); err != nil {
		return "", "", "", errors.New("invalid commandRequestId")
	}
	revision, err := canonicalDecimal(request.ExpectedStateRevision)
	if err != nil {
		return "", "", "", err
	}
	scope := "OPERATION\x1f" + operationID + "\x1fCANCEL\x1f" + request.CommandRequestID
	digest := digestValues("CancelOperationV1", operationID, revision, StateCanceledNotSent)
	return revision, scope, digest, nil
}

func (s *Service) operationControl(operationID string) *operationControl {
	s.controlsMu.Lock()
	defer s.controlsMu.Unlock()
	return s.controls[operationID]
}
