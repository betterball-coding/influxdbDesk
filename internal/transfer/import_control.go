package transfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

const (
	importCommandRetention = 90 * 24 * time.Hour
	importStopUserCanceled = "USER_CANCELED"
)

// ImportCommandEnvelope is shared by Import state-control commands. Revisions
// remain exact decimal strings across SQLite and Wails boundaries.
type ImportCommandEnvelope struct {
	CommandRequestID      string `json:"commandRequestId"`
	ExpectedStateRevision string `json:"expectedStateRevision"`
}

// ImportCleanupFileFunc removes all application-managed files for one Import
// job and returns only after absence has been verified. It must be idempotent.
type ImportCleanupFileFunc func(context.Context, string) error

// CancelImport durably serializes a user cancellation with Import dispatch and
// settlement. Exact command replay is checked before live state or revision.
func (r *Repository) CancelImport(
	ctx context.Context,
	jobID string,
	command ImportCommandEnvelope,
) (ImportJob, bool, error) {
	revision, scope, digest, err := validateImportControlCommand(jobID, "CANCEL", command)
	if err != nil {
		return ImportJob{}, false, err
	}
	if out, found, err := r.replayImportControlCommand(ctx, jobID, scope, digest); err != nil || found {
		return out, found, err
	}

	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	replayed := false
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			if entry.ResourceKind != "IMPORT" || entry.ResourceID != jobID {
				return tasks.ErrCommandIdempotencyConflict
			}
			out, err = r.getImportInTx(ctx, tx, jobID)
			replayed = true
			return err
		}

		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		segment, segmentFound, err := readLiveImportSegment(ctx, tx, jobID)
		if err != nil {
			return err
		}
		var sending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND state='SENDING'`, jobID).Scan(&sending); err != nil {
			return err
		}
		if sending > 1 || (sending == 1 && (!segmentFound || !segment.activeAttemptID.Valid)) {
			return ErrImportState
		}

		// Cancel/Close commands are target-satisfied commands. A second command
		// may acknowledge an already requested cancellation despite a stale
		// expected revision, but it still receives its own durable ledger row.
		targetSatisfied := job.Task.State == ImportCanceled ||
			(segmentFound && segment.state == "STOPPING" && segment.stopReason.String == importStopUserCanceled)
		if targetSatisfied {
			if err := insertImportControlCommand(ctx, tx, scope, digest, jobID, job.Task, now); err != nil {
				return err
			}
			out = job
			return nil
		}
		if job.Task.StateRevision != revision {
			return tasks.ErrRevisionConflict
		}
		if job.OpenIncident != nil || job.Task.State == ImportNeedsPartialDecision ||
			job.Task.State == ImportNeedsUnknownDecision {
			return ErrOpenIncident
		}
		if job.Task.Terminal || job.Task.State == ImportCleaning {
			return ErrImportState
		}

		if sending == 0 {
			if segmentFound {
				result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
					active_attempt_id=NULL,stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=?
					AND state IN ('ACTIVE','STOPPING') AND active_attempt_id IS NULL`,
					importStopUserCanceled, now.Format(time.RFC3339Nano), jobID, segment.id)
				if err != nil {
					return err
				}
				if err := requireOneRow(result, ErrImportState); err != nil {
					return err
				}
			}
			updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
				State: ImportCanceled, StateChanged: true, Terminal: true, ChangeType: "CANCELED",
			})
			if err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
				snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
				WHERE job_id=? AND state=?`, ImportCanceled, updated.StateRevision,
				updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID, job.Task.State)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrImportState); err != nil {
				return err
			}
			job.Task = updated
			job.ActiveRunSegmentID = nil
			job.PauseRequested = false
		} else {
			if job.Task.State != ImportRunning || !segmentFound || segment.state != "ACTIVE" ||
				job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segment.id {
				return ErrImportState
			}
			result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='STOPPING',
				stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=? AND state='ACTIVE'
				AND active_attempt_id IS NOT NULL`, importStopUserCanceled,
				now.Format(time.RFC3339Nano), jobID, segment.id)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrImportState); err != nil {
				return err
			}
			updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
				State: ImportRunning, StateChanged: true, ChangeType: "CANCEL_REQUESTED",
			})
			if err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state_revision=?,
				snapshot_revision=?,pause_requested=1,updated_at=? WHERE job_id=? AND state=?
				AND active_run_segment_id=?`, updated.StateRevision, updated.SnapshotRevision,
				now.Format(time.RFC3339Nano), jobID, ImportRunning, segment.id)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrImportState); err != nil {
				return err
			}
			job.Task = updated
			job.PauseRequested = true
		}

		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			job.Task.State, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		if err := insertImportControlCommand(ctx, tx, scope, digest, jobID, job.Task, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) VALUES('IMPORT','CANCEL',?,?,'REQUESTED',?,?)`, jobID, job.CheckpointDigest,
			command.CommandRequestID, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		out = job
		return nil
	})
	if err == nil {
		r.permits.Revoke(jobID)
	}
	return out, replayed, err
}

// CleanupImport first commits CLEANING, then removes protected files, and only
// after verified deletion atomically releases all reservations and retention.
// A crash at either boundary is resumed by exact replay or startup recovery.
func (r *Repository) CleanupImport(
	ctx context.Context,
	jobID string,
	command ImportCommandEnvelope,
	removeFiles ImportCleanupFileFunc,
) (ImportJob, bool, error) {
	if removeFiles == nil {
		return ImportJob{}, false, ErrStageFileCleanup
	}
	revision, scope, digest, err := validateImportControlCommand(jobID, "CLEANUP", command)
	if err != nil {
		return ImportJob{}, false, err
	}

	if out, entry, found, err := r.lookupImportControlCommand(ctx, jobID, scope, digest); err != nil {
		return ImportJob{}, false, err
	} else if found && entry.ResultState != ImportCleaning {
		return out, true, nil
	}

	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()

	replayed := false
	finished := false
	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			if entry.ResourceKind != "IMPORT" || entry.ResourceID != jobID {
				return tasks.ErrCommandIdempotencyConflict
			}
			replayed = true
			out, err = r.getImportInTx(ctx, tx, jobID)
			if err != nil {
				return err
			}
			if entry.ResultState != ImportCleaning {
				finished = true
				return nil
			}
			if out.Task.State != ImportCleaning {
				return ErrImportState
			}
			return nil
		}

		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.StateRevision != revision {
			return tasks.ErrRevisionConflict
		}
		if err := validateImportCleanupInTx(ctx, tx, job); err != nil {
			return err
		}
		now := r.now().UTC()
		cleanupFinalState := importCleanupFinalState(job.Task.State)
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportCleaning, StateChanged: true, ChangeType: "CLEANING",
		})
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=?`, ImportCleaning, updated.StateRevision,
			updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID, job.Task.State)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		// transfer_jobs.state intentionally stores the post-cleanup recovery
		// target while the public task is CLEANING. Paused jobs become CANCELED
		// because their staging is about to be irreversibly removed.
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			cleanupFinalState, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		if err := tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "IMPORT", ResourceID: jobID,
			ResultState: ImportCleaning, ResultStateRevision: updated.StateRevision,
			CommittedAt: now, ExpiresAt: now.Add(importCommandRetention),
		}); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) VALUES('IMPORT','CLEANUP',?,?,'STARTED',?,?)`, jobID, job.CheckpointDigest,
			command.CommandRequestID, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = nil
		job.PauseRequested = false
		out = job
		return nil
	})
	if err != nil || finished {
		return out, replayed, err
	}
	r.permits.Revoke(jobID)
	if err := removeFiles(ctx, jobID); err != nil {
		return out, replayed, ErrStageFileCleanup
	}
	out, err = r.completeImportCleanupLocked(ctx, jobID, scope, digest, command.CommandRequestID)
	return out, replayed, err
}

// RecoverImportCleanups completes CLEANING jobs before network entry points are
// opened. File deletion is idempotently re-verified before quota is released.
func (r *Repository) RecoverImportCleanups(ctx context.Context, removeFiles ImportCleanupFileFunc) (int, error) {
	if removeFiles == nil {
		return 0, ErrStageFileCleanup
	}
	rows, err := r.store.DB().QueryContext(ctx, `SELECT c.resource_id,c.scope,c.request_digest
		FROM command_ledger c JOIN import_jobs i ON i.job_id=c.resource_id
		WHERE c.resource_kind='IMPORT' AND c.result_state=? AND i.state=?`,
		ImportCleaning, ImportCleaning)
	if err != nil {
		return 0, err
	}
	type pending struct{ jobID, scope, digest string }
	var jobs []pending
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.jobID, &item.scope, &item.digest); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, item)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	completed := 0
	for _, item := range jobs {
		lock := r.jobLock(item.jobID)
		lock.Lock()
		r.permits.Revoke(item.jobID)
		if err := removeFiles(ctx, item.jobID); err != nil {
			lock.Unlock()
			return completed, ErrStageFileCleanup
		}
		_, err := r.completeImportCleanupLocked(ctx, item.jobID, item.scope, item.digest, "RECOVERY")
		lock.Unlock()
		if err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

func (r *Repository) completeImportCleanupLocked(
	ctx context.Context,
	jobID, scope, digest, correlationID string,
) (ImportJob, error) {
	var out ImportJob
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if !found || entry.ResourceKind != "IMPORT" || entry.ResourceID != jobID {
			return tasks.ErrCommandIdempotencyConflict
		}
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if entry.ResultState != ImportCleaning {
			out = job
			return nil
		}
		if job.Task.State != ImportCleaning {
			return ErrImportState
		}
		if err := validateNoLiveImportWorkInTx(ctx, tx, jobID); err != nil {
			return err
		}
		var originalState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM transfer_jobs WHERE job_id=?`, jobID).Scan(&originalState); err != nil {
			return err
		}
		if !isImportCleanupState(originalState) {
			return ErrImportState
		}
		now := r.now().UTC()
		if _, err := tx.ExecContext(ctx, `DELETE FROM transfer_reservations WHERE job_id=?`, jobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET retained=0,state=?,updated_at=?
			WHERE job_id=?`, originalState, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_preflight_details SET staging_file_name=NULL,
			updated_at=? WHERE job_id=?`, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: originalState, StateChanged: true, Terminal: isTerminalImportState(originalState),
			ChangeType: "CLEANED",
		})
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=?`, originalState, updated.StateRevision,
			updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID, ImportCleaning)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE command_ledger SET result_state=?,
			result_state_revision=? WHERE scope=? AND request_digest=? AND resource_kind='IMPORT'
			AND resource_id=? AND result_state=?`, originalState, updated.StateRevision,
			scope, digest, jobID, ImportCleaning)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, tasks.ErrCommandIdempotencyConflict); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) VALUES('IMPORT','CLEANUP',?,?,'SUCCEEDED',?,?)`, jobID, job.CheckpointDigest,
			correlationID, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		job.Task = updated
		out = job
		return nil
	})
	return out, err
}

func validateImportCleanupInTx(ctx context.Context, tx store.Executor, job ImportJob) error {
	if !isImportCleanupState(job.Task.State) {
		return ErrImportState
	}
	return validateNoLiveImportWorkInTx(ctx, tx, job.Task.ID)
}

func validateNoLiveImportWorkInTx(ctx context.Context, tx store.Executor, jobID string) error {
	var liveSegments, sending, openIncidents int
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM import_run_segments WHERE job_id=? AND state IN ('ACTIVE','STOPPING')),
		(SELECT COUNT(*) FROM import_attempts WHERE job_id=? AND state='SENDING'),
		(SELECT COUNT(*) FROM import_incidents WHERE job_id=? AND status='OPEN')`,
		jobID, jobID, jobID).Scan(&liveSegments, &sending, &openIncidents)
	if err != nil {
		return err
	}
	if liveSegments != 0 || sending != 0 || openIncidents != 0 {
		return ErrImportState
	}
	return nil
}

func isImportCleanupState(state string) bool {
	switch state {
	case ImportAborted, ImportCanceled, ImportFailed, ImportSucceeded,
		ImportPausedSafe, ImportPausedRestage:
		return true
	default:
		return false
	}
}

func importCleanupFinalState(state string) string {
	switch state {
	case ImportPausedSafe, ImportPausedRestage:
		return ImportCanceled
	default:
		return state
	}
}

func isTerminalImportState(state string) bool {
	switch state {
	case ImportAborted, ImportCanceled, ImportFailed, ImportSucceeded:
		return true
	default:
		return false
	}
}

type liveImportSegment struct {
	id              string
	state           string
	activeAttemptID sql.NullString
	stopReason      sql.NullString
}

func readLiveImportSegment(ctx context.Context, tx store.Executor, jobID string) (liveImportSegment, bool, error) {
	var segment liveImportSegment
	err := tx.QueryRowContext(ctx, `SELECT run_segment_id,state,active_attempt_id,stop_reason
		FROM import_run_segments WHERE job_id=? AND state IN ('ACTIVE','STOPPING')`, jobID).Scan(
		&segment.id, &segment.state, &segment.activeAttemptID, &segment.stopReason)
	if errors.Is(err, sql.ErrNoRows) {
		return liveImportSegment{}, false, nil
	}
	if err != nil {
		return liveImportSegment{}, false, err
	}
	return segment, true, nil
}

func validateImportControlCommand(
	jobID, kind string,
	command ImportCommandEnvelope,
) (string, string, string, error) {
	if jobID == "" {
		return "", "", "", tasks.ErrNotFound
	}
	if _, err := uuid.Parse(command.CommandRequestID); err != nil {
		return "", "", "", errors.New("invalid commandRequestId")
	}
	revision, err := canonicalDecimal(command.ExpectedStateRevision)
	if err != nil {
		return "", "", "", err
	}
	scope := "IMPORT\x1f" + jobID + "\x1f" + kind + "\x1f" + command.CommandRequestID
	canonical := "ImportControlCommandV1\n" + kind + "\n" + jobID + "\n" +
		command.CommandRequestID + "\n" + revision
	sum := sha256.Sum256([]byte(canonical))
	return revision, scope, hex.EncodeToString(sum[:]), nil
}

func (r *Repository) replayImportControlCommand(
	ctx context.Context,
	jobID, scope, digest string,
) (ImportJob, bool, error) {
	out, _, found, err := r.lookupImportControlCommand(ctx, jobID, scope, digest)
	return out, found, err
}

func (r *Repository) lookupImportControlCommand(
	ctx context.Context,
	jobID, scope, digest string,
) (ImportJob, tasks.CommandEntry, bool, error) {
	entry, found, err := tasks.LookupCommandInTx(ctx, r.store.DB(), scope, digest)
	if err != nil || !found {
		return ImportJob{}, entry, found, err
	}
	if entry.ResourceKind != "IMPORT" || entry.ResourceID != jobID {
		return ImportJob{}, entry, true, tasks.ErrCommandIdempotencyConflict
	}
	out, err := r.GetImport(ctx, jobID)
	return out, entry, true, err
}

func insertImportControlCommand(
	ctx context.Context,
	tx store.Executor,
	scope, digest, jobID string,
	meta tasks.Meta,
	committedAt time.Time,
) error {
	return tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
		Scope: scope, RequestDigest: digest, ResourceKind: "IMPORT", ResourceID: jobID,
		ResultState: meta.State, ResultStateRevision: meta.StateRevision,
		CommittedAt: committedAt, ExpiresAt: committedAt.Add(importCommandRetention),
	})
}
