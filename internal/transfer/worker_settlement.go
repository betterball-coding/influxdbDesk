package transfer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

var ErrImportStagingBoundary = errors.New("IMPORT_STAGING_BOUNDARY_CONFLICT")

type FinishImportRequest struct {
	JobID              string
	RunSegmentID       string
	CheckpointDigest   string
	StagingLogicalSize string
}

// MarkAttemptNotSent settles a persisted SENDING row only when the Dispatcher
// start barrier was not crossed. The active run remains usable, but this
// attempt can never later be mistaken for an unknown server outcome.
func (r *Repository) MarkAttemptNotSent(ctx context.Context, attemptID string) (ImportJob, error) {
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ImportJob{}, nil
		}
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var segment, state, parent, segmentState string
		var stopReason sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT a.run_segment_id,a.state,a.parent_checkpoint_digest,
			s.state,s.stop_reason FROM import_attempts a JOIN import_run_segments s
			ON s.run_segment_id=a.run_segment_id WHERE a.attempt_id=?`, attemptID).Scan(
			&segment, &state, &parent, &segmentState, &stopReason); err != nil {
			return err
		}
		if state != "SENDING" {
			return ErrImportState
		}
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != parent ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segment {
			return ErrCheckpointConflict
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='FAILED_NOT_SENT',updated_at=?
			WHERE attempt_id=? AND state='SENDING' AND parent_checkpoint_digest=?`, now, attemptID, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		newSegmentState := segmentState
		var committedStopReason any
		if segmentState == "STOPPING" {
			newSegmentState = "CLOSED"
			committedStopReason = stopReason.String
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state=?,active_attempt_id=NULL,
			stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=? AND state IN ('ACTIVE','STOPPING')
			AND committed_checkpoint_digest=? AND active_attempt_id=?`, newSegmentState,
			committedStopReason, now, jobID, segment, parent, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		if segmentState == "STOPPING" {
			targetState := ImportPausedSafe
			terminal := false
			changeType := "PAUSED"
			if stopReason.String == importStopUserCanceled {
				targetState = ImportCanceled
				terminal = true
				changeType = "CANCELED"
			}
			updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
				State: targetState, StateChanged: true, Terminal: terminal, ChangeType: changeType,
			})
			if err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
				snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
				WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
				targetState, updated.StateRevision, updated.SnapshotRevision, now,
				jobID, ImportRunning, parent, segment)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
				targetState, now, jobID); err != nil {
				return err
			}
			job.Task = updated
			job.ActiveRunSegmentID = nil
			job.PauseRequested = false
		}
		out = job
		return nil
	})
	if err == nil && out.Task.State != ImportRunning {
		r.permits.Revoke(jobID)
	}
	return out, err
}

// SettleRejected records a definitive, non-partial client rejection. It never
// advances the checkpoint and never retries the payload.
func (r *Repository) SettleRejected(ctx context.Context, attemptID string) (ImportJob, error) {
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var segment, state, parent, segmentState string
		var existingStopReason sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT a.run_segment_id,a.state,a.parent_checkpoint_digest,s.state,s.stop_reason
			FROM import_attempts a JOIN import_run_segments s ON s.run_segment_id=a.run_segment_id
			WHERE a.attempt_id=?`, attemptID).Scan(&segment, &state, &parent, &segmentState, &existingStopReason); err != nil {
			return err
		}
		if state != "SENDING" || (segmentState != "ACTIVE" && segmentState != "STOPPING") {
			return ErrImportState
		}
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != parent ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segment {
			return ErrCheckpointConflict
		}

		now := r.now().UTC()
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='REJECTED',updated_at=?
			WHERE attempt_id=? AND state='SENDING' AND parent_checkpoint_digest=?`,
			now.Format(time.RFC3339Nano), attemptID, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}

		targetState := ImportFailed
		terminal := true
		stopReason := "WRITE_REJECTED"
		code := "IMPORT_WRITE_REJECTED"
		var publicCode *string = &code
		if segmentState == "STOPPING" || job.PauseRequested {
			targetState = ImportPausedSafe
			terminal = false
			stopReason = "PAUSED_AFTER_WRITE_REJECTION"
			publicCode = nil
			if existingStopReason.String == importStopUserCanceled {
				targetState = ImportCanceled
				terminal = true
				stopReason = importStopUserCanceled
			}
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			active_attempt_id=NULL,stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=?
			AND state IN ('ACTIVE','STOPPING') AND active_attempt_id=?`, stopReason,
			now.Format(time.RFC3339Nano), jobID, segment, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: true, Terminal: terminal,
			ChangeType: "STATE", PublicErrorCode: publicCode,
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
			targetState, updated.StateRevision, updated.SnapshotRevision,
			now.Format(time.RFC3339Nano), jobID, ImportRunning, parent, segment)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			targetState, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = nil
		job.PauseRequested = false
		out = job
		return nil
	})
	if err == nil {
		r.permits.Revoke(jobID)
	}
	return out, err
}

// FinishImport closes the active segment only after the final ACK checkpoint
// reaches the verified staging length. A repeated finish is a read-only replay.
func (r *Repository) FinishImport(ctx context.Context, request FinishImportRequest) (ImportJob, bool, error) {
	logicalSize, err := canonicalDecimal(request.StagingLogicalSize)
	if err != nil || logicalSize == "0" || request.JobID == "" || request.RunSegmentID == "" ||
		request.CheckpointDigest == "" {
		if err != nil {
			return ImportJob{}, false, err
		}
		return ImportJob{}, false, ErrImportStagingBoundary
	}
	lock := r.jobLock(request.JobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	replayed := false
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, request.JobID)
		if err != nil {
			return err
		}
		if job.Task.State == ImportSucceeded {
			out = job
			replayed = true
			return nil
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != request.CheckpointDigest ||
			job.Checkpoint.LogicalOffset != logicalSize || job.ActiveRunSegmentID == nil ||
			*job.ActiveRunSegmentID != request.RunSegmentID || job.OpenIncident != nil {
			return ErrImportStagingBoundary
		}
		permit, found := r.permits.Get(request.JobID)
		if !found || permit.RunSegmentID != request.RunSegmentID ||
			permit.CurrentCheckpointDigest != request.CheckpointDigest {
			return ErrPermitMismatch
		}
		var segmentState, committedDigest string
		var activeAttempt sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state,committed_checkpoint_digest,active_attempt_id
			FROM import_run_segments WHERE job_id=? AND run_segment_id=?`, request.JobID,
			request.RunSegmentID).Scan(&segmentState, &committedDigest, &activeAttempt); err != nil {
			return err
		}
		if segmentState != "ACTIVE" || committedDigest != request.CheckpointDigest || activeAttempt.Valid {
			return ErrImportStagingBoundary
		}
		var sending, openIncidents int
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM import_attempts WHERE job_id=? AND state='SENDING'),
			(SELECT COUNT(*) FROM import_incidents WHERE job_id=? AND status='OPEN')`,
			request.JobID, request.JobID).Scan(&sending, &openIncidents); err != nil {
			return err
		}
		if sending != 0 || openIncidents != 0 {
			return ErrImportStagingBoundary
		}

		now := r.now().UTC()
		result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			active_attempt_id=NULL,stop_reason='STAGING_EOF',updated_at=?
			WHERE job_id=? AND run_segment_id=? AND state='ACTIVE'
			AND committed_checkpoint_digest=? AND active_attempt_id IS NULL`,
			now.Format(time.RFC3339Nano), request.JobID, request.RunSegmentID,
			request.CheckpointDigest)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportStagingBoundary); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportSucceeded, StateChanged: true, Terminal: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
			ImportSucceeded, updated.StateRevision, updated.SnapshotRevision,
			now.Format(time.RFC3339Nano), request.JobID, ImportRunning,
			request.CheckpointDigest, request.RunSegmentID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportStagingBoundary); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			ImportSucceeded, now.Format(time.RFC3339Nano), request.JobID); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = nil
		job.PauseRequested = false
		out = job
		return nil
	})
	if err == nil && !replayed {
		r.permits.Revoke(request.JobID)
	}
	return out, replayed, err
}
