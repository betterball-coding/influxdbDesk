package transfer

import (
	"context"
	"database/sql"
	"math/big"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

// SettleTooLarge handles a definitive, non-partial HTTP 413 response. Normal
// runs commit a batch-limit child checkpoint; exact replay never changes its
// bytes or limits and returns to the original incident decision state.
func (r *Repository) SettleTooLarge(ctx context.Context, attemptID string) (ImportJob, error) {
	kind, err := r.attemptSegmentKind(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	if kind == "REPLAY_EXACT" {
		return r.settleReplayTooLarge(ctx, attemptID)
	}
	return r.settleNormalTooLarge(ctx, attemptID)
}

func (r *Repository) settleNormalTooLarge(ctx context.Context, attemptID string) (ImportJob, error) {
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	var parent, child, segment string
	var continueRun bool
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var batchID, state, endOffset string
		if err := tx.QueryRowContext(ctx, `SELECT batch_id,run_segment_id,state,
			parent_checkpoint_digest,end_offset FROM import_attempts WHERE attempt_id=?`,
			attemptID).Scan(&batchID, &segment, &state, &parent, &endOffset); err != nil {
			return err
		}
		if state != "SENDING" {
			return ErrImportState
		}
		var segmentState, segmentKind string
		var existingStopReason sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state,kind,stop_reason FROM import_run_segments
			WHERE job_id=? AND run_segment_id=?`, jobID, segment).Scan(
			&segmentState, &segmentKind, &existingStopReason); err != nil {
			return err
		}
		if segmentKind != "NORMAL" {
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
		if segmentState == "STOPPING" && existingStopReason.String == importStopUserCanceled {
			result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='REJECTED_TOO_LARGE',
				updated_at=? WHERE attempt_id=? AND state='SENDING' AND run_segment_id=?
				AND parent_checkpoint_digest=?`, now.Format(time.RFC3339Nano), attemptID, segment, parent)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
				active_attempt_id=NULL,stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=?
				AND state='STOPPING' AND active_attempt_id=?`, importStopUserCanceled,
				now.Format(time.RFC3339Nano), jobID, segment, attemptID)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
				return err
			}
			updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
				State: ImportCanceled, StateChanged: true, Terminal: true, ChangeType: "CANCELED",
			})
			if err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
				snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
				WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
				ImportCanceled, updated.StateRevision, updated.SnapshotRevision,
				now.Format(time.RFC3339Nano), jobID, ImportRunning, parent, segment)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
				ImportCanceled, now.Format(time.RFC3339Nano), jobID); err != nil {
				return err
			}
			job.Task = updated
			job.ActiveRunSegmentID = nil
			job.PauseRequested = false
			out = job
			return nil
		}
		if job.Checkpoint.AdaptiveMaxPoints == "1" {
			return r.failSinglePointTooLarge(ctx, tx, job, attemptID, segment, parent, now, &out)
		}
		next := job.Checkpoint
		next.Sequence, err = incrementDecimal(next.Sequence)
		if err != nil {
			return err
		}
		next.ParentDigest = parent
		next.LastSettledBatchID = batchID
		next.LastDisposition = "REJECTED_TOO_LARGE"
		next.AdaptiveMaxPoints, err = halvePositiveDecimal(next.AdaptiveMaxPoints)
		if err != nil {
			return err
		}
		next.AdaptiveMaxBytes, err = halvePositiveDecimal(next.AdaptiveMaxBytes)
		if err != nil {
			return err
		}
		child, err = CheckpointDigest(next)
		if err != nil {
			return err
		}
		if err := insertCheckpoint(ctx, tx, jobID, child, next, now); err != nil {
			return err
		}
		if err := insertTransition(ctx, tx, Transition{
			JobID: jobID, Sequence: next.Sequence, OwnerRunSegmentID: segment,
			Cause: "BATCH_LIMIT_REDUCED", ParentCheckpointDigest: parent,
			ChildCheckpointDigest: child, BatchID: batchID, AttemptID: attemptID,
			CommittedAt: now.Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='REJECTED_TOO_LARGE',
			new_checkpoint_digest=?,updated_at=? WHERE attempt_id=? AND state='SENDING'
			AND run_segment_id=? AND parent_checkpoint_digest=?`, child,
			now.Format(time.RFC3339Nano), attemptID, segment, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		continueRun = segmentState == "ACTIVE" && !job.PauseRequested
		targetState := ImportPausedSafe
		newSegmentState := "CLOSED"
		var activeSegment any
		var stopReason any = "PAUSED_AFTER_BATCH_LIMIT_REDUCTION"
		if continueRun {
			targetState = ImportRunning
			newSegmentState = "ACTIVE"
			activeSegment = segment
			stopReason = nil
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state=?,
			committed_checkpoint_digest=?,active_attempt_id=NULL,stop_reason=?,updated_at=?
			WHERE job_id=? AND run_segment_id=? AND state IN ('ACTIVE','STOPPING')
			AND committed_checkpoint_digest=? AND active_attempt_id=?`, newSegmentState, child,
			stopReason, now.Format(time.RFC3339Nano), jobID, segment, parent, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: !continueRun, ChangeType: "PROGRESS",
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,checkpoint_digest=?,active_run_segment_id=?,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
			targetState, updated.StateRevision, updated.SnapshotRevision, child, activeSegment,
			now.Format(time.RFC3339Nano), jobID, ImportRunning, parent, segment)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		job.Task = updated
		job.Checkpoint = next
		job.CheckpointDigest = child
		job.PauseRequested = false
		if continueRun {
			job.ActiveRunSegmentID = &segment
		} else {
			job.ActiveRunSegmentID = nil
		}
		out = job
		_ = endOffset
		return nil
	})
	if err != nil {
		return ImportJob{}, err
	}
	if out.Task.State == ImportFailed || !continueRun {
		r.permits.Revoke(jobID)
		return out, nil
	}
	if !r.permits.CAS(jobID, segment, parent, child) {
		if err := r.pausePermitDesync(ctx, jobID, segment, child); err != nil {
			return ImportJob{}, err
		}
		return r.GetImport(ctx, jobID)
	}
	return out, nil
}

func (r *Repository) failSinglePointTooLarge(
	ctx context.Context,
	tx store.Executor,
	job ImportJob,
	attemptID, segmentID, parent string,
	now time.Time,
	out *ImportJob,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='REJECTED_TOO_LARGE',
		updated_at=? WHERE attempt_id=? AND state='SENDING' AND parent_checkpoint_digest=?`,
		now.Format(time.RFC3339Nano), attemptID, parent)
	if err != nil {
		return err
	}
	if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
		active_attempt_id=NULL,stop_reason='SINGLE_POINT_TOO_LARGE',updated_at=?
		WHERE run_segment_id=? AND active_attempt_id=?`, now.Format(time.RFC3339Nano),
		segmentID, attemptID)
	if err != nil {
		return err
	}
	if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
		return err
	}
	code := "IMPORT_POINT_TOO_LARGE"
	updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
		State: ImportFailed, StateChanged: true, Terminal: true, ChangeType: "STATE",
		PublicErrorCode: &code,
	})
	if err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
		snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
		WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
		ImportFailed, updated.StateRevision, updated.SnapshotRevision,
		now.Format(time.RFC3339Nano), job.Task.ID, ImportRunning, parent, segmentID)
	if err != nil {
		return err
	}
	if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
		return err
	}
	job.Task = updated
	job.ActiveRunSegmentID = nil
	job.PauseRequested = false
	*out = job
	return nil
}

func (r *Repository) settleReplayTooLarge(ctx context.Context, attemptID string) (ImportJob, error) {
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var segment, state, parent, start, end, payload string
		if err := tx.QueryRowContext(ctx, `SELECT run_segment_id,state,parent_checkpoint_digest,
			start_offset,end_offset,payload_digest FROM import_attempts WHERE attempt_id=?`,
			attemptID).Scan(&segment, &state, &parent, &start, &end, &payload); err != nil {
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
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segment ||
			job.OpenIncident == nil || job.OpenIncident.ParentCheckpointDigest != parent ||
			job.OpenIncident.StartOffset != start || job.OpenIncident.EndOffset != end ||
			job.OpenIncident.PayloadDigest != payload {
			return ErrCheckpointConflict
		}
		targetState := ImportNeedsUnknownDecision
		if job.OpenIncident.Kind == "PARTIAL" {
			targetState = ImportNeedsPartialDecision
		}
		now := r.now().UTC()
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='REJECTED_TOO_LARGE',
			updated_at=? WHERE attempt_id=? AND state='SENDING' AND parent_checkpoint_digest=?`,
			now.Format(time.RFC3339Nano), attemptID, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			active_attempt_id=NULL,stop_reason='REPLAY_REJECTED_TOO_LARGE',updated_at=?
			WHERE run_segment_id=? AND kind='REPLAY_EXACT' AND active_attempt_id=?`,
			now.Format(time.RFC3339Nano), segment, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: true, ChangeType: "STATE",
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

func halvePositiveDecimal(value string) (string, error) {
	canonical, err := canonicalDecimal(value)
	if err != nil {
		return "", err
	}
	number := new(big.Int)
	number.SetString(canonical, 10)
	if number.Sign() <= 0 {
		return "", tasks.ErrInvalidDecimal
	}
	number.Div(number, big.NewInt(2))
	if number.Sign() == 0 {
		number.SetInt64(1)
	}
	return number.String(), nil
}
