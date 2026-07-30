package transfer

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

// RecoverImports clears all process-local permits and reconciles persisted
// import attempts before any dispatcher or Wails entry point is opened.
func (r *Repository) RecoverImports(ctx context.Context) (int, error) {
	r.permits.Reset()
	rows, err := r.store.DB().QueryContext(ctx, `SELECT job_id FROM import_jobs
		WHERE state IN ('PREFLIGHTING','STAGING','RUNNING')`)
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
	for _, jobID := range ids {
		lock := r.jobLock(jobID)
		lock.Lock()
		err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
			job, err := r.getImportInTx(ctx, tx, jobID)
			if err != nil {
				return err
			}
			now := r.now().UTC()
			if job.Task.State == "PREFLIGHTING" || job.Task.State == "STAGING" {
				return r.recoverImportState(ctx, tx, job, ImportPausedRestage, "RESTAGE_REQUIRED", now)
			}

			attempt, found, err := readSendingAttempt(ctx, tx, jobID)
			if err != nil {
				return err
			}
			if !found {
				if job.ActiveRunSegmentID != nil {
					if _, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
						active_attempt_id=NULL,stop_reason='LOST_ON_RESTART',updated_at=?
						WHERE run_segment_id=?`, now.Format(time.RFC3339Nano), *job.ActiveRunSegmentID); err != nil {
						return err
					}
				}
				return r.recoverImportState(ctx, tx, job, ImportPausedSafe, "LOST_ON_RESTART", now)
			}

			incidentID := uuid.NewString()
			if _, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='UNKNOWN',updated_at=?
				WHERE attempt_id=? AND state='SENDING'`, now.Format(time.RFC3339Nano), attempt.attemptID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
				active_attempt_id=NULL,stop_reason='LOST_ON_RESTART',updated_at=?
				WHERE run_segment_id=?`, now.Format(time.RFC3339Nano), attempt.segmentID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO import_incidents(
				incident_id,job_id,kind,status,parent_checkpoint_digest,run_segment_id,
				batch_id,attempt_id,start_offset,end_offset,payload_digest,created_at
			) VALUES(?,?,'UNKNOWN','OPEN',?,?,?,?,?,?,?,?)`, incidentID, jobID,
				attempt.parentDigest, attempt.segmentID, attempt.batchID, attempt.attemptID,
				attempt.startOffset, attempt.endOffset, attempt.payloadDigest,
				now.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			return r.recoverImportState(ctx, tx, job, ImportNeedsUnknownDecision, "UNKNOWN_ON_RESTART", now)
		})
		lock.Unlock()
		if err != nil {
			return changed, err
		}
		changed++
	}
	return changed, nil
}

func (r *Repository) recoverImportState(ctx context.Context, tx store.Executor, job ImportJob, state, reason string, now time.Time) error {
	code := reason
	updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
		State: state, StateChanged: true, ChangeType: "RECOVERED", PublicErrorCode: &code,
	})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
		snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
		WHERE job_id=?`, state, updated.StateRevision, updated.SnapshotRevision,
		now.Format(time.RFC3339Nano), job.Task.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
		state, now.Format(time.RFC3339Nano), job.Task.ID)
	return err
}

type sendingAttempt struct {
	attemptID, batchID, segmentID, parentDigest string
	startOffset, endOffset, payloadDigest       string
}

func readSendingAttempt(ctx context.Context, tx store.Executor, jobID string) (sendingAttempt, bool, error) {
	var attempt sendingAttempt
	err := tx.QueryRowContext(ctx, `SELECT attempt_id,batch_id,run_segment_id,
		parent_checkpoint_digest,start_offset,end_offset,payload_digest
		FROM import_attempts WHERE job_id=? AND state='SENDING'`, jobID).Scan(
		&attempt.attemptID, &attempt.batchID, &attempt.segmentID, &attempt.parentDigest,
		&attempt.startOffset, &attempt.endOffset, &attempt.payloadDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return sendingAttempt{}, false, nil
	}
	if err != nil {
		return sendingAttempt{}, false, err
	}
	return attempt, true, nil
}
