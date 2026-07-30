package exportjob

import (
	"context"
	"time"

	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

// Recover pauses queued and active exports before the dispatcher is opened. WRITING
// fragments become CORRUPT so their .part files must be removed and the whole
// quantum retried. FINALIZING fragments remain pending explicit file
// reconciliation. COMPLETE fragments are not reusable in this process until
// ValidateFragmentForReuse succeeds under the new validation epoch.
func (r *Repository) Recover(ctx context.Context) (int, error) {
	rows, err := r.store.DB().QueryContext(ctx, `SELECT job_id FROM export_jobs
		WHERE state IN ('QUEUED','RUNNING','FINALIZING') ORDER BY job_id`)
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
		err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
			meta, err := tasks.GetInTx(ctx, tx, jobID)
			if err != nil {
				return err
			}
			var exportState string
			if err := tx.QueryRowContext(ctx, "SELECT state FROM export_jobs WHERE job_id=?", jobID).Scan(&exportState); err != nil {
				return err
			}
			if exportState != StateQueued && exportState != StateRunning && exportState != StateFinalizing {
				return nil
			}
			now := r.now().UTC().Format(time.RFC3339Nano)
			fragmentResult, err := tx.ExecContext(ctx, `UPDATE export_fragments
				SET state='CORRUPT',validation_epoch=NULL,updated_at=?
				WHERE job_id=? AND state='WRITING'`, now, jobID)
			if err != nil {
				return err
			}
			corruptCount, err := fragmentResult.RowsAffected()
			if err != nil {
				return err
			}

			// tasks.RecoverCore may have already paused the generic task. Mirror
			// that committed snapshot instead of producing a duplicate event.
			if meta.State == StatePausedRestartable && !meta.Terminal {
				if _, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state=?,state_revision=?,
					snapshot_revision=?,cancel_requested=0,updated_at=? WHERE job_id=?`,
					StatePausedRestartable, meta.StateRevision, meta.SnapshotRevision, now, jobID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?",
					StatePausedRestartable, now, jobID); err != nil {
					return err
				}
				if corruptCount != 0 {
					job, err := r.getInTx(ctx, tx, jobID, false)
					if err != nil {
						return err
					}
					_, err = r.touchSnapshotInTx(ctx, tx, job, "RECOVERED")
					return err
				}
				return nil
			}

			job, err := r.getInTx(ctx, tx, jobID, false)
			if err != nil {
				return err
			}
			code := "LOST_ON_RESTART"
			_, err = r.applyStateInTx(ctx, tx, job, StatePausedRestartable, false, false,
				"RECOVERED", nil, &code)
			return err
		})
		if err != nil {
			return changed, err
		}
		changed++
	}
	return changed, nil
}
