package transfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

type Repository struct {
	store   *store.Store
	tasks   *tasks.Repository
	quota   *QuotaManager
	permits *PermitRegistry
	now     func() time.Time

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func NewRepository(s *store.Store, taskRepo *tasks.Repository, permits *PermitRegistry, now func() time.Time) *Repository {
	if now == nil {
		now = time.Now
	}
	if permits == nil {
		permits = NewPermitRegistry()
	}
	return &Repository{
		store: s, tasks: taskRepo, quota: NewQuotaManager(s, now), permits: permits,
		now: now, locks: make(map[string]*sync.Mutex),
	}
}

func (r *Repository) jobLock(jobID string) *sync.Mutex {
	r.locksMu.Lock()
	defer r.locksMu.Unlock()
	lock := r.locks[jobID]
	if lock == nil {
		lock = &sync.Mutex{}
		r.locks[jobID] = lock
	}
	return lock
}

func (r *Repository) CreateImport(ctx context.Context, req CreateImportRequest) (ImportJob, bool, error) {
	checkpointDigest, err := CheckpointDigest(req.InitialCheckpoint)
	if err != nil {
		return ImportJob{}, false, err
	}
	var job ImportJob
	replayed := false
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupIdempotencyInTx(ctx, tx, req.ClientScope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != req.RequestDigest {
				return tasks.ErrIdempotencyConflict
			}
			job, err = r.getImportInTx(ctx, tx, entry.ResourceID)
			replayed = true
			return err
		}
		if err := r.quota.AdmitInTx(ctx, tx, req.JobID, req.ProfileID, "IMPORT", ImportReady); err != nil {
			return err
		}
		meta, err := r.tasks.CreateInTx(ctx, tx, req.JobID, "IMPORT", ImportReady, false)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_jobs(
			job_id,state,state_revision,snapshot_revision,checkpoint_digest,updated_at
		) VALUES(?,?,?,?,?,?)`, req.JobID, ImportReady, meta.StateRevision,
			meta.SnapshotRevision, checkpointDigest, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err := insertCheckpoint(ctx, tx, req.JobID, checkpointDigest, req.InitialCheckpoint, now); err != nil {
			return err
		}
		if err := tasks.InsertIdempotencyInTx(ctx, tx, tasks.IdempotencyEntry{
			Scope: req.ClientScope, RequestDigest: req.RequestDigest, ResourceKind: "IMPORT",
			ResourceID: req.JobID, CreatedAt: now, ExpiresAt: req.LedgerExpiresAt.UTC(),
		}); err != nil {
			return err
		}
		job = ImportJob{Task: meta, ProfileID: req.ProfileID, CheckpointDigest: checkpointDigest,
			Checkpoint: req.InitialCheckpoint}
		return nil
	})
	return job, replayed, err
}

func (r *Repository) GetImport(ctx context.Context, jobID string) (ImportJob, error) {
	return r.getImportInTx(ctx, r.store.DB(), jobID)
}

func (r *Repository) ReplayCommand(ctx context.Context, scope, digest, jobID string) (ImportJob, bool, error) {
	entry, found, err := tasks.LookupCommandInTx(ctx, r.store.DB(), scope, digest)
	if err != nil || !found {
		return ImportJob{}, found, err
	}
	if entry.ResourceID != jobID {
		return ImportJob{}, true, tasks.ErrCommandIdempotencyConflict
	}
	job, err := r.GetImport(ctx, jobID)
	return job, true, err
}

func (r *Repository) getImportInTx(ctx context.Context, tx store.Executor, jobID string) (ImportJob, error) {
	meta, err := tasks.GetInTx(ctx, tx, jobID)
	if err != nil {
		return ImportJob{}, err
	}
	var job ImportJob
	var active sql.NullString
	var pause int
	if err := tx.QueryRowContext(ctx, `SELECT t.profile_id,i.checkpoint_digest,
		i.active_run_segment_id,i.pause_requested
		FROM import_jobs i JOIN transfer_jobs t ON t.job_id=i.job_id WHERE i.job_id=?`, jobID).Scan(
		&job.ProfileID, &job.CheckpointDigest, &active, &pause); err != nil {
		return ImportJob{}, err
	}
	job.Task = meta
	job.PauseRequested = pause != 0
	if active.Valid {
		job.ActiveRunSegmentID = &active.String
	}
	if job.CheckpointDigest != "" {
		job.Checkpoint, err = readCheckpoint(ctx, tx, jobID, job.CheckpointDigest)
		if err != nil {
			return ImportJob{}, err
		}
	}
	incident, found, err := readOpenIncident(ctx, tx, jobID)
	if err != nil {
		return ImportJob{}, err
	}
	if found {
		job.OpenIncident = &incident
	}
	return job, nil
}

func (r *Repository) StartRun(ctx context.Context, req StartRunRequest) (ImportJob, bool, error) {
	lock := r.jobLock(req.JobID)
	lock.Lock()
	defer lock.Unlock()
	if req.RunSegmentID == "" {
		req.RunSegmentID = uuid.NewString()
	}
	var out ImportJob
	replayed := false
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		_, found, err := tasks.LookupCommandInTx(ctx, tx, req.CommandScope, req.CommandDigest)
		if err != nil {
			return err
		}
		if found {
			out, err = r.getImportInTx(ctx, tx, req.JobID)
			if err == nil && out.ProfileID != req.ExpectedProfileID {
				return ErrImportProfileMismatch
			}
			replayed = true
			return err
		}
		job, err := r.getImportInTx(ctx, tx, req.JobID)
		if err != nil {
			return err
		}
		if job.ProfileID != req.ExpectedProfileID {
			return ErrImportProfileMismatch
		}
		now := r.now().UTC()
		if req.GrantExpiresAt.IsZero() || !now.Before(req.GrantExpiresAt.UTC()) {
			return ErrGrantExpired
		}
		if job.Task.StateRevision != req.ExpectedStateRevision {
			return tasks.ErrRevisionConflict
		}
		if job.Task.State != ImportReady && job.Task.State != ImportPausedSafe {
			return ErrImportState
		}
		if req.Permit.RunSegmentID != req.RunSegmentID || req.Permit.CurrentCheckpointDigest != job.CheckpointDigest {
			return ErrPermitMismatch
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_run_segments(
			run_segment_id,job_id,kind,state,initial_checkpoint_digest,
			committed_checkpoint_digest,created_at,updated_at
		) VALUES(?,?,?,'ACTIVE',?,?,?,?)`, req.RunSegmentID, req.JobID, req.Kind,
			job.CheckpointDigest, job.CheckpointDigest, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportRunning, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=?,pause_requested=0,updated_at=? WHERE job_id=?`,
			ImportRunning, updated.StateRevision, updated.SnapshotRevision, req.RunSegmentID,
			now.Format(time.RFC3339Nano), req.JobID); err != nil {
			return err
		}
		if err := tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: req.CommandScope, RequestDigest: req.CommandDigest, ResourceKind: "IMPORT",
			ResourceID: req.JobID, ResultState: ImportRunning, ResultStateRevision: updated.StateRevision,
			CommittedAt: now, ExpiresAt: req.CommandExpiresAt.UTC(),
		}); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = &req.RunSegmentID
		out = job
		return nil
	})
	if err != nil || replayed {
		return out, replayed, err
	}
	if err := r.permits.Activate(req.JobID, req.Permit); err != nil {
		_ = r.pausePermitDesync(ctx, req.JobID, req.RunSegmentID, req.Permit.CurrentCheckpointDigest)
		return ImportJob{}, false, err
	}
	return out, false, nil
}

// beginAttemptLocked is only the durable callback used by
// RunAuthorizer.BeginAttempt while dispatchGate and the job lock are held.
func (r *Repository) beginAttemptLocked(
	ctx context.Context,
	req AttemptRequest,
	expectedProfileID string,
	expectedPermit Permit,
) error {
	if req.RunSegmentID != expectedPermit.RunSegmentID ||
		req.ParentCheckpointDigest != expectedPermit.CurrentCheckpointDigest {
		return ErrPermitMismatch
	}
	if err := r.permits.Validate(req.JobID, expectedPermit); err != nil {
		return err
	}
	startOffset, err := canonicalDecimal(req.StartOffset)
	if err != nil {
		return err
	}
	endOffset, err := canonicalDecimal(req.EndOffset)
	if err != nil {
		return err
	}
	if comparison, err := compareCanonicalDecimals(endOffset, startOffset); err != nil || comparison <= 0 {
		if err != nil {
			return err
		}
		return ErrImportOffsetConflict
	}
	return r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, req.JobID)
		if err != nil {
			return err
		}
		if job.ProfileID != expectedProfileID || expectedPermit.ProfileID != expectedProfileID {
			return ErrImportProfileMismatch
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != req.ParentCheckpointDigest ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != req.RunSegmentID {
			return ErrCheckpointConflict
		}
		checkpointOffset, err := canonicalDecimal(job.Checkpoint.LogicalOffset)
		if err != nil {
			return err
		}
		if startOffset != checkpointOffset {
			return ErrImportOffsetConflict
		}
		var segmentState, segmentKind, committedDigest string
		var activeAttempt sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state,kind,committed_checkpoint_digest,active_attempt_id
			FROM import_run_segments WHERE job_id=? AND run_segment_id=?`, req.JobID,
			req.RunSegmentID).Scan(&segmentState, &segmentKind, &committedDigest, &activeAttempt); err != nil {
			return err
		}
		if segmentState != "ACTIVE" || committedDigest != req.ParentCheckpointDigest {
			return ErrCheckpointConflict
		}
		if segmentKind == "REPLAY_EXACT" {
			var incidentParent, incidentStart, incidentEnd, incidentPayload string
			if err := tx.QueryRowContext(ctx, `SELECT parent_checkpoint_digest,start_offset,
				end_offset,payload_digest FROM import_incidents
				WHERE job_id=? AND status='OPEN'`, req.JobID).Scan(&incidentParent,
				&incidentStart, &incidentEnd, &incidentPayload); err != nil {
				return err
			}
			if incidentParent != req.ParentCheckpointDigest || incidentStart != req.StartOffset ||
				incidentEnd != req.EndOffset || incidentPayload != req.PayloadDigest {
				return ErrCheckpointConflict
			}
		}
		if activeAttempt.Valid {
			return ErrImportState
		}
		var live int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND state='SENDING'`, req.JobID).Scan(&live); err != nil {
			return err
		}
		if live != 0 {
			return ErrImportState
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_attempts(
			attempt_id,job_id,batch_id,run_segment_id,state,parent_checkpoint_digest,
			start_offset,end_offset,payload_digest,created_at,updated_at
		) VALUES(?,?,?,?,'SENDING',?,?,?,?,?,?)`, req.AttemptID, req.JobID, req.BatchID,
			req.RunSegmentID, req.ParentCheckpointDigest, req.StartOffset, req.EndOffset,
			req.PayloadDigest, now, now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET active_attempt_id=?,updated_at=?
			WHERE job_id=? AND run_segment_id=? AND state='ACTIVE'
			AND committed_checkpoint_digest=? AND active_attempt_id IS NULL`, req.AttemptID, now,
			req.JobID, req.RunSegmentID, req.ParentCheckpointDigest)
		if err != nil {
			return err
		}
		return requireOneRow(result, ErrCheckpointConflict)
	})
}

// InvalidatePermits safely stops all run segments bound to an invalidated
// protection revision. Callers invoke this after Lock/Close has established its
// dispatchGate barrier, so no matching Permit can cross a new start barrier.
func (r *Repository) InvalidatePermits(
	ctx context.Context,
	connectionID, generation, protectionRevision string,
	reason PermitInvalidationReason,
) (int, error) {
	if connectionID == "" || !validPermitInvalidationReason(reason) {
		return 0, ErrPermitMismatch
	}
	if _, err := canonicalDecimal(generation); err != nil {
		return 0, err
	}
	if _, err := canonicalDecimal(protectionRevision); err != nil {
		return 0, err
	}
	entries := r.permits.Matching(connectionID, generation, protectionRevision)
	invalidated := 0
	for _, entry := range entries {
		changed, err := r.invalidatePermit(ctx, entry.jobID, entry.permit, reason)
		if err != nil {
			return invalidated, err
		}
		if changed {
			invalidated++
		}
	}
	return invalidated, nil
}

// PauseRun revokes exactly the requested process-local run segment. It is used
// when the background runner cannot safely schedule another batch after a
// local, pre-dispatch failure. A stale runner can never pause a newer segment.
func (r *Repository) PauseRun(
	ctx context.Context,
	jobID, runSegmentID string,
	reason PermitInvalidationReason,
) (bool, error) {
	if jobID == "" || runSegmentID == "" || !validPermitInvalidationReason(reason) {
		return false, ErrPermitMismatch
	}
	permit, found := r.permits.Get(jobID)
	if !found || permit.RunSegmentID != runSegmentID {
		return false, nil
	}
	return r.invalidatePermit(ctx, jobID, permit, reason)
}

func (r *Repository) invalidatePermit(
	ctx context.Context,
	jobID string,
	permit Permit,
	reason PermitInvalidationReason,
) (bool, error) {
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	current, found := r.permits.Get(jobID)
	if !found || current != permit {
		return false, nil
	}
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.ProfileID != permit.ProfileID {
			return ErrImportProfileMismatch
		}
		if job.Task.State != ImportRunning || job.ActiveRunSegmentID == nil ||
			*job.ActiveRunSegmentID != permit.RunSegmentID {
			return nil
		}
		var sending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND run_segment_id=? AND state='SENDING'`, jobID,
			permit.RunSegmentID).Scan(&sending); err != nil {
			return err
		}
		if sending > 1 {
			return ErrImportState
		}
		now := r.now().UTC()
		if sending == 0 {
			result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
				active_attempt_id=NULL,stop_reason=?,updated_at=?
				WHERE job_id=? AND run_segment_id=? AND state IN ('ACTIVE','STOPPING')
				AND active_attempt_id IS NULL`, string(reason), now.Format(time.RFC3339Nano),
				jobID, permit.RunSegmentID)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrImportState); err != nil {
				return err
			}
			updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
				State: ImportPausedSafe, StateChanged: true, ChangeType: "PAUSED",
			})
			if err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
				snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
				WHERE job_id=? AND state=? AND active_run_segment_id=?`, ImportPausedSafe,
				updated.StateRevision, updated.SnapshotRevision, now.Format(time.RFC3339Nano),
				jobID, ImportRunning, permit.RunSegmentID)
			if err != nil {
				return err
			}
			return requireOneRow(result, ErrImportState)
		}

		result, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='STOPPING',
			stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=?
			AND state IN ('ACTIVE','STOPPING') AND active_attempt_id IS NOT NULL`, string(reason),
			now.Format(time.RFC3339Nano), jobID, permit.RunSegmentID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrImportState); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportRunning, StateChanged: true, ChangeType: "CONTROL",
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state_revision=?,
			snapshot_revision=?,pause_requested=1,updated_at=?
			WHERE job_id=? AND state=? AND active_run_segment_id=?`, updated.StateRevision,
			updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID, ImportRunning,
			permit.RunSegmentID)
		if err != nil {
			return err
		}
		return requireOneRow(result, ErrImportState)
	})
	if err != nil {
		return false, err
	}
	r.permits.RevokeIfMatch(jobID, permit)
	return true, nil
}

func validPermitInvalidationReason(reason PermitInvalidationReason) bool {
	switch reason {
	case PermitInvalidatedProtectionLocked, PermitInvalidatedBindingChanged,
		PermitInvalidatedConnectionClosed,
		PermitInvalidatedLeaseExpired, PermitInvalidatedSessionLocked,
		PermitInvalidatedRunnerStopped:
		return true
	default:
		return false
	}
}

func (r *Repository) SettleACK(ctx context.Context, attemptID string) (ImportJob, error) {
	segmentKind, err := r.attemptSegmentKind(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	if segmentKind == "REPLAY_EXACT" {
		return r.settleReplayACK(ctx, attemptID)
	}
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
		var batchID, state, startOffset, endOffset, payloadDigest string
		if err := tx.QueryRowContext(ctx, `SELECT batch_id,run_segment_id,state,
			parent_checkpoint_digest,start_offset,end_offset,payload_digest
			FROM import_attempts WHERE attempt_id=?`, attemptID).Scan(&batchID, &segment,
			&state, &parent, &startOffset, &endOffset, &payloadDigest); err != nil {
			return err
		}
		_ = startOffset
		_ = payloadDigest
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
		var segmentState string
		var stopReason sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state,stop_reason FROM import_run_segments
			WHERE job_id=? AND run_segment_id=?`, jobID, segment).Scan(&segmentState, &stopReason); err != nil {
			return err
		}
		if segmentState != "ACTIVE" && segmentState != "STOPPING" {
			return ErrImportState
		}
		continueRun = segmentState == "ACTIVE" && !job.PauseRequested
		userCanceled := segmentState == "STOPPING" && stopReason.String == importStopUserCanceled
		next := job.Checkpoint
		next.Sequence, err = incrementDecimal(next.Sequence)
		if err != nil {
			return err
		}
		next.ParentDigest = parent
		next.LogicalOffset = endOffset
		next.LastSettledBatchID = batchID
		next.LastDisposition = "ACKED"
		child, err = CheckpointDigest(next)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		if err := insertCheckpoint(ctx, tx, jobID, child, next, now); err != nil {
			return err
		}
		if err := insertTransition(ctx, tx, Transition{
			JobID: jobID, Sequence: next.Sequence, OwnerRunSegmentID: segment, Cause: "ACK",
			ParentCheckpointDigest: parent, ChildCheckpointDigest: child, BatchID: batchID,
			AttemptID: attemptID, CommittedAt: now.Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='ACKED',
			new_checkpoint_digest=?,updated_at=? WHERE attempt_id=? AND job_id=?
			AND run_segment_id=? AND state='SENDING' AND parent_checkpoint_digest=?`,
			child, now.Format(time.RFC3339Nano), attemptID, jobID, segment, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		newSegmentState := "ACTIVE"
		var committedStopReason any
		if !continueRun {
			newSegmentState = "CLOSED"
			committedStopReason = stopReason.String
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state=?,
			committed_checkpoint_digest=?,active_attempt_id=NULL,stop_reason=?,updated_at=?
			WHERE job_id=? AND run_segment_id=? AND state IN ('ACTIVE','STOPPING')
			AND committed_checkpoint_digest=? AND active_attempt_id=?`, newSegmentState, child,
			committedStopReason, now.Format(time.RFC3339Nano), jobID, segment, parent, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		targetState := ImportRunning
		terminal := false
		changeType := "PROGRESS"
		stateChanged := false
		var activeSegment any = segment
		if !continueRun {
			targetState = ImportPausedSafe
			changeType = "PAUSED"
			stateChanged = true
			activeSegment = nil
			if userCanceled {
				targetState = ImportCanceled
				terminal = true
				changeType = "CANCELED"
			}
		}
		updatedTask, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: stateChanged, Terminal: terminal, ChangeType: changeType,
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,checkpoint_digest=?,active_run_segment_id=?,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`,
			targetState, updatedTask.StateRevision, updatedTask.SnapshotRevision, child,
			activeSegment, now.Format(time.RFC3339Nano), jobID, ImportRunning, parent, segment)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		job.Task = updatedTask
		job.Checkpoint = next
		job.CheckpointDigest = child
		job.PauseRequested = false
		if continueRun {
			job.ActiveRunSegmentID = &segment
		} else {
			job.ActiveRunSegmentID = nil
			if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
				targetState, now.Format(time.RFC3339Nano), jobID); err != nil {
				return err
			}
		}
		out = job
		return nil
	})
	if err != nil {
		return ImportJob{}, err
	}
	if !continueRun {
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

func (r *Repository) RecordIncident(ctx context.Context, attemptID, incidentID, kind string) (ImportJob, error) {
	if kind != "PARTIAL" && kind != "UNKNOWN" {
		return ImportJob{}, errors.New("invalid incident kind")
	}
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var batchID, segment, state, parent, start, end, payload string
		if err := tx.QueryRowContext(ctx, `SELECT batch_id,run_segment_id,state,
			parent_checkpoint_digest,start_offset,end_offset,payload_digest
			FROM import_attempts WHERE attempt_id=?`, attemptID).Scan(&batchID, &segment,
			&state, &parent, &start, &end, &payload); err != nil {
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
		now := r.now().UTC()
		newState := ImportNeedsUnknownDecision
		if kind == "PARTIAL" {
			newState = ImportNeedsPartialDecision
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state=?,updated_at=?
			WHERE attempt_id=? AND job_id=? AND run_segment_id=? AND state='SENDING'
			AND parent_checkpoint_digest=?`, kind, now.Format(time.RFC3339Nano), attemptID,
			jobID, segment, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			active_attempt_id=NULL,stop_reason=?,updated_at=? WHERE job_id=? AND run_segment_id=?
			AND state IN ('ACTIVE','STOPPING') AND active_attempt_id=?`, kind,
			now.Format(time.RFC3339Nano), jobID, segment, attemptID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_incidents(
			incident_id,job_id,kind,status,parent_checkpoint_digest,run_segment_id,
			batch_id,attempt_id,start_offset,end_offset,payload_digest,created_at
		) VALUES(?,?,?,'OPEN',?,?,?,?,?,?,?,?)`, incidentID, jobID, kind, parent,
			segment, batchID, attemptID, start, end, payload, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: newState, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id=?`, newState,
			updated.StateRevision, updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID,
			ImportRunning, parent, segment)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			newState, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = nil
		job.PauseRequested = false
		job.OpenIncident = &Incident{
			IncidentID: incidentID, Kind: kind, Status: "OPEN", ParentCheckpointDigest: parent,
			RunSegmentID: segment, BatchID: batchID, AttemptID: attemptID,
			StartOffset: start, EndOffset: end, PayloadDigest: payload,
		}
		out = job
		return nil
	})
	if err == nil {
		r.permits.Revoke(jobID)
	}
	return out, err
}

func (r *Repository) Abort(ctx context.Context, req AbortRequest) (ImportJob, bool, error) {
	if _, err := uuid.Parse(req.CommandRequestID); err != nil {
		return ImportJob{}, false, errors.New("invalid commandRequestId")
	}
	scope := AbortCommandScope(req)
	digest := AbortCommandDigest(req)
	lock := r.jobLock(req.JobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	replayed := false
	// Revocation happens before the durable transition and while the job lock is
	// held. A rollback therefore fails closed: the incident stays OPEN but no
	// stale permit can dispatch another batch.
	r.permits.Revoke(req.JobID)
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		_, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			out, err = r.getImportInTx(ctx, tx, req.JobID)
			replayed = true
			return err
		}
		job, err := r.getImportInTx(ctx, tx, req.JobID)
		if err != nil {
			return err
		}
		if job.Task.StateRevision != req.ExpectedStateRevision {
			return tasks.ErrRevisionConflict
		}
		if job.Task.State != ImportNeedsPartialDecision && job.Task.State != ImportNeedsUnknownDecision {
			return ErrImportState
		}
		if job.CheckpointDigest != req.ParentCheckpointDigest || job.OpenIncident == nil ||
			job.OpenIncident.IncidentID != req.IncidentID || job.OpenIncident.ParentCheckpointDigest != req.ParentCheckpointDigest {
			return ErrCheckpointConflict
		}
		var sending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND state='SENDING'`, req.JobID).Scan(&sending); err != nil {
			return err
		}
		if sending != 0 {
			return ErrImportState
		}
		now := r.now().UTC()
		result, err := tx.ExecContext(ctx, `UPDATE import_incidents SET status='ABORTED',
			resolution='ABORT',resolved_at=?,resolution_command_request_id=?
			WHERE incident_id=? AND job_id=? AND status='OPEN' AND parent_checkpoint_digest=?`,
			now.Format(time.RFC3339Nano), req.CommandRequestID, req.IncidentID, req.JobID,
			req.ParentCheckpointDigest)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			return ErrOpenIncident
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			active_attempt_id=NULL,stop_reason='USER_ABORTED',updated_at=?
			WHERE job_id=? AND run_segment_id=?`, now.Format(time.RFC3339Nano), req.JobID,
			job.OpenIncident.RunSegmentID); err != nil {
			return err
		}
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportAborted, StateChanged: true, Terminal: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,pause_requested=0,updated_at=?
			WHERE job_id=?`, ImportAborted, updated.StateRevision, updated.SnapshotRevision,
			now.Format(time.RFC3339Nano), req.JobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			ImportAborted, now.Format(time.RFC3339Nano), req.JobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) VALUES('IMPORT','ABORT',?,?,'SUCCEEDED',?,?)`, req.JobID,
			req.ParentCheckpointDigest, req.CommandRequestID, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err := tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "IMPORT", ResourceID: req.JobID,
			ResultState: ImportAborted, ResultStateRevision: updated.StateRevision,
			CommittedAt: now, ExpiresAt: now.Add(90 * 24 * time.Hour),
		}); err != nil {
			return err
		}
		job.Task = updated
		job.ActiveRunSegmentID = nil
		job.PauseRequested = false
		job.OpenIncident = nil
		out = job
		return nil
	})
	return out, replayed, err
}

func AbortCommandScope(req AbortRequest) string {
	return "IMPORT\x1f" + req.JobID + "\x1fABORT\x1f" + req.CommandRequestID
}

func AbortCommandDigest(req AbortRequest) string {
	canonical := "AbortImportV1\n" + req.JobID + "\n" + req.IncidentID + "\n" +
		req.ParentCheckpointDigest + "\n" + req.ExpectedStateRevision
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func (r *Repository) CanCleanup(ctx context.Context, jobID string) error {
	job, err := r.GetImport(ctx, jobID)
	if err != nil {
		return err
	}
	switch job.Task.State {
	case ImportAborted, ImportCanceled, ImportFailed, ImportSucceeded, ImportPausedSafe, ImportPausedRestage:
	default:
		return ErrImportState
	}
	var liveSegments, sending, openIncidents int
	err = r.store.DB().QueryRowContext(ctx, `SELECT
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

func (r *Repository) pausePermitDesync(ctx context.Context, jobID, segmentID, checkpointDigest string) error {
	r.permits.Revoke(jobID)
	return r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != checkpointDigest ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segmentID {
			return nil
		}
		var sending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND state='SENDING'`, jobID).Scan(&sending); err != nil {
			return err
		}
		if sending != 0 {
			return nil
		}
		now := r.now().UTC()
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: ImportPausedSafe, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			stop_reason='PERMIT_CHECKPOINT_DESYNC',updated_at=? WHERE run_segment_id=?`,
			now.Format(time.RFC3339Nano), segmentID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,updated_at=? WHERE job_id=?`,
			ImportPausedSafe, updated.StateRevision, updated.SnapshotRevision,
			now.Format(time.RFC3339Nano), jobID)
		return err
	})
}

func (r *Repository) attemptJob(ctx context.Context, attemptID string) (string, error) {
	var jobID string
	if err := r.store.DB().QueryRowContext(ctx, "SELECT job_id FROM import_attempts WHERE attempt_id=?", attemptID).Scan(&jobID); err != nil {
		return "", err
	}
	return jobID, nil
}

func insertCheckpoint(ctx context.Context, tx store.Executor, jobID, digest string, cp Checkpoint, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(
		job_id,digest,sequence,parent_digest,logical_offset,last_settled_batch_id,
		last_disposition,adaptive_max_points,adaptive_max_bytes,source_sha256,
		staging_sha256,normalization_version,spec_digest,target_digest,lossy,created_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, jobID, digest, cp.Sequence,
		nullString(cp.ParentDigest), cp.LogicalOffset, nullString(cp.LastSettledBatchID),
		nullString(cp.LastDisposition), cp.AdaptiveMaxPoints, cp.AdaptiveMaxBytes,
		cp.SourceSHA256, cp.StagingSHA256, cp.NormalizationVersion, cp.SpecDigest,
		cp.TargetDigest, boolInt(cp.Lossy), now.Format(time.RFC3339Nano))
	return err
}

func readCheckpoint(ctx context.Context, tx store.Executor, jobID, digest string) (Checkpoint, error) {
	var cp Checkpoint
	var parent, batch, disposition sql.NullString
	var lossy int
	err := tx.QueryRowContext(ctx, `SELECT sequence,parent_digest,logical_offset,
		last_settled_batch_id,last_disposition,adaptive_max_points,adaptive_max_bytes,
		source_sha256,staging_sha256,normalization_version,spec_digest,target_digest,lossy
		FROM import_checkpoints WHERE job_id=? AND digest=?`, jobID, digest).Scan(
		&cp.Sequence, &parent, &cp.LogicalOffset, &batch, &disposition,
		&cp.AdaptiveMaxPoints, &cp.AdaptiveMaxBytes, &cp.SourceSHA256, &cp.StagingSHA256,
		&cp.NormalizationVersion, &cp.SpecDigest, &cp.TargetDigest, &lossy)
	if err != nil {
		return Checkpoint{}, err
	}
	cp.ParentDigest = parent.String
	cp.LastSettledBatchID = batch.String
	cp.LastDisposition = disposition.String
	cp.Lossy = lossy != 0
	return cp, nil
}

func insertTransition(ctx context.Context, tx store.Executor, transition Transition) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoint_transitions(
		job_id,sequence,owner_run_segment_id,cause,parent_checkpoint_digest,
		child_checkpoint_digest,batch_id,attempt_id,incident_id,command_request_id,committed_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, transition.JobID, transition.Sequence,
		transition.OwnerRunSegmentID, transition.Cause, transition.ParentCheckpointDigest,
		transition.ChildCheckpointDigest, nullString(transition.BatchID), nullString(transition.AttemptID),
		nullString(transition.IncidentID), nullString(transition.CommandRequestID), transition.CommittedAt)
	return err
}

func readOpenIncident(ctx context.Context, tx store.Executor, jobID string) (Incident, bool, error) {
	var incident Incident
	var replay, resolution, resolvedAt, commandID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT incident_id,kind,status,parent_checkpoint_digest,
		run_segment_id,batch_id,attempt_id,start_offset,end_offset,payload_digest,
		replay_parent_incident_id,resolution,resolved_at,resolution_command_request_id
		FROM import_incidents WHERE job_id=? AND status='OPEN'`, jobID).Scan(
		&incident.IncidentID, &incident.Kind, &incident.Status, &incident.ParentCheckpointDigest,
		&incident.RunSegmentID, &incident.BatchID, &incident.AttemptID, &incident.StartOffset,
		&incident.EndOffset, &incident.PayloadDigest, &replay, &resolution, &resolvedAt, &commandID)
	if errors.Is(err, sql.ErrNoRows) {
		return Incident{}, false, nil
	}
	if err != nil {
		return Incident{}, false, err
	}
	incident.ReplayParentIncidentID = nullPtr(replay)
	incident.Resolution = nullPtr(resolution)
	incident.ResolvedAt = nullPtr(resolvedAt)
	incident.ResolutionCommandRequestID = nullPtr(commandID)
	return incident, true, nil
}

func nullPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	value := v.String
	return &value
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func requireOneRow(result sql.Result, conflict error) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return conflict
	}
	return nil
}
