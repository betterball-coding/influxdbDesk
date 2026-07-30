package transfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

type IncidentDecision string

const (
	DecisionReplayExact     IncidentDecision = "REPLAY_EXACT"
	DecisionAssumeCommitted IncidentDecision = "ASSUME_COMMITTED"
	DecisionAcceptPartial   IncidentDecision = "ACCEPT_PARTIAL"
	DecisionAbort           IncidentDecision = "ABORT"
)

type ResolutionDisposition string

const (
	ResolutionContinue ResolutionDisposition = "CONTINUE"
	ResolutionPause    ResolutionDisposition = "PAUSE"
)

// IncidentResolutionCommit is accepted only after the caller has performed
// connection/protection authorization. GrantTokenHash is the SHA-256 of the
// opaque grant and is used only in the durable command digest.
type IncidentResolutionCommit struct {
	JobID                  string
	IncidentID             string
	ParentCheckpointDigest string
	ExpectedStateRevision  string
	CommandRequestID       string
	Decision               IncidentDecision
	AfterResolution        ResolutionDisposition
	GrantTokenHash         string
	RunSegmentID           string
	Permit                 *Permit
}

func IncidentDigest(incident Incident) string {
	canonical := strings.Join([]string{
		"ImportIncidentV1", incident.IncidentID, incident.Kind, incident.Status,
		incident.ParentCheckpointDigest, incident.RunSegmentID, incident.BatchID,
		incident.AttemptID, incident.StartOffset, incident.EndOffset, incident.PayloadDigest,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func ResolutionCommandScope(request IncidentResolutionCommit) string {
	return "IMPORT\x1f" + request.JobID + "\x1fRESOLVE\x1f" + request.CommandRequestID
}

func ResolutionCommandDigest(request IncidentResolutionCommit) string {
	grantHash := request.GrantTokenHash
	if grantHash == "" {
		grantHash = "ABSENT"
	}
	canonical := strings.Join([]string{
		"ResolveImportBatchV1", request.JobID, request.IncidentID,
		request.ParentCheckpointDigest, request.ExpectedStateRevision,
		request.CommandRequestID, string(request.Decision), string(request.AfterResolution),
		grantHash,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func ResolutionNeedsGrant(decision IncidentDecision, after ResolutionDisposition) bool {
	return decision == DecisionReplayExact ||
		(decision == DecisionAssumeCommitted || decision == DecisionAcceptPartial) && after == ResolutionContinue
}

// ResolveIncident commits a human incident decision, its command ledger row,
// audit record, checkpoint transition, run segment and task event atomically.
func (r *Repository) ResolveIncident(ctx context.Context, request IncidentResolutionCommit) (ImportJob, bool, error) {
	if err := validateResolutionCommit(request); err != nil {
		return ImportJob{}, false, err
	}
	if request.RunSegmentID == "" {
		request.RunSegmentID = uuid.NewString()
	}
	scope := ResolutionCommandScope(request)
	digest := ResolutionCommandDigest(request)
	lock := r.jobLock(request.JobID)
	lock.Lock()
	defer lock.Unlock()

	var out ImportJob
	replayed := false
	var committedCheckpoint string
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		_, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			out, err = r.getImportInTx(ctx, tx, request.JobID)
			replayed = true
			return err
		}

		job, err := r.getImportInTx(ctx, tx, request.JobID)
		if err != nil {
			return err
		}
		if job.Task.StateRevision != request.ExpectedStateRevision {
			return tasks.ErrRevisionConflict
		}
		if err := validateResolutionState(request, job); err != nil {
			return err
		}
		var sending int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM import_attempts
			WHERE job_id=? AND state='SENDING'`, request.JobID).Scan(&sending); err != nil {
			return err
		}
		if sending != 0 {
			return ErrImportState
		}

		incident := *job.OpenIncident
		now := r.now().UTC()
		checkpoint := job.Checkpoint
		committedCheckpoint = job.CheckpointDigest
		segmentKind := "REPLAY_EXACT"
		segmentState := "ACTIVE"
		var stopReason any
		var afterResolution any = string(request.AfterResolution)
		targetState := ImportRunning
		activeSegment := any(request.RunSegmentID)

		if request.Decision != DecisionReplayExact {
			segmentKind = "NORMAL"
			checkpoint.Sequence, err = incrementDecimal(checkpoint.Sequence)
			if err != nil {
				return err
			}
			checkpoint.ParentDigest = job.CheckpointDigest
			checkpoint.LogicalOffset = incident.EndOffset
			checkpoint.LastSettledBatchID = incident.BatchID
			switch request.Decision {
			case DecisionAssumeCommitted:
				checkpoint.LastDisposition = "ASSUMED_COMMITTED"
			case DecisionAcceptPartial:
				checkpoint.LastDisposition = "ACCEPTED_PARTIAL"
				checkpoint.Lossy = true
			}
			committedCheckpoint, err = CheckpointDigest(checkpoint)
			if err != nil {
				return err
			}
			if request.AfterResolution == ResolutionPause {
				segmentKind = "RESOLUTION_ONLY"
				segmentState = "CLOSED"
				stopReason = "AFTER_RESOLUTION_PAUSE"
				targetState = ImportPausedSafe
				activeSegment = nil
			}
			afterResolution = nil
		}

		if _, err := tx.ExecContext(ctx, `INSERT INTO import_run_segments(
			run_segment_id,job_id,kind,state,initial_checkpoint_digest,
			committed_checkpoint_digest,stop_reason,after_resolution,resolution_command_request_id,
			created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, request.RunSegmentID, request.JobID, segmentKind,
			segmentState, job.CheckpointDigest, committedCheckpoint, stopReason,
			afterResolution, request.CommandRequestID, now.Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano)); err != nil {
			return err
		}

		if request.Decision != DecisionReplayExact {
			if err := insertCheckpoint(ctx, tx, request.JobID, committedCheckpoint, checkpoint, now); err != nil {
				return err
			}
			cause := string(request.Decision)
			if err := insertTransition(ctx, tx, Transition{
				JobID: request.JobID, Sequence: checkpoint.Sequence,
				OwnerRunSegmentID: request.RunSegmentID, Cause: cause,
				ParentCheckpointDigest: request.ParentCheckpointDigest,
				ChildCheckpointDigest:  committedCheckpoint, BatchID: incident.BatchID,
				AttemptID: incident.AttemptID, IncidentID: incident.IncidentID,
				CommandRequestID: request.CommandRequestID,
				CommittedAt:      now.Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `UPDATE import_incidents SET status='RESOLVED',
				resolution=?,resolved_at=?,resolution_command_request_id=?
				WHERE incident_id=? AND job_id=? AND status='OPEN' AND parent_checkpoint_digest=?`,
				request.Decision, now.Format(time.RFC3339Nano), request.CommandRequestID,
				request.IncidentID, request.JobID, request.ParentCheckpointDigest)
			if err != nil {
				return err
			}
			if err := requireOneRow(result, ErrOpenIncident); err != nil {
				return err
			}
		}

		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,checkpoint_digest=?,active_run_segment_id=?,pause_requested=0,updated_at=?
			WHERE job_id=? AND state=? AND checkpoint_digest=? AND active_run_segment_id IS NULL`,
			targetState, updated.StateRevision, updated.SnapshotRevision, committedCheckpoint,
			activeSegment, now.Format(time.RFC3339Nano), request.JobID, job.Task.State,
			request.ParentCheckpointDigest)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) VALUES('IMPORT',?, ?,?,'SUCCEEDED',?,?)`, string(request.Decision), request.JobID,
			request.ParentCheckpointDigest, request.CommandRequestID, now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err := tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "IMPORT", ResourceID: request.JobID,
			ResultState: targetState, ResultStateRevision: updated.StateRevision,
			CommittedAt: now, ExpiresAt: now.Add(90 * 24 * time.Hour),
		}); err != nil {
			return err
		}

		job.Task = updated
		job.Checkpoint = checkpoint
		job.CheckpointDigest = committedCheckpoint
		if activeSegment == nil {
			job.ActiveRunSegmentID = nil
		} else {
			segmentID := request.RunSegmentID
			job.ActiveRunSegmentID = &segmentID
		}
		if request.Decision != DecisionReplayExact {
			job.OpenIncident = nil
		}
		out = job
		return nil
	})
	if err != nil || replayed {
		return out, replayed, err
	}

	if !ResolutionNeedsGrant(request.Decision, request.AfterResolution) {
		return out, false, nil
	}
	permit := *request.Permit
	permit.RunSegmentID = request.RunSegmentID
	permit.CurrentCheckpointDigest = committedCheckpoint
	if err := r.permits.Activate(request.JobID, permit); err != nil {
		if request.Decision == DecisionReplayExact {
			_ = r.restoreReplayPermitFailure(ctx, request.JobID, request.RunSegmentID, committedCheckpoint)
		} else {
			_ = r.pausePermitDesync(ctx, request.JobID, request.RunSegmentID, committedCheckpoint)
		}
		current, readErr := r.GetImport(ctx, request.JobID)
		return current, false, errors.Join(err, readErr)
	}
	return out, false, nil
}

func validateResolutionCommit(request IncidentResolutionCommit) error {
	if _, err := uuid.Parse(request.CommandRequestID); err != nil || request.JobID == "" ||
		request.IncidentID == "" || request.ParentCheckpointDigest == "" {
		return ErrImportState
	}
	if _, err := canonicalDecimal(request.ExpectedStateRevision); err != nil {
		return err
	}
	if request.Decision == DecisionAbort {
		return ErrImportState
	}
	switch request.AfterResolution {
	case ResolutionContinue, ResolutionPause:
	default:
		return ErrImportState
	}
	needsGrant := ResolutionNeedsGrant(request.Decision, request.AfterResolution)
	if needsGrant {
		if request.Permit == nil || len(request.GrantTokenHash) != sha256.Size*2 {
			return ErrGrantMismatch
		}
		if _, err := hex.DecodeString(request.GrantTokenHash); err != nil || !validPermit(*request.Permit) {
			return ErrGrantMismatch
		}
		if request.RunSegmentID == "" || request.Permit.RunSegmentID != request.RunSegmentID ||
			request.Permit.CurrentCheckpointDigest != request.ParentCheckpointDigest {
			return ErrPermitMismatch
		}
	} else if request.Permit != nil || request.GrantTokenHash != "" || request.AfterResolution != ResolutionPause {
		return ErrGrantMismatch
	}
	return nil
}

func validateResolutionState(request IncidentResolutionCommit, job ImportJob) error {
	if job.CheckpointDigest != request.ParentCheckpointDigest || job.OpenIncident == nil ||
		job.OpenIncident.IncidentID != request.IncidentID ||
		job.OpenIncident.ParentCheckpointDigest != request.ParentCheckpointDigest ||
		job.OpenIncident.Status != "OPEN" {
		return ErrCheckpointConflict
	}
	if request.Permit != nil && request.Permit.ProfileID != job.ProfileID {
		return ErrImportProfileMismatch
	}
	switch request.Decision {
	case DecisionReplayExact:
		if job.Task.State != ImportNeedsPartialDecision && job.Task.State != ImportNeedsUnknownDecision {
			return ErrImportState
		}
	case DecisionAssumeCommitted:
		if job.Task.State != ImportNeedsUnknownDecision || job.OpenIncident.Kind != "UNKNOWN" {
			return ErrImportState
		}
	case DecisionAcceptPartial:
		if job.Task.State != ImportNeedsPartialDecision || job.OpenIncident.Kind != "PARTIAL" {
			return ErrImportState
		}
	default:
		return ErrImportState
	}
	return nil
}

func (r *Repository) restoreReplayPermitFailure(ctx context.Context, jobID, segmentID, checkpointDigest string) error {
	r.permits.Revoke(jobID)
	return r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != checkpointDigest ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segmentID || job.OpenIncident == nil {
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
		target := ImportNeedsUnknownDecision
		if job.OpenIncident.Kind == "PARTIAL" {
			target = ImportNeedsPartialDecision
		}
		now := r.now().UTC()
		updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: target, StateChanged: true, ChangeType: "STATE",
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE import_run_segments SET state='CLOSED',
			stop_reason='PERMIT_ACTIVATION_FAILED',updated_at=? WHERE run_segment_id=? AND state='ACTIVE'`,
			now.Format(time.RFC3339Nano), segmentID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE import_jobs SET state=?,state_revision=?,
			snapshot_revision=?,active_run_segment_id=NULL,updated_at=? WHERE job_id=?`, target,
			updated.StateRevision, updated.SnapshotRevision, now.Format(time.RFC3339Nano), jobID)
		return err
	})
}

func (r *Repository) attemptSegmentKind(ctx context.Context, attemptID string) (string, error) {
	var kind string
	err := r.store.DB().QueryRowContext(ctx, `SELECT s.kind FROM import_attempts a
		JOIN import_run_segments s ON s.run_segment_id=a.run_segment_id
		WHERE a.attempt_id=?`, attemptID).Scan(&kind)
	return kind, err
}

func (r *Repository) settleReplayACK(ctx context.Context, attemptID string) (ImportJob, error) {
	jobID, err := r.attemptJob(ctx, attemptID)
	if err != nil {
		return ImportJob{}, err
	}
	lock := r.jobLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	var out ImportJob
	var parent, child, segment string
	var shouldContinue bool
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		var batchID, state, startOffset, endOffset, payloadDigest string
		if err := tx.QueryRowContext(ctx, `SELECT batch_id,run_segment_id,state,
			parent_checkpoint_digest,start_offset,end_offset,payload_digest
			FROM import_attempts WHERE attempt_id=?`, attemptID).Scan(&batchID, &segment,
			&state, &parent, &startOffset, &endOffset, &payloadDigest); err != nil {
			return err
		}
		if state != "SENDING" {
			return ErrImportState
		}
		var segmentState, resolutionCommandID string
		var after, existingStopReason sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state,after_resolution,resolution_command_request_id,stop_reason
			FROM import_run_segments
			WHERE job_id=? AND run_segment_id=? AND kind='REPLAY_EXACT'`, jobID, segment).Scan(
			&segmentState, &after, &resolutionCommandID, &existingStopReason); err != nil {
			return err
		}
		job, err := r.getImportInTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if job.Task.State != ImportRunning || job.CheckpointDigest != parent ||
			job.ActiveRunSegmentID == nil || *job.ActiveRunSegmentID != segment ||
			job.OpenIncident == nil || job.OpenIncident.ParentCheckpointDigest != parent ||
			job.OpenIncident.StartOffset != startOffset || job.OpenIncident.EndOffset != endOffset ||
			job.OpenIncident.PayloadDigest != payloadDigest || job.OpenIncident.Status != "OPEN" {
			return ErrCheckpointConflict
		}
		incident := *job.OpenIncident
		next := job.Checkpoint
		next.Sequence, err = incrementDecimal(next.Sequence)
		if err != nil {
			return err
		}
		next.ParentDigest = parent
		next.LogicalOffset = incident.EndOffset
		next.LastSettledBatchID = incident.BatchID
		next.LastDisposition = "REPLAY_ACKED"
		child, err = CheckpointDigest(next)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		if err := insertCheckpoint(ctx, tx, jobID, child, next, now); err != nil {
			return err
		}
		if err := insertTransition(ctx, tx, Transition{
			JobID: jobID, Sequence: next.Sequence, OwnerRunSegmentID: segment,
			Cause: "REPLAY_ACK", ParentCheckpointDigest: parent, ChildCheckpointDigest: child,
			BatchID: batchID, AttemptID: attemptID, IncidentID: incident.IncidentID,
			CommittedAt: now.Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_attempts SET state='ACKED',
			new_checkpoint_digest=?,updated_at=? WHERE attempt_id=? AND state='SENDING'
			AND run_segment_id=? AND parent_checkpoint_digest=?`, child,
			now.Format(time.RFC3339Nano), attemptID, segment, parent)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrCheckpointConflict); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE import_incidents SET status='RESOLVED',
			resolution='REPLAY_EXACT',resolved_at=?,resolution_command_request_id=?
			WHERE incident_id=? AND job_id=? AND status='OPEN'`, now.Format(time.RFC3339Nano),
			resolutionCommandID, incident.IncidentID, jobID)
		if err != nil {
			return err
		}
		if err := requireOneRow(result, ErrOpenIncident); err != nil {
			return err
		}
		shouldContinue = after.Valid && after.String == string(ResolutionContinue) &&
			segmentState == "ACTIVE" && !job.PauseRequested
		targetState := ImportPausedSafe
		terminal := false
		var activeSegment any
		newSegmentState := "CLOSED"
		var stopReason any = "AFTER_RESOLUTION_PAUSE"
		if segmentState == "STOPPING" && existingStopReason.String == importStopUserCanceled {
			targetState = ImportCanceled
			terminal = true
			stopReason = importStopUserCanceled
		}
		if shouldContinue {
			targetState = ImportRunning
			activeSegment = segment
			newSegmentState = "ACTIVE"
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
		updatedTask, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
			State: targetState, StateChanged: true, Terminal: terminal, ChangeType: "STATE",
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
		job.OpenIncident = nil
		job.PauseRequested = false
		if shouldContinue {
			job.ActiveRunSegmentID = &segment
		} else {
			job.ActiveRunSegmentID = nil
		}
		out = job
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?`,
			targetState, now.Format(time.RFC3339Nano), jobID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return ImportJob{}, err
	}
	if !shouldContinue {
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
