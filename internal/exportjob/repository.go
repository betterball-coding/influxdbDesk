package exportjob

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
)

const commandRetention = 90 * 24 * time.Hour

type Repository struct {
	store           *store.Store
	tasks           *tasks.Repository
	quota           *transfer.QuotaManager
	now             func() time.Time
	validationEpoch string
}

func NewRepository(s *store.Store, taskRepo *tasks.Repository, now func() time.Time) *Repository {
	if now == nil {
		now = time.Now
	}
	if taskRepo == nil {
		taskRepo = tasks.NewRepository(s, now)
	}
	return &Repository{
		store: s, tasks: taskRepo, quota: transfer.NewQuotaManager(s, now), now: now,
		validationEpoch: uuid.NewString(),
	}
}

func (r *Repository) Start(ctx context.Context, req StartRequest) (Job, bool, error) {
	if err := validateStartRequest(req); err != nil {
		return Job{}, false, err
	}
	scope := startScope(req)
	var out Job
	replayed := false
	err := r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupIdempotencyInTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != req.RequestDigest {
				return tasks.ErrIdempotencyConflict
			}
			if entry.ResourceKind != "EXPORT" {
				return tasks.ErrIdempotencyConflict
			}
			out, err = r.getInTx(ctx, tx, entry.ResourceID, true)
			replayed = true
			return err
		}
		var schedulable int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM export_jobs
			WHERE connection_id=? AND connection_generation=?
			AND state IN ('QUEUED','RUNNING','FINALIZING')`, req.ConnectionID,
			req.ConnectionGeneration).Scan(&schedulable); err != nil {
			return err
		}
		if schedulable >= 8 {
			return ErrConnectionJobLimit
		}

		if err := r.quota.AdmitInTx(ctx, tx, req.JobID, req.ProfileID, "EXPORT", StateQueued); err != nil {
			return err
		}
		meta, err := r.tasks.CreateInTx(ctx, tx, req.JobID, "EXPORT", StateQueued, false)
		if err != nil {
			return err
		}
		reservationID, err := r.reserveInitialTargetInTx(ctx, tx, req)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		if _, err := tx.ExecContext(ctx, `INSERT INTO export_jobs(
			job_id,profile_revision,connection_id,connection_generation,spec_digest,
			target_digest,target_volume_id,target_reservation_id,state,state_revision,
			snapshot_revision,cancel_requested,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?,?)`, req.JobID, req.ProfileRevision,
			req.ConnectionID, req.ConnectionGeneration, req.SpecDigest, req.TargetDigest,
			req.TargetVolumeID, reservationID, StateQueued, meta.StateRevision,
			meta.SnapshotRevision, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("insert export job: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO export_plan_details(
			job_id,schema_version,database_name,retention_policy,measurement,start_ns,end_ns,
			slice_width_ns,output_directory,type_preserving,lossy,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, req.JobID, req.Plan.SchemaVersion,
			req.Plan.Database, req.Plan.RetentionPolicy, req.Plan.Measurement, req.Plan.StartNS,
			req.Plan.EndNS, req.Plan.SliceWidthNS, req.Plan.OutputDirectory,
			boolInt(req.Plan.TypePreserving), boolInt(req.Plan.Lossy),
			now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("insert export plan detail: %w", err)
		}
		if err := tasks.InsertIdempotencyInTx(ctx, tx, tasks.IdempotencyEntry{
			Scope: scope, RequestDigest: req.RequestDigest, ResourceKind: "EXPORT",
			ResourceID: req.JobID, CreatedAt: now,
			ExpiresAt: now.Add(commandRetention),
		}); err != nil {
			return err
		}
		reservation := reservationID
		out = Job{
			Task: meta, ProfileID: req.ProfileID, ProfileRevision: req.ProfileRevision,
			ConnectionID: req.ConnectionID, ConnectionGeneration: req.ConnectionGeneration,
			SpecDigest: req.SpecDigest, TargetDigest: req.TargetDigest,
			TargetVolumeID: req.TargetVolumeID, TargetReservationID: &reservation,
			Retained: true, Plan: req.Plan,
		}
		return nil
	})
	return out, replayed, err
}

// ReplayStart gives the facade a ledger-first path before it probes the target
// volume. A miss has no side effects and Start still repeats the lookup inside
// its admission transaction.
func (r *Repository) ReplayStart(
	ctx context.Context,
	profileID, connectionID, clientRequestID, requestDigest string,
) (Job, bool, error) {
	if profileID == "" || connectionID == "" || uuid.Validate(clientRequestID) != nil || !validSHA256(requestDigest) {
		return Job{}, false, ErrInvalidRequest
	}
	scope := "EXPORT/" + profileID + "/" + connectionID + "/" + clientRequestID
	entry, found, err := tasks.LookupIdempotencyInTx(ctx, r.store.DB(), scope)
	if err != nil || !found {
		return Job{}, found, err
	}
	if entry.RequestDigest != requestDigest || entry.ResourceKind != "EXPORT" {
		return Job{}, true, tasks.ErrIdempotencyConflict
	}
	job, err := r.Get(ctx, entry.ResourceID)
	return job, true, err
}

func (r *Repository) Get(ctx context.Context, jobID string) (Job, error) {
	tx, err := r.store.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Job{}, err
	}
	job, readErr := r.getInTx(ctx, tx, jobID, true)
	if readErr != nil {
		_ = tx.Rollback()
		return Job{}, readErr
	}
	if err := tx.Commit(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (r *Repository) List(ctx context.Context, filter ListFilter) ([]Job, error) {
	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 100
	}
	if filter.State != "" && !validState(filter.State) {
		return nil, ErrInvalidRequest
	}
	query := `SELECT e.job_id FROM export_jobs e
		JOIN transfer_jobs t ON t.job_id=e.job_id WHERE 1=1`
	args := make([]any, 0, 3)
	if filter.ProfileID != "" {
		query += " AND t.profile_id=?"
		args = append(args, filter.ProfileID)
	}
	if filter.State != "" {
		query += " AND e.state=?"
		args = append(args, filter.State)
	}
	query += " ORDER BY e.created_at DESC,e.job_id LIMIT ?"
	args = append(args, filter.Limit)
	rows, err := r.store.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, len(ids))
	for _, id := range ids {
		job, err := r.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (r *Repository) LaneRequest(ctx context.Context, jobID string, quanta []exportlane.Quantum) (exportlane.JobRequest, error) {
	job, err := r.Get(ctx, jobID)
	if err != nil {
		return exportlane.JobRequest{}, err
	}
	for _, fragment := range job.Fragments {
		if fragment.State == FragmentComplete && !fragment.Reusable {
			return exportlane.JobRequest{}, ErrFragmentValidationRequired
		}
		if fragment.State == FragmentWriting || fragment.State == FragmentFinalizing {
			return exportlane.JobRequest{}, ErrFragmentState
		}
	}
	return job.LaneRequest(quanta)
}

func (r *Repository) Restart(ctx context.Context, jobID string, envelope CommandEnvelope) (Job, bool, error) {
	return r.runCommand(ctx, jobID, "RESTART", StateQueued, envelope, func(
		ctx context.Context, tx store.Executor, job Job,
	) (Job, error) {
		if job.Task.State != StatePausedRestartable || job.Task.Terminal || job.CancelRequested {
			return Job{}, ErrStateConflict
		}
		var invalid int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM export_fragments
			WHERE job_id=? AND ((state='COMPLETE' AND (validation_epoch IS NULL OR validation_epoch<>?))
			OR state IN ('WRITING','FINALIZING'))`, jobID, r.validationEpoch).Scan(&invalid); err != nil {
			return Job{}, err
		}
		if invalid != 0 {
			return Job{}, ErrFragmentValidationRequired
		}
		return r.applyStateInTx(ctx, tx, job, StateQueued, false, false, "STATE", nil)
	})
}

func (r *Repository) Cancel(ctx context.Context, jobID string, envelope CommandEnvelope) (Job, bool, error) {
	return r.runCommand(ctx, jobID, "CANCEL", StateCanceled, envelope, func(
		ctx context.Context, tx store.Executor, job Job,
	) (Job, error) {
		if job.Task.State == StateCanceled || job.CancelRequested {
			return job, nil
		}
		switch job.Task.State {
		case StateQueued, StatePausedRestartable:
			return r.applyStateInTx(ctx, tx, job, StateCanceled, true, false, "STATE", nil)
		case StateRunning, StateFinalizing:
			return r.applyStateInTx(ctx, tx, job, job.Task.State, false, true, "CONTROL", nil)
		default:
			return Job{}, ErrStateConflict
		}
	})
}

func (r *Repository) Cleanup(ctx context.Context, jobID string, envelope CommandEnvelope) (Job, bool, error) {
	return r.runCommand(ctx, jobID, "CLEANUP", StateCleaning, envelope, func(
		ctx context.Context, tx store.Executor, job Job,
	) (Job, error) {
		var finalState string
		switch job.Task.State {
		case StatePausedRestartable:
			finalState = StateCanceled
		case StateSucceeded, StateCanceled, StateFailed:
			finalState = job.Task.State
		default:
			return Job{}, ErrStateConflict
		}
		if !job.Retained {
			if job.Task.State == StateSucceeded {
				return job, nil
			}
			return Job{}, ErrStateConflict
		}
		updated, err := r.applyStateInTx(ctx, tx, job, StateCleaning, false, false, "STATE", &finalState)
		if err != nil {
			return Job{}, err
		}
		if updated.Task.TerminalAt != nil {
			if _, err := tx.ExecContext(ctx, "UPDATE tasks SET terminal_at=NULL WHERE id=?", jobID); err != nil {
				return Job{}, err
			}
			updated.Task.TerminalAt = nil
		}
		return updated, nil
	})
}

// CompleteCleanupAfterFilesRemoved is called only after the cleanup worker has
// stopped writers, closed handles, removed app-managed files, and confirmed the
// removals. It releases reservations and the retained slot in the same FULL
// transaction as the terminal snapshot.
func (r *Repository) CompleteCleanupAfterFilesRemoved(ctx context.Context, completion CleanupCompletion) (Job, error) {
	if completion.JobID == "" {
		return Job{}, ErrInvalidRequest
	}
	expected, err := canonicalUnsigned(completion.ExpectedStateRevision)
	if err != nil {
		return Job{}, err
	}
	var out Job
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, completion.JobID, true)
		if err != nil {
			return err
		}
		if !job.Retained && job.Task.Terminal {
			out = job
			return nil
		}
		if job.Task.StateRevision != expected {
			return tasks.ErrRevisionConflict
		}
		if job.Task.State != StateCleaning || job.CleanupFinalState == nil {
			return ErrStateConflict
		}
		// This boundary is invoked only after every app-managed artifact has
		// been confirmed removed. Preserve coherent artifact ledgers before
		// deleting their transfer reservation rows (whose extent mappings
		// cascade).
		if _, err := tx.ExecContext(ctx, `UPDATE export_artifact_reservations SET
			state='ABORTED',settled_bytes='0',updated_at=?
			WHERE job_id=? AND state='ACTIVE'`, r.now().UTC().Format(time.RFC3339Nano),
			completion.JobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM transfer_reservations WHERE job_id=?", completion.JobID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET retained=0,state=?,updated_at=?
			WHERE job_id=? AND retained=1`, *job.CleanupFinalState,
			r.now().UTC().Format(time.RFC3339Nano), completion.JobID)
		if err != nil {
			return err
		}
		if err := requireOne(result); err != nil {
			return err
		}
		out, err = r.applyStateInTx(ctx, tx, job, *job.CleanupFinalState, true, false, "CLEANED", nil)
		if err != nil {
			return err
		}
		out.Retained = false
		out.TargetReservationID = nil
		return nil
	})
	return out, err
}

func (r *Repository) BeginRun(ctx context.Context, transition WorkerTransition) (Job, error) {
	return r.workerTransition(ctx, transition, []string{StateQueued}, StateRunning, false, "STATE")
}

func (r *Repository) BeginFinalizing(ctx context.Context, transition WorkerTransition) (Job, error) {
	return r.workerTransition(ctx, transition, []string{StateRunning}, StateFinalizing, false, "STATE")
}

func (r *Repository) PauseRestartable(ctx context.Context, transition WorkerTransition) (Job, error) {
	return r.workerTransition(ctx, transition, []string{StateQueued, StateRunning, StateFinalizing}, StatePausedRestartable, false, "STATE")
}

func (r *Repository) Succeed(ctx context.Context, transition WorkerTransition) (Job, error) {
	return r.workerTransition(ctx, transition, []string{StateFinalizing}, StateSucceeded, true, "STATE")
}

// CompleteDelivery commits the successful terminal snapshot together with
// release of the retained slot and all target-volume reservations. COMPLETE
// files are user-owned delivery output and are intentionally left untouched.
func (r *Repository) CompleteDelivery(ctx context.Context, transition WorkerTransition) (Job, error) {
	if transition.JobID == "" {
		return Job{}, ErrInvalidRequest
	}
	expected, err := canonicalUnsigned(transition.ExpectedStateRevision)
	if err != nil {
		return Job{}, err
	}
	var out Job
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, transition.JobID, true)
		if err != nil {
			return err
		}
		if job.Task.StateRevision != expected {
			return tasks.ErrRevisionConflict
		}
		if job.Task.State != StateFinalizing || job.CancelRequested || !job.Retained {
			return ErrStateConflict
		}
		if len(job.Fragments) == 0 {
			return ErrFragmentState
		}
		for _, fragment := range job.Fragments {
			if fragment.State != FragmentComplete {
				return ErrFragmentState
			}
		}
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM export_artifact_reservations
			WHERE job_id=? AND state<>'SETTLED'`, transition.JobID).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return ErrArtifactReservation
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM transfer_reservations WHERE job_id=?", transition.JobID); err != nil {
			return err
		}
		now := r.now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE transfer_jobs SET retained=0,state=?,updated_at=?
			WHERE job_id=? AND retained=1`, StateSucceeded, now, transition.JobID)
		if err != nil {
			return err
		}
		if err := requireOne(result); err != nil {
			return err
		}
		out, err = r.applyStateInTx(ctx, tx, job, StateSucceeded, true, false, "DELIVERED", nil)
		if err != nil {
			return err
		}
		out.Retained = false
		out.TargetReservationID = nil
		return nil
	})
	return out, err
}

func (r *Repository) Fail(ctx context.Context, transition WorkerTransition) (Job, error) {
	return r.workerTransition(ctx, transition,
		[]string{StateQueued, StateRunning, StateFinalizing, StatePausedRestartable}, StateFailed, true, "STATE")
}

func (r *Repository) FinishCancel(ctx context.Context, transition WorkerTransition) (Job, error) {
	if transition.JobID == "" {
		return Job{}, ErrInvalidRequest
	}
	expected, err := canonicalUnsigned(transition.ExpectedStateRevision)
	if err != nil {
		return Job{}, err
	}
	var out Job
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, transition.JobID, true)
		if err != nil {
			return err
		}
		if job.Task.State == StateCanceled {
			out = job
			return nil
		}
		if job.Task.StateRevision != expected {
			return tasks.ErrRevisionConflict
		}
		if !job.CancelRequested || (job.Task.State != StateRunning && job.Task.State != StateFinalizing) {
			return ErrStateConflict
		}
		out, err = r.applyStateInTx(ctx, tx, job, StateCanceled, true, false, "STATE", nil)
		return err
	})
	return out, err
}

func (r *Repository) workerTransition(
	ctx context.Context,
	transition WorkerTransition,
	allowed []string,
	target string,
	terminal bool,
	changeType string,
) (Job, error) {
	if transition.JobID == "" || !validState(target) {
		return Job{}, ErrInvalidRequest
	}
	expected, err := canonicalUnsigned(transition.ExpectedStateRevision)
	if err != nil {
		return Job{}, err
	}
	var code *string
	if transition.PublicErrorCode != "" {
		value := transition.PublicErrorCode
		code = &value
	}
	var out Job
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		job, err := r.getInTx(ctx, tx, transition.JobID, true)
		if err != nil {
			return err
		}
		if job.Task.StateRevision != expected {
			return tasks.ErrRevisionConflict
		}
		if !contains(allowed, job.Task.State) || job.CancelRequested {
			return ErrStateConflict
		}
		out, err = r.applyStateInTx(ctx, tx, job, target, terminal, false, changeType, nil, code)
		return err
	})
	return out, err
}

func (r *Repository) runCommand(
	ctx context.Context,
	jobID, kind, target string,
	envelope CommandEnvelope,
	apply func(context.Context, store.Executor, Job) (Job, error),
) (Job, bool, error) {
	if jobID == "" || uuid.Validate(envelope.CommandRequestID) != nil {
		return Job{}, false, ErrInvalidRequest
	}
	expected, err := canonicalUnsigned(envelope.ExpectedStateRevision)
	if err != nil {
		return Job{}, false, err
	}
	envelope.ExpectedStateRevision = expected
	scope := commandScope(jobID, kind, envelope.CommandRequestID)
	digest, err := commandDigest(jobID, kind, target, envelope)
	if err != nil {
		return Job{}, false, err
	}
	var out Job
	replayed := false
	err = r.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupCommandInTx(ctx, tx, scope, digest)
		if err != nil {
			return err
		}
		if found {
			if entry.ResourceKind != "EXPORT" || entry.ResourceID != jobID {
				return tasks.ErrCommandIdempotencyConflict
			}
			out, err = r.getInTx(ctx, tx, jobID, true)
			replayed = true
			return err
		}
		job, err := r.getInTx(ctx, tx, jobID, true)
		if err != nil {
			return err
		}
		// Cancel is the only command whose already-satisfied target ignores a
		// stale revision. Restart and Cleanup never infer completion.
		cancelSatisfied := kind == "CANCEL" && (job.Task.State == StateCanceled || job.CancelRequested)
		if !cancelSatisfied && job.Task.StateRevision != expected {
			return tasks.ErrRevisionConflict
		}
		out, err = apply(ctx, tx, job)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		return tasks.InsertCommandInTx(ctx, tx, tasks.CommandEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "EXPORT", ResourceID: jobID,
			ResultState: out.Task.State, ResultStateRevision: out.Task.StateRevision,
			CommittedAt: now, ExpiresAt: now.Add(commandRetention),
		})
	})
	return out, replayed, err
}

func (r *Repository) applyStateInTx(
	ctx context.Context,
	tx store.Executor,
	job Job,
	state string,
	terminal bool,
	cancelRequested bool,
	changeType string,
	cleanupFinalState *string,
	errorCode ...*string,
) (Job, error) {
	var code *string
	if len(errorCode) != 0 {
		code = errorCode[0]
	}
	updated, err := r.tasks.ApplyInTx(ctx, tx, job.Task, tasks.Change{
		State: state, Terminal: terminal, StateChanged: true, ChangeType: changeType,
		PublicErrorCode: code,
	})
	if err != nil {
		return Job{}, err
	}
	now := r.now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state=?,state_revision=?,
		snapshot_revision=?,cancel_requested=?,cleanup_final_state=?,updated_at=?
		WHERE job_id=? AND state_revision=?`, state, updated.StateRevision,
		updated.SnapshotRevision, boolInt(cancelRequested), nullableString(cleanupFinalState), now,
		job.Task.ID, job.Task.StateRevision)
	if err != nil {
		return Job{}, err
	}
	if err := requireOne(result); err != nil {
		return Job{}, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE transfer_jobs SET state=?,updated_at=? WHERE job_id=?",
		state, now, job.Task.ID); err != nil {
		return Job{}, err
	}
	job.Task = updated
	job.CancelRequested = cancelRequested
	job.CleanupFinalState = cleanupFinalState
	return job, nil
}

func (r *Repository) getInTx(ctx context.Context, tx store.Executor, jobID string, fragments bool) (Job, error) {
	meta, err := tasks.GetInTx(ctx, tx, jobID)
	if err != nil {
		return Job{}, err
	}
	var job Job
	var targetReservation, cleanupState sql.NullString
	var cancelRequested, retained int
	var mirrorState, stateRevision, snapshotRevision string
	err = tx.QueryRowContext(ctx, `SELECT t.profile_id,t.retained,e.profile_revision,
		e.connection_id,e.connection_generation,e.spec_digest,e.target_digest,e.target_volume_id,
		e.target_reservation_id,e.state,e.state_revision,e.snapshot_revision,e.cancel_requested,
		e.cleanup_final_state FROM export_jobs e JOIN transfer_jobs t ON t.job_id=e.job_id
		WHERE e.job_id=?`, jobID).Scan(&job.ProfileID, &retained, &job.ProfileRevision,
		&job.ConnectionID, &job.ConnectionGeneration, &job.SpecDigest, &job.TargetDigest,
		&job.TargetVolumeID, &targetReservation, &mirrorState, &stateRevision,
		&snapshotRevision, &cancelRequested, &cleanupState)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, tasks.ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	if meta.Kind != "EXPORT" || mirrorState != meta.State || stateRevision != meta.StateRevision ||
		snapshotRevision != meta.SnapshotRevision {
		return Job{}, errors.New("export task mirror mismatch")
	}
	job.Task = meta
	job.CancelRequested = cancelRequested != 0
	job.Retained = retained != 0
	if targetReservation.Valid {
		job.TargetReservationID = &targetReservation.String
	}
	if cleanupState.Valid {
		job.CleanupFinalState = &cleanupState.String
	}
	var typePreserving, lossy int
	err = tx.QueryRowContext(ctx, `SELECT schema_version,database_name,retention_policy,
		measurement,start_ns,end_ns,slice_width_ns,output_directory,type_preserving,lossy
		FROM export_plan_details WHERE job_id=?`, jobID).Scan(&job.Plan.SchemaVersion,
		&job.Plan.Database, &job.Plan.RetentionPolicy, &job.Plan.Measurement, &job.Plan.StartNS,
		&job.Plan.EndNS, &job.Plan.SliceWidthNS, &job.Plan.OutputDirectory,
		&typePreserving, &lossy)
	if err != nil {
		return Job{}, err
	}
	job.Plan.TypePreserving = typePreserving != 0
	job.Plan.Lossy = lossy != 0
	if fragments {
		job.Fragments, err = r.listFragmentsInTx(ctx, tx, jobID)
		if err != nil {
			return Job{}, err
		}
	}
	return job, nil
}

func (r *Repository) reserveInitialTargetInTx(ctx context.Context, tx store.Executor, req StartRequest) (string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT reserved_bytes,consumed_bytes
		FROM transfer_reservations WHERE scope_kind='VOLUME' AND scope_id=?`, req.TargetVolumeID)
	if err != nil {
		return "", err
	}
	var outstanding int64
	for rows.Next() {
		var reservedText, consumedText string
		if err := rows.Scan(&reservedText, &consumedText); err != nil {
			rows.Close()
			return "", err
		}
		reserved, err := parseInt64Decimal(reservedText)
		if err != nil {
			rows.Close()
			return "", err
		}
		consumed, err := parseInt64Decimal(consumedText)
		if err != nil {
			rows.Close()
			return "", err
		}
		if consumed > reserved {
			rows.Close()
			return "", errors.New("invalid target reservation accounting")
		}
		unwritten := reserved - consumed
		if unwritten > math.MaxInt64-outstanding {
			rows.Close()
			return "", errors.New("target reservation accounting overflow")
		}
		outstanding += unwritten
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	remaining := req.TargetVolumeFreeBytes
	if outstanding > remaining {
		return "", transfer.ErrTargetLowSpace
	}
	remaining -= outstanding
	if req.TargetReservationBytes > remaining {
		return "", transfer.ErrTargetLowSpace
	}
	remaining -= req.TargetReservationBytes
	if remaining < transfer.VolumeReserveFloor(req.TargetVolumeCapacity) {
		return "", transfer.ErrTargetLowSpace
	}
	reservationID := uuid.NewString()
	now := r.now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `INSERT INTO transfer_reservations(
		reservation_id,job_id,scope_kind,scope_id,reserved_bytes,consumed_bytes,created_at,updated_at
	) VALUES(?,?,'VOLUME',?,?,'0',?,?)`, reservationID, req.JobID, req.TargetVolumeID,
		fmt.Sprintf("%d", req.TargetReservationBytes), now, now)
	return reservationID, err
}

func validateStartRequest(req StartRequest) error {
	if uuid.Validate(req.JobID) != nil || uuid.Validate(req.ClientRequestID) != nil ||
		strings.TrimSpace(req.ProfileID) == "" || strings.TrimSpace(req.ProfileRevision) == "" ||
		strings.TrimSpace(req.ConnectionID) == "" || strings.TrimSpace(req.SpecDigest) == "" ||
		strings.TrimSpace(req.TargetVolumeID) == "" || !validSHA256(req.RequestDigest) ||
		!validSHA256(req.SpecDigest) || !validSHA256(req.TargetDigest) {
		return ErrInvalidRequest
	}
	if _, err := canonicalUnsigned(req.ProfileRevision); err != nil {
		return err
	}
	if _, err := canonicalUnsigned(req.ConnectionGeneration); err != nil {
		return err
	}
	if req.TargetReservationBytes != transfer.ReservationExtentBytes ||
		req.TargetVolumeFreeBytes < 0 || req.TargetVolumeCapacity <= 0 {
		return ErrInvalidRequest
	}
	if err := validatePlanDetail(req.Plan); err != nil {
		return err
	}
	return nil
}

func validatePlanDetail(plan PlanDetail) error {
	if plan.SchemaVersion != 1 || !validPlanText(plan.Database) ||
		!validPlanText(plan.RetentionPolicy) || !validPlanText(plan.Measurement) ||
		plan.OutputDirectory == "" || !filepath.IsAbs(plan.OutputDirectory) ||
		filepath.Clean(plan.OutputDirectory) != plan.OutputDirectory {
		return ErrInvalidRequest
	}
	start, err := canonicalSigned(plan.StartNS)
	if err != nil || start != plan.StartNS {
		return ErrInvalidRequest
	}
	end, err := canonicalSigned(plan.EndNS)
	if err != nil || end != plan.EndNS {
		return ErrInvalidRequest
	}
	startNumber, endNumber := new(big.Int), new(big.Int)
	startNumber.SetString(start, 10)
	endNumber.SetString(end, 10)
	if startNumber.Cmp(endNumber) >= 0 {
		return ErrInvalidRequest
	}
	width, err := canonicalUnsigned(plan.SliceWidthNS)
	if err != nil || width != plan.SliceWidthNS || width == "0" {
		return ErrInvalidRequest
	}
	return nil
}

func validPlanText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 4096 &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func startScope(req StartRequest) string {
	return "EXPORT/" + req.ProfileID + "/" + req.ConnectionID + "/" + req.ClientRequestID
}

func commandScope(jobID, kind, commandID string) string {
	return "EXPORT/" + jobID + "/" + kind + "/" + commandID
}

func commandDigest(jobID, kind, target string, envelope CommandEnvelope) (string, error) {
	canonical := struct {
		SchemaVersion         int    `json:"schemaVersion"`
		Kind                  string `json:"kind"`
		JobID                 string `json:"jobId"`
		ExpectedStateRevision string `json:"expectedStateRevision"`
		Target                string `json:"target"`
	}{1, kind, jobID, envelope.ExpectedStateRevision, target}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalUnsigned(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") ||
		(len(value) > 1 && value[0] == '0') {
		return "", tasks.ErrInvalidDecimal
	}
	number := new(big.Int)
	if _, ok := number.SetString(value, 10); !ok {
		return "", tasks.ErrInvalidDecimal
	}
	return number.String(), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validState(state string) bool {
	switch state {
	case StateQueued, StateRunning, StateFinalizing, StatePausedRestartable,
		StateCleaning, StateSucceeded, StateCanceled, StateFailed:
		return true
	default:
		return false
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func requireOne(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return tasks.ErrRevisionConflict
	}
	return nil
}

func parseInt64Decimal(value string) (int64, error) {
	canonical, err := canonicalUnsigned(value)
	if err != nil {
		return 0, err
	}
	number := new(big.Int)
	number.SetString(canonical, 10)
	if !number.IsInt64() {
		return 0, tasks.ErrInvalidDecimal
	}
	return number.Int64(), nil
}

func sortFragments(fragments []Fragment) {
	sort.SliceStable(fragments, func(left, right int) bool {
		leftNumber := new(big.Int)
		rightNumber := new(big.Int)
		leftNumber.SetString(fragments[left].Ordinal, 10)
		rightNumber.SetString(fragments[right].Ordinal, 10)
		comparison := leftNumber.Cmp(rightNumber)
		if comparison == 0 {
			return fragments[left].FragmentID < fragments[right].FragmentID
		}
		return comparison < 0
	})
}
