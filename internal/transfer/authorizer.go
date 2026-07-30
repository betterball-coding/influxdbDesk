package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/protection"
)

type PreviewRunRequest struct {
	JobID                 string                `json:"jobId"`
	CommandRequestID      string                `json:"commandRequestId"`
	ExpectedStateRevision string                `json:"expectedStateRevision"`
	Action                GrantAction           `json:"action"`
	Decision              IncidentDecision      `json:"decision,omitempty"`
	AfterResolution       ResolutionDisposition `json:"afterResolution,omitempty"`
}

type ImportRunPreview struct {
	JobID             string          `json:"jobId"`
	State             string          `json:"state"`
	CheckpointDigest  string          `json:"checkpointDigest"`
	LogicalOffset     string          `json:"logicalOffset"`
	AdaptiveMaxPoints string          `json:"adaptiveMaxPoints"`
	AdaptiveMaxBytes  string          `json:"adaptiveMaxBytes"`
	TargetDigest      string          `json:"targetDigest"`
	Executable        bool            `json:"executable"`
	ImportRunGrant    *ImportRunGrant `json:"importRunGrant,omitempty"`
}

type AuthorizedStartRequest struct {
	JobID                 string      `json:"jobId"`
	CommandRequestID      string      `json:"commandRequestId"`
	ExpectedStateRevision string      `json:"expectedStateRevision"`
	Action                GrantAction `json:"action"`
	ImportRunGrant        string      `json:"importRunGrant"`
}

type AuthorizedResolveRequest struct {
	JobID                  string                `json:"jobId"`
	IncidentID             string                `json:"incidentId"`
	ParentCheckpointDigest string                `json:"parentCheckpointDigest"`
	CommandRequestID       string                `json:"commandRequestId"`
	ExpectedStateRevision  string                `json:"expectedStateRevision"`
	Decision               IncidentDecision      `json:"decision"`
	AfterResolution        ResolutionDisposition `json:"afterResolution,omitempty"`
	ImportRunGrant         string                `json:"importRunGrant,omitempty"`
}

type RunAuthorizer struct {
	repository *Repository
	protection *protection.Manager
	grants     *GrantRegistry

	profileID, profileRevision string
	connectionID, generation   string
	now                        func() time.Time
}

func NewRunAuthorizer(
	repository *Repository,
	protectionManager *protection.Manager,
	grants *GrantRegistry,
	profileID, profileRevision, connectionID, generation string,
	now func() time.Time,
) *RunAuthorizer {
	if grants == nil {
		grants = NewGrantRegistry(now, nil)
	}
	if now == nil {
		now = time.Now
	}
	return &RunAuthorizer{
		repository: repository, protection: protectionManager, grants: grants,
		profileID: profileID, profileRevision: profileRevision,
		connectionID: connectionID, generation: generation, now: now,
	}
}

func (a *RunAuthorizer) Preview(ctx context.Context, request PreviewRunRequest) (ImportRunPreview, error) {
	if _, err := uuid.Parse(request.CommandRequestID); err != nil {
		return ImportRunPreview{}, ErrGrantMismatch
	}
	job, err := a.repository.GetImport(ctx, request.JobID)
	if err != nil {
		return ImportRunPreview{}, err
	}
	if job.ProfileID != a.profileID {
		return ImportRunPreview{}, ErrImportProfileMismatch
	}
	if job.Task.StateRevision != request.ExpectedStateRevision || !actionAllowed(request, job) {
		return ImportRunPreview{}, ErrImportState
	}
	preview := ImportRunPreview{
		JobID: job.Task.ID, State: job.Task.State, CheckpointDigest: job.CheckpointDigest,
		LogicalOffset: job.Checkpoint.LogicalOffset, AdaptiveMaxPoints: job.Checkpoint.AdaptiveMaxPoints,
		AdaptiveMaxBytes: job.Checkpoint.AdaptiveMaxBytes, TargetDigest: job.Checkpoint.TargetDigest,
	}
	if request.Action == GrantResolve {
		needsGrant, err := resolutionAllowed(job, request.Decision, request.AfterResolution)
		if err != nil {
			return ImportRunPreview{}, err
		}
		if !needsGrant {
			err := a.protection.WithGenerationGate(ctx, a.connectionID, a.generation,
				func(context.Context) error {
					preview.Executable = true
					return nil
				})
			return preview, err
		}
	}
	var grant ImportRunGrant
	_, err = a.protection.WithGrantBinding(ctx, a.connectionID, a.generation,
		func(dispatchBinding protection.DispatchBinding, snapshot protection.Snapshot) error {
			if dispatchBinding.ProfileRevision != a.profileRevision || snapshot.UnlockedUntil == nil {
				return protection.ErrBindingMismatch
			}
			binding := a.binding(request, job, dispatchBinding)
			expires := a.now().UTC().Add(120 * time.Second)
			leaseExpiry, err := time.Parse(time.RFC3339Nano, *snapshot.UnlockedUntil)
			if err != nil {
				return protection.ErrBindingMismatch
			}
			if leaseExpiry.Before(expires) {
				expires = leaseExpiry
			}
			grant, err = a.grants.Issue(binding, expires)
			return err
		})
	if err != nil {
		if errors.Is(err, protection.ErrLocked) || errors.Is(err, protection.ErrLeaseExpired) {
			return preview, nil
		}
		return ImportRunPreview{}, err
	}
	preview.Executable = true
	preview.ImportRunGrant = &grant
	return preview, nil
}

func (a *RunAuthorizer) Start(ctx context.Context, request AuthorizedStartRequest) (ImportJob, bool, error) {
	if _, err := uuid.Parse(request.CommandRequestID); err != nil || request.ImportRunGrant == "" {
		return ImportJob{}, false, ErrGrantMismatch
	}
	scope := commandScopeForRun(request)
	digest := commandDigestForRun(request)
	if job, found, err := a.repository.ReplayCommand(ctx, scope, digest, request.JobID); err != nil || found {
		if err == nil && job.ProfileID != a.profileID {
			return ImportJob{}, found, ErrImportProfileMismatch
		}
		return job, found, err
	}
	job, err := a.repository.GetImport(ctx, request.JobID)
	if err != nil {
		return ImportJob{}, false, err
	}
	if job.ProfileID != a.profileID {
		return ImportJob{}, false, ErrImportProfileMismatch
	}
	previewRequest := PreviewRunRequest{
		JobID: request.JobID, CommandRequestID: request.CommandRequestID,
		ExpectedStateRevision: request.ExpectedStateRevision, Action: request.Action,
	}
	if job.Task.StateRevision != request.ExpectedStateRevision || !actionAllowed(previewRequest, job) {
		return ImportJob{}, false, ErrImportState
	}
	dispatchBinding, _, err := a.protection.GrantBinding(ctx, a.connectionID, a.generation)
	if err != nil {
		return ImportJob{}, false, err
	}
	binding := a.binding(previewRequest, job, dispatchBinding)
	reservation, err := a.grants.Reserve(request.ImportRunGrant, binding)
	if err != nil {
		return ImportJob{}, false, err
	}
	defer reservation.Rollback()
	runSegmentID := uuid.NewString()
	permit := Permit{
		RunSegmentID: runSegmentID, CurrentCheckpointDigest: job.CheckpointDigest,
		ProfileID: binding.ProfileID, ProfileRevision: binding.ProfileRevision,
		ConnectionID:         binding.ConnectionID,
		ConnectionGeneration: binding.ConnectionGeneration,
		ProtectionRevision:   binding.ProtectionRevision, LeaseID: binding.LeaseID,
	}
	var out ImportJob
	var replayed bool
	err = a.protection.WithAuthorizedGate(ctx, ProtectionBinding(binding), func(ctx context.Context) error {
		if err := reservation.ValidateLive(); err != nil {
			return err
		}
		var err error
		out, replayed, err = a.repository.StartRun(ctx, StartRunRequest{
			JobID: request.JobID, ExpectedProfileID: a.profileID,
			RunSegmentID: runSegmentID, Kind: "NORMAL",
			CommandScope: scope, CommandDigest: digest,
			ExpectedStateRevision: request.ExpectedStateRevision,
			GrantExpiresAt:        reservation.ExpiresAt(),
			CommandExpiresAt:      a.now().UTC().Add(90 * 24 * time.Hour), Permit: permit,
		})
		return err
	})
	if err != nil {
		if committed, found, replayErr := a.repository.ReplayCommand(ctx, scope, digest, request.JobID); found {
			if replayErr == nil {
				_ = reservation.Commit()
			}
			return committed, true, replayErr
		}
		return ImportJob{}, false, err
	}
	if err := reservation.Commit(); err != nil {
		return ImportJob{}, false, err
	}
	return out, replayed, nil
}

// Resolve applies an incident decision through the same grant reservation,
// protection gate and command-ledger-first replay ordering as Start/Resume.
func (a *RunAuthorizer) Resolve(ctx context.Context, request AuthorizedResolveRequest) (ImportJob, bool, error) {
	if _, err := uuid.Parse(request.CommandRequestID); err != nil {
		return ImportJob{}, false, ErrGrantMismatch
	}
	commit := IncidentResolutionCommit{
		JobID: request.JobID, IncidentID: request.IncidentID,
		ParentCheckpointDigest: request.ParentCheckpointDigest,
		ExpectedStateRevision:  request.ExpectedStateRevision,
		CommandRequestID:       request.CommandRequestID, Decision: request.Decision,
		AfterResolution: request.AfterResolution,
	}
	if request.ImportRunGrant != "" {
		tokenHash := sha256.Sum256([]byte(request.ImportRunGrant))
		commit.GrantTokenHash = hex.EncodeToString(tokenHash[:])
	}
	if request.Decision == DecisionAbort {
		abort := AbortRequest{
			JobID: request.JobID, IncidentID: request.IncidentID,
			ParentCheckpointDigest: request.ParentCheckpointDigest,
			ExpectedStateRevision:  request.ExpectedStateRevision,
			CommandRequestID:       request.CommandRequestID,
		}
		if replay, found, err := a.repository.ReplayCommand(ctx, AbortCommandScope(abort),
			AbortCommandDigest(abort), request.JobID); err != nil || found {
			if err == nil && replay.ProfileID != a.profileID {
				return ImportJob{}, found, ErrImportProfileMismatch
			}
			return replay, found, err
		}
	} else {
		scope, digest := ResolutionCommandScope(commit), ResolutionCommandDigest(commit)
		if replay, found, err := a.repository.ReplayCommand(ctx, scope, digest, request.JobID); err != nil || found {
			if err == nil && replay.ProfileID != a.profileID {
				return ImportJob{}, found, ErrImportProfileMismatch
			}
			return replay, found, err
		}
	}
	job, err := a.repository.GetImport(ctx, request.JobID)
	if err != nil {
		return ImportJob{}, false, err
	}
	if job.ProfileID != a.profileID {
		return ImportJob{}, false, ErrImportProfileMismatch
	}
	if job.Task.StateRevision != request.ExpectedStateRevision || job.CheckpointDigest != request.ParentCheckpointDigest ||
		job.OpenIncident == nil || job.OpenIncident.IncidentID != request.IncidentID {
		return ImportJob{}, false, ErrImportState
	}
	needsGrant, err := resolutionAllowed(job, request.Decision, request.AfterResolution)
	if err != nil {
		return ImportJob{}, false, err
	}

	if request.Decision == DecisionAbort {
		var out ImportJob
		var replayed bool
		err := a.protection.WithGenerationGate(ctx, a.connectionID, a.generation, func(ctx context.Context) error {
			var err error
			out, replayed, err = a.repository.Abort(ctx, AbortRequest{
				JobID: request.JobID, IncidentID: request.IncidentID,
				ParentCheckpointDigest: request.ParentCheckpointDigest,
				ExpectedStateRevision:  request.ExpectedStateRevision,
				CommandRequestID:       request.CommandRequestID,
			})
			return err
		})
		return out, replayed, err
	}

	scope, digest := ResolutionCommandScope(commit), ResolutionCommandDigest(commit)

	if !needsGrant {
		var out ImportJob
		var replayed bool
		err := a.protection.WithGenerationGate(ctx, a.connectionID, a.generation, func(ctx context.Context) error {
			var err error
			out, replayed, err = a.repository.ResolveIncident(ctx, commit)
			return err
		})
		return out, replayed, err
	}
	if request.ImportRunGrant == "" {
		return ImportJob{}, false, ErrGrantMismatch
	}

	dispatchBinding, _, err := a.protection.GrantBinding(ctx, a.connectionID, a.generation)
	if err != nil {
		return ImportJob{}, false, err
	}
	previewRequest := PreviewRunRequest{
		JobID: request.JobID, CommandRequestID: request.CommandRequestID,
		ExpectedStateRevision: request.ExpectedStateRevision, Action: GrantResolve,
		Decision: request.Decision, AfterResolution: request.AfterResolution,
	}
	binding := a.binding(previewRequest, job, dispatchBinding)
	reservation, err := a.grants.Reserve(request.ImportRunGrant, binding)
	if err != nil {
		return ImportJob{}, false, err
	}
	defer reservation.Rollback()
	segmentID := uuid.NewString()
	permit := Permit{
		RunSegmentID: segmentID, CurrentCheckpointDigest: job.CheckpointDigest,
		ProfileID: binding.ProfileID, ProfileRevision: binding.ProfileRevision,
		ConnectionID: binding.ConnectionID, ConnectionGeneration: binding.ConnectionGeneration,
		ProtectionRevision: binding.ProtectionRevision, LeaseID: binding.LeaseID,
	}
	commit.RunSegmentID = segmentID
	commit.Permit = &permit

	var out ImportJob
	var replayed bool
	err = a.protection.WithAuthorizedGate(ctx, ProtectionBinding(binding), func(ctx context.Context) error {
		if err := reservation.ValidateLive(); err != nil {
			return err
		}
		var err error
		out, replayed, err = a.repository.ResolveIncident(ctx, commit)
		return err
	})
	if err != nil {
		if committed, found, replayErr := a.repository.ReplayCommand(ctx, scope, digest, request.JobID); found {
			if replayErr == nil {
				_ = reservation.Commit()
			}
			return committed, true, replayErr
		}
		return ImportJob{}, false, err
	}
	if err := reservation.Commit(); err != nil {
		return ImportJob{}, false, err
	}
	return out, replayed, nil
}

// BeginAttempt is the only production entry point for an Import batch start.
// beginRoundTrip must be the direct Dispatcher barrier and must return only
// after the underlying transport's unique RoundTrip call has been entered.
func (a *RunAuthorizer) BeginAttempt(
	ctx context.Context,
	request AttemptRequest,
	beginRoundTrip func(context.Context) error,
) error {
	if beginRoundTrip == nil {
		return ErrRoundTripBarrier
	}
	permit, ok := a.repository.permits.Get(request.JobID)
	if !ok || request.RunSegmentID != permit.RunSegmentID ||
		request.ParentCheckpointDigest != permit.CurrentCheckpointDigest {
		return ErrPermitMismatch
	}
	if permit.ProfileID != a.profileID {
		return ErrImportProfileMismatch
	}
	if permit.ProfileRevision != a.profileRevision || permit.ConnectionID != a.connectionID ||
		permit.ConnectionGeneration != a.generation {
		return protection.ErrBindingMismatch
	}
	binding := protection.DispatchBinding{
		ConnectionID: permit.ConnectionID, ConnectionGeneration: permit.ConnectionGeneration,
		ProfileRevision: permit.ProfileRevision, ProtectionRevision: permit.ProtectionRevision,
		LeaseID: permit.LeaseID,
	}
	runLock := a.repository.jobLock(request.JobID)
	locked := false
	unlockRun := func() {
		if locked {
			locked = false
			runLock.Unlock()
		}
	}
	err := a.protection.BeginMutationDispatch(ctx, binding,
		func(ctx context.Context) (persistErr error) {
			runLock.Lock()
			locked = true
			keepLocked := false
			defer func() {
				if !keepLocked {
					unlockRun()
				}
			}()
			if err := a.repository.beginAttemptLocked(ctx, request, a.profileID, permit); err != nil {
				return err
			}
			keepLocked = true
			return nil
		},
		func(ctx context.Context) error {
			defer unlockRun()
			return beginRoundTrip(ctx)
		},
	)
	// Defensive for future changes to BeginMutationDispatch: never retain the
	// job lock if a persist callback succeeded but the barrier was skipped.
	unlockRun()
	if err != nil {
		return a.invalidateAfterProtectionError(err, permit)
	}
	return nil
}

func (a *RunAuthorizer) invalidateAfterProtectionError(err error, permit Permit) error {
	reason, invalidate := permitInvalidationReasonFor(err)
	if !invalidate {
		return err
	}
	_, invalidateErr := a.repository.InvalidatePermits(context.Background(), permit.ConnectionID,
		permit.ConnectionGeneration, permit.ProtectionRevision, reason)
	if invalidateErr != nil {
		return errors.Join(err, invalidateErr)
	}
	return err
}

func permitInvalidationReasonFor(err error) (PermitInvalidationReason, bool) {
	switch {
	case errors.Is(err, protection.ErrLeaseExpired):
		return PermitInvalidatedLeaseExpired, true
	case errors.Is(err, protection.ErrConnectionClosed):
		return PermitInvalidatedConnectionClosed, true
	case errors.Is(err, protection.ErrBindingMismatch):
		return PermitInvalidatedBindingChanged, true
	case errors.Is(err, protection.ErrLocked):
		return PermitInvalidatedProtectionLocked, true
	default:
		return "", false
	}
}

func (a *RunAuthorizer) binding(request PreviewRunRequest, job ImportJob, dispatch protection.DispatchBinding) GrantBinding {
	binding := GrantBinding{
		JobID: request.JobID, CommandRequestID: request.CommandRequestID,
		Action: request.Action, ExpectedStateRevision: request.ExpectedStateRevision,
		Decision: string(request.Decision), AfterResolution: string(request.AfterResolution),
		CheckpointDigest: job.CheckpointDigest, SourceSHA256: job.Checkpoint.SourceSHA256,
		StagingSHA256:        job.Checkpoint.StagingSHA256,
		NormalizationVersion: job.Checkpoint.NormalizationVersion,
		SpecDigest:           job.Checkpoint.SpecDigest, TargetDigest: job.Checkpoint.TargetDigest,
		ProfileID: a.profileID, ProfileRevision: a.profileRevision,
		ConnectionID: a.connectionID, ConnectionGeneration: dispatch.ConnectionGeneration,
		ProtectionRevision: dispatch.ProtectionRevision, LeaseID: dispatch.LeaseID,
	}
	if request.Action == GrantResolve && job.OpenIncident != nil {
		binding.IncidentDigest = IncidentDigest(*job.OpenIncident)
	}
	return binding
}

func actionAllowed(request PreviewRunRequest, job ImportJob) bool {
	if request.Action == GrantStart {
		return job.Task.State == ImportReady && request.Decision == "" && request.AfterResolution == ""
	}
	if request.Action == GrantResume {
		return job.Task.State == ImportPausedSafe && request.Decision == "" && request.AfterResolution == ""
	}
	if request.Action != GrantResolve {
		return false
	}
	_, err := resolutionAllowed(job, request.Decision, request.AfterResolution)
	return err == nil
}

func resolutionAllowed(job ImportJob, decision IncidentDecision, after ResolutionDisposition) (bool, error) {
	if job.OpenIncident == nil || job.OpenIncident.Status != "OPEN" {
		return false, ErrOpenIncident
	}
	switch decision {
	case DecisionReplayExact:
		if after != ResolutionContinue && after != ResolutionPause {
			return false, ErrImportState
		}
		if job.Task.State != ImportNeedsPartialDecision && job.Task.State != ImportNeedsUnknownDecision {
			return false, ErrImportState
		}
		return true, nil
	case DecisionAssumeCommitted:
		if job.Task.State != ImportNeedsUnknownDecision || job.OpenIncident.Kind != "UNKNOWN" {
			return false, ErrImportState
		}
	case DecisionAcceptPartial:
		if job.Task.State != ImportNeedsPartialDecision || job.OpenIncident.Kind != "PARTIAL" {
			return false, ErrImportState
		}
	case DecisionAbort:
		if after != "" || (job.Task.State != ImportNeedsPartialDecision && job.Task.State != ImportNeedsUnknownDecision) {
			return false, ErrImportState
		}
		return false, nil
	default:
		return false, ErrImportState
	}
	if after != ResolutionContinue && after != ResolutionPause {
		return false, ErrImportState
	}
	return after == ResolutionContinue, nil
}

func commandScopeForRun(request AuthorizedStartRequest) string {
	return "IMPORT\x1f" + request.JobID + "\x1f" + string(request.Action) + "\x1f" + request.CommandRequestID
}

func commandDigestForRun(request AuthorizedStartRequest) string {
	tokenHash := sha256.Sum256([]byte(request.ImportRunGrant))
	canonical := "ImportRunCommandV1\n" + request.JobID + "\n" + request.CommandRequestID + "\n" +
		string(request.Action) + "\n" + request.ExpectedStateRevision + "\n" + hex.EncodeToString(tokenHash[:])
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}
