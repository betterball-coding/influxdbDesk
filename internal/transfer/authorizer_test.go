package transfer

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

type blockingGrantReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingGrantReader) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	for index := range buffer {
		buffer[index] = 'r'
	}
	return len(buffer), nil
}

func newAuthorizedImport(t *testing.T) (*RunAuthorizer, *Repository, *protection.Manager, *store.Store, ImportJob) {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	taskRepository := tasks.NewRepository(database, time.Now)
	repository := NewRepository(database, taskRepository, nil, time.Now)
	job, _, err := repository.CreateImport(context.Background(), CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: "profile-1", ClientScope: "preflight/scope",
		RequestDigest: "preflight-digest", LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000",
			AdaptiveMaxBytes: "5242880", SourceSHA256: "source", StagingSHA256: "staging",
			NormalizationVersion: "1", SpecDigest: "spec", TargetDigest: "target",
		},
	})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	protectionManager := protection.NewManager(database, time.Now, strings.NewReader(strings.Repeat("g", 8192)))
	if _, err := protectionManager.Register(context.Background(), protection.RegisterRequest{
		ConnectionID: "connection-1", ConnectionGeneration: "1", ProfileID: "profile-1",
		ProfileRevision: "3", Mode: protection.ProtectedLocked,
	}); err != nil {
		t.Fatal(err)
	}
	authorizer := NewRunAuthorizer(repository, protectionManager, nil,
		"profile-1", "3", "connection-1", "1", time.Now)
	return authorizer, repository, protectionManager, database, job
}

func startAuthorizedImport(
	t *testing.T,
	authorizer *RunAuthorizer,
	manager *protection.Manager,
	job ImportJob,
) ImportJob {
	t.Helper()
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	commandID := uuid.NewString()
	preview, err := authorizer.Preview(context.Background(), PreviewRunRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
	})
	if err != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	started, replayed, err := authorizer.Start(context.Background(), AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if err != nil || replayed || started.ActiveRunSegmentID == nil {
		t.Fatalf("started=%+v replayed=%v err=%v", started, replayed, err)
	}
	return started
}

func TestImportGrantStartsOneSegmentAndLedgerDominatesReplay(t *testing.T) {
	authorizer, _, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	commandID := uuid.NewString()
	previewRequest := PreviewRunRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
	}
	locked, err := authorizer.Preview(context.Background(), previewRequest)
	if err != nil || locked.Executable || locked.ImportRunGrant != nil {
		t.Fatalf("locked preview=%+v err=%v", locked, err)
	}
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	preview, err := authorizer.Preview(context.Background(), previewRequest)
	if err != nil || !preview.Executable || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	request := AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	}
	started, replayed, err := authorizer.Start(context.Background(), request)
	if err != nil || replayed || started.Task.State != ImportRunning || started.ActiveRunSegmentID == nil {
		t.Fatalf("started=%+v replayed=%v err=%v", started, replayed, err)
	}
	retry, replayed, err := authorizer.Start(context.Background(), request)
	if err != nil || !replayed || retry.Task.ID != started.Task.ID || retry.ActiveRunSegmentID == nil || *retry.ActiveRunSegmentID != *started.ActiveRunSegmentID {
		t.Fatalf("retry=%+v replayed=%v err=%v", retry, replayed, err)
	}
}

func TestLockInvalidatesIssuedImportGrant(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	commandID := uuid.NewString()
	preview, err := authorizer.Preview(context.Background(), PreviewRunRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Lock(context.Background(), protection.LockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err = authorizer.Start(context.Background(), AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if !errors.Is(err, protection.ErrLocked) {
		t.Fatalf("expected locked grant rejection, got %v", err)
	}
	current, err := repository.GetImport(context.Background(), job.Task.ID)
	if err != nil || current.Task.State != ImportReady || current.ActiveRunSegmentID != nil {
		t.Fatalf("job changed after stale grant: %+v err=%v", current, err)
	}
}

func TestConcurrentLockWaitsForImportGrantIssuance(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	reader := &blockingGrantReader{started: make(chan struct{}), release: make(chan struct{})}
	authorizer.grants = NewGrantRegistry(time.Now, reader)
	commandID := uuid.NewString()
	previewDone := make(chan struct{})
	var preview ImportRunPreview
	var previewErr error
	go func() {
		preview, previewErr = authorizer.Preview(context.Background(), PreviewRunRequest{
			JobID: job.Task.ID, CommandRequestID: commandID,
			ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
		})
		close(previewDone)
	}()
	<-reader.started

	lockDone := make(chan struct{})
	var lockErr error
	go func() {
		_, lockErr = manager.Lock(context.Background(), protection.LockRequest{
			ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
			ExpectedConnectionGeneration: "1",
		})
		close(lockDone)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		probeCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _, err := manager.GrantBinding(probeCtx, "connection-1", "1")
		cancel()
		if errors.Is(err, protection.ErrLockPending) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Lock did not become pending while grant issuance held the gate")
		}
		runtime.Gosched()
	}
	select {
	case <-lockDone:
		t.Fatal("Lock returned before grant issuance completed")
	default:
	}
	close(reader.release)
	<-previewDone
	<-lockDone
	if previewErr != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, previewErr)
	}
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	_, _, err := authorizer.Start(context.Background(), AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if !errors.Is(err, protection.ErrLocked) && !errors.Is(err, protection.ErrBindingMismatch) {
		t.Fatalf("grant issued immediately before Lock remained executable: %v", err)
	}
	current, err := repository.GetImport(context.Background(), job.Task.ID)
	if err != nil || current.Task.State != ImportReady || current.ActiveRunSegmentID != nil {
		t.Fatalf("job changed after locked grant: %+v err=%v", current, err)
	}
}

func TestGrantConcurrentReservationHasOneOwner(t *testing.T) {
	now := time.Now().UTC()
	registry := NewGrantRegistry(func() time.Time { return now }, strings.NewReader(strings.Repeat("z", 2048)))
	binding := GrantBinding{
		JobID: "job", CommandRequestID: uuid.NewString(), Action: GrantStart,
		ExpectedStateRevision: "1", CheckpointDigest: "checkpoint", SourceSHA256: "source",
		StagingSHA256: "staging", NormalizationVersion: "1", SpecDigest: "spec",
		TargetDigest: "target", ProfileID: "profile", ProfileRevision: "1",
		ConnectionID: "connection", ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
	}
	grant, err := registry.Issue(binding, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var owners int
	var mutex sync.Mutex
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reservation, err := registry.Reserve(grant.Token, binding)
			if err == nil {
				mutex.Lock()
				owners++
				mutex.Unlock()
				_ = reservation.Commit()
			}
		}()
	}
	wait.Wait()
	if owners != 1 {
		t.Fatalf("reservation owners=%d", owners)
	}
}

func TestImportGrantRejectsCrossProfileWithoutPersistentSideEffects(t *testing.T) {
	authorizer, repository, manager, database, _ := newAuthorizedImport(t)
	defer database.Close()
	foreign, _, err := repository.CreateImport(context.Background(), CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: "profile-2", ClientScope: "preflight/profile-2/request-1",
		RequestDigest: "foreign-preflight-digest", LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000",
			AdaptiveMaxBytes: "5242880", SourceSHA256: "foreign-source", StagingSHA256: "foreign-staging",
			NormalizationVersion: "1", SpecDigest: "foreign-spec", TargetDigest: "foreign-target",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	request := PreviewRunRequest{
		JobID: foreign.Task.ID, CommandRequestID: uuid.NewString(),
		ExpectedStateRevision: foreign.Task.StateRevision, Action: GrantStart,
	}
	if _, err := authorizer.Preview(context.Background(), request); !errors.Is(err, ErrImportProfileMismatch) {
		t.Fatalf("cross-profile preview error=%v", err)
	}

	dispatchBinding, _, err := manager.GrantBinding(context.Background(), "connection-1", "1")
	if err != nil {
		t.Fatal(err)
	}
	binding := authorizer.binding(request, foreign, dispatchBinding)
	grant, err := authorizer.grants.Issue(binding, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := authorizer.Start(context.Background(), AuthorizedStartRequest{
		JobID: request.JobID, CommandRequestID: request.CommandRequestID,
		ExpectedStateRevision: request.ExpectedStateRevision, Action: request.Action,
		ImportRunGrant: grant.Token,
	}); !errors.Is(err, ErrImportProfileMismatch) {
		t.Fatalf("cross-profile start error=%v", err)
	}
	reservation, err := authorizer.grants.Reserve(grant.Token, binding)
	if err != nil {
		t.Fatalf("cross-profile rejection consumed grant: %v", err)
	}
	reservation.Rollback()

	segmentID := uuid.NewString()
	if _, _, err := repository.StartRun(context.Background(), StartRunRequest{
		JobID: foreign.Task.ID, ExpectedProfileID: "profile-1", RunSegmentID: segmentID,
		Kind: "NORMAL", CommandScope: "IMPORT/foreign/START/cmd",
		CommandDigest: "foreign-start-digest", ExpectedStateRevision: foreign.Task.StateRevision,
		GrantExpiresAt:   time.Now().UTC().Add(time.Minute),
		CommandExpiresAt: time.Now().UTC().Add(90 * 24 * time.Hour),
		Permit: Permit{
			RunSegmentID: segmentID, CurrentCheckpointDigest: foreign.CheckpointDigest,
			ProfileID: "profile-1", ProfileRevision: "3", ConnectionID: "connection-1",
			ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
		},
	}); !errors.Is(err, ErrImportProfileMismatch) {
		t.Fatalf("cross-profile StartRun error=%v", err)
	}

	current, err := repository.GetImport(context.Background(), foreign.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Task.State != ImportReady || current.Task.SnapshotRevision != "1" || current.ActiveRunSegmentID != nil {
		t.Fatalf("cross-profile attempt changed job: %+v", current)
	}
	var segments, commands, events int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM import_run_segments WHERE job_id=?", foreign.Task.ID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM command_ledger WHERE resource_id=?", foreign.Task.ID).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM task_events WHERE resource_id=?", foreign.Task.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if segments != 0 || commands != 0 || events != 1 {
		t.Fatalf("cross-profile side effects segments=%d commands=%d events=%d", segments, commands, events)
	}
}

func TestGrantReservationExpiresAtAuthorizationBoundary(t *testing.T) {
	now := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	registry := NewGrantRegistry(func() time.Time { return now }, strings.NewReader(strings.Repeat("t", 256)))
	binding := GrantBinding{
		JobID: "job", CommandRequestID: uuid.NewString(), Action: GrantStart,
		ExpectedStateRevision: "1", CheckpointDigest: "checkpoint", SourceSHA256: "source",
		StagingSHA256: "staging", NormalizationVersion: "1", SpecDigest: "spec",
		TargetDigest: "target", ProfileID: "profile", ProfileRevision: "1",
		ConnectionID: "connection", ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
	}
	expires := now.Add(time.Minute)
	grant, err := registry.Issue(binding, expires)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := registry.Reserve(grant.Token, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Rollback()
	now = expires.Add(-time.Nanosecond)
	if err := reservation.ValidateLive(); err != nil {
		t.Fatalf("grant expired before boundary: %v", err)
	}
	now = expires
	if err := reservation.ValidateLive(); !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("grant at expiry boundary error=%v", err)
	}
}

func TestImportAttemptCrossesRoundTripBarrierExactlyOnce(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	started := startAuthorizedImport(t, authorizer, manager, job)
	request := AttemptRequest{
		JobID: started.Task.ID, BatchID: "batch-1", AttemptID: uuid.NewString(),
		RunSegmentID: *started.ActiveRunSegmentID, ParentCheckpointDigest: started.CheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}
	var barriers atomic.Int32
	var observedJobLock atomic.Bool
	runLock := repository.jobLock(started.Task.ID)
	barrier := func(context.Context) error {
		barriers.Add(1)
		if runLock.TryLock() {
			runLock.Unlock()
		} else {
			observedJobLock.Store(true)
		}
		return nil
	}
	if err := authorizer.BeginAttempt(context.Background(), request, barrier); err != nil {
		t.Fatal(err)
	}
	if barriers.Load() != 1 {
		t.Fatalf("barrier calls=%d", barriers.Load())
	}
	if !observedJobLock.Load() {
		t.Fatal("job lock was released before the start barrier")
	}
	if !runLock.TryLock() {
		t.Fatal("job lock remains held after the start barrier")
	}
	runLock.Unlock()
	if err := authorizer.BeginAttempt(context.Background(), request, barrier); !errors.Is(err, ErrImportState) {
		t.Fatalf("duplicate attempt error=%v", err)
	}
	if barriers.Load() != 1 {
		t.Fatalf("duplicate attempt crossed barrier, calls=%d", barriers.Load())
	}
	var state string
	if err := database.DB().QueryRow("SELECT state FROM import_attempts WHERE attempt_id=?", request.AttemptID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "SENDING" {
		t.Fatalf("attempt state=%s", state)
	}
}

func TestResolveGrantCommitsChildAndLedgerReplayPrecedesLiveState(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	started := startAuthorizedImport(t, authorizer, manager, job)
	_, found := repository.permits.Get(started.Task.ID)
	if !found {
		t.Fatal("started import has no permit")
	}
	attemptID := uuid.NewString()
	if err := authorizer.BeginAttempt(context.Background(), AttemptRequest{
		JobID: started.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
		RunSegmentID: *started.ActiveRunSegmentID, ParentCheckpointDigest: started.CheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	incident, err := repository.RecordIncident(context.Background(), attemptID, uuid.NewString(), "UNKNOWN")
	if err != nil {
		t.Fatal(err)
	}
	if _, stillActive := repository.permits.Get(started.Task.ID); stillActive {
		t.Fatal("incident retained the original permit")
	}
	commandID := uuid.NewString()
	previewRequest := PreviewRunRequest{
		JobID: incident.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: incident.Task.StateRevision, Action: GrantResolve,
		Decision: DecisionAssumeCommitted, AfterResolution: ResolutionContinue,
	}
	preview, err := authorizer.Preview(context.Background(), previewRequest)
	if err != nil || !preview.Executable || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	request := AuthorizedResolveRequest{
		JobID: incident.Task.ID, IncidentID: incident.OpenIncident.IncidentID,
		ParentCheckpointDigest: incident.CheckpointDigest, CommandRequestID: commandID,
		ExpectedStateRevision: incident.Task.StateRevision, Decision: DecisionAssumeCommitted,
		AfterResolution: ResolutionContinue, ImportRunGrant: preview.ImportRunGrant.Token,
	}
	resolved, replayed, err := authorizer.Resolve(context.Background(), request)
	if err != nil || replayed || resolved.Task.State != ImportRunning || resolved.OpenIncident != nil ||
		resolved.Checkpoint.LastDisposition != "ASSUMED_COMMITTED" {
		t.Fatalf("resolved=%+v replayed=%v err=%v", resolved, replayed, err)
	}
	retry, replayed, err := authorizer.Resolve(context.Background(), request)
	if err != nil || !replayed || retry.CheckpointDigest != resolved.CheckpointDigest ||
		retry.Task.StateRevision != resolved.Task.StateRevision {
		t.Fatalf("retry=%+v replayed=%v err=%v", retry, replayed, err)
	}
	conflict := request
	conflict.ImportRunGrant = "different-token"
	if _, _, err := authorizer.Resolve(context.Background(), conflict); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
		t.Fatalf("same command id with different grant error=%v", err)
	}
}

func TestAuthorizedAbortReplayPrecedesAbortedLiveState(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	started := startAuthorizedImport(t, authorizer, manager, job)
	attemptID := uuid.NewString()
	if err := authorizer.BeginAttempt(context.Background(), AttemptRequest{
		JobID: started.Task.ID, BatchID: "batch-abort", AttemptID: attemptID,
		RunSegmentID: *started.ActiveRunSegmentID, ParentCheckpointDigest: started.CheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	incident, err := repository.RecordIncident(context.Background(), attemptID, uuid.NewString(), "PARTIAL")
	if err != nil {
		t.Fatal(err)
	}
	request := AuthorizedResolveRequest{
		JobID: incident.Task.ID, IncidentID: incident.OpenIncident.IncidentID,
		ParentCheckpointDigest: incident.CheckpointDigest,
		CommandRequestID:       uuid.NewString(), ExpectedStateRevision: incident.Task.StateRevision,
		Decision: DecisionAbort,
	}
	aborted, replayed, err := authorizer.Resolve(context.Background(), request)
	if err != nil || replayed || aborted.Task.State != ImportAborted {
		t.Fatalf("aborted=%+v replayed=%v err=%v", aborted, replayed, err)
	}
	retry, replayed, err := authorizer.Resolve(context.Background(), request)
	if err != nil || !replayed || retry.Task.State != ImportAborted ||
		retry.Task.StateRevision != aborted.Task.StateRevision {
		t.Fatalf("retry=%+v replayed=%v err=%v", retry, replayed, err)
	}
}

func TestLockAfterStartPreventsSendingAttempt(t *testing.T) {
	authorizer, repository, manager, database, job := newAuthorizedImport(t)
	defer database.Close()
	started := startAuthorizedImport(t, authorizer, manager, job)
	if _, err := manager.Lock(context.Background(), protection.LockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1",
	}); err != nil {
		t.Fatal(err)
	}
	var barriers atomic.Int32
	err := authorizer.BeginAttempt(context.Background(), AttemptRequest{
		JobID: started.Task.ID, BatchID: "batch-1", AttemptID: uuid.NewString(),
		RunSegmentID: *started.ActiveRunSegmentID, ParentCheckpointDigest: started.CheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, func(context.Context) error {
		barriers.Add(1)
		return nil
	})
	if !errors.Is(err, protection.ErrBindingMismatch) && !errors.Is(err, protection.ErrLocked) {
		t.Fatalf("attempt after Lock error=%v", err)
	}
	if barriers.Load() != 0 {
		t.Fatalf("attempt after Lock crossed %d barriers", barriers.Load())
	}
	var attempts int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM import_attempts WHERE job_id=?", started.Task.ID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("attempt after Lock persisted %d SENDING rows", attempts)
	}
	current, getErr := repository.GetImport(context.Background(), started.Task.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if current.Task.State != ImportPausedSafe || current.ActiveRunSegmentID != nil {
		t.Fatalf("invalid Permit was not paused after Lock: %+v", current)
	}
	if _, found := repository.permits.Get(started.Task.ID); found {
		t.Fatal("invalid Permit remains after Lock rejection")
	}
	var segmentState, stopReason string
	if err := database.DB().QueryRow(`SELECT state,stop_reason FROM import_run_segments
		WHERE job_id=?`, started.Task.ID).Scan(&segmentState, &stopReason); err != nil {
		t.Fatal(err)
	}
	if segmentState != "CLOSED" || stopReason != string(PermitInvalidatedBindingChanged) {
		t.Fatalf("segment state=%s reason=%s", segmentState, stopReason)
	}
}
