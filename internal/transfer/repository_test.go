package transfer

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

var repositoryTestNow = time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)

func newTransferRepository(t *testing.T) (*Repository, *store.Store) {
	t.Helper()
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	tasksRepo := tasks.NewRepository(s, func() time.Time { return repositoryTestNow })
	return NewRepository(s, tasksRepo, nil, func() time.Time { return repositoryTestNow }), s
}

func initialCheckpoint() Checkpoint {
	return Checkpoint{
		Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000",
		AdaptiveMaxBytes: "5242880", SourceSHA256: "source", StagingSHA256: "staging",
		NormalizationVersion: "lp-v1", SpecDigest: "spec", TargetDigest: "target",
	}
}

func createRunningImport(t *testing.T, repo *Repository) (ImportJob, string, Permit) {
	t.Helper()
	ctx := context.Background()
	job, _, err := repo.CreateImport(ctx, CreateImportRequest{
		JobID: "job-1", ProfileID: "profile-1", ClientScope: "preflight/profile-1/request-1",
		RequestDigest: "request-digest", LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: initialCheckpoint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	segmentID := uuid.NewString()
	permit := Permit{
		RunSegmentID: segmentID, CurrentCheckpointDigest: job.CheckpointDigest,
		ProfileID: "profile-1", ProfileRevision: "1", ConnectionID: "connection-1",
		ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
	}
	job, _, err = repo.StartRun(ctx, StartRunRequest{
		JobID: job.Task.ID, ExpectedProfileID: "profile-1", RunSegmentID: segmentID, Kind: "NORMAL",
		CommandScope: "IMPORT/job-1/START/cmd", CommandDigest: "start-digest",
		ExpectedStateRevision: "1", CommandExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		GrantExpiresAt: repositoryTestNow.Add(time.Minute),
		Permit:         permit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job, segmentID, permit
}

func beginAttemptForTest(repo *Repository, ctx context.Context, req AttemptRequest, profileID string, permit Permit) error {
	lock := repo.jobLock(req.JobID)
	lock.Lock()
	defer lock.Unlock()
	return repo.beginAttemptLocked(ctx, req, profileID, permit)
}

func TestCheckpointChainAdvancesPermitOnlyToDirectChild(t *testing.T) {
	t.Parallel()
	repo, s := newTransferRepository(t)
	defer s.Close()
	job, segmentID, permit := createRunningImport(t, repo)
	ctx := context.Background()
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repo, ctx, AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, "profile-1", permit); err != nil {
		t.Fatal(err)
	}
	settled, err := repo.SettleACK(ctx, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Checkpoint.ParentDigest != permit.CurrentCheckpointDigest || settled.Checkpoint.LogicalOffset != "120" {
		t.Fatalf("checkpoint=%+v", settled.Checkpoint)
	}
	active, ok := repo.permits.Get(job.Task.ID)
	if !ok || active.CurrentCheckpointDigest != settled.CheckpointDigest {
		t.Fatalf("permit=%+v ok=%v", active, ok)
	}
	var transitions int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM import_checkpoint_transitions WHERE job_id=?", job.Task.ID).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 1 {
		t.Fatalf("transitions=%d", transitions)
	}
}

func TestBeginAttemptRejectsNonContiguousOffsets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, start, end string
	}{
		{name: "gap", start: "1", end: "2"},
		{name: "empty", start: "0", end: "0"},
		{name: "reverse", start: "1", end: "0"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			repo, s := newTransferRepository(t)
			defer s.Close()
			job, segmentID, permit := createRunningImport(t, repo)
			err := beginAttemptForTest(repo, context.Background(), AttemptRequest{
				JobID: job.Task.ID, BatchID: "batch-1", AttemptID: uuid.NewString(),
				RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
				StartOffset: test.start, EndOffset: test.end, PayloadDigest: "payload",
			}, "profile-1", permit)
			if !errors.Is(err, ErrImportOffsetConflict) {
				t.Fatalf("offset [%s,%s) error=%v", test.start, test.end, err)
			}
			var attempts int
			if err := s.DB().QueryRow("SELECT COUNT(*) FROM import_attempts WHERE job_id=?", job.Task.ID).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			var state string
			var active sql.NullString
			if err := s.DB().QueryRow(`SELECT state,active_attempt_id FROM import_run_segments
				WHERE run_segment_id=?`, segmentID).Scan(&state, &active); err != nil {
				t.Fatal(err)
			}
			if attempts != 0 || state != "ACTIVE" || active.Valid {
				t.Fatalf("offset rejection side effects attempts=%d state=%s active=%v", attempts, state, active)
			}
		})
	}
}

func TestInvalidatePermitPausesSafely(t *testing.T) {
	t.Parallel()
	t.Run("without sending attempt", func(t *testing.T) {
		repo, s := newTransferRepository(t)
		defer s.Close()
		job, segmentID, permit := createRunningImport(t, repo)
		invalidated, err := repo.InvalidatePermits(context.Background(), permit.ConnectionID,
			permit.ConnectionGeneration, permit.ProtectionRevision, PermitInvalidatedProtectionLocked)
		if err != nil || invalidated != 1 {
			t.Fatalf("invalidated=%d err=%v", invalidated, err)
		}
		current, err := repo.GetImport(context.Background(), job.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Task.State != ImportPausedSafe || current.PauseRequested || current.ActiveRunSegmentID != nil {
			t.Fatalf("paused job=%+v", current)
		}
		if _, found := repo.permits.Get(job.Task.ID); found {
			t.Fatal("invalidated Permit remains active")
		}
		var state, reason string
		if err := s.DB().QueryRow(`SELECT state,stop_reason FROM import_run_segments
			WHERE run_segment_id=?`, segmentID).Scan(&state, &reason); err != nil {
			t.Fatal(err)
		}
		if state != "CLOSED" || reason != string(PermitInvalidatedProtectionLocked) {
			t.Fatalf("segment state=%s reason=%s", state, reason)
		}
	})

	t.Run("with sending attempt", func(t *testing.T) {
		repo, s := newTransferRepository(t)
		defer s.Close()
		job, segmentID, permit := createRunningImport(t, repo)
		attemptID := uuid.NewString()
		if err := beginAttemptForTest(repo, context.Background(), AttemptRequest{
			JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
			RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
			StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
		}, "profile-1", permit); err != nil {
			t.Fatal(err)
		}
		invalidated, err := repo.InvalidatePermits(context.Background(), permit.ConnectionID,
			permit.ConnectionGeneration, permit.ProtectionRevision, PermitInvalidatedConnectionClosed)
		if err != nil || invalidated != 1 {
			t.Fatalf("invalidated=%d err=%v", invalidated, err)
		}
		current, err := repo.GetImport(context.Background(), job.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Task.State != ImportRunning || !current.PauseRequested || current.ActiveRunSegmentID == nil ||
			*current.ActiveRunSegmentID != segmentID {
			t.Fatalf("stopping job=%+v", current)
		}
		if _, found := repo.permits.Get(job.Task.ID); found {
			t.Fatal("STOPPING segment retained invalid Permit")
		}
		var segmentState, reason, attemptState string
		if err := s.DB().QueryRow(`SELECT state,stop_reason FROM import_run_segments
			WHERE run_segment_id=?`, segmentID).Scan(&segmentState, &reason); err != nil {
			t.Fatal(err)
		}
		if err := s.DB().QueryRow("SELECT state FROM import_attempts WHERE attempt_id=?", attemptID).Scan(&attemptState); err != nil {
			t.Fatal(err)
		}
		if segmentState != "STOPPING" || reason != string(PermitInvalidatedConnectionClosed) || attemptState != "SENDING" {
			t.Fatalf("segment=%s reason=%s attempt=%s", segmentState, reason, attemptState)
		}
	})
}

func TestAbortClosesIncidentWithoutAdvancingCheckpoint(t *testing.T) {
	t.Parallel()
	repo, s := newTransferRepository(t)
	defer s.Close()
	job, segmentID, permit := createRunningImport(t, repo)
	ctx := context.Background()
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repo, ctx, AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, "profile-1", permit); err != nil {
		t.Fatal(err)
	}
	incidentID := uuid.NewString()
	incidentJob, err := repo.RecordIncident(ctx, attemptID, incidentID, "UNKNOWN")
	if err != nil {
		t.Fatal(err)
	}
	beforeDigest := incidentJob.CheckpointDigest
	commandID := uuid.NewString()
	aborted, replayed, err := repo.Abort(ctx, AbortRequest{
		JobID: job.Task.ID, IncidentID: incidentID, ParentCheckpointDigest: beforeDigest,
		ExpectedStateRevision: incidentJob.Task.StateRevision, CommandRequestID: commandID,
	})
	if err != nil || replayed {
		t.Fatalf("abort replayed=%v err=%v", replayed, err)
	}
	if aborted.Task.State != ImportAborted || aborted.CheckpointDigest != beforeDigest || aborted.OpenIncident != nil {
		t.Fatalf("aborted=%+v", aborted)
	}
	var incidentStatus, resolution string
	if err := s.DB().QueryRow(`SELECT status,resolution FROM import_incidents
		WHERE incident_id=?`, incidentID).Scan(&incidentStatus, &resolution); err != nil {
		t.Fatal(err)
	}
	if incidentStatus != "ABORTED" || resolution != "ABORT" {
		t.Fatalf("incident status=%s resolution=%s", incidentStatus, resolution)
	}
	var transitions int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM import_checkpoint_transitions WHERE job_id=?", job.Task.ID).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 0 {
		t.Fatalf("ABORT created %d transitions", transitions)
	}
	if err := repo.CanCleanup(ctx, job.Task.ID); err != nil {
		t.Fatalf("cleanup remains blocked: %v", err)
	}
	replayedJob, replayed, err := repo.Abort(ctx, AbortRequest{
		JobID: job.Task.ID, IncidentID: incidentID, ParentCheckpointDigest: beforeDigest,
		ExpectedStateRevision: incidentJob.Task.StateRevision, CommandRequestID: commandID,
	})
	if err != nil || !replayed || replayedJob.Task.State != ImportAborted {
		t.Fatalf("abort replay job=%+v replayed=%v err=%v", replayedJob, replayed, err)
	}
	if !reflect.DeepEqual(aborted, replayedJob) {
		t.Fatalf("abort replay snapshot differs\nfirst:  %+v\nreplay: %+v", aborted, replayedJob)
	}
}

func TestStartRunRejectsGrantAtExpiryWithoutSideEffects(t *testing.T) {
	t.Parallel()
	repo, s := newTransferRepository(t)
	defer s.Close()
	ctx := context.Background()
	job, _, err := repo.CreateImport(ctx, CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: "profile-1", ClientScope: "preflight/expiry/request-1",
		RequestDigest: "expiry-request-digest", LedgerExpiresAt: repositoryTestNow.Add(90 * 24 * time.Hour),
		InitialCheckpoint: initialCheckpoint(),
	})
	if err != nil {
		t.Fatal(err)
	}
	segmentID := uuid.NewString()
	_, _, err = repo.StartRun(ctx, StartRunRequest{
		JobID: job.Task.ID, ExpectedProfileID: job.ProfileID, RunSegmentID: segmentID, Kind: "NORMAL",
		CommandScope: "IMPORT/expiry/START/cmd", CommandDigest: "expiry-start-digest",
		ExpectedStateRevision: job.Task.StateRevision, GrantExpiresAt: repositoryTestNow,
		CommandExpiresAt: repositoryTestNow.Add(90 * 24 * time.Hour),
		Permit: Permit{
			RunSegmentID: segmentID, CurrentCheckpointDigest: job.CheckpointDigest,
			ProfileID: job.ProfileID, ProfileRevision: "1", ConnectionID: "connection-1",
			ConnectionGeneration: "1", ProtectionRevision: "2", LeaseID: "lease",
		},
	})
	if !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("StartRun at grant expiry error=%v", err)
	}
	current, err := repo.GetImport(ctx, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Task.State != ImportReady || current.Task.SnapshotRevision != "1" || current.ActiveRunSegmentID != nil {
		t.Fatalf("expired grant changed job: %+v", current)
	}
	var segments, commands int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM import_run_segments WHERE job_id=?", job.Task.ID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM command_ledger WHERE resource_id=?", job.Task.ID).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	if segments != 0 || commands != 0 {
		t.Fatalf("expired grant side effects segments=%d commands=%d", segments, commands)
	}
}

func TestRetainedProfileLimitAndReservationFloor(t *testing.T) {
	t.Parallel()
	repo, s := newTransferRepository(t)
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < RetainedTransferJobsPerProfile; i++ {
		if err := repo.quota.Admit(ctx, uuid.NewString(), "profile", "EXPORT", "QUEUED"); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.quota.Admit(ctx, uuid.NewString(), "profile", "IMPORT", ImportReady); !errors.Is(err, ErrTransferProfileLimit) {
		t.Fatalf("17th retained job error=%v", err)
	}
	jobID := uuid.NewString()
	if err := repo.quota.Admit(ctx, jobID, "other-profile", "EXPORT", "QUEUED"); err != nil {
		t.Fatal(err)
	}
	capacity := int64(100 * 1024 * 1024 * 1024)
	free := VolumeReserveFloor(capacity) + ReservationExtentBytes - 1
	if _, err := repo.quota.ReserveExtent(ctx, jobID, "VOLUME", "volume-1", ReservationExtentBytes, free, capacity); !errors.Is(err, ErrTargetLowSpace) {
		t.Fatalf("low target space error=%v", err)
	}
}

func TestRecoverSendingAttemptCreatesUnknownIncident(t *testing.T) {
	t.Parallel()
	repo, s := newTransferRepository(t)
	defer s.Close()
	job, segmentID, permit := createRunningImport(t, repo)
	ctx := context.Background()
	attemptID := uuid.NewString()
	if err := beginAttemptForTest(repo, ctx, AttemptRequest{
		JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
		RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
		StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
	}, "profile-1", permit); err != nil {
		t.Fatal(err)
	}
	restarted := NewRepository(s, tasks.NewRepository(s, nil), NewPermitRegistry(), nil)
	changed, err := restarted.RecoverImports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed=%d", changed)
	}
	recovered, err := restarted.GetImport(ctx, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Task.State != ImportNeedsUnknownDecision || recovered.OpenIncident == nil ||
		recovered.OpenIncident.Kind != "UNKNOWN" || recovered.CheckpointDigest != permit.CurrentCheckpointDigest {
		t.Fatalf("recovered=%+v", recovered)
	}
	if _, exists := restarted.permits.Get(job.Task.ID); exists {
		t.Fatal("permit survived restart recovery")
	}
}
