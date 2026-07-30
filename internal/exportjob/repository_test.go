package exportjob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

var exportTestNow = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

func TestStartChecksIdempotencyBeforeAdmissionAndTargetReservation(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	ctx := context.Background()
	req := testStartRequest("profile-1")

	created, replayed, err := repo.Start(ctx, req)
	if err != nil || replayed {
		t.Fatalf("Start() job=%+v replayed=%v error=%v", created, replayed, err)
	}
	if created.Task.State != StateQueued || !created.Retained || created.TargetReservationID == nil {
		t.Fatalf("created job = %+v", created)
	}
	var scopeKind, scopeID, reserved string
	if err := database.DB().QueryRowContext(ctx, `SELECT scope_kind,scope_id,reserved_bytes
		FROM transfer_reservations WHERE reservation_id=?`, *created.TargetReservationID).Scan(
		&scopeKind, &scopeID, &reserved); err != nil {
		t.Fatal(err)
	}
	if scopeKind != "VOLUME" || scopeID != req.TargetVolumeID || reserved != "67108864" {
		t.Fatalf("target reservation kind=%s scope=%s bytes=%s", scopeKind, scopeID, reserved)
	}

	// Replay ignores current quota/space observations and returns the original
	// durable resource before attempting a second admission.
	replayReq := req
	replayReq.TargetVolumeFreeBytes = 0
	replayedJob, replayed, err := repo.Start(ctx, replayReq)
	if err != nil || !replayed || replayedJob.Task.ID != created.Task.ID {
		t.Fatalf("replay job=%+v replayed=%v error=%v", replayedJob, replayed, err)
	}

	conflict := replayReq
	conflict.JobID = uuid.NewString()
	conflict.RequestDigest = digestText("different canonical export request")
	if _, _, err := repo.Start(ctx, conflict); !errors.Is(err, tasks.ErrIdempotencyConflict) {
		t.Fatalf("different digest error = %v", err)
	}

	lowSpace := testStartRequest("profile-2")
	lowSpace.TargetVolumeFreeBytes = transfer.VolumeReserveFloor(lowSpace.TargetVolumeCapacity) +
		lowSpace.TargetReservationBytes - 1
	if _, _, err := repo.Start(ctx, lowSpace); !errors.Is(err, transfer.ErrTargetLowSpace) {
		t.Fatalf("low-space Start() error = %v", err)
	}
	assertCount(t, database, "tasks", 1)
	assertCount(t, database, "transfer_jobs", 1)
	assertCount(t, database, "transfer_reservations", 1)
	assertCount(t, database, "idempotency_ledger", 1)

	got, err := repo.Get(ctx, created.Task.ID)
	if err != nil || got.Task.ID != created.Task.ID {
		t.Fatalf("Get() job=%+v error=%v", got, err)
	}
	listed, err := repo.List(ctx, ListFilter{ProfileID: req.ProfileID})
	if err != nil || len(listed) != 1 || listed[0].Task.ID != created.Task.ID {
		t.Fatalf("List() jobs=%+v error=%v", listed, err)
	}
}

func TestRetainedAdmissionIsAtomicAtProfileAndGlobalBoundaries(t *testing.T) {
	t.Run("per profile 16", func(t *testing.T) {
		repo, database, _ := newExportRepository(t)
		ctx := context.Background()
		for index := 0; index < transfer.RetainedTransferJobsPerProfile-1; index++ {
			request := testStartRequest("profile-limit")
			request.ConnectionID = "profile-limit-connection-" + strconv.Itoa(index/8)
			mustStart(t, repo, request)
		}

		requests := []StartRequest{testStartRequest("profile-limit"), testStartRequest("profile-limit")}
		requests[0].ConnectionID = "profile-limit-boundary"
		requests[1].ConnectionID = "profile-limit-boundary"
		errorsSeen := concurrentStarts(ctx, repo, requests)
		if countMatching(errorsSeen, nil) != 1 || countMatching(errorsSeen, transfer.ErrTransferProfileLimit) != 1 {
			t.Fatalf("boundary errors = %v", errorsSeen)
		}
		assertCount(t, database, "transfer_jobs", transfer.RetainedTransferJobsPerProfile)
	})

	t.Run("global 64", func(t *testing.T) {
		repo, database, _ := newExportRepository(t)
		ctx := context.Background()
		var replayRequest StartRequest
		for index := 0; index < transfer.RetainedTransferJobsGlobal-1; index++ {
			profile := "profile-global-" + string(rune('A'+index/16))
			request := testStartRequest(profile)
			request.ConnectionID = "global-connection-" + strconv.Itoa(index/8)
			if index == 0 {
				replayRequest = request
			}
			mustStart(t, repo, request)
		}

		first := testStartRequest("profile-extra-1")
		second := testStartRequest("profile-extra-2")
		first.ConnectionID = "global-boundary-1"
		second.ConnectionID = "global-boundary-2"
		errorsSeen := concurrentStarts(ctx, repo, []StartRequest{first, second})
		if countMatching(errorsSeen, nil) != 1 || countMatching(errorsSeen, transfer.ErrTransferGlobalLimit) != 1 {
			t.Fatalf("global boundary errors = %v", errorsSeen)
		}
		assertCount(t, database, "transfer_jobs", transfer.RetainedTransferJobsGlobal)

		// A replay remains available even after the global cap is reached.
		replayRequest.TargetVolumeFreeBytes = 0
		replayedJob, replayed, err := repo.Start(ctx, replayRequest)
		if err != nil || !replayed || replayedJob.Task.ID != replayRequest.JobID {
			t.Fatalf("at-cap replay job=%+v replayed=%v error=%v", replayedJob, replayed, err)
		}
	})
}

func TestSchedulableExportLimitIsPerConnectionGeneration(t *testing.T) {
	repo, database, _ := newExportRepository(t)
	defer database.Close()
	for index := 0; index < 8; index++ {
		mustStart(t, repo, testStartRequest("profile-"+strconv.Itoa(index)))
	}
	ninth := testStartRequest("profile-ninth")
	if _, _, err := repo.Start(context.Background(), ninth); !errors.Is(err, ErrConnectionJobLimit) {
		t.Fatalf("ninth schedulable export error=%v", err)
	}
	ninth.ConnectionGeneration = "8"
	if _, _, err := repo.Start(context.Background(), ninth); err != nil {
		t.Fatalf("different generation was incorrectly limited: %v", err)
	}
}

func TestCancelRestartAndCleanupCommandLedgers(t *testing.T) {
	t.Run("queued cancel and cleanup", func(t *testing.T) {
		repo, database, _ := newExportRepository(t)
		ctx := context.Background()
		job := mustStart(t, repo, testStartRequest("profile-1"))
		cancel := CommandEnvelope{CommandRequestID: uuid.NewString(), ExpectedStateRevision: "1"}

		canceled, replayed, err := repo.Cancel(ctx, job.Task.ID, cancel)
		if err != nil || replayed || canceled.Task.State != StateCanceled ||
			!canceled.Task.Terminal || canceled.Task.StateRevision != "2" {
			t.Fatalf("Cancel() job=%+v replayed=%v error=%v", canceled, replayed, err)
		}
		replayedCancel, replayed, err := repo.Cancel(ctx, job.Task.ID, cancel)
		if err != nil || !replayed || replayedCancel.Task.StateRevision != "2" {
			t.Fatalf("Cancel replay job=%+v replayed=%v error=%v", replayedCancel, replayed, err)
		}
		conflict := cancel
		conflict.ExpectedStateRevision = "2"
		if _, _, err := repo.Cancel(ctx, job.Task.ID, conflict); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
			t.Fatalf("Cancel command conflict error = %v", err)
		}

		// A distinct Cancel observes that the target is already satisfied even
		// with the stale revision and records its own deterministic result.
		already := CommandEnvelope{CommandRequestID: uuid.NewString(), ExpectedStateRevision: "1"}
		alreadyCanceled, replayed, err := repo.Cancel(ctx, job.Task.ID, already)
		if err != nil || replayed || alreadyCanceled.Task.StateRevision != "2" {
			t.Fatalf("already canceled job=%+v replayed=%v error=%v", alreadyCanceled, replayed, err)
		}

		cleanup := CommandEnvelope{CommandRequestID: uuid.NewString(), ExpectedStateRevision: "2"}
		cleaning, replayed, err := repo.Cleanup(ctx, job.Task.ID, cleanup)
		if err != nil || replayed || cleaning.Task.State != StateCleaning ||
			cleaning.Task.Terminal || !cleaning.Retained || cleaning.Task.TerminalAt != nil {
			t.Fatalf("Cleanup() job=%+v replayed=%v error=%v", cleaning, replayed, err)
		}
		cleaningReplay, replayed, err := repo.Cleanup(ctx, job.Task.ID, cleanup)
		if err != nil || !replayed || cleaningReplay.Task.State != StateCleaning {
			t.Fatalf("Cleanup replay job=%+v replayed=%v error=%v", cleaningReplay, replayed, err)
		}
		assertCount(t, database, "transfer_reservations", 1)

		cleaned, err := repo.CompleteCleanupAfterFilesRemoved(ctx, CleanupCompletion{
			JobID: job.Task.ID, ExpectedStateRevision: cleaning.Task.StateRevision,
		})
		if err != nil || cleaned.Task.State != StateCanceled || !cleaned.Task.Terminal ||
			cleaned.Retained || cleaned.TargetReservationID != nil {
			t.Fatalf("complete cleanup job=%+v error=%v", cleaned, err)
		}
		assertCount(t, database, "transfer_reservations", 0)
		cleanedAgain, err := repo.CompleteCleanupAfterFilesRemoved(ctx, CleanupCompletion{
			JobID: job.Task.ID, ExpectedStateRevision: cleaning.Task.StateRevision,
		})
		if err != nil || cleanedAgain.Task.StateRevision != cleaned.Task.StateRevision {
			t.Fatalf("cleanup completion replay job=%+v error=%v", cleanedAgain, err)
		}
		finalReplay, replayed, err := repo.Cleanup(ctx, job.Task.ID, cleanup)
		if err != nil || !replayed || finalReplay.Retained {
			t.Fatalf("completed Cleanup replay job=%+v replayed=%v error=%v", finalReplay, replayed, err)
		}
	})

	t.Run("restart", func(t *testing.T) {
		repo, _, _ := newExportRepository(t)
		ctx := context.Background()
		job := mustStart(t, repo, testStartRequest("profile-1"))
		running, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"})
		if err != nil {
			t.Fatal(err)
		}
		paused, err := repo.PauseRestartable(ctx, WorkerTransition{
			JobID: job.Task.ID, ExpectedStateRevision: running.Task.StateRevision,
			PublicErrorCode: exportlane.ErrorIdleTimeout,
		})
		if err != nil || paused.Task.State != StatePausedRestartable {
			t.Fatalf("PauseRestartable() job=%+v error=%v", paused, err)
		}
		restart := CommandEnvelope{
			CommandRequestID: uuid.NewString(), ExpectedStateRevision: paused.Task.StateRevision,
		}
		restarted, replayed, err := repo.Restart(ctx, job.Task.ID, restart)
		if err != nil || replayed || restarted.Task.State != StateQueued || restarted.Task.PublicErrorCode != nil {
			t.Fatalf("Restart() job=%+v replayed=%v error=%v", restarted, replayed, err)
		}
		replayedJob, replayed, err := repo.Restart(ctx, job.Task.ID, restart)
		if err != nil || !replayed || replayedJob.Task.StateRevision != restarted.Task.StateRevision {
			t.Fatalf("Restart replay job=%+v replayed=%v error=%v", replayedJob, replayed, err)
		}
		changed := restart
		changed.ExpectedStateRevision = restarted.Task.StateRevision
		if _, _, err := repo.Restart(ctx, job.Task.ID, changed); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
			t.Fatalf("Restart command conflict error = %v", err)
		}
	})

	t.Run("in-flight cancel", func(t *testing.T) {
		repo, _, _ := newExportRepository(t)
		ctx := context.Background()
		job := mustStart(t, repo, testStartRequest("profile-1"))
		running, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"})
		if err != nil {
			t.Fatal(err)
		}
		cancel := CommandEnvelope{
			CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.Task.StateRevision,
		}
		requested, _, err := repo.Cancel(ctx, job.Task.ID, cancel)
		if err != nil || requested.Task.State != StateRunning || !requested.CancelRequested || requested.Task.Terminal {
			t.Fatalf("in-flight Cancel() job=%+v error=%v", requested, err)
		}
		finished, err := repo.FinishCancel(ctx, WorkerTransition{
			JobID: job.Task.ID, ExpectedStateRevision: requested.Task.StateRevision,
		})
		if err != nil || finished.Task.State != StateCanceled || !finished.Task.Terminal || finished.CancelRequested {
			t.Fatalf("FinishCancel() job=%+v error=%v", finished, err)
		}
	})
}

func TestFragmentValidationGatesRestartAndTransferLane(t *testing.T) {
	repo, _, _ := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	running, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"})
	if err != nil {
		t.Fatal(err)
	}
	fragment := completeTestFragment(t, repo, running.Task.ID, "0")
	paused, err := repo.PauseRestartable(ctx, WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: running.Task.StateRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	restart := CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: paused.Task.StateRevision,
	}
	if _, _, err := repo.Restart(ctx, job.Task.ID, restart); !errors.Is(err, ErrFragmentValidationRequired) {
		t.Fatalf("Restart without validation error = %v", err)
	}

	validated, err := repo.ValidateFragmentForReuse(ctx, fragment.FragmentID, matchingValidator(fragment))
	if err != nil || !validated.Reusable {
		t.Fatalf("validated fragment=%+v error=%v", validated, err)
	}
	reusable, err := repo.ListReusableFragments(ctx, job.Task.ID)
	if err != nil || len(reusable) != 1 || reusable[0].FragmentID != fragment.FragmentID {
		t.Fatalf("reusable fragments=%+v error=%v", reusable, err)
	}
	restarted, replayed, err := repo.Restart(ctx, job.Task.ID, restart)
	if err != nil || replayed || restarted.Task.State != StateQueued {
		t.Fatalf("Restart after validation job=%+v replayed=%v error=%v", restarted, replayed, err)
	}
	request, err := repo.LaneRequest(ctx, job.Task.ID, []exportlane.Quantum{{
		ID: "slice-1", Kind: exportlane.QuantumTimeSlice,
		Request: transport.AuthorizedReadQuery{Database: "telemetry", Query: "SELECT * FROM cpu"},
	}})
	if err != nil || request.ID != job.Task.ID || request.Generation.Generation != job.ConnectionGeneration {
		t.Fatalf("LaneRequest() request=%+v error=%v", request, err)
	}

	// A new process uses a new validation epoch; persisted COMPLETE metadata is
	// not enough to authorize reuse.
	restartedProcess := NewRepository(repo.store, nil, func() time.Time { return exportTestNow })
	if _, err := restartedProcess.LaneRequest(ctx, job.Task.ID, request.Quanta); !errors.Is(err, ErrFragmentValidationRequired) {
		t.Fatalf("new-process LaneRequest error = %v", err)
	}
}

func TestFragmentValidationFailureMarksCorrupt(t *testing.T) {
	repo, _, _ := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	running, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"})
	if err != nil {
		t.Fatal(err)
	}
	fragment := completeTestFragment(t, repo, running.Task.ID, "0")
	bad := matchingValidation(fragment)
	bad.ChecksumSHA256 = digestText("different artifact")
	corrupt, err := repo.ValidateFragmentForReuse(ctx, fragment.FragmentID,
		FragmentValidatorFunc(func(context.Context, Fragment) (ArtifactValidation, error) { return bad, nil }))
	if !errors.Is(err, ErrFragmentCorrupt) || corrupt.State != FragmentCorrupt || corrupt.Reusable {
		t.Fatalf("corrupt fragment=%+v error=%v", corrupt, err)
	}
}

func TestDurableGzipHooksPersistFragmentAndConcreteReuseValidation(t *testing.T) {
	repo, _, _ := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	if _, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"}); err != nil {
		t.Fatal(err)
	}
	start, end := "0", "10"
	directory := t.TempDir()
	partPath := filepath.Join(directory, "slice.lp.gz.part")
	finalPath := filepath.Join(directory, "slice.lp.gz")
	fragment, err := repo.BeginFragment(ctx, BeginFragmentRequest{
		FragmentID: uuid.NewString(), JobID: job.Task.ID, Ordinal: "0", Kind: FragmentData,
		StartNS: &start, EndNS: &end, PartPath: partPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = transfer.WriteDurableGzip(ctx, partPath, finalPath, func(writer io.Writer) error {
		_, err := io.WriteString(writer, "cpu,host=a value=1i 1\n")
		return err
	}, transfer.FinalizeHooks{
		BeforeRename: func(ctx context.Context, meta transfer.ArtifactMeta) error {
			var err error
			fragment, err = repo.MarkFragmentFinalizing(ctx, FinalizeFragmentRequest{
				FragmentID: fragment.FragmentID, PartPath: meta.PartPath, FinalPath: meta.FinalPath,
				CompressedSize:   strconv.FormatInt(meta.CompressedSize, 10),
				UncompressedSize: strconv.FormatInt(meta.UncompressedSize, 10),
				ChecksumSHA256:   meta.SHA256,
			})
			return err
		},
		AfterRename: func(ctx context.Context, _ transfer.ArtifactMeta) error {
			var err error
			fragment, err = repo.CompleteFragment(ctx, fragment.FragmentID)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fragment.State != FragmentComplete || fragment.Reusable {
		t.Fatalf("post-rename fragment = %+v", fragment)
	}
	validated, err := repo.ValidateFragmentForReuse(ctx, fragment.FragmentID, GzipFileValidator{})
	if err != nil || !validated.Reusable {
		t.Fatalf("concrete gzip validation fragment=%+v error=%v", validated, err)
	}
}

func TestRecoveryPausesActiveJobsAndInvalidatesFragments(t *testing.T) {
	repo, _, taskRepo := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	running, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"})
	if err != nil {
		t.Fatal(err)
	}
	complete := completeTestFragment(t, repo, running.Task.ID, "0")
	complete, err = repo.ValidateFragmentForReuse(ctx, complete.FragmentID, matchingValidator(complete))
	if err != nil || !complete.Reusable {
		t.Fatal(err)
	}
	start, end := "10", "20"
	writing, err := repo.BeginFragment(ctx, BeginFragmentRequest{
		FragmentID: uuid.NewString(), JobID: job.Task.ID, Ordinal: "1", Kind: FragmentData,
		StartNS: &start, EndNS: &end, PartPath: filepath.Join(t.TempDir(), "slice-1.part"),
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := repo.Get(ctx, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	finalizing, err := repo.BeginFinalizing(ctx, WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: current.Task.StateRevision,
	})
	if err != nil || finalizing.Task.State != StateFinalizing {
		t.Fatalf("BeginFinalizing() job=%+v error=%v", finalizing, err)
	}

	restartedProcess := NewRepository(repo.store, taskRepo, func() time.Time { return exportTestNow })
	changed, err := restartedProcess.Recover(ctx)
	if err != nil || changed != 1 {
		t.Fatalf("Recover() changed=%d error=%v", changed, err)
	}
	recovered, err := restartedProcess.Get(ctx, job.Task.ID)
	if err != nil || recovered.Task.State != StatePausedRestartable || recovered.Task.Terminal {
		t.Fatalf("recovered job=%+v error=%v", recovered, err)
	}
	if recovered.Task.PublicErrorCode == nil || *recovered.Task.PublicErrorCode != "LOST_ON_RESTART" {
		t.Fatalf("recovery error code = %v", recovered.Task.PublicErrorCode)
	}
	states := make(map[string]string)
	for _, fragment := range recovered.Fragments {
		states[fragment.FragmentID] = fragment.State
		if fragment.FragmentID == complete.FragmentID && fragment.Reusable {
			t.Fatal("complete fragment remained reusable across process restart")
		}
	}
	if states[writing.FragmentID] != FragmentCorrupt || states[complete.FragmentID] != FragmentComplete {
		t.Fatalf("recovered fragment states = %v", states)
	}
}

func TestRecoveryMirrorsGenericTaskRecoveryWithoutDuplicateFailure(t *testing.T) {
	repo, _, taskRepo := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	if _, err := repo.BeginRun(ctx, WorkerTransition{JobID: job.Task.ID, ExpectedStateRevision: "1"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := taskRepo.RecoverCore(ctx); err != nil || changed != 1 {
		t.Fatalf("RecoverCore() changed=%d error=%v", changed, err)
	}
	restartedProcess := NewRepository(repo.store, taskRepo, func() time.Time { return exportTestNow })
	if changed, err := restartedProcess.Recover(ctx); err != nil || changed != 1 {
		t.Fatalf("export Recover() changed=%d error=%v", changed, err)
	}
	got, err := restartedProcess.Get(ctx, job.Task.ID)
	if err != nil || got.Task.State != StatePausedRestartable {
		t.Fatalf("mirrored recovery job=%+v error=%v", got, err)
	}
}

func TestRecoveryMirrorsQueuedTaskAfterGenericRecovery(t *testing.T) {
	repo, _, taskRepo := newExportRepository(t)
	ctx := context.Background()
	job := mustStart(t, repo, testStartRequest("profile-1"))
	if changed, err := taskRepo.RecoverCore(ctx); err != nil || changed != 1 {
		t.Fatalf("RecoverCore() changed=%d error=%v", changed, err)
	}
	restartedProcess := NewRepository(repo.store, taskRepo, func() time.Time { return exportTestNow })
	if changed, err := restartedProcess.Recover(ctx); err != nil || changed != 1 {
		t.Fatalf("export Recover() changed=%d error=%v", changed, err)
	}
	got, err := restartedProcess.Get(ctx, job.Task.ID)
	if err != nil || got.Task.State != StatePausedRestartable || got.Task.StateRevision != "2" {
		t.Fatalf("queued recovery job=%+v error=%v", got, err)
	}
}

func newExportRepository(t *testing.T) (*Repository, *store.Store, *tasks.Repository) {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	clock := func() time.Time { return exportTestNow }
	taskRepo := tasks.NewRepository(database, clock)
	return NewRepository(database, taskRepo, clock), database, taskRepo
}

func testStartRequest(profileID string) StartRequest {
	return StartRequest{
		JobID: uuid.NewString(), ProfileID: profileID, ProfileRevision: "1",
		ConnectionID: "connection-1", ConnectionGeneration: "7",
		ClientRequestID: uuid.NewString(), RequestDigest: digestText(uuid.NewString()),
		SpecDigest: digestText("spec"), TargetDigest: digestText("target"),
		TargetVolumeID: "volume-guid-1", TargetReservationBytes: transfer.ReservationExtentBytes,
		TargetVolumeFreeBytes: 90 * 1024 * 1024 * 1024,
		TargetVolumeCapacity:  100 * 1024 * 1024 * 1024,
		Plan: PlanDetail{
			SchemaVersion: 1, Database: "db", RetentionPolicy: "autogen", Measurement: "cpu",
			StartNS: "0", EndNS: "3600000000000", SliceWidthNS: "3600000000000",
			OutputDirectory: filepath.Join(os.TempDir(), "influxdesk-export-"+profileID),
			TypePreserving:  true,
		},
	}
}

func mustStart(t *testing.T, repo *Repository, req StartRequest) Job {
	t.Helper()
	job, replayed, err := repo.Start(context.Background(), req)
	if err != nil || replayed {
		t.Fatalf("Start() job=%+v replayed=%v error=%v", job, replayed, err)
	}
	return job
}

func completeTestFragment(t *testing.T, repo *Repository, jobID, ordinal string) Fragment {
	t.Helper()
	start, end := "0", "10"
	partPath := filepath.Join(t.TempDir(), "slice-"+ordinal+".lp.gz.part")
	finalPath := filepath.Join(filepath.Dir(partPath), "slice-"+ordinal+".lp.gz")
	fragment, err := repo.BeginFragment(context.Background(), BeginFragmentRequest{
		FragmentID: uuid.NewString(), JobID: jobID, Ordinal: ordinal, Kind: FragmentData,
		StartNS: &start, EndNS: &end, PartPath: partPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	fragment, err = repo.MarkFragmentFinalizing(context.Background(), FinalizeFragmentRequest{
		FragmentID: fragment.FragmentID, PartPath: partPath, FinalPath: finalPath,
		CompressedSize: "120", UncompressedSize: "480", ChecksumSHA256: digestText("artifact-" + ordinal),
	})
	if err != nil {
		t.Fatal(err)
	}
	fragment, err = repo.CompleteFragment(context.Background(), fragment.FragmentID)
	if err != nil {
		t.Fatal(err)
	}
	return fragment
}

func matchingValidator(fragment Fragment) FragmentValidator {
	validation := matchingValidation(fragment)
	return FragmentValidatorFunc(func(context.Context, Fragment) (ArtifactValidation, error) {
		return validation, nil
	})
}

func matchingValidation(fragment Fragment) ArtifactValidation {
	return ArtifactValidation{
		CompressedSize: *fragment.CompressedSize, UncompressedSize: *fragment.UncompressedSize,
		ChecksumSHA256: *fragment.ChecksumSHA256,
	}
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func concurrentStarts(ctx context.Context, repo *Repository, requests []StartRequest) []error {
	start := make(chan struct{})
	errorsSeen := make([]error, len(requests))
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			_, _, errorsSeen[index] = repo.Start(ctx, requests[index])
		}(index)
	}
	close(start)
	group.Wait()
	return errorsSeen
}

func countMatching(values []error, target error) int {
	count := 0
	for _, value := range values {
		if target == nil && value == nil {
			count++
		}
		if target != nil && errors.Is(value, target) {
			count++
		}
	}
	return count
}

func assertCount(t *testing.T, database *store.Store, table string, wanted int) {
	t.Helper()
	var count int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != wanted {
		t.Fatalf("%s count = %d, want %d", table, count, wanted)
	}
}
