package transfer

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func TestCancelImportNoAttemptIsLedgerFirstAndTargetSatisfied(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, segmentID, _ := createRunningImport(t, repository)
	command := ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	}

	canceled, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, command)
	if err != nil || replayed {
		t.Fatalf("cancel replayed=%v err=%v", replayed, err)
	}
	if canceled.Task.State != ImportCanceled || !canceled.Task.Terminal ||
		canceled.ActiveRunSegmentID != nil || canceled.PauseRequested {
		t.Fatalf("canceled=%+v", canceled)
	}
	if _, found := repository.permits.Get(job.Task.ID); found {
		t.Fatal("Cancel retained Permit")
	}
	var segmentState, stopReason string
	if err := database.DB().QueryRow(`SELECT state,stop_reason FROM import_run_segments
		WHERE run_segment_id=?`, segmentID).Scan(&segmentState, &stopReason); err != nil {
		t.Fatal(err)
	}
	if segmentState != "CLOSED" || stopReason != importStopUserCanceled {
		t.Fatalf("segment state=%s reason=%s", segmentState, stopReason)
	}

	replayedJob, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, command)
	if err != nil || !replayed || !reflect.DeepEqual(canceled, replayedJob) {
		t.Fatalf("replay=%+v replayed=%v err=%v", replayedJob, replayed, err)
	}
	conflict := command
	conflict.ExpectedStateRevision = canceled.Task.StateRevision
	if _, _, err := repository.CancelImport(context.Background(), job.Task.ID, conflict); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
		t.Fatalf("same command ID different digest error=%v", err)
	}

	// Cancel is target-satisfied: a new command with a stale revision records a
	// deterministic success without changing state or revision.
	targetSatisfied, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil || replayed || targetSatisfied.Task.StateRevision != canceled.Task.StateRevision {
		t.Fatalf("target satisfied=%+v replayed=%v err=%v", targetSatisfied, replayed, err)
	}
}

func TestCancelImportConcurrentExactCommandCommitsOnce(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job, _, _ := createRunningImport(t, repository)
	command := ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	}
	type result struct {
		job      ImportJob
		replayed bool
		err      error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			out, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, command)
			results <- result{job: out, replayed: replayed, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	replays := 0
	for item := range results {
		if item.err != nil || item.job.Task.State != ImportCanceled {
			t.Fatalf("result=%+v", item)
		}
		if item.replayed {
			replays++
		}
	}
	if replays != 1 {
		t.Fatalf("replayed calls=%d", replays)
	}
	var commands, canceledEvents int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM command_ledger WHERE resource_id=?
		AND scope LIKE '%CANCEL%'`, job.Task.ID).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM task_events WHERE resource_id=?
		AND state=?`, job.Task.ID, ImportCanceled).Scan(&canceledEvents); err != nil {
		t.Fatal(err)
	}
	if commands != 1 || canceledEvents != 1 {
		t.Fatalf("commands=%d canceledEvents=%d", commands, canceledEvents)
	}
}

func TestCancelImportInFlightSettlement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		settle     func(*Repository, string) (ImportJob, error)
		wantState  string
		wantOffset string
		wantOpen   bool
	}{
		{
			name: "ack advances checkpoint then cancels",
			settle: func(repository *Repository, attemptID string) (ImportJob, error) {
				return repository.SettleACK(context.Background(), attemptID)
			},
			wantState: ImportCanceled, wantOffset: "120",
		},
		{
			name: "partial remains decision state",
			settle: func(repository *Repository, attemptID string) (ImportJob, error) {
				return repository.RecordIncident(context.Background(), attemptID, uuid.NewString(), "PARTIAL")
			},
			wantState: ImportNeedsPartialDecision, wantOffset: "0", wantOpen: true,
		},
		{
			name: "unknown remains decision state",
			settle: func(repository *Repository, attemptID string) (ImportJob, error) {
				return repository.RecordIncident(context.Background(), attemptID, uuid.NewString(), "UNKNOWN")
			},
			wantState: ImportNeedsUnknownDecision, wantOffset: "0", wantOpen: true,
		},
		{
			name: "explicit rejection cancels without checkpoint advance",
			settle: func(repository *Repository, attemptID string) (ImportJob, error) {
				return repository.SettleRejected(context.Background(), attemptID)
			},
			wantState: ImportCanceled, wantOffset: "0",
		},
		{
			name: "explicit 413 cancels without adapting batch",
			settle: func(repository *Repository, attemptID string) (ImportJob, error) {
				return repository.SettleTooLarge(context.Background(), attemptID)
			},
			wantState: ImportCanceled, wantOffset: "0",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			repository, database := newTransferRepository(t)
			defer database.Close()
			job, segmentID, permit := createRunningImport(t, repository)
			attemptID := uuid.NewString()
			if err := beginAttemptForTest(repository, context.Background(), AttemptRequest{
				JobID: job.Task.ID, BatchID: "batch-1", AttemptID: attemptID,
				RunSegmentID: segmentID, ParentCheckpointDigest: permit.CurrentCheckpointDigest,
				StartOffset: "0", EndOffset: "120", PayloadDigest: "payload",
			}, job.ProfileID, permit); err != nil {
				t.Fatal(err)
			}
			command := ImportCommandEnvelope{
				CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
			}
			canceling, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, command)
			if err != nil || replayed || canceling.Task.State != ImportRunning || !canceling.PauseRequested {
				t.Fatalf("canceling=%+v replayed=%v err=%v", canceling, replayed, err)
			}
			settled, err := test.settle(repository, attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if settled.Task.State != test.wantState || settled.Checkpoint.LogicalOffset != test.wantOffset ||
				(settled.OpenIncident != nil) != test.wantOpen {
				t.Fatalf("settled=%+v", settled)
			}
			if _, found := repository.permits.Get(job.Task.ID); found {
				t.Fatal("settlement retained canceled Permit")
			}
			replayedJob, replayed, err := repository.CancelImport(context.Background(), job.Task.ID, command)
			if err != nil || !replayed || replayedJob.Task.State != test.wantState {
				t.Fatalf("cancel replay after settlement=%+v replayed=%v err=%v", replayedJob, replayed, err)
			}
		})
	}
}

func TestCleanupImportDeletesBeforeAtomicQuotaReleaseAndReplays(t *testing.T) {
	t.Parallel()
	repository, database := newTransferRepository(t)
	defer database.Close()
	job := createCanceledImportForCleanup(t, repository)
	directory, coordinator, path := prepareImportCleanupFile(t, repository, job)
	command := ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	}
	var removals atomic.Int32
	remove := func(ctx context.Context, jobID string) error {
		removals.Add(1)
		return coordinator.RemoveImportFiles(ctx, directory, jobID)
	}

	cleaned, replayed, err := repository.CleanupImport(context.Background(), job.Task.ID, command, remove)
	if err != nil || replayed {
		t.Fatalf("cleanup replayed=%v err=%v", replayed, err)
	}
	if cleaned.Task.State != ImportCanceled || !cleaned.Task.Terminal {
		t.Fatalf("cleaned=%+v", cleaned)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging still exists: %v", err)
	}
	assertReleasedImportQuota(t, database.DB(), job.Task.ID)

	replayedJob, replayed, err := repository.CleanupImport(context.Background(), job.Task.ID, command, remove)
	if err != nil || !replayed || replayedJob.Task.State != ImportCanceled || removals.Load() != 1 {
		t.Fatalf("cleanup replay=%+v replayed=%v removals=%d err=%v",
			replayedJob, replayed, removals.Load(), err)
	}
}

func TestCleanupImportFailureStaysCleaningAndRecoveryUsesDurableFinalState(t *testing.T) {
	t.Parallel()
	for _, startState := range []string{ImportCanceled, ImportPausedSafe, ImportPausedRestage} {
		startState := startState
		t.Run(startState, func(t *testing.T) {
			repository, database := newTransferRepository(t)
			defer database.Close()
			job := createImportInCleanupState(t, repository, startState)
			directory, coordinator, path := prepareImportCleanupFile(t, repository, job)
			command := ImportCommandEnvelope{
				CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
			}
			coordinator.hooks.remove = func(string) error { return errors.New("injected remove failure") }
			_, replayed, err := repository.CleanupImport(context.Background(), job.Task.ID, command,
				coordinator.CleanupFileFunc(directory))
			if !errors.Is(err, ErrStageFileCleanup) || replayed {
				t.Fatalf("failed cleanup replayed=%v err=%v", replayed, err)
			}
			current, err := repository.GetImport(context.Background(), job.Task.ID)
			if err != nil || current.Task.State != ImportCleaning {
				t.Fatalf("after failure=%+v err=%v", current, err)
			}
			var retained, reservations int
			var durableFinalState, ledgerState string
			if err := database.DB().QueryRow(`SELECT retained,state FROM transfer_jobs WHERE job_id=?`,
				job.Task.ID).Scan(&retained, &durableFinalState); err != nil {
				t.Fatal(err)
			}
			if err := database.DB().QueryRow(`SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?`,
				job.Task.ID).Scan(&reservations); err != nil {
				t.Fatal(err)
			}
			if err := database.DB().QueryRow(`SELECT result_state FROM command_ledger
				WHERE resource_id=? AND scope LIKE '%CLEANUP%'`, job.Task.ID).Scan(&ledgerState); err != nil {
				t.Fatal(err)
			}
			wantFinal := importCleanupFinalState(startState)
			if retained != 1 || reservations != 1 || durableFinalState != wantFinal || ledgerState != ImportCleaning {
				t.Fatalf("retained=%d reservations=%d durable=%s ledger=%s want=%s",
					retained, reservations, durableFinalState, ledgerState, wantFinal)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("failed cleanup removed file: %v", err)
			}

			coordinator.hooks.remove = nil
			restarted := NewRepository(database, tasks.NewRepository(database, nil), NewPermitRegistry(), nil)
			completed, err := restarted.RecoverImportCleanups(context.Background(), coordinator.CleanupFileFunc(directory))
			if err != nil || completed != 1 {
				t.Fatalf("recovery completed=%d err=%v", completed, err)
			}
			recovered, err := restarted.GetImport(context.Background(), job.Task.ID)
			if err != nil || recovered.Task.State != wantFinal || !recovered.Task.Terminal {
				t.Fatalf("recovered=%+v err=%v", recovered, err)
			}
			assertReleasedImportQuota(t, database.DB(), job.Task.ID)
		})
	}
}

func createCanceledImportForCleanup(t *testing.T, repository *Repository) ImportJob {
	t.Helper()
	job, _, _ := createRunningImport(t, repository)
	job, _, err := repository.CancelImport(context.Background(), job.Task.ID, ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func createImportInCleanupState(t *testing.T, repository *Repository, state string) ImportJob {
	t.Helper()
	if state == ImportCanceled {
		return createCanceledImportForCleanup(t, repository)
	}
	job, segmentID, _ := createRunningImport(t, repository)
	if _, err := repository.InvalidatePermits(context.Background(), "connection-1", "1", "2",
		PermitInvalidatedProtectionLocked); err != nil {
		t.Fatal(err)
	}
	job, err := repository.GetImport(context.Background(), job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state == ImportPausedRestage {
		updated, err := repository.tasks.Apply(context.Background(), job.Task.ID, job.Task.StateRevision, tasks.Change{
			State: ImportPausedRestage, StateChanged: true, ChangeType: "TEST",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repository.store.DB().Exec(`UPDATE import_jobs SET state=?,state_revision=?,snapshot_revision=?
			WHERE job_id=?`, ImportPausedRestage, updated.StateRevision, updated.SnapshotRevision, job.Task.ID); err != nil {
			t.Fatal(err)
		}
		job, err = repository.GetImport(context.Background(), job.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = segmentID
	return job
}

func prepareImportCleanupFile(
	t *testing.T,
	repository *Repository,
	job ImportJob,
) (string, *StagingFileCoordinator, string) {
	t.Helper()
	directory := t.TempDir()
	stat := func(context.Context, string) (StagingVolumeStat, error) {
		return StagingVolumeStat{ID: "private-volume", FreeBytes: 100 << 30, CapacityBytes: 200 << 30}, nil
	}
	coordinator := NewStagingFileCoordinator(repository.quota, stat)
	_, _, finalPath, err := stagingFilePaths(directory, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, []byte("m f=1i 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reservation, err := repository.quota.ReserveExtent(context.Background(), job.Task.ID,
		"PRIVATE", "private-volume", ReservationExtentBytes, 100<<30, 200<<30)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.quota.Consume(context.Background(), reservation.ReservationID, int64(len("m f=1i 1\n"))); err != nil {
		t.Fatal(err)
	}
	return directory, coordinator, finalPath
}

func assertReleasedImportQuota(t *testing.T, database interface {
	QueryRow(query string, args ...any) *sql.Row
}, jobID string) {
	t.Helper()
	var retained, reservations int
	if err := database.QueryRow(`SELECT retained FROM transfer_jobs WHERE job_id=?`, jobID).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?`, jobID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if retained != 0 || reservations != 0 {
		t.Fatalf("retained=%d reservations=%d", retained, reservations)
	}
}
