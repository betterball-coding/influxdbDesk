package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func newPreflightTest(
	t *testing.T,
	volumeStat StagingVolumeStatFunc,
) (*ImportPreflightService, *Repository, *store.Store, string, string) {
	t.Helper()
	root := t.TempDir()
	databasePath := filepath.Join(root, "state.db")
	database, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return repositoryTestNow }
	repository := NewRepository(database, tasks.NewRepository(database, now), nil, now)
	stagingDirectory := filepath.Join(root, "private-staging")
	if err := os.Mkdir(stagingDirectory, 0o700); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if volumeStat == nil {
		volumeStat = preflightHealthyVolume
	}
	service := repository.NewImportPreflightService(stagingDirectory, volumeStat)
	return service, repository, database, databasePath, stagingDirectory
}

func preflightHealthyVolume(context.Context, string) (StagingVolumeStat, error) {
	return StagingVolumeStat{
		ID: "private-volume", FreeBytes: 80 * 1024 * 1024 * 1024,
		CapacityBytes: 100 * 1024 * 1024 * 1024,
	}, nil
}

func writePreflightSource(t *testing.T, directory, name string, payload []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func preflightRequest(path string, payload []byte) PreflightImportRequest {
	digest := sha256.Sum256(payload)
	return PreflightImportRequest{
		ClientRequestID: uuid.NewString(), ProfileID: "profile-1", ProfileRevision: "7",
		ConnectionID: "connection-1", ConnectionGeneration: "11", SourcePath: path,
		Source: ImportSourceIdentity{
			SHA256: hex.EncodeToString(digest[:]), SizeBytes: strconv.Itoa(len(payload)),
		},
		Format: ImportStageLP,
		Target: ImportTarget{Database: "metrics", RetentionPolicy: "autogen"},
	}
}

func TestPreflightImportStagesCheckpointAndReplaysWithoutSource(t *testing.T) {
	t.Parallel()
	service, _, database, _, stagingDirectory := newPreflightTest(t, nil)
	defer database.Close()
	payload := []byte("cpu,host=a value=9007199254740993i 1735689600000000001\n")
	sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "source.lp", payload)
	request := preflightRequest(sourcePath, payload)

	created, err := service.PreflightImport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if created.Replayed || !created.Ready || created.Job.Task.State != ImportReady ||
		created.Job.CheckpointDigest == "" || created.Job.Checkpoint.Sequence != "0" ||
		created.Job.Checkpoint.LogicalOffset != "0" ||
		created.Job.Checkpoint.SourceSHA256 != request.Source.SHA256 {
		t.Fatalf("created=%+v", created)
	}
	stagingPath := filepath.Join(stagingDirectory, "import-"+created.Job.Task.ID+".canonical.lp")
	staged, err := os.ReadFile(stagingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(staged, payload) {
		t.Fatalf("staged payload=%q", staged)
	}
	stagingDigest := sha256.Sum256(staged)
	if created.Job.Checkpoint.StagingSHA256 != hex.EncodeToString(stagingDigest[:]) {
		t.Fatalf("staging sha=%s", created.Job.Checkpoint.StagingSHA256)
	}
	var checkpoints, reservations, tasksBefore int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM import_checkpoints WHERE job_id=?", created.Job.Task.ID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?", created.Job.Task.ID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM tasks").Scan(&tasksBefore); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 1 || reservations != 1 {
		t.Fatalf("checkpoints=%d reservations=%d", checkpoints, reservations)
	}
	if err := os.Remove(sourcePath); err != nil {
		t.Fatal(err)
	}

	replayed, err := service.PreflightImport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || !replayed.Ready || replayed.Job.Task.ID != created.Job.Task.ID ||
		replayed.Job.CheckpointDigest != created.Job.CheckpointDigest {
		t.Fatalf("replayed=%+v", replayed)
	}

	conflict := request
	conflict.Target.Database = "other"
	if _, err := service.PreflightImport(context.Background(), conflict); !errors.Is(err, tasks.ErrIdempotencyConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	var tasksAfter int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM tasks").Scan(&tasksAfter); err != nil {
		t.Fatal(err)
	}
	if tasksAfter != tasksBefore {
		t.Fatalf("conflict created tasks before=%d after=%d", tasksBefore, tasksAfter)
	}
}

func TestPreflightImportConcurrentSameDigestCreatesOneJob(t *testing.T) {
	t.Parallel()
	service, _, database, _, stagingDirectory := newPreflightTest(t, nil)
	defer database.Close()
	payload := []byte("cpu value=1i 1\n")
	sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "concurrent.lp", payload)
	request := preflightRequest(sourcePath, payload)

	responses := make([]PreflightImportResponse, 2)
	errorsSeen := make([]error, 2)
	var wait sync.WaitGroup
	for index := range responses {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			responses[index], errorsSeen[index] = service.PreflightImport(context.Background(), request)
		}(index)
	}
	wait.Wait()
	for _, err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if responses[0].Job.Task.ID == "" || responses[0].Job.Task.ID != responses[1].Job.Task.ID {
		t.Fatalf("responses=%+v", responses)
	}
	var taskCount, ledgerCount int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM tasks WHERE kind='IMPORT'").Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM idempotency_ledger WHERE resource_kind='IMPORT'").Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 1 || ledgerCount != 1 {
		t.Fatalf("tasks=%d ledgers=%d", taskCount, ledgerCount)
	}
	current, err := service.repository.GetImport(context.Background(), responses[0].Job.Task.ID)
	if err != nil || current.Task.State != ImportReady || current.CheckpointDigest == "" {
		t.Fatalf("current=%+v err=%v", current, err)
	}
}

func TestPreflightImportRetainedLimitRejectsBeforeSourceOpen(t *testing.T) {
	t.Parallel()
	service, repository, database, _, stagingDirectory := newPreflightTest(t, nil)
	defer database.Close()
	for index := 0; index < RetainedTransferJobsPerProfile; index++ {
		if err := repository.quota.Admit(context.Background(), uuid.NewString(), "profile-1", "EXPORT", "QUEUED"); err != nil {
			t.Fatal(err)
		}
	}
	missingSource := filepath.Join(filepath.Dir(stagingDirectory), "missing.lp")
	request := preflightRequest(missingSource, []byte("cpu value=1i 1\n"))
	if _, err := service.PreflightImport(context.Background(), request); !errors.Is(err, ErrTransferProfileLimit) {
		t.Fatalf("limit error=%v", err)
	}
	var tasksCount int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM tasks WHERE kind='IMPORT'").Scan(&tasksCount); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(stagingDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if tasksCount != 0 || len(entries) != 0 {
		t.Fatalf("tasks=%d staging entries=%d", tasksCount, len(entries))
	}
}

func TestPreflightImportQuotaFailurePersistsSafeNonGrantableTask(t *testing.T) {
	t.Parallel()
	lowSpace := func(context.Context, string) (StagingVolumeStat, error) {
		capacity := int64(100 * 1024 * 1024 * 1024)
		return StagingVolumeStat{
			ID: "private-volume", CapacityBytes: capacity,
			FreeBytes: VolumeReserveFloor(capacity) + ReservationExtentBytes - 1,
		}, nil
	}
	service, _, database, _, stagingDirectory := newPreflightTest(t, lowSpace)
	defer database.Close()
	payload := []byte("cpu value=1i 1\n")
	sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "quota.lp", payload)
	response, err := service.PreflightImport(context.Background(), preflightRequest(sourcePath, payload))
	if err != nil {
		t.Fatal(err)
	}
	if response.Ready || response.Job.Task.State != ImportFailed || !response.Job.Task.Terminal ||
		response.Job.CheckpointDigest != "" || response.Job.Task.PublicErrorCode == nil ||
		*response.Job.Task.PublicErrorCode != "PRIVATE_TRANSFER_QUOTA_EXCEEDED" {
		t.Fatalf("response=%+v", response)
	}
	if actionAllowed(PreviewRunRequest{Action: GrantStart}, response.Job) {
		t.Fatal("failed preflight became grantable")
	}
	var checkpoints int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM import_checkpoints WHERE job_id=?", response.Job.Task.ID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 0 {
		t.Fatalf("quota failure checkpoints=%d", checkpoints)
	}
}

func TestPreflightImportCrashReplayAndRecoveryDoNotRestage(t *testing.T) {
	t.Parallel()
	t.Run("after admission", func(t *testing.T) {
		service, repository, database, _, stagingDirectory := newPreflightTest(t, nil)
		defer database.Close()
		payload := []byte("cpu value=1i 1\n")
		sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "admit-crash.lp", payload)
		request := preflightRequest(sourcePath, payload)
		crash := errors.New("injected crash after admission")
		service.hooks.afterCreate = func() error { return crash }
		if _, err := service.PreflightImport(context.Background(), request); !errors.Is(err, crash) {
			t.Fatalf("crash error=%v", err)
		}
		if err := os.Remove(sourcePath); err != nil {
			t.Fatal(err)
		}
		service.hooks.afterCreate = nil
		replay, err := service.PreflightImport(context.Background(), request)
		if err != nil || !replay.Replayed || replay.Job.Task.State != ImportPreflighting || replay.Job.CheckpointDigest != "" {
			t.Fatalf("replay=%+v err=%v", replay, err)
		}
		changed, err := repository.RecoverImports(context.Background())
		if err != nil || changed != 1 {
			t.Fatalf("recovered=%d err=%v", changed, err)
		}
		recovered, err := repository.GetImport(context.Background(), replay.Job.Task.ID)
		if err != nil || recovered.Task.State != ImportPausedRestage || recovered.CheckpointDigest != "" {
			t.Fatalf("job=%+v err=%v", recovered, err)
		}
	})

	t.Run("after staging commit", func(t *testing.T) {
		service, repository, database, _, stagingDirectory := newPreflightTest(t, nil)
		defer database.Close()
		payload := []byte("cpu value=1i 1\n")
		sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "stage-crash.lp", payload)
		request := preflightRequest(sourcePath, payload)
		crash := errors.New("injected crash before ready")
		service.hooks.beforeReady = func() error { return crash }
		if _, err := service.PreflightImport(context.Background(), request); !errors.Is(err, crash) {
			t.Fatalf("crash error=%v", err)
		}
		var jobID string
		if err := database.DB().QueryRow("SELECT id FROM tasks WHERE kind='IMPORT'").Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		stagingPath := filepath.Join(stagingDirectory, "import-"+jobID+".canonical.lp")
		if _, err := os.Stat(stagingPath); err != nil {
			t.Fatalf("committed staging missing: %v", err)
		}
		if err := os.Remove(sourcePath); err != nil {
			t.Fatal(err)
		}
		service.hooks.beforeReady = nil
		replay, err := service.PreflightImport(context.Background(), request)
		if err != nil || !replay.Replayed || replay.Job.Task.State != ImportStaging || replay.Job.CheckpointDigest != "" {
			t.Fatalf("replay=%+v err=%v", replay, err)
		}
		changed, err := repository.RecoverImports(context.Background())
		if err != nil || changed != 1 {
			t.Fatalf("recovered=%d err=%v", changed, err)
		}
		var reservations int
		if err := database.DB().QueryRow("SELECT COUNT(*) FROM transfer_reservations WHERE job_id=?", jobID).Scan(&reservations); err != nil {
			t.Fatal(err)
		}
		if reservations != 1 {
			t.Fatalf("staging reservation lost after recovery: %d", reservations)
		}
	})
}

func TestPreflightReadyTransactionRollsBackCheckpointAndTaskTogether(t *testing.T) {
	t.Parallel()
	service, repository, database, _, stagingDirectory := newPreflightTest(t, nil)
	defer database.Close()
	if _, err := database.DB().Exec(`CREATE TRIGGER reject_import_ready BEFORE UPDATE ON import_jobs
		WHEN NEW.state='READY' BEGIN SELECT RAISE(ABORT, 'injected ready failure'); END`); err != nil {
		t.Fatal(err)
	}
	payload := []byte("cpu value=1i 1\n")
	sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), "ready-rollback.lp", payload)
	request := preflightRequest(sourcePath, payload)
	if _, err := service.PreflightImport(context.Background(), request); err == nil {
		t.Fatal("expected READY transaction failure")
	}
	var jobID string
	if err := database.DB().QueryRow("SELECT id FROM tasks WHERE kind='IMPORT'").Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	job, err := repository.GetImport(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoints, readyEvents int
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM import_checkpoints WHERE job_id=?", jobID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow("SELECT COUNT(*) FROM task_events WHERE resource_id=? AND state='READY'", jobID).Scan(&readyEvents); err != nil {
		t.Fatal(err)
	}
	if job.Task.State != ImportStaging || job.CheckpointDigest != "" || checkpoints != 0 || readyEvents != 0 {
		t.Fatalf("job=%+v checkpoints=%d readyEvents=%d", job, checkpoints, readyEvents)
	}
	if actionAllowed(PreviewRunRequest{Action: GrantStart}, job) {
		t.Fatal("rolled-back READY task became grantable")
	}
}

func TestPreflightSQLiteDoesNotContainSourcePathOrContentCanaries(t *testing.T) {
	t.Parallel()
	service, _, database, databasePath, stagingDirectory := newPreflightTest(t, nil)
	pathCanary := "SOURCE_PATH_PASSWORD_CANARY_6f585aa1"
	contentCanary := "JWT_CONTENT_CANARY_983cad51"
	payload := []byte("cpu secret=\"" + contentCanary + "\" 1\n")
	sourcePath := writePreflightSource(t, filepath.Dir(stagingDirectory), pathCanary+".lp", payload)
	response, err := service.PreflightImport(context.Background(), preflightRequest(sourcePath, payload))
	if err != nil || !response.Ready {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		contents, err := os.ReadFile(databasePath + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(contents, []byte(pathCanary)) || bytes.Contains(contents, []byte(contentCanary)) {
			t.Fatalf("database artifact %s contains preflight canary", filepath.Base(databasePath+suffix))
		}
	}
}
