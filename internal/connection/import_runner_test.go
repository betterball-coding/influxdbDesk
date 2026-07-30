package connection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type importRunnerHarness struct {
	manager     *Manager
	repository  *transfer.Repository
	protections *protection.Manager
	profile     profile.Profile
	opened      Snapshot
	job         transfer.ImportJob
	start       transfer.AuthorizedStartRequest
}

func newImportRunnerHarness(t *testing.T, payload []byte, write http.HandlerFunc) importRunnerHarness {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
		case "/write":
			write(writer, request)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))

	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(ctx, profile.SaveRequest{
		Name: "runner", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		database.Close()
		server.Close()
		t.Fatal(err)
	}
	protections := protection.NewManager(database, time.Now, nil)
	repository := transfer.NewRepository(database, tasks.NewRepository(database, time.Now), nil, time.Now)
	stagingDirectory := filepath.Join(t.TempDir(), "staging")
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	preflight := repository.NewImportPreflightService(stagingDirectory, transfer.SystemStagingVolumeStat)
	manager := NewManager(profiles, fakeCredentials{}, protections, time.Now)
	manager.EnableTransfers(repository, stagingDirectory)
	opened, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if manager.IsOpen(created.ID) {
			_ = manager.Close(context.Background(), created.ID)
		}
		_ = database.Close()
		server.Close()
	})

	sourcePath := filepath.Join(t.TempDir(), "source.lp")
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	prepared, err := preflight.PreflightImport(ctx, transfer.PreflightImportRequest{
		ClientRequestID: uuid.NewString(), ProfileID: created.ID, ProfileRevision: created.Revision,
		ConnectionID: opened.ConnectionID, ConnectionGeneration: opened.ConnectionGeneration,
		SourcePath: sourcePath, Source: transfer.ImportSourceIdentity{
			SHA256: hex.EncodeToString(sum[:]), SizeBytes: strconv.Itoa(len(payload)),
		},
		Format: transfer.ImportStageLP,
		Target: transfer.ImportTarget{Database: "metrics", RetentionPolicy: "autogen"},
	})
	if err != nil || !prepared.Ready {
		t.Fatalf("preflight=%+v err=%v", prepared, err)
	}
	if _, err := manager.Unlock(ctx, protection.UnlockRequest{
		ConnectionID: opened.ConnectionID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: opened.ConnectionGeneration,
		ExpectedProtectionRevision:   opened.Protection.ProtectionRevision,
	}); err != nil {
		t.Fatal(err)
	}
	commandID := uuid.NewString()
	preview, err := manager.PreviewImportRun(ctx, created.ID, transfer.PreviewRunRequest{
		JobID: prepared.Job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: prepared.Job.Task.StateRevision, Action: transfer.GrantStart,
	})
	if err != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	return importRunnerHarness{
		manager: manager, repository: repository, protections: protections, profile: created,
		opened: opened, job: prepared.Job,
		start: transfer.AuthorizedStartRequest{
			JobID: prepared.Job.Task.ID, CommandRequestID: commandID,
			ExpectedStateRevision: prepared.Job.Task.StateRevision, Action: transfer.GrantStart,
			ImportRunGrant: preview.ImportRunGrant.Token,
		},
	}
}

func importPayload(points int) []byte {
	var output strings.Builder
	for index := 0; index < points; index++ {
		fmt.Fprintf(&output, "cpu,host=a value=%di %d\n", index, 1_700_000_000_000_000_000+index)
	}
	return []byte(output.String())
}

func waitImport(t *testing.T, repository *transfer.Repository, jobID string, accept func(transfer.ImportJob) bool) transfer.ImportJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, err := repository.GetImport(context.Background(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		if accept(job) {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for import state: %+v", job)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestImportRunnerContinuouslyDispatchesOneBatchAtATime(t *testing.T) {
	var calls atomic.Int32
	harness := newImportRunnerHarness(t, importPayload(5001), func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Query().Get("db") != "metrics" || request.URL.Query().Get("rp") != "autogen" {
			t.Errorf("untrusted target reached dispatcher: %s", request.URL.RawQuery)
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	started, replayed, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start)
	if err != nil || replayed || started.Task.State != transfer.ImportRunning {
		t.Fatalf("started=%+v replayed=%v err=%v", started, replayed, err)
	}
	finished := waitImport(t, harness.repository, harness.job.Task.ID, func(job transfer.ImportJob) bool {
		return job.Task.State == transfer.ImportSucceeded
	})
	if calls.Load() != 2 || !finished.Task.Terminal || finished.Checkpoint.Sequence != "2" {
		t.Fatalf("calls=%d finished=%+v", calls.Load(), finished)
	}
}

func TestImportStartReplayKeepsSingleRunner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	harness := newImportRunnerHarness(t, importPayload(1), func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		writer.WriteHeader(http.StatusNoContent)
	})
	if _, replayed, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start); err != nil || replayed {
		t.Fatalf("initial start replayed=%v err=%v", replayed, err)
	}
	<-entered
	if _, replayed, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start); err != nil || !replayed {
		t.Fatalf("retry replayed=%v err=%v", replayed, err)
	}
	harness.manager.mu.RLock()
	runnerCount := len(harness.manager.importRunners)
	harness.manager.mu.RUnlock()
	if runnerCount != 1 {
		t.Fatalf("runner count=%d", runnerCount)
	}
	if _, err := harness.manager.RunImportNext(context.Background(), harness.profile.ID,
		harness.job.Task.ID, "other", "autogen"); !errors.Is(err, ErrImportRunnerBusy) {
		t.Fatalf("manual runner error=%v", err)
	}
	close(release)
	waitImport(t, harness.repository, harness.job.Task.ID, func(job transfer.ImportJob) bool {
		return job.Task.State == transfer.ImportSucceeded
	})
	if calls.Load() != 1 {
		t.Fatalf("write calls=%d", calls.Load())
	}
}

func TestImportRunnerLockPreservesInflightSettlement(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	var calls atomic.Int32
	harness := newImportRunnerHarness(t, importPayload(5001), func(writer http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			writer.WriteHeader(http.StatusNoContent)
		case <-request.Context().Done():
			canceled <- struct{}{}
		}
	})
	if _, _, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err := harness.manager.Lock(context.Background(), protection.LockRequest{
		ConnectionID: harness.opened.ConnectionID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: harness.opened.ConnectionGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
		t.Fatal("Lock canceled an in-flight request")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	paused := waitImport(t, harness.repository, harness.job.Task.ID, func(job transfer.ImportJob) bool {
		return job.Task.State == transfer.ImportPausedSafe
	})
	if calls.Load() != 1 || paused.Checkpoint.Sequence != "1" {
		t.Fatalf("calls=%d paused=%+v", calls.Load(), paused)
	}
}

func TestCloseDrainsCanceledInflightImportBeforeReturning(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	harness := newImportRunnerHarness(t, importPayload(5001), func(_ http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-request.Context().Done():
		case <-release:
		}
	})
	if _, _, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start); err != nil {
		t.Fatal(err)
	}
	<-entered
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := harness.manager.Close(closeCtx, harness.profile.ID); err != nil {
		t.Fatal(err)
	}
	settled, err := harness.repository.GetImport(context.Background(), harness.job.Task.ID)
	if err != nil || settled.Task.State != transfer.ImportNeedsUnknownDecision ||
		settled.OpenIncident == nil || settled.Checkpoint.Sequence != "0" || calls.Load() != 1 {
		t.Fatalf("settled=%+v calls=%d err=%v", settled, calls.Load(), err)
	}
	harness.manager.mu.RLock()
	runnerCount := len(harness.manager.importRunners)
	harness.manager.mu.RUnlock()
	if runnerCount != 0 {
		t.Fatalf("Close returned with %d import runners", runnerCount)
	}
}

func TestImportRunnerCancelPreservesInflightSettlement(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(http.ResponseWriter)
		state string
	}{
		{name: "acked", write: func(writer http.ResponseWriter) { writer.WriteHeader(http.StatusNoContent) },
			state: transfer.ImportCanceled},
		{name: "unknown", write: func(writer http.ResponseWriter) {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(writer, `{"error":"unavailable"}`)
		}, state: transfer.ImportNeedsUnknownDecision},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			harness := newImportRunnerHarness(t, importPayload(5001), func(writer http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					close(entered)
				}
				<-release
				test.write(writer)
			})
			started, _, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start)
			if err != nil {
				t.Fatal(err)
			}
			<-entered
			canceling, _, err := harness.manager.CancelImport(context.Background(), harness.job.Task.ID,
				transfer.ImportCommandEnvelope{CommandRequestID: uuid.NewString(),
					ExpectedStateRevision: started.Task.StateRevision})
			if err != nil || !canceling.PauseRequested {
				t.Fatalf("canceling=%+v err=%v", canceling, err)
			}
			close(release)
			settled := waitImport(t, harness.repository, harness.job.Task.ID, func(job transfer.ImportJob) bool {
				return job.Task.State == test.state
			})
			if calls.Load() != 1 || (test.state == transfer.ImportNeedsUnknownDecision && settled.OpenIncident == nil) {
				t.Fatalf("calls=%d settled=%+v", calls.Load(), settled)
			}
		})
	}
}

func TestImportRunnerStopsOnIncident(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		state  string
	}{
		{name: "partial", status: http.StatusBadRequest,
			body: `{"error":"partial write: field type conflict"}`, state: transfer.ImportNeedsPartialDecision},
		{name: "unknown", status: http.StatusServiceUnavailable,
			body: `{"error":"unavailable"}`, state: transfer.ImportNeedsUnknownDecision},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			harness := newImportRunnerHarness(t, importPayload(5001), func(writer http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body)
			})
			if _, _, err := harness.manager.StartImport(context.Background(), harness.profile.ID, harness.start); err != nil {
				t.Fatal(err)
			}
			job := waitImport(t, harness.repository, harness.job.Task.ID, func(job transfer.ImportJob) bool {
				return job.Task.State == test.state
			})
			time.Sleep(25 * time.Millisecond)
			if calls.Load() != 1 || job.OpenIncident == nil || job.Checkpoint.Sequence != "0" {
				t.Fatalf("calls=%d job=%+v", calls.Load(), job)
			}
		})
	}
}

func TestImportRecoveryNeverStartsRunner(t *testing.T) {
	var calls atomic.Int32
	harness := newImportRunnerHarness(t, importPayload(1), func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	})
	harness.manager.mu.RLock()
	active := harness.manager.active[harness.profile.ID]
	harness.manager.mu.RUnlock()
	if active == nil {
		t.Fatal("connection missing")
	}
	started, _, err := active.imports.Start(context.Background(), harness.start)
	if err != nil || started.Task.State != transfer.ImportRunning {
		t.Fatalf("direct start=%+v err=%v", started, err)
	}
	if changed, err := harness.repository.RecoverImports(context.Background()); err != nil || changed != 1 {
		t.Fatalf("recovery changed=%d err=%v", changed, err)
	}
	time.Sleep(25 * time.Millisecond)
	recovered, err := harness.repository.GetImport(context.Background(), harness.job.Task.ID)
	if err != nil || recovered.Task.State != transfer.ImportPausedSafe || calls.Load() != 0 {
		t.Fatalf("recovered=%+v calls=%d err=%v", recovered, calls.Load(), err)
	}
}
