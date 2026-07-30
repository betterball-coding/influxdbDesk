package connection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/operation"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type fakeCredentials map[string][]byte

func (f fakeCredentials) Get(profileID string, kind credential.Kind) ([]byte, error) {
	value := f[profileID+"/"+string(kind)]
	if value == nil {
		return nil, fmt.Errorf("missing")
	}
	return append([]byte(nil), value...), nil
}

func TestOpenCloseUsesMonotonicGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db"]]}]}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(ctx, profile.SaveRequest{
		Name: "test", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthBasic, Username: "user", ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	credentials := fakeCredentials{created.ID + "/" + string(credential.KindInfluxPassword): []byte("password")}
	protections := protection.NewManager(database, time.Now, nil)
	manager := NewManager(profiles, credentials, protections, time.Now)

	first, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.ConnectionGeneration != "1" || first.Protection.ProtectionRevision != "1" || first.Version != "1.12.4" {
		t.Fatalf("unexpected first snapshot: %+v", first)
	}
	if err := manager.Close(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	second, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.ConnectionGeneration != "2" {
		t.Fatalf("generation was reused: %+v", second)
	}
}

func TestLockPausesImportPermitBeforeReturning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(ctx, profile.SaveRequest{
		Name: "test", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	protections := protection.NewManager(database, time.Now, nil)
	taskRepository := tasks.NewRepository(database, time.Now)
	transfers := transfer.NewRepository(database, taskRepository, nil, time.Now)
	manager := NewManager(profiles, fakeCredentials{}, protections, time.Now)
	manager.EnableTransfers(transfers)
	opened, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := transfers.CreateImport(ctx, transfer.CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: created.ID,
		ClientScope: "preflight/" + created.ID + "/" + uuid.NewString(), RequestDigest: "digest",
		LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: transfer.Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000", AdaptiveMaxBytes: "5242880",
			SourceSHA256: "source", StagingSHA256: "staging", NormalizationVersion: "lp-v1",
			SpecDigest: "spec", TargetDigest: "target",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Unlock(ctx, protection.UnlockRequest{
		ConnectionID: created.ID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: opened.ConnectionGeneration, ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	commandID := uuid.NewString()
	preview, err := manager.PreviewImportRun(ctx, created.ID, transfer.PreviewRunRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: transfer.GrantStart,
	})
	if err != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	started, _, err := manager.StartImport(ctx, created.ID, transfer.AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: transfer.GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if err != nil || started.Task.State != transfer.ImportRunning {
		t.Fatalf("started=%+v err=%v", started, err)
	}
	if _, err := manager.Lock(ctx, protection.LockRequest{
		ConnectionID: created.ID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: opened.ConnectionGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	paused, err := transfers.GetImport(ctx, job.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Task.State != transfer.ImportPausedSafe || paused.ActiveRunSegmentID != nil || paused.PauseRequested {
		t.Fatalf("import was not safely paused before Lock returned: %+v", paused)
	}
}

func TestClosedConnectionCanCancelStartedOperation(t *testing.T) {
	mutationEntered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			if err := request.ParseForm(); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.Form.Get("q") == "SHOW DATABASES" {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
				return
			}
			select {
			case mutationEntered <- struct{}{}:
			default:
			}
			<-request.Context().Done()
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(ctx, profile.SaveRequest{
		Name: "test", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	protections := protection.NewManager(database, time.Now, nil)
	taskRepository := tasks.NewRepository(database, time.Now)
	manager := NewManager(profiles, fakeCredentials{}, protections, time.Now)
	manager.EnableOperations(database, taskRepository)
	opened, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Unlock(ctx, protection.UnlockRequest{
		ConnectionID: created.ID, CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: opened.ConnectionGeneration, ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	preview, err := manager.PreviewMutation(ctx, created.ID, operation.PreviewRequest{
		Query: `CREATE DATABASE "close-cancel"`,
	})
	if err != nil || preview.Token == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	createdOperation, err := manager.ExecuteMutation(ctx, created.ID, operation.ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-mutationEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation did not cross the start barrier")
	}
	running, err := manager.GetOperation(ctx, createdOperation.Task.ID)
	if err != nil || running.Task.State != operation.StateDispatching {
		t.Fatalf("running=%+v err=%v", running, err)
	}
	if err := manager.Close(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	canceling, err := manager.CancelOperation(ctx, createdOperation.Task.ID, operation.CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.Task.StateRevision,
	})
	if err != nil || !canceling.CancelRequested {
		t.Fatalf("canceling=%+v err=%v", canceling, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		finished, err := manager.GetOperation(ctx, createdOperation.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if finished.Task.Terminal {
			if finished.Task.State != operation.StateOutcomeUnknown || !finished.CancelRequested {
				t.Fatalf("finished=%+v", finished)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation did not finish after cancel: %+v", finished)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestImportControlUsesPersistedGenerationGateAndClosedFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Influxdb-Version", "1.12.4")
		switch request.URL.Path {
		case "/ping":
			writer.WriteHeader(http.StatusNoContent)
		case "/query":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	profiles := profile.NewRepository(database, time.Now)
	created, err := profiles.Save(ctx, profile.SaveRequest{
		Name: "import-control", BaseURL: server.URL, Environment: profile.EnvironmentDevelopment,
		AuthMode: transport.AuthNone, ProtectionMode: protection.ProtectedLocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	protections := protection.NewManager(database, time.Now, nil)
	taskRepository := tasks.NewRepository(database, time.Now)
	transfers := transfer.NewRepository(database, taskRepository, nil, time.Now)
	stagingDirectory := filepath.Join(t.TempDir(), "staging")
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	preflightService := transfers.NewImportPreflightService(stagingDirectory, transfer.SystemStagingVolumeStat)
	manager := NewManager(profiles, fakeCredentials{}, protections, time.Now)
	manager.EnableTransfers(transfers, stagingDirectory)
	manager.EnableImportCleanup(preflightService.CleanupFileFunc())
	opened, err := manager.Open(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("cpu value=1i 1700000000000000001\n")
	sourcePath := filepath.Join(t.TempDir(), "source.lp")
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	preflight, err := preflightService.PreflightImport(ctx, transfer.PreflightImportRequest{
		ClientRequestID: uuid.NewString(), ProfileID: created.ID, ProfileRevision: created.Revision,
		ConnectionID: opened.ConnectionID, ConnectionGeneration: opened.ConnectionGeneration,
		SourcePath: sourcePath, Source: transfer.ImportSourceIdentity{
			SHA256: hex.EncodeToString(sum[:]), SizeBytes: strconv.Itoa(len(payload)),
		},
		Format: transfer.ImportStageLP, Target: transfer.ImportTarget{Database: "metrics"},
	})
	if err != nil || !preflight.Ready {
		t.Fatalf("preflight=%+v err=%v", preflight, err)
	}

	gateEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		gateDone <- protections.WithGenerationGate(ctx, opened.ConnectionID,
			opened.ConnectionGeneration, func(context.Context) error {
				close(gateEntered)
				<-releaseGate
				return nil
			})
	}()
	<-gateEntered
	type cancelResult struct {
		job transfer.ImportJob
		err error
	}
	cancelDone := make(chan cancelResult, 1)
	cancelCommand := transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: preflight.Job.Task.StateRevision,
	}
	go func() {
		job, _, err := manager.CancelImport(ctx, preflight.Job.Task.ID, cancelCommand)
		cancelDone <- cancelResult{job: job, err: err}
	}()
	select {
	case result := <-cancelDone:
		t.Fatalf("CancelImport crossed a held generation gate: %+v err=%v", result.job, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseGate)
	if err := <-gateDone; err != nil {
		t.Fatal(err)
	}
	canceled := <-cancelDone
	if canceled.err != nil || canceled.job.Task.State != transfer.ImportCanceled ||
		canceled.job.Task.StateRevision == "" {
		t.Fatalf("canceled=%+v err=%v", canceled.job, canceled.err)
	}
	if err := manager.Close(ctx, created.ID); err != nil {
		t.Fatal(err)
	}

	// Closed generations are not reconstructed by protection recovery. The new
	// manager can still clean a reconciled, non-dispatching job from storage.
	restartedProtections := protection.NewManager(database, time.Now, nil)
	if err := restartedProtections.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(profiles, fakeCredentials{}, restartedProtections, time.Now)
	restarted.EnableTransfers(transfers, stagingDirectory)
	restarted.EnableImportCleanup(preflightService.CleanupFileFunc())
	cleaned, _, err := restarted.CleanupImport(ctx, canceled.job.Task.ID, transfer.ImportCommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: canceled.job.Task.StateRevision,
	})
	if err != nil || cleaned.Task.State != transfer.ImportCanceled || !cleaned.Task.Terminal {
		t.Fatalf("cleaned=%+v err=%v", cleaned, err)
	}
	stagingPath := filepath.Join(stagingDirectory, "import-"+canceled.job.Task.ID+".canonical.lp")
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("staging file remains after CleanupImport: %v", err)
	}
}
