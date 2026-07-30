package importworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type workerFixture struct {
	authorizer *transfer.RunAuthorizer
	repository *transfer.Repository
	database   *store.Store
	job        transfer.ImportJob
	path       string
	target     string
}

func newWorkerFixture(t *testing.T, staging []byte) workerFixture {
	t.Helper()
	directory := t.TempDir()
	stagingPath := filepath.Join(directory, "canonical.lp")
	if err := os.WriteFile(stagingPath, staging, 0o600); err != nil {
		t.Fatal(err)
	}
	stagingDigest := sha256.Sum256(staging)
	database, err := store.Open(context.Background(), filepath.Join(directory, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now
	taskRepository := tasks.NewRepository(database, now)
	repository := transfer.NewRepository(database, taskRepository, nil, now)
	targetDigest, err := transfer.ImportTargetDigest(transfer.ImportTarget{
		Database: "metrics", RetentionPolicy: "autogen",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := repository.CreateImport(context.Background(), transfer.CreateImportRequest{
		JobID: uuid.NewString(), ProfileID: "profile-1",
		ClientScope: "PREFLIGHT/profile-1/" + uuid.NewString(), RequestDigest: "request-digest",
		LedgerExpiresAt: time.Now().Add(90 * 24 * time.Hour),
		InitialCheckpoint: transfer.Checkpoint{
			Sequence: "0", LogicalOffset: "0", AdaptiveMaxPoints: "5000",
			AdaptiveMaxBytes: "5242880", SourceSHA256: "source-digest",
			StagingSHA256: hex.EncodeToString(stagingDigest[:]), NormalizationVersion: "lp-v1",
			SpecDigest: "spec-digest", TargetDigest: targetDigest,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := protection.NewManager(database, now, strings.NewReader(strings.Repeat("r", 8192)))
	if _, err := manager.Register(context.Background(), protection.RegisterRequest{
		ConnectionID: "connection-1", ConnectionGeneration: "1", ProfileID: "profile-1",
		ProfileRevision: "3", Mode: protection.ProtectedLocked,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
	authorizer := transfer.NewRunAuthorizer(repository, manager, nil,
		"profile-1", "3", "connection-1", "1", now)
	commandID := uuid.NewString()
	preview, err := authorizer.Preview(context.Background(), transfer.PreviewRunRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: transfer.GrantStart,
	})
	if err != nil || preview.ImportRunGrant == nil {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	job, replayed, err := authorizer.Start(context.Background(), transfer.AuthorizedStartRequest{
		JobID: job.Task.ID, CommandRequestID: commandID,
		ExpectedStateRevision: job.Task.StateRevision, Action: transfer.GrantStart,
		ImportRunGrant: preview.ImportRunGrant.Token,
	})
	if err != nil || replayed || job.ActiveRunSegmentID == nil {
		t.Fatalf("start=%+v replay=%v err=%v", job, replayed, err)
	}
	return workerFixture{
		authorizer: authorizer, repository: repository, database: database,
		job: job, path: stagingPath, target: targetDigest,
	}
}

func newHTTPWorker(t *testing.T, fixture workerFixture, handler http.HandlerFunc) (*Worker, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(fixture.authorizer, fixture.repository, dispatcher, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return worker, server
}

func runRequest(fixture workerFixture) RunNextRequest {
	return RunNextRequest{
		JobID: fixture.job.Task.ID, StagingPath: fixture.path,
		TargetDigest: fixture.target, Database: "metrics", RetentionPolicy: "autogen",
	}
}

func TestRunNextACKFinishesAtVerifiedEOFIdempotently(t *testing.T) {
	staging := []byte("cpu,host=a value=1i 1700000000000000001\n")
	fixture := newWorkerFixture(t, staging)
	segmentID := *fixture.job.ActiveRunSegmentID
	var calls atomic.Int32
	worker, _ := newHTTPWorker(t, fixture, func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/write" || request.URL.Query().Get("precision") != "ns" ||
			request.URL.Query().Get("db") != "metrics" || request.URL.Query().Get("rp") != "autogen" {
			t.Errorf("request target=%s?%s", request.URL.Path, request.URL.RawQuery)
		}
		payload, _ := io.ReadAll(request.Body)
		if string(payload) != strings.TrimSuffix(string(staging), "\n") {
			t.Errorf("payload mismatch length=%d", len(payload))
		}
		writer.WriteHeader(http.StatusNoContent)
	})

	result, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeSucceeded || result.Job.Task.State != transfer.ImportSucceeded ||
		!result.Job.Task.Terminal || result.Job.Checkpoint.LogicalOffset != strconv.Itoa(len(staging)) ||
		calls.Load() != 1 {
		t.Fatalf("result=%+v calls=%d", result, calls.Load())
	}
	revision := result.Job.Task.SnapshotRevision
	replayed, wasReplay, err := fixture.repository.FinishImport(context.Background(), transfer.FinishImportRequest{
		JobID: result.Job.Task.ID, RunSegmentID: segmentID,
		CheckpointDigest: result.Job.CheckpointDigest, StagingLogicalSize: strconv.Itoa(len(staging)),
	})
	if err != nil || !wasReplay || replayed.Task.SnapshotRevision != revision {
		t.Fatalf("finish replay=%+v replayed=%v err=%v", replayed, wasReplay, err)
	}
	responseReplay, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || responseReplay.Outcome != OutcomeSucceeded ||
		responseReplay.Job.Task.SnapshotRevision != revision || calls.Load() != 1 {
		t.Fatalf("worker replay=%+v calls=%d err=%v", responseReplay, calls.Load(), err)
	}
}

func TestRunNextResponseSettlements(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		outcome    Outcome
		state      string
		incident   string
		checkpoint string
	}{
		{name: "413", status: 413, body: `{"error":"request too large"}`,
			outcome: OutcomeTooLarge, state: transfer.ImportRunning, checkpoint: "0"},
		{name: "partial", status: 400, body: `{"error":"partial write: field type conflict dropped=1"}`,
			outcome: OutcomePartial, state: transfer.ImportNeedsPartialDecision, incident: "PARTIAL", checkpoint: "0"},
		{name: "server", status: 503, body: `{"error":"unavailable"}`,
			outcome: OutcomeUnknown, state: transfer.ImportNeedsUnknownDecision, incident: "UNKNOWN", checkpoint: "0"},
		{name: "malformed success", status: 200, body: `{"results":`,
			outcome: OutcomeUnknown, state: transfer.ImportNeedsUnknownDecision, incident: "UNKNOWN", checkpoint: "0"},
		{name: "clear rejection", status: 401, body: `{"error":"denied"}`,
			outcome: OutcomeRejected, state: transfer.ImportFailed, checkpoint: "0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
			var calls atomic.Int32
			worker, _ := newHTTPWorker(t, fixture, func(writer http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body)
			})
			result, err := worker.RunNext(context.Background(), runRequest(fixture))
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome != test.outcome || result.Job.Task.State != test.state ||
				result.Job.Checkpoint.LogicalOffset != test.checkpoint || calls.Load() != 1 {
				t.Fatalf("result=%+v calls=%d", result, calls.Load())
			}
			if test.incident != "" && (result.Job.OpenIncident == nil || result.Job.OpenIncident.Kind != test.incident) {
				t.Fatalf("incident=%+v", result.Job.OpenIncident)
			}
		})
	}
}

func TestRunNextSendsExactFiveMiBPointOnce(t *testing.T) {
	prefix, suffix := `wide value="`, `" 1`
	line := prefix + strings.Repeat("x", int(MaxBatchBytes)-len(prefix)-len(suffix)) + suffix
	if len(line) != int(MaxBatchBytes) {
		t.Fatalf("line length=%d", len(line))
	}
	fixture := newWorkerFixture(t, append([]byte(line), '\n'))
	var calls atomic.Int32
	worker, _ := newHTTPWorker(t, fixture, func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		payload, _ := io.ReadAll(request.Body)
		if len(payload) != int(MaxBatchBytes) {
			t.Errorf("payload length=%d", len(payload))
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	result, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || result.Outcome != OutcomeSucceeded || calls.Load() != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, calls.Load(), err)
	}
}

type failingStarter struct{ calls atomic.Int32 }

func (s *failingStarter) Begin(context.Context, transport.AuthorizedWriteBatch) (attemptWaiter, error) {
	s.calls.Add(1)
	return nil, errors.New("safe adapter failure")
}

func TestBarrierFailureIsPersistedNotSentAndNeverCallsHTTP(t *testing.T) {
	fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
	starter := &failingStarter{}
	worker, err := newWorker(fixture.authorizer, fixture.repository, starter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = worker.RunNext(context.Background(), runRequest(fixture))
	if !errors.Is(err, ErrRoundTripNotStarted) || starter.calls.Load() != 1 {
		t.Fatalf("calls=%d err=%v", starter.calls.Load(), err)
	}
	var state string
	if err := fixture.database.DB().QueryRow(`SELECT state FROM import_attempts WHERE job_id=?`,
		fixture.job.Task.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED_NOT_SENT" {
		t.Fatalf("attempt state=%s", state)
	}
	current, err := fixture.repository.GetImport(context.Background(), fixture.job.Task.ID)
	if err != nil || current.Task.State != transfer.ImportRunning || current.Checkpoint.LogicalOffset != "0" {
		t.Fatalf("current=%+v err=%v", current, err)
	}
}

type immediateAttempt struct {
	response *transport.Response
	err      error
}

func (a immediateAttempt) Wait() (*transport.Response, error) { return a.response, a.err }

type immediateStarter struct {
	attempt attemptWaiter
	calls   atomic.Int32
}

func (s *immediateStarter) Begin(context.Context, transport.AuthorizedWriteBatch) (attemptWaiter, error) {
	s.calls.Add(1)
	return s.attempt, nil
}

func TestStartedTransportFailureCreatesUnknownIncident(t *testing.T) {
	fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
	starter := &immediateStarter{attempt: immediateAttempt{err: context.DeadlineExceeded}}
	worker, err := newWorker(fixture.authorizer, fixture.repository, starter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || result.Outcome != OutcomeUnknown || result.Job.OpenIncident == nil ||
		result.Job.OpenIncident.Kind != "UNKNOWN" || starter.calls.Load() != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, starter.calls.Load(), err)
	}
}

type ackFailRepository struct {
	*transfer.Repository
}

func (r ackFailRepository) SettleACK(context.Context, string) (transfer.ImportJob, error) {
	return transfer.ImportJob{}, errors.New("injected ACK transaction failure")
}

func TestACKSettlementFailureBecomesUnknown(t *testing.T) {
	fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
	response := &transport.Response{
		StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), ContentLength: 0,
	}
	starter := &immediateStarter{attempt: immediateAttempt{response: response}}
	worker, err := newWorker(fixture.authorizer, ackFailRepository{fixture.repository}, starter, Options{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || result.Outcome != OutcomeUnknown || result.Job.OpenIncident == nil ||
		result.Job.OpenIncident.Kind != "UNKNOWN" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestBatchLimitUsesFiveThousandPointsWithoutRewritingQuery(t *testing.T) {
	var staging strings.Builder
	for index := 0; index < MaxBatchPoints+1; index++ {
		staging.WriteString("cpu value=1i ")
		staging.WriteString(strconv.Itoa(index + 1))
		staging.WriteByte('\n')
	}
	fixture := newWorkerFixture(t, []byte(staging.String()))
	var points []int
	worker, _ := newHTTPWorker(t, fixture, func(writer http.ResponseWriter, request *http.Request) {
		payload, _ := io.ReadAll(request.Body)
		points = append(points, strings.Count(string(payload), "\n")+1)
		writer.WriteHeader(http.StatusNoContent)
	})
	first, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || first.Outcome != OutcomeACKED || first.Job.Task.State != transfer.ImportRunning {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := worker.RunNext(context.Background(), runRequest(fixture))
	if err != nil || second.Outcome != OutcomeSucceeded || len(points) != 2 ||
		points[0] != MaxBatchPoints || points[1] != 1 {
		t.Fatalf("second=%+v points=%v err=%v", second, points, err)
	}
}

func TestEmptyAndTamperedStagingNeverDispatch(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		fixture := newWorkerFixture(t, nil)
		starter := &failingStarter{}
		worker, _ := newWorker(fixture.authorizer, fixture.repository, starter, Options{})
		_, err := worker.RunNext(context.Background(), runRequest(fixture))
		if !errors.Is(err, ErrEmptyStaging) || starter.calls.Load() != 0 {
			t.Fatalf("calls=%d err=%v", starter.calls.Load(), err)
		}
	})
	t.Run("tampered", func(t *testing.T) {
		fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
		if err := os.WriteFile(fixture.path, []byte("cpu value=2i 1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		starter := &failingStarter{}
		worker, _ := newWorker(fixture.authorizer, fixture.repository, starter, Options{})
		_, err := worker.RunNext(context.Background(), runRequest(fixture))
		if !errors.Is(err, ErrStagingIntegrity) || starter.calls.Load() != 0 {
			t.Fatalf("calls=%d err=%v", starter.calls.Load(), err)
		}
	})
}

func TestWriteRequestUsesEncodedTargetOnly(t *testing.T) {
	fixture := newWorkerFixture(t, []byte("cpu value=1i 1\n"))
	worker, _ := newHTTPWorker(t, fixture, func(writer http.ResponseWriter, request *http.Request) {
		values, err := url.ParseQuery(request.URL.RawQuery)
		if err != nil || values.Get("db") != "metrics" || values.Get("precision") != "ns" {
			t.Errorf("query=%q err=%v", request.URL.RawQuery, err)
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	if _, err := worker.RunNext(context.Background(), runRequest(fixture)); err != nil {
		t.Fatal(err)
	}
}
