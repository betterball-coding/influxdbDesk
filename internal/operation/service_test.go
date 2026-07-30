package operation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transport"
)

func newOperationService(t *testing.T, handler http.Handler) (*Service, *protection.Manager, *store.Store, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	protectionManager := protection.NewManager(database, time.Now, strings.NewReader(strings.Repeat("r", 8192)))
	if _, err := protectionManager.Register(context.Background(), protection.RegisterRequest{
		ConnectionID: "connection-1", ConnectionGeneration: "1", ProfileID: "profile-1",
		ProfileRevision: "3", Mode: protection.ProtectedLocked,
	}); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(database, tasks.NewRepository(database, time.Now), protectionManager,
		dispatcher, "profile-1", "3", "connection-1", "1", Options{})
	return service, protectionManager, database, server
}

func unlock(t *testing.T, manager *protection.Manager) {
	t.Helper()
	if _, err := manager.Unlock(context.Background(), protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "1",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPreviewExecuteAndIdempotentReplay(t *testing.T) {
	var hits atomic.Int32
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	defer database.Close()

	locked, err := service.Preview(context.Background(), PreviewRequest{Query: `DROP DATABASE "secret-db"`})
	if err != nil || locked.Executable || locked.Token != nil {
		t.Fatalf("locked preview=%+v err=%v", locked, err)
	}
	unlock(t, manager)
	preview, err := service.Preview(context.Background(), PreviewRequest{Query: `DROP DATABASE "secret-db"`})
	if err != nil || !preview.Executable || preview.Token == nil || preview.Target != "secret-db" {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	if _, err := service.Execute(context.Background(), ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token, Confirmation: "other",
	}); !errors.Is(err, ErrConfirmationMismatch) {
		t.Fatalf("confirmation error=%v", err)
	}
	request := ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token, Confirmation: "secret-db",
	}
	created, err := service.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if created.ProfileID != "profile-1" {
		t.Fatalf("created.ProfileID = %q, want profile-1", created.ProfileID)
	}
	finished := waitForOperation(t, service, created.Task.ID)
	if finished.Task.State != StateSucceeded || !finished.DispatchAttempted || hits.Load() != 1 {
		t.Fatalf("finished=%+v hits=%d", finished, hits.Load())
	}
	replayed, err := service.Execute(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.Task.ID != created.Task.ID || hits.Load() != 1 {
		t.Fatalf("replayed=%+v err=%v hits=%d", replayed, err, hits.Load())
	}

	if _, err := database.DB().Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(databasePath(t, database))
	if err == nil && bytes.Contains(contents, []byte(`DROP DATABASE "secret-db"`)) {
		t.Fatal("mutation body was persisted in SQLite")
	}
}

func TestClassifyMutationResponseRejectsOversizedBody(t *testing.T) {
	prefix := `{"results":[{"statement_id":0}]}`
	body := prefix + strings.Repeat(" ", (64<<10)+1-len(prefix)) + "malformed-tail"
	response := &transport.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	state, code, _, audit := classifyMutationResponse(response)
	if state != StateOutcomeUnknown || code != "MUTATION_OUTCOME_UNKNOWN" || audit != "UNKNOWN" {
		t.Fatalf("classifyMutationResponse() = (%q, %q, %q), want OUTCOME_UNKNOWN", state, code, audit)
	}
}

func TestPreviewTokenStoreCompletesBeforeLockReturns(t *testing.T) {
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	defer database.Close()
	unlock(t, manager)

	beforeStore := make(chan struct{})
	releaseStore := make(chan struct{})
	service.beforeTokenStore = func() {
		close(beforeStore)
		<-releaseStore
	}
	type previewResult struct {
		preview Preview
		err     error
	}
	previewDone := make(chan previewResult, 1)
	go func() {
		preview, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "gated-db"`})
		previewDone <- previewResult{preview: preview, err: err}
	}()
	<-beforeStore

	lockDone := make(chan error, 1)
	go func() {
		_, err := manager.Lock(context.Background(), protection.LockRequest{
			ConnectionID: "connection-1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	waitForOperationLockPending(t, manager)
	select {
	case err := <-lockDone:
		t.Fatalf("Lock returned before token store completed: %v", err)
	default:
	}
	close(releaseStore)
	issued := <-previewDone
	if issued.err != nil || !issued.preview.Executable || issued.preview.Token == nil {
		t.Fatalf("preview=%+v err=%v", issued.preview, issued.err)
	}
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}

	service.tokensMu.Lock()
	record, found := service.tokens[*issued.preview.Token]
	countAfterLock := len(service.tokens)
	service.tokensMu.Unlock()
	if !found || countAfterLock != 1 || record.binding.ProtectionRevision != "2" {
		t.Fatalf("stored=%t count=%d record=%+v", found, countAfterLock, record)
	}
	locked, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "after-lock"`})
	if err != nil || locked.Executable || locked.Token != nil {
		t.Fatalf("locked preview=%+v err=%v", locked, err)
	}
	service.tokensMu.Lock()
	countAfterRetry := len(service.tokens)
	service.tokensMu.Unlock()
	if countAfterRetry != countAfterLock {
		t.Fatalf("old-revision token count increased after Lock: before=%d after=%d", countAfterLock, countAfterRetry)
	}
}

func TestCancelQueuedOperationIsDurableIdempotentAndNotSent(t *testing.T) {
	var hits atomic.Int32
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	defer database.Close()
	unlock(t, manager)
	dispatchBlocked := make(chan struct{})
	releaseDispatch := make(chan struct{})
	service.beforeDispatch = func() {
		close(dispatchBlocked)
		<-releaseDispatch
	}
	preview, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "cancel-me"`})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Execute(context.Background(), ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-dispatchBlocked

	if _, err := service.Cancel(context.Background(), created.Task.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: "99",
	}); !errors.Is(err, tasks.ErrRevisionConflict) {
		t.Fatalf("revision error=%v", err)
	}
	unchanged, err := service.Get(context.Background(), created.Task.ID)
	if err != nil || unchanged.Task.State != StateQueued || unchanged.CancelRequested {
		t.Fatalf("unchanged=%+v err=%v", unchanged, err)
	}

	commandID := uuid.NewString()
	request := CommandEnvelope{CommandRequestID: commandID, ExpectedStateRevision: created.Task.StateRevision}
	canceled, err := service.Cancel(context.Background(), created.Task.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Task.State != StateCanceledNotSent || !canceled.Task.Terminal || !canceled.CancelRequested || canceled.Task.StateRevision != "2" {
		t.Fatalf("canceled=%+v", canceled)
	}
	replayed, err := service.Cancel(context.Background(), created.Task.ID, request)
	if err != nil || !replayed.Replayed || replayed.Task.StateRevision != canceled.Task.StateRevision {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	if _, err := service.Cancel(context.Background(), created.Task.ID, CommandEnvelope{
		CommandRequestID: commandID, ExpectedStateRevision: canceled.Task.StateRevision,
	}); !errors.Is(err, tasks.ErrCommandIdempotencyConflict) {
		t.Fatalf("digest conflict=%v", err)
	}
	targetSatisfied, err := service.Cancel(context.Background(), created.Task.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: "1",
	})
	if err != nil || targetSatisfied.Task.StateRevision != canceled.Task.StateRevision {
		t.Fatalf("target satisfied=%+v err=%v", targetSatisfied, err)
	}
	if _, err := service.Cancel(context.Background(), created.Task.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: "01",
	}); !errors.Is(err, tasks.ErrInvalidDecimal) {
		t.Fatalf("invalid decimal error=%v", err)
	}

	close(releaseDispatch)
	waitForOperationControlRemoval(t, service, created.Task.ID)
	finished, err := service.Get(context.Background(), created.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Task.State != StateCanceledNotSent || hits.Load() != 0 {
		t.Fatalf("finished=%+v hits=%d", finished, hits.Load())
	}
}

func TestCancelStartedOperationRequestsCancelAndEndsUnknown(t *testing.T) {
	entered := make(chan struct{})
	var hits atomic.Int32
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		hits.Add(1)
		_ = request.ParseForm()
		close(entered)
		<-request.Context().Done()
	}))
	defer server.Close()
	defer database.Close()
	unlock(t, manager)
	preview, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "started-cancel"`})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Execute(context.Background(), ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation did not start")
	}
	running, err := service.Get(context.Background(), created.Task.ID)
	if err != nil || running.Task.State != StateDispatching {
		t.Fatalf("running=%+v err=%v", running, err)
	}
	request := CommandEnvelope{CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.Task.StateRevision}
	canceling, err := service.Cancel(context.Background(), created.Task.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	if canceling.Task.State != StateDispatching || canceling.Task.Terminal || !canceling.CancelRequested || canceling.Task.StateRevision != "3" {
		t.Fatalf("canceling=%+v", canceling)
	}
	targetSatisfiedRequest := CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.Task.StateRevision,
	}
	targetSatisfied, err := service.Cancel(context.Background(), created.Task.ID, targetSatisfiedRequest)
	if err != nil || targetSatisfied.Replayed || targetSatisfied.Task.StateRevision != canceling.Task.StateRevision || !targetSatisfied.CancelRequested {
		t.Fatalf("target satisfied=%+v err=%v", targetSatisfied, err)
	}
	targetSatisfiedReplay, err := service.Cancel(context.Background(), created.Task.ID, targetSatisfiedRequest)
	if err != nil || !targetSatisfiedReplay.Replayed || targetSatisfiedReplay.Task.StateRevision != canceling.Task.StateRevision {
		t.Fatalf("target satisfied replay=%+v err=%v", targetSatisfiedReplay, err)
	}
	finished := waitForOperation(t, service, created.Task.ID)
	if finished.Task.State != StateOutcomeUnknown || !finished.CancelRequested || hits.Load() != 1 {
		t.Fatalf("finished=%+v hits=%d", finished, hits.Load())
	}
	replayed, err := service.Cancel(context.Background(), created.Task.ID, request)
	if err != nil || !replayed.Replayed || replayed.Task.State != StateOutcomeUnknown {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
}

func TestCancelQueuedOperationSerializesWithLock(t *testing.T) {
	var hits atomic.Int32
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	defer database.Close()
	unlock(t, manager)
	dispatchBlocked := make(chan struct{})
	releaseDispatch := make(chan struct{})
	service.beforeDispatch = func() {
		close(dispatchBlocked)
		<-releaseDispatch
	}
	preview, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "lock-cancel"`})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Execute(context.Background(), ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	<-dispatchBlocked

	gateHeld := make(chan struct{})
	releaseGate := make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		gateDone <- manager.WithGenerationGate(context.Background(), "connection-1", "1", func(context.Context) error {
			close(gateHeld)
			<-releaseGate
			return nil
		})
	}()
	<-gateHeld
	lockDone := make(chan error, 1)
	go func() {
		_, err := manager.Lock(context.Background(), protection.LockRequest{
			ConnectionID: "connection-1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	waitForOperationLockPending(t, manager)
	type cancelResult struct {
		operation Operation
		err       error
	}
	cancelDone := make(chan cancelResult, 1)
	go func() {
		operation, err := service.Cancel(context.Background(), created.Task.ID, CommandEnvelope{
			CommandRequestID: uuid.NewString(), ExpectedStateRevision: created.Task.StateRevision,
		})
		cancelDone <- cancelResult{operation: operation, err: err}
	}()
	close(releaseGate)
	if err := <-gateDone; err != nil {
		t.Fatal(err)
	}
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	canceled := <-cancelDone
	if canceled.err != nil || canceled.operation.Task.State != StateCanceledNotSent {
		t.Fatalf("cancel=%+v err=%v", canceled.operation, canceled.err)
	}
	protectionSnapshot, err := manager.Get("connection-1", "1")
	if err != nil || protectionSnapshot.Mode != protection.ProtectedLocked || protectionSnapshot.ProtectionRevision != "3" {
		t.Fatalf("protection=%+v err=%v", protectionSnapshot, err)
	}
	close(releaseDispatch)
	waitForOperationControlRemoval(t, service, created.Task.ID)
	if hits.Load() != 0 {
		t.Fatalf("network hits=%d", hits.Load())
	}
}

func TestLockAfterStartBarrierAllowsOnlyEnteredRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var hits atomic.Int32
	service, manager, database, server := newOperationService(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			close(entered)
		}
		<-release
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"statement_id":0}]}`))
	}))
	defer server.Close()
	defer database.Close()
	unlock(t, manager)
	preview, err := service.Preview(context.Background(), PreviewRequest{Query: `CREATE DATABASE "new-db"`})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Execute(context.Background(), ExecuteRequest{
		ClientRequestID: uuid.NewString(), PreviewToken: *preview.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation did not cross start barrier")
	}
	lockDone := make(chan error, 1)
	go func() {
		_, err := manager.Lock(context.Background(), protection.LockRequest{
			ConnectionID: "connection-1", CommandRequestID: uuid.NewString(), ExpectedConnectionGeneration: "1",
		})
		lockDone <- err
	}()
	select {
	case err := <-lockDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Lock remained blocked after start barrier")
	}
	close(release)
	finished := waitForOperation(t, service, created.Task.ID)
	if finished.Task.State != StateSucceeded || hits.Load() != 1 {
		t.Fatalf("finished=%+v hits=%d", finished, hits.Load())
	}
}

func waitForOperationLockPending(t *testing.T, manager *protection.Manager) {
	t.Helper()
	request := protection.UnlockRequest{
		ConnectionID: "connection-1", CommandRequestID: uuid.NewString(),
		ExpectedConnectionGeneration: "1", ExpectedProtectionRevision: "2",
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		_, err := manager.Unlock(ctx, request)
		cancel()
		if errors.Is(err, protection.ErrLockPending) {
			return
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lock-pending probe error=%v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("Lock did not become pending")
		}
	}
}

func waitForOperation(t *testing.T, service *Service, id string) Operation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		value, err := service.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if value.Task.Terminal {
			return value
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation did not finish: %+v", value)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForOperationControlRemoval(t *testing.T, service *Service, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for service.operationControl(id) != nil {
		if time.Now().After(deadline) {
			t.Fatal("operation control was not released")
		}
		time.Sleep(time.Millisecond)
	}
}

// database/sql intentionally does not expose its backing path. The test only
// performs the raw file scan when the modernc driver reports it through PRAGMA.
func databasePath(t *testing.T, database *store.Store) string {
	t.Helper()
	var sequence int
	var name, path string
	if err := database.DB().QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
