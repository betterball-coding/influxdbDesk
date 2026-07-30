package query

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
)

func newPersistentQueryService(
	t *testing.T,
	dispatcher Dispatcher,
	now *time.Time,
	options ServiceOptions,
) (*Service, *store.Store, *tasks.Repository, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return *now }
	taskRepository := tasks.NewRepository(database, clock)
	options.Now = clock
	service, err := NewPersistentService(dispatcher, database, taskRepository, options)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	return service, database, taskRepository, path
}

func persistentStartRequest(id string) StartRequest {
	return StartRequest{
		ProfileID: "profile-1", ProfileRevision: "7", ConnectionID: "connection-1",
		Generation: "3", Database: "database-canary", RetentionPolicy: "rp-canary",
		Query: `SELECT value FROM "measurement-canary"`, ClientRequestID: id,
	}
}

func TestPersistentStartReplayConflictAndSecretExclusion(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	rawCanary := "raw-server-error-canary"
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"error":"` + rawCanary + `"}]}`}
	service, database, taskRepository, path := newPersistentQueryService(t, dispatcher, &now, ServiceOptions{})
	defer database.Close()
	request := persistentStartRequest(uuid.NewString())
	created, err := service.StartReadQuery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitTerminal(t, service, created.ID)
	if terminal.State != SessionSucceededWithErrors || terminal.StateRevision != "3" || terminal.LastEventSeq == "0" {
		t.Fatalf("terminal=%+v", terminal)
	}
	if raw, err := service.GetTransientIssue(created.ID); err != nil || raw != rawCanary {
		t.Fatalf("raw=%q err=%v", raw, err)
	}
	var eventCount int
	var taskHead, mirrorHead string
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM task_events WHERE resource_id=?`, created.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT t.last_event_seq,q.last_event_seq
		FROM tasks t JOIN query_sessions q ON q.session_id=t.id WHERE t.id=?`, created.ID).Scan(&taskHead, &mirrorHead); err != nil {
		t.Fatal(err)
	}
	if eventCount != 3 || taskHead != mirrorHead || taskHead != terminal.LastEventSeq {
		t.Fatalf("events=%d taskHead=%s mirrorHead=%s terminalHead=%s", eventCount, taskHead, mirrorHead, terminal.LastEventSeq)
	}

	restartedDispatcher := &fakeDispatcher{}
	restarted, err := NewPersistentService(restartedDispatcher, database, taskRepository, ServiceOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.StartReadQuery(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.ID != created.ID || restartedDispatcher.count() != 0 {
		t.Fatalf("replay=%+v err=%v hits=%d", replayed, err, restartedDispatcher.count())
	}
	conflict := request
	conflict.ProfileRevision = "8"
	if _, err := restarted.StartReadQuery(context.Background(), conflict); requestErrorCode(err) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("profile revision conflict err=%v", err)
	}
	conflict = request
	conflict.Generation = "4"
	if _, err := restarted.StartReadQuery(context.Background(), conflict); requestErrorCode(err) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("connection generation conflict err=%v", err)
	}

	if _, err := database.DB().Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		contents, err := os.ReadFile(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, canary := range []string{request.Query, request.Database, request.RetentionPolicy, rawCanary} {
			if bytes.Contains(contents, []byte(canary)) {
				t.Fatalf("%s contains forbidden canary %q", candidate, canary)
			}
		}
	}
}

func TestPersistentConcurrentStartAndQueueAdmission(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	dispatcher := &fakeDispatcher{
		response: `{"results":[{"statement_id":0}]}`,
		started:  make(chan struct{}, 1), release: make(chan struct{}),
	}
	service, database, _, _ := newPersistentQueryService(t, dispatcher, &now, ServiceOptions{
		MaxInFlightPerConnection: 1, QueueLimitPerConnection: 1,
	})
	defer database.Close()
	request := persistentStartRequest(uuid.NewString())
	const callers = 16
	results := make(chan QuerySession, callers)
	errorsSeen := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			result, err := service.StartReadQuery(context.Background(), request)
			results <- result
			errorsSeen <- err
		}()
	}
	ready.Wait()
	close(start)
	ids := make(map[string]struct{})
	for range callers {
		if err := <-errorsSeen; err != nil {
			t.Fatal(err)
		}
		ids[(<-results).ID] = struct{}{}
	}
	if len(ids) != 1 {
		t.Fatalf("session ids=%v", ids)
	}
	<-dispatcher.started
	queued, err := service.StartReadQuery(context.Background(), persistentStartRequest(uuid.NewString()))
	if err != nil || queued.State != SessionQueued {
		t.Fatalf("queued=%+v err=%v", queued, err)
	}
	if _, err := service.StartReadQuery(context.Background(), persistentStartRequest(uuid.NewString())); requestErrorCode(err) != "QUERY_QUEUE_FULL" {
		t.Fatalf("queue full err=%v", err)
	}
	replay, err := service.StartReadQuery(context.Background(), request)
	if err != nil || !replay.Replayed {
		t.Fatalf("full-queue replay=%+v err=%v", replay, err)
	}
	var taskCount, ledgerCount int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM tasks WHERE kind='QUERY'`).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM idempotency_ledger WHERE resource_kind='QUERY'`).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 2 || ledgerCount != 2 || dispatcher.count() != 1 {
		t.Fatalf("tasks=%d ledgers=%d dispatch=%d", taskCount, ledgerCount, dispatcher.count())
	}
	close(dispatcher.release)
	for id := range ids {
		waitTerminal(t, service, id)
	}
	waitTerminal(t, service, queued.ID)
}

func TestPersistentCancelCloseCommands(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	dispatcher := &fakeDispatcher{
		response: `{"results":[{"statement_id":0}]}`,
		started:  make(chan struct{}, 1), release: make(chan struct{}),
	}
	service, database, _, _ := newPersistentQueryService(t, dispatcher, &now, ServiceOptions{})
	defer database.Close()
	created, err := service.StartReadQuery(context.Background(), persistentStartRequest(uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	<-dispatcher.started
	running, err := service.GetQuerySession(created.ID)
	if err != nil || running.State != SessionRunning {
		t.Fatalf("running=%+v err=%v", running, err)
	}
	if _, err := service.CancelQuery(context.Background(), created.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: "99",
	}); requestErrorCode(err) != "REVISION_CONFLICT" {
		t.Fatalf("revision err=%v", err)
	}
	commandID := uuid.NewString()
	command := CommandEnvelope{CommandRequestID: commandID, ExpectedStateRevision: running.StateRevision}
	canceling, err := service.CancelQuery(context.Background(), created.ID, command)
	if err != nil || canceling.State != SessionCancelRequested {
		t.Fatalf("canceling=%+v err=%v", canceling, err)
	}
	replayed, err := service.CancelQuery(context.Background(), created.ID, command)
	if err != nil || !replayed.Replayed || replayed.ID != canceling.ID ||
		(replayed.State != SessionCancelRequested && replayed.State != SessionCanceled) {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	if _, err := service.CancelQuery(context.Background(), created.ID, CommandEnvelope{
		CommandRequestID: commandID, ExpectedStateRevision: canceling.StateRevision,
	}); requestErrorCode(err) != "COMMAND_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("digest conflict err=%v", err)
	}
	targetSatisfied, err := service.CancelQuery(context.Background(), created.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: running.StateRevision,
	})
	if err != nil || targetSatisfied.ID != canceling.ID ||
		(targetSatisfied.State != SessionCancelRequested && targetSatisfied.State != SessionCanceled) {
		t.Fatalf("target satisfied=%+v err=%v", targetSatisfied, err)
	}
	terminal := waitTerminal(t, service, created.ID)
	if terminal.State != SessionCanceled {
		t.Fatalf("terminal=%+v", terminal)
	}
	closeCommand := CommandEnvelope{CommandRequestID: uuid.NewString(), ExpectedStateRevision: terminal.StateRevision}
	closed, err := service.CloseQuery(context.Background(), created.ID, closeCommand)
	if err != nil || !closed.Closed || closed.ResultAvailable || closed.StateRevision == terminal.StateRevision {
		t.Fatalf("closed=%+v err=%v", closed, err)
	}
	closeReplay, err := service.CloseQuery(context.Background(), created.ID, closeCommand)
	if err != nil || !closeReplay.Replayed || closeReplay.StateRevision != closed.StateRevision {
		t.Fatalf("close replay=%+v err=%v", closeReplay, err)
	}
	closedAgain, err := service.CloseQuery(context.Background(), created.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: terminal.StateRevision,
	})
	if err != nil || closedAgain.StateRevision != closed.StateRevision {
		t.Fatalf("close target satisfied=%+v err=%v", closedAgain, err)
	}
	var ledgers int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM command_ledger WHERE resource_kind='QUERY'`).Scan(&ledgers); err != nil {
		t.Fatal(err)
	}
	if ledgers != 4 {
		t.Fatalf("command ledgers=%d, want 4", ledgers)
	}
}

func TestPersistentResultIdleExpiryUpdatesSnapshotOnly(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,2]]}]}]}`}
	service, database, _, _ := newPersistentQueryService(t, dispatcher, &now, ServiceOptions{})
	defer database.Close()
	created, err := service.StartReadQuery(context.Background(), persistentStartRequest(uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitTerminal(t, service, created.ID)
	if !terminal.ResultAvailable {
		t.Fatalf("terminal=%+v", terminal)
	}
	now = now.Add(2 * time.Hour)
	if err := service.ExpireResults(); err != nil {
		t.Fatal(err)
	}
	expired, err := service.GetQuerySession(created.ID)
	if err != nil || expired.ResultAvailable || expired.StateRevision != terminal.StateRevision ||
		expired.SnapshotRevision == terminal.SnapshotRevision {
		t.Fatalf("expired=%+v terminal=%+v err=%v", expired, terminal, err)
	}
}

func TestPersistentRestartAndRetentionBoundaries(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0}]}`}
	service, database, taskRepository, _ := newPersistentQueryService(t, dispatcher, &now, ServiceOptions{
		MaxInFlightPerConnection: 1,
	})
	defer database.Close()
	request := persistentStartRequest(uuid.NewString())
	laneKey := request.ConnectionID + "\x00" + request.Generation
	service.mu.Lock()
	service.lanes[laneKey] = &queryLane{running: 1}
	service.mu.Unlock()
	queued, err := service.StartReadQuery(context.Background(), request)
	if err != nil || queued.State != SessionQueued {
		t.Fatalf("queued=%+v err=%v", queued, err)
	}
	if _, err := taskRepository.RecoverCore(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := GetStoredSession(context.Background(), database, queued.ID)
	if err != nil || recovered.State != SessionInterrupted || recovered.ResultAvailable {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	restartDispatcher := &fakeDispatcher{}
	restarted, err := NewPersistentService(restartDispatcher, database, taskRepository, ServiceOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.StartReadQuery(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.State != SessionInterrupted || restartDispatcher.count() != 0 {
		t.Fatalf("restart replay=%+v err=%v hits=%d", replayed, err, restartDispatcher.count())
	}
	recovered, err = restarted.CloseQuery(context.Background(), queued.ID, CommandEnvelope{
		CommandRequestID: uuid.NewString(), ExpectedStateRevision: recovered.StateRevision,
	})
	if err != nil || !recovered.Closed {
		t.Fatalf("close recovered=%+v err=%v", recovered, err)
	}

	terminalAt, err := time.Parse(time.RFC3339Nano, recovered.TerminalAtValue())
	if err != nil {
		t.Fatal(err)
	}
	if sweep, err := SweepRetention(context.Background(), database, taskRepository, terminalAt.Add(24*time.Hour-time.Second)); err != nil || sweep != (RetentionSweep{}) {
		t.Fatalf("23:59:59 sweep=%+v err=%v", sweep, err)
	}
	if sweep, err := SweepRetention(context.Background(), database, taskRepository, terminalAt.Add(24*time.Hour)); err != nil || sweep.Archived != 1 {
		t.Fatalf("24h sweep=%+v err=%v", sweep, err)
	}
	archived, err := GetStoredSession(context.Background(), database, queued.ID)
	if err != nil || !archived.Archived || archived.ResultAvailable || archived.ConnectionID != "" {
		t.Fatalf("archived=%+v err=%v", archived, err)
	}
	replayed, err = restarted.StartReadQuery(context.Background(), request)
	if err != nil || !replayed.Replayed || !replayed.Archived || restartDispatcher.count() != 0 {
		t.Fatalf("archived replay=%+v err=%v hits=%d", replayed, err, restartDispatcher.count())
	}
	if sweep, err := SweepRetention(context.Background(), database, taskRepository, terminalAt.Add(90*time.Hour*24-time.Second)); err != nil || sweep.Purged != 0 {
		t.Fatalf("89d23:59:59 sweep=%+v err=%v", sweep, err)
	}
	if sweep, err := SweepRetention(context.Background(), database, taskRepository, terminalAt.Add(queryResourceRetention)); err != nil || sweep.Purged != 1 {
		t.Fatalf("90d sweep=%+v err=%v", sweep, err)
	}
	if _, err := GetStoredSession(context.Background(), database, queued.ID); requestErrorCode(err) != "QUERY_NOT_FOUND" {
		t.Fatalf("post-purge err=%v", err)
	}
	var idempotencyRows, commandRows int
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM idempotency_ledger WHERE resource_id=?`, queued.ID).Scan(&idempotencyRows); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM command_ledger WHERE resource_id=?`, queued.ID).Scan(&commandRows); err != nil {
		t.Fatal(err)
	}
	if idempotencyRows != 0 || commandRows != 0 {
		t.Fatalf("post-purge idempotency=%d commands=%d", idempotencyRows, commandRows)
	}
}

func (s QuerySession) TerminalAtValue() string {
	if s.TerminalAt == nil {
		return ""
	}
	return *s.TerminalAt
}

func requestErrorCode(err error) string {
	var requestErr *RequestError
	if errors.As(err, &requestErr) {
		return requestErr.Code
	}
	return ""
}
