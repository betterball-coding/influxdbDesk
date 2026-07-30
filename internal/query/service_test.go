package query

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/transport"
)

const testRequestID = "11111111-1111-4111-8111-111111111111"

type fakeDispatcher struct {
	mu       sync.Mutex
	requests []transport.Request
	response string
	status   int
	started  chan struct{}
	release  chan struct{}
}

func (f *fakeDispatcher) Dispatch(ctx context.Context, request transport.Request) (*transport.Response, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	status := f.status
	if status == 0 {
		status = 200
	}
	return &transport.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(f.response))}, nil
}

func (f *fakeDispatcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func TestServiceRejectsMutationsBeforeDispatch(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{}
	service, err := NewService(dispatcher, ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value INTO out FROM cpu`, ClientRequestID: testRequestID,
	})
	requestErr, ok := err.(*RequestError)
	if !ok || requestErr.Code != "QUERY_NOT_READ_ONLY" {
		t.Fatalf("error = %#v", err)
	}
	if dispatcher.count() != 0 {
		t.Fatal("rejected query reached dispatcher")
	}
}

func TestQueryExactMetadataDoesNotWrapAtUint64(t *testing.T) {
	t.Parallel()
	if got := incrementDecimal("18446744073709551615"); got != "18446744073709551616" {
		t.Fatalf("incrementDecimal wrapped: %s", got)
	}
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0}]}`}
	service, err := NewService(dispatcher, ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "18446744073709551616",
		Query: `SHOW DATABASES`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal := waitTerminal(t, service, snapshot.ID); terminal.Generation != "18446744073709551616" {
		t.Fatalf("generation changed: %s", terminal.Generation)
	}
}

func TestServiceExecutesAndPagesExactResult(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","value"],"values":[[1700000000000000001,9007199254740993],[1700000000000000002,9007199254740994]]}]}]}`}
	service, err := NewService(dispatcher, ServiceOptions{
		NumericResolver: NumericKindResolverFunc(func(NumericContext) (ScalarKind, bool) { return ScalarInt64, true }),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Database: "metrics", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionSucceeded || snapshot.RowCount != "2" || !snapshot.ResultAvailable {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	series, err := service.ListResultSeries(snapshot.ID, 0)
	if err != nil || len(series) != 1 {
		t.Fatalf("series = %#v, error = %v", series, err)
	}
	first, err := service.GetResultPage(PageRequest{SessionID: snapshot.ID, StatementID: 0, SeriesID: series[0].ID, Limit: 1})
	if err != nil || first.EOF || first.NextCursor == "" {
		t.Fatalf("first page = %#v, error = %v", first, err)
	}
	if got := first.Rows[0][1].DecimalText; got != "9007199254740993" {
		t.Fatalf("exact value = %q", got)
	}
	second, err := service.GetResultPage(PageRequest{SessionID: snapshot.ID, Cursor: first.NextCursor, Limit: 1})
	if err != nil || !second.EOF || second.Rows[0][1].DecimalText != "9007199254740994" {
		t.Fatalf("second page = %#v, error = %v", second, err)
	}
}

func TestResultTailOnlyReportsEOFAfterTerminalState(t *testing.T) {
	t.Parallel()

	service, err := NewService(&fakeDispatcher{}, ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	series := &Series{
		ID: "series-1", StatementID: 0, Measurement: "cpu", Columns: []string{"value"},
		Rows: [][]TypedScalar{{{Kind: ScalarInt64, DecimalText: "9007199254740993"}}}, RowCount: 1,
	}
	service.mu.Lock()
	service.sessions["session-1"] = &sessionRecord{
		snapshot: QuerySession{
			ID: "session-1", State: SessionRunning, SnapshotRevision: "1", StateRevision: "1",
			ConnectionID: "connection", Generation: "1", ResultAvailable: true,
		},
		result:           &ResultSet{Statements: []StatementResult{{ID: 0, Series: []*Series{series}}}},
		resultLastAccess: time.Now(),
	}
	service.mu.Unlock()

	first, err := service.GetResultPage(PageRequest{
		SessionID: "session-1", StatementID: 0, SeriesID: series.ID, Limit: 10,
	})
	if err != nil || first.EOF || first.NextCursor == "" || len(first.Rows) != 1 {
		t.Fatalf("running first page = %#v, error = %v", first, err)
	}
	tail, err := service.GetResultPage(PageRequest{SessionID: "session-1", Cursor: first.NextCursor, Limit: 10})
	if err != nil || tail.EOF || tail.NextCursor != first.NextCursor || len(tail.Rows) != 0 {
		t.Fatalf("running tail page = %#v, error = %v", tail, err)
	}

	service.mu.Lock()
	record := service.sessions["session-1"]
	record.snapshot.Terminal = true
	record.snapshot.State = SessionSucceeded
	service.mu.Unlock()
	terminalTail, err := service.GetResultPage(PageRequest{SessionID: "session-1", Cursor: first.NextCursor, Limit: 10})
	if err != nil || !terminalTail.EOF || terminalTail.NextCursor != "" || len(terminalTail.Rows) != 0 {
		t.Fatalf("terminal tail page = %#v, error = %v", terminalTail, err)
	}
}

func TestServiceIdempotencyAndQueueLimit(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{
		response: `{"results":[{"statement_id":0}]}`,
		started:  make(chan struct{}, 1), release: make(chan struct{}),
	}
	service, err := NewService(dispatcher, ServiceOptions{MaxInFlightPerConnection: 1, QueueLimitPerConnection: 1})
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := StartRequest{ConnectionID: "connection", Generation: "1", Query: `SHOW DATABASES`, ClientRequestID: testRequestID}
	first, err := service.StartReadQuery(context.Background(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	<-dispatcher.started
	replayed, err := service.StartReadQuery(context.Background(), firstRequest)
	if err != nil || !replayed.Replayed || replayed.ID != first.ID {
		t.Fatalf("replay = %#v, error = %v", replayed, err)
	}
	_, err = service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SHOW USERS`, ClientRequestID: "22222222-2222-4222-8222-222222222222",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SHOW QUERIES`, ClientRequestID: "33333333-3333-4333-8333-333333333333",
	})
	requestErr, ok := err.(*RequestError)
	if !ok || requestErr.Code != "QUERY_QUEUE_FULL" {
		t.Fatalf("queue error = %#v", err)
	}
	close(dispatcher.release)
	waitTerminal(t, service, first.ID)
}

func TestServiceStatementErrorAndTransientIssue(t *testing.T) {
	t.Parallel()

	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"error":"secret measurement failed"}]}`}
	service, _ := NewService(dispatcher, ServiceOptions{})
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionSucceededWithErrors || !snapshot.HasStatementErrors {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if strings.Contains(snapshot.PublicSafeMessage, "secret") {
		t.Fatal("raw server error leaked into public snapshot")
	}
	raw, err := service.GetTransientIssue(snapshot.ID)
	if err != nil || raw != "secret measurement failed" {
		t.Fatalf("raw = %q, error = %v", raw, err)
	}
}

func waitTerminal(t *testing.T, service *Service, id string) QuerySession {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := service.GetQuerySession(id)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Terminal {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("query did not reach a terminal state")
	return QuerySession{}
}

func commandFor(snapshot QuerySession) CommandEnvelope {
	return CommandEnvelope{
		CommandRequestID: testRequestID, ExpectedStateRevision: snapshot.StateRevision,
	}
}
