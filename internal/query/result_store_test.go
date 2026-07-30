package query

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/transport"
)

func TestTypedRowV1RoundTripPreservesEveryScalar(t *testing.T) {
	t.Parallel()

	trueValue := true
	row := []TypedScalar{
		{Kind: ScalarNull},
		{Kind: ScalarString, StringValue: "文本/plaintext"},
		{Kind: ScalarBoolean, BooleanValue: &trueValue},
		{Kind: ScalarTimestampNS, DecimalText: "1700000000000000001"},
		{Kind: ScalarInt64, DecimalText: "9007199254740993"},
		{Kind: ScalarUint64, DecimalText: "18446744073709551615"},
		{Kind: ScalarFloat64, DecimalText: "-0.0"},
		{Kind: ScalarNumericText, DecimalText: "1e1000"},
	}
	encoded, err := encodeTypedRowV1(row)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeTypedRowV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(row) {
		t.Fatalf("decoded scalar count = %d, want %d", len(decoded), len(row))
	}
	for i := range row {
		if decoded[i].Kind != row[i].Kind || decoded[i].DecimalText != row[i].DecimalText || decoded[i].StringValue != row[i].StringValue {
			t.Fatalf("scalar[%d] = %#v, want %#v", i, decoded[i], row[i])
		}
		if row[i].BooleanValue != nil && (decoded[i].BooleanValue == nil || *decoded[i].BooleanValue != *row[i].BooleanValue) {
			t.Fatalf("boolean scalar[%d] = %#v, want %#v", i, decoded[i], row[i])
		}
	}
}

func TestServiceSpillsEncryptedExactRowsAndCloseDeletesFile(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","signed","unsigned","ratio","unknown","label"],"values":[[1700000000000000001,9007199254740993,18446744073709551615,-0.0,1e1000,"spill-canary"]]}]}]}`}
	service, err := NewService(dispatcher, ServiceOptions{
		SpillDirectory:         directory,
		ResultMemoryLimitBytes: 1,
		NumericResolver: NumericKindResolverFunc(func(ctx NumericContext) (ScalarKind, bool) {
			switch ctx.Column {
			case "signed":
				return ScalarInt64, true
			case "unsigned":
				return ScalarUint64, true
			case "ratio":
				return ScalarFloat64, true
			default:
				return "", false
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT * FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionSucceeded || snapshot.RowCount != "1" || !snapshot.ResultAvailable {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	spillPath := serviceSpillPath(t, service, snapshot.ID)
	fileBytes, err := os.ReadFile(spillPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, plaintext := range [][]byte{[]byte("spill-canary"), []byte("9007199254740993"), []byte("18446744073709551615")} {
		if bytes.Contains(fileBytes, plaintext) {
			t.Fatalf("spill contains plaintext %q", plaintext)
		}
	}

	series, err := service.ListResultSeries(snapshot.ID, 0)
	if err != nil || len(series) != 1 || series[0].Rows != "1" {
		t.Fatalf("series = %#v, error = %v", series, err)
	}
	page, err := service.GetResultPage(PageRequest{SessionID: snapshot.ID, StatementID: 0, SeriesID: series[0].ID, Limit: 10})
	if err != nil || !page.EOF || len(page.Rows) != 1 {
		t.Fatalf("page = %#v, error = %v", page, err)
	}
	want := []string{"1700000000000000001", "9007199254740993", "18446744073709551615", "-0.0", "1e1000"}
	for i, expected := range want {
		if page.Rows[0][i].DecimalText != expected {
			t.Fatalf("decimalText[%d] = %q, want %q", i, page.Rows[0][i].DecimalText, expected)
		}
	}
	if page.Rows[0][5].StringValue != "spill-canary" {
		t.Fatalf("string value = %q", page.Rows[0][5].StringValue)
	}

	closed, err := service.CloseQuery(context.Background(), snapshot.ID, commandFor(snapshot))
	if err != nil || closed.ResultAvailable {
		t.Fatalf("closed snapshot = %#v, error = %v", closed, err)
	}
	if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill still exists after CloseQuery: %v", err)
	}
}

func TestServiceTruncatesAtRowLimitAndClosesBody(t *testing.T) {
	t.Parallel()

	body := &trackingReadCloser{Reader: strings.NewReader(
		`{"results":[{"statement_id":0,"partial":true,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,10],[2,20],[3,30]],"partial":true}]}]}` + "\n" +
			`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[4,40]]}]}]}`,
	)}
	service, err := NewService(&bodyDispatcher{body: body}, ServiceOptions{ResultMaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionTruncated || snapshot.RowCount != "2" || snapshot.Complete || !snapshot.ResultAvailable {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if body.closed.Load() != 1 {
		t.Fatalf("response body close count = %d, want 1", body.closed.Load())
	}
	series, err := service.ListResultSeries(snapshot.ID, 0)
	if err != nil || len(series) != 1 || series[0].Rows != "2" {
		t.Fatalf("series = %#v, error = %v", series, err)
	}
	_, _ = service.CloseQuery(context.Background(), snapshot.ID, commandFor(snapshot))
}

func TestServiceTruncatesBeforeExceedingByteLimit(t *testing.T) {
	t.Parallel()

	row, err := encodeTypedRowV1([]TypedScalar{
		{Kind: ScalarTimestampNS, DecimalText: "1"},
		{Kind: ScalarNumericText, DecimalText: "10"},
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,10]]}]}]}`}
	service, err := NewService(dispatcher, ServiceOptions{ResultMaxBytes: uint64(len(row) - 1)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionTruncated || snapshot.RowCount != "0" || !snapshot.ResultAvailable {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	_, _ = service.CloseQuery(context.Background(), snapshot.ID, commandFor(snapshot))

	exactService, err := NewService(dispatcher, ServiceOptions{
		ResultMemoryLimitBytes: uint64(len(row) + 1),
		ResultMaxBytes:         uint64(len(row)),
	})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := exactService.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	exact = waitTerminal(t, exactService, exact.ID)
	if exact.State != SessionTruncated || exact.RowCount != "1" || !exact.ResultAvailable {
		t.Fatalf("exact-boundary snapshot = %#v", exact)
	}
	_, _ = exactService.CloseQuery(context.Background(), exact.ID, commandFor(exact))
}

func TestServiceFailsClosedWhenSpillDirectoryIsInvalid(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	invalidPath := filepath.Join(directory, "not-a-directory")
	if err := os.WriteFile(invalidPath, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,10]]}]}]}`}
	service, err := NewService(dispatcher, ServiceOptions{SpillDirectory: invalidPath, ResultMemoryLimitBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	if snapshot.State != SessionFailed || snapshot.PublicErrorCode != string(ProtocolResultStorage) || snapshot.ResultAvailable {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if strings.Contains(snapshot.PublicSafeMessage, invalidPath) {
		t.Fatal("private spill path leaked into public safe message")
	}
}

func TestServiceRejectsTamperedSpillAndIdleExpiryDeletesIt(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	dispatcher := &fakeDispatcher{response: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,10]]}]}]}`}
	service, err := NewService(dispatcher, ServiceOptions{
		SpillDirectory: directory, ResultMemoryLimitBytes: 1, ResultIdleTTL: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.StartReadQuery(context.Background(), StartRequest{
		ConnectionID: "connection", Generation: "1", Query: `SELECT value FROM cpu`, ClientRequestID: testRequestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot = waitTerminal(t, service, snapshot.ID)
	spillPath := serviceSpillPath(t, service, snapshot.ID)
	series, err := service.ListResultSeries(snapshot.ID, 0)
	if err != nil || len(series) != 1 {
		t.Fatalf("series = %#v, error = %v", series, err)
	}

	file, err := os.OpenFile(spillPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = service.GetResultPage(PageRequest{SessionID: snapshot.ID, StatementID: 0, SeriesID: series[0].ID, Limit: 1})
	requestErr, ok := err.(*RequestError)
	if !ok || requestErr.Code != "RESULT_STORAGE_FAILED" {
		t.Fatalf("page error = %#v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(spillPath); errors.Is(err, os.ErrNotExist) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill still exists after idle expiry: %v", err)
	}
	snapshot, err = service.GetQuerySession(snapshot.ID)
	if err != nil || snapshot.ResultAvailable {
		t.Fatalf("expired snapshot = %#v, error = %v", snapshot, err)
	}
}

type trackingReadCloser struct {
	io.Reader
	closed atomic.Int32
}

func (b *trackingReadCloser) Close() error {
	b.closed.Add(1)
	return nil
}

type bodyDispatcher struct {
	body io.ReadCloser
}

func (d *bodyDispatcher) Dispatch(context.Context, transport.Request) (*transport.Response, error) {
	return &transport.Response{StatusCode: 200, Body: d.body}, nil
}

func serviceSpillPath(t *testing.T, service *Service, sessionID string) string {
	t.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	record := service.sessions[sessionID]
	if record == nil || record.result == nil || record.result.store == nil || record.result.store.spillPath == "" {
		t.Fatal("query result did not create a spill file")
	}
	return record.result.store.spillPath
}
