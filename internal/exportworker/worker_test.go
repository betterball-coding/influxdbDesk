package exportworker

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

func TestMeasurementExportRunsThroughTransferLaneAndCommitsArtifacts(t *testing.T) {
	server, queries := newInfluxExportServer(t)
	defer server.Close()
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.CloseIdleConnections()

	jobID := uuid.NewString()
	generation := exportlane.GenerationKey{ConnectionID: "connection-1", Generation: "7"}
	plan, err := BuildPlan(jobID, generation, MeasurementSpec{
		Database: "db", RetentionPolicy: "autogen", Measurement: "cpu",
		StartNS: "1", EndNS: "2", OutputDirectory: t.TempDir(), TypePreserving: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fragments := newMemoryFragments()
	reservations := newMemoryReservations()
	worker, err := New(
		func(got exportlane.GenerationKey) (RoundTripDispatcher, error) {
			if got != generation {
				return nil, ErrQuantumMismatch
			}
			return dispatcher, nil
		},
		fragments,
		reservations.hooks(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	lanes := scheduler.NewReadLanes()
	laneScheduler, err := exportlane.New(worker, func(exportlane.GenerationKey) *scheduler.ReadLanes { return lanes })
	if err != nil {
		t.Fatal(err)
	}
	defer laneScheduler.Close()
	request, err := plan.LaneRequest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := laneScheduler.Submit(request); err != nil {
		t.Fatal(err)
	}
	waitExportState(t, laneScheduler, jobID, exportlane.StateSucceeded)
	if snapshot := lanes.Snapshot(); snapshot.TransferInFlight != 0 || snapshot.InteractiveInFlight != 0 {
		t.Fatalf("lane snapshot after completion = %+v", snapshot)
	}

	queries.mu.Lock()
	gotQueries := append([]string(nil), queries.values...)
	queries.mu.Unlock()
	if len(gotQueries) != 3 {
		t.Fatalf("queries = %v, want two metadata and one slice", gotQueries)
	}
	if !strings.HasPrefix(gotQueries[0], "SHOW TAG KEYS") ||
		!strings.HasPrefix(gotQueries[1], "SHOW FIELD KEYS") ||
		!strings.HasPrefix(gotQueries[2], "SELECT *") ||
		!strings.Contains(gotQueries[2], "time >= 1") ||
		!strings.Contains(gotQueries[2], "time < 2") ||
		!strings.Contains(gotQueries[2], "GROUP BY *") {
		t.Fatalf("wire queries are not the planned metadata and half-open slice: %v", gotQueries)
	}

	validated, err := transfer.ValidateGzipArtifact(plan.Slices[0].FinalPath)
	if err != nil {
		t.Fatal(err)
	}
	if validated.SHA256 == "" {
		t.Fatal("data checksum is empty")
	}
	data := readGzip(t, plan.Slices[0].FinalPath)
	wantLP := "cpu,host=a count=9007199254740993i,ratio=0.1 1\n"
	if data != wantLP {
		t.Fatalf("LP = %q, want %q", data, wantLP)
	}

	manifest, _, ok := worker.Manifest(jobID)
	if !ok {
		t.Fatal("manifest was not committed")
	}
	if manifest.SnapshotConsistent || !manifest.TypePreserving || manifest.Lossy ||
		manifest.ExportKind != "LOGICAL_EXPORT" || len(manifest.Fragments) != 1 ||
		manifest.Fragments[0].SHA256 != validated.SHA256 {
		t.Fatalf("manifest = %+v", manifest)
	}
	manifestBytes, err := os.ReadFile(plan.ManifestFinalPath)
	if err != nil {
		t.Fatal(err)
	}
	var diskManifest Manifest
	if err := json.Unmarshal(manifestBytes, &diskManifest); err != nil || diskManifest.SchemaVersion != ManifestSchemaV1 {
		t.Fatalf("disk manifest=%+v error=%v", diskManifest, err)
	}
	if fragments.state(plan.Slices[0].FragmentID) != exportjob.FragmentComplete ||
		fragments.state(plan.ManifestFragmentID) != exportjob.FragmentComplete {
		t.Fatalf("fragment states = %+v", fragments.states())
	}
	if !reservations.settled(plan.Slices[0].FragmentID) || !reservations.settled(plan.ManifestFragmentID) ||
		reservations.outstanding() != 0 {
		t.Fatalf("reservation accounting = %+v", reservations.snapshot())
	}
}

func TestCanceledArtifactWriteDeletesPartAndMarksFragmentCorrupt(t *testing.T) {
	server, _ := newInfluxExportServer(t)
	defer server.Close()
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.CloseIdleConnections()

	jobID := uuid.NewString()
	generation := exportlane.GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	plan, err := BuildPlan(jobID, generation, MeasurementSpec{
		Database: "db", RetentionPolicy: "autogen", Measurement: "cpu",
		StartNS: "1", EndNS: "2", OutputDirectory: t.TempDir(), TypePreserving: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fragments := newMemoryFragments()
	reserveEntered := make(chan struct{})
	aborted := make(chan struct{}, 1)
	worker, err := New(
		func(exportlane.GenerationKey) (RoundTripDispatcher, error) { return dispatcher, nil },
		fragments,
		TargetReservationHooks{
			Reserve: func(ctx context.Context, _, _ string, _ int64) error {
				select {
				case <-reserveEntered:
				default:
					close(reserveEntered)
				}
				<-ctx.Done()
				return ctx.Err()
			},
			Settle: func(context.Context, string, string, int64) error { return nil },
			Abort: func(context.Context, string, string) error {
				aborted <- struct{}{}
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	timeouts := exportlane.Timeouts{ResponseHeader: time.Second, ChunkIdle: time.Second, Absolute: time.Second}
	for _, quantum := range plan.Quanta[:2] {
		if err := worker.ExecuteQuantum(context.Background(), generation, jobID, quantum, timeouts); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.ExecuteQuantum(ctx, generation, jobID, plan.Quanta[2], timeouts) }()
	select {
	case <-reserveEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("artifact writer did not request an extent")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ExecuteQuantum() error = %v", err)
	}
	if _, err := os.Stat(plan.Slices[0].PartPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part file remains after cancellation: %v", err)
	}
	if state := fragments.state(plan.Slices[0].FragmentID); state != exportjob.FragmentCorrupt {
		t.Fatalf("fragment state = %s, want CORRUPT", state)
	}
	select {
	case <-aborted:
	default:
		t.Fatal("canceled artifact reservation was not aborted")
	}
}

func TestBuildPlanUsesDeterministicHalfOpenSlices(t *testing.T) {
	jobID := uuid.NewString()
	generation := exportlane.GenerationKey{ConnectionID: "connection", Generation: "9"}
	directory := t.TempDir()
	first, err := BuildPlan(jobID, generation, MeasurementSpec{
		Database: "metrics", RetentionPolicy: "autogen", Measurement: "cpu load",
		StartNS: "0", EndNS: "10", SliceWidthNS: "4", OutputDirectory: directory,
		TypePreserving: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlan(jobID, generation, first.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("BuildPlan is not deterministic")
	}
	want := [][2]string{{"0", "4"}, {"4", "8"}, {"8", "10"}}
	if len(first.Slices) != len(want) || len(first.Quanta) != 2+len(want) {
		t.Fatalf("plan slices=%d quanta=%d", len(first.Slices), len(first.Quanta))
	}
	for index, bounds := range want {
		slice := first.Slices[index]
		if slice.StartNS != bounds[0] || slice.EndNS != bounds[1] {
			t.Fatalf("slice %d = [%s,%s), want [%s,%s)", index, slice.StartNS, slice.EndNS, bounds[0], bounds[1])
		}
		queryText := first.Quanta[index+2].Request.Query
		if !strings.Contains(queryText, "time >= "+bounds[0]) || !strings.Contains(queryText, "time < "+bounds[1]) ||
			!strings.Contains(queryText, `"metrics"."autogen"."cpu load"`) {
			t.Fatalf("slice query %d = %q", index, queryText)
		}
	}
}

func TestResponseHeaderTimeoutLeavesNoArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-time.After(250 * time.Millisecond):
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0}]}`)
		case <-request.Context().Done():
		}
	}))
	defer server.Close()
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.CloseIdleConnections()
	jobID := uuid.NewString()
	generation := exportlane.GenerationKey{ConnectionID: "connection", Generation: "1"}
	plan, err := BuildPlan(jobID, generation, MeasurementSpec{
		Database: "db", RetentionPolicy: "autogen", Measurement: "cpu",
		StartNS: "1", EndNS: "2", OutputDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(
		func(exportlane.GenerationKey) (RoundTripDispatcher, error) { return dispatcher, nil },
		newMemoryFragments(), newMemoryReservations().hooks(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = worker.ExecuteQuantum(context.Background(), generation, jobID, plan.Quanta[0], exportlane.Timeouts{
		ResponseHeader: 25 * time.Millisecond, ChunkIdle: time.Second, Absolute: time.Second,
	})
	if !errors.Is(err, exportlane.ErrHeaderTimeout) {
		t.Fatalf("ExecuteQuantum() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("header timeout elapsed = %v", elapsed)
	}
	if matches, err := filepath.Glob(filepath.Join(plan.Spec.OutputDirectory, "*.part")); err != nil || len(matches) != 0 {
		t.Fatalf("part files after header timeout = %v, error=%v", matches, err)
	}
}

type recordedQueries struct {
	mu     sync.Mutex
	values []string
}

func newInfluxExportServer(t *testing.T) (*httptest.Server, *recordedQueries) {
	t.Helper()
	recorded := &recordedQueries{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/query" || request.Method != http.MethodPost {
			http.NotFound(writer, request)
			return
		}
		if err := request.ParseForm(); err != nil {
			t.Errorf("ParseForm() error = %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		queryText := request.Form.Get("q")
		recorded.mu.Lock()
		recorded.values = append(recorded.values, queryText)
		recorded.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(queryText, "SHOW TAG KEYS"):
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"]]}]}]}`)
		case strings.HasPrefix(queryText, "SHOW FIELD KEYS"):
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["count","integer"],["ratio","float"]]}]}]}`)
		case strings.HasPrefix(queryText, "SELECT *"):
			_, _ = io.WriteString(writer, `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","count","ratio"],"values":[[1,9007199254740993,0.1]]}]}]}`)
		default:
			t.Errorf("unexpected query %q", queryText)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	return server, recorded
}

func waitExportState(t *testing.T, laneScheduler *exportlane.Scheduler, jobID string, want exportlane.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := laneScheduler.Get(jobID)
		if err == nil && snapshot.State == want {
			return
		}
		if err == nil && snapshot.Terminal && snapshot.State != want {
			t.Fatalf("export terminal state = %+v", snapshot)
		}
		time.Sleep(5 * time.Millisecond)
	}
	snapshot, _ := laneScheduler.Get(jobID)
	t.Fatalf("timed out waiting for %s: %+v", want, snapshot)
}

func readGzip(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type memoryFragments struct {
	mu        sync.Mutex
	fragments map[string]exportjob.Fragment
}

func newMemoryFragments() *memoryFragments {
	return &memoryFragments{fragments: make(map[string]exportjob.Fragment)}
}

func (m *memoryFragments) BeginFragment(_ context.Context, request exportjob.BeginFragmentRequest) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.fragments[request.FragmentID]; exists {
		return exportjob.Fragment{}, exportjob.ErrFragmentState
	}
	fragment := exportjob.Fragment{
		FragmentID: request.FragmentID, JobID: request.JobID, Ordinal: request.Ordinal,
		Kind: request.Kind, State: exportjob.FragmentWriting, StartNS: request.StartNS,
		EndNS: request.EndNS, PartPath: stringPointer(request.PartPath),
	}
	m.fragments[request.FragmentID] = fragment
	return fragment, nil
}

func (m *memoryFragments) MarkFragmentFinalizing(_ context.Context, request exportjob.FinalizeFragmentRequest) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fragment, found := m.fragments[request.FragmentID]
	if !found || fragment.State != exportjob.FragmentWriting {
		return exportjob.Fragment{}, exportjob.ErrFragmentState
	}
	fragment.State = exportjob.FragmentFinalizing
	fragment.FinalPath = stringPointer(request.FinalPath)
	fragment.CompressedSize = stringPointer(request.CompressedSize)
	fragment.UncompressedSize = stringPointer(request.UncompressedSize)
	fragment.ChecksumSHA256 = stringPointer(request.ChecksumSHA256)
	m.fragments[request.FragmentID] = fragment
	return fragment, nil
}

func (m *memoryFragments) CompleteFragment(_ context.Context, id string) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fragment, found := m.fragments[id]
	if !found || fragment.State != exportjob.FragmentFinalizing {
		return exportjob.Fragment{}, exportjob.ErrFragmentState
	}
	fragment.State = exportjob.FragmentComplete
	m.fragments[id] = fragment
	return fragment, nil
}

func (m *memoryFragments) MarkFragmentCorrupt(_ context.Context, id string) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fragment, found := m.fragments[id]
	if !found {
		return exportjob.Fragment{}, exportjob.ErrFragmentNotFound
	}
	fragment.State = exportjob.FragmentCorrupt
	m.fragments[id] = fragment
	return fragment, nil
}

func (m *memoryFragments) RetryCorruptFragment(_ context.Context, id, partPath string) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fragment, found := m.fragments[id]
	if !found || fragment.State != exportjob.FragmentCorrupt {
		return exportjob.Fragment{}, exportjob.ErrFragmentState
	}
	fragment.State, fragment.PartPath = exportjob.FragmentWriting, stringPointer(partPath)
	m.fragments[id] = fragment
	return fragment, nil
}

func (m *memoryFragments) GetFragment(_ context.Context, id string) (exportjob.Fragment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fragment, found := m.fragments[id]
	if !found {
		return exportjob.Fragment{}, exportjob.ErrFragmentNotFound
	}
	return fragment, nil
}

func (m *memoryFragments) state(id string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fragments[id].State
}

func (m *memoryFragments) states() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]string, len(m.fragments))
	for id, fragment := range m.fragments {
		result[id] = fragment.State
	}
	return result
}

func stringPointer(value string) *string { return &value }

type memoryReservations struct {
	mu       sync.Mutex
	reserved map[string]int64
	actual   map[string]int64
	aborted  map[string]bool
}

func newMemoryReservations() *memoryReservations {
	return &memoryReservations{
		reserved: make(map[string]int64), actual: make(map[string]int64), aborted: make(map[string]bool),
	}
}

func (m *memoryReservations) hooks() TargetReservationHooks {
	return TargetReservationHooks{
		Reserve: func(_ context.Context, _, artifactID string, bytes int64) error {
			m.mu.Lock()
			m.reserved[artifactID] += bytes
			m.mu.Unlock()
			return nil
		},
		Settle: func(_ context.Context, _, artifactID string, actual int64) error {
			m.mu.Lock()
			defer m.mu.Unlock()
			if actual < 0 || actual > m.reserved[artifactID] {
				return errors.New("invalid test settlement")
			}
			m.actual[artifactID] = actual
			m.reserved[artifactID] = actual
			return nil
		},
		Abort: func(_ context.Context, _, artifactID string) error {
			m.mu.Lock()
			m.aborted[artifactID] = true
			delete(m.reserved, artifactID)
			m.mu.Unlock()
			return nil
		},
	}
}

func (m *memoryReservations) settled(artifactID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, found := m.actual[artifactID]
	return found
}

func (m *memoryReservations) outstanding() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var outstanding int64
	for artifactID, reserved := range m.reserved {
		outstanding += reserved - m.actual[artifactID]
	}
	return outstanding
}

func (m *memoryReservations) snapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]int64, len(m.reserved))
	for artifactID, reserved := range m.reserved {
		result[artifactID] = reserved - m.actual[artifactID]
	}
	return result
}

var _ FragmentRepository = (*memoryFragments)(nil)
