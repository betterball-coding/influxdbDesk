package exportlane

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type executorFunc func(context.Context, GenerationKey, string, Quantum, Timeouts) error

func (f executorFunc) ExecuteQuantum(
	ctx context.Context,
	generation GenerationKey,
	jobID string,
	quantum Quantum,
	timeouts Timeouts,
) error {
	return f(ctx, generation, jobID, quantum, timeouts)
}

type executionCall struct {
	generation GenerationKey
	jobID      string
	quantumID  string
	timeouts   Timeouts
}

func TestSchedulerRotatesJobsAfterEveryQuantum(t *testing.T) {
	started := make(chan executionCall, 8)
	proceed := make(chan struct{})
	s := newTestScheduler(t, executorFunc(func(
		ctx context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		started <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), nil)

	generation := GenerationKey{ConnectionID: "connection-1", Generation: "7"}
	mustSubmit(t, s, testJob("A", generation, "A1", "A2", "A3"))
	first := receiveCall(t, started)
	mustSubmit(t, s, testJob("B", generation, "B1", "B2"))

	want := []string{"A1", "B1", "A2", "B2", "A3"}
	got := []string{first.quantumID}
	for range want[1:] {
		proceed <- struct{}{}
		got = append(got, receiveCall(t, started).quantumID)
	}
	proceed <- struct{}{}

	waitForState(t, s, "A", StateSucceeded)
	waitForState(t, s, "B", StateSucceeded)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("quantum order = %v, want %v", got, want)
	}
}

func TestSchedulerAllowsOnlyOneInFlightQuantumPerGeneration(t *testing.T) {
	started := make(chan executionCall, 2)
	proceed := make(chan struct{})
	lanes := scheduler.NewReadLanes()
	s := newTestScheduler(t, blockingExecutor(started, proceed), func(GenerationKey) *scheduler.ReadLanes {
		return lanes
	})

	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	mustSubmit(t, s, testJob("A", generation, "A1"))
	mustSubmit(t, s, testJob("B", generation, "B1"))
	first := receiveCall(t, started)
	if first.jobID != "A" {
		t.Fatalf("first job = %q, want A", first.jobID)
	}
	if snapshot := lanes.Snapshot(); snapshot.TransferInFlight != 1 {
		t.Fatalf("transfer lane snapshot = %+v, want one in flight", snapshot)
	}
	assertNoCall(t, started, 50*time.Millisecond)

	proceed <- struct{}{}
	second := receiveCall(t, started)
	if second.jobID != "B" {
		t.Fatalf("second job = %q, want B", second.jobID)
	}
	proceed <- struct{}{}
	waitForState(t, s, "B", StateSucceeded)
}

func TestSchedulerRunsDifferentGenerationsConcurrently(t *testing.T) {
	started := make(chan executionCall, 2)
	proceed := make(chan struct{})
	s := newTestScheduler(t, blockingExecutor(started, proceed), nil)

	firstGeneration := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	secondGeneration := GenerationKey{ConnectionID: "connection-1", Generation: "2"}
	mustSubmit(t, s, testJob("A", firstGeneration, "A1"))
	first := receiveCall(t, started)
	if first.generation != firstGeneration {
		t.Fatalf("first generation = %+v, want %+v", first.generation, firstGeneration)
	}

	mustSubmit(t, s, testJob("B", secondGeneration, "B1"))
	second := receiveCall(t, started)
	if second.generation != secondGeneration {
		t.Fatalf("second generation = %+v, want %+v", second.generation, secondGeneration)
	}

	proceed <- struct{}{}
	proceed <- struct{}{}
	waitForState(t, s, "A", StateSucceeded)
	waitForState(t, s, "B", StateSucceeded)
}

func TestCancelQueuedJobNeverExecutesIt(t *testing.T) {
	started := make(chan executionCall, 2)
	proceed := make(chan struct{})
	s := newTestScheduler(t, blockingExecutor(started, proceed), nil)
	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}

	mustSubmit(t, s, testJob("running", generation, "running-1"))
	receiveCall(t, started)
	mustSubmit(t, s, testJob("queued", generation, "queued-1"))

	canceled, err := s.Cancel("queued")
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if canceled.State != StateCanceled || !canceled.Terminal {
		t.Fatalf("canceled snapshot = %+v", canceled)
	}

	proceed <- struct{}{}
	waitForState(t, s, "running", StateSucceeded)
	assertNoCall(t, started, 100*time.Millisecond)
}

func TestCancelInFlightHoldsLaneUntilExecutorCleanupReturns(t *testing.T) {
	started := make(chan executionCall, 2)
	cancelObserved := make(chan struct{})
	cleanupRelease := make(chan struct{})
	lanes := scheduler.NewReadLanes()
	var once sync.Once
	s := newTestScheduler(t, executorFunc(func(
		ctx context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		started <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		if jobID != "cancel-me" {
			return nil
		}
		<-ctx.Done()
		once.Do(func() { close(cancelObserved) })
		<-cleanupRelease
		return ctx.Err()
	}), func(GenerationKey) *scheduler.ReadLanes {
		return lanes
	})

	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	mustSubmit(t, s, testJob("cancel-me", generation, "cancel-1"))
	receiveCall(t, started)
	mustSubmit(t, s, testJob("next", generation, "next-1"))

	canceling, err := s.Cancel("cancel-me")
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if canceling.State != StateCancelRequested || canceling.Terminal {
		t.Fatalf("cancel snapshot before cleanup = %+v", canceling)
	}
	waitChannel(t, cancelObserved, "executor did not observe cancellation")
	if snapshot := lanes.Snapshot(); snapshot.TransferInFlight != 1 {
		t.Fatalf("lane released before cleanup: %+v", snapshot)
	}
	assertNoCall(t, started, 100*time.Millisecond)

	close(cleanupRelease)
	next := receiveCall(t, started)
	if next.jobID != "next" {
		t.Fatalf("next execution = %+v, want next job", next)
	}
	waitForState(t, s, "cancel-me", StateCanceled)
	waitForState(t, s, "next", StateSucceeded)
}

func TestCancelWhileWaitingForSharedTransferLaneIsNotDispatched(t *testing.T) {
	lanes := scheduler.NewReadLanes()
	externalLease, err := lanes.Acquire(context.Background(), scheduler.TransferLane)
	if err != nil {
		t.Fatal(err)
	}
	defer externalLease.Release()

	started := make(chan executionCall, 1)
	s := newTestScheduler(t, executorFunc(func(
		_ context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		started <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		return nil
	}), func(GenerationKey) *scheduler.ReadLanes {
		return lanes
	})

	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	mustSubmit(t, s, testJob("waiting", generation, "waiting-1"))
	waitFor(t, func() bool { return lanes.Snapshot().TransferWaiting == 1 })

	canceled, err := s.Cancel("waiting")
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if canceled.State != StateCanceled || !canceled.Terminal {
		t.Fatalf("canceled snapshot = %+v", canceled)
	}
	waitFor(t, func() bool { return lanes.Snapshot().TransferWaiting == 0 })
	externalLease.Release()
	assertNoCall(t, started, 100*time.Millisecond)
}

func TestTimeoutsPauseWithoutConsumingQuantum(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "response header", err: ErrHeaderTimeout, code: ErrorHeaderTimeout},
		{name: "chunk idle", err: ErrIdleTimeout, code: ErrorIdleTimeout},
		{name: "absolute", err: ErrRequestDeadline, code: ErrorRequestDeadline},
		{name: "context deadline", err: context.DeadlineExceeded, code: ErrorRequestDeadline},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := make(chan executionCall, 1)
			s := newTestScheduler(t, executorFunc(func(
				_ context.Context,
				generation GenerationKey,
				jobID string,
				quantum Quantum,
				timeouts Timeouts,
			) error {
				calls <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
				return fmt.Errorf("dispatcher: %w", test.err)
			}), nil)

			generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
			mustSubmit(t, s, testJob("export", generation, "slice-1"))
			call := receiveCall(t, calls)
			if call.timeouts != DefaultTimeouts() {
				t.Fatalf("timeouts = %+v, want %+v", call.timeouts, DefaultTimeouts())
			}
			paused := waitForState(t, s, "export", StatePausedRestartable)
			if paused.PublicErrorCode != test.code || paused.CompletedQuanta != 0 || paused.RemainingQuanta != 1 {
				t.Fatalf("paused snapshot = %+v", paused)
			}
		})
	}
}

func TestRestartRetriesTheSameQuantum(t *testing.T) {
	calls := make(chan executionCall, 2)
	var mu sync.Mutex
	attempt := 0
	s := newTestScheduler(t, executorFunc(func(
		_ context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		calls <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		mu.Lock()
		defer mu.Unlock()
		attempt++
		if attempt == 1 {
			return ErrIdleTimeout
		}
		return nil
	}), nil)

	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	mustSubmit(t, s, testJob("export", generation, "slice-1"))
	first := receiveCall(t, calls)
	waitForState(t, s, "export", StatePausedRestartable)

	restarted, err := s.Restart("export")
	if err != nil {
		t.Fatalf("Restart() error = %v", err)
	}
	if restarted.State != StateQueued || restarted.PublicErrorCode != "" {
		t.Fatalf("restart snapshot = %+v", restarted)
	}
	second := receiveCall(t, calls)
	if first.quantumID != second.quantumID {
		t.Fatalf("retried quantum = %q, want %q", second.quantumID, first.quantumID)
	}
	finished := waitForState(t, s, "export", StateSucceeded)
	if finished.CompletedQuanta != 1 || finished.RemainingQuanta != 0 {
		t.Fatalf("finished snapshot = %+v", finished)
	}
}

func TestPauseGenerationWaitsForCleanupAndDoesNotCancelJob(t *testing.T) {
	started := make(chan executionCall, 1)
	cleaned := make(chan struct{})
	s := newTestScheduler(t, executorFunc(func(
		ctx context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		started <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		<-ctx.Done()
		close(cleaned)
		return ctx.Err()
	}), nil)
	generation := GenerationKey{ConnectionID: "connection-1", Generation: "3"}
	mustSubmit(t, s, testJob("export", generation, "slice-1"))
	receiveCall(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.PauseGeneration(ctx, generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("PauseGeneration returned before executor cleanup")
	}
	snapshot, err := s.Get("export")
	if err != nil || snapshot.State != StatePausedRestartable || snapshot.Terminal || snapshot.CancelRequested {
		t.Fatalf("paused snapshot=%+v err=%v", snapshot, err)
	}
}

func TestExecutionFailureIsTerminal(t *testing.T) {
	s := newTestScheduler(t, executorFunc(func(
		context.Context,
		GenerationKey,
		string,
		Quantum,
		Timeouts,
	) error {
		return errors.New("writer failed")
	}), nil)
	generation := GenerationKey{ConnectionID: "connection-1", Generation: "1"}
	mustSubmit(t, s, testJob("export", generation, "slice-1"))

	failed := waitForState(t, s, "export", StateFailed)
	if !failed.Terminal || failed.PublicErrorCode != ErrorExecutionFailed {
		t.Fatalf("failed snapshot = %+v", failed)
	}
}

func TestSchedulerRejectsInvalidDuplicateAndClosedSubmissions(t *testing.T) {
	s, err := New(executorFunc(func(context.Context, GenerationKey, string, Quantum, Timeouts) error {
		return nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}

	invalid := []JobRequest{
		{},
		{ID: "export", Generation: GenerationKey{ConnectionID: "connection-1", Generation: "1"}},
		testJob("export", GenerationKey{ConnectionID: "", Generation: "1"}, "slice-1"),
		{
			ID:         "export",
			Generation: GenerationKey{ConnectionID: "connection-1", Generation: "1"},
			Quanta: []Quantum{{
				ID: "slice-1", Kind: QuantumKind("UNKNOWN"),
				Request: transport.AuthorizedReadQuery{Query: "SELECT * FROM cpu"},
			}},
		},
	}
	for _, request := range invalid {
		if _, err := s.Submit(request); !errors.Is(err, ErrInvalidJob) {
			t.Fatalf("Submit(%+v) error = %v, want ErrInvalidJob", request, err)
		}
	}

	valid := testJob("export", GenerationKey{ConnectionID: "connection-1", Generation: "1"}, "slice-1")
	mustSubmit(t, s, valid)
	if _, err := s.Submit(valid); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate Submit() error = %v, want ErrDuplicateJob", err)
	}
	s.Close()
	if _, err := s.Submit(testJob("later", valid.Generation, "slice-2")); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("Submit() after Close error = %v, want ErrSchedulerClosed", err)
	}
}

func blockingExecutor(started chan<- executionCall, proceed <-chan struct{}) Executor {
	return executorFunc(func(
		ctx context.Context,
		generation GenerationKey,
		jobID string,
		quantum Quantum,
		timeouts Timeouts,
	) error {
		started <- executionCall{generation: generation, jobID: jobID, quantumID: quantum.ID, timeouts: timeouts}
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func newTestScheduler(t *testing.T, executor Executor, laneProvider LaneProvider) *Scheduler {
	t.Helper()
	s, err := New(executor, laneProvider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func testJob(id string, generation GenerationKey, quantumIDs ...string) JobRequest {
	quanta := make([]Quantum, 0, len(quantumIDs))
	for index, quantumID := range quantumIDs {
		kind := QuantumTimeSlice
		if index == 0 && len(quantumIDs) > 1 {
			kind = QuantumMetadata
		}
		quanta = append(quanta, Quantum{
			ID:   quantumID,
			Kind: kind,
			Request: transport.AuthorizedReadQuery{
				Database: "telemetry",
				Query:    `SELECT * FROM "cpu"`,
			},
		})
	}
	return JobRequest{ID: id, Generation: generation, Quanta: quanta}
}

func mustSubmit(t *testing.T, s *Scheduler, request JobRequest) Snapshot {
	t.Helper()
	snapshot, err := s.Submit(request)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	return snapshot
}

func receiveCall(t *testing.T, calls <-chan executionCall) executionCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(2 * time.Second):
		t.Fatal("executor was not called")
		return executionCall{}
	}
}

func assertNoCall(t *testing.T, calls <-chan executionCall, duration time.Duration) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("unexpected executor call: %+v", call)
	case <-time.After(duration):
	}
}

func waitForState(t *testing.T, s *Scheduler, jobID string, state State) Snapshot {
	t.Helper()
	var result Snapshot
	waitFor(t, func() bool {
		snapshot, err := s.Get(jobID)
		if err != nil {
			return false
		}
		result = snapshot
		return snapshot.State == state
	})
	return result
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitChannel(t *testing.T, channel <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatal(failure)
	}
}
