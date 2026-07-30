package exportlane

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type State string

const (
	StateQueued            State = "QUEUED"
	StateRunning           State = "RUNNING"
	StateCancelRequested   State = "CANCEL_REQUESTED"
	StatePausedRestartable State = "PAUSED_RESTARTABLE"
	StateSucceeded         State = "SUCCEEDED"
	StateCanceled          State = "CANCELED"
	StateFailed            State = "FAILED"
)

type QuantumKind string

const (
	QuantumMetadata  QuantumKind = "METADATA"
	QuantumTimeSlice QuantumKind = "TIME_SLICE"
)

const (
	ErrorHeaderTimeout   = "EXPORT_HEADER_TIMEOUT"
	ErrorIdleTimeout     = "EXPORT_IDLE_TIMEOUT"
	ErrorRequestDeadline = "EXPORT_REQUEST_DEADLINE"
	ErrorExecutionFailed = "EXPORT_EXECUTION_FAILED"
)

var (
	ErrHeaderTimeout    = errors.New(ErrorHeaderTimeout)
	ErrIdleTimeout      = errors.New(ErrorIdleTimeout)
	ErrRequestDeadline  = errors.New(ErrorRequestDeadline)
	ErrInvalidJob       = errors.New("INVALID_EXPORT_JOB")
	ErrDuplicateJob     = errors.New("EXPORT_JOB_ALREADY_EXISTS")
	ErrJobNotFound      = errors.New("EXPORT_JOB_NOT_FOUND")
	ErrStateConflict    = errors.New("EXPORT_JOB_STATE_CONFLICT")
	ErrSchedulerClosed  = errors.New("EXPORT_SCHEDULER_CLOSED")
	ErrLaneUnavailable  = errors.New("EXPORT_TRANSFER_LANE_UNAVAILABLE")
	ErrGenerationPaused = errors.New("EXPORT_CONNECTION_PAUSED")
)

type GenerationKey struct {
	ConnectionID string
	Generation   string
}

type Timeouts struct {
	ResponseHeader time.Duration
	ChunkIdle      time.Duration
	Absolute       time.Duration
}

func DefaultTimeouts() Timeouts {
	return Timeouts{
		ResponseHeader: 60 * time.Second,
		ChunkIdle:      120 * time.Second,
		Absolute:       30 * time.Minute,
	}
}

type Quantum struct {
	ID      string
	Kind    QuantumKind
	Request transport.AuthorizedReadQuery
}

type JobRequest struct {
	ID         string
	Generation GenerationKey
	Quanta     []Quantum
}

type Snapshot struct {
	ID              string
	Generation      GenerationKey
	State           State
	Terminal        bool
	TotalQuanta     int
	CompletedQuanta int
	RemainingQuanta int
	ActiveQuantumID string
	CancelRequested bool
	PublicErrorCode string
}

type RestartableError struct {
	Code  string
	Cause error
}

func (e *RestartableError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return e.Code + ": " + e.Cause.Error()
	}
	return e.Code
}

func (e *RestartableError) Unwrap() error { return e.Cause }

func Restartable(code string, cause error) error {
	if strings.TrimSpace(code) == "" {
		code = ErrorExecutionFailed
	}
	return &RestartableError{Code: code, Cause: cause}
}

// Executor is the adapter boundary to InfluxDispatcher and the export writer.
// ExecuteQuantum must perform exactly one metadata request or time-slice SELECT.
// Its timeout clocks start only after Dispatcher.BeginRoundTrip crosses the real
// RoundTrip barrier. ChunkIdle is refreshed only after a complete valid chunk is
// decoded. Before returning, it must close the body and delete/settle the current
// .part as appropriate; the TransferLane lease is held until this method returns.
type Executor interface {
	ExecuteQuantum(context.Context, GenerationKey, string, Quantum, Timeouts) error
}

type LaneProvider func(GenerationKey) *scheduler.ReadLanes

type Scheduler struct {
	mu           sync.Mutex
	executor     Executor
	laneProvider LaneProvider
	timeouts     Timeouts
	jobs         map[string]*jobRecord
	groups       map[GenerationKey]*generationGroup
	closed       bool
	wg           sync.WaitGroup
}

type jobRecord struct {
	id              string
	generation      GenerationKey
	state           State
	terminal        bool
	total           int
	completed       int
	quanta          []Quantum
	activeQuantumID string
	cancelRequested bool
	pauseRequested  bool
	publicErrorCode string
	claimed         bool
	cancel          context.CancelFunc
}

type generationGroup struct {
	key   GenerationKey
	lane  *scheduler.ReadLanes
	ready []string
	wake  chan struct{}
	stop  chan struct{}
}

func New(executor Executor, laneProvider LaneProvider) (*Scheduler, error) {
	if executor == nil {
		return nil, ErrInvalidJob
	}
	if laneProvider == nil {
		laneProvider = func(GenerationKey) *scheduler.ReadLanes { return scheduler.NewReadLanes() }
	}
	return &Scheduler{
		executor: executor, laneProvider: laneProvider, timeouts: DefaultTimeouts(),
		jobs: make(map[string]*jobRecord), groups: make(map[GenerationKey]*generationGroup),
	}, nil
}

func (s *Scheduler) Submit(request JobRequest) (Snapshot, error) {
	if err := validateJobRequest(request); err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Snapshot{}, ErrSchedulerClosed
	}
	if s.jobs[request.ID] != nil {
		return Snapshot{}, ErrDuplicateJob
	}
	group := s.groups[request.Generation]
	if group == nil {
		lane := s.laneProvider(request.Generation)
		if lane == nil {
			return Snapshot{}, ErrLaneUnavailable
		}
		group = &generationGroup{
			key: request.Generation, lane: lane,
			wake: make(chan struct{}, 1), stop: make(chan struct{}),
		}
		s.groups[request.Generation] = group
		s.wg.Add(1)
		go s.runGeneration(group)
	}
	record := &jobRecord{
		id: request.ID, generation: request.Generation, state: StateQueued,
		total: len(request.Quanta), quanta: cloneQuanta(request.Quanta),
	}
	s.jobs[request.ID] = record
	group.ready = append(group.ready, request.ID)
	notify(group.wake)
	return snapshot(record), nil
}

func (s *Scheduler) Get(jobID string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.jobs[jobID]
	if record == nil {
		return Snapshot{}, ErrJobNotFound
	}
	return snapshot(record), nil
}

// Cancel atomically removes a queued job or cancels the in-flight quantum.
// A running job becomes CANCELED only after ExecuteQuantum has completed cleanup.
func (s *Scheduler) Cancel(jobID string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.jobs[jobID]
	if record == nil {
		return Snapshot{}, ErrJobNotFound
	}
	if record.terminal || record.state == StateCanceled {
		return snapshot(record), nil
	}
	record.cancelRequested = true
	group := s.groups[record.generation]
	switch record.state {
	case StateQueued:
		group.ready = removeJob(group.ready, jobID)
		record.state = StateCanceled
		record.terminal = true
		if record.cancel != nil {
			record.cancel()
		}
	case StateRunning, StateCancelRequested:
		record.state = StateCancelRequested
		if record.cancel != nil {
			record.cancel()
		}
	case StatePausedRestartable:
		record.state = StateCanceled
		record.terminal = true
	default:
		return Snapshot{}, ErrStateConflict
	}
	return snapshot(record), nil
}

func (s *Scheduler) Restart(jobID string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Snapshot{}, ErrSchedulerClosed
	}
	record := s.jobs[jobID]
	if record == nil {
		return Snapshot{}, ErrJobNotFound
	}
	if record.state != StatePausedRestartable || record.terminal || record.claimed {
		return Snapshot{}, ErrStateConflict
	}
	record.state = StateQueued
	record.cancelRequested = false
	record.pauseRequested = false
	record.publicErrorCode = ""
	s.groups[record.generation].ready = append(s.groups[record.generation].ready, jobID)
	notify(s.groups[record.generation].wake)
	return snapshot(record), nil
}

// PauseGeneration prevents stale connection generations from being requeued.
// Running work is canceled and the method waits until ExecuteQuantum has
// completed body/.part cleanup and released its claim.
func (s *Scheduler) PauseGeneration(ctx context.Context, generation GenerationKey) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	group := s.groups[generation]
	for _, record := range s.jobs {
		if record.generation != generation || record.terminal || record.cancelRequested {
			continue
		}
		record.pauseRequested = true
		switch record.state {
		case StateQueued:
			if group != nil {
				group.ready = removeJob(group.ready, record.id)
			}
			if !record.claimed {
				record.state = StatePausedRestartable
			} else if record.cancel != nil {
				record.cancel()
			}
		case StateRunning, StateCancelRequested:
			if record.cancel != nil {
				record.cancel()
			}
		}
	}
	s.mu.Unlock()
	for {
		s.mu.Lock()
		pending := false
		for _, record := range s.jobs {
			if record.generation == generation && record.pauseRequested && record.claimed {
				pending = true
				break
			}
		}
		s.mu.Unlock()
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (s *Scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for _, record := range s.jobs {
		if record.terminal {
			continue
		}
		record.pauseRequested = true
		if record.state == StateRunning || record.state == StateCancelRequested {
			// ExecuteQuantum observes cancellation, performs cleanup, and is then
			// settled as PAUSED_RESTARTABLE by finishQuantum.
		} else {
			record.state = StatePausedRestartable
		}
		if record.cancel != nil {
			record.cancel()
		}
	}
	for _, group := range s.groups {
		close(group.stop)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Scheduler) runGeneration(group *generationGroup) {
	defer s.wg.Done()
	for {
		jobID, quantum, ctx, ok := s.claim(group)
		if !ok {
			select {
			case <-group.wake:
				continue
			case <-group.stop:
				return
			}
		}
		lease, err := group.lane.Acquire(ctx, scheduler.TransferLane)
		if err != nil {
			s.finishBeforeDispatch(jobID, err)
			continue
		}
		if !s.markRunning(jobID, quantum.ID) {
			lease.Release()
			s.finishBeforeDispatch(jobID, context.Canceled)
			continue
		}
		err = s.executor.ExecuteQuantum(ctx, group.key, jobID, quantum, s.timeouts)
		lease.Release()
		s.finishQuantum(group, jobID, quantum.ID, err)
	}
}

func (s *Scheduler) claim(group *generationGroup) (string, Quantum, context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(group.ready) != 0 {
		jobID := group.ready[0]
		group.ready = group.ready[1:]
		record := s.jobs[jobID]
		if record == nil || record.state != StateQueued || record.terminal || record.claimed || len(record.quanta) == 0 {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		record.claimed = true
		record.cancel = cancel
		return jobID, record.quanta[0], ctx, true
	}
	return "", Quantum{}, nil, false
}

func (s *Scheduler) markRunning(jobID, quantumID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.jobs[jobID]
	if record == nil || record.state != StateQueued || record.terminal || record.cancelRequested || !record.claimed {
		return false
	}
	record.state = StateRunning
	record.activeQuantumID = quantumID
	return true
}

func (s *Scheduler) finishBeforeDispatch(jobID string, dispatchErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.jobs[jobID]
	if record == nil {
		return
	}
	clearClaim(record)
	if record.pauseRequested {
		record.state = StatePausedRestartable
		return
	}
	if record.terminal || record.cancelRequested || errors.Is(dispatchErr, context.Canceled) {
		record.state = StateCanceled
		record.terminal = true
		return
	}
	record.state = StateFailed
	record.terminal = true
	record.publicErrorCode = ErrorExecutionFailed
}

func (s *Scheduler) finishQuantum(group *generationGroup, jobID, quantumID string, executionErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.jobs[jobID]
	if record == nil {
		return
	}
	clearClaim(record)
	if record.cancelRequested || record.state == StateCancelRequested {
		record.state = StateCanceled
		record.terminal = true
		return
	}
	if record.pauseRequested {
		record.state = StatePausedRestartable
		record.publicErrorCode = ErrorExecutionFailed
		return
	}
	if executionErr != nil {
		if code, timeout := timeoutCode(executionErr); timeout {
			record.state = StatePausedRestartable
			record.publicErrorCode = code
			return
		}
		record.state = StateFailed
		record.terminal = true
		record.publicErrorCode = ErrorExecutionFailed
		return
	}
	if len(record.quanta) == 0 || record.quanta[0].ID != quantumID {
		record.state = StateFailed
		record.terminal = true
		record.publicErrorCode = ErrorExecutionFailed
		return
	}
	record.quanta = record.quanta[1:]
	record.completed++
	if len(record.quanta) == 0 {
		record.state = StateSucceeded
		record.terminal = true
		return
	}
	record.state = StateQueued
	group.ready = append(group.ready, jobID)
	notify(group.wake)
}

func timeoutCode(err error) (string, bool) {
	var restartable *RestartableError
	if errors.As(err, &restartable) {
		return restartable.Code, true
	}
	switch {
	case errors.Is(err, ErrHeaderTimeout):
		return ErrorHeaderTimeout, true
	case errors.Is(err, ErrIdleTimeout):
		return ErrorIdleTimeout, true
	case errors.Is(err, ErrRequestDeadline), errors.Is(err, context.DeadlineExceeded):
		return ErrorRequestDeadline, true
	default:
		return "", false
	}
}

func validateJobRequest(request JobRequest) error {
	if strings.TrimSpace(request.ID) == "" || strings.TrimSpace(request.Generation.ConnectionID) == "" ||
		strings.TrimSpace(request.Generation.Generation) == "" || len(request.Quanta) == 0 {
		return ErrInvalidJob
	}
	seen := make(map[string]struct{}, len(request.Quanta))
	for _, quantum := range request.Quanta {
		if strings.TrimSpace(quantum.ID) == "" || strings.TrimSpace(quantum.Request.Query) == "" {
			return ErrInvalidJob
		}
		if quantum.Kind != QuantumMetadata && quantum.Kind != QuantumTimeSlice {
			return ErrInvalidJob
		}
		if _, exists := seen[quantum.ID]; exists {
			return ErrInvalidJob
		}
		seen[quantum.ID] = struct{}{}
	}
	return nil
}

func cloneQuanta(values []Quantum) []Quantum {
	result := make([]Quantum, len(values))
	copy(result, values)
	return result
}

func clearClaim(record *jobRecord) {
	if record.cancel != nil {
		record.cancel()
	}
	record.cancel = nil
	record.claimed = false
	record.activeQuantumID = ""
}

func snapshot(record *jobRecord) Snapshot {
	return Snapshot{
		ID: record.id, Generation: record.generation, State: record.state, Terminal: record.terminal,
		TotalQuanta: record.total, CompletedQuanta: record.completed, RemainingQuanta: len(record.quanta),
		ActiveQuantumID: record.activeQuantumID, CancelRequested: record.cancelRequested,
		PublicErrorCode: record.publicErrorCode,
	}
}

func removeJob(values []string, jobID string) []string {
	for index, value := range values {
		if value == jobID {
			return append(values[:index], values[index+1:]...)
		}
	}
	return values
}

func notify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
