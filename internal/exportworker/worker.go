package exportworker

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

const maxHTTPErrorBody = 64 * 1024
const MaxQuantumResponseBytes int64 = 128 * 1024 * 1024

var (
	ErrHTTPStatus       = errors.New("EXPORT_HTTP_ERROR")
	ErrSchemaDrift      = errors.New("EXPORT_SCHEMA_DRIFT")
	ErrUnknownTag       = errors.New("EXPORT_UNKNOWN_TAG")
	ErrUnknownField     = errors.New("EXPORT_UNKNOWN_FIELD")
	ErrTagFieldConflict = errors.New("EXPORT_TAG_FIELD_CONFLICT")
	ErrArtifactState    = errors.New("EXPORT_ARTIFACT_STATE_CONFLICT")
	ErrResponseTooLarge = errors.New("EXPORT_RESPONSE_TOO_LARGE")
)

type RoundTripDispatcher interface {
	BeginRoundTrip(context.Context, transport.Request) (*transport.Attempt, error)
}

type DispatcherProvider func(exportlane.GenerationKey) (RoundTripDispatcher, error)

// FragmentRepository is implemented by exportjob.Repository. It keeps the
// filesystem protocol ordered around the FULL WRITING/FINALIZING/COMPLETE
// state transitions without coupling this worker to application wiring.
type FragmentRepository interface {
	BeginFragment(context.Context, exportjob.BeginFragmentRequest) (exportjob.Fragment, error)
	MarkFragmentFinalizing(context.Context, exportjob.FinalizeFragmentRequest) (exportjob.Fragment, error)
	CompleteFragment(context.Context, string) (exportjob.Fragment, error)
	MarkFragmentCorrupt(context.Context, string) (exportjob.Fragment, error)
	RetryCorruptFragment(context.Context, string, string) (exportjob.Fragment, error)
	GetFragment(context.Context, string) (exportjob.Fragment, error)
}

// TargetReservationHooks is the adapter boundary to target-volume accounting.
// Reserve must consume the StartExport reservation before allocating another
// extent. Settle records the committed artifact's actual compressed length and
// releases its unwritten tail. Abort releases only this artifact's unwritten
// reservation after its .part has been removed.
type TargetReservationHooks struct {
	Reserve func(context.Context, string, string, int64) error
	Settle  func(context.Context, string, string, int64) error
	Abort   func(context.Context, string, string) error
}

type Worker struct {
	mu          sync.Mutex
	dispatchers DispatcherProvider
	fragments   FragmentRepository
	reservation TargetReservationHooks
	jobs        map[string]*jobRuntime
	outputs     map[string]string
}

type jobRuntime struct {
	plan         Plan
	tagKeys      map[string]struct{}
	fieldTypes   map[string]query.ScalarKind
	tagsLoaded   bool
	fieldsLoaded bool
	completed    map[string]ManifestFragment
	manifest     *Manifest
	manifestMeta *transfer.ArtifactMeta
}

func New(dispatchers DispatcherProvider, fragments FragmentRepository, reservation TargetReservationHooks) (*Worker, error) {
	if dispatchers == nil || fragments == nil || reservation.Reserve == nil ||
		reservation.Settle == nil || reservation.Abort == nil {
		return nil, ErrInvalidPlan
	}
	return &Worker{
		dispatchers: dispatchers, fragments: fragments, reservation: reservation,
		jobs: make(map[string]*jobRuntime), outputs: make(map[string]string),
	}, nil
}

func (w *Worker) RegisterPlan(plan Plan) error {
	expected, err := BuildPlan(plan.JobID, plan.Generation, plan.Spec)
	if err != nil || !reflect.DeepEqual(expected, plan) {
		return ErrInvalidPlan
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing := w.jobs[plan.JobID]; existing != nil {
		if reflect.DeepEqual(existing.plan, plan) {
			return nil
		}
		return ErrInvalidPlan
	}
	// A Plan owns its package directory. Multi-measurement orchestration must
	// allocate a stable subdirectory per measurement before registration.
	if owner := w.outputs[plan.Spec.OutputDirectory]; owner != "" && owner != plan.JobID {
		return ErrInvalidPlan
	}
	w.jobs[plan.JobID] = &jobRuntime{
		plan: plan, completed: make(map[string]ManifestFragment),
	}
	w.outputs[plan.Spec.OutputDirectory] = plan.JobID
	return nil
}

func (w *Worker) UnregisterPlan(jobID string) {
	w.mu.Lock()
	if runtime := w.jobs[jobID]; runtime != nil && w.outputs[runtime.plan.Spec.OutputDirectory] == jobID {
		delete(w.outputs, runtime.plan.Spec.OutputDirectory)
	}
	delete(w.jobs, jobID)
	w.mu.Unlock()
}

func (w *Worker) Manifest(jobID string) (Manifest, transfer.ArtifactMeta, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtime := w.jobs[jobID]
	if runtime == nil || runtime.manifest == nil || runtime.manifestMeta == nil {
		return Manifest{}, transfer.ArtifactMeta{}, false
	}
	return cloneManifest(*runtime.manifest), *runtime.manifestMeta, true
}

func (w *Worker) ExecuteQuantum(
	ctx context.Context,
	generation exportlane.GenerationKey,
	jobID string,
	quantum exportlane.Quantum,
	timeouts exportlane.Timeouts,
) error {
	plan, slice, err := w.quantumPlan(jobID, generation, quantum)
	if err != nil {
		return err
	}
	dispatcher, err := w.dispatchers(generation)
	if err != nil || dispatcher == nil {
		return ErrQuantumMismatch
	}

	switch quantum.ID {
	case tagKeysQuantumID:
		result, err := executeRead(ctx, dispatcher, quantum.Request, timeouts, query.DecodeConfig{
			Statements: []query.StatementSpec{{TimeColumn: "__no_time_column__"}},
		})
		if err != nil {
			return err
		}
		keys, err := parseTagKeys(result, plan.Spec.Measurement)
		if err != nil {
			return err
		}
		return w.setTagKeys(jobID, keys)
	case fieldKeysQuantumID:
		result, err := executeRead(ctx, dispatcher, quantum.Request, timeouts, query.DecodeConfig{
			Statements: []query.StatementSpec{{TimeColumn: "__no_time_column__"}},
		})
		if err != nil {
			return err
		}
		fields, err := parseFieldKeys(result, plan.Spec.Measurement)
		if err != nil {
			return err
		}
		return w.setFieldTypes(jobID, fields)
	default:
		if slice == nil {
			return ErrQuantumMismatch
		}
		tags, fields, err := w.schema(jobID)
		if err != nil {
			return err
		}
		resolver := query.NumericKindResolverFunc(func(context query.NumericContext) (query.ScalarKind, bool) {
			kind, found := fields[context.Column]
			return kind, found && (kind == query.ScalarInt64 || kind == query.ScalarUint64 || kind == query.ScalarFloat64)
		})
		result, err := executeRead(ctx, dispatcher, quantum.Request, timeouts, query.DecodeConfig{
			Statements: []query.StatementSpec{{TimeColumn: "time"}}, Resolver: resolver,
		})
		if err != nil {
			return err
		}
		return w.commitSlice(ctx, jobID, plan, *slice, result, tags, fields)
	}
}

func (w *Worker) quantumPlan(
	jobID string,
	generation exportlane.GenerationKey,
	quantum exportlane.Quantum,
) (Plan, *TimeSlice, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtime := w.jobs[jobID]
	if runtime == nil {
		return Plan{}, nil, ErrPlanNotRegistered
	}
	if runtime.plan.Generation != generation {
		return Plan{}, nil, ErrQuantumMismatch
	}
	for index, expected := range runtime.plan.Quanta {
		if expected != quantum {
			continue
		}
		if index < 2 {
			return runtime.plan, nil, nil
		}
		sliceIndex := index - 2
		if sliceIndex < 0 || sliceIndex >= len(runtime.plan.Slices) ||
			runtime.plan.Slices[sliceIndex].QuantumID != quantum.ID {
			return Plan{}, nil, ErrQuantumMismatch
		}
		sliceCopy := runtime.plan.Slices[sliceIndex]
		return runtime.plan, &sliceCopy, nil
	}
	return Plan{}, nil, ErrQuantumMismatch
}

func (w *Worker) setTagKeys(jobID string, keys map[string]struct{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtime := w.jobs[jobID]
	if runtime == nil {
		return ErrPlanNotRegistered
	}
	if runtime.fieldsLoaded {
		for key := range keys {
			if _, collision := runtime.fieldTypes[key]; collision {
				return ErrTagFieldConflict
			}
		}
	}
	runtime.tagKeys, runtime.tagsLoaded = cloneSet(keys), true
	return nil
}

func (w *Worker) setFieldTypes(jobID string, fields map[string]query.ScalarKind) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtime := w.jobs[jobID]
	if runtime == nil {
		return ErrPlanNotRegistered
	}
	if runtime.tagsLoaded {
		for key := range fields {
			if _, collision := runtime.tagKeys[key]; collision {
				return ErrTagFieldConflict
			}
		}
	}
	runtime.fieldTypes, runtime.fieldsLoaded = cloneFields(fields), true
	return nil
}

func (w *Worker) schema(jobID string) (map[string]struct{}, map[string]query.ScalarKind, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	runtime := w.jobs[jobID]
	if runtime == nil {
		return nil, nil, ErrPlanNotRegistered
	}
	if !runtime.tagsLoaded || !runtime.fieldsLoaded {
		return nil, nil, ErrSchemaDrift
	}
	return cloneSet(runtime.tagKeys), cloneFields(runtime.fieldTypes), nil
}

func (w *Worker) commitSlice(
	ctx context.Context,
	jobID string,
	plan Plan,
	slice TimeSlice,
	result *query.ResultSet,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) error {
	if err := validateDataResult(result, plan.Spec.Measurement, slice.StartNS, slice.EndNS, tagKeys, fieldTypes); err != nil {
		return err
	}
	if err := os.MkdirAll(plan.Spec.OutputDirectory, 0o700); err != nil {
		return ErrArtifactState
	}
	fragment, skip, err := w.prepareDataFragment(ctx, jobID, slice)
	if err != nil {
		return err
	}
	if skip {
		manifestFragment, err := manifestFragmentFromStored(fragment)
		if err != nil {
			return err
		}
		return w.recordCompleted(ctx, jobID, plan, slice, manifestFragment, tagKeys, fieldTypes)
	}

	pointCount := int64(0)
	meta, err := transfer.WriteDurableGzip(ctx, slice.PartPath, slice.FinalPath, func(writer io.Writer) error {
		for _, statement := range result.Statements {
			for _, series := range statement.Series {
				for _, row := range series.Rows {
					if err := ctx.Err(); err != nil {
						return err
					}
					line, err := rowToLP(series, row, plan.Spec.Measurement, tagKeys, fieldTypes)
					if err != nil {
						return err
					}
					if _, err := io.WriteString(writer, line+"\n"); err != nil {
						return err
					}
					pointCount++
				}
			}
		}
		return nil
	}, transfer.FinalizeHooks{
		Reserve: func(reserveCtx context.Context, bytes int64) error {
			return w.reservation.Reserve(reserveCtx, jobID, slice.FragmentID, bytes)
		},
		BeforeRename: func(hookCtx context.Context, artifact transfer.ArtifactMeta) error {
			_, err := w.fragments.MarkFragmentFinalizing(hookCtx, exportjob.FinalizeFragmentRequest{
				FragmentID: slice.FragmentID, PartPath: artifact.PartPath, FinalPath: artifact.FinalPath,
				CompressedSize:   strconv.FormatInt(artifact.CompressedSize, 10),
				UncompressedSize: strconv.FormatInt(artifact.UncompressedSize, 10),
				ChecksumSHA256:   artifact.SHA256,
			})
			return err
		},
		AfterRename: func(hookCtx context.Context, _ transfer.ArtifactMeta) error {
			_, err := w.fragments.CompleteFragment(hookCtx, slice.FragmentID)
			return err
		},
	})
	if err != nil {
		w.markWritingCorrupt(ctx, slice.FragmentID)
		if _, statErr := os.Stat(slice.FinalPath); errors.Is(statErr, os.ErrNotExist) {
			_ = w.reservation.Abort(context.WithoutCancel(ctx), jobID, slice.FragmentID)
		}
		return err
	}
	if err := w.reservation.Settle(ctx, jobID, slice.FragmentID, meta.CompressedSize); err != nil {
		return err
	}
	completed := ManifestFragment{
		File: filepathBase(meta.FinalPath), StartNS: slice.StartNS, EndNS: slice.EndNS,
		CompressedSize:   strconv.FormatInt(meta.CompressedSize, 10),
		UncompressedSize: strconv.FormatInt(meta.UncompressedSize, 10),
		SHA256:           meta.SHA256, PointCount: strconv.FormatInt(pointCount, 10),
	}
	return w.recordCompleted(ctx, jobID, plan, slice, completed, tagKeys, fieldTypes)
}

func (w *Worker) prepareDataFragment(
	ctx context.Context,
	jobID string,
	slice TimeSlice,
) (exportjob.Fragment, bool, error) {
	fragment, err := w.fragments.GetFragment(ctx, slice.FragmentID)
	if errors.Is(err, exportjob.ErrFragmentNotFound) {
		fragment, err = w.fragments.BeginFragment(ctx, exportjob.BeginFragmentRequest{
			FragmentID: slice.FragmentID, JobID: jobID, Ordinal: slice.Ordinal,
			Kind: exportjob.FragmentData, StartNS: &slice.StartNS, EndNS: &slice.EndNS,
			PartPath: slice.PartPath,
		})
		return fragment, false, err
	}
	if err != nil {
		return exportjob.Fragment{}, false, err
	}
	switch fragment.State {
	case exportjob.FragmentCorrupt:
		_ = os.Remove(slice.PartPath)
		fragment, err = w.fragments.RetryCorruptFragment(ctx, slice.FragmentID, slice.PartPath)
		return fragment, false, err
	case exportjob.FragmentComplete:
		if !fragment.Reusable {
			return exportjob.Fragment{}, false, exportjob.ErrFragmentValidationRequired
		}
		return fragment, true, nil
	default:
		return exportjob.Fragment{}, false, ErrArtifactState
	}
}

func (w *Worker) markWritingCorrupt(ctx context.Context, fragmentID string) {
	fragment, err := w.fragments.GetFragment(context.WithoutCancel(ctx), fragmentID)
	if err == nil && fragment.State == exportjob.FragmentWriting {
		_, _ = w.fragments.MarkFragmentCorrupt(context.WithoutCancel(ctx), fragmentID)
	}
}

func cloneSet(values map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for key := range values {
		result[key] = struct{}{}
	}
	return result
}

func cloneFields(values map[string]query.ScalarKind) map[string]query.ScalarKind {
	result := make(map[string]query.ScalarKind, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func filepathBase(path string) string {
	for index := len(path) - 1; index >= 0; index-- {
		if path[index] == '/' || path[index] == '\\' {
			return path[index+1:]
		}
	}
	return path
}

func executeRead(
	ctx context.Context,
	dispatcher RoundTripDispatcher,
	request transport.AuthorizedReadQuery,
	timeouts exportlane.Timeouts,
	config query.DecodeConfig,
) (*query.ResultSet, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	timeouts = normalizedTimeouts(timeouts)
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	attempt, err := dispatcher.BeginRoundTrip(requestCtx, request)
	if err != nil {
		return nil, err
	}

	var cause atomic.Int32
	headerTimer := timeoutTimer(timeouts.ResponseHeader, timeoutHeader, &cause, cancel)
	idleTimer := newResettableTimeout(timeouts.ChunkIdle, timeoutIdle, &cause, cancel)
	absoluteTimer := timeoutTimer(timeouts.Absolute, timeoutAbsolute, &cause, cancel)
	defer headerTimer.Stop()
	defer idleTimer.Stop()
	defer absoluteTimer.Stop()

	response, err := attempt.Wait()
	headerTimer.Stop()
	if err != nil {
		return nil, mapExecutionError(ctx, err, timeoutReason(cause.Load()))
	}
	defer response.Close()
	if reason := timeoutReason(cause.Load()); reason != timeoutNone {
		return nil, mapExecutionError(ctx, context.Canceled, reason)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxHTTPErrorBody))
		return nil, ErrHTTPStatus
	}
	config.OnChunkDecoded = idleTimer.Reset
	result, err := query.DecodeChunked(&hardLimitReader{reader: response.Body, remaining: MaxQuantumResponseBytes}, config)
	if err != nil {
		return nil, mapExecutionError(ctx, err, timeoutReason(cause.Load()))
	}
	if reason := timeoutReason(cause.Load()); reason != timeoutNone {
		return nil, mapExecutionError(ctx, context.Canceled, reason)
	}
	return result, nil
}

type hardLimitReader struct {
	reader    io.Reader
	remaining int64
}

func (r *hardLimitReader) Read(buffer []byte) (int, error) {
	if r.remaining > 0 {
		if int64(len(buffer)) > r.remaining {
			buffer = buffer[:r.remaining]
		}
		n, err := r.reader.Read(buffer)
		r.remaining -= int64(n)
		return n, err
	}
	var extra [1]byte
	n, err := r.reader.Read(extra[:])
	if n != 0 {
		return 0, ErrResponseTooLarge
	}
	return 0, err
}

type timeoutReason int32

const (
	timeoutNone timeoutReason = iota
	timeoutHeader
	timeoutIdle
	timeoutAbsolute
)

func normalizedTimeouts(values exportlane.Timeouts) exportlane.Timeouts {
	defaults := exportlane.DefaultTimeouts()
	if values.ResponseHeader <= 0 {
		values.ResponseHeader = defaults.ResponseHeader
	}
	if values.ChunkIdle <= 0 {
		values.ChunkIdle = defaults.ChunkIdle
	}
	if values.Absolute <= 0 {
		values.Absolute = defaults.Absolute
	}
	return values
}

func timeoutTimer(duration time.Duration, reason timeoutReason, cause *atomic.Int32, cancel context.CancelFunc) *time.Timer {
	return time.AfterFunc(duration, func() {
		if cause.CompareAndSwap(int32(timeoutNone), int32(reason)) {
			cancel()
		}
	})
}

type resettableTimeout struct {
	mu         sync.Mutex
	duration   time.Duration
	reason     timeoutReason
	cause      *atomic.Int32
	cancel     context.CancelFunc
	timer      *time.Timer
	generation uint64
	stopped    bool
}

func newResettableTimeout(
	duration time.Duration,
	reason timeoutReason,
	cause *atomic.Int32,
	cancel context.CancelFunc,
) *resettableTimeout {
	timeout := &resettableTimeout{duration: duration, reason: reason, cause: cause, cancel: cancel}
	timeout.scheduleLocked()
	return timeout
}

func (t *resettableTimeout) scheduleLocked() {
	generation := t.generation
	t.timer = time.AfterFunc(t.duration, func() { t.fire(generation) })
}

func (t *resettableTimeout) fire(generation uint64) {
	t.mu.Lock()
	if t.stopped || generation != t.generation {
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	if t.cause.CompareAndSwap(int32(timeoutNone), int32(t.reason)) {
		t.cancel()
	}
}

func (t *resettableTimeout) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.cause.Load() != int32(timeoutNone) {
		return
	}
	t.timer.Stop()
	t.generation++
	t.scheduleLocked()
}

func (t *resettableTimeout) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	t.stopped = true
	t.generation++
	t.timer.Stop()
}

func mapExecutionError(ctx context.Context, err error, reason timeoutReason) error {
	switch reason {
	case timeoutHeader:
		return exportlane.ErrHeaderTimeout
	case timeoutIdle:
		return exportlane.ErrIdleTimeout
	case timeoutAbsolute:
		return exportlane.ErrRequestDeadline
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

var _ exportlane.Executor = (*Worker)(nil)
