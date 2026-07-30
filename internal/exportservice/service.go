package exportservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/exportworker"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

const ErrorRestartRequired = "EXPORT_RESTART_REQUIRED"

type ConnectionRuntime interface {
	ExportDispatcher(exportlane.GenerationKey) (*transport.Dispatcher, error)
	ExportReadLanes(exportlane.GenerationKey) (*scheduler.ReadLanes, error)
}

type Service struct {
	repository  *exportjob.Repository
	connections ConnectionRuntime
	worker      *exportworker.Worker
	scheduler   *exportlane.Scheduler

	mu       sync.Mutex
	attempts map[artifactKey]*artifactAttempt
}

type artifactKey struct{ jobID, artifactID string }
type artifactAttempt struct {
	id      string
	ordinal uint64
}

func New(repository *exportjob.Repository, connections ConnectionRuntime) (*Service, error) {
	if repository == nil || connections == nil {
		return nil, exportjob.ErrInvalidRequest
	}
	service := &Service{
		repository: repository, connections: connections,
		attempts: make(map[artifactKey]*artifactAttempt),
	}
	worker, err := exportworker.New(service.dispatcher, repository, exportworker.TargetReservationHooks{
		Reserve: service.reserve,
		Settle:  service.settle,
		Abort:   service.abort,
	})
	if err != nil {
		return nil, err
	}
	service.worker = worker
	laneScheduler, err := exportlane.New(service, service.lanes)
	if err != nil {
		return nil, err
	}
	service.scheduler = laneScheduler
	return service, nil
}

func (s *Service) Schedule(ctx context.Context, job exportjob.Job) (exportjob.Job, error) {
	plan, err := rebuildPlan(job)
	if err != nil {
		return s.failCreated(ctx, job, err)
	}
	if _, err := s.connections.ExportDispatcher(plan.Generation); err != nil {
		return s.pauseCreated(ctx, job, err)
	}
	if lanes, err := s.connections.ExportReadLanes(plan.Generation); err != nil || lanes == nil {
		if err == nil {
			err = exportlane.ErrLaneUnavailable
		}
		return s.pauseCreated(ctx, job, err)
	}
	if err := s.worker.RegisterPlan(plan); err != nil {
		return s.failCreated(ctx, job, err)
	}
	if current, getErr := s.scheduler.Get(job.Task.ID); getErr == nil {
		if current.Generation != plan.Generation || current.Terminal {
			return s.pauseCreated(ctx, job, exportlane.ErrStateConflict)
		}
		if current.State == exportlane.StatePausedRestartable && job.Task.State == exportjob.StateQueued {
			if _, err := s.scheduler.Restart(job.Task.ID); err != nil {
				return s.pauseCreated(ctx, job, err)
			}
		}
		return job, nil
	} else if !errors.Is(getErr, exportlane.ErrJobNotFound) {
		return s.pauseCreated(ctx, job, getErr)
	}
	request, err := plan.LaneRequest()
	if err == nil {
		_, err = s.scheduler.Submit(request)
	}
	if errors.Is(err, exportlane.ErrDuplicateJob) {
		return job, nil
	}
	if err != nil {
		if _, getErr := s.scheduler.Get(job.Task.ID); errors.Is(getErr, exportlane.ErrJobNotFound) {
			s.worker.UnregisterPlan(job.Task.ID)
		}
		return s.pauseCreated(ctx, job, err)
	}
	return job, nil
}

func (s *Service) ExecuteQuantum(
	ctx context.Context,
	generation exportlane.GenerationKey,
	jobID string,
	quantum exportlane.Quantum,
	timeouts exportlane.Timeouts,
) error {
	job, err := s.repository.Get(context.WithoutCancel(ctx), jobID)
	if err != nil {
		return err
	}
	if job.Task.State == exportjob.StateQueued {
		job, err = s.repository.BeginRun(context.WithoutCancel(ctx), exportjob.WorkerTransition{
			JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
		})
		if err != nil {
			return err
		}
	}
	if job.Task.State != exportjob.StateRunning || job.CancelRequested {
		return context.Canceled
	}

	err = s.worker.ExecuteQuantum(ctx, generation, jobID, quantum, timeouts)
	if err != nil {
		return s.settleExecutionError(ctx, jobID, err)
	}
	if _, _, complete := s.worker.Manifest(jobID); !complete {
		return nil
	}
	job, err = s.repository.Get(context.WithoutCancel(ctx), jobID)
	if err != nil {
		return err
	}
	if job.CancelRequested {
		_, _ = s.repository.FinishCancel(context.WithoutCancel(ctx), exportjob.WorkerTransition{
			JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
		})
		return context.Canceled
	}
	job, err = s.repository.BeginFinalizing(context.WithoutCancel(ctx), exportjob.WorkerTransition{
		JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil {
		return err
	}
	_, err = s.repository.CompleteDelivery(context.WithoutCancel(ctx), exportjob.WorkerTransition{
		JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
	})
	if err != nil {
		return s.settleExecutionError(ctx, jobID, err)
	}
	return nil
}

func (s *Service) Cancel(ctx context.Context, jobID string, envelope exportjob.CommandEnvelope) (exportjob.Job, error) {
	job, replayed, err := s.repository.Cancel(ctx, jobID, envelope)
	if err != nil {
		return job, err
	}
	if _, schedulerErr := s.scheduler.Cancel(jobID); schedulerErr != nil &&
		!errors.Is(schedulerErr, exportlane.ErrJobNotFound) {
		return job, schedulerErr
	}
	_ = replayed
	return s.repository.Get(context.WithoutCancel(ctx), jobID)
}

func (s *Service) Restart(ctx context.Context, jobID string, envelope exportjob.CommandEnvelope) (exportjob.Job, error) {
	before, err := s.repository.Get(ctx, jobID)
	if err != nil {
		return exportjob.Job{}, err
	}
	if before.Task.State == exportjob.StatePausedRestartable {
		if err := s.prepareRestart(ctx, before); err != nil {
			return before, err
		}
	}
	job, replayed, err := s.repository.Restart(ctx, jobID, envelope)
	if err != nil {
		return job, err
	}
	if replayed && job.Task.State != exportjob.StateQueued {
		return job, nil
	}
	return s.Schedule(ctx, job)
}

func (s *Service) Cleanup(ctx context.Context, jobID string, envelope exportjob.CommandEnvelope) (exportjob.Job, error) {
	job, _, err := s.repository.Cleanup(ctx, jobID, envelope)
	if err != nil || job.Task.State != exportjob.StateCleaning {
		return job, err
	}
	_, _ = s.scheduler.Cancel(jobID)
	s.worker.UnregisterPlan(jobID)
	plan, err := rebuildPlan(job)
	if err != nil {
		return job, err
	}
	remove := removeManagedArtifacts
	if job.CleanupFinalState != nil && *job.CleanupFinalState == exportjob.StateSucceeded {
		remove = removeManagedParts
	}
	if err := remove(plan); err != nil {
		return job, err
	}
	return s.repository.CompleteCleanupAfterFilesRemoved(context.WithoutCancel(ctx), exportjob.CleanupCompletion{
		JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
	})
}

// RestorePaused reconstructs only immutable in-memory plans. It never submits
// work and therefore cannot open a network path during startup recovery.
func (s *Service) RestorePaused(ctx context.Context) error {
	jobs, err := s.repository.List(ctx, exportjob.ListFilter{State: exportjob.StatePausedRestartable, Limit: 500})
	if err != nil {
		return err
	}
	for _, job := range jobs {
		plan, err := rebuildPlan(job)
		if err != nil {
			return err
		}
		if err := s.worker.RegisterPlan(plan); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) PauseGeneration(ctx context.Context, generation exportlane.GenerationKey) error {
	if err := s.scheduler.PauseGeneration(ctx, generation); err != nil {
		return err
	}
	jobs, err := s.activeJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.ConnectionID != generation.ConnectionID || job.ConnectionGeneration != generation.Generation ||
			job.Task.State == exportjob.StatePausedRestartable || job.CancelRequested {
			continue
		}
		if _, err := s.repository.PauseRestartable(context.WithoutCancel(ctx), exportjob.WorkerTransition{
			JobID: job.Task.ID, ExpectedStateRevision: job.Task.StateRevision,
			PublicErrorCode: "CONNECTION_CLOSED",
		}); err != nil && !errors.Is(err, tasks.ErrRevisionConflict) {
			return err
		}
	}
	return nil
}

func (s *Service) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	jobs, _ := s.activeJobs(ctx)
	seen := make(map[exportlane.GenerationKey]struct{})
	for _, job := range jobs {
		key := exportlane.GenerationKey{ConnectionID: job.ConnectionID, Generation: job.ConnectionGeneration}
		if _, found := seen[key]; found {
			continue
		}
		seen[key] = struct{}{}
		_ = s.PauseGeneration(ctx, key)
	}
	s.scheduler.Close()
}

func (s *Service) activeJobs(ctx context.Context) ([]exportjob.Job, error) {
	var result []exportjob.Job
	for _, state := range []string{exportjob.StateQueued, exportjob.StateRunning, exportjob.StateFinalizing} {
		jobs, err := s.repository.List(ctx, exportjob.ListFilter{State: state, Limit: 500})
		if err != nil {
			return nil, err
		}
		result = append(result, jobs...)
	}
	return result, nil
}

func (s *Service) prepareRestart(ctx context.Context, job exportjob.Job) error {
	plan, err := rebuildPlan(job)
	if err != nil {
		return err
	}
	for _, fragment := range job.Fragments {
		switch fragment.State {
		case exportjob.FragmentComplete:
			validator := exportjob.FragmentValidator(exportjob.GzipFileValidator{})
			if fragment.Kind == exportjob.FragmentManifest {
				validator = manifestValidator{plan: plan}
			}
			if _, err := s.repository.ValidateFragmentForReuse(ctx, fragment.FragmentID, validator); err != nil {
				return err
			}
		case exportjob.FragmentWriting, exportjob.FragmentFinalizing, exportjob.FragmentCorrupt:
			if err := removeFragmentFiles(fragment); err != nil {
				return err
			}
			if fragment.State != exportjob.FragmentCorrupt {
				if _, err := s.repository.MarkFragmentCorrupt(ctx, fragment.FragmentID); err != nil {
					return err
				}
			}
			if _, err := s.repository.AbortArtifactReservation(context.WithoutCancel(ctx), job.Task.ID, fragment.FragmentID); err != nil &&
				!errors.Is(err, exportjob.ErrArtifactReservation) {
				return err
			}
		}
	}
	return nil
}

type manifestValidator struct{ plan exportworker.Plan }

func (v manifestValidator) ValidateFragment(ctx context.Context, fragment exportjob.Fragment) (exportjob.ArtifactValidation, error) {
	if fragment.FinalPath == nil || fragment.Kind != exportjob.FragmentManifest {
		return exportjob.ArtifactValidation{}, exportjob.ErrFragmentCorrupt
	}
	file, err := os.Open(*fragment.FinalPath)
	if err != nil {
		return exportjob.ArtifactValidation{}, err
	}
	hash := sha256.New()
	data, err := io.ReadAll(io.TeeReader(file, hash))
	closeErr := file.Close()
	if err != nil {
		return exportjob.ArtifactValidation{}, err
	}
	if closeErr != nil {
		return exportjob.ArtifactValidation{}, closeErr
	}
	if err := ctx.Err(); err != nil {
		return exportjob.ArtifactValidation{}, err
	}
	var manifest exportworker.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil || manifest.SchemaVersion != exportworker.ManifestSchemaV1 ||
		manifest.Database != v.plan.Spec.Database || manifest.RetentionPolicy != v.plan.Spec.RetentionPolicy ||
		manifest.Measurement != v.plan.Spec.Measurement || manifest.Range.StartNS != v.plan.Spec.StartNS ||
		manifest.Range.EndNS != v.plan.Spec.EndNS || manifest.SnapshotConsistent {
		return exportjob.ArtifactValidation{}, exportjob.ErrFragmentCorrupt
	}
	return exportjob.ArtifactValidation{
		CompressedSize: strconv.Itoa(len(data)), UncompressedSize: strconv.Itoa(len(data)),
		ChecksumSHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

func removeManagedArtifacts(plan exportworker.Plan) error {
	info, err := os.Lstat(plan.Spec.OutputDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return exportworker.ErrArtifactState
	}
	for _, slice := range plan.Slices {
		for _, path := range []string{slice.PartPath, slice.FinalPath} {
			if err := removeExact(path); err != nil {
				return err
			}
		}
	}
	for _, path := range []string{plan.ManifestPartPath, plan.ManifestFinalPath} {
		if err := removeExact(path); err != nil {
			return err
		}
	}
	if err := os.Remove(plan.Spec.OutputDirectory); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func removeManagedParts(plan exportworker.Plan) error {
	info, err := os.Lstat(plan.Spec.OutputDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return exportworker.ErrArtifactState
	}
	for _, slice := range plan.Slices {
		if err := removeExact(slice.PartPath); err != nil {
			return err
		}
	}
	return removeExact(plan.ManifestPartPath)
}

func removeFragmentFiles(fragment exportjob.Fragment) error {
	for _, path := range []*string{fragment.PartPath, fragment.FinalPath} {
		if path != nil {
			if err := removeExact(*path); err != nil {
				return err
			}
		}
	}
	return nil
}

func removeExact(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return exportworker.ErrArtifactState
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return exportworker.ErrArtifactState
	}
	return nil
}

func (s *Service) settleExecutionError(ctx context.Context, jobID string, executionErr error) error {
	job, getErr := s.repository.Get(context.WithoutCancel(ctx), jobID)
	if getErr != nil {
		return getErr
	}
	if job.CancelRequested {
		_, finishErr := s.repository.FinishCancel(context.WithoutCancel(ctx), exportjob.WorkerTransition{
			JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
		})
		if finishErr != nil && !errors.Is(finishErr, tasks.ErrRevisionConflict) {
			return finishErr
		}
		return context.Canceled
	}
	transition := exportjob.WorkerTransition{
		JobID: jobID, ExpectedStateRevision: job.Task.StateRevision,
		PublicErrorCode: safeErrorCode(executionErr),
	}
	if strictFailure(executionErr) {
		_, transitionErr := s.repository.Fail(context.WithoutCancel(ctx), transition)
		if transitionErr != nil {
			return transitionErr
		}
		return executionErr
	}
	_, transitionErr := s.repository.PauseRestartable(context.WithoutCancel(ctx), transition)
	if transitionErr != nil {
		return transitionErr
	}
	return exportlane.Restartable(transition.PublicErrorCode, executionErr)
}

func (s *Service) failCreated(ctx context.Context, job exportjob.Job, cause error) (exportjob.Job, error) {
	failed, err := s.repository.Fail(context.WithoutCancel(ctx), exportjob.WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: job.Task.StateRevision,
		PublicErrorCode: safeErrorCode(cause),
	})
	if err != nil {
		return job, err
	}
	return failed, nil
}

func (s *Service) pauseCreated(ctx context.Context, job exportjob.Job, cause error) (exportjob.Job, error) {
	paused, err := s.repository.PauseRestartable(context.WithoutCancel(ctx), exportjob.WorkerTransition{
		JobID: job.Task.ID, ExpectedStateRevision: job.Task.StateRevision,
		PublicErrorCode: safeErrorCode(cause),
	})
	if err != nil {
		return job, err
	}
	return paused, nil
}

func rebuildPlan(job exportjob.Job) (exportworker.Plan, error) {
	if job.Plan.SchemaVersion != 1 {
		return exportworker.Plan{}, exportworker.ErrInvalidPlan
	}
	return exportworker.BuildPlan(job.Task.ID, exportlane.GenerationKey{
		ConnectionID: job.ConnectionID, Generation: job.ConnectionGeneration,
	}, exportworker.MeasurementSpec{
		Database: job.Plan.Database, RetentionPolicy: job.Plan.RetentionPolicy,
		Measurement: job.Plan.Measurement, StartNS: job.Plan.StartNS, EndNS: job.Plan.EndNS,
		SliceWidthNS: job.Plan.SliceWidthNS, OutputDirectory: job.Plan.OutputDirectory,
		TypePreserving: job.Plan.TypePreserving, Lossy: job.Plan.Lossy,
	})
}

func (s *Service) dispatcher(key exportlane.GenerationKey) (exportworker.RoundTripDispatcher, error) {
	return s.connections.ExportDispatcher(key)
}

func (s *Service) lanes(key exportlane.GenerationKey) *scheduler.ReadLanes {
	lanes, err := s.connections.ExportReadLanes(key)
	if err != nil {
		return nil
	}
	return lanes
}

func (s *Service) reserve(ctx context.Context, jobID, artifactID string, bytes int64) error {
	key := artifactKey{jobID: jobID, artifactID: artifactID}
	s.mu.Lock()
	attempt := s.attempts[key]
	if attempt == nil {
		attempt = &artifactAttempt{id: uuid.NewString()}
		s.attempts[key] = attempt
	}
	ordinal := attempt.ordinal
	s.mu.Unlock()

	job, err := s.repository.Get(context.WithoutCancel(ctx), jobID)
	if err != nil {
		return err
	}
	volume, err := transfer.SystemStagingVolumeStat(ctx, job.Plan.OutputDirectory)
	if err != nil {
		return err
	}
	if volume.ID != job.TargetVolumeID {
		return transfer.ErrTargetLowSpace
	}
	_, err = s.repository.ReserveArtifactExtent(context.WithoutCancel(ctx), exportjob.ReserveArtifactExtentRequest{
		JobID: jobID, ArtifactID: artifactID, AttemptID: attempt.id,
		ExtentOrdinal: strconv.FormatUint(ordinal, 10), VolumeID: volume.ID,
		RequestedBytes: bytes, VolumeFreeBytes: volume.FreeBytes,
		VolumeCapacity: volume.CapacityBytes, Takeover: ordinal == 0,
	})
	if err == nil {
		s.mu.Lock()
		if current := s.attempts[key]; current == attempt && current.ordinal == ordinal {
			current.ordinal++
		}
		s.mu.Unlock()
	}
	return err
}

func (s *Service) settle(ctx context.Context, jobID, artifactID string, actual int64) error {
	_, err := s.repository.SettleArtifactReservation(context.WithoutCancel(ctx), jobID, artifactID, actual)
	s.mu.Lock()
	delete(s.attempts, artifactKey{jobID: jobID, artifactID: artifactID})
	s.mu.Unlock()
	return err
}

func (s *Service) abort(ctx context.Context, jobID, artifactID string) error {
	_, err := s.repository.AbortArtifactReservation(context.WithoutCancel(ctx), jobID, artifactID)
	s.mu.Lock()
	delete(s.attempts, artifactKey{jobID: jobID, artifactID: artifactID})
	s.mu.Unlock()
	return err
}

func strictFailure(err error) bool {
	var decode *query.DecodeError
	return errors.As(err, &decode) || errors.Is(err, exportworker.ErrSchemaDrift) ||
		errors.Is(err, exportworker.ErrUnknownTag) || errors.Is(err, exportworker.ErrUnknownField) ||
		errors.Is(err, exportworker.ErrTagFieldConflict) || errors.Is(err, exportworker.ErrResponseTooLarge) ||
		errors.Is(err, exportworker.ErrInvalidPlan) || errors.Is(err, exportworker.ErrQuantumMismatch)
}

func safeErrorCode(err error) string {
	switch {
	case errors.Is(err, exportlane.ErrHeaderTimeout):
		return exportlane.ErrorHeaderTimeout
	case errors.Is(err, exportlane.ErrIdleTimeout):
		return exportlane.ErrorIdleTimeout
	case errors.Is(err, exportlane.ErrRequestDeadline):
		return exportlane.ErrorRequestDeadline
	case errors.Is(err, transfer.ErrTargetLowSpace):
		return "TARGET_VOLUME_LOW_SPACE"
	case errors.Is(err, exportworker.ErrSchemaDrift):
		return "EXPORT_SCHEMA_DRIFT"
	case errors.Is(err, exportworker.ErrUnknownTag), errors.Is(err, exportworker.ErrUnknownField),
		errors.Is(err, exportworker.ErrTagFieldConflict):
		return "EXPORT_SCHEMA_CONFLICT"
	default:
		return ErrorRestartRequired
	}
}

var _ exportlane.Executor = (*Service)(nil)
