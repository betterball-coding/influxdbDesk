package connection

import (
	"context"
	"sync"

	"github.com/influxdesk/influxdesk/internal/importworker"
	"github.com/influxdesk/influxdesk/internal/transfer"
)

type importRunnerSpec struct {
	profileID    string
	connectionID string
	generation   string
	jobID        string
	segmentID    string
	active       *activeConnection
	repository   *transfer.Repository
	stagingRoot  string
}

type importRunner struct {
	spec     importRunnerSpec
	stop     chan struct{}
	done     chan struct{}
	runCtx   context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func (r *importRunner) requestStop() {
	r.stopOnce.Do(func() { close(r.stop) })
}

func (r *importRunner) cancelInFlight() {
	r.requestStop()
	r.cancel()
}

func (m *Manager) ensureImportRunner(profileID string, active *activeConnection, job transfer.ImportJob) {
	if active == nil || active.importWorker == nil || job.Task.State != transfer.ImportRunning ||
		job.ActiveRunSegmentID == nil {
		return
	}
	spec := importRunnerSpec{
		profileID: profileID, connectionID: active.snapshot.ConnectionID,
		generation: active.snapshot.ConnectionGeneration, jobID: job.Task.ID,
		segmentID: *job.ActiveRunSegmentID, active: active,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stagingDirectory == "" || m.transfers == nil || m.active[profileID] != active {
		return
	}
	spec.repository = m.transfers
	spec.stagingRoot = m.stagingDirectory
	if current := m.importRunners[job.Task.ID]; current != nil {
		if current.spec.segmentID == spec.segmentID {
			return
		}
		m.importPending[job.Task.ID] = spec
		current.requestStop()
		return
	}
	m.launchImportRunnerLocked(spec)
}

func (m *Manager) launchImportRunnerLocked(spec importRunnerSpec) {
	runCtx, cancel := context.WithCancel(context.Background())
	runner := &importRunner{
		spec: spec, stop: make(chan struct{}), done: make(chan struct{}),
		runCtx: runCtx, cancel: cancel,
	}
	m.importRunners[spec.jobID] = runner
	delete(m.importPending, spec.jobID)
	go m.runImport(runner)
}

func (m *Manager) runImport(runner *importRunner) {
	defer m.finishImportRunner(runner)

	repository := runner.spec.repository
	if repository == nil || runner.spec.stagingRoot == "" {
		return
	}

	for {
		select {
		case <-runner.stop:
			return
		default:
		}

		source, err := repository.GetImportRuntimeSource(runner.runCtx, runner.spec.jobID, runner.spec.stagingRoot)
		if err != nil {
			m.pauseFailedImportRunner(repository, runner)
			return
		}

		// Lock and task control close only stop, so an attempt already beyond the
		// barrier retains its natural settlement. Connection Close also cancels
		// runCtx; Worker then settles a crossed barrier as UNKNOWN before done.
		result, err := runner.spec.active.importWorker.RunNext(runner.runCtx, importworker.RunNextRequest{
			JobID: runner.spec.jobID, StagingPath: source.StagingPath,
			TargetDigest: source.TargetDigest, Database: source.Database,
			RetentionPolicy: source.RetentionPolicy,
		})
		if err != nil {
			job, readErr := repository.GetImport(context.Background(), runner.spec.jobID)
			if readErr == nil && job.Task.State == transfer.ImportRunning &&
				job.ActiveRunSegmentID != nil && *job.ActiveRunSegmentID == runner.spec.segmentID {
				m.pauseFailedImportRunner(repository, runner)
			}
			return
		}
		if result.Job.Task.State != transfer.ImportRunning || result.Job.ActiveRunSegmentID == nil ||
			*result.Job.ActiveRunSegmentID != runner.spec.segmentID {
			return
		}
	}
}

func (m *Manager) pauseFailedImportRunner(repository *transfer.Repository, runner *importRunner) {
	_, _ = repository.PauseRun(context.Background(), runner.spec.jobID, runner.spec.segmentID,
		transfer.PermitInvalidatedRunnerStopped)
}

func (m *Manager) finishImportRunner(runner *importRunner) {
	runner.cancel()
	defer close(runner.done)
	var pending importRunnerSpec
	var hasPending bool
	m.mu.Lock()
	if m.importRunners[runner.spec.jobID] == runner {
		delete(m.importRunners, runner.spec.jobID)
		pending, hasPending = m.importPending[runner.spec.jobID]
		delete(m.importPending, runner.spec.jobID)
	}
	m.mu.Unlock()
	if !hasPending {
		return
	}
	job, err := pending.repository.GetImport(context.Background(), pending.jobID)
	if err != nil || job.Task.State != transfer.ImportRunning || job.ActiveRunSegmentID == nil ||
		*job.ActiveRunSegmentID != pending.segmentID {
		return
	}
	m.ensureImportRunner(pending.profileID, pending.active, job)
}

func (m *Manager) stopImportRunner(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.importPending, jobID)
	if runner := m.importRunners[jobID]; runner != nil {
		runner.requestStop()
	}
}

func (m *Manager) stopImportRunnersForGeneration(connectionID, generation string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopImportRunnersForGenerationLocked(connectionID, generation)
}

func (m *Manager) stopImportRunnersForGenerationLocked(connectionID, generation string) {
	for jobID, runner := range m.importRunners {
		if runner.spec.connectionID == connectionID && runner.spec.generation == generation {
			delete(m.importPending, jobID)
			runner.requestStop()
		}
	}
}

func (m *Manager) cancelImportRunnersForGenerationLocked(connectionID, generation string) []*importRunner {
	runners := make([]*importRunner, 0)
	for jobID, runner := range m.importRunners {
		if runner.spec.connectionID == connectionID && runner.spec.generation == generation {
			delete(m.importPending, jobID)
			runner.cancelInFlight()
			runners = append(runners, runner)
		}
	}
	return runners
}

func waitImportRunners(ctx context.Context, runners []*importRunner) error {
	for _, runner := range runners {
		select {
		case <-runner.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
