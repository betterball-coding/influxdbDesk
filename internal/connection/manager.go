package connection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/importworker"
	"github.com/influxdesk/influxdesk/internal/operation"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/scheduler"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

var (
	ErrAlreadyOpen      = errors.New("CONNECTION_ALREADY_OPEN")
	ErrNotOpen          = errors.New("CONNECTION_NOT_OPEN")
	ErrProbeFailed      = errors.New("CONNECTION_PROBE_FAILED")
	ErrCredentialAbsent = errors.New("CONNECTION_CREDENTIAL_UNAVAILABLE")
	ErrImportRunnerBusy = errors.New("IMPORT_RUNNER_ACTIVE")
	ErrImportTarget     = errors.New("IMPORT_TARGET_MISMATCH")
)

type credentialReader interface {
	Get(string, credential.Kind) ([]byte, error)
}

type Snapshot struct {
	ConnectionID         string              `json:"connectionId"`
	ConnectionGeneration string              `json:"connectionGeneration"`
	ProfileID            string              `json:"profileId"`
	ProfileRevision      string              `json:"profileRevision"`
	Version              string              `json:"version,omitempty"`
	Protection           protection.Snapshot `json:"protection"`
}

type ProbeResult struct {
	OK        bool   `json:"ok"`
	LatencyMS int64  `json:"latencyMs"`
	Version   string `json:"version,omitempty"`
}

type activeConnection struct {
	snapshot     Snapshot
	dispatcher   *transport.Dispatcher
	readLanes    *scheduler.ReadLanes
	queries      *query.Service
	operations   *operation.Service
	imports      *transfer.RunAuthorizer
	importWorker *importworker.Worker
}

type Manager struct {
	profiles         *profile.Repository
	credentials      credentialReader
	protection       *protection.Manager
	now              func() time.Time
	queryOptions     query.ServiceOptions
	store            *store.Store
	tasks            *tasks.Repository
	transfers        *transfer.Repository
	stagingDirectory string
	importCleanup    transfer.ImportCleanupFileFunc

	mu              sync.RWMutex
	active          map[string]*activeConnection
	sessions        map[string]*query.Service
	operationRoutes map[string]*operation.Service
	importRunners   map[string]*importRunner
	importPending   map[string]importRunnerSpec
}

func (m *Manager) EnableOperations(s *store.Store, taskRepository *tasks.Repository) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.active) != 0 {
		panic("operation dependencies must be configured before opening connections")
	}
	m.store, m.tasks = s, taskRepository
}

func (m *Manager) EnableTransfers(repository *transfer.Repository, stagingDirectory ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.active) != 0 {
		panic("transfer dependencies must be configured before opening connections")
	}
	if len(stagingDirectory) > 1 {
		panic("at most one staging directory may be configured")
	}
	m.transfers = repository
	if len(stagingDirectory) == 1 {
		m.stagingDirectory = stagingDirectory[0]
	}
}

func (m *Manager) EnableImportCleanup(removeFiles transfer.ImportCleanupFileFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.active) != 0 {
		panic("import cleanup dependencies must be configured before opening connections")
	}
	m.importCleanup = removeFiles
}

func NewManager(profiles *profile.Repository, credentials credentialReader, protectionManager *protection.Manager, now func() time.Time) *Manager {
	return NewManagerWithQueryOptions(profiles, credentials, protectionManager, now, query.ServiceOptions{})
}

func NewManagerWithQueryOptions(profiles *profile.Repository, credentials credentialReader, protectionManager *protection.Manager, now func() time.Time, queryOptions query.ServiceOptions) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{
		profiles: profiles, credentials: credentials, protection: protectionManager, now: now,
		queryOptions: queryOptions,
		active:       make(map[string]*activeConnection), sessions: make(map[string]*query.Service),
		operationRoutes: make(map[string]*operation.Service),
		importRunners:   make(map[string]*importRunner), importPending: make(map[string]importRunnerSpec),
	}
}

func (m *Manager) Open(ctx context.Context, profileID string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.active[profileID]; existing != nil {
		return Snapshot{}, ErrAlreadyOpen
	}
	value, err := m.profiles.Get(ctx, profileID)
	if err != nil {
		return Snapshot{}, err
	}
	auth, err := m.authFor(value)
	if err != nil {
		return Snapshot{}, err
	}
	generation, err := m.profiles.NextGeneration(ctx, profileID)
	if err != nil {
		return Snapshot{}, err
	}
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: value.BaseURL, Auth: auth})
	if err != nil {
		return Snapshot{}, err
	}
	probe, err := probe(ctx, dispatcher, m.now)
	if err != nil {
		dispatcher.CloseIdleConnections()
		return Snapshot{}, err
	}
	protectionSnapshot, err := m.protection.Register(ctx, protection.RegisterRequest{
		ConnectionID: profileID, ConnectionGeneration: generation,
		ProfileID: profileID, ProfileRevision: value.Revision, Mode: value.ProtectionMode,
	})
	if err != nil {
		dispatcher.CloseIdleConnections()
		return Snapshot{}, err
	}
	var queryService *query.Service
	if m.store != nil && m.tasks != nil {
		queryService, err = query.NewPersistentService(dispatcher, m.store, m.tasks, m.queryOptions)
	} else {
		queryService, err = query.NewService(dispatcher, m.queryOptions)
	}
	if err != nil {
		_ = m.protection.CloseGeneration(context.Background(), profileID, generation)
		dispatcher.CloseIdleConnections()
		return Snapshot{}, err
	}
	var operationService *operation.Service
	if m.store != nil && m.tasks != nil {
		operationService = operation.NewService(m.store, m.tasks, m.protection, dispatcher,
			value.ID, value.Revision, profileID, generation, operation.Options{})
	}
	var importAuthorizer *transfer.RunAuthorizer
	var importWorker *importworker.Worker
	if m.transfers != nil {
		importAuthorizer = transfer.NewRunAuthorizer(m.transfers, m.protection, nil,
			value.ID, value.Revision, profileID, generation, m.now)
		importWorker, err = importworker.New(importAuthorizer, m.transfers, dispatcher, importworker.Options{})
		if err != nil {
			_ = m.protection.CloseGeneration(context.Background(), profileID, generation)
			dispatcher.CloseIdleConnections()
			return Snapshot{}, err
		}
	}
	snapshot := Snapshot{
		ConnectionID: profileID, ConnectionGeneration: generation,
		ProfileID: profileID, ProfileRevision: value.Revision, Version: probe.Version,
		Protection: protectionSnapshot,
	}
	m.active[profileID] = &activeConnection{
		snapshot: snapshot, dispatcher: dispatcher, readLanes: scheduler.NewReadLanes(), queries: queryService,
		operations: operationService, imports: importAuthorizer, importWorker: importWorker,
	}
	return snapshot, nil
}

// ExportDispatcher returns the sealed dispatcher owned by exactly one active
// connection generation. Callers may only pass AuthorizedReadQuery values to
// the export worker; arbitrary HTTP construction remains inside transport.
func (m *Manager) ExportDispatcher(key exportlane.GenerationKey) (*transport.Dispatcher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	active := m.active[key.ConnectionID]
	if !matchesGeneration(active, key) {
		return nil, ErrNotOpen
	}
	return active.dispatcher, nil
}

// ExportReadLanes returns the generation-local 4+1 read budget. A missing or
// stale generation is an error; this method never manufactures a fallback lane.
func (m *Manager) ExportReadLanes(key exportlane.GenerationKey) (*scheduler.ReadLanes, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	active := m.active[key.ConnectionID]
	if !matchesGeneration(active, key) {
		return nil, ErrNotOpen
	}
	return active.readLanes, nil
}

func matchesGeneration(active *activeConnection, key exportlane.GenerationKey) bool {
	return active != nil && key.ConnectionID != "" && key.Generation != "" &&
		active.snapshot.ConnectionID == key.ConnectionID &&
		active.snapshot.ConnectionGeneration == key.Generation
}

func (m *Manager) Close(ctx context.Context, profileID string) error {
	m.mu.Lock()
	active := m.active[profileID]
	if active == nil {
		m.mu.Unlock()
		return ErrNotOpen
	}
	delete(m.active, profileID)
	transferRepository := m.transfers
	importRunners := m.cancelImportRunnersForGenerationLocked(
		active.snapshot.ConnectionID, active.snapshot.ConnectionGeneration)
	m.mu.Unlock()

	for _, session := range active.queries.ListActiveQuerySessions() {
		if !session.Terminal {
			_, _ = active.queries.CancelQuery(context.Background(), session.ID, query.CommandEnvelope{
				CommandRequestID: uuid.NewString(), ExpectedStateRevision: session.StateRevision,
			})
		}
	}
	currentProtection, err := m.protection.Get(active.snapshot.ConnectionID, active.snapshot.ConnectionGeneration)
	if err != nil {
		waitErr := waitImportRunners(ctx, importRunners)
		active.dispatcher.CloseIdleConnections()
		return errors.Join(err, waitErr)
	}
	invalidatedRevision := currentProtection.ProtectionRevision
	if err := m.protection.CloseGeneration(ctx, active.snapshot.ConnectionID, active.snapshot.ConnectionGeneration); err != nil {
		// The generation remains unavailable in-memory even if persistence fails;
		// startup reconciliation will close it before networking is reopened.
		waitErr := waitImportRunners(ctx, importRunners)
		active.dispatcher.CloseIdleConnections()
		return errors.Join(err, waitErr)
	}
	if transferRepository != nil {
		if _, err := transferRepository.InvalidatePermits(context.Background(), active.snapshot.ConnectionID,
			active.snapshot.ConnectionGeneration, invalidatedRevision, transfer.PermitInvalidatedConnectionClosed); err != nil {
			waitErr := waitImportRunners(ctx, importRunners)
			active.dispatcher.CloseIdleConnections()
			return errors.Join(fmt.Errorf("connection closed but import pause failed: %w", err), waitErr)
		}
	}
	if err := waitImportRunners(ctx, importRunners); err != nil {
		active.dispatcher.CloseIdleConnections()
		return err
	}
	active.dispatcher.CloseIdleConnections()
	return nil
}

func (m *Manager) Get(profileID string) (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	active := m.active[profileID]
	if active == nil {
		return Snapshot{}, ErrNotOpen
	}
	snapshot := active.snapshot
	current, err := m.protection.Get(snapshot.ConnectionID, snapshot.ConnectionGeneration)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Protection = current
	return snapshot, nil
}

func (m *Manager) IsOpen(profileID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active[profileID] != nil
}

func (m *Manager) CloseAll(ctx context.Context) error {
	m.mu.RLock()
	ids := make([]string, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	var first error
	for _, id := range ids {
		if err := m.Close(ctx, id); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) ListQuerySessions(ctx context.Context) ([]query.QuerySession, error) {
	if m.store != nil {
		return query.ListStoredSessions(ctx, m.store, 1000)
	}
	m.mu.RLock()
	services := make(map[*query.Service]struct{}, len(m.sessions))
	for _, service := range m.sessions {
		services[service] = struct{}{}
	}
	m.mu.RUnlock()
	var result []query.QuerySession
	for service := range services {
		result = append(result, service.ListActiveQuerySessions()...)
	}
	return result, nil
}

func (m *Manager) TestSaved(ctx context.Context, profileID string) (ProbeResult, error) {
	value, err := m.profiles.Get(ctx, profileID)
	if err != nil {
		return ProbeResult{}, err
	}
	auth, err := m.authFor(value)
	if err != nil {
		return ProbeResult{}, err
	}
	dispatcher, err := transport.NewDispatcher(transport.Config{BaseURL: value.BaseURL, Auth: auth})
	if err != nil {
		return ProbeResult{}, err
	}
	defer dispatcher.CloseIdleConnections()
	return probe(ctx, dispatcher, m.now)
}

// TestConfig probes an unsaved draft through the sealed Ping and ShowDatabases requests.
func TestConfig(ctx context.Context, config transport.Config, now func() time.Time) (ProbeResult, error) {
	dispatcher, err := transport.NewDispatcher(config)
	if err != nil {
		return ProbeResult{}, err
	}
	defer dispatcher.CloseIdleConnections()
	if now == nil {
		now = time.Now
	}
	return probe(ctx, dispatcher, now)
}

func (m *Manager) StartReadQuery(ctx context.Context, profileID string, request query.StartRequest) (query.QuerySession, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil {
		return query.QuerySession{}, ErrNotOpen
	}
	request.ConnectionID = active.snapshot.ConnectionID
	request.Generation = active.snapshot.ConnectionGeneration
	request.ProfileID = active.snapshot.ProfileID
	request.ProfileRevision = active.snapshot.ProfileRevision
	snapshot, err := active.queries.StartReadQuery(ctx, request)
	if err == nil {
		m.mu.Lock()
		m.sessions[snapshot.ID] = active.queries
		m.mu.Unlock()
	}
	return snapshot, err
}

func (m *Manager) GetQuerySession(ctx context.Context, sessionID string) (query.QuerySession, error) {
	m.mu.RLock()
	service := m.sessions[sessionID]
	m.mu.RUnlock()
	if service != nil {
		return service.GetQuerySession(sessionID)
	}
	return query.GetStoredSession(ctx, m.store, sessionID)
}

func (m *Manager) CancelQuery(ctx context.Context, sessionID string, request query.CommandEnvelope) (query.QuerySession, error) {
	m.mu.RLock()
	service := m.sessions[sessionID]
	m.mu.RUnlock()
	if service != nil {
		return service.CancelQuery(ctx, sessionID, request)
	}
	if m.store == nil || m.tasks == nil {
		return query.GetStoredSession(ctx, nil, sessionID)
	}
	return query.CancelStoredQuery(ctx, m.store, m.tasks, m.now, sessionID, request)
}

func (m *Manager) CloseQuery(ctx context.Context, sessionID string, request query.CommandEnvelope) (query.QuerySession, error) {
	m.mu.RLock()
	service := m.sessions[sessionID]
	m.mu.RUnlock()
	if service != nil {
		return service.CloseQuery(ctx, sessionID, request)
	}
	if m.store == nil || m.tasks == nil {
		return query.GetStoredSession(ctx, nil, sessionID)
	}
	return query.CloseStoredQuery(ctx, m.store, m.tasks, m.now, sessionID, request)
}

func (m *Manager) ListResultSeries(sessionID string, statementID int) ([]query.SeriesSummary, error) {
	service, err := m.queryService(sessionID)
	if err != nil {
		return nil, err
	}
	return service.ListResultSeries(sessionID, statementID)
}

func (m *Manager) GetResultPage(request query.PageRequest) (query.ResultPage, error) {
	service, err := m.queryService(request.SessionID)
	if err != nil {
		return query.ResultPage{}, err
	}
	return service.GetResultPage(request)
}

func (m *Manager) WriteResultCSV(
	ctx context.Context,
	request query.CSVExportRequest,
	output io.Writer,
	location *time.Location,
) (uint64, error) {
	service, err := m.queryService(request.SessionID)
	if err != nil {
		return 0, err
	}
	return service.WriteResultCSV(ctx, request, output, location)
}

func (m *Manager) GetTransientIssue(sessionID string) (string, error) {
	service, err := m.queryService(sessionID)
	if err != nil {
		return "", err
	}
	return service.GetTransientIssue(sessionID)
}

func (m *Manager) PreviewMutation(ctx context.Context, profileID string, request operation.PreviewRequest) (operation.Preview, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil || active.operations == nil {
		return operation.Preview{}, ErrNotOpen
	}
	return active.operations.Preview(ctx, request)
}

func (m *Manager) ExecuteMutation(ctx context.Context, profileID string, request operation.ExecuteRequest) (operation.Operation, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil || active.operations == nil {
		return operation.Operation{}, ErrNotOpen
	}
	out, err := active.operations.Execute(ctx, request)
	if err == nil {
		m.mu.Lock()
		m.operationRoutes[out.Task.ID] = active.operations
		m.mu.Unlock()
	}
	return out, err
}

func (m *Manager) GetOperation(ctx context.Context, operationID string) (operation.Operation, error) {
	if m.store == nil {
		return operation.Operation{}, operation.ErrNotFound
	}
	return operation.GetStored(ctx, m.store, operationID)
}

func (m *Manager) ListOperations(ctx context.Context, limit int) ([]operation.Operation, error) {
	if m.store == nil {
		return nil, operation.ErrNotFound
	}
	return operation.ListStored(ctx, m.store, limit)
}

func (m *Manager) CancelOperation(ctx context.Context, operationID string, request operation.CommandEnvelope) (operation.Operation, error) {
	m.mu.RLock()
	service := m.operationRoutes[operationID]
	m.mu.RUnlock()
	if service != nil {
		return service.Cancel(ctx, operationID, request)
	}
	if m.store == nil || m.tasks == nil {
		return operation.Operation{}, operation.ErrNotFound
	}
	return operation.CancelStored(ctx, m.store, m.tasks, m.now, operationID, request)
}

func (m *Manager) PreviewImportRun(ctx context.Context, profileID string, request transfer.PreviewRunRequest) (transfer.ImportRunPreview, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil || active.imports == nil {
		return transfer.ImportRunPreview{}, ErrNotOpen
	}
	return active.imports.Preview(ctx, request)
}

func (m *Manager) StartImport(ctx context.Context, profileID string, request transfer.AuthorizedStartRequest) (transfer.ImportJob, bool, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil || active.imports == nil {
		return transfer.ImportJob{}, false, ErrNotOpen
	}
	job, replayed, err := active.imports.Start(ctx, request)
	if err == nil && !replayed {
		m.ensureImportRunner(profileID, active, job)
	}
	return job, replayed, err
}

func (m *Manager) ResolveImportBatch(ctx context.Context, profileID string, request transfer.AuthorizedResolveRequest) (transfer.ImportJob, bool, error) {
	m.mu.RLock()
	active := m.active[profileID]
	m.mu.RUnlock()
	if active == nil || active.imports == nil {
		return transfer.ImportJob{}, false, ErrNotOpen
	}
	job, replayed, err := active.imports.Resolve(ctx, request)
	if err == nil && !replayed {
		m.ensureImportRunner(profileID, active, job)
	}
	return job, replayed, err
}

func (m *Manager) RunImportNext(
	ctx context.Context,
	profileID, jobID, database, retentionPolicy string,
) (importworker.Result, error) {
	m.mu.RLock()
	active := m.active[profileID]
	stagingDirectory := m.stagingDirectory
	runner := m.importRunners[jobID]
	m.mu.RUnlock()
	if active == nil || active.importWorker == nil || m.transfers == nil || stagingDirectory == "" {
		return importworker.Result{}, ErrNotOpen
	}
	if runner != nil {
		return importworker.Result{}, ErrImportRunnerBusy
	}
	job, err := m.transfers.GetImport(ctx, jobID)
	if err != nil {
		return importworker.Result{}, err
	}
	if job.Task.State == transfer.ImportSucceeded {
		return importworker.Result{Job: job, Outcome: importworker.OutcomeSucceeded}, nil
	}
	source, err := m.transfers.GetImportRuntimeSource(ctx, jobID, stagingDirectory)
	if err != nil {
		return importworker.Result{}, err
	}
	if database != source.Database || retentionPolicy != source.RetentionPolicy {
		return importworker.Result{}, ErrImportTarget
	}
	return active.importWorker.RunNext(ctx, importworker.RunNextRequest{
		JobID: jobID, StagingPath: source.StagingPath, TargetDigest: source.TargetDigest,
		Database: source.Database, RetentionPolicy: source.RetentionPolicy,
	})
}

func (m *Manager) CancelImport(
	ctx context.Context,
	jobID string,
	command transfer.ImportCommandEnvelope,
) (transfer.ImportJob, bool, error) {
	job, replayed, err := m.controlImport(ctx, jobID, func(ctx context.Context) (transfer.ImportJob, bool, error) {
		return m.transfers.CancelImport(ctx, jobID, command)
	})
	if err == nil {
		m.stopImportRunner(jobID)
	}
	return job, replayed, err
}

func (m *Manager) CleanupImport(
	ctx context.Context,
	jobID string,
	command transfer.ImportCommandEnvelope,
) (transfer.ImportJob, bool, error) {
	m.mu.RLock()
	removeFiles := m.importCleanup
	m.mu.RUnlock()
	if removeFiles == nil {
		return transfer.ImportJob{}, false, transfer.ErrStageFileCleanup
	}
	return m.controlImport(ctx, jobID, func(ctx context.Context) (transfer.ImportJob, bool, error) {
		return m.transfers.CleanupImport(ctx, jobID, command, removeFiles)
	})
}

func (m *Manager) controlImport(
	ctx context.Context,
	jobID string,
	control func(context.Context) (transfer.ImportJob, bool, error),
) (transfer.ImportJob, bool, error) {
	m.mu.RLock()
	repository := m.transfers
	m.mu.RUnlock()
	if repository == nil || control == nil {
		return transfer.ImportJob{}, false, ErrNotOpen
	}
	binding, err := repository.GetImportConnectionBinding(ctx, jobID)
	if err != nil {
		return transfer.ImportJob{}, false, err
	}
	var out transfer.ImportJob
	var replayed bool
	err = m.protection.WithGenerationGate(ctx, binding.ConnectionID, binding.ConnectionGeneration,
		func(gateContext context.Context) error {
			var controlErr error
			out, replayed, controlErr = control(gateContext)
			return controlErr
		})
	if err == nil {
		return out, replayed, nil
	}
	if !errors.Is(err, protection.ErrNotFound) {
		return transfer.ImportJob{}, false, err
	}

	// A closed generation is intentionally absent after restart. Startup Import
	// recovery has already removed every live Permit and reconciled SENDING
	// attempts before Manager exists, so this path cannot race a network worker.
	return control(ctx)
}

func (m *Manager) GetProtection(profileID string) (protection.Snapshot, error) {
	snapshot, err := m.Get(profileID)
	return snapshot.Protection, err
}

func (m *Manager) Unlock(ctx context.Context, request protection.UnlockRequest) (protection.CommandResult, error) {
	snapshot, err := m.Get(request.ConnectionID)
	if err != nil {
		return protection.CommandResult{}, err
	}
	request.ConnectionID = snapshot.ConnectionID
	return m.protection.Unlock(ctx, request)
}

func (m *Manager) Lock(ctx context.Context, request protection.LockRequest) (protection.CommandResult, error) {
	snapshot, err := m.Get(request.ConnectionID)
	if err != nil {
		return protection.CommandResult{}, err
	}
	request.ConnectionID = snapshot.ConnectionID
	result, err := m.protection.Lock(ctx, request)
	if err != nil || result.Replayed || m.transfers == nil {
		return result, err
	}
	m.stopImportRunnersForGeneration(snapshot.ConnectionID, snapshot.ConnectionGeneration)
	if _, pauseErr := m.transfers.InvalidatePermits(context.Background(), snapshot.ConnectionID,
		snapshot.ConnectionGeneration, snapshot.Protection.ProtectionRevision,
		transfer.PermitInvalidatedProtectionLocked); pauseErr != nil {
		return result, fmt.Errorf("protection locked but import pause failed: %w", pauseErr)
	}
	return result, nil
}

func (m *Manager) authFor(value profile.Profile) (transport.AuthConfig, error) {
	auth := transport.AuthConfig{Mode: value.AuthMode, Username: value.Username}
	if value.CredentialKind == nil {
		if value.AuthMode == transport.AuthNone {
			return auth, nil
		}
		return transport.AuthConfig{}, ErrCredentialAbsent
	}
	secret, err := m.credentials.Get(value.ID, *value.CredentialKind)
	if err != nil {
		return transport.AuthConfig{}, fmt.Errorf("%w: %v", ErrCredentialAbsent, err)
	}
	defer zero(secret)
	switch value.AuthMode {
	case transport.AuthBasic:
		auth.Password = string(secret)
	case transport.AuthBearer:
		auth.Token = string(secret)
	default:
		return transport.AuthConfig{}, ErrCredentialAbsent
	}
	return auth, nil
}

func (m *Manager) queryService(sessionID string) (*query.Service, error) {
	m.mu.RLock()
	service := m.sessions[sessionID]
	m.mu.RUnlock()
	if service == nil {
		return nil, errors.New("QUERY_NOT_FOUND")
	}
	return service, nil
}

func probe(parent context.Context, dispatcher *transport.Dispatcher, now func() time.Time) (ProbeResult, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	started := now()
	ping, err := dispatcher.Dispatch(ctx, transport.PingRequest{})
	if err != nil {
		return ProbeResult{}, ErrProbeFailed
	}
	version := headerValue(ping.Header, "X-Influxdb-Version")
	_ = ping.Close()
	if ping.StatusCode < 200 || ping.StatusCode >= 300 {
		return ProbeResult{}, ErrProbeFailed
	}

	show, err := dispatcher.Dispatch(ctx, transport.ShowDatabasesRequest{})
	if err != nil {
		return ProbeResult{}, ErrProbeFailed
	}
	defer show.Close()
	if show.StatusCode < 200 || show.StatusCode >= 300 || show.StatusCode == 204 {
		return ProbeResult{}, ErrProbeFailed
	}
	limited := io.LimitReader(show.Body, (1<<20)+1)
	decoder := json.NewDecoder(limited)
	decoder.UseNumber()
	var envelope struct {
		Error   string `json:"error"`
		Results []struct {
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := decoder.Decode(&envelope); err != nil || envelope.Error != "" || len(envelope.Results) == 0 {
		return ProbeResult{}, ErrProbeFailed
	}
	for _, result := range envelope.Results {
		if result.Error != "" {
			return ProbeResult{}, ErrProbeFailed
		}
	}
	return ProbeResult{OK: true, LatencyMS: now().Sub(started).Milliseconds(), Version: version}, nil
}

func headerValue(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) != 0 {
			return values[0]
		}
	}
	return ""
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
