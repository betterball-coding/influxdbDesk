package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"os"
	"path"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	influxql "github.com/influxdata/influxql"
	"github.com/influxdesk/influxdesk/internal/connection"
	"github.com/influxdesk/influxdesk/internal/credential"
	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/exportservice"
	"github.com/influxdesk/influxdesk/internal/importworker"
	"github.com/influxdesk/influxdesk/internal/localapp"
	"github.com/influxdesk/influxdesk/internal/operation"
	"github.com/influxdesk/influxdesk/internal/profile"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

var ErrApplicationNotReady = errors.New("APPLICATION_NOT_READY")

type App struct {
	ctx context.Context

	mu               sync.RWMutex
	ready            bool
	startupCode      string
	store            *store.Store
	profiles         *profile.Service
	connections      *connection.Manager
	tasks            *tasks.Repository
	transfers        *transfer.Repository
	preflight        *transfer.ImportPreflightService
	exports          *exportjob.Repository
	exportRun        *exportservice.Service
	eventCancel      context.CancelFunc
	schemaQuerySlots chan struct{}
}

type RuntimeStatus struct {
	Ready       bool   `json:"ready"`
	StartupCode string `json:"startupCode,omitempty"`
}

type RuntimePlatform struct {
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	PrimaryModifier string `json:"primaryModifier"`
}

type SaveProfileInput struct {
	ID                string              `json:"id,omitempty"`
	ExpectedRevision  string              `json:"expectedRevision,omitempty"`
	Name              string              `json:"name"`
	BaseURL           string              `json:"baseUrl"`
	DefaultDatabase   string              `json:"defaultDatabase,omitempty"`
	Environment       profile.Environment `json:"environment"`
	AuthMode          transport.AuthMode  `json:"authMode"`
	Username          string              `json:"username,omitempty"`
	AllowInsecureAuth bool                `json:"allowInsecureAuth"`
	ProtectionMode    protection.Mode     `json:"protectionMode"`
	Secret            *string             `json:"secret,omitempty"`
}

type ProfileView struct {
	ID                string              `json:"id"`
	Revision          string              `json:"revision"`
	Name              string              `json:"name"`
	BaseURL           string              `json:"baseUrl"`
	DefaultDatabase   string              `json:"defaultDatabase"`
	Environment       profile.Environment `json:"environment"`
	AuthMode          transport.AuthMode  `json:"authMode"`
	Username          string              `json:"username,omitempty"`
	AllowInsecureAuth bool                `json:"allowInsecureAuth"`
	ProtectionMode    protection.Mode     `json:"protectionMode"`
	CreatedAt         string              `json:"createdAt"`
	UpdatedAt         string              `json:"updatedAt"`
}

type TestConnectionInput struct {
	BaseURL           string             `json:"baseUrl"`
	AuthMode          transport.AuthMode `json:"authMode"`
	Username          string             `json:"username,omitempty"`
	Secret            string             `json:"secret,omitempty"`
	AllowInsecureAuth bool               `json:"allowInsecureAuth"`
}

type StartReadQueryInput struct {
	ClientRequestID string `json:"clientRequestId"`
	ProfileID       string `json:"profileId"`
	Database        string `json:"database"`
	RetentionPolicy string `json:"retentionPolicy,omitempty"`
	Query           string `json:"query"`
}

type StartExportInput struct {
	ClientRequestID string   `json:"clientRequestId"`
	ProfileID       string   `json:"profileId"`
	TargetDirectory string   `json:"targetDirectory"`
	Database        string   `json:"database"`
	RetentionPolicy string   `json:"retentionPolicy,omitempty"`
	Measurements    []string `json:"measurements"`
	StartNS         string   `json:"startNs"`
	EndNS           string   `json:"endNs"`
	Strict          bool     `json:"strict"`
}

type ImportSourceInspection struct {
	SourcePath  string `json:"sourcePath"`
	DisplayName string `json:"displayName"`
	SHA256      string `json:"sha256"`
	SizeBytes   string `json:"sizeBytes"`
}

// PreflightImportInput is the public Wails request. Connection-owned identity
// and revision fields are derived from the active backend snapshot below.
type PreflightImportInput struct {
	ClientRequestID     string                             `json:"clientRequestId"`
	ProfileID           string                             `json:"profileId"`
	SourcePath          string                             `json:"sourcePath"`
	Source              transfer.ImportSourceIdentity      `json:"source"`
	Format              transfer.ImportStageFormat         `json:"format"`
	Target              transfer.ImportTarget              `json:"target"`
	CSVMapping          *transfer.CSVMapping               `json:"csvMapping,omitempty"`
	NumericTextMappings []transfer.NumericTextFieldMapping `json:"numericTextMappings,omitempty"`
}

type SchemaDatabase struct {
	Name              string              `json:"name"`
	RetentionPolicies []string            `json:"retentionPolicies"`
	Measurements      []SchemaMeasurement `json:"measurements"`
}

type SchemaMeasurement struct {
	Name   string        `json:"name"`
	Fields []SchemaField `json:"fields"`
	Tags   []string      `json:"tags"`
}

type SchemaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryScalarView is the Wails wire contract for a query cell. Value is only
// populated with a string or boolean; numeric values always use DecimalText.
type QueryScalarView struct {
	Kind        query.ScalarKind `json:"kind"`
	DecimalText string           `json:"decimalText,omitempty"`
	Value       any              `json:"value,omitempty"`
}

type QueryResultPageView struct {
	Rows       [][]QueryScalarView `json:"rows"`
	NextCursor string              `json:"nextCursor,omitempty"`
	EOF        bool                `json:"eof"`
}

type ExportQueryResultInput struct {
	SessionID     string   `json:"sessionId"`
	StatementID   int      `json:"statementId"`
	SeriesID      string   `json:"seriesId"`
	AllRows       bool     `json:"allRows"`
	RowIndexes    []string `json:"rowIndexes,omitempty"`
	SuggestedName string   `json:"suggestedName,omitempty"`
}

type QueryResultExportView struct {
	Path     string `json:"path"`
	RowCount string `json:"rowCount"`
}

func NewApp() *App {
	return &App{schemaQuerySlots: make(chan struct{}, 2)}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	root, err := localapp.PreparePrivateRoot()
	if err == nil {
		err = a.initialize(ctx, root)
	}
	if err != nil {
		a.mu.Lock()
		a.startupCode = "LOCAL_SECURITY_OR_RECOVERY_FAILED"
		a.ready = false
		a.mu.Unlock()
		return
	}
	a.startTaskEventPump(ctx)
}

func (a *App) initialize(ctx context.Context, root string) error {
	privateDirectories := make(map[string]string)
	for _, directory := range []string{"spill", "staging", "transfer", "logs", "updates"} {
		path, err := localapp.PreparePrivateSubdir(root, directory)
		if err != nil {
			return err
		}
		privateDirectories[directory] = path
	}
	if err := removeOrphanQuerySpill(privateDirectories["spill"]); err != nil {
		return err
	}
	database, err := store.Open(ctx, filepath.Join(root, "influxdesk.db"))
	if err != nil {
		return err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = database.Close()
		}
	}()

	taskRepository := tasks.NewRepository(database, time.Now)
	if _, err := taskRepository.RecoverCore(ctx); err != nil {
		return err
	}
	exportRepository := exportjob.NewRepository(database, taskRepository, time.Now)
	if _, err := exportRepository.Recover(ctx); err != nil {
		return err
	}
	if _, err := query.SweepRetention(ctx, database, taskRepository, time.Now().UTC()); err != nil {
		return err
	}
	protectionManager := protection.NewManager(database, time.Now, nil)
	if err := protectionManager.Recover(ctx); err != nil {
		return err
	}
	if err := closeRecoveredGenerations(ctx, database, protectionManager); err != nil {
		return err
	}
	if _, err := protectionManager.PurgeClosed(ctx); err != nil {
		return err
	}
	transferRepository := transfer.NewRepository(database, taskRepository, nil, time.Now)
	preflightService := transferRepository.NewImportPreflightService(
		privateDirectories["staging"], transfer.SystemStagingVolumeStat,
	)
	cleanupImportFiles := preflightService.CleanupFileFunc()
	if _, err := transferRepository.RecoverImportCleanups(ctx, cleanupImportFiles); err != nil {
		return err
	}
	if _, err := transferRepository.RecoverImports(ctx); err != nil {
		return err
	}

	profileRepository := profile.NewRepository(database, time.Now)
	credentialStore := credential.NewStore()
	profileService := profile.NewService(profileRepository, credentialStore)
	connectionManager := connection.NewManagerWithQueryOptions(profileRepository, credentialStore, protectionManager, time.Now,
		query.ServiceOptions{SpillDirectory: privateDirectories["spill"]})
	connectionManager.EnableOperations(database, taskRepository)
	connectionManager.EnableTransfers(transferRepository, privateDirectories["staging"])
	connectionManager.EnableImportCleanup(cleanupImportFiles)
	exportRuntime, err := exportservice.New(exportRepository, connectionManager)
	if err != nil {
		return err
	}
	if err := exportRuntime.RestorePaused(ctx); err != nil {
		exportRuntime.Close()
		return err
	}

	a.mu.Lock()
	a.store = database
	a.profiles = profileService
	a.connections = connectionManager
	a.tasks = taskRepository
	a.transfers = transferRepository
	a.preflight = preflightService
	a.exports = exportRepository
	a.exportRun = exportRuntime
	a.startupCode = ""
	a.ready = true
	a.mu.Unlock()
	closeOnError = false
	return nil
}

func removeOrphanQuerySpill(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".query-") || !strings.HasSuffix(name, ".spill") {
			continue
		}
		if entry.IsDir() {
			return errors.New("query spill reconciliation found an unexpected directory")
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			return err
		}
	}
	return nil
}

func closeRecoveredGenerations(ctx context.Context, database *store.Store, manager *protection.Manager) error {
	rows, err := database.DB().QueryContext(ctx, `SELECT connection_id,connection_generation
		FROM protection_snapshots WHERE closed_at IS NULL`)
	if err != nil {
		return err
	}
	type generation struct{ id, value string }
	var generations []generation
	for rows.Next() {
		var item generation
		if err := rows.Scan(&item.id, &item.value); err != nil {
			rows.Close()
			return err
		}
		generations = append(generations, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range generations {
		if err := manager.CloseGeneration(ctx, item.id, item.value); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) shutdown(ctx context.Context) {
	a.mu.Lock()
	a.ready = false
	connections := a.connections
	exportRuntime := a.exportRun
	database := a.store
	eventCancel := a.eventCancel
	a.mu.Unlock()
	if eventCancel != nil {
		eventCancel()
	}
	if exportRuntime != nil {
		exportRuntime.Close()
	}
	if connections != nil {
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = connections.CloseAll(closeCtx)
		cancel()
	}
	if database != nil {
		_ = database.Close()
	}
}

func (a *App) startTaskEventPump(ctx context.Context) {
	a.mu.Lock()
	if !a.ready || a.tasks == nil || a.eventCancel != nil {
		a.mu.Unlock()
		return
	}
	eventCtx, cancel := context.WithCancel(ctx)
	a.eventCancel = cancel
	repository := a.tasks
	a.mu.Unlock()
	head, err := repository.EventHead(eventCtx)
	if err != nil {
		return
	}
	go func(after string) {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-eventCtx.Done():
				return
			case <-ticker.C:
			}
			events, err := repository.Changes(eventCtx, after, 1000)
			if err != nil {
				continue
			}
			for _, event := range events {
				wailsruntime.EventsEmit(ctx, "task.changed.v1", event)
				after = event.Seq
			}
		}
	}(head)
}

func (a *App) GetRuntimeStatus() RuntimeStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return RuntimeStatus{Ready: a.ready, StartupCode: a.startupCode}
}

func (a *App) GetRuntimePlatform() RuntimePlatform {
	modifier := "Ctrl"
	if goruntime.GOOS == "darwin" {
		modifier = "Command"
	}
	return RuntimePlatform{OS: goruntime.GOOS, Arch: goruntime.GOARCH, PrimaryModifier: modifier}
}

func (a *App) ListProfiles() ([]ProfileView, error) {
	profiles, _, _, err := a.services()
	if err != nil {
		return nil, err
	}
	values, err := profiles.List(a.ctx)
	if err != nil {
		return nil, err
	}
	result := make([]ProfileView, len(values))
	for i, value := range values {
		result[i] = profileView(value)
	}
	return result, nil
}

func (a *App) GetProfile(id string) (ProfileView, error) {
	profiles, _, _, err := a.services()
	if err != nil {
		return ProfileView{}, err
	}
	value, err := profiles.Get(a.ctx, id)
	return profileView(value), err
}

func (a *App) SaveProfile(input SaveProfileInput) (ProfileView, error) {
	profiles, connections, _, err := a.services()
	if err != nil {
		return ProfileView{}, err
	}
	if input.ID != "" && connections.IsOpen(input.ID) {
		return ProfileView{}, profile.ErrProfileInUse
	}
	value, err := profiles.Save(a.ctx, profile.SaveCommand{Profile: profile.SaveRequest{
		ID: input.ID, ExpectedRevision: input.ExpectedRevision, Name: input.Name,
		BaseURL: input.BaseURL, DefaultDatabase: input.DefaultDatabase,
		Environment: input.Environment, AuthMode: input.AuthMode,
		Username: input.Username, AllowInsecureAuth: input.AllowInsecureAuth,
		ProtectionMode: input.ProtectionMode,
	}, Secret: input.Secret})
	return profileView(value), err
}

func (a *App) DeleteProfile(id, expectedRevision string) error {
	profiles, connections, _, err := a.services()
	if err != nil {
		return err
	}
	if connections.IsOpen(id) {
		return profile.ErrProfileInUse
	}
	return profiles.Delete(a.ctx, id, expectedRevision)
}

func (a *App) TestConnection(input TestConnectionInput) (connection.ProbeResult, error) {
	if _, _, _, err := a.services(); err != nil {
		return connection.ProbeResult{}, err
	}
	auth := transport.AuthConfig{Mode: input.AuthMode, Username: input.Username}
	switch input.AuthMode {
	case transport.AuthBasic:
		auth.Password = input.Secret
	case transport.AuthBearer:
		auth.Token = input.Secret
	}
	return connection.TestConfig(a.ctx, transport.Config{
		BaseURL: input.BaseURL, Auth: auth, AllowInsecureAuth: input.AllowInsecureAuth,
	}, time.Now)
}

func (a *App) TestSavedConnection(profileID string) (connection.ProbeResult, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return connection.ProbeResult{}, err
	}
	return connections.TestSaved(a.ctx, profileID)
}

func (a *App) OpenConnection(profileID string) (connection.Snapshot, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return connection.Snapshot{}, err
	}
	return connections.Open(a.ctx, profileID)
}

func (a *App) GetConnectionState(profileID string) (connection.Snapshot, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return connection.Snapshot{}, err
	}
	return connections.Get(profileID)
}

func (a *App) CloseConnection(profileID string) error {
	_, connections, _, err := a.services()
	if err != nil {
		return err
	}
	snapshot, err := connections.Get(profileID)
	if err != nil {
		return err
	}
	a.mu.RLock()
	exportRuntime := a.exportRun
	a.mu.RUnlock()
	if exportRuntime != nil {
		pauseCtx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
		err = exportRuntime.PauseGeneration(pauseCtx, exportlane.GenerationKey{
			ConnectionID: snapshot.ConnectionID, Generation: snapshot.ConnectionGeneration,
		})
		cancel()
		if err != nil {
			return err
		}
	}
	return connections.Close(a.ctx, profileID)
}

func (a *App) GetProtectionState(profileID string) (protection.Snapshot, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return protection.Snapshot{}, err
	}
	return connections.GetProtection(profileID)
}

func (a *App) UnlockProtection(input protection.UnlockRequest) (protection.CommandResult, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return protection.CommandResult{}, err
	}
	return connections.Unlock(a.ctx, input)
}

func (a *App) LockProtection(input protection.LockRequest) (protection.CommandResult, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return protection.CommandResult{}, err
	}
	return connections.Lock(a.ctx, input)
}

func (a *App) StartReadQuery(input StartReadQueryInput) (query.QuerySession, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return query.QuerySession{}, err
	}
	return connections.StartReadQuery(a.ctx, input.ProfileID, query.StartRequest{
		Database: input.Database, RetentionPolicy: input.RetentionPolicy,
		Query: input.Query, ClientRequestID: input.ClientRequestID,
	})
}

func (a *App) GetQuerySession(sessionID string) (query.QuerySession, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return query.QuerySession{}, err
	}
	return connections.GetQuerySession(a.ctx, sessionID)
}

func (a *App) ListQuerySessions() ([]query.QuerySession, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return nil, err
	}
	return connections.ListQuerySessions(a.ctx)
}

func (a *App) CancelQuery(sessionID string, input query.CommandEnvelope) (query.QuerySession, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return query.QuerySession{}, err
	}
	return connections.CancelQuery(a.ctx, sessionID, input)
}

func (a *App) CloseQuery(sessionID string, input query.CommandEnvelope) (query.QuerySession, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return query.QuerySession{}, err
	}
	return connections.CloseQuery(a.ctx, sessionID, input)
}

func (a *App) ListResultSeries(sessionID string, statementID int) ([]query.SeriesSummary, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return nil, err
	}
	return connections.ListResultSeries(sessionID, statementID)
}

func (a *App) GetResultPage(input query.PageRequest) (QueryResultPageView, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return QueryResultPageView{}, err
	}
	page, err := connections.GetResultPage(input)
	if err != nil {
		return QueryResultPageView{}, err
	}
	return queryResultPageView(page)
}

func (a *App) ExportQueryResultCSV(input ExportQueryResultInput) (QueryResultExportView, error) {
	a.mu.RLock()
	ready, ctx, connections := a.ready, a.ctx, a.connections
	a.mu.RUnlock()
	if !ready || ctx == nil || connections == nil {
		return QueryResultExportView{}, ErrApplicationNotReady
	}
	request, err := queryResultCSVRequest(input)
	if err != nil {
		return QueryResultExportView{}, err
	}
	path, err := wailsruntime.SaveFileDialog(ctx, wailsruntime.SaveDialogOptions{
		Title:                "导出查询结果",
		DefaultFilename:      queryResultCSVFilename(input.SuggestedName),
		Filters:              []wailsruntime.FileFilter{{DisplayName: "CSV 文件", Pattern: "*.csv"}},
		CanCreateDirectories: true,
	})
	if err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_TARGET_FAILED")
	}
	if path == "" {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_CANCELED")
	}
	if filepath.Ext(path) == "" {
		path += ".csv"
	}
	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil || absPath != filepath.Clean(absPath) {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_TARGET_INVALID")
	}
	directory := filepath.Dir(absPath)
	info, err := os.Lstat(directory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_TARGET_INVALID")
	}

	part, err := os.CreateTemp(directory, "."+filepath.Base(absPath)+"-*.part")
	if err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_WRITE_FAILED")
	}
	partPath := part.Name()
	committed := false
	defer func() {
		_ = part.Close()
		if !committed {
			_ = os.Remove(partPath)
		}
	}()
	if err := part.Chmod(0o600); err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_WRITE_FAILED")
	}
	rowCount, err := connections.WriteResultCSV(ctx, request, part, time.Local)
	if err != nil {
		return QueryResultExportView{}, err
	}
	if err := part.Sync(); err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_WRITE_FAILED")
	}
	if err := part.Close(); err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_WRITE_FAILED")
	}
	if target, statErr := os.Lstat(absPath); statErr == nil {
		if target.Mode()&os.ModeSymlink != 0 || !target.Mode().IsRegular() {
			return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_TARGET_INVALID")
		}
	} else if !os.IsNotExist(statErr) {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_TARGET_INVALID")
	}
	if err := transfer.CommitSameVolumeReplace(partPath, absPath); err != nil {
		return QueryResultExportView{}, errors.New("QUERY_RESULT_EXPORT_WRITE_FAILED")
	}
	committed = true
	return QueryResultExportView{Path: absPath, RowCount: strconv.FormatUint(rowCount, 10)}, nil
}

func queryResultCSVRequest(input ExportQueryResultInput) (query.CSVExportRequest, error) {
	if input.SessionID == "" || input.SeriesID == "" || input.StatementID < 0 ||
		input.AllRows && len(input.RowIndexes) > 0 || !input.AllRows && len(input.RowIndexes) == 0 {
		return query.CSVExportRequest{}, errors.New("INVALID_QUERY_RESULT_EXPORT")
	}
	rowIndexes := make([]uint64, len(input.RowIndexes))
	for index, raw := range input.RowIndexes {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return query.CSVExportRequest{}, errors.New("INVALID_QUERY_RESULT_EXPORT")
		}
		rowIndexes[index] = value
	}
	return query.CSVExportRequest{
		SessionID: input.SessionID, StatementID: input.StatementID, SeriesID: input.SeriesID,
		AllRows: input.AllRows, RowIndexes: rowIndexes,
	}, nil
}

func queryResultCSVFilename(suggested string) string {
	return queryResultCSVFilenameForPlatform(suggested, goruntime.GOOS)
}

func queryResultCSVFilenameForPlatform(suggested, goos string) string {
	name := strings.TrimSpace(path.Base(suggested))
	if name == "" || name == "." {
		name = "query-result"
	}
	name = strings.Map(func(value rune) rune {
		invalid := value < 32 || value == '/'
		if goos == "windows" {
			invalid = invalid || strings.ContainsRune(`<>:"\\|?*`, value)
		} else if goos == "darwin" {
			invalid = invalid || value == ':'
		}
		if invalid {
			return '_'
		}
		return value
	}, name)
	name = strings.TrimSpace(name)
	if name == "" {
		name = "query-result"
	}
	if !strings.EqualFold(filepath.Ext(name), ".csv") {
		name += ".csv"
	}
	return name
}

func (a *App) GetTransientIssue(sessionID string) (string, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return "", err
	}
	return connections.GetTransientIssue(sessionID)
}

func (a *App) PreviewMutation(profileID string, input operation.PreviewRequest) (operation.Preview, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return operation.Preview{}, err
	}
	return connections.PreviewMutation(a.ctx, profileID, input)
}

func (a *App) ExecuteMutation(profileID string, input operation.ExecuteRequest) (operation.Operation, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return operation.Operation{}, err
	}
	return connections.ExecuteMutation(a.ctx, profileID, input)
}

func (a *App) GetOperation(operationID string) (operation.Operation, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return operation.Operation{}, err
	}
	return connections.GetOperation(a.ctx, operationID)
}

func (a *App) ListOperations(limit int) ([]operation.Operation, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return nil, err
	}
	return connections.ListOperations(a.ctx, limit)
}

func (a *App) CancelOperation(operationID string, input operation.CommandEnvelope) (operation.Operation, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return operation.Operation{}, err
	}
	return connections.CancelOperation(a.ctx, operationID, input)
}

func (a *App) GetSchemaSnapshot(profileID string) ([]SchemaDatabase, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return nil, err
	}
	expected, err := connections.Get(profileID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.ctx, 3*time.Minute)
	defer cancel()
	release, err := a.acquireSchemaQuerySlot(ctx)
	if err != nil {
		return nil, err
	}
	names, err := schemaFirstColumn(ctx, connections, profileID, "", "SHOW DATABASES")
	release()
	if err != nil {
		return nil, err
	}
	result := make([]SchemaDatabase, len(names))
	loadContext, stopLoading := context.WithCancel(ctx)
	defer stopLoading()
	var loadGroup sync.WaitGroup
	var loadError error
	var loadErrorOnce sync.Once
	for index, name := range names {
		loadGroup.Add(1)
		go func(index int, name string) {
			defer loadGroup.Done()
			release, err := a.acquireSchemaQuerySlot(loadContext)
			if err != nil {
				loadErrorOnce.Do(func() {
					loadError = err
					stopLoading()
				})
				return
			}
			defer release()
			columns, err := schemaFirstColumns(loadContext, connections, profileID, name,
				"SHOW RETENTION POLICIES; SHOW MEASUREMENTS", 2)
			if err != nil {
				loadErrorOnce.Do(func() {
					loadError = err
					stopLoading()
				})
				return
			}
			measurements := make([]SchemaMeasurement, len(columns[1]))
			for measurementIndex, measurementName := range columns[1] {
				measurements[measurementIndex] = SchemaMeasurement{
					Name: measurementName, Fields: []SchemaField{}, Tags: []string{},
				}
			}
			result[index] = SchemaDatabase{
				Name: name, RetentionPolicies: columns[0], Measurements: measurements,
			}
		}(index, name)
	}
	loadGroup.Wait()
	if loadError != nil {
		return nil, loadError
	}
	if err := verifySchemaConnection(connections, profileID, expected); err != nil {
		return nil, err
	}
	return result, nil
}

// GetMeasurementSchema lazily loads only field and tag keys for one
// measurement. Callers provide identifiers, never executable InfluxQL.
func (a *App) GetMeasurementSchema(
	profileID, database, measurement string,
) (SchemaMeasurement, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return SchemaMeasurement{}, err
	}
	expected, err := connections.Get(profileID)
	if err != nil {
		return SchemaMeasurement{}, err
	}
	fieldQuery, tagQuery, err := measurementSchemaQueries(database, measurement)
	if err != nil {
		return SchemaMeasurement{}, err
	}
	ctx, cancel := context.WithTimeout(a.ctx, 3*time.Minute)
	defer cancel()
	release, err := a.acquireSchemaQuerySlot(ctx)
	if err != nil {
		return SchemaMeasurement{}, err
	}
	defer release()

	fieldTypes := make(map[string]string)
	tagSet := make(map[string]struct{})
	err = visitSchemaStatementRows(ctx, connections, profileID, database,
		fieldQuery+"; "+tagQuery, 2,
		func(statementID int, series query.SeriesSummary, row []query.TypedScalar) error {
			if statementID == 1 {
				name, err := schemaStringColumn(series, row, "tagKey")
				if err != nil {
					return err
				}
				tagSet[name] = struct{}{}
				return nil
			}
			name, err := schemaStringColumn(series, row, "fieldKey")
			if err != nil {
				return err
			}
			fieldType, err := schemaStringColumn(series, row, "fieldType")
			if err != nil {
				return err
			}
			if previous, exists := fieldTypes[name]; exists && previous != fieldType {
				return errors.New("SCHEMA_FIELD_TYPE_CONFLICT")
			}
			fieldTypes[name] = fieldType
			return nil
		})
	if err != nil {
		return SchemaMeasurement{}, err
	}
	if err := verifySchemaConnection(connections, profileID, expected); err != nil {
		return SchemaMeasurement{}, err
	}

	fields := make([]SchemaField, 0, len(fieldTypes))
	for name, fieldType := range fieldTypes {
		fields = append(fields, SchemaField{Name: name, Type: fieldType})
	}
	sort.Slice(fields, func(left, right int) bool {
		if fields[left].Name == fields[right].Name {
			return fields[left].Type < fields[right].Type
		}
		return fields[left].Name < fields[right].Name
	})
	tags := make([]string, 0, len(tagSet))
	for name := range tagSet {
		tags = append(tags, name)
	}
	sort.Strings(tags)
	return SchemaMeasurement{Name: measurement, Fields: fields, Tags: tags}, nil
}

func (a *App) acquireSchemaQuerySlot(ctx context.Context) (func(), error) {
	select {
	case a.schemaQuerySlots <- struct{}{}:
		return func() { <-a.schemaQuerySlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func verifySchemaConnection(
	connections *connection.Manager,
	profileID string,
	expected connection.Snapshot,
) error {
	current, err := connections.Get(profileID)
	if err != nil {
		return errors.New("SCHEMA_CONNECTION_CHANGED")
	}
	if current.ConnectionID != expected.ConnectionID ||
		current.ConnectionGeneration != expected.ConnectionGeneration ||
		current.ProfileRevision != expected.ProfileRevision {
		return errors.New("SCHEMA_CONNECTION_CHANGED")
	}
	return nil
}

func measurementSchemaQueries(database, measurement string) (string, string, error) {
	if !validSchemaIdentifier(database) || !validSchemaIdentifier(measurement) {
		return "", "", errors.New("INVALID_SCHEMA_IDENTIFIER")
	}
	sources := influxql.Sources{&influxql.Measurement{Name: measurement}}
	fieldQuery := (&influxql.ShowFieldKeysStatement{
		Database: database, Sources: sources,
	}).String()
	tagQuery := (&influxql.ShowTagKeysStatement{
		Database: database, Sources: sources,
	}).String()
	return fieldQuery, tagQuery, nil
}

func validSchemaIdentifier(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\r\n\x00")
}

func schemaStringColumn(
	series query.SeriesSummary,
	row []query.TypedScalar,
	column string,
) (string, error) {
	index := -1
	for candidate, name := range series.Columns {
		if name != column {
			continue
		}
		if index >= 0 {
			return "", errors.New("SCHEMA_QUERY_PROTOCOL_ERROR")
		}
		index = candidate
	}
	if index < 0 || index >= len(row) || row[index].Kind != query.ScalarString ||
		row[index].StringValue == "" {
		return "", errors.New("SCHEMA_QUERY_PROTOCOL_ERROR")
	}
	return row[index].StringValue, nil
}

func schemaFirstColumn(
	ctx context.Context,
	connections *connection.Manager,
	profileID string,
	database string,
	influxQL string,
) ([]string, error) {
	columns, err := schemaFirstColumns(ctx, connections, profileID, database, influxQL, 1)
	if err != nil {
		return nil, err
	}
	return columns[0], nil
}

func schemaFirstColumns(
	ctx context.Context,
	connections *connection.Manager,
	profileID string,
	database string,
	influxQL string,
	statementCount int,
) ([][]string, error) {
	if statementCount <= 0 {
		return nil, errors.New("SCHEMA_QUERY_PROTOCOL_ERROR")
	}
	seen := make([]map[string]struct{}, statementCount)
	for index := range seen {
		seen[index] = make(map[string]struct{})
	}
	err := visitSchemaStatementRows(ctx, connections, profileID, database, influxQL, statementCount,
		func(statementID int, _ query.SeriesSummary, row []query.TypedScalar) error {
			if len(row) == 0 || row[0].Kind != query.ScalarString || row[0].StringValue == "" {
				return nil
			}
			seen[statementID][row[0].StringValue] = struct{}{}
			return nil
		})
	if err != nil {
		return nil, err
	}
	columns := make([][]string, statementCount)
	for statementID := range seen {
		columns[statementID] = make([]string, 0, len(seen[statementID]))
		for name := range seen[statementID] {
			columns[statementID] = append(columns[statementID], name)
		}
		sort.Strings(columns[statementID])
	}
	return columns, nil
}

func visitSchemaStatementRows(
	ctx context.Context,
	connections *connection.Manager,
	profileID string,
	database string,
	influxQL string,
	statementCount int,
	visit func(int, query.SeriesSummary, []query.TypedScalar) error,
) error {
	if statementCount <= 0 {
		return errors.New("SCHEMA_QUERY_PROTOCOL_ERROR")
	}
	session, err := connections.StartReadQuery(ctx, profileID, query.StartRequest{
		Database: database, Query: influxQL, ClientRequestID: uuid.NewString(),
	})
	if err != nil {
		return err
	}
	defer func() {
		_, _ = connections.CloseQuery(context.Background(), session.ID, query.CommandEnvelope{
			CommandRequestID: uuid.NewString(), ExpectedStateRevision: session.StateRevision,
		})
	}()
	for !session.Terminal {
		select {
		case <-ctx.Done():
			_, _ = connections.CancelQuery(context.Background(), session.ID, query.CommandEnvelope{
				CommandRequestID: uuid.NewString(), ExpectedStateRevision: session.StateRevision,
			})
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		session, err = connections.GetQuerySession(ctx, session.ID)
		if err != nil {
			return err
		}
	}
	if session.State != query.SessionSucceeded {
		if session.State == query.SessionTruncated {
			return errors.New("SCHEMA_QUERY_TRUNCATED")
		}
		return errors.New("SCHEMA_QUERY_FAILED")
	}
	for statementID := 0; statementID < statementCount; statementID++ {
		series, err := connections.ListResultSeries(session.ID, statementID)
		if err != nil {
			return err
		}
		for _, item := range series {
			cursor := ""
			for {
				page, err := connections.GetResultPage(query.PageRequest{
					SessionID: session.ID, StatementID: statementID, SeriesID: item.ID,
					Cursor: cursor, Limit: 5000,
				})
				if err != nil {
					return err
				}
				for _, row := range page.Rows {
					if err := visit(statementID, item, row); err != nil {
						return err
					}
				}
				if page.EOF {
					break
				}
				if page.NextCursor == "" || page.NextCursor == cursor {
					return errors.New("SCHEMA_CURSOR_STALLED")
				}
				cursor = page.NextCursor
			}
		}
	}
	return nil
}

func (a *App) GetTaskEventHead() (string, error) {
	_, _, taskRepository, err := a.services()
	if err != nil {
		return "", err
	}
	return taskRepository.EventHead(a.ctx)
}

func (a *App) GetTaskChanges(after string, limit int) ([]tasks.Event, error) {
	_, _, taskRepository, err := a.services()
	if err != nil {
		return nil, err
	}
	return taskRepository.Changes(a.ctx, after, limit)
}

func (a *App) GetImportJob(jobID string) (transfer.ImportJob, error) {
	a.mu.RLock()
	ready, repository := a.ready, a.transfers
	a.mu.RUnlock()
	if !ready || repository == nil {
		return transfer.ImportJob{}, ErrApplicationNotReady
	}
	return repository.GetImport(a.ctx, jobID)
}

func (a *App) ListRecoverableImports(profileID string, limit int) ([]transfer.ImportJob, error) {
	a.mu.RLock()
	ready, repository := a.ready, a.transfers
	a.mu.RUnlock()
	if !ready || repository == nil {
		return nil, ErrApplicationNotReady
	}
	return repository.ListRecoverableImports(a.ctx, profileID, limit)
}

func (a *App) SelectImportSource() (ImportSourceInspection, error) {
	a.mu.RLock()
	ready, ctx := a.ready, a.ctx
	a.mu.RUnlock()
	if !ready || ctx == nil {
		return ImportSourceInspection{}, ErrApplicationNotReady
	}
	path, err := wailsruntime.OpenFileDialog(ctx, wailsruntime.OpenDialogOptions{
		Title: "选择导入源文件",
		Filters: []wailsruntime.FileFilter{
			{DisplayName: "InfluxDesk 导入文件", Pattern: "*.lp;*.txt;*.gz;*.jsonl;*.csv"},
			{DisplayName: "所有文件", Pattern: "*.*"},
		},
		ResolvesAliases: true,
	})
	if err != nil {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_SELECTION_FAILED")
	}
	if path == "" {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_SELECTION_CANCELED")
	}
	return inspectImportSource(ctx, path)
}

func (a *App) InspectImportSource(path string) (ImportSourceInspection, error) {
	a.mu.RLock()
	ready, ctx := a.ready, a.ctx
	a.mu.RUnlock()
	if !ready || ctx == nil {
		return ImportSourceInspection{}, ErrApplicationNotReady
	}
	return inspectImportSource(ctx, path)
}

func (a *App) SelectExportDirectory() (string, error) {
	a.mu.RLock()
	ready, ctx := a.ready, a.ctx
	a.mu.RUnlock()
	if !ready || ctx == nil {
		return "", ErrApplicationNotReady
	}
	path, err := wailsruntime.OpenDirectoryDialog(ctx, wailsruntime.OpenDialogOptions{
		Title: "选择逻辑导出目录", CanCreateDirectories: true, ResolvesAliases: true,
	})
	if err != nil {
		return "", errors.New("EXPORT_TARGET_SELECTION_FAILED")
	}
	if path == "" {
		return "", errors.New("EXPORT_TARGET_SELECTION_CANCELED")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("INVALID_EXPORT_TARGET")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("INVALID_EXPORT_TARGET")
	}
	return filepath.Clean(abs), nil
}

func inspectImportSource(ctx context.Context, path string) (ImportSourceInspection, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_PATH_INVALID")
	}
	file, err := os.Open(path)
	if err != nil {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_UNAVAILABLE")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_UNAVAILABLE")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, &contextReader{ctx: ctx, reader: file}); err != nil {
		if ctx.Err() != nil {
			return ImportSourceInspection{}, ctx.Err()
		}
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_INSPECTION_FAILED")
	}
	after, err := file.Stat()
	if err != nil || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return ImportSourceInspection{}, errors.New("IMPORT_SOURCE_CHANGED")
	}
	return ImportSourceInspection{
		SourcePath: path, DisplayName: filepath.Base(path),
		SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: afterSizeText(after.Size()),
	}, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func afterSizeText(size int64) string {
	return new(big.Int).SetInt64(size).String()
}

func (a *App) PreflightImport(input PreflightImportInput) (transfer.PreflightImportResponse, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.PreflightImportResponse{}, err
	}
	snapshot, err := connections.Get(input.ProfileID)
	if err != nil {
		return transfer.PreflightImportResponse{}, err
	}
	request := transfer.PreflightImportRequest{
		ClientRequestID:      input.ClientRequestID,
		ProfileID:            snapshot.ProfileID,
		ProfileRevision:      snapshot.ProfileRevision,
		ConnectionID:         snapshot.ConnectionID,
		ConnectionGeneration: snapshot.ConnectionGeneration,
		SourcePath:           input.SourcePath,
		Source:               input.Source,
		Format:               input.Format,
		Target:               input.Target,
		CSVMapping:           input.CSVMapping,
		NumericTextMappings:  input.NumericTextMappings,
	}
	a.mu.RLock()
	preflight := a.preflight
	a.mu.RUnlock()
	if preflight == nil {
		return transfer.PreflightImportResponse{}, ErrApplicationNotReady
	}
	return preflight.PreflightImport(a.ctx, request)
}

func (a *App) PreviewImportRun(profileID string, input transfer.PreviewRunRequest) (transfer.ImportRunPreview, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportRunPreview{}, err
	}
	return connections.PreviewImportRun(a.ctx, profileID, input)
}

func (a *App) StartImport(profileID string, input transfer.AuthorizedStartRequest) (transfer.ImportJob, error) {
	if input.Action != transfer.GrantStart {
		return transfer.ImportJob{}, transfer.ErrGrantMismatch
	}
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportJob{}, err
	}
	job, _, err := connections.StartImport(a.ctx, profileID, input)
	return job, err
}

func (a *App) ResumeImport(profileID string, input transfer.AuthorizedStartRequest) (transfer.ImportJob, error) {
	if input.Action != transfer.GrantResume {
		return transfer.ImportJob{}, transfer.ErrGrantMismatch
	}
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportJob{}, err
	}
	job, _, err := connections.StartImport(a.ctx, profileID, input)
	return job, err
}

func (a *App) ResolveImportBatch(profileID string, input transfer.AuthorizedResolveRequest) (transfer.ImportJob, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportJob{}, err
	}
	job, _, err := connections.ResolveImportBatch(a.ctx, profileID, input)
	return job, err
}

func (a *App) RunImportNext(
	profileID, jobID, database, retentionPolicy string,
) (importworker.Result, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return importworker.Result{}, err
	}
	return connections.RunImportNext(a.ctx, profileID, jobID, database, retentionPolicy)
}

func (a *App) CancelImport(jobID string, input transfer.ImportCommandEnvelope) (transfer.ImportJob, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportJob{}, err
	}
	job, _, err := connections.CancelImport(a.ctx, jobID, input)
	return job, err
}

func (a *App) CleanupImport(jobID string, input transfer.ImportCommandEnvelope) (transfer.ImportJob, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return transfer.ImportJob{}, err
	}
	job, _, err := connections.CleanupImport(a.ctx, jobID, input)
	return job, err
}

func (a *App) StartExport(input StartExportInput) (exportjob.Job, error) {
	_, connections, _, err := a.services()
	if err != nil {
		return exportjob.Job{}, err
	}
	snapshot, err := connections.Get(input.ProfileID)
	if err != nil {
		return exportjob.Job{}, err
	}
	canonical, err := canonicalExportInput(input, snapshot)
	if err != nil {
		return exportjob.Job{}, err
	}
	a.mu.RLock()
	repository, exportRuntime := a.exports, a.exportRun
	a.mu.RUnlock()
	if repository == nil || exportRuntime == nil {
		return exportjob.Job{}, ErrApplicationNotReady
	}
	if replay, found, err := repository.ReplayStart(a.ctx, snapshot.ProfileID,
		snapshot.ConnectionID, input.ClientRequestID, canonical.requestDigest); err != nil || found {
		if err != nil || !found || replay.Task.State != exportjob.StateQueued {
			return replay, err
		}
		return exportRuntime.Schedule(a.ctx, replay)
	}
	if len(canonical.measurements) != 1 || canonical.retentionPolicy == "" || !canonical.strict {
		return exportjob.Job{}, errors.New("UNSUPPORTED_EXPORT_SPEC")
	}

	info, err := os.Lstat(canonical.targetDirectory)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return exportjob.Job{}, errors.New("INVALID_EXPORT_TARGET")
	}
	volume, err := transfer.SystemStagingVolumeStat(a.ctx, canonical.targetDirectory)
	if err != nil {
		return exportjob.Job{}, err
	}
	targetDigest, err := digestCanonical(struct {
		SchemaVersion int    `json:"schemaVersion"`
		Directory     string `json:"directory"`
		VolumeID      string `json:"volumeId"`
	}{1, canonical.targetDirectory, volume.ID})
	if err != nil {
		return exportjob.Job{}, err
	}
	jobID := uuid.NewString()
	outputDirectory := filepath.Join(canonical.targetDirectory, "influxdesk-export-"+jobID)
	job, replayed, err := repository.Start(a.ctx, exportjob.StartRequest{
		JobID: jobID, ProfileID: snapshot.ProfileID,
		ProfileRevision: snapshot.ProfileRevision, ConnectionID: snapshot.ConnectionID,
		ConnectionGeneration: snapshot.ConnectionGeneration, ClientRequestID: input.ClientRequestID,
		RequestDigest: canonical.requestDigest, SpecDigest: canonical.specDigest,
		TargetDigest: targetDigest, TargetVolumeID: volume.ID,
		TargetReservationBytes: transfer.ReservationExtentBytes,
		TargetVolumeFreeBytes:  volume.FreeBytes, TargetVolumeCapacity: volume.CapacityBytes,
		Plan: exportjob.PlanDetail{
			SchemaVersion: 1, Database: canonical.database,
			RetentionPolicy: canonical.retentionPolicy, Measurement: canonical.measurements[0],
			StartNS: canonical.startNS, EndNS: canonical.endNS,
			SliceWidthNS: "3600000000000", OutputDirectory: outputDirectory,
			TypePreserving: true, Lossy: false,
		},
	})
	if err != nil {
		return job, err
	}
	if replayed && job.Task.State != exportjob.StateQueued {
		return job, nil
	}
	return exportRuntime.Schedule(a.ctx, job)
}

func (a *App) GetExportJob(jobID string) (exportjob.Job, error) {
	a.mu.RLock()
	ready, repository := a.ready, a.exports
	a.mu.RUnlock()
	if !ready || repository == nil {
		return exportjob.Job{}, ErrApplicationNotReady
	}
	return repository.Get(a.ctx, jobID)
}

type canonicalExport struct {
	targetDirectory string
	database        string
	retentionPolicy string
	measurements    []string
	startNS         string
	endNS           string
	strict          bool
	specDigest      string
	requestDigest   string
}

func canonicalExportInput(input StartExportInput, snapshot connection.Snapshot) (canonicalExport, error) {
	if _, err := uuid.Parse(input.ClientRequestID); err != nil || input.ProfileID == "" ||
		!validExportText(input.Database) ||
		(input.RetentionPolicy != "" && !validExportText(input.RetentionPolicy)) || len(input.Measurements) == 0 {
		return canonicalExport{}, errors.New("INVALID_EXPORT_REQUEST")
	}
	start, err := canonicalSignedInt64(input.StartNS)
	if err != nil {
		return canonicalExport{}, err
	}
	end, err := canonicalSignedInt64(input.EndNS)
	if err != nil {
		return canonicalExport{}, err
	}
	startNumber, endNumber := new(big.Int), new(big.Int)
	startNumber.SetString(start, 10)
	endNumber.SetString(end, 10)
	if startNumber.Cmp(endNumber) >= 0 {
		return canonicalExport{}, errors.New("INVALID_EXPORT_RANGE")
	}
	measurements := append([]string(nil), input.Measurements...)
	sort.Strings(measurements)
	for index, measurement := range measurements {
		if !validExportText(measurement) || index > 0 && measurement == measurements[index-1] {
			return canonicalExport{}, errors.New("INVALID_EXPORT_REQUEST")
		}
	}
	if input.TargetDirectory == "" || !filepath.IsAbs(input.TargetDirectory) ||
		filepath.Clean(input.TargetDirectory) != input.TargetDirectory {
		return canonicalExport{}, errors.New("INVALID_EXPORT_TARGET")
	}
	specDigest, err := digestCanonical(struct {
		SchemaVersion   int      `json:"schemaVersion"`
		Database        string   `json:"database"`
		RetentionPolicy string   `json:"retentionPolicy"`
		Measurements    []string `json:"measurements"`
		StartNS         string   `json:"startNs"`
		EndNS           string   `json:"endNs"`
		Strict          bool     `json:"strict"`
	}{1, input.Database, input.RetentionPolicy, measurements, start, end, input.Strict})
	if err != nil {
		return canonicalExport{}, err
	}
	requestDigest, err := digestCanonical(struct {
		SchemaVersion        int    `json:"schemaVersion"`
		ClientRequestID      string `json:"clientRequestId"`
		ProfileID            string `json:"profileId"`
		ProfileRevision      string `json:"profileRevision"`
		ConnectionID         string `json:"connectionId"`
		ConnectionGeneration string `json:"connectionGeneration"`
		SpecDigest           string `json:"specDigest"`
		TargetDirectory      string `json:"targetDirectory"`
	}{1, input.ClientRequestID, snapshot.ProfileID, snapshot.ProfileRevision,
		snapshot.ConnectionID, snapshot.ConnectionGeneration, specDigest, input.TargetDirectory})
	if err != nil {
		return canonicalExport{}, err
	}
	return canonicalExport{
		targetDirectory: input.TargetDirectory, database: input.Database,
		retentionPolicy: input.RetentionPolicy, measurements: measurements,
		startNS: start, endNS: end, strict: input.Strict,
		specDigest: specDigest, requestDigest: requestDigest,
	}, nil
}

func canonicalSignedInt64(value string) (string, error) {
	if value == "" || strings.HasPrefix(value, "+") || value == "-0" ||
		(len(value) > 1 && value[0] == '0') || (len(value) > 2 && strings.HasPrefix(value, "-0")) {
		return "", errors.New("INVALID_EXACT_NUMBER")
	}
	number := new(big.Int)
	minimum := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 63), big.NewInt(1))
	if _, ok := number.SetString(value, 10); !ok || number.Cmp(minimum) < 0 || number.Cmp(maximum) > 0 {
		return "", errors.New("INVALID_EXACT_NUMBER")
	}
	if number.String() != value {
		return "", errors.New("INVALID_EXACT_NUMBER")
	}
	return value, nil
}

func validExportText(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\x00\r\n")
}

func digestCanonical(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (a *App) ListExports(filter exportjob.ListFilter) ([]exportjob.Job, error) {
	a.mu.RLock()
	ready, repository := a.ready, a.exports
	a.mu.RUnlock()
	if !ready || repository == nil {
		return nil, ErrApplicationNotReady
	}
	return repository.List(a.ctx, filter)
}

func (a *App) RestartExport(jobID string, input exportjob.CommandEnvelope) (exportjob.Job, error) {
	a.mu.RLock()
	ready, runtime := a.ready, a.exportRun
	a.mu.RUnlock()
	if !ready || runtime == nil {
		return exportjob.Job{}, ErrApplicationNotReady
	}
	return runtime.Restart(a.ctx, jobID, input)
}

func (a *App) CancelExport(jobID string, input exportjob.CommandEnvelope) (exportjob.Job, error) {
	a.mu.RLock()
	ready, runtime := a.ready, a.exportRun
	a.mu.RUnlock()
	if !ready || runtime == nil {
		return exportjob.Job{}, ErrApplicationNotReady
	}
	return runtime.Cancel(a.ctx, jobID, input)
}

func (a *App) CleanupExport(jobID string, input exportjob.CommandEnvelope) (exportjob.Job, error) {
	a.mu.RLock()
	ready, runtime := a.ready, a.exportRun
	a.mu.RUnlock()
	if !ready || runtime == nil {
		return exportjob.Job{}, ErrApplicationNotReady
	}
	return runtime.Cleanup(a.ctx, jobID, input)
}

func (a *App) services() (*profile.Service, *connection.Manager, *tasks.Repository, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.ready || a.profiles == nil || a.connections == nil || a.tasks == nil {
		return nil, nil, nil, ErrApplicationNotReady
	}
	return a.profiles, a.connections, a.tasks, nil
}

func profileView(value profile.Profile) ProfileView {
	return ProfileView{
		ID: value.ID, Revision: value.Revision, Name: value.Name, BaseURL: value.BaseURL,
		DefaultDatabase: value.DefaultDatabase,
		Environment:     value.Environment, AuthMode: value.AuthMode, Username: value.Username,
		AllowInsecureAuth: value.AllowInsecureAuth, ProtectionMode: value.ProtectionMode,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}

func queryResultPageView(page query.ResultPage) (QueryResultPageView, error) {
	rows := make([][]QueryScalarView, len(page.Rows))
	for rowIndex, row := range page.Rows {
		rows[rowIndex] = make([]QueryScalarView, len(row))
		for columnIndex, scalar := range row {
			view, err := queryScalarView(scalar)
			if err != nil {
				return QueryResultPageView{}, err
			}
			rows[rowIndex][columnIndex] = view
		}
	}
	return QueryResultPageView{Rows: rows, NextCursor: page.NextCursor, EOF: page.EOF}, nil
}

func queryScalarView(scalar query.TypedScalar) (QueryScalarView, error) {
	view := QueryScalarView{Kind: scalar.Kind}
	switch scalar.Kind {
	case query.ScalarNull:
		return view, nil
	case query.ScalarString:
		view.Value = scalar.StringValue
		return view, nil
	case query.ScalarBoolean:
		if scalar.BooleanValue == nil {
			return QueryScalarView{}, errors.New("INVALID_TYPED_SCALAR")
		}
		view.Value = *scalar.BooleanValue
		return view, nil
	case query.ScalarTimestampNS, query.ScalarInt64, query.ScalarUint64,
		query.ScalarFloat64, query.ScalarNumericText:
		if scalar.DecimalText == "" {
			return QueryScalarView{}, errors.New("INVALID_TYPED_SCALAR")
		}
		view.DecimalText = scalar.DecimalText
		return view, nil
	default:
		return QueryScalarView{}, errors.New("INVALID_TYPED_SCALAR")
	}
}
