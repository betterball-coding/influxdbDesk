package query

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ast "github.com/influxdata/influxql"
	policy "github.com/influxdesk/influxdesk/internal/influxql"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transport"
)

const maxTransientIssueBytes = 64 << 10

type SessionState string

const (
	SessionQueued              SessionState = "QUEUED"
	SessionRunning             SessionState = "RUNNING"
	SessionCancelRequested     SessionState = "CANCEL_REQUESTED"
	SessionSucceeded           SessionState = "SUCCEEDED"
	SessionSucceededWithErrors SessionState = "SUCCEEDED_WITH_ERRORS"
	SessionTruncated           SessionState = "TRUNCATED"
	SessionTimedOut            SessionState = "TIMED_OUT"
	SessionCanceled            SessionState = "CANCELED"
	SessionFailed              SessionState = "FAILED"
	SessionInterrupted         SessionState = "INTERRUPTED"
)

type Dispatcher interface {
	Dispatch(context.Context, transport.Request) (*transport.Response, error)
}

type barrierDispatcher interface {
	BeginRoundTrip(context.Context, transport.Request) (*transport.Attempt, error)
}

type ServiceOptions struct {
	MaxInFlightPerConnection int
	QueueLimitPerConnection  int
	QueryTimeout             time.Duration
	CursorTTL                time.Duration
	TransientIssueTTL        time.Duration
	ResultIdleTTL            time.Duration
	// SpillDirectory must be inside the caller's already-protected private
	// data root. Queries fail closed if they cross the memory threshold without it.
	SpillDirectory string
	// Zero values select the PLAN 39 limits: 32 MiB memory, 128 MiB total,
	// and 100000 rows. Byte limits count the canonical V1 row encoding.
	ResultMemoryLimitBytes uint64
	ResultMaxBytes         uint64
	ResultMaxRows          uint64
	NumericResolver        NumericKindResolver
	Now                    func() time.Time
}

type Service struct {
	dispatcher Dispatcher
	options    ServiceOptions
	cursor     cursorCodec
	store      *store.Store
	tasks      *tasks.Repository

	mu       sync.Mutex
	sessions map[string]*sessionRecord
	lanes    map[string]*queryLane
	idem     map[string]idempotencyRecord
}

type queryLane struct {
	running int
	queue   []string
}

type idempotencyRecord struct {
	digest    string
	sessionID string
}

type sessionRecord struct {
	snapshot         QuerySession
	request          StartRequest
	analysis         policy.Analysis
	result           *ResultSet
	resultLastAccess time.Time
	resultExpiry     *time.Timer
	rawIssue         string
	rawIssueAt       time.Time
	cancel           context.CancelFunc
	laneKey          string
	closed           bool
}

type StartRequest struct {
	ProfileID       string
	ProfileRevision string
	ConnectionID    string
	Generation      string
	Database        string
	RetentionPolicy string
	Query           string
	ClientRequestID string
}

type QuerySession struct {
	ID                 string       `json:"id"`
	State              SessionState `json:"state"`
	Terminal           bool         `json:"terminal"`
	SnapshotRevision   string       `json:"snapshotRevision"`
	StateRevision      string       `json:"stateRevision"`
	LastEventSeq       string       `json:"lastEventSeq"`
	ProfileID          string       `json:"profileId,omitempty"`
	ProfileRevision    string       `json:"profileRevision,omitempty"`
	ConnectionID       string       `json:"connectionId"`
	Generation         string       `json:"generation"`
	CreatedAt          string       `json:"createdAt"`
	UpdatedAt          string       `json:"updatedAt"`
	TerminalAt         *string      `json:"terminalAt,omitempty"`
	PublicErrorCode    string       `json:"publicErrorCode,omitempty"`
	PublicSafeMessage  string       `json:"publicSafeMessage,omitempty"`
	StatementCount     string       `json:"statementCount"`
	SeriesCount        string       `json:"seriesCount"`
	RowCount           string       `json:"rowCount"`
	HasStatementErrors bool         `json:"hasStatementErrors"`
	Complete           bool         `json:"complete"`
	ResultAvailable    bool         `json:"resultAvailable"`
	Closed             bool         `json:"closed"`
	Archived           bool         `json:"archived"`
	Replayed           bool         `json:"replayed,omitempty"`
}

type CommandEnvelope struct {
	CommandRequestID      string `json:"commandRequestId"`
	ExpectedStateRevision string `json:"expectedStateRevision"`
}

type RequestError struct {
	Code    string
	Message string
}

func (e *RequestError) Error() string { return e.Code + ": " + e.Message }

type StatementSummary struct {
	ID       int    `json:"id"`
	Complete bool   `json:"complete"`
	HasError bool   `json:"hasError"`
	Series   string `json:"series"`
}

type SeriesSummary struct {
	ID          string            `json:"id"`
	StatementID int               `json:"statementId"`
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags,omitempty"`
	Columns     []string          `json:"columns"`
	Rows        string            `json:"rows"`
}

type PageRequest struct {
	SessionID   string `json:"sessionId"`
	StatementID int    `json:"statementId"`
	SeriesID    string `json:"seriesId"`
	Cursor      string `json:"cursor"`
	Limit       int    `json:"limit"`
}

type ResultPage struct {
	Rows       [][]TypedScalar `json:"rows"`
	NextCursor string          `json:"nextCursor,omitempty"`
	EOF        bool            `json:"eof"`
}

func NewService(dispatcher Dispatcher, options ServiceOptions) (*Service, error) {
	return newService(dispatcher, nil, nil, options)
}

func NewPersistentService(
	dispatcher Dispatcher,
	database *store.Store,
	taskRepository *tasks.Repository,
	options ServiceOptions,
) (*Service, error) {
	if database == nil || taskRepository == nil {
		return nil, errors.New("query persistence is required")
	}
	return newService(dispatcher, database, taskRepository, options)
}

func newService(
	dispatcher Dispatcher,
	database *store.Store,
	taskRepository *tasks.Repository,
	options ServiceOptions,
) (*Service, error) {
	if dispatcher == nil {
		return nil, errors.New("query dispatcher is required")
	}
	if options.MaxInFlightPerConnection <= 0 {
		options.MaxInFlightPerConnection = 4
	}
	if options.QueueLimitPerConnection <= 0 {
		options.QueueLimitPerConnection = 32
	}
	if options.QueryTimeout <= 0 {
		options.QueryTimeout = 60 * time.Second
	}
	if options.CursorTTL <= 0 {
		options.CursorTTL = 2 * time.Hour
	}
	if options.TransientIssueTTL <= 0 {
		options.TransientIssueTTL = 15 * time.Minute
	}
	if options.ResultIdleTTL <= 0 {
		options.ResultIdleTTL = 2 * time.Hour
	}
	if options.ResultMemoryLimitBytes == 0 {
		options.ResultMemoryLimitBytes = defaultResultMemoryLimitBytes
	}
	if options.ResultMaxBytes == 0 {
		options.ResultMaxBytes = defaultResultMaxBytes
	}
	if options.ResultMaxRows == 0 {
		options.ResultMaxRows = defaultResultMaxRows
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("create cursor key: %w", err)
	}
	return &Service{
		dispatcher: dispatcher,
		options:    options,
		cursor:     cursorCodec{key: key},
		store:      database,
		tasks:      taskRepository,
		sessions:   make(map[string]*sessionRecord),
		lanes:      make(map[string]*queryLane),
		idem:       make(map[string]idempotencyRecord),
	}, nil
}

// StartReadQuery authorizes before task creation. Rejected AST and full queues
// have zero persistent or network side effects.
func (s *Service) StartReadQuery(ctx context.Context, request StartRequest) (QuerySession, error) {
	if request.ConnectionID == "" || !isCanonicalUint(request.Generation) || !isUUID(request.ClientRequestID) {
		return QuerySession{}, requestError("INVALID_REQUEST", "连接、generation 或 clientRequestId 无效")
	}
	if request.ProfileID == "" {
		request.ProfileID = request.ConnectionID
	}
	if request.ProfileRevision == "" && s.store == nil {
		request.ProfileRevision = "0"
	}
	if request.ProfileID == "" || !isCanonicalUint(request.ProfileRevision) {
		return QuerySession{}, requestError("INVALID_REQUEST", "profile 或 profile revision 无效")
	}
	analysis, err := policy.ParseAndClassify(request.Query)
	if err != nil {
		return QuerySession{}, requestError("INVALID_QUERY", "InfluxQL 语法无效")
	}
	if analysis.Classification != policy.ReadOnly {
		return QuerySession{}, requestError("QUERY_NOT_READ_ONLY", "只读查询接口拒绝了该语句")
	}

	scope := "START_READ_QUERY\x1f" + request.ProfileID + "\x1f" + request.ConnectionID + "\x1f" + request.ClientRequestID
	digest := digestStartRequest(request, analysis.Canonical)
	laneKey := request.ConnectionID + "\x00" + request.Generation
	if s.store != nil {
		return s.startPersistent(ctx, request, analysis, scope, digest, laneKey)
	}
	return s.startInMemory(request, analysis, scope, digest, laneKey)
}

func (s *Service) startInMemory(
	request StartRequest,
	analysis policy.Analysis,
	scope, digest, laneKey string,
) (QuerySession, error) {
	now := s.options.Now()

	s.mu.Lock()
	if existing, ok := s.idem[scope]; ok {
		if existing.digest != digest {
			s.mu.Unlock()
			return QuerySession{}, requestError("IDEMPOTENCY_CONFLICT", "clientRequestId 已绑定到不同请求")
		}
		snapshot := s.sessions[existing.sessionID].snapshot
		snapshot.Replayed = true
		s.mu.Unlock()
		return snapshot, nil
	}
	lane := s.lanes[laneKey]
	if lane == nil {
		lane = &queryLane{}
		s.lanes[laneKey] = lane
	}
	if lane.running >= s.options.MaxInFlightPerConnection && len(lane.queue) >= s.options.QueueLimitPerConnection {
		s.mu.Unlock()
		return QuerySession{}, requestError("QUERY_QUEUE_FULL", "该连接的查询等待队列已满")
	}

	id, err := randomUUID()
	if err != nil {
		s.mu.Unlock()
		return QuerySession{}, requestError("INTERNAL_ERROR", "无法创建查询任务")
	}
	snapshot := QuerySession{
		ID:               id,
		State:            SessionQueued,
		SnapshotRevision: "1",
		StateRevision:    "1",
		LastEventSeq:     "0",
		ProfileID:        request.ProfileID,
		ProfileRevision:  request.ProfileRevision,
		ConnectionID:     request.ConnectionID,
		Generation:       request.Generation,
		CreatedAt:        now.UTC().Format(time.RFC3339Nano),
		UpdatedAt:        now.UTC().Format(time.RFC3339Nano),
		StatementCount:   strconv.Itoa(len(analysis.Statements)),
		SeriesCount:      "0",
		RowCount:         "0",
	}
	record := &sessionRecord{snapshot: snapshot, request: request, analysis: analysis, laneKey: laneKey}
	s.sessions[id] = record
	s.idem[scope] = idempotencyRecord{digest: digest, sessionID: id}
	startNow := lane.running < s.options.MaxInFlightPerConnection
	if startNow {
		lane.running++
		transition(&record.snapshot, SessionRunning, false, now)
	} else {
		lane.queue = append(lane.queue, id)
	}
	snapshot = record.snapshot
	s.mu.Unlock()

	if startNow {
		go s.execute(id)
	}
	return snapshot, nil
}

func (s *Service) startPersistent(
	ctx context.Context,
	request StartRequest,
	analysis policy.Analysis,
	scope, digest, laneKey string,
) (QuerySession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if replay, found, err := s.lookupStartReplay(ctx, scope, digest); err != nil || found {
		return replay, persistentStartError(err)
	}
	lane := s.lanes[laneKey]
	if lane == nil {
		lane = &queryLane{}
		s.lanes[laneKey] = lane
	}
	if lane.running >= s.options.MaxInFlightPerConnection && len(lane.queue) >= s.options.QueueLimitPerConnection {
		return QuerySession{}, requestError("QUERY_QUEUE_FULL", "该连接的查询等待队列已满")
	}
	id, err := randomUUID()
	if err != nil {
		return QuerySession{}, requestError("INTERNAL_ERROR", "无法创建查询任务")
	}
	startNow := lane.running < s.options.MaxInFlightPerConnection
	var snapshot QuerySession
	replayed := false
	err = s.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupIdempotencyInTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != digest || entry.ResourceKind != queryKind {
				return tasks.ErrIdempotencyConflict
			}
			snapshot, err = readStoredSessionTx(ctx, tx, entry.ResourceID)
			replayed = true
			return err
		}
		meta, err := s.tasks.CreateInTx(ctx, tx, id, queryKind, string(SessionQueued), false)
		if err != nil {
			return err
		}
		snapshot = QuerySession{
			ID: id, ProfileID: request.ProfileID, ProfileRevision: request.ProfileRevision,
			ConnectionID: request.ConnectionID, Generation: request.Generation,
			StatementCount: strconv.Itoa(len(analysis.Statements)), SeriesCount: "0", RowCount: "0",
		}
		applyTaskMeta(&snapshot, meta)
		if err := insertQuerySessionTx(ctx, tx, snapshot); err != nil {
			return err
		}
		now := s.options.Now().UTC()
		if err := tasks.InsertIdempotencyInTx(ctx, tx, tasks.IdempotencyEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: queryKind, ResourceID: id,
			CreatedAt: now, ExpiresAt: now.Add(queryResourceRetention),
		}); err != nil {
			return err
		}
		if startNow {
			snapshot.State = SessionRunning
			snapshot.Terminal = false
			snapshot, err = applyDesiredQueryTx(ctx, tx, s.tasks, snapshot, true, "RUNNING")
		}
		return err
	})
	if err != nil {
		return QuerySession{}, persistentStartError(err)
	}
	if replayed {
		snapshot.Replayed = true
		if record := s.sessions[snapshot.ID]; record != nil {
			record.snapshot = snapshot
		}
		return snapshot, nil
	}
	record := &sessionRecord{snapshot: snapshot, request: request, analysis: analysis, laneKey: laneKey}
	s.sessions[id] = record
	if startNow {
		lane.running++
		go s.execute(id)
	} else {
		lane.queue = append(lane.queue, id)
	}
	return snapshot, nil
}

func (s *Service) lookupStartReplay(ctx context.Context, scope, digest string) (QuerySession, bool, error) {
	entry, found, err := tasks.LookupIdempotencyInTx(ctx, s.store.DB(), scope)
	if err != nil || !found {
		return QuerySession{}, found, err
	}
	if entry.RequestDigest != digest || entry.ResourceKind != queryKind {
		return QuerySession{}, true, tasks.ErrIdempotencyConflict
	}
	snapshot, err := readStoredSessionTx(ctx, s.store.DB(), entry.ResourceID)
	if err != nil {
		return QuerySession{}, true, err
	}
	snapshot.Replayed = true
	return snapshot, true, nil
}

func persistentStartError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, tasks.ErrIdempotencyConflict) {
		return requestError("IDEMPOTENCY_CONFLICT", "clientRequestId 已绑定到不同请求")
	}
	if errors.Is(err, tasks.ErrNotFound) {
		return requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	return err
}

func (s *Service) GetQuerySession(id string) (QuerySession, error) {
	_ = s.ExpireResults()
	if s.store != nil {
		return GetStoredSession(context.Background(), s.store, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.sessions[id]
	if record == nil {
		return QuerySession{}, requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	return record.snapshot, nil
}

func (s *Service) ListQuerySessions() []QuerySession {
	_ = s.ExpireResults()
	if s.store != nil {
		result, err := ListStoredSessions(context.Background(), s.store, 1000)
		if err == nil {
			return result
		}
		return nil
	}
	return s.ListActiveQuerySessions()
}

func (s *Service) ListActiveQuerySessions() []QuerySession {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]QuerySession, 0, len(s.sessions))
	for _, record := range s.sessions {
		result = append(result, record.snapshot)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func (s *Service) GetTransientIssue(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.sessions[id]
	if record == nil {
		return "", requestError("QUERY_NOT_FOUND", "查询任务不存在")
	}
	if record.rawIssue == "" || s.options.Now().Sub(record.rawIssueAt) > s.options.TransientIssueTTL {
		record.rawIssue = ""
		return "", requestError("TRANSIENT_ISSUE_UNAVAILABLE", "原始错误已不可用")
	}
	return record.rawIssue, nil
}

func (s *Service) ListStatementResults(id string) ([]StatementSummary, error) {
	_ = s.ExpireResults()
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.sessions[id]
	if record == nil || record.result == nil {
		return nil, requestError("RESULT_UNAVAILABLE", "查询结果不可用")
	}
	s.touchResultLocked(id, record)
	result := make([]StatementSummary, len(record.result.Statements))
	for i, statement := range record.result.Statements {
		result[i] = StatementSummary{
			ID: statement.ID, Complete: statement.Complete, HasError: statement.HasError,
			Series: strconv.Itoa(len(statement.Series)),
		}
	}
	return result, nil
}

func (s *Service) ListResultSeries(id string, statementID int) ([]SeriesSummary, error) {
	_ = s.ExpireResults()
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.sessions[id]
	if record == nil || record.result == nil || statementID < 0 || statementID >= len(record.result.Statements) {
		return nil, requestError("RESULT_UNAVAILABLE", "查询结果或 statement 不可用")
	}
	s.touchResultLocked(id, record)
	series := record.result.Statements[statementID].Series
	result := make([]SeriesSummary, len(series))
	for i, item := range series {
		result[i] = SeriesSummary{
			ID: item.ID, StatementID: item.StatementID, Measurement: item.Measurement,
			Tags: cloneTags(item.Tags), Columns: append([]string(nil), item.Columns...), Rows: strconv.FormatUint(item.RowCount, 10),
		}
	}
	return result, nil
}

func (s *Service) GetResultPage(request PageRequest) (ResultPage, error) {
	_ = s.ExpireResults()
	if request.Limit <= 0 {
		request.Limit = 500
	}
	if request.Limit > 5000 {
		return ResultPage{}, requestError("INVALID_PAGE_SIZE", "结果页大小不能超过 5000")
	}
	now := s.options.Now()
	statementID, seriesID, offset := request.StatementID, request.SeriesID, uint64(0)
	if request.Cursor != "" {
		payload, err := s.cursor.decode(request.Cursor, now)
		if err != nil || payload.SessionID != request.SessionID {
			return ResultPage{}, requestError("INVALID_CURSOR", "结果游标无效或已过期")
		}
		statementID, seriesID = payload.StatementID, payload.SeriesID
		offset, _ = strconv.ParseUint(payload.NextRow, 10, 64)

		s.mu.Lock()
		record := s.sessions[request.SessionID]
		validGeneration := record != nil && record.snapshot.Generation == payload.Generation
		s.mu.Unlock()
		if !validGeneration {
			return ResultPage{}, requestError("INVALID_CURSOR", "结果游标 generation 已失效")
		}
	}

	s.mu.Lock()
	record := s.sessions[request.SessionID]
	if record == nil || record.result == nil || statementID < 0 || statementID >= len(record.result.Statements) {
		s.mu.Unlock()
		return ResultPage{}, requestError("RESULT_UNAVAILABLE", "查询结果不可用")
	}
	var selected *Series
	for _, candidate := range record.result.Statements[statementID].Series {
		if candidate.ID == seriesID {
			selected = candidate
			break
		}
	}
	if selected == nil || offset > selected.RowCount {
		s.mu.Unlock()
		return ResultPage{}, requestError("RESULT_SERIES_NOT_FOUND", "结果 series 或行偏移无效")
	}
	var (
		rows  [][]TypedScalar
		total = selected.RowCount
		err   error
	)
	if record.result.store != nil {
		rows, total, err = record.result.store.ReadRows(statementID, seriesID, offset, request.Limit)
	} else {
		end := offset + uint64(request.Limit)
		if end > uint64(len(selected.Rows)) {
			end = uint64(len(selected.Rows))
		}
		rows = cloneRows(selected.Rows[offset:end])
		total = uint64(len(selected.Rows))
	}
	if err != nil {
		s.mu.Unlock()
		return ResultPage{}, requestError("RESULT_STORAGE_FAILED", "查询结果文件无法读取")
	}
	end := offset + uint64(len(rows))
	generation := record.snapshot.Generation
	terminal := record.snapshot.Terminal
	s.touchResultLocked(request.SessionID, record)
	s.mu.Unlock()

	page := ResultPage{Rows: rows, EOF: terminal && end == total}
	if !page.EOF {
		if len(rows) == 0 && request.Cursor != "" {
			page.NextCursor = request.Cursor
			return page, nil
		}
		cursor, err := s.cursor.encode(cursorPayload{
			Version: 1, SessionID: request.SessionID, Generation: generation,
			StatementID: statementID, SeriesID: seriesID, NextRow: strconv.FormatUint(end, 10),
			ExpiresUnix: now.Add(s.options.CursorTTL).Unix(),
		})
		if err != nil {
			return ResultPage{}, requestError("INTERNAL_ERROR", "无法创建结果游标")
		}
		page.NextCursor = cursor
	}
	return page, nil
}

func (s *Service) execute(id string) {
	s.mu.Lock()
	record := s.sessions[id]
	if record == nil {
		s.mu.Unlock()
		return
	}
	if record.snapshot.State == SessionCancelRequested {
		s.mu.Unlock()
		s.finish(id, SessionCanceled, nil, "", "", "")
		return
	}
	if record.snapshot.State != SessionRunning {
		s.mu.Unlock()
		return
	}
	request := record.request
	specs := statementSpecs(record.analysis.Parsed)
	ctx, cancel := context.WithCancel(context.Background())
	record.cancel = cancel
	s.mu.Unlock()
	defer cancel()

	rowStore, err := newResultStore(id, resultStoreOptions{
		directory:   s.options.SpillDirectory,
		memoryBytes: s.options.ResultMemoryLimitBytes,
		maxBytes:    s.options.ResultMaxBytes,
		maxRows:     s.options.ResultMaxRows,
	})
	if err != nil {
		s.finish(id, SessionFailed, nil, "FAILED_RESULT_STORAGE", "无法初始化查询结果存储", err.Error())
		return
	}
	defer func() {
		if rowStore != nil {
			_ = rowStore.Close()
		}
	}()

	requestOperation := transport.ReadQueryRequest{
		Database: request.Database, RetentionPolicy: request.RetentionPolicy, Query: record.analysis.Canonical,
	}
	var response *transport.Response
	var timedOut atomic.Bool
	var deadlineTimer *time.Timer
	defer func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}()
	if dispatcher, ok := s.dispatcher.(barrierDispatcher); ok {
		attempt, beginErr := dispatcher.BeginRoundTrip(ctx, requestOperation)
		if beginErr != nil {
			err = beginErr
		} else {
			deadlineTimer = time.AfterFunc(s.options.QueryTimeout, func() {
				timedOut.Store(true)
				cancel()
			})
			response, err = attempt.Wait()
		}
	} else {
		// Controlled test adapters may implement only Dispatch. Production uses
		// BeginRoundTrip so the deadline starts at the real network barrier.
		deadlineTimer = time.AfterFunc(s.options.QueryTimeout, func() {
			timedOut.Store(true)
			cancel()
		})
		response, err = s.dispatcher.Dispatch(ctx, requestOperation)
	}
	isTimedOut := func() bool {
		return timedOut.Load()
	}
	if err != nil {
		state, code, message := SessionFailed, "QUERY_TRANSPORT_FAILED", "查询网络请求失败"
		if isTimedOut() {
			state, code, message = SessionTimedOut, "QUERY_TIMEOUT", "查询超过 60 秒执行期限"
		} else if errors.Is(ctx.Err(), context.Canceled) {
			state, code, message = SessionCanceled, "", ""
		}
		s.finish(id, state, nil, code, message, err.Error())
		return
	}
	responseClosed := false
	closeResponse := func() {
		if responseClosed {
			return
		}
		responseClosed = true
		_ = response.Close()
		response.Body = nil
	}
	defer closeResponse()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw := readLimited(response.Body, maxTransientIssueBytes)
		closeResponse()
		s.finish(id, SessionFailed, nil, "FAILED_HTTP", "InfluxDB 返回了非成功状态", raw)
		return
	}
	if response.StatusCode == 204 {
		closeResponse()
		s.finish(id, SessionFailed, nil, "FAILED_EMPTY_RESPONSE", "InfluxDB 返回了空响应", "")
		return
	}

	result, decodeErr := DecodeChunked(response.Body, DecodeConfig{
		Statements: specs,
		Resolver:   s.options.NumericResolver,
		rowSink:    rowStore,
	})
	closeResponse()
	if decodeErr != nil {
		if isTimedOut() {
			s.finish(id, SessionTimedOut, nil, "QUERY_TIMEOUT", "查询超过 60 秒执行期限", decodeErr.Error())
			return
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			s.finish(id, SessionCanceled, nil, "", "", "")
			return
		}
		var protocol *DecodeError
		if errors.As(decodeErr, &protocol) {
			raw := protocol.RawIssue
			if raw == "" && protocol.Cause != nil {
				raw = protocol.Cause.Error()
			}
			s.finish(id, SessionFailed, nil, string(protocol.Code), protocol.SafeMessage, raw)
			return
		}
		s.finish(id, SessionFailed, nil, "FAILED_PROTOCOL", "无法解码查询响应", decodeErr.Error())
		return
	}
	result.store = rowStore
	rowStore = nil
	hasErrors := false
	issues := make([]string, 0)
	for _, issue := range result.StatementErrorText {
		if issue != "" {
			hasErrors = true
			issues = append(issues, issue)
		}
	}
	state := SessionSucceeded
	if result.Truncated {
		state = SessionTruncated
	} else if hasErrors {
		state = SessionSucceededWithErrors
	}
	s.finish(id, state, result, "", "", strings.Join(issues, "\n"))
}

func (s *Service) finish(id string, state SessionState, result *ResultSet, code, safeMessage, rawIssue string) {
	now := s.options.Now()
	var (
		next      string
		discarded *ResultSet
	)
	s.mu.Lock()
	record := s.sessions[id]
	if record == nil {
		s.mu.Unlock()
		_ = closeResultSet(result)
		return
	}
	if record.snapshot.Terminal {
		s.mu.Unlock()
		_ = closeResultSet(result)
		return
	}
	if record.closed && result != nil {
		discarded = result
		result = nil
	}
	desired := record.snapshot
	desired.PublicErrorCode = code
	desired.PublicSafeMessage = safeMessage
	if result != nil {
		var seriesCount, rowCount uint64
		hasStatementErrors := false
		for _, statement := range result.Statements {
			seriesCount += uint64(len(statement.Series))
			hasStatementErrors = hasStatementErrors || statement.HasError
			for _, series := range statement.Series {
				rowCount += series.RowCount
			}
		}
		desired.SeriesCount = strconv.FormatUint(seriesCount, 10)
		desired.RowCount = strconv.FormatUint(rowCount, 10)
		desired.ResultAvailable = !record.closed
		desired.Complete = !result.Truncated
		desired.HasStatementErrors = hasStatementErrors
	}
	if state == SessionCanceled || state == SessionTruncated {
		desired.Complete = false
	}
	desired.State = state
	desired.Terminal = true
	if s.store != nil {
		persisted, err := s.persistDesired(context.Background(), desired, true, "TERMINAL")
		if err != nil {
			record.rawIssue = "query terminal persistence failed"
			record.rawIssueAt = now
			s.mu.Unlock()
			_ = closeResultSet(result)
			_ = closeResultSet(discarded)
			return
		}
		desired = persisted
	} else {
		transition(&desired, state, true, now)
	}
	record.cancel = nil
	record.result = result
	record.snapshot = desired
	if rawIssue != "" {
		if len(rawIssue) > maxTransientIssueBytes {
			rawIssue = rawIssue[:maxTransientIssueBytes]
		}
		record.rawIssue, record.rawIssueAt = rawIssue, now
	}
	if result != nil {
		record.resultLastAccess = now
		s.armResultExpiryLocked(id, record)
	}
	lane := s.lanes[record.laneKey]
	if lane != nil && lane.running > 0 {
		lane.running--
	}
	for lane != nil && len(lane.queue) > 0 && lane.running < s.options.MaxInFlightPerConnection {
		candidate := lane.queue[0]
		queued := s.sessions[candidate]
		if queued == nil || queued.snapshot.State != SessionQueued {
			lane.queue = lane.queue[1:]
			continue
		}
		desiredQueued := queued.snapshot
		desiredQueued.State = SessionRunning
		desiredQueued.Terminal = false
		if s.store != nil {
			persisted, err := s.persistDesired(context.Background(), desiredQueued, true, "RUNNING")
			if err != nil {
				break
			}
			desiredQueued = persisted
		} else {
			transition(&desiredQueued, SessionRunning, false, now)
		}
		lane.queue = lane.queue[1:]
		lane.running++
		queued.snapshot = desiredQueued
		next = candidate
		break
	}
	s.mu.Unlock()
	_ = closeResultSet(discarded)
	if next != "" {
		go s.execute(next)
	}
}

// ExpireResults removes terminal row data that has been idle for ResultIdleTTL.
// Task metadata and idempotency records remain available.
func (s *Service) ExpireResults() error {
	now := s.options.Now()
	var expired []*ResultSet
	s.mu.Lock()
	for _, record := range s.sessions {
		if record.result == nil || record.resultLastAccess.IsZero() || now.Before(record.resultLastAccess.Add(s.options.ResultIdleTTL)) {
			continue
		}
		expired = append(expired, s.detachResultLocked(record, now))
	}
	s.mu.Unlock()
	return closeResultSets(expired)
}

func (s *Service) touchResultLocked(id string, record *sessionRecord) {
	record.resultLastAccess = s.options.Now()
	s.armResultExpiryLocked(id, record)
}

func (s *Service) armResultExpiryLocked(id string, record *sessionRecord) {
	if record.resultExpiry != nil {
		record.resultExpiry.Stop()
	}
	record.resultExpiry = time.AfterFunc(s.options.ResultIdleTTL, func() {
		s.expireResult(id)
	})
}

func (s *Service) expireResult(id string) {
	now := s.options.Now()
	var expired *ResultSet
	s.mu.Lock()
	record := s.sessions[id]
	if record != nil && record.result != nil {
		deadline := record.resultLastAccess.Add(s.options.ResultIdleTTL)
		if !now.Before(deadline) {
			expired = s.detachResultLocked(record, now)
		} else {
			record.resultExpiry = time.AfterFunc(deadline.Sub(now), func() {
				s.expireResult(id)
			})
		}
	}
	s.mu.Unlock()
	_ = closeResultSet(expired)
}

func (s *Service) detachResultLocked(record *sessionRecord, now time.Time) *ResultSet {
	desired := record.snapshot
	desired.ResultAvailable = false
	if s.store != nil {
		persisted, err := s.persistDesired(context.Background(), desired, false, "RESULT_EXPIRED")
		if err != nil {
			return nil
		}
		desired = persisted
	} else {
		bumpSnapshot(&desired, now)
	}
	result := record.result
	record.result = nil
	record.snapshot = desired
	if record.resultExpiry != nil {
		record.resultExpiry.Stop()
		record.resultExpiry = nil
	}
	return result
}

func closeResultSets(results []*ResultSet) error {
	var cleanupErr error
	for _, result := range results {
		cleanupErr = errors.Join(cleanupErr, closeResultSet(result))
	}
	return cleanupErr
}

func closeResultSet(result *ResultSet) error {
	if result == nil || result.store == nil {
		return nil
	}
	store := result.store
	result.store = nil
	return store.Close()
}

func statementSpecs(query *ast.Query) []StatementSpec {
	if query == nil {
		return nil
	}
	result := make([]StatementSpec, len(query.Statements))
	for i, statement := range query.Statements {
		switch typed := statement.(type) {
		case *ast.SelectStatement:
			result[i].TimeColumn = typed.TimeFieldName()
		case *ast.ExplainStatement:
			if typed.Statement != nil {
				result[i].TimeColumn = typed.Statement.TimeFieldName()
			}
		}
	}
	return result
}

func transition(snapshot *QuerySession, state SessionState, terminal bool, now time.Time) {
	snapshot.State = state
	snapshot.Terminal = terminal
	snapshot.StateRevision = incrementDecimal(snapshot.StateRevision)
	bumpSnapshot(snapshot, now)
	if terminal {
		finished := now.UTC().Format(time.RFC3339Nano)
		snapshot.TerminalAt = &finished
	}
}

func bumpSnapshot(snapshot *QuerySession, now time.Time) {
	snapshot.SnapshotRevision = incrementDecimal(snapshot.SnapshotRevision)
	snapshot.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
}

func incrementDecimal(value string) string {
	parsed := new(big.Int)
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "1"
	}
	if _, ok := parsed.SetString(value, 10); !ok || parsed.Sign() < 0 {
		return "1"
	}
	return parsed.Add(parsed, big.NewInt(1)).String()
}

func digestStartRequest(request StartRequest, canonical string) string {
	hash := sha256.New()
	for _, value := range []string{
		"StartReadQueryV1", request.ProfileID, request.ProfileRevision,
		request.ConnectionID, request.Generation,
		request.Database, request.RetentionPolicy, canonical,
	} {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func digestValues(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func randomUUID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func isUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}

func isCanonicalUint(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value[0] == '0' {
		return false
	}
	parsed := new(big.Int)
	_, ok := parsed.SetString(value, 10)
	return ok && parsed.Sign() >= 0
}

func requestError(code, message string) *RequestError {
	return &RequestError{Code: code, Message: message}
}

func readLimited(reader io.Reader, limit int64) string {
	if reader == nil {
		return ""
	}
	value, _ := io.ReadAll(io.LimitReader(reader, limit))
	return string(value)
}

func removeString(values []string, target string) []string {
	for i, value := range values {
		if value == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func cloneRows(rows [][]TypedScalar) [][]TypedScalar {
	clone := make([][]TypedScalar, len(rows))
	for i, row := range rows {
		clone[i] = append([]TypedScalar(nil), row...)
		for j := range clone[i] {
			if row[j].BooleanValue != nil {
				value := *row[j].BooleanValue
				clone[i][j].BooleanValue = &value
			}
		}
	}
	return clone
}
