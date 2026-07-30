package operation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	ast "github.com/influxdata/influxql"
	policy "github.com/influxdesk/influxdesk/internal/influxql"
	"github.com/influxdesk/influxdesk/internal/protection"
	"github.com/influxdesk/influxdesk/internal/store"
	"github.com/influxdesk/influxdesk/internal/tasks"
	"github.com/influxdesk/influxdesk/internal/transport"
)

type dispatcher interface {
	BeginRoundTrip(context.Context, transport.Request) (*transport.Attempt, error)
}

type tokenRecord struct {
	token                string
	canonicalQuery       string
	database             string
	retentionPolicy      string
	operationKind        string
	target               string
	confirmationRequired bool
	actionDigest         string
	binding              protection.DispatchBinding
	expires              time.Time
}

type operationControl struct {
	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
}

type Service struct {
	store      *store.Store
	tasks      *tasks.Repository
	protection *protection.Manager
	dispatcher dispatcher

	profileID, profileRevision string
	connectionID, generation   string
	options                    Options

	tokensMu   sync.Mutex
	tokens     map[string]tokenRecord
	controlsMu sync.Mutex
	controls   map[string]*operationControl

	beforeTokenStore func()
	beforeDispatch   func()
}

func NewService(
	s *store.Store,
	taskRepository *tasks.Repository,
	protectionManager *protection.Manager,
	dispatcher dispatcher,
	profileID, profileRevision, connectionID, generation string,
	options Options,
) *Service {
	if options.PreviewTTL <= 0 || options.PreviewTTL > 120*time.Second {
		options.PreviewTTL = 120 * time.Second
	}
	if options.MutationTimeout <= 0 {
		options.MutationTimeout = 60 * time.Second
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Service{
		store: s, tasks: taskRepository, protection: protectionManager, dispatcher: dispatcher,
		profileID: profileID, profileRevision: profileRevision,
		connectionID: connectionID, generation: generation,
		options: options, tokens: make(map[string]tokenRecord), controls: make(map[string]*operationControl),
	}
}

func (s *Service) Preview(ctx context.Context, request PreviewRequest) (Preview, error) {
	analysis, err := policy.ParseAndClassify(request.Query)
	if err != nil || analysis.Classification != policy.MayMutate || len(analysis.Statements) != 1 {
		return Preview{}, ErrPreviewUnavailable
	}
	kind, target, confirmation := mutationTarget(analysis.Parsed.Statements[0])
	preview := Preview{
		CanonicalQuery: analysis.Canonical, OperationKind: kind, Target: target,
		ConfirmationRequired: confirmation,
	}
	_, err = s.protection.WithGrantBinding(ctx, s.connectionID, s.generation, func(binding protection.DispatchBinding, snapshot protection.Snapshot) error {
		if binding.ProfileRevision != s.profileRevision {
			return protection.ErrBindingMismatch
		}
		expires := s.options.Now().UTC().Add(s.options.PreviewTTL)
		if snapshot.UnlockedUntil == nil {
			return protection.ErrLocked
		}
		leaseExpiry, err := time.Parse(time.RFC3339Nano, *snapshot.UnlockedUntil)
		if err != nil {
			return protection.ErrBindingMismatch
		}
		if leaseExpiry.Before(expires) {
			expires = leaseExpiry
		}
		if !s.options.Now().UTC().Before(expires) {
			return protection.ErrLeaseExpired
		}

		s.tokensMu.Lock()
		defer s.tokensMu.Unlock()
		s.purgeExpiredLocked()
		var token string
		for attempts := 0; attempts < 4; attempts++ {
			bytes := make([]byte, 32)
			if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
				return err
			}
			token = hex.EncodeToString(bytes)
			if _, exists := s.tokens[token]; !exists {
				break
			}
			token = ""
		}
		if token == "" {
			return errors.New("cannot allocate unique preview token")
		}
		record := tokenRecord{
			token: token, canonicalQuery: analysis.Canonical,
			database: request.Database, retentionPolicy: request.RetentionPolicy,
			operationKind: kind, target: target, confirmationRequired: confirmation,
			binding: binding, expires: expires,
		}
		record.actionDigest = actionDigest(record)
		if s.beforeTokenStore != nil {
			s.beforeTokenStore()
		}
		if !s.options.Now().UTC().Before(expires) {
			return protection.ErrLeaseExpired
		}
		s.tokens[token] = record
		expiresText := expires.Format(time.RFC3339Nano)
		preview.Executable = true
		preview.Token = &token
		preview.ExpiresAt = &expiresText
		return nil
	})
	if err != nil {
		if errors.Is(err, protection.ErrLocked) || errors.Is(err, protection.ErrLeaseExpired) {
			return preview, nil
		}
		return Preview{}, err
	}
	return preview, nil
}

func (s *Service) Execute(ctx context.Context, request ExecuteRequest) (Operation, error) {
	if _, err := uuid.Parse(request.ClientRequestID); err != nil || request.PreviewToken == "" {
		return Operation{}, ErrPreviewUnavailable
	}
	scope := "EXECUTE_MUTATION\x1f" + s.profileID + "\x1f" + request.ClientRequestID
	digest := executeDigest(request)
	if existing, found, err := s.lookupIdempotency(ctx, scope, digest); err != nil || found {
		return existing, err
	}

	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	if existing, found, err := s.lookupIdempotency(ctx, scope, digest); err != nil || found {
		return existing, err
	}
	s.purgeExpiredLocked()
	record, found := s.tokens[request.PreviewToken]
	if !found {
		return Operation{}, ErrPreviewExpired
	}
	if record.confirmationRequired && request.Confirmation != record.target {
		return Operation{}, ErrConfirmationMismatch
	}
	if !s.options.Now().UTC().Before(record.expires) {
		delete(s.tokens, request.PreviewToken)
		return Operation{}, ErrPreviewExpired
	}

	operationID := uuid.NewString()
	var out Operation
	err := s.store.WithImmediate(ctx, func(tx store.Executor) error {
		entry, found, err := tasks.LookupIdempotencyInTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		if found {
			if entry.RequestDigest != digest {
				return tasks.ErrIdempotencyConflict
			}
			out, err = getInTx(ctx, tx, entry.ResourceID)
			out.Replayed = true
			return err
		}
		now := s.options.Now().UTC()
		meta, err := s.tasks.CreateInTx(ctx, tx, operationID, "OPERATION", StateQueued, false)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO operations(
			operation_id,profile_id,profile_revision,connection_id,connection_generation,
			protection_revision,action_digest,operation_kind,state,dispatch_attempted,
			created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,0,?,?)`, operationID,
			s.profileID, s.profileRevision, s.connectionID, s.generation,
			record.binding.ProtectionRevision, record.actionDigest, record.operationKind,
			StateQueued, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err := tasks.InsertIdempotencyInTx(ctx, tx, tasks.IdempotencyEntry{
			Scope: scope, RequestDigest: digest, ResourceKind: "OPERATION", ResourceID: operationID,
			CreatedAt: now, ExpiresAt: now.Add(90 * 24 * time.Hour),
		}); err != nil {
			return err
		}
		out = operationFromRecord(meta, record, operationID, s.profileID, false)
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	delete(s.tokens, request.PreviewToken)
	runCtx, cancel := context.WithTimeout(context.Background(), s.options.MutationTimeout)
	control := &operationControl{cancel: cancel}
	s.controlsMu.Lock()
	s.controls[operationID] = control
	s.controlsMu.Unlock()
	go s.dispatch(runCtx, operationID, record, control)
	return out, nil
}

func (s *Service) Get(ctx context.Context, operationID string) (Operation, error) {
	return getInTx(ctx, s.store.DB(), operationID)
}

func GetStored(ctx context.Context, s *store.Store, operationID string) (Operation, error) {
	return getInTx(ctx, s.DB(), operationID)
}

func ListStored(ctx context.Context, s *store.Store, limit int) ([]Operation, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.DB().QueryContext(ctx, `SELECT operation_id FROM operations ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]Operation, 0, len(ids))
	for _, id := range ids {
		value, err := getInTx(ctx, s.DB(), id)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (s *Service) dispatch(ctx context.Context, operationID string, record tokenRecord, control *operationControl) {
	defer func() {
		control.cancel()
		s.controlsMu.Lock()
		delete(s.controls, operationID)
		s.controlsMu.Unlock()
	}()
	if s.beforeDispatch != nil {
		s.beforeDispatch()
	}
	var attempt *transport.Attempt
	persistAttempted := false
	err := s.protection.BeginMutationDispatch(ctx, record.binding,
		func(ctx context.Context) error {
			persistAttempted = true
			return s.transition(ctx, operationID, StateDispatching, false, "DISPATCHING", "ATTEMPTED")
		},
		func(ctx context.Context) error {
			var err error
			attempt, err = s.dispatcher.BeginRoundTrip(ctx, transport.AuthorizedMutationQuery{
				Database: record.database, RetentionPolicy: record.retentionPolicy, Query: record.canonicalQuery,
			})
			if err == nil {
				control.mu.Lock()
				control.started = true
				control.mu.Unlock()
			}
			return err
		})
	if err != nil {
		state := StateRejectedNotSent
		code := "MUTATION_AUTHORIZATION_REJECTED"
		if persistAttempted {
			state, code = StateFailedInternalNotSent, "MUTATION_START_FAILED_NOT_SENT"
		}
		_ = s.finish(operationID, state, code, "Mutation was not sent", "REJECTED")
		return
	}
	response, err := attempt.Wait()
	if err != nil {
		_ = s.finish(operationID, StateOutcomeUnknown, "MUTATION_OUTCOME_UNKNOWN", "Mutation outcome is unknown", "UNKNOWN")
		return
	}
	defer response.Close()
	state, code, message, audit := classifyMutationResponse(response)
	_ = s.finish(operationID, state, code, message, audit)
}

func (s *Service) transition(ctx context.Context, operationID, state string, terminal bool, changeType, auditOutcome string) error {
	return s.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, err := tasks.GetInTx(ctx, tx, operationID)
		if err != nil {
			return err
		}
		if current.Terminal || current.State != StateQueued || state != StateDispatching {
			return ErrStateConflict
		}
		updated, err := s.tasks.ApplyInTx(ctx, tx, current, tasks.Change{
			State: state, Terminal: terminal, StateChanged: true, ChangeType: changeType,
		})
		if err != nil {
			return err
		}
		now := s.options.Now().UTC().Format(time.RFC3339Nano)
		attempted := 0
		if state == StateDispatching {
			attempted = 1
		}
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,dispatch_attempted=max(dispatch_attempted,?),updated_at=?
			WHERE operation_id=? AND state=?`, state, attempted, now, operationID, StateQueued)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return ErrStateConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) SELECT 'MUTATION',operation_kind,operation_id,action_digest,?,operation_id,?
			FROM operations WHERE operation_id=?`, auditOutcome, now, operationID)
		_ = updated
		return err
	})
}

func (s *Service) finish(operationID, state, code, message, audit string) error {
	ctx := context.Background()
	return s.store.WithImmediate(ctx, func(tx store.Executor) error {
		current, err := tasks.GetInTx(ctx, tx, operationID)
		if err != nil {
			return err
		}
		if current.Terminal {
			return nil
		}
		previousState := current.State
		var codePtr, messagePtr *string
		if code != "" {
			codePtr, messagePtr = &code, &message
		}
		if _, err := s.tasks.ApplyInTx(ctx, tx, current, tasks.Change{
			State: state, Terminal: true, StateChanged: true, ChangeType: "STATE",
			PublicErrorCode: codePtr, PublicSafeMessage: messagePtr,
		}); err != nil {
			return err
		}
		now := s.options.Now().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,updated_at=? WHERE operation_id=? AND state=?`,
			state, now, operationID, previousState)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return ErrStateConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(
			category,action,target_id,target_digest,outcome,correlation_id,occurred_at
		) SELECT 'MUTATION',operation_kind,operation_id,action_digest,?,operation_id,?
			FROM operations WHERE operation_id=?`, audit, now, operationID)
		return err
	})
}

func (s *Service) lookupIdempotency(ctx context.Context, scope, digest string) (Operation, bool, error) {
	entry, found, err := tasks.LookupIdempotencyInTx(ctx, s.store.DB(), scope)
	if err != nil || !found {
		return Operation{}, found, err
	}
	if entry.RequestDigest != digest {
		return Operation{}, true, tasks.ErrIdempotencyConflict
	}
	value, err := getInTx(ctx, s.store.DB(), entry.ResourceID)
	value.Replayed = true
	return value, true, err
}

func getInTx(ctx context.Context, tx store.Executor, operationID string) (Operation, error) {
	meta, err := tasks.GetInTx(ctx, tx, operationID)
	if errors.Is(err, tasks.ErrNotFound) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, err
	}
	var out Operation
	var attempted, cancelRequested int
	err = tx.QueryRowContext(ctx, `SELECT profile_id,profile_revision,connection_id,
		connection_generation,protection_revision,action_digest,operation_kind,dispatch_attempted,cancel_requested
		FROM operations WHERE operation_id=?`, operationID).Scan(&out.ProfileID, &out.ProfileRevision,
		&out.ConnectionID, &out.ConnectionGeneration, &out.ProtectionRevision,
		&out.ActionDigest, &out.OperationKind, &attempted, &cancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	out.Task = meta
	out.DispatchAttempted = attempted != 0
	out.CancelRequested = cancelRequested != 0
	return out, err
}

func operationFromRecord(meta tasks.Meta, record tokenRecord, operationID, profileID string, replayed bool) Operation {
	return Operation{
		Task: meta, ProfileID: profileID,
		ConnectionID: record.binding.ConnectionID, ConnectionGeneration: record.binding.ConnectionGeneration,
		ProfileRevision: record.binding.ProfileRevision, ProtectionRevision: record.binding.ProtectionRevision,
		ActionDigest: record.actionDigest, OperationKind: record.operationKind, Replayed: replayed,
	}
}

func (s *Service) purgeExpiredLocked() {
	now := s.options.Now().UTC()
	for token, record := range s.tokens {
		if !now.Before(record.expires) {
			delete(s.tokens, token)
		}
	}
}

func actionDigest(record tokenRecord) string {
	return digestValues("MutationActionV1", record.operationKind, record.database,
		record.retentionPolicy, record.canonicalQuery, record.binding.ConnectionGeneration,
		record.binding.ProtectionRevision)
}

func executeDigest(request ExecuteRequest) string {
	tokenHash := sha256.Sum256([]byte(request.PreviewToken))
	return digestValues("ExecuteMutationV1", hex.EncodeToString(tokenHash[:]), request.Confirmation)
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

func mutationTarget(statement ast.Statement) (kind, target string, confirmation bool) {
	kind = fmt.Sprintf("%T", statement)
	if index := strings.LastIndex(kind, "."); index >= 0 {
		kind = strings.TrimSuffix(kind[index+1:], "Statement")
	}
	switch value := statement.(type) {
	case *ast.DropDatabaseStatement:
		return kind, value.Name, true
	case *ast.DropMeasurementStatement:
		return kind, value.Name, true
	case *ast.DropRetentionPolicyStatement:
		return kind, value.Name, true
	case *ast.DropContinuousQueryStatement:
		return kind, value.Name, true
	case *ast.DropUserStatement:
		return kind, value.Name, true
	case *ast.DeleteStatement:
		return kind, value.Source.String(), true
	case *ast.DropSeriesStatement:
		return kind, value.Sources.String(), true
	case *ast.DeleteSeriesStatement:
		return kind, value.Sources.String(), true
	case *ast.KillQueryStatement:
		return kind, strconv.FormatUint(value.QueryID, 10), true
	default:
		return kind, "", false
	}
}

func classifyMutationResponse(response *transport.Response) (state, code, message, audit string) {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= 500 {
			return StateOutcomeUnknown, "MUTATION_OUTCOME_UNKNOWN", "Mutation outcome is unknown", "UNKNOWN"
		}
		return StateRejected, "MUTATION_REJECTED", "InfluxDB rejected the mutation", "REJECTED"
	}
	const maxResponseBytes = 64 << 10
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return StateOutcomeUnknown, "MUTATION_OUTCOME_UNKNOWN", "Mutation outcome is unknown", "UNKNOWN"
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var envelope struct {
		Error   string `json:"error"`
		Results []struct {
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := decoder.Decode(&envelope); err != nil {
		return StateOutcomeUnknown, "MUTATION_OUTCOME_UNKNOWN", "Mutation outcome is unknown", "UNKNOWN"
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) || len(envelope.Results) == 0 {
		return StateOutcomeUnknown, "MUTATION_OUTCOME_UNKNOWN", "Mutation outcome is unknown", "UNKNOWN"
	}
	if envelope.Error != "" {
		return StateRejected, "MUTATION_REJECTED", "InfluxDB rejected the mutation", "REJECTED"
	}
	for _, result := range envelope.Results {
		if result.Error != "" {
			return StateRejected, "MUTATION_REJECTED", "InfluxDB rejected the mutation", "REJECTED"
		}
	}
	return StateSucceeded, "", "", "SUCCEEDED"
}
