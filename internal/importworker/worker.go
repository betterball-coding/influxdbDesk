package importworker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/transfer"
	"github.com/influxdesk/influxdesk/internal/transport"
)

const (
	MaxBatchPoints        = 5000
	MaxBatchBytes   int64 = 5 * 1024 * 1024
	maxResponseBody       = 64 * 1024
)

var (
	ErrInvalidRequest      = errors.New("IMPORT_WORKER_INVALID_REQUEST")
	ErrStagingIntegrity    = errors.New("IMPORT_STAGING_INTEGRITY_FAILED")
	ErrStagingOffset       = errors.New("IMPORT_STAGING_OFFSET_INVALID")
	ErrEmptyStaging        = errors.New("IMPORT_STAGING_EMPTY")
	ErrResponseTooLarge    = errors.New("IMPORT_RESPONSE_TOO_LARGE")
	ErrResponseProtocol    = errors.New("IMPORT_RESPONSE_PROTOCOL_ERROR")
	ErrSettlementFailed    = errors.New("IMPORT_SETTLEMENT_FAILED")
	ErrRoundTripNotStarted = errors.New("IMPORT_ROUND_TRIP_NOT_STARTED")
)

type Outcome string

const (
	OutcomeACKED     Outcome = "ACKED"
	OutcomeSucceeded Outcome = "SUCCEEDED"
	OutcomeTooLarge  Outcome = "REJECTED_TOO_LARGE"
	OutcomePartial   Outcome = "PARTIAL"
	OutcomeUnknown   Outcome = "UNKNOWN"
	OutcomeRejected  Outcome = "REJECTED"
)

type RunNextRequest struct {
	JobID           string
	StagingPath     string
	TargetDigest    string
	Database        string
	RetentionPolicy string
}

type Result struct {
	Job         transfer.ImportJob `json:"job"`
	Outcome     Outcome            `json:"outcome"`
	AttemptID   string             `json:"attemptId,omitempty"`
	BatchID     string             `json:"batchId,omitempty"`
	StartOffset string             `json:"startOffset,omitempty"`
	EndOffset   string             `json:"endOffset,omitempty"`
	PointCount  string             `json:"pointCount,omitempty"`
}

type Options struct {
	// AdditionalSuccessStatuses must be fixed by an InfluxDB version contract.
	// HTTP 204 with an empty body is always the default success response.
	AdditionalSuccessStatuses []int
}

type attemptWaiter interface {
	Wait() (*transport.Response, error)
}

type attemptStarter interface {
	Begin(context.Context, transport.AuthorizedWriteBatch) (attemptWaiter, error)
}

type runAuthorizer interface {
	BeginAttempt(context.Context, transfer.AttemptRequest, func(context.Context) error) error
}

type repository interface {
	GetImport(context.Context, string) (transfer.ImportJob, error)
	MarkAttemptNotSent(context.Context, string) (transfer.ImportJob, error)
	SettleACK(context.Context, string) (transfer.ImportJob, error)
	SettleTooLarge(context.Context, string) (transfer.ImportJob, error)
	SettleRejected(context.Context, string) (transfer.ImportJob, error)
	RecordIncident(context.Context, string, string, string) (transfer.ImportJob, error)
	FinishImport(context.Context, transfer.FinishImportRequest) (transfer.ImportJob, bool, error)
}

type Worker struct {
	authorizer runAuthorizer
	repository repository
	dispatcher attemptStarter
	success    map[int]struct{}
	newID      func() string
}

type dispatcherAdapter struct{ dispatcher *transport.Dispatcher }

func (d dispatcherAdapter) Begin(ctx context.Context, request transport.AuthorizedWriteBatch) (attemptWaiter, error) {
	return d.dispatcher.BeginRoundTrip(ctx, request)
}

func New(
	authorizer *transfer.RunAuthorizer,
	repository *transfer.Repository,
	dispatcher *transport.Dispatcher,
	options Options,
) (*Worker, error) {
	if authorizer == nil || repository == nil || dispatcher == nil {
		return nil, ErrInvalidRequest
	}
	return newWorker(authorizer, repository, dispatcherAdapter{dispatcher: dispatcher}, options)
}

func newWorker(authorizer runAuthorizer, repository repository, dispatcher attemptStarter, options Options) (*Worker, error) {
	if authorizer == nil || repository == nil || dispatcher == nil {
		return nil, ErrInvalidRequest
	}
	success := map[int]struct{}{204: {}}
	for _, status := range options.AdditionalSuccessStatuses {
		if status < 200 || status > 299 {
			return nil, ErrInvalidRequest
		}
		success[status] = struct{}{}
	}
	return &Worker{
		authorizer: authorizer, repository: repository, dispatcher: dispatcher,
		success: success, newID: uuid.NewString,
	}, nil
}

// RunNext verifies the complete immutable staging stream, dispatches at most
// one batch and durably settles its outcome. Callers schedule another quantum
// only when the returned job remains RUNNING.
func (w *Worker) RunNext(ctx context.Context, request RunNextRequest) (Result, error) {
	if w == nil || ctx == nil || request.JobID == "" || request.StagingPath == "" ||
		request.TargetDigest == "" || request.Database == "" {
		return Result{}, ErrInvalidRequest
	}
	job, err := w.repository.GetImport(ctx, request.JobID)
	if err != nil {
		return Result{}, err
	}
	targetDigest, err := transfer.ImportTargetDigest(transfer.ImportTarget{
		Database: request.Database, RetentionPolicy: request.RetentionPolicy,
	})
	if err != nil || targetDigest != request.TargetDigest {
		return Result{}, ErrInvalidRequest
	}
	if job.Task.State == transfer.ImportSucceeded {
		return Result{Job: job, Outcome: OutcomeSucceeded}, nil
	}
	if job.Task.State != transfer.ImportRunning || job.ActiveRunSegmentID == nil ||
		job.Checkpoint.TargetDigest != request.TargetDigest {
		return Result{}, transfer.ErrImportState
	}

	batch, err := readVerifiedBatch(ctx, request.StagingPath, job)
	if err != nil {
		return Result{}, err
	}
	if len(batch.payload) == 0 {
		if batch.stagingSize == 0 {
			return Result{}, ErrEmptyStaging
		}
		finished, _, err := w.repository.FinishImport(ctx, transfer.FinishImportRequest{
			JobID: job.Task.ID, RunSegmentID: *job.ActiveRunSegmentID,
			CheckpointDigest:   job.CheckpointDigest,
			StagingLogicalSize: strconv.FormatInt(batch.stagingSize, 10),
		})
		if err != nil {
			return Result{}, err
		}
		return Result{Job: finished, Outcome: OutcomeSucceeded}, nil
	}

	attemptID := w.newID()
	batchID := w.newID()
	if job.OpenIncident != nil {
		batchID = job.OpenIncident.BatchID
	}
	attemptRequest := transfer.AttemptRequest{
		JobID: job.Task.ID, BatchID: batchID, AttemptID: attemptID,
		RunSegmentID: *job.ActiveRunSegmentID, ParentCheckpointDigest: job.CheckpointDigest,
		StartOffset: strconv.FormatInt(batch.startOffset, 10),
		EndOffset:   strconv.FormatInt(batch.endOffset, 10), PayloadDigest: batch.payloadDigest,
	}
	var attempt attemptWaiter
	err = w.authorizer.BeginAttempt(ctx, attemptRequest, func(barrierCtx context.Context) error {
		var beginErr error
		attempt, beginErr = w.dispatcher.Begin(barrierCtx, transport.AuthorizedWriteBatch{
			Database: request.Database, RetentionPolicy: request.RetentionPolicy,
			Payload: batch.payload,
		})
		return beginErr
	})
	if err != nil {
		// A Begin callback error is before the real Dispatcher barrier. If the
		// SENDING transaction happened, close only that not-sent attempt.
		_, settleErr := w.repository.MarkAttemptNotSent(context.WithoutCancel(ctx), attemptID)
		if settleErr != nil && !errors.Is(settleErr, os.ErrNotExist) {
			return Result{}, errors.Join(err, fmt.Errorf("%w: %v", ErrSettlementFailed, settleErr))
		}
		return Result{}, errors.Join(ErrRoundTripNotStarted, err)
	}
	if attempt == nil {
		return w.recordIncident(context.WithoutCancel(ctx), Result{
			AttemptID: attemptID, BatchID: batchID,
			StartOffset: attemptRequest.StartOffset, EndOffset: attemptRequest.EndOffset,
			PointCount: strconv.Itoa(batch.points),
		}, attemptID, "UNKNOWN", ErrRoundTripNotStarted)
	}

	baseResult := Result{
		AttemptID: attemptID, BatchID: batchID,
		StartOffset: attemptRequest.StartOffset, EndOffset: attemptRequest.EndOffset,
		PointCount: strconv.Itoa(batch.points),
	}
	response, waitErr := attempt.Wait()
	if waitErr != nil {
		return w.recordIncident(context.WithoutCancel(ctx), baseResult, attemptID, "UNKNOWN", waitErr)
	}
	body, bodyErr := readAndCloseResponse(response)
	if bodyErr != nil {
		return w.recordIncident(context.WithoutCancel(ctx), baseResult, attemptID, "UNKNOWN", bodyErr)
	}
	classification := w.classify(response, body)
	settleCtx := context.WithoutCancel(ctx)
	switch classification {
	case OutcomeACKED:
		settled, err := w.repository.SettleACK(settleCtx, attemptID)
		if err != nil {
			return w.recordIncident(settleCtx, baseResult, attemptID, "UNKNOWN", err)
		}
		baseResult.Job = settled
		baseResult.Outcome = OutcomeACKED
		if settled.Task.State == transfer.ImportRunning && batch.endOffset == batch.stagingSize {
			finished, _, finishErr := w.repository.FinishImport(settleCtx, transfer.FinishImportRequest{
				JobID: settled.Task.ID, RunSegmentID: attemptRequest.RunSegmentID,
				CheckpointDigest:   settled.CheckpointDigest,
				StagingLogicalSize: strconv.FormatInt(batch.stagingSize, 10),
			})
			if finishErr != nil {
				return baseResult, finishErr
			}
			baseResult.Job = finished
			baseResult.Outcome = OutcomeSucceeded
		}
		return baseResult, nil
	case OutcomeTooLarge:
		settled, err := w.repository.SettleTooLarge(settleCtx, attemptID)
		if err != nil {
			return Result{}, err
		}
		baseResult.Job, baseResult.Outcome = settled, OutcomeTooLarge
		return baseResult, nil
	case OutcomePartial:
		return w.recordIncident(settleCtx, baseResult, attemptID, "PARTIAL", nil)
	case OutcomeRejected:
		settled, err := w.repository.SettleRejected(settleCtx, attemptID)
		if err != nil {
			return Result{}, err
		}
		baseResult.Job, baseResult.Outcome = settled, OutcomeRejected
		return baseResult, nil
	default:
		return w.recordIncident(settleCtx, baseResult, attemptID, "UNKNOWN", ErrResponseProtocol)
	}
}

func (w *Worker) recordIncident(
	ctx context.Context,
	result Result,
	attemptID, kind string,
	cause error,
) (Result, error) {
	job, err := w.repository.RecordIncident(ctx, attemptID, w.newID(), kind)
	if err != nil {
		if cause != nil {
			return Result{}, errors.Join(fmt.Errorf("%w: %v", ErrSettlementFailed, err), cause)
		}
		return Result{}, err
	}
	result.Job = job
	if kind == "PARTIAL" {
		result.Outcome = OutcomePartial
	} else {
		result.Outcome = OutcomeUnknown
	}
	return result, nil
}

func (w *Worker) classify(response *transport.Response, body []byte) Outcome {
	if response == nil {
		return OutcomeUnknown
	}
	inspection := inspectJSONBody(body)
	if response.StatusCode >= 500 {
		return OutcomeUnknown
	}
	if response.StatusCode == 413 {
		if inspection.partial {
			return OutcomePartial
		}
		if inspection.valid {
			return OutcomeTooLarge
		}
		return OutcomeUnknown
	}
	if response.StatusCode >= 400 && response.StatusCode <= 499 {
		if inspection.partial {
			return OutcomePartial
		}
		if inspection.valid {
			return OutcomeRejected
		}
		return OutcomeUnknown
	}
	if _, allowed := w.success[response.StatusCode]; allowed {
		if response.StatusCode == 204 && (len(body) != 0 || response.ContentLength > 0) {
			return OutcomeUnknown
		}
		if inspection.valid && !inspection.partial && !inspection.hasError {
			return OutcomeACKED
		}
	}
	return OutcomeUnknown
}

func readAndCloseResponse(response *transport.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, ErrResponseProtocol
	}
	limited := io.LimitReader(response.Body, maxResponseBody+1)
	body, readErr := io.ReadAll(limited)
	closeErr := response.Close()
	if readErr != nil || closeErr != nil {
		return nil, ErrResponseProtocol
	}
	if len(body) > maxResponseBody {
		return nil, ErrResponseTooLarge
	}
	return body, nil
}

type bodyInspection struct {
	valid    bool
	partial  bool
	hasError bool
}

func inspectJSONBody(body []byte) bodyInspection {
	if len(body) == 0 {
		return bodyInspection{valid: true}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return bodyInspection{}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return bodyInspection{}
	}
	if _, ok := root.(map[string]any); !ok {
		return bodyInspection{}
	}
	inspection := bodyInspection{valid: true}
	inspectResponseValue(root, &inspection)
	return inspection
}

func inspectResponseValue(value any, inspection *bodyInspection) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch strings.ToLower(key) {
			case "partial":
				flag, ok := child.(bool)
				if !ok {
					inspection.valid = false
				} else if flag {
					inspection.partial = true
				}
			case "error":
				if child != nil {
					message, ok := child.(string)
					if !ok {
						inspection.valid = false
					} else {
						inspection.hasError = true
						if strings.Contains(strings.ToLower(message), "partial write") {
							inspection.partial = true
						}
					}
				}
			}
			inspectResponseValue(child, inspection)
		}
	case []any:
		for _, child := range typed {
			inspectResponseValue(child, inspection)
		}
	}
}

type stagedBatch struct {
	payload       []byte
	payloadDigest string
	startOffset   int64
	endOffset     int64
	stagingSize   int64
	points        int
}

func readVerifiedBatch(ctx context.Context, path string, job transfer.ImportJob) (stagedBatch, error) {
	start, err := exactNonNegativeInt64(job.Checkpoint.LogicalOffset)
	if err != nil {
		return stagedBatch{}, ErrStagingOffset
	}
	maxPoints, err := boundedPositiveInt(job.Checkpoint.AdaptiveMaxPoints, MaxBatchPoints)
	if err != nil {
		return stagedBatch{}, ErrInvalidRequest
	}
	maxBytes, err := boundedPositiveInt64(job.Checkpoint.AdaptiveMaxBytes, MaxBatchBytes)
	if err != nil {
		return stagedBatch{}, ErrInvalidRequest
	}
	expectedHash, err := decodeSHA256(job.Checkpoint.StagingSHA256)
	if err != nil {
		return stagedBatch{}, ErrStagingIntegrity
	}

	pathInfo, err := os.Lstat(path)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() {
		return stagedBatch{}, ErrStagingIntegrity
	}
	file, err := os.Open(path)
	if err != nil {
		return stagedBatch{}, ErrStagingIntegrity
	}
	defer file.Close()
	openInfo, err := file.Stat()
	if err != nil || !openInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openInfo) {
		return stagedBatch{}, ErrStagingIntegrity
	}

	replay := job.OpenIncident != nil
	replayEnd := int64(0)
	if replay {
		if job.OpenIncident.ParentCheckpointDigest != job.CheckpointDigest {
			return stagedBatch{}, transfer.ErrCheckpointConflict
		}
		replayStart, offsetErr := exactNonNegativeInt64(job.OpenIncident.StartOffset)
		if offsetErr != nil || replayStart != start {
			return stagedBatch{}, ErrStagingOffset
		}
		replayEnd, err = exactNonNegativeInt64(job.OpenIncident.EndOffset)
		if err != nil || replayEnd <= start {
			return stagedBatch{}, ErrStagingOffset
		}
	}

	hash := sha256.New()
	reader := bufio.NewReaderSize(file, 64*1024)
	batch := stagedBatch{startOffset: start, endOffset: start}
	var cursor int64
	selectionClosed := false
	for {
		if err := contextError(ctx); err != nil {
			return stagedBatch{}, err
		}
		record, readErr := reader.ReadBytes('\n')
		if len(record) != 0 {
			if record[len(record)-1] != '\n' || len(record) == 1 ||
				int64(len(record)-1) > transfer.StageCanonicalPointLimitBytes {
				return stagedBatch{}, ErrStagingIntegrity
			}
			if _, err := hash.Write(record); err != nil {
				return stagedBatch{}, ErrStagingIntegrity
			}
			if int64(len(record)) > math.MaxInt64-cursor {
				return stagedBatch{}, ErrStagingIntegrity
			}
			recordStart := cursor
			cursor += int64(len(record))
			if recordStart < start && cursor > start {
				return stagedBatch{}, ErrStagingOffset
			}
			if recordStart == start || (recordStart > start && batch.points > 0 && !selectionClosed) {
				line := record[:len(record)-1]
				candidateSize := int64(len(line))
				if batch.points > 0 {
					candidateSize += int64(len(batch.payload)) + 1
				}
				allowed := replay || batch.points == 0 ||
					(batch.points < maxPoints && candidateSize <= maxBytes)
				if replay && cursor > replayEnd {
					return stagedBatch{}, ErrStagingOffset
				}
				if !replay && batch.points >= maxPoints {
					allowed = false
				}
				if !allowed {
					selectionClosed = true
				} else {
					if batch.points > 0 {
						batch.payload = append(batch.payload, '\n')
					}
					batch.payload = append(batch.payload, line...)
					batch.points++
					batch.endOffset = cursor
					if int64(len(batch.payload)) > MaxBatchBytes || batch.points > MaxBatchPoints {
						return stagedBatch{}, ErrStagingIntegrity
					}
					if replay && cursor == replayEnd {
						selectionClosed = true
					}
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return stagedBatch{}, ErrStagingIntegrity
		}
	}
	batch.stagingSize = cursor
	if cursor != openInfo.Size() {
		return stagedBatch{}, ErrStagingIntegrity
	}
	if start > cursor || (start < cursor && batch.points == 0) {
		return stagedBatch{}, ErrStagingOffset
	}
	if replay && batch.endOffset != replayEnd {
		return stagedBatch{}, ErrStagingOffset
	}
	if !bytes.Equal(hash.Sum(nil), expectedHash) {
		return stagedBatch{}, ErrStagingIntegrity
	}
	afterInfo, err := file.Stat()
	if err != nil || afterInfo.Size() != openInfo.Size() || !afterInfo.ModTime().Equal(openInfo.ModTime()) {
		return stagedBatch{}, ErrStagingIntegrity
	}
	if batch.points != 0 {
		digest := sha256.Sum256(batch.payload)
		batch.payloadDigest = hex.EncodeToString(digest[:])
		if replay && !strings.EqualFold(batch.payloadDigest, job.OpenIncident.PayloadDigest) {
			return stagedBatch{}, transfer.ErrCheckpointConflict
		}
	}
	return batch, nil
}

func decodeSHA256(value string) ([]byte, error) {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return nil, ErrStagingIntegrity
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return nil, ErrStagingIntegrity
	}
	return decoded, nil
}

func exactNonNegativeInt64(value string) (int64, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return 0, ErrInvalidRequest
	}
	number := new(big.Int)
	if _, ok := number.SetString(value, 10); !ok || number.Sign() < 0 || !number.IsInt64() {
		return 0, ErrInvalidRequest
	}
	return number.Int64(), nil
}

func boundedPositiveInt(value string, maximum int) (int, error) {
	number, err := exactNonNegativeInt64(value)
	if err != nil || number <= 0 {
		return 0, ErrInvalidRequest
	}
	if number > int64(maximum) {
		return maximum, nil
	}
	return int(number), nil
}

func boundedPositiveInt64(value string, maximum int64) (int64, error) {
	number, err := exactNonNegativeInt64(value)
	if err != nil || number <= 0 {
		return 0, ErrInvalidRequest
	}
	if number > maximum {
		return maximum, nil
	}
	return number, nil
}

func contextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
