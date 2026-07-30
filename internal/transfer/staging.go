package transfer

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math"
	"strconv"
)

const (
	StageCanonicalPointLimitBytes int64 = 5 * 1024 * 1024
	StageGzipLimitBytes           int64 = 20 * 1024 * 1024 * 1024
)

type ImportStageFormat string

const (
	ImportStageLP         ImportStageFormat = "LP"
	ImportStageTXT        ImportStageFormat = "TXT"
	ImportStageLPGzip     ImportStageFormat = "LP_GZ"
	ImportStageTypedJSONL ImportStageFormat = "TYPED_JSONL_V1"
	ImportStageCSV        ImportStageFormat = "CSV"
)

var (
	ErrStageInvalidRequest     = errors.New("IMPORT_STAGE_INVALID_REQUEST")
	ErrStageInvalidInput       = errors.New("IMPORT_STAGE_INVALID_INPUT")
	ErrStageInvalidGzip        = errors.New("IMPORT_STAGE_INVALID_GZIP")
	ErrStageGzipLimit          = errors.New("IMPORT_STAGE_GZIP_LIMIT_EXCEEDED")
	ErrStagePointTooLarge      = errors.New("IMPORT_STAGE_POINT_TOO_LARGE")
	ErrStageMissingTimestamp   = errors.New("IMPORT_STAGE_TIMESTAMP_REQUIRED")
	ErrStageAmbiguousNumeric   = errors.New("IMPORT_STAGE_NUMERIC_TYPE_REQUIRED")
	ErrStageReservation        = errors.New("IMPORT_STAGE_RESERVATION_FAILED")
	ErrStageSettlement         = errors.New("IMPORT_STAGE_RESERVATION_SETTLEMENT_FAILED")
	ErrStageWrite              = errors.New("IMPORT_STAGE_WRITE_FAILED")
	ErrStageRead               = errors.New("IMPORT_STAGE_READ_FAILED")
	ErrStageCanceled           = errors.New("IMPORT_STAGE_CANCELED")
	ErrStageCSVMappingRequired = errors.New("IMPORT_STAGE_CSV_MAPPING_REQUIRED")
	ErrStageCSVMappingInvalid  = errors.New("IMPORT_STAGE_CSV_MAPPING_INVALID")
	ErrStageEmpty              = errors.New("IMPORT_STAGE_EMPTY")
)

var errStageGzipLimitInternal = errors.New("stage gzip limit")

type StageReservationFunc func(context.Context, int64) error
type StageSettlementFunc func(context.Context, int64, int64) error

type NumericTextKind string

const (
	NumericTextAsInt64   NumericTextKind = "int64"
	NumericTextAsUint64  NumericTextKind = "uint64"
	NumericTextAsFloat64 NumericTextKind = "float64"
)

type NumericTextResolver func(measurement, field string) (NumericTextKind, bool)

type StageImportRequest struct {
	Format      ImportStageFormat
	Source      io.Reader
	Destination io.Writer
	Reserve     StageReservationFunc
	// SettleReservation atomically replaces the conservative reserved amount
	// with the successful staging file's actual logical length.
	SettleReservation StageSettlementFunc

	NumericTextResolver NumericTextResolver
	CSVMapping          *CSVMapping

	// Tests and constrained callers may lower, but never raise, these limits.
	GzipUncompressedLimitBytes int64
	ReservationUnitBytes       int64
}

type StageImportResult struct {
	SourceSHA256  string `json:"sourceSha256"`
	StagingSHA256 string `json:"stagingSha256"`
	LogicalBytes  string `json:"logicalBytes"`
	ReservedBytes string `json:"reservedBytes"`
	PointCount    string `json:"pointCount"`
}

// StageImport streams an import source into canonical, explicit-nanosecond LP.
// It never creates files; the caller owns the protected destination lifecycle.
func StageImport(ctx context.Context, request StageImportRequest) (StageImportResult, error) {
	if ctx == nil || request.Source == nil || request.Destination == nil || request.Reserve == nil ||
		request.SettleReservation == nil {
		return StageImportResult{}, ErrStageInvalidRequest
	}
	if err := stageContextError(ctx); err != nil {
		return StageImportResult{}, err
	}
	extent := request.ReservationUnitBytes
	if extent <= 0 || extent > ReservationExtentBytes {
		extent = ReservationExtentBytes
	}
	sourceHash := sha256.New()
	stagingHash := sha256.New()
	destination := &stageExtentWriter{
		ctx: ctx, destination: request.Destination, reserve: request.Reserve,
		extent: extent, digest: stagingHash,
	}
	processor := stageProcessor{writer: destination}
	hashedSource := io.TeeReader(request.Source, sourceHash)

	var err error
	switch request.Format {
	case ImportStageLP, ImportStageTXT:
		err = processor.processLP(ctx, bufio.NewReaderSize(hashedSource, 64*1024), false)
		if err != nil && !isSafeStageError(err) {
			err = ErrStageRead
		}
	case ImportStageLPGzip:
		err = stageGzipLP(ctx, hashedSource, request.GzipUncompressedLimitBytes, &processor)
	case ImportStageTypedJSONL:
		err = processor.processTypedJSONL(ctx, bufio.NewReaderSize(hashedSource, 64*1024), request.NumericTextResolver)
		if err != nil && !isSafeStageError(err) {
			err = ErrStageRead
		}
	case ImportStageCSV:
		if request.CSVMapping == nil {
			err = ErrStageCSVMappingRequired
		} else {
			err = processor.processCSV(ctx, hashedSource, *request.CSVMapping)
			if err != nil && !isSafeStageError(err) {
				err = ErrStageRead
			}
		}
	default:
		err = ErrStageInvalidRequest
	}
	if err != nil {
		return StageImportResult{}, err
	}
	if processor.points == 0 {
		return StageImportResult{}, ErrStageEmpty
	}
	if err := stageContextError(ctx); err != nil {
		return StageImportResult{}, err
	}
	if err := request.SettleReservation(ctx, destination.reserved, destination.written); err != nil {
		return StageImportResult{}, ErrStageSettlement
	}
	return StageImportResult{
		SourceSHA256:  hex.EncodeToString(sourceHash.Sum(nil)),
		StagingSHA256: hex.EncodeToString(stagingHash.Sum(nil)),
		LogicalBytes:  strconv.FormatInt(destination.written, 10),
		ReservedBytes: strconv.FormatInt(destination.reserved, 10),
		PointCount:    strconv.FormatInt(processor.points, 10),
	}, nil
}

type stageProcessor struct {
	writer *stageExtentWriter
	points int64
}

func (p *stageProcessor) writePoint(ctx context.Context, canonical string) error {
	if err := stageContextError(ctx); err != nil {
		return err
	}
	if int64(len(canonical)) > StageCanonicalPointLimitBytes {
		return ErrStagePointTooLarge
	}
	line := make([]byte, len(canonical)+1)
	copy(line, canonical)
	line[len(canonical)] = '\n'
	if _, err := p.writer.Write(line); err != nil {
		return err
	}
	if p.points == math.MaxInt64 {
		return ErrStageInvalidInput
	}
	p.points++
	return nil
}

type stageExtentWriter struct {
	ctx         context.Context
	destination io.Writer
	reserve     StageReservationFunc
	extent      int64
	digest      hash.Hash
	written     int64
	reserved    int64
}

func (w *stageExtentWriter) Write(payload []byte) (int, error) {
	if err := stageContextError(w.ctx); err != nil {
		return 0, err
	}
	if int64(len(payload)) > math.MaxInt64-w.written {
		return 0, ErrStageWrite
	}
	needed := w.written + int64(len(payload))
	for needed > w.reserved {
		if err := w.reserve(w.ctx, w.extent); err != nil {
			return 0, ErrStageReservation
		}
		if w.reserved > math.MaxInt64-w.extent {
			return 0, ErrStageReservation
		}
		w.reserved += w.extent
	}
	if err := stageContextError(w.ctx); err != nil {
		return 0, err
	}
	written, err := w.destination.Write(payload)
	if written < 0 || written > len(payload) {
		return 0, ErrStageWrite
	}
	if written > 0 {
		_, _ = w.digest.Write(payload[:written])
		w.written += int64(written)
	}
	if err != nil || written != len(payload) {
		return written, ErrStageWrite
	}
	return written, nil
}

func stageGzipLP(ctx context.Context, source io.Reader, requestedLimit int64, processor *stageProcessor) error {
	limit := requestedLimit
	if limit <= 0 || limit > StageGzipLimitBytes {
		limit = StageGzipLimitBytes
	}
	buffered := bufio.NewReaderSize(source, 64*1024)
	decoder, err := gzip.NewReader(buffered)
	if err != nil {
		return ErrStageInvalidGzip
	}
	decoder.Multistream(false)
	limited := &stageGzipLimitReader{reader: decoder, remaining: limit}
	err = processor.processLP(ctx, bufio.NewReaderSize(limited, 64*1024), true)
	if err != nil {
		_ = decoder.Close()
		if errors.Is(err, errStageGzipLimitInternal) {
			return ErrStageGzipLimit
		}
		if isSafeStageError(err) {
			return err
		}
		return ErrStageInvalidGzip
	}
	if err := decoder.Close(); err != nil {
		return ErrStageInvalidGzip
	}
	if _, err := buffered.Peek(1); !errors.Is(err, io.EOF) {
		return ErrStageInvalidGzip
	}
	return nil
}

type stageGzipLimitReader struct {
	reader    io.Reader
	remaining int64
}

func (r *stageGzipLimitReader) Read(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	if r.remaining > 0 {
		if int64(len(payload)) > r.remaining {
			payload = payload[:r.remaining]
		}
		read, err := r.reader.Read(payload)
		r.remaining -= int64(read)
		return read, err
	}
	var probe [1]byte
	read, err := r.reader.Read(probe[:])
	if read != 0 {
		return 0, errStageGzipLimitInternal
	}
	return 0, err
}

func readStageRecord(ctx context.Context, reader *bufio.Reader, maximum int) ([]byte, error) {
	buffer := make([]byte, 0, min(maximum, 64*1024))
	for {
		if err := stageContextError(ctx); err != nil {
			return nil, err
		}
		fragment, err := reader.ReadSlice('\n')
		buffer = append(buffer, fragment...)
		if len(buffer) > maximum+2 {
			return nil, ErrStagePointTooLarge
		}
		switch {
		case err == nil:
			return trimStageLineEnding(buffer, maximum)
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(buffer) == 0 {
				return nil, io.EOF
			}
			return trimStageLineEnding(buffer, maximum)
		default:
			return nil, err
		}
	}
}

func trimStageLineEnding(line []byte, maximum int) ([]byte, error) {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) > maximum {
		return nil, ErrStagePointTooLarge
	}
	return line, nil
}

func stageContextError(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ErrStageCanceled
	default:
		return nil
	}
}

func isSafeStageError(err error) bool {
	return errors.Is(err, ErrStageInvalidRequest) || errors.Is(err, ErrStageInvalidInput) ||
		errors.Is(err, ErrStageInvalidGzip) || errors.Is(err, ErrStageGzipLimit) ||
		errors.Is(err, ErrStagePointTooLarge) || errors.Is(err, ErrStageMissingTimestamp) ||
		errors.Is(err, ErrStageAmbiguousNumeric) || errors.Is(err, ErrStageReservation) ||
		errors.Is(err, ErrStageSettlement) || errors.Is(err, ErrStageWrite) || errors.Is(err, ErrStageRead) ||
		errors.Is(err, ErrStageCanceled) || errors.Is(err, ErrStageCSVMappingRequired) ||
		errors.Is(err, ErrStageCSVMappingInvalid) || errors.Is(err, ErrStageEmpty)
}
