package query

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

type ProtocolErrorCode string

const (
	ProtocolInvalidJSON       ProtocolErrorCode = "FAILED_PROTOCOL"
	ProtocolEmptyResponse     ProtocolErrorCode = "FAILED_EMPTY_RESPONSE"
	ProtocolTopLevelError     ProtocolErrorCode = "FAILED_SERVER"
	ProtocolIncomplete        ProtocolErrorCode = "FAILED_INCOMPLETE_RESPONSE"
	ProtocolStatementMismatch ProtocolErrorCode = "FAILED_PROTOCOL"
	ProtocolResultStorage     ProtocolErrorCode = "FAILED_RESULT_STORAGE"
)

// DecodeError separates a safe local message from transient server text.
type DecodeError struct {
	Code        ProtocolErrorCode
	SafeMessage string
	RawIssue    string
	Cause       error
}

func (e *DecodeError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.SafeMessage
}

func (e *DecodeError) Unwrap() error { return e.Cause }

type StatementSpec struct {
	TimeColumn string
}

type DecodeConfig struct {
	Statements []StatementSpec
	Resolver   NumericKindResolver
	// OnChunkDecoded runs only after one complete chunk has passed the
	// structural and scalar checks above. Export callers use it to refresh an
	// idle deadline without treating arbitrary response bytes as progress.
	OnChunkDecoded func()
	rowSink        decodedRowSink
}

// decodedRowSink lets the service retain the versioned row encoding without
// making the public decoder API depend on a storage implementation.
type decodedRowSink interface {
	AppendDecodedRow(statementID int, seriesID string, row []TypedScalar) (accepted, limitReached bool, err error)
}

type resultStorageError struct{ cause error }

func (e *resultStorageError) Error() string { return e.cause.Error() }
func (e *resultStorageError) Unwrap() error { return e.cause }

type ResultSet struct {
	Statements         []StatementResult
	StatementErrorText []string
	ChunkCount         int
	Truncated          bool
	store              *resultStore
}

type StatementResult struct {
	ID          int
	Series      []*Series
	HasError    bool
	Complete    bool
	seriesIndex map[string]*Series
}

type Series struct {
	ID          string
	StatementID int
	Measurement string
	Tags        map[string]string
	Columns     []string
	Rows        [][]TypedScalar
	RowCount    uint64
}

type rawEnvelope struct {
	Results []rawResult `json:"results"`
	Error   string      `json:"error"`
}

type rawResult struct {
	StatementID *int        `json:"statement_id"`
	Series      []rawSeries `json:"series"`
	Error       string      `json:"error"`
	Partial     bool        `json:"partial"`
}

type rawSeries struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]any           `json:"values"`
	Partial bool              `json:"partial"`
}

type statementDecodeState struct {
	seen   bool
	closed bool
}

// DecodeChunked consumes consecutive InfluxDB chunk objects. UseNumber is set
// before the first Decode, preserving every wire numeric token exactly.
func DecodeChunked(reader io.Reader, config DecodeConfig) (*ResultSet, error) {
	if reader == nil || len(config.Statements) == 0 {
		return nil, protocolError(ProtocolInvalidJSON, "查询响应解码配置无效", "", nil)
	}
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()

	resultSet := &ResultSet{
		Statements:         make([]StatementResult, len(config.Statements)),
		StatementErrorText: make([]string, len(config.Statements)),
	}
	states := make([]statementDecodeState, len(config.Statements))
	for i := range resultSet.Statements {
		resultSet.Statements[i] = StatementResult{ID: i, seriesIndex: make(map[string]*Series)}
	}

	currentStatement := -1
	for {
		var envelope rawEnvelope
		err := decoder.Decode(&envelope)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, protocolError(ProtocolInvalidJSON, "InfluxDB 返回了无效 JSON", "", err)
		}
		resultSet.ChunkCount++
		if envelope.Error != "" {
			return nil, protocolError(ProtocolTopLevelError, "InfluxDB 拒绝了查询", envelope.Error, nil)
		}
		if len(envelope.Results) != 1 {
			return nil, protocolError(ProtocolStatementMismatch, "查询分块必须且只能包含一个 result", "", nil)
		}

		wireResult := envelope.Results[0]
		if wireResult.StatementID == nil {
			return nil, protocolError(ProtocolStatementMismatch, "查询结果缺少 statement_id", "", nil)
		}
		statementID := *wireResult.StatementID
		if statementID < 0 || statementID >= len(states) {
			return nil, protocolError(ProtocolStatementMismatch, "查询返回了越界的 statement_id", "", nil)
		}
		if currentStatement == -1 && statementID != 0 {
			return nil, protocolError(ProtocolStatementMismatch, "查询结果缺少前置 statement", "", nil)
		}
		if currentStatement >= 0 {
			if statementID < currentStatement || statementID > currentStatement+1 {
				return nil, protocolError(ProtocolStatementMismatch, "statement_id 顺序无效", "", nil)
			}
			if statementID > currentStatement && !states[currentStatement].closed {
				return nil, protocolError(ProtocolIncomplete, "前一条 statement 的 partial 响应未结束", "", nil)
			}
		}
		if states[statementID].closed {
			return nil, protocolError(ProtocolStatementMismatch, "已结束的 statement 再次出现", "", nil)
		}
		currentStatement = statementID
		states[statementID].seen = true

		statement := &resultSet.Statements[statementID]
		if wireResult.Error != "" {
			if len(wireResult.Series) != 0 || wireResult.Partial {
				return nil, protocolError(ProtocolStatementMismatch, "statement error 与数据或 partial 同时出现", wireResult.Error, nil)
			}
			statement.HasError = true
			statement.Complete = true
			resultSet.StatementErrorText[statementID] = wireResult.Error
			states[statementID].closed = true
			if config.OnChunkDecoded != nil {
				config.OnChunkDecoded()
			}
			continue
		}

		partial := wireResult.Partial
		for _, wireSeries := range wireResult.Series {
			if wireSeries.Partial {
				partial = true
			}
			truncated, err := appendSeries(statement, wireSeries, config.Statements[statementID], config.Resolver, config.rowSink)
			if err != nil {
				var storageError *resultStorageError
				if errors.As(err, &storageError) {
					return nil, protocolError(ProtocolResultStorage, "查询结果无法安全保存", "", storageError)
				}
				return nil, protocolError(ProtocolInvalidJSON, "查询 series 结构或数值类型无效", "", err)
			}
			if truncated {
				resultSet.Truncated = true
				clearSeriesIndexes(resultSet)
				return resultSet, nil
			}
		}
		if !partial {
			statement.Complete = true
			states[statementID].closed = true
		}
		if config.OnChunkDecoded != nil {
			config.OnChunkDecoded()
		}
	}

	if resultSet.ChunkCount == 0 {
		return nil, protocolError(ProtocolEmptyResponse, "InfluxDB 返回了空响应", "", nil)
	}
	for i, state := range states {
		if !state.seen {
			return nil, protocolError(ProtocolStatementMismatch, "查询响应缺少 statement", "", nil)
		}
		if !state.closed {
			return nil, protocolError(ProtocolIncomplete, "查询在 partial 响应结束前中断", "", nil)
		}
		resultSet.Statements[i].seriesIndex = nil
	}
	return resultSet, nil
}

func appendSeries(
	statement *StatementResult,
	wire rawSeries,
	spec StatementSpec,
	resolver NumericKindResolver,
	sink decodedRowSink,
) (bool, error) {
	if len(wire.Columns) == 0 {
		return false, errors.New("series columns are empty")
	}
	seriesID := MakeSeriesID(statement.ID, wire.Name, wire.Tags)
	series := statement.seriesIndex[seriesID]
	if series == nil {
		series = &Series{
			ID:          seriesID,
			StatementID: statement.ID,
			Measurement: wire.Name,
			Tags:        cloneTags(wire.Tags),
			Columns:     append([]string(nil), wire.Columns...),
		}
		statement.seriesIndex[seriesID] = series
		statement.Series = append(statement.Series, series)
	} else if !equalStrings(series.Columns, wire.Columns) {
		return false, errors.New("series columns changed across chunks")
	}

	timeColumn := spec.TimeColumn
	if timeColumn == "" {
		timeColumn = "time"
	}
	for _, wireRow := range wire.Values {
		if len(wireRow) != len(series.Columns) {
			return false, errors.New("row width does not match columns")
		}
		row := make([]TypedScalar, len(wireRow))
		for i, value := range wireRow {
			kind := ScalarNumericText
			if series.Columns[i] == timeColumn {
				if _, ok := value.(json.Number); !ok {
					return false, errors.New("nanosecond timestamp is not a JSON number")
				}
				kind = ScalarTimestampNS
			} else if _, ok := value.(json.Number); ok && resolver != nil {
				resolved, known := resolver.ResolveNumeric(NumericContext{
					StatementID: statement.ID,
					Measurement: wire.Name,
					Tags:        cloneTags(wire.Tags),
					Column:      series.Columns[i],
				})
				if known {
					kind = resolved
				}
			}
			scalar, err := scalarFromValue(value, kind)
			if err != nil {
				return false, fmt.Errorf("column %q: %w", series.Columns[i], err)
			}
			row[i] = scalar
		}
		if sink == nil {
			series.Rows = append(series.Rows, row)
			series.RowCount++
			continue
		}
		accepted, limitReached, err := sink.AppendDecodedRow(statement.ID, series.ID, row)
		if err != nil {
			return false, &resultStorageError{cause: err}
		}
		if accepted {
			series.RowCount++
		}
		if limitReached {
			return true, nil
		}
	}
	return false, nil
}

func clearSeriesIndexes(result *ResultSet) {
	for i := range result.Statements {
		result.Statements[i].seriesIndex = nil
	}
}

// MakeSeriesID uses an unambiguous length-prefixed identity: statement,
// measurement, then tags sorted by key.
func MakeSeriesID(statementID int, measurement string, tags map[string]string) string {
	hash := sha256.New()
	writeLengthPrefixed(hash, []byte(fmt.Sprintf("%d", statementID)))
	writeLengthPrefixed(hash, []byte(measurement))
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeLengthPrefixed(hash, []byte(key))
		writeLengthPrefixed(hash, []byte(tags[key]))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func writeLengthPrefixed(writer io.Writer, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func protocolError(code ProtocolErrorCode, safe, raw string, cause error) *DecodeError {
	return &DecodeError{Code: code, SafeMessage: safe, RawIssue: raw, Cause: cause}
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	clone := make(map[string]string, len(tags))
	for key, value := range tags {
		clone[key] = value
	}
	return clone
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
