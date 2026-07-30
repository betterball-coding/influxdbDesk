package query

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CSVExportRequest struct {
	SessionID   string
	StatementID int
	SeriesID    string
	AllRows     bool
	RowIndexes  []uint64
}

func (s *Service) WriteResultCSV(
	ctx context.Context,
	request CSVExportRequest,
	output io.Writer,
	location *time.Location,
) (uint64, error) {
	if output == nil || request.SessionID == "" || request.SeriesID == "" || request.StatementID < 0 {
		return 0, errors.New("INVALID_QUERY_RESULT_EXPORT")
	}
	if !request.AllRows && len(request.RowIndexes) == 0 {
		return 0, errors.New("QUERY_RESULT_EXPORT_EMPTY_SELECTION")
	}
	if location == nil {
		location = time.Local
	}

	series, err := s.ListResultSeries(request.SessionID, request.StatementID)
	if err != nil {
		return 0, err
	}
	var selectedSeries *SeriesSummary
	for index := range series {
		if series[index].ID == request.SeriesID {
			selectedSeries = &series[index]
			break
		}
	}
	if selectedSeries == nil {
		return 0, errors.New("QUERY_RESULT_EXPORT_SERIES_NOT_FOUND")
	}
	totalRows, err := strconv.ParseUint(selectedSeries.Rows, 10, 64)
	if err != nil {
		return 0, errors.New("QUERY_RESULT_EXPORT_INVALID_ROW_COUNT")
	}

	selectedRows := make(map[uint64]struct{}, len(request.RowIndexes))
	if !request.AllRows {
		indexes := append([]uint64(nil), request.RowIndexes...)
		sort.Slice(indexes, func(left, right int) bool { return indexes[left] < indexes[right] })
		for index, rowIndex := range indexes {
			if rowIndex >= totalRows || index > 0 && rowIndex == indexes[index-1] {
				return 0, errors.New("QUERY_RESULT_EXPORT_INVALID_SELECTION")
			}
			selectedRows[rowIndex] = struct{}{}
		}
	}

	if _, err := io.WriteString(output, "\ufeff"); err != nil {
		return 0, err
	}
	writer := csv.NewWriter(output)
	writer.UseCRLF = true
	if err := writer.Write(selectedSeries.Columns); err != nil {
		return 0, err
	}

	var (
		cursor  string
		offset  uint64
		written uint64
		wanted  = uint64(len(selectedRows))
	)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := s.GetResultPage(PageRequest{
			SessionID: request.SessionID, StatementID: request.StatementID,
			SeriesID: request.SeriesID, Cursor: cursor, Limit: 5000,
		})
		if err != nil {
			return 0, err
		}
		for _, row := range page.Rows {
			_, chosen := selectedRows[offset]
			if request.AllRows || chosen {
				record := make([]string, len(selectedSeries.Columns))
				for column := range record {
					if column < len(row) {
						value, err := csvScalarText(row[column], location)
						if err != nil {
							return 0, err
						}
						record[column] = value
					}
				}
				if err := writer.Write(record); err != nil {
					return 0, err
				}
				written++
			}
			offset++
		}
		if !request.AllRows && written == wanted || page.EOF {
			break
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			return 0, errors.New("QUERY_RESULT_EXPORT_CURSOR_STALLED")
		}
		cursor = page.NextCursor
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return 0, err
	}
	if !request.AllRows && written != wanted {
		return 0, errors.New("QUERY_RESULT_EXPORT_SELECTION_MISSING")
	}
	return written, nil
}

func csvScalarText(value TypedScalar, location *time.Location) (string, error) {
	switch value.Kind {
	case ScalarNull:
		return "", nil
	case ScalarString:
		return value.StringValue, nil
	case ScalarBoolean:
		if value.BooleanValue == nil {
			return "", errors.New("INVALID_TYPED_SCALAR")
		}
		return strconv.FormatBool(*value.BooleanValue), nil
	case ScalarTimestampNS:
		nanoseconds, err := strconv.ParseInt(value.DecimalText, 10, 64)
		if err != nil {
			return "", errors.New("INVALID_TYPED_SCALAR")
		}
		instant := time.Unix(0, nanoseconds).In(location)
		formatted := instant.Format("2006-01-02 15:04:05")
		if fraction := instant.Nanosecond(); fraction != 0 {
			formatted += "." + strings.TrimRight(strconv.Itoa(fraction + 1_000_000_000)[1:], "0")
		}
		return formatted, nil
	case ScalarInt64, ScalarUint64, ScalarFloat64, ScalarNumericText:
		if value.DecimalText == "" {
			return "", errors.New("INVALID_TYPED_SCALAR")
		}
		return value.DecimalText, nil
	default:
		return "", errors.New("INVALID_TYPED_SCALAR")
	}
}
