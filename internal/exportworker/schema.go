package exportworker

import (
	"errors"
	"math/big"
	"sort"
	"strings"

	"github.com/influxdesk/influxdesk/internal/exportfmt"
	"github.com/influxdesk/influxdesk/internal/query"
)

func parseTagKeys(result *query.ResultSet, measurement string) (map[string]struct{}, error) {
	statement, err := oneSuccessfulStatement(result)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for _, series := range statement.Series {
		if series.Measurement != measurement || len(series.Tags) != 0 ||
			len(series.Columns) != 1 || series.Columns[0] != "tagKey" {
			return nil, ErrSchemaDrift
		}
		for _, row := range series.Rows {
			if len(row) != 1 || row[0].Kind != query.ScalarString || !validSchemaName(row[0].StringValue) {
				return nil, ErrSchemaDrift
			}
			keys[row[0].StringValue] = struct{}{}
		}
	}
	return keys, nil
}

func parseFieldKeys(result *query.ResultSet, measurement string) (map[string]query.ScalarKind, error) {
	statement, err := oneSuccessfulStatement(result)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]query.ScalarKind)
	for _, series := range statement.Series {
		if series.Measurement != measurement || len(series.Tags) != 0 ||
			len(series.Columns) != 2 || series.Columns[0] != "fieldKey" || series.Columns[1] != "fieldType" {
			return nil, ErrSchemaDrift
		}
		for _, row := range series.Rows {
			if len(row) != 2 || row[0].Kind != query.ScalarString || row[1].Kind != query.ScalarString ||
				!validSchemaName(row[0].StringValue) {
				return nil, ErrSchemaDrift
			}
			kind, ok := influxFieldKind(row[1].StringValue)
			if !ok {
				return nil, ErrSchemaDrift
			}
			if existing, duplicate := fields[row[0].StringValue]; duplicate && existing != kind {
				return nil, ErrSchemaDrift
			}
			fields[row[0].StringValue] = kind
		}
	}
	if len(fields) == 0 {
		return nil, ErrSchemaDrift
	}
	return fields, nil
}

func influxFieldKind(value string) (query.ScalarKind, bool) {
	switch value {
	case "float":
		return query.ScalarFloat64, true
	case "integer":
		return query.ScalarInt64, true
	case "unsigned":
		return query.ScalarUint64, true
	case "string":
		return query.ScalarString, true
	case "boolean":
		return query.ScalarBoolean, true
	default:
		return "", false
	}
}

func oneSuccessfulStatement(result *query.ResultSet) (*query.StatementResult, error) {
	if result == nil || result.Truncated || len(result.Statements) != 1 ||
		!result.Statements[0].Complete || result.Statements[0].HasError {
		return nil, ErrSchemaDrift
	}
	return &result.Statements[0], nil
}

func validateDataResult(
	result *query.ResultSet,
	measurement string,
	startNS string,
	endNS string,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) error {
	statement, err := oneSuccessfulStatement(result)
	if err != nil {
		return err
	}
	var expectedColumns []string
	for _, series := range statement.Series {
		if series.Measurement != measurement {
			return ErrSchemaDrift
		}
		for tag := range series.Tags {
			if _, found := tagKeys[tag]; !found {
				return ErrUnknownTag
			}
			if _, collision := fieldTypes[tag]; collision {
				return ErrTagFieldConflict
			}
		}
		fields, err := validateColumns(series.Columns, tagKeys, fieldTypes)
		if err != nil {
			return err
		}
		if expectedColumns == nil {
			expectedColumns = fields
		} else if !equalStringSlices(expectedColumns, fields) {
			return ErrSchemaDrift
		}
		for _, row := range series.Rows {
			if err := validateRow(series.Columns, row, startNS, endNS, fieldTypes); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateColumns(
	columns []string,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) ([]string, error) {
	seen := make(map[string]struct{}, len(columns))
	fields := make([]string, 0, len(columns))
	timeColumns := 0
	for _, column := range columns {
		if _, duplicate := seen[column]; duplicate {
			return nil, ErrSchemaDrift
		}
		seen[column] = struct{}{}
		if column == "time" {
			timeColumns++
			continue
		}
		if _, tag := tagKeys[column]; tag {
			return nil, ErrTagFieldConflict
		}
		if _, field := fieldTypes[column]; !field {
			return nil, ErrUnknownField
		}
		fields = append(fields, column)
	}
	if timeColumns != 1 || len(fields) == 0 {
		return nil, ErrSchemaDrift
	}
	sort.Strings(fields)
	return fields, nil
}

func validateRow(
	columns []string,
	row []query.TypedScalar,
	startNS string,
	endNS string,
	fieldTypes map[string]query.ScalarKind,
) error {
	if len(columns) != len(row) {
		return ErrSchemaDrift
	}
	fields := 0
	for index, column := range columns {
		value := row[index]
		if column == "time" {
			if value.Kind != query.ScalarTimestampNS {
				return ErrSchemaDrift
			}
			timestamp, start, end := new(big.Int), new(big.Int), new(big.Int)
			if _, ok := timestamp.SetString(value.DecimalText, 10); !ok {
				return ErrSchemaDrift
			}
			start.SetString(startNS, 10)
			end.SetString(endNS, 10)
			if timestamp.Cmp(start) < 0 || timestamp.Cmp(end) >= 0 {
				return ErrSchemaDrift
			}
			continue
		}
		if value.Kind == query.ScalarNull {
			continue
		}
		if expected := fieldTypes[column]; value.Kind != expected {
			return ErrSchemaDrift
		}
		fields++
	}
	if fields == 0 {
		return ErrSchemaDrift
	}
	return nil
}

func validSchemaName(value string) bool {
	return value != "" && !strings.ContainsAny(value, "\r\n\x00")
}

func rowToLP(
	series *query.Series,
	row []query.TypedScalar,
	measurement string,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) (string, error) {
	if series == nil || len(series.Columns) != len(row) {
		return "", ErrSchemaDrift
	}
	fields := make(map[string]query.TypedScalar)
	var timestamp query.TypedScalar
	for index, column := range series.Columns {
		value := row[index]
		if column == "time" {
			timestamp = value
			continue
		}
		if _, tag := tagKeys[column]; tag {
			return "", ErrTagFieldConflict
		}
		if _, known := fieldTypes[column]; !known {
			return "", ErrUnknownField
		}
		if value.Kind != query.ScalarNull {
			fields[column] = value
		}
	}
	line, err := exportfmt.EncodeLPPoint(measurement, series.Tags, fields, timestamp)
	if err != nil {
		if errors.Is(err, exportfmt.ErrAmbiguousNumeric) {
			return "", ErrSchemaDrift
		}
		return "", err
	}
	return line, nil
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
