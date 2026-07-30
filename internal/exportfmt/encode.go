package exportfmt

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdesk/influxdesk/internal/query"
)

var (
	ErrAmbiguousNumeric = errors.New("NUMERIC_TYPE_AMBIGUOUS")
	ErrInvalidPoint     = errors.New("INVALID_LINE_PROTOCOL_POINT")
)

type TypedRow map[string]query.TypedScalar

type JSONLScalar struct {
	Kind        query.ScalarKind `json:"kind"`
	DecimalText string           `json:"decimalText,omitempty"`
	Value       any              `json:"value,omitempty"`
}

// WriteCSV writes authoritative scalar text. A caller that needs type
// preservation must persist the separate schema sidecar alongside this file.
func WriteCSV(writer io.Writer, columns []string, rows [][]query.TypedScalar) error {
	if writer == nil || len(columns) == 0 {
		return errors.New("CSV columns and writer are required")
	}
	csvWriter := csv.NewWriter(writer)
	if err := csvWriter.Write(columns); err != nil {
		return err
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return errors.New("CSV row width does not match columns")
		}
		values := make([]string, len(row))
		for i, scalar := range row {
			value, err := scalarText(scalar)
			if err != nil {
				return err
			}
			values[i] = value
		}
		if err := csvWriter.Write(values); err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return csvWriter.Error()
}

// WriteTypedJSONL emits every numeric cell as {kind,decimalText}; numeric
// values are never serialized as JSON numbers.
func WriteTypedJSONL(writer io.Writer, rows []TypedRow) error {
	if writer == nil {
		return errors.New("JSONL writer is required")
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		encoded := make(map[string]JSONLScalar, len(row))
		for column, scalar := range row {
			value, err := jsonlScalar(scalar)
			if err != nil {
				return err
			}
			encoded[column] = value
		}
		if err := encoder.Encode(encoded); err != nil {
			return err
		}
	}
	return nil
}

// EncodeLPPoint creates one canonical, explicit-nanosecond LP line.
func EncodeLPPoint(measurement string, tags map[string]string, fields map[string]query.TypedScalar, timestamp query.TypedScalar) (string, error) {
	if measurement == "" || strings.ContainsAny(measurement, "\r\n") || len(fields) == 0 {
		return "", ErrInvalidPoint
	}
	if timestamp.Kind != query.ScalarTimestampNS || timestamp.DecimalText == "" {
		return "", ErrInvalidPoint
	}
	if _, err := strconv.ParseInt(timestamp.DecimalText, 10, 64); err != nil {
		return "", ErrInvalidPoint
	}

	var builder strings.Builder
	builder.WriteString(escapeMeasurement(measurement))
	tagKeys := sortedKeys(tags)
	for _, key := range tagKeys {
		if key == "" || strings.ContainsAny(key, "\r\n") || strings.ContainsAny(tags[key], "\r\n") {
			return "", ErrInvalidPoint
		}
		builder.WriteByte(',')
		builder.WriteString(escapeTag(key))
		builder.WriteByte('=')
		builder.WriteString(escapeTag(tags[key]))
	}
	builder.WriteByte(' ')
	fieldKeys := make([]string, 0, len(fields))
	for key := range fields {
		fieldKeys = append(fieldKeys, key)
	}
	sort.Strings(fieldKeys)
	for i, key := range fieldKeys {
		if key == "" || strings.ContainsAny(key, "\r\n") {
			return "", ErrInvalidPoint
		}
		value, err := lpField(fields[key])
		if err != nil {
			return "", err
		}
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(escapeFieldKey(key))
		builder.WriteByte('=')
		builder.WriteString(value)
	}
	builder.WriteByte(' ')
	builder.WriteString(timestamp.DecimalText)
	return builder.String(), nil
}

func scalarText(value query.TypedScalar) (string, error) {
	switch value.Kind {
	case query.ScalarNull:
		return "", nil
	case query.ScalarString:
		return value.StringValue, nil
	case query.ScalarBoolean:
		if value.BooleanValue == nil {
			return "", errors.New("boolean scalar has no value")
		}
		return strconv.FormatBool(*value.BooleanValue), nil
	case query.ScalarTimestampNS, query.ScalarInt64, query.ScalarUint64, query.ScalarFloat64, query.ScalarNumericText:
		if value.DecimalText == "" {
			return "", errors.New("numeric scalar has no decimalText")
		}
		return value.DecimalText, nil
	default:
		return "", fmt.Errorf("unknown scalar kind %q", value.Kind)
	}
}

func jsonlScalar(value query.TypedScalar) (JSONLScalar, error) {
	result := JSONLScalar{Kind: value.Kind}
	switch value.Kind {
	case query.ScalarNull:
		return result, nil
	case query.ScalarString:
		result.Value = value.StringValue
	case query.ScalarBoolean:
		if value.BooleanValue == nil {
			return JSONLScalar{}, errors.New("boolean scalar has no value")
		}
		result.Value = *value.BooleanValue
	case query.ScalarTimestampNS, query.ScalarInt64, query.ScalarUint64, query.ScalarFloat64, query.ScalarNumericText:
		if value.DecimalText == "" {
			return JSONLScalar{}, errors.New("numeric scalar has no decimalText")
		}
		result.DecimalText = value.DecimalText
	default:
		return JSONLScalar{}, fmt.Errorf("unknown scalar kind %q", value.Kind)
	}
	return result, nil
}

func lpField(value query.TypedScalar) (string, error) {
	switch value.Kind {
	case query.ScalarString:
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value.StringValue)
		return `"` + escaped + `"`, nil
	case query.ScalarBoolean:
		if value.BooleanValue == nil {
			return "", ErrInvalidPoint
		}
		return strconv.FormatBool(*value.BooleanValue), nil
	case query.ScalarInt64:
		if _, err := strconv.ParseInt(value.DecimalText, 10, 64); err != nil {
			return "", ErrInvalidPoint
		}
		return value.DecimalText + "i", nil
	case query.ScalarUint64:
		if _, err := strconv.ParseUint(value.DecimalText, 10, 64); err != nil {
			return "", ErrInvalidPoint
		}
		return value.DecimalText + "u", nil
	case query.ScalarFloat64:
		number, err := strconv.ParseFloat(value.DecimalText, 64)
		if err != nil {
			return "", ErrInvalidPoint
		}
		return strconv.FormatFloat(number, 'g', -1, 64), nil
	case query.ScalarNumericText:
		return "", ErrAmbiguousNumeric
	default:
		return "", ErrInvalidPoint
	}
}

func escapeMeasurement(value string) string {
	return strings.NewReplacer(`\`, `\\`, `,`, `\,`, ` `, `\ `).Replace(value)
}

func escapeTag(value string) string {
	return strings.NewReplacer(`\`, `\\`, `,`, `\,`, `=`, `\=`, ` `, `\ `).Replace(value)
}

func escapeFieldKey(value string) string { return escapeTag(value) }

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
