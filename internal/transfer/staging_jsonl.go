package transfer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"strconv"
)

type stageJSONPoint struct {
	SchemaVersion int                        `json:"schemaVersion"`
	Measurement   string                     `json:"measurement"`
	Tags          map[string]string          `json:"tags"`
	Fields        map[string]json.RawMessage `json:"fields"`
	Timestamp     json.RawMessage            `json:"timestamp"`
}

type stageJSONScalar struct {
	Kind        string          `json:"kind"`
	DecimalText *string         `json:"decimalText,omitempty"`
	Value       json.RawMessage `json:"value,omitempty"`
}

func (p *stageProcessor) processTypedJSONL(
	ctx context.Context,
	reader *bufio.Reader,
	resolve NumericTextResolver,
) error {
	maximumInput := int(StageCanonicalPointLimitBytes * 4)
	for {
		line, err := readStageRecord(ctx, reader, maximumInput)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(line) == 0 {
			continue
		}
		canonical, err := parseTypedJSONLPoint(line, resolve)
		if err != nil {
			return err
		}
		if err := p.writePoint(ctx, canonical); err != nil {
			return err
		}
	}
}

func parseTypedJSONLPoint(line []byte, resolve NumericTextResolver) (string, error) {
	var point stageJSONPoint
	if err := decodeStrictStageJSON(line, &point); err != nil {
		return "", ErrStageInvalidInput
	}
	if point.SchemaVersion != 1 || !validStageText(point.Measurement) || len(point.Fields) == 0 {
		return "", ErrStageInvalidInput
	}
	if len(point.Timestamp) == 0 {
		return "", ErrStageMissingTimestamp
	}
	for key, value := range point.Tags {
		if !validStageText(key) || !validStageText(value) {
			return "", ErrStageInvalidInput
		}
	}
	timestamp, err := parseStageJSONTimestamp(point.Timestamp)
	if err != nil {
		return "", err
	}
	fields := make(map[string]string, len(point.Fields))
	for field, encoded := range point.Fields {
		if !validStageText(field) {
			return "", ErrStageInvalidInput
		}
		value, err := parseStageJSONField(point.Measurement, field, encoded, resolve)
		if err != nil {
			return "", err
		}
		fields[field] = value
	}
	return encodeCanonicalStagePoint(point.Measurement, point.Tags, fields, timestamp)
}

func parseStageJSONTimestamp(encoded json.RawMessage) (string, error) {
	scalar, err := decodeStageJSONScalar(encoded)
	if err != nil || scalar.Kind != "timestamp_ns" || scalar.DecimalText == nil || len(scalar.Value) != 0 {
		return "", ErrStageMissingTimestamp
	}
	timestamp, err := strconv.ParseInt(*scalar.DecimalText, 10, 64)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	return strconv.FormatInt(timestamp, 10), nil
}

func parseStageJSONField(
	measurement, field string,
	encoded json.RawMessage,
	resolve NumericTextResolver,
) (string, error) {
	scalar, err := decodeStageJSONScalar(encoded)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	switch scalar.Kind {
	case "string":
		if scalar.DecimalText != nil || len(scalar.Value) == 0 {
			return "", ErrStageInvalidInput
		}
		var value string
		if err := decodeStrictStageJSON(scalar.Value, &value); err != nil || !validStageStringValue(value) {
			return "", ErrStageInvalidInput
		}
		return encodeStageString(value), nil
	case "boolean":
		if scalar.DecimalText != nil || len(scalar.Value) == 0 {
			return "", ErrStageInvalidInput
		}
		var value bool
		if err := decodeStrictStageJSON(scalar.Value, &value); err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatBool(value), nil
	case "int64":
		return stageJSONInt64(scalar)
	case "uint64":
		return stageJSONUint64(scalar)
	case "float64":
		return stageJSONFloat64(scalar)
	case "numeric_text":
		if scalar.DecimalText == nil || len(scalar.Value) != 0 || resolve == nil {
			return "", ErrStageAmbiguousNumeric
		}
		kind, ok := resolve(measurement, field)
		if !ok {
			return "", ErrStageAmbiguousNumeric
		}
		scalar.Kind = string(kind)
		switch kind {
		case NumericTextAsInt64:
			return stageJSONInt64(scalar)
		case NumericTextAsUint64:
			return stageJSONUint64(scalar)
		case NumericTextAsFloat64:
			return stageJSONFloat64(scalar)
		default:
			return "", ErrStageAmbiguousNumeric
		}
	default:
		return "", ErrStageInvalidInput
	}
}

func stageJSONInt64(scalar stageJSONScalar) (string, error) {
	if scalar.DecimalText == nil || len(scalar.Value) != 0 {
		return "", ErrStageInvalidInput
	}
	value, err := strconv.ParseInt(*scalar.DecimalText, 10, 64)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	return strconv.FormatInt(value, 10) + "i", nil
}

func stageJSONUint64(scalar stageJSONScalar) (string, error) {
	if scalar.DecimalText == nil || len(scalar.Value) != 0 {
		return "", ErrStageInvalidInput
	}
	value, err := strconv.ParseUint(*scalar.DecimalText, 10, 64)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	return strconv.FormatUint(value, 10) + "u", nil
}

func stageJSONFloat64(scalar stageJSONScalar) (string, error) {
	if scalar.DecimalText == nil || len(scalar.Value) != 0 {
		return "", ErrStageInvalidInput
	}
	value, err := strconv.ParseFloat(*scalar.DecimalText, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return "", ErrStageInvalidInput
	}
	return strconv.FormatFloat(value, 'g', -1, 64), nil
}

func decodeStageJSONScalar(encoded json.RawMessage) (stageJSONScalar, error) {
	var scalar stageJSONScalar
	if err := decodeStrictStageJSON(encoded, &scalar); err != nil || scalar.Kind == "" {
		return stageJSONScalar{}, ErrStageInvalidInput
	}
	return scalar, nil
}

func decodeStrictStageJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ErrStageInvalidInput
		}
		return err
	}
	return nil
}
