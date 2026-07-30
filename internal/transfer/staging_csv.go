package transfer

import (
	"context"
	"encoding/csv"
	"io"
	"math"
	"strconv"
)

type CSVFieldKind string

const (
	CSVFieldString  CSVFieldKind = "string"
	CSVFieldBoolean CSVFieldKind = "boolean"
	CSVFieldInt64   CSVFieldKind = "int64"
	CSVFieldUint64  CSVFieldKind = "uint64"
	CSVFieldFloat64 CSVFieldKind = "float64"
)

type CSVFieldMapping struct {
	Target string       `json:"target"`
	Kind   CSVFieldKind `json:"kind"`
}

type CSVMapping struct {
	StaticMeasurement string `json:"staticMeasurement,omitempty"`
	MeasurementColumn string `json:"measurementColumn,omitempty"`
	TimestampColumn   string `json:"timestampColumn"`
	// Maps source column names to canonical tag keys.
	TagColumns map[string]string `json:"tagColumns,omitempty"`
	// Maps source column names to canonical field keys and fixed field types.
	FieldColumns map[string]CSVFieldMapping `json:"fieldColumns"`
}

type csvStagePlan struct {
	staticMeasurement string
	measurementIndex  int
	timestampIndex    int
	tags              []csvStageTag
	fields            []csvStageField
	width             int
}

type csvStageTag struct {
	index  int
	target string
}

type csvStageField struct {
	index  int
	target string
	kind   CSVFieldKind
}

func (p *stageProcessor) processCSV(ctx context.Context, source io.Reader, mapping CSVMapping) error {
	reader := csv.NewReader(source)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err == io.EOF {
		return ErrStageEmpty
	}
	if err != nil {
		return ErrStageInvalidInput
	}
	plan, err := buildCSVStagePlan(header, mapping)
	if err != nil {
		return err
	}
	for {
		if err := stageContextError(ctx); err != nil {
			return err
		}
		record, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil || len(record) != plan.width {
			return ErrStageInvalidInput
		}
		canonical, err := plan.canonicalPoint(record)
		if err != nil {
			return err
		}
		if err := p.writePoint(ctx, canonical); err != nil {
			return err
		}
	}
}

func buildCSVStagePlan(header []string, mapping CSVMapping) (csvStagePlan, error) {
	if len(header) == 0 || (mapping.StaticMeasurement == "") == (mapping.MeasurementColumn == "") ||
		mapping.TimestampColumn == "" || len(mapping.FieldColumns) == 0 {
		return csvStagePlan{}, ErrStageCSVMappingInvalid
	}
	if mapping.StaticMeasurement != "" && !validStageText(mapping.StaticMeasurement) {
		return csvStagePlan{}, ErrStageCSVMappingInvalid
	}
	headerIndexes := make(map[string]int, len(header))
	for index, column := range header {
		if !validStageText(column) {
			return csvStagePlan{}, ErrStageInvalidInput
		}
		if _, duplicate := headerIndexes[column]; duplicate {
			return csvStagePlan{}, ErrStageInvalidInput
		}
		headerIndexes[column] = index
	}
	used := make(map[string]struct{}, len(header))
	claim := func(column string) (int, error) {
		index, found := headerIndexes[column]
		if !found {
			return 0, ErrStageCSVMappingInvalid
		}
		if _, duplicate := used[column]; duplicate {
			return 0, ErrStageCSVMappingInvalid
		}
		used[column] = struct{}{}
		return index, nil
	}
	plan := csvStagePlan{staticMeasurement: mapping.StaticMeasurement, measurementIndex: -1, width: len(header)}
	var err error
	if mapping.MeasurementColumn != "" {
		plan.measurementIndex, err = claim(mapping.MeasurementColumn)
		if err != nil {
			return csvStagePlan{}, err
		}
	}
	plan.timestampIndex, err = claim(mapping.TimestampColumn)
	if err != nil {
		return csvStagePlan{}, err
	}
	tagTargets := make(map[string]struct{}, len(mapping.TagColumns))
	for source, target := range mapping.TagColumns {
		if !validStageText(source) || !validStageText(target) {
			return csvStagePlan{}, ErrStageCSVMappingInvalid
		}
		index, err := claim(source)
		if err != nil {
			return csvStagePlan{}, err
		}
		if _, duplicate := tagTargets[target]; duplicate {
			return csvStagePlan{}, ErrStageCSVMappingInvalid
		}
		tagTargets[target] = struct{}{}
		plan.tags = append(plan.tags, csvStageTag{index: index, target: target})
	}
	fieldTargets := make(map[string]struct{}, len(mapping.FieldColumns))
	for source, field := range mapping.FieldColumns {
		if !validStageText(source) || !validStageText(field.Target) || !validCSVFieldKind(field.Kind) {
			return csvStagePlan{}, ErrStageCSVMappingInvalid
		}
		index, err := claim(source)
		if err != nil {
			return csvStagePlan{}, err
		}
		if _, duplicate := fieldTargets[field.Target]; duplicate {
			return csvStagePlan{}, ErrStageCSVMappingInvalid
		}
		if _, collision := tagTargets[field.Target]; collision {
			return csvStagePlan{}, ErrStageCSVMappingInvalid
		}
		fieldTargets[field.Target] = struct{}{}
		plan.fields = append(plan.fields, csvStageField{index: index, target: field.Target, kind: field.Kind})
	}
	if len(used) != len(header) {
		return csvStagePlan{}, ErrStageCSVMappingInvalid
	}
	return plan, nil
}

func (p csvStagePlan) canonicalPoint(record []string) (string, error) {
	measurement := p.staticMeasurement
	if p.measurementIndex >= 0 {
		measurement = record[p.measurementIndex]
	}
	if !validStageText(measurement) {
		return "", ErrStageInvalidInput
	}
	timestampText := record[p.timestampIndex]
	if timestampText == "" {
		return "", ErrStageMissingTimestamp
	}
	timestamp, err := strconv.ParseInt(timestampText, 10, 64)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	tags := make(map[string]string, len(p.tags))
	for _, mapping := range p.tags {
		value := record[mapping.index]
		if !validStageText(value) {
			return "", ErrStageInvalidInput
		}
		tags[mapping.target] = value
	}
	fields := make(map[string]string, len(p.fields))
	for _, mapping := range p.fields {
		value, err := canonicalCSVField(record[mapping.index], mapping.kind)
		if err != nil {
			return "", err
		}
		fields[mapping.target] = value
	}
	return encodeCanonicalStagePoint(measurement, tags, fields, strconv.FormatInt(timestamp, 10))
}

func canonicalCSVField(value string, kind CSVFieldKind) (string, error) {
	switch kind {
	case CSVFieldString:
		if !validStageStringValue(value) {
			return "", ErrStageInvalidInput
		}
		return encodeStageString(value), nil
	case CSVFieldBoolean:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatBool(parsed), nil
	case CSVFieldInt64:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatInt(parsed, 10) + "i", nil
	case CSVFieldUint64:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatUint(parsed, 10) + "u", nil
	case CSVFieldFloat64:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatFloat(parsed, 'g', -1, 64), nil
	default:
		return "", ErrStageCSVMappingInvalid
	}
}

func validCSVFieldKind(kind CSVFieldKind) bool {
	switch kind {
	case CSVFieldString, CSVFieldBoolean, CSVFieldInt64, CSVFieldUint64, CSVFieldFloat64:
		return true
	default:
		return false
	}
}
