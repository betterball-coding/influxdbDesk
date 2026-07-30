package transfer

import (
	"bufio"
	"context"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

func (p *stageProcessor) processLP(ctx context.Context, reader *bufio.Reader, _ bool) error {
	maximumInput := int(StageCanonicalPointLimitBytes * 2)
	for {
		line, err := readStageRecord(ctx, reader, maximumInput)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		canonical, err := parseCanonicalLPPoint(string(line))
		if err != nil {
			return err
		}
		if err := p.writePoint(ctx, canonical); err != nil {
			return err
		}
	}
}

func parseCanonicalLPPoint(line string) (string, error) {
	if line == "" || !utf8.ValidString(line) || strings.ContainsAny(line, "\r\n\x00") {
		return "", ErrStageInvalidInput
	}
	seriesEnd := findUnescapedByte(line, ' ')
	if seriesEnd <= 0 {
		return "", ErrStageMissingTimestamp
	}
	fieldsEnd := findUnquotedByte(line, ' ', seriesEnd+1)
	if fieldsEnd <= seriesEnd+1 {
		return "", ErrStageMissingTimestamp
	}
	if fieldsEnd+1 >= len(line) || strings.ContainsRune(line[fieldsEnd+1:], ' ') {
		return "", ErrStageInvalidInput
	}
	timestamp, err := strconv.ParseInt(line[fieldsEnd+1:], 10, 64)
	if err != nil {
		return "", ErrStageInvalidInput
	}
	measurement, tags, err := parseStageSeries(line[:seriesEnd])
	if err != nil {
		return "", err
	}
	fields, err := parseStageFields(line[seriesEnd+1 : fieldsEnd])
	if err != nil {
		return "", err
	}
	return encodeCanonicalStagePoint(measurement, tags, fields, strconv.FormatInt(timestamp, 10))
}

func parseStageSeries(value string) (string, map[string]string, error) {
	parts, err := splitStageEscaped(value, ',', false)
	if err != nil || len(parts) == 0 {
		return "", nil, ErrStageInvalidInput
	}
	measurement, err := unescapeStageToken(parts[0], " ,\\")
	if err != nil || measurement == "" {
		return "", nil, ErrStageInvalidInput
	}
	tags := make(map[string]string, len(parts)-1)
	for _, part := range parts[1:] {
		assignment := findUnescapedByte(part, '=')
		if assignment <= 0 || assignment+1 >= len(part) {
			return "", nil, ErrStageInvalidInput
		}
		key, keyErr := unescapeStageToken(part[:assignment], " ,=\\")
		value, valueErr := unescapeStageToken(part[assignment+1:], " ,=\\")
		if keyErr != nil || valueErr != nil || key == "" || value == "" {
			return "", nil, ErrStageInvalidInput
		}
		if _, duplicate := tags[key]; duplicate {
			return "", nil, ErrStageInvalidInput
		}
		tags[key] = value
	}
	return measurement, tags, nil
}

func parseStageFields(value string) (map[string]string, error) {
	parts, err := splitStageEscaped(value, ',', true)
	if err != nil || len(parts) == 0 {
		return nil, ErrStageInvalidInput
	}
	fields := make(map[string]string, len(parts))
	for _, part := range parts {
		assignment := findUnquotedByte(part, '=', 0)
		if assignment <= 0 || assignment+1 >= len(part) {
			return nil, ErrStageInvalidInput
		}
		key, err := unescapeStageToken(part[:assignment], " ,=\\")
		if err != nil || key == "" {
			return nil, ErrStageInvalidInput
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, ErrStageInvalidInput
		}
		encoded, err := canonicalStageFieldValue(part[assignment+1:])
		if err != nil {
			return nil, err
		}
		fields[key] = encoded
	}
	if len(fields) == 0 {
		return nil, ErrStageInvalidInput
	}
	return fields, nil
}

func canonicalStageFieldValue(value string) (string, error) {
	if value == "" {
		return "", ErrStageInvalidInput
	}
	if value[0] == '"' {
		decoded, err := decodeStageString(value)
		if err != nil {
			return "", err
		}
		return encodeStageString(decoded), nil
	}
	lower := strings.ToLower(value)
	switch lower {
	case "t", "true":
		return "true", nil
	case "f", "false":
		return "false", nil
	}
	if strings.HasSuffix(value, "i") {
		number, err := strconv.ParseInt(value[:len(value)-1], 10, 64)
		if err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatInt(number, 10) + "i", nil
	}
	if strings.HasSuffix(value, "u") {
		number, err := strconv.ParseUint(value[:len(value)-1], 10, 64)
		if err != nil {
			return "", ErrStageInvalidInput
		}
		return strconv.FormatUint(number, 10) + "u", nil
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
		return "", ErrStageInvalidInput
	}
	return strconv.FormatFloat(number, 'g', -1, 64), nil
}

func decodeStageString(value string) (string, error) {
	if len(value) < 2 || value[len(value)-1] != '"' {
		return "", ErrStageInvalidInput
	}
	var output strings.Builder
	for index := 1; index < len(value)-1; index++ {
		current := value[index]
		if current != '\\' {
			if current == '"' {
				return "", ErrStageInvalidInput
			}
			output.WriteByte(current)
			continue
		}
		if index+1 >= len(value)-1 {
			return "", ErrStageInvalidInput
		}
		next := value[index+1]
		if next == '\\' || next == '"' {
			output.WriteByte(next)
			index++
			continue
		}
		output.WriteByte('\\')
	}
	decoded := output.String()
	if !utf8.ValidString(decoded) || strings.ContainsAny(decoded, "\r\n\x00") {
		return "", ErrStageInvalidInput
	}
	return decoded, nil
}

func encodeCanonicalStagePoint(
	measurement string,
	tags map[string]string,
	fields map[string]string,
	timestamp string,
) (string, error) {
	if !validStageText(measurement) || len(fields) == 0 {
		return "", ErrStageInvalidInput
	}
	for key := range tags {
		if _, collision := fields[key]; collision {
			return "", ErrStageInvalidInput
		}
	}
	var output strings.Builder
	output.WriteString(escapeStageToken(measurement, true, false))
	tagKeys := sortedStageKeys(tags)
	for _, key := range tagKeys {
		if !validStageText(key) || !validStageText(tags[key]) {
			return "", ErrStageInvalidInput
		}
		output.WriteByte(',')
		output.WriteString(escapeStageToken(key, true, true))
		output.WriteByte('=')
		output.WriteString(escapeStageToken(tags[key], true, true))
	}
	output.WriteByte(' ')
	fieldKeys := sortedStageKeys(fields)
	for index, key := range fieldKeys {
		if !validStageText(key) || fields[key] == "" || strings.ContainsAny(fields[key], "\r\n\x00") {
			return "", ErrStageInvalidInput
		}
		if index != 0 {
			output.WriteByte(',')
		}
		output.WriteString(escapeStageToken(key, true, true))
		output.WriteByte('=')
		output.WriteString(fields[key])
	}
	output.WriteByte(' ')
	output.WriteString(timestamp)
	return output.String(), nil
}

func validStageText(value string) bool {
	return value != "" && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func validStageStringValue(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

func findUnescapedByte(value string, target byte) int {
	escaped := false
	for index := 0; index < len(value); index++ {
		if escaped {
			escaped = false
			continue
		}
		if value[index] == '\\' {
			escaped = true
			continue
		}
		if value[index] == target {
			return index
		}
	}
	return -1
}

func findUnquotedByte(value string, target byte, start int) int {
	escaped := false
	quoted := false
	for index := start; index < len(value); index++ {
		if escaped {
			escaped = false
			continue
		}
		if value[index] == '\\' {
			escaped = true
			continue
		}
		if value[index] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted && value[index] == target {
			return index
		}
	}
	return -1
}

func splitStageEscaped(value string, separator byte, quotes bool) ([]string, error) {
	var parts []string
	start := 0
	escaped := false
	quoted := false
	for index := 0; index < len(value); index++ {
		if escaped {
			escaped = false
			continue
		}
		if value[index] == '\\' {
			escaped = true
			continue
		}
		if quotes && value[index] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted && value[index] == separator {
			if index == start {
				return nil, ErrStageInvalidInput
			}
			parts = append(parts, value[start:index])
			start = index + 1
		}
	}
	if escaped || quoted || start >= len(value) {
		return nil, ErrStageInvalidInput
	}
	parts = append(parts, value[start:])
	return parts, nil
}

func unescapeStageToken(value, allowed string) (string, error) {
	var output strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			output.WriteByte(value[index])
			continue
		}
		if index+1 >= len(value) || !strings.ContainsRune(allowed, rune(value[index+1])) {
			return "", ErrStageInvalidInput
		}
		index++
		output.WriteByte(value[index])
	}
	decoded := output.String()
	if !utf8.ValidString(decoded) || strings.ContainsAny(decoded, "\r\n\x00") {
		return "", ErrStageInvalidInput
	}
	return decoded, nil
}

func escapeStageToken(value string, escapeComma, escapeEquals bool) string {
	var output strings.Builder
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current == '\\' || current == ' ' || (escapeComma && current == ',') ||
			(escapeEquals && current == '=') {
			output.WriteByte('\\')
		}
		output.WriteByte(current)
	}
	return output.String()
}

func encodeStageString(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

func sortedStageKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
