package exportfmt

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/influxdesk/influxdesk/internal/query"
)

func TestExactFormatsNeverRoundLargeIntegers(t *testing.T) {
	rows := [][]query.TypedScalar{{
		{Kind: query.ScalarInt64, DecimalText: "9007199254740993"},
		{Kind: query.ScalarUint64, DecimalText: "18446744073709551615"},
	}}
	var csvOutput bytes.Buffer
	if err := WriteCSV(&csvOutput, []string{"signed", "unsigned"}, rows); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(csvOutput.String(), "9007199254740993,18446744073709551615") {
		t.Fatalf("CSV rounded exact values: %s", csvOutput.String())
	}

	var jsonl bytes.Buffer
	if err := WriteTypedJSONL(&jsonl, []TypedRow{{"signed": rows[0][0], "unsigned": rows[0][1]}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(jsonl.String(), `"signed":9007199254740993`) || !strings.Contains(jsonl.String(), `"decimalText":"9007199254740993"`) {
		t.Fatalf("JSONL emitted a bare number: %s", jsonl.String())
	}
}

func TestStrictLPTypesAndEscaping(t *testing.T) {
	boolean := true
	line, err := EncodeLPPoint(
		"cpu load",
		map[string]string{"host name": "east,1"},
		map[string]query.TypedScalar{
			"count": {Kind: query.ScalarInt64, DecimalText: "9007199254740993"},
			"ok":    {Kind: query.ScalarBoolean, BooleanValue: &boolean},
			"ratio": {Kind: query.ScalarFloat64, DecimalText: "0.10000000000000001"},
		},
		query.TypedScalar{Kind: query.ScalarTimestampNS, DecimalText: "1720000000000000001"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `cpu\ load,host\ name=east\,1 count=9007199254740993i,ok=true,ratio=0.1 1720000000000000001`
	if line != want {
		t.Fatalf("unexpected LP\nwant %s\n got %s", want, line)
	}
}

func TestStrictLPRejectsNumericText(t *testing.T) {
	_, err := EncodeLPPoint("m", nil, map[string]query.TypedScalar{
		"value": {Kind: query.ScalarNumericText, DecimalText: "1"},
	}, query.TypedScalar{Kind: query.ScalarTimestampNS, DecimalText: "1"})
	if !errors.Is(err, ErrAmbiguousNumeric) {
		t.Fatalf("expected ambiguity error, got %v", err)
	}
}
