package query

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestTypedScalarWailsJSONNeverUsesBareNumber(t *testing.T) {
	encoded, err := json.Marshal([]TypedScalar{
		{Kind: ScalarString, StringValue: "host"},
		{Kind: ScalarInt64, DecimalText: "9007199254740993"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := string(encoded)
	if got != `[{"kind":"string","value":"host"},{"kind":"int64","decimalText":"9007199254740993"}]` {
		t.Fatalf("unexpected Wails scalar JSON: %s", got)
	}
}

func TestDecodeChunkedPreservesExactNumbersAndMergesSeries(t *testing.T) {
	t.Parallel()

	wire := strings.Join([]string{
		`{"results":[{"statement_id":0,"partial":true,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","signed","unsigned","ratio","unknown"],"values":[[1700000000000000001,9007199254740993,18446744073709551615,0.1,1e20]],"partial":true}]}]}`,
		`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","signed","unsigned","ratio","unknown"],"values":[[1700000000000000002,9007199254740994,18446744073709551614,-0.0,2e20]]}]}]}`,
	}, "\n")
	resolver := NumericKindResolverFunc(func(ctx NumericContext) (ScalarKind, bool) {
		switch ctx.Column {
		case "signed":
			return ScalarInt64, true
		case "unsigned":
			return ScalarUint64, true
		case "ratio":
			return ScalarFloat64, true
		default:
			return "", false
		}
	})
	result, err := DecodeChunked(strings.NewReader(wire), DecodeConfig{
		Statements: []StatementSpec{{TimeColumn: "time"}},
		Resolver:   resolver,
	})
	if err != nil {
		t.Fatalf("DecodeChunked() error = %v", err)
	}
	series := result.Statements[0].Series
	if len(series) != 1 || len(series[0].Rows) != 2 {
		t.Fatalf("series/rows = %d/%d, want 1/2", len(series), len(series[0].Rows))
	}
	first := series[0].Rows[0]
	want := []struct {
		kind ScalarKind
		text string
	}{
		{ScalarTimestampNS, "1700000000000000001"},
		{ScalarInt64, "9007199254740993"},
		{ScalarUint64, "18446744073709551615"},
		{ScalarFloat64, "0.1"},
		{ScalarNumericText, "1e20"},
	}
	for i, expected := range want {
		if first[i].Kind != expected.kind || first[i].DecimalText != expected.text {
			t.Errorf("scalar[%d] = {%s %q}, want {%s %q}", i, first[i].Kind, first[i].DecimalText, expected.kind, expected.text)
		}
	}
	if got := series[0].Rows[1][3].DecimalText; got != "-0.0" {
		t.Fatalf("negative zero token = %q", got)
	}
}

func TestDecodeChunkedStatementReconciliation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wire string
		code ProtocolErrorCode
	}{
		{name: "empty", wire: "", code: ProtocolEmptyResponse},
		{name: "missing result", wire: `{}`, code: ProtocolStatementMismatch},
		{name: "multiple results", wire: `{"results":[{"statement_id":0},{"statement_id":1}]}`, code: ProtocolStatementMismatch},
		{name: "missing first statement", wire: `{"results":[{"statement_id":1}]}`, code: ProtocolStatementMismatch},
		{name: "partial eof", wire: `{"results":[{"statement_id":0,"partial":true}]}`, code: ProtocolIncomplete},
		{name: "repeated closed", wire: "{\"results\":[{\"statement_id\":0}]}\n{\"results\":[{\"statement_id\":0}]}", code: ProtocolStatementMismatch},
		{name: "top error", wire: `{"error":"database not found"}`, code: ProtocolTopLevelError},
		{name: "row width", wire: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1]]}]}]}`, code: ProtocolInvalidJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeChunked(strings.NewReader(tt.wire), DecodeConfig{Statements: []StatementSpec{{}, {}}})
			var protocol *DecodeError
			if !errors.As(err, &protocol) || protocol.Code != tt.code {
				t.Fatalf("error = %#v, want protocol code %s", err, tt.code)
			}
		})
	}
}

func TestDecodeChunkedStatementErrorIsNonFatal(t *testing.T) {
	t.Parallel()

	wire := "{\"results\":[{\"statement_id\":0,\"error\":\"bad field\"}]}\n" +
		"{\"results\":[{\"statement_id\":1}]}"
	result, err := DecodeChunked(strings.NewReader(wire), DecodeConfig{Statements: []StatementSpec{{}, {}}})
	if err != nil {
		t.Fatalf("DecodeChunked() error = %v", err)
	}
	if !result.Statements[0].HasError || result.StatementErrorText[0] != "bad field" {
		t.Fatalf("statement error was not retained transiently: %#v", result)
	}
}

func TestDecodeChunkedRejectsInvalidExactTypes(t *testing.T) {
	t.Parallel()

	wire := `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[[1,18446744073709551615]]}]}]}`
	resolver := NumericKindResolverFunc(func(NumericContext) (ScalarKind, bool) { return ScalarInt64, true })
	_, err := DecodeChunked(strings.NewReader(wire), DecodeConfig{
		Statements: []StatementSpec{{}}, Resolver: resolver,
	})
	var protocol *DecodeError
	if !errors.As(err, &protocol) || protocol.Code != ProtocolInvalidJSON {
		t.Fatalf("error = %#v, want invalid protocol", err)
	}
}

func TestDecodeChunkedFramesConsecutiveObjectsWithoutNewlines(t *testing.T) {
	wire := `{"results":[{"statement_id":0,"partial":true,"series":[{"name":"cpu","columns":["value"],"values":[["text }{ remains text"]]}]}]}` +
		`{"results":[{"statement_id":0}]}`
	result, err := DecodeChunked(strings.NewReader(wire), DecodeConfig{Statements: []StatementSpec{{}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.ChunkCount != 2 || result.Statements[0].Series[0].Rows[0][0].StringValue != "text }{ remains text" {
		t.Fatalf("decoded result = %#v", result)
	}
}

func TestDecodeChunkedRejectsOversizedTopLevelObjectBeforeDecode(t *testing.T) {
	wire := `{"results":[{"statement_id":0}],"padding":"` + strings.Repeat("x", 128) + `"}`
	_, err := DecodeChunked(strings.NewReader(wire), DecodeConfig{
		Statements: []StatementSpec{{}}, MaxChunkBytes: 64,
	})
	var protocol *DecodeError
	if !errors.As(err, &protocol) || protocol.Code != ProtocolChunkTooLarge ||
		!errors.Is(protocol.Cause, errQueryChunkTooLarge) {
		t.Fatalf("oversized chunk error = %#v", err)
	}
}

func TestMakeSeriesIDSortsTagsAndSeparatesBoundaries(t *testing.T) {
	t.Parallel()

	left := MakeSeriesID(0, "cpu", map[string]string{"host": "a", "zone": "b"})
	right := MakeSeriesID(0, "cpu", map[string]string{"zone": "b", "host": "a"})
	if left != right {
		t.Fatal("tag insertion order changed series ID")
	}
	if left == MakeSeriesID(0, "cpuhost", map[string]string{"": "a", "zone": "b"}) {
		t.Fatal("length-prefix identity collided")
	}
}
