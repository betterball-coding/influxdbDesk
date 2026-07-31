package transfer

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestStageImportCSVExplicitMappingPreservesExactTypes(t *testing.T) {
	source := "measurement,timestamp,host,signed,unsigned,zero,note\n" +
		`"cpu load",1720000000000000001,"east,1",9007199254740993,18446744073709551615,-0,"hello, ""quoted"""` + "\n"
	var destination bytes.Buffer
	result, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageCSV, Source: strings.NewReader(source), Destination: &destination,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		CSVMapping: &CSVMapping{
			MeasurementColumn: "measurement", TimestampColumn: "timestamp",
			TagColumns: map[string]string{"host": "host name"},
			FieldColumns: map[string]CSVFieldMapping{
				"signed":   {Target: "count", Kind: CSVFieldInt64},
				"unsigned": {Target: "max", Kind: CSVFieldUint64},
				"zero":     {Target: "ratio", Kind: CSVFieldFloat64},
				"note":     {Target: "note", Kind: CSVFieldString},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "cpu\\ load,host\\ name=east\\,1 count=9007199254740993i,max=18446744073709551615u,note=\"hello, \\\"quoted\\\"\",ratio=-0 1720000000000000001\n"
	if destination.String() != want || result.PointCount != "1" {
		t.Fatalf("CSV canonical LP\nwant %q\n got %q\nresult=%+v", want, destination.String(), result)
	}
}

func TestStageImportCSVStaticMeasurement(t *testing.T) {
	var destination bytes.Buffer
	_, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageCSV, Source: strings.NewReader("time,value\n1,true\n"), Destination: &destination,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		CSVMapping: &CSVMapping{
			StaticMeasurement: "static measurement", TimestampColumn: "time",
			FieldColumns: map[string]CSVFieldMapping{"value": {Target: "ok", Kind: CSVFieldBoolean}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if destination.String() != "static\\ measurement ok=true 1\n" {
		t.Fatalf("static measurement output=%q", destination.String())
	}
}

func TestStageImportCSVRequiresCompleteUnambiguousMapping(t *testing.T) {
	t.Run("missing mapping", func(t *testing.T) {
		_, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageCSV, Source: strings.NewReader("time,value\n1,2\n"), Destination: &bytes.Buffer{},
			Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		})
		if !errors.Is(err, ErrStageCSVMappingRequired) {
			t.Fatalf("missing mapping error=%v", err)
		}
	})

	validFields := map[string]CSVFieldMapping{"value": {Target: "value", Kind: CSVFieldInt64}}
	tests := []struct {
		name    string
		source  string
		mapping CSVMapping
	}{
		{
			name: "both measurement modes", source: "measurement,time,value\nm,1,2\n",
			mapping: CSVMapping{StaticMeasurement: "m", MeasurementColumn: "measurement",
				TimestampColumn: "time", FieldColumns: validFields},
		},
		{
			name: "neither measurement mode", source: "time,value\n1,2\n",
			mapping: CSVMapping{TimestampColumn: "time", FieldColumns: validFields},
		},
		{
			name: "missing timestamp mapping", source: "time,value\n1,2\n",
			mapping: CSVMapping{StaticMeasurement: "m", FieldColumns: validFields},
		},
		{
			name: "no field mapping", source: "time\n1\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time"},
		},
		{
			name: "duplicate header", source: "time,value,value\n1,2,3\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time", FieldColumns: validFields},
		},
		{
			name: "unknown header", source: "time,value,ignored\n1,2,3\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time", FieldColumns: validFields},
		},
		{
			name: "missing mapped column", source: "time,value\n1,2\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time",
				FieldColumns: map[string]CSVFieldMapping{"missing": {Target: "value", Kind: CSVFieldInt64}}},
		},
		{
			name: "tag field target collision", source: "time,host,value\n1,east,2\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time",
				TagColumns:   map[string]string{"host": "shared"},
				FieldColumns: map[string]CSVFieldMapping{"value": {Target: "shared", Kind: CSVFieldInt64}}},
		},
		{
			name: "unknown field kind", source: "time,value\n1,2\n",
			mapping: CSVMapping{StaticMeasurement: "m", TimestampColumn: "time",
				FieldColumns: map[string]CSVFieldMapping{"value": {Target: "value", Kind: "guessed"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := StageImport(context.Background(), StageImportRequest{
				Format: ImportStageCSV, Source: strings.NewReader(test.source), Destination: &bytes.Buffer{},
				Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
				CSVMapping: &test.mapping,
			})
			if !errors.Is(err, ErrStageCSVMappingInvalid) && !errors.Is(err, ErrStageInvalidInput) {
				t.Fatalf("mapping error=%v", err)
			}
		})
	}
}

func TestStageImportCSVRejectsRowAndTypeErrors(t *testing.T) {
	baseMapping := func(kind CSVFieldKind) *CSVMapping {
		return &CSVMapping{
			StaticMeasurement: "m", TimestampColumn: "time",
			FieldColumns: map[string]CSVFieldMapping{"value": {Target: "value", Kind: kind}},
		}
	}
	tests := []struct {
		name, source string
		kind         CSVFieldKind
		want         error
	}{
		{name: "width change", source: "time,value\n1,2,3\n", kind: CSVFieldInt64, want: ErrStageInvalidInput},
		{name: "empty timestamp", source: "time,value\n,2\n", kind: CSVFieldInt64, want: ErrStageMissingTimestamp},
		{name: "bad timestamp", source: "time,value\n1.5,2\n", kind: CSVFieldInt64, want: ErrStageInvalidInput},
		{name: "int64 overflow", source: "time,value\n1,9223372036854775808\n", kind: CSVFieldInt64, want: ErrStageInvalidInput},
		{name: "uint64 overflow", source: "time,value\n1,18446744073709551616\n", kind: CSVFieldUint64, want: ErrStageInvalidInput},
		{name: "negative uint64", source: "time,value\n1,-1\n", kind: CSVFieldUint64, want: ErrStageInvalidInput},
		{name: "nonfinite float", source: "time,value\n1,NaN\n", kind: CSVFieldFloat64, want: ErrStageInvalidInput},
		{name: "invalid boolean", source: "time,value\n1,maybe\n", kind: CSVFieldBoolean, want: ErrStageInvalidInput},
		{name: "quoted newline string", source: "time,value\n1,\"hello\nworld\"\n", kind: CSVFieldString, want: ErrStageInvalidInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := StageImport(context.Background(), StageImportRequest{
				Format: ImportStageCSV, Source: strings.NewReader(test.source), Destination: &bytes.Buffer{},
				Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
				CSVMapping: baseMapping(test.kind),
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("row/type error=%v want=%v", err, test.want)
			}
		})
	}
	t.Run("empty measurement column", func(t *testing.T) {
		_, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageCSV, Source: strings.NewReader("measurement,time,value\n,1,2\n"),
			Destination: &bytes.Buffer{}, Reserve: func(context.Context, int64) error { return nil },
			SettleReservation: settleStageReservation,
			CSVMapping: &CSVMapping{
				MeasurementColumn: "measurement", TimestampColumn: "time",
				FieldColumns: map[string]CSVFieldMapping{"value": {Target: "value", Kind: CSVFieldInt64}},
			},
		})
		if !errors.Is(err, ErrStageInvalidInput) {
			t.Fatalf("empty measurement error=%v", err)
		}
	})
}

func TestStageImportRejectsEmptySourcesWithoutSettlement(t *testing.T) {
	tests := []StageImportRequest{
		{Format: ImportStageLP, Source: strings.NewReader("# comment only\n"), Destination: &bytes.Buffer{}},
		{Format: ImportStageLPGzip, Source: bytes.NewReader(gzipStageBytes(t, nil)), Destination: &bytes.Buffer{}},
		{Format: ImportStageTypedJSONL, Source: strings.NewReader("\n"), Destination: &bytes.Buffer{}},
		{
			Format: ImportStageCSV, Source: strings.NewReader("time,value\n"), Destination: &bytes.Buffer{},
			CSVMapping: &CSVMapping{StaticMeasurement: "m", TimestampColumn: "time",
				FieldColumns: map[string]CSVFieldMapping{"value": {Target: "value", Kind: CSVFieldInt64}}},
		},
	}
	for _, request := range tests {
		settlements := 0
		request.Reserve = func(context.Context, int64) error { return nil }
		request.SettleReservation = func(context.Context, int64, int64) error {
			settlements++
			return nil
		}
		if _, err := StageImport(context.Background(), request); !errors.Is(err, ErrStageEmpty) {
			t.Fatalf("empty %s error=%v", request.Format, err)
		}
		if settlements != 0 {
			t.Fatalf("empty %s settled reservation", request.Format)
		}
	}
}

func TestStageImportCSVRejectsOversizedRecordsBeforeCSVDecode(t *testing.T) {
	base := StageImportRequest{
		Format: ImportStageCSV, Destination: &bytes.Buffer{}, CSVRecordLimitBytes: 64,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		CSVMapping: &CSVMapping{
			StaticMeasurement: "m", TimestampColumn: "time",
			FieldColumns: map[string]CSVFieldMapping{"value": {Target: "value", Kind: CSVFieldString}},
		},
	}
	tests := []struct {
		name   string
		source string
	}{
		{name: "header", source: strings.Repeat("h", 65) + ",value\n1,text\n"},
		{name: "row", source: "time,value\n1," + strings.Repeat("x", 64) + "\n"},
		{name: "multiline quoted row", source: "time,value\n1,\"" + strings.Repeat("x\n", 40) + "\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Source = strings.NewReader(test.source)
			_, err := StageImport(context.Background(), request)
			if !errors.Is(err, ErrStagePointTooLarge) {
				t.Fatalf("oversized CSV record error = %v", err)
			}
		})
	}
}
