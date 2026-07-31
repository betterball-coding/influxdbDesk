package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"strings"
	"testing"
	"time"
)

func TestWriteResultCSVPreservesExactValuesAndSelection(t *testing.T) {
	service := newCSVResultService(t)
	result := &ResultSet{Statements: []StatementResult{{
		ID: 0,
		Series: []*Series{{
			ID: "series-1", StatementID: 0, Measurement: "cpu",
			Columns: []string{"time", "host", "value"},
			Rows: [][]TypedScalar{
				{{Kind: ScalarTimestampNS, DecimalText: "1700000000123456788"}, {Kind: ScalarString, StringValue: "edge,\n01"}, {Kind: ScalarInt64, DecimalText: "9007199254740992"}},
				{{Kind: ScalarTimestampNS, DecimalText: "1700000015123456788"}, {Kind: ScalarString, StringValue: "edge-02"}, {Kind: ScalarInt64, DecimalText: "9007199254740993"}},
			},
			RowCount: 2,
		}},
	}}}
	installCSVResult(service, "session-export", result)

	var output bytes.Buffer
	written, err := service.WriteResultCSV(context.Background(), CSVExportRequest{
		SessionID: "session-export", StatementID: 0, SeriesID: "series-1", RowIndexes: []uint64{1},
	}, &output, time.FixedZone("CST", 8*60*60))
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 {
		t.Fatalf("written=%d", written)
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(output.String(), "\ufeff")))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"time", "host", "value"},
		{"2023-11-15 06:13:35.123456788", "edge-02", "9007199254740993"},
	}
	if len(records) != len(want) {
		t.Fatalf("records=%v", records)
	}
	for index := range want {
		if strings.Join(records[index], "|") != strings.Join(want[index], "|") {
			t.Fatalf("record %d=%v want=%v", index, records[index], want[index])
		}
	}
}

func TestWriteResultCSVExportsAllPages(t *testing.T) {
	service := newCSVResultService(t)
	rows := make([][]TypedScalar, 5001)
	for index := range rows {
		rows[index] = []TypedScalar{{Kind: ScalarInt64, DecimalText: "9007199254740993"}}
	}
	result := &ResultSet{Statements: []StatementResult{{
		ID: 0, Series: []*Series{{ID: "series-1", StatementID: 0, Measurement: "cpu", Columns: []string{"value"}, Rows: rows, RowCount: uint64(len(rows))}},
	}}}
	installCSVResult(service, "session-export-all", result)

	var output bytes.Buffer
	written, err := service.WriteResultCSV(context.Background(), CSVExportRequest{
		SessionID: "session-export-all", StatementID: 0, SeriesID: "series-1", AllRows: true,
	}, &output, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if written != 5001 {
		t.Fatalf("written=%d", written)
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(output.String(), "\ufeff")))
	count := 0
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 5002 {
		t.Fatalf("csv records=%d", count)
	}
}

func TestWriteResultCSVNeutralizesFormulaTextWithoutChangingTypedNumbers(t *testing.T) {
	service := newCSVResultService(t)
	result := &ResultSet{Statements: []StatementResult{{
		ID: 0, Series: []*Series{{
			ID: "series-formulas", StatementID: 0, Measurement: "unsafe",
			Columns: []string{"=column", "plus", "minus", "at", "tab", "carriage", "number"},
			Rows: [][]TypedScalar{{
				{Kind: ScalarString, StringValue: "=1+1"},
				{Kind: ScalarString, StringValue: "+cmd"},
				{Kind: ScalarString, StringValue: "-text"},
				{Kind: ScalarString, StringValue: "@SUM(A1:A2)"},
				{Kind: ScalarString, StringValue: "\tformula"},
				{Kind: ScalarString, StringValue: "\r=cmd"},
				{Kind: ScalarInt64, DecimalText: "-42"},
			}}, RowCount: 1,
		}},
	}}}
	installCSVResult(service, "session-formulas", result)

	var output bytes.Buffer
	_, err := service.WriteResultCSV(context.Background(), CSVExportRequest{
		SessionID: "session-formulas", StatementID: 0, SeriesID: "series-formulas", AllRows: true,
	}, &output, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(output.String(), "\ufeff")))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	wantHeader := []string{"'=column", "plus", "minus", "at", "tab", "carriage", "number"}
	// encoding/csv strips CR bytes while writing; the injected apostrophe must
	// remain first so a following formula marker cannot become active.
	wantRow := []string{"'=1+1", "'+cmd", "'-text", "'@SUM(A1:A2)", "'\tformula", "'=cmd", "-42"}
	if len(records) != 2 || strings.Join(records[0], "|") != strings.Join(wantHeader, "|") ||
		strings.Join(records[1], "|") != strings.Join(wantRow, "|") {
		t.Fatalf("formula-safe CSV records = %#v", records)
	}
}

func newCSVResultService(t *testing.T) *Service {
	t.Helper()
	service, err := NewService(&fakeDispatcher{}, ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func installCSVResult(service *Service, sessionID string, result *ResultSet) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.sessions[sessionID] = &sessionRecord{
		snapshot: QuerySession{
			ID: sessionID, State: SessionSucceeded, Terminal: true, Generation: "1",
			ResultAvailable: true, Complete: true,
		},
		result: result, resultLastAccess: time.Now(),
	}
}
