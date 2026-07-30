package influxql

import (
	"testing"

	ast "github.com/influxdata/influxql"
)

func TestParseAndClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		text string
		want Classification
	}{
		{name: "select", text: `SELECT value FROM cpu`, want: ReadOnly},
		{name: "nested select", text: `SELECT mean(value) FROM (SELECT value FROM cpu)`, want: ReadOnly},
		{name: "select into", text: `SELECT value INTO archive FROM cpu`, want: MayMutate},
		{name: "explain", text: `EXPLAIN SELECT value FROM cpu`, want: ReadOnly},
		{name: "explain analyze", text: `EXPLAIN ANALYZE SELECT value FROM cpu`, want: ReadOnly},
		{name: "delete", text: `DELETE FROM cpu WHERE time < now() - 1h`, want: MayMutate},
		{name: "show", text: `SHOW FIELD KEYS`, want: ReadOnly},
		{name: "drop shard excluded", text: `DROP SHARD 7`, want: Unsupported},
		{name: "subscription excluded", text: `CREATE SUBSCRIPTION sub ON db.autogen DESTINATIONS ANY 'http://localhost:8086'`, want: Unsupported},
		{name: "read batch", text: `SHOW DATABASES; SHOW USERS`, want: ReadOnly},
		{name: "mixed batch", text: `SHOW DATABASES; DROP DATABASE db`, want: Unsupported},
		{name: "multiple mutations", text: `CREATE DATABASE a; CREATE DATABASE b`, want: Unsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAndClassify(tt.text)
			if err != nil {
				t.Fatalf("ParseAndClassify() error = %v", err)
			}
			if got.Classification != tt.want {
				t.Fatalf("classification = %s, want %s", got.Classification, tt.want)
			}
		})
	}
}

func TestClassifySelectRecursesAndFailsClosed(t *testing.T) {
	t.Parallel()

	mutatingSubquery := &ast.SelectStatement{
		Sources: ast.Sources{&ast.SubQuery{Statement: &ast.SelectStatement{
			Target:  &ast.Target{Measurement: &ast.Measurement{Name: "out"}},
			Sources: ast.Sources{&ast.Measurement{Name: "cpu"}},
		}}},
	}
	if got := ClassifyStatement(mutatingSubquery); got != MayMutate {
		t.Fatalf("nested SELECT INTO = %s, want %s", got, MayMutate)
	}

	var nilSelect *ast.SelectStatement
	if got := ClassifyStatement(nilSelect); got != Unsupported {
		t.Fatalf("typed nil = %s, want %s", got, Unsupported)
	}

	badSource := &ast.SelectStatement{Sources: ast.Sources{(*ast.SubQuery)(nil)}}
	if got := ClassifyStatement(badSource); got != Unsupported {
		t.Fatalf("nil subquery = %s, want %s", got, Unsupported)
	}

	explainInto := &ast.ExplainStatement{Statement: &ast.SelectStatement{
		Target:  &ast.Target{Measurement: &ast.Measurement{Name: "out"}},
		Sources: ast.Sources{&ast.Measurement{Name: "cpu"}},
	}}
	if got := ClassifyStatement(explainInto); got != MayMutate {
		t.Fatalf("EXPLAIN SELECT INTO = %s, want %s", got, MayMutate)
	}
}

func TestClassifyQueryNilAndEmpty(t *testing.T) {
	t.Parallel()

	if got := ClassifyQuery(nil).Classification; got != Unsupported {
		t.Fatalf("nil query = %s, want %s", got, Unsupported)
	}
	if got := ClassifyQuery(&ast.Query{}).Classification; got != Unsupported {
		t.Fatalf("empty query = %s, want %s", got, Unsupported)
	}
	if got := ClassifyQuery(&ast.Query{Statements: ast.Statements{nil}}).Classification; got != Unsupported {
		t.Fatalf("query with nil statement = %s, want %s", got, Unsupported)
	}
}
