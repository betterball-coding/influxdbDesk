// Package influxql owns the local safety policy for parsed InfluxQL.
package influxql

import (
	"fmt"
	"reflect"

	ast "github.com/influxdata/influxql"
)

// Classification is deliberately fail-closed. Unsupported includes syntax
// that is valid InfluxQL but outside InfluxDesk's supported operation set.
type Classification string

const (
	ReadOnly    Classification = "READ_ONLY"
	MayMutate   Classification = "MAY_MUTATE"
	Unsupported Classification = "UNSUPPORTED"
)

// StatementAnalysis records the policy result for one parsed statement.
type StatementAnalysis struct {
	Index          int
	Classification Classification
	Canonical      string
}

// Analysis contains the normalized AST and the aggregate batch decision.
// Parsed is intended for internal consumers that need statement metadata; it
// must never be accepted back from an untrusted IPC caller.
type Analysis struct {
	Classification Classification
	Canonical      string
	Statements     []StatementAnalysis
	Parsed         *ast.Query
}

// ParseAndClassify parses text once and applies the local allowlist to the AST.
func ParseAndClassify(text string) (Analysis, error) {
	query, err := ast.ParseQuery(text)
	if err != nil {
		return Analysis{}, fmt.Errorf("parse influxql: %w", err)
	}
	return ClassifyQuery(query), nil
}

// ClassifyQuery classifies a parsed batch. A batch is read-only only when all
// statements are read-only. Exactly one mutating statement is accepted as
// MayMutate; mixed batches and multiple mutations are unsupported.
func ClassifyQuery(query *ast.Query) Analysis {
	analysis := Analysis{Classification: Unsupported, Parsed: query}
	if query == nil || len(query.Statements) == 0 {
		return analysis
	}

	analysis.Statements = make([]StatementAnalysis, 0, len(query.Statements))
	readCount, mutationCount := 0, 0
	for i, statement := range query.Statements {
		classification := ClassifyStatement(statement)
		canonical := ""
		if !isNilInterface(statement) {
			canonical = statement.String()
		}
		analysis.Statements = append(analysis.Statements, StatementAnalysis{
			Index:          i,
			Classification: classification,
			Canonical:      canonical,
		})
		switch classification {
		case ReadOnly:
			readCount++
		case MayMutate:
			mutationCount++
		default:
			return analysis
		}
	}

	switch {
	case readCount == len(query.Statements):
		analysis.Classification = ReadOnly
	case mutationCount == 1 && len(query.Statements) == 1:
		analysis.Classification = MayMutate
	default:
		analysis.Classification = Unsupported
	}
	analysis.Canonical = query.String()
	return analysis
}

// ClassifyStatement applies a version-locked allowlist. Unknown statement
// types default to Unsupported rather than inheriting library privilege data.
func ClassifyStatement(statement ast.Statement) Classification {
	if isNilInterface(statement) {
		return Unsupported
	}

	switch stmt := statement.(type) {
	case *ast.SelectStatement:
		return classifySelect(stmt)
	case *ast.ExplainStatement:
		if stmt == nil || stmt.Statement == nil {
			return Unsupported
		}
		return classifySelect(stmt.Statement)

	// Version-locked read-only allowlist (influxql v1.4.1).
	case *ast.ShowContinuousQueriesStatement,
		*ast.ShowDatabasesStatement,
		*ast.ShowDiagnosticsStatement,
		*ast.ShowFieldKeyCardinalityStatement,
		*ast.ShowFieldKeysStatement,
		*ast.ShowGrantsForUserStatement,
		*ast.ShowMeasurementCardinalityStatement,
		*ast.ShowMeasurementsStatement,
		*ast.ShowQueriesStatement,
		*ast.ShowRetentionPoliciesStatement,
		*ast.ShowSeriesCardinalityStatement,
		*ast.ShowSeriesStatement,
		*ast.ShowShardGroupsStatement,
		*ast.ShowShardsStatement,
		*ast.ShowStatsStatement,
		*ast.ShowSubscriptionsStatement,
		*ast.ShowTagKeyCardinalityStatement,
		*ast.ShowTagKeysStatement,
		*ast.ShowTagValuesCardinalityStatement,
		*ast.ShowTagValuesStatement,
		*ast.ShowUsersStatement:
		return ReadOnly

	// Supported state-changing statements.
	case *ast.AlterRetentionPolicyStatement,
		*ast.CreateContinuousQueryStatement,
		*ast.CreateDatabaseStatement,
		*ast.CreateRetentionPolicyStatement,
		*ast.CreateUserStatement,
		*ast.DeleteSeriesStatement,
		*ast.DeleteStatement,
		*ast.DropContinuousQueryStatement,
		*ast.DropDatabaseStatement,
		*ast.DropMeasurementStatement,
		*ast.DropRetentionPolicyStatement,
		*ast.DropSeriesStatement,
		*ast.DropUserStatement,
		*ast.GrantAdminStatement,
		*ast.GrantStatement,
		*ast.KillQueryStatement,
		*ast.RevokeAdminStatement,
		*ast.RevokeStatement,
		*ast.SetPasswordUserStatement:
		return MayMutate

	// Explicit product exclusions.
	case *ast.CreateSubscriptionStatement,
		*ast.DropShardStatement,
		*ast.DropSubscriptionStatement:
		return Unsupported
	default:
		return Unsupported
	}
}

func classifySelect(statement *ast.SelectStatement) Classification {
	if statement == nil {
		return Unsupported
	}
	if statement.Target != nil {
		return MayMutate
	}
	for _, source := range statement.Sources {
		if isNilInterface(source) {
			return Unsupported
		}
		switch src := source.(type) {
		case *ast.Measurement:
			if src == nil {
				return Unsupported
			}
		case *ast.SubQuery:
			if src == nil || src.Statement == nil {
				return Unsupported
			}
			classification := classifySelect(src.Statement)
			if classification != ReadOnly {
				return classification
			}
		default:
			return Unsupported
		}
	}
	return ReadOnly
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
