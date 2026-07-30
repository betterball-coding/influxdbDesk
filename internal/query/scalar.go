package query

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

type ScalarKind string

const (
	ScalarNull        ScalarKind = "null"
	ScalarString      ScalarKind = "string"
	ScalarBoolean     ScalarKind = "boolean"
	ScalarTimestampNS ScalarKind = "timestamp_ns"
	ScalarInt64       ScalarKind = "int64"
	ScalarUint64      ScalarKind = "uint64"
	ScalarFloat64     ScalarKind = "float64"
	ScalarNumericText ScalarKind = "numeric_text"
)

// TypedScalar never sends a 64-bit value over the Wails boundary as a
// JavaScript number. Numeric values always use DecimalText.
type TypedScalar struct {
	Kind         ScalarKind `json:"kind"`
	DecimalText  string     `json:"decimalText,omitempty"`
	StringValue  string     `json:"stringValue,omitempty"`
	BooleanValue *bool      `json:"booleanValue,omitempty"`
}

// MarshalJSON keeps the Wails wire shape compact while preserving all numeric
// values as decimal strings. In-memory field names are intentionally not part
// of the IPC contract.
func (s TypedScalar) MarshalJSON() ([]byte, error) {
	switch s.Kind {
	case ScalarNull:
		return json.Marshal(struct {
			Kind ScalarKind `json:"kind"`
		}{Kind: s.Kind})
	case ScalarString:
		return json.Marshal(struct {
			Kind  ScalarKind `json:"kind"`
			Value string     `json:"value"`
		}{Kind: s.Kind, Value: s.StringValue})
	case ScalarBoolean:
		if s.BooleanValue == nil {
			return nil, fmt.Errorf("boolean scalar has no value")
		}
		return json.Marshal(struct {
			Kind  ScalarKind `json:"kind"`
			Value bool       `json:"value"`
		}{Kind: s.Kind, Value: *s.BooleanValue})
	case ScalarTimestampNS, ScalarInt64, ScalarUint64, ScalarFloat64, ScalarNumericText:
		if s.DecimalText == "" {
			return nil, fmt.Errorf("numeric scalar has no decimalText")
		}
		return json.Marshal(struct {
			Kind        ScalarKind `json:"kind"`
			DecimalText string     `json:"decimalText"`
		}{Kind: s.Kind, DecimalText: s.DecimalText})
	default:
		return nil, fmt.Errorf("invalid scalar kind %q", s.Kind)
	}
}

type NumericContext struct {
	StatementID int
	Measurement string
	Tags        map[string]string
	Column      string
}

// NumericKindResolver combines AST/schema/function metadata. Returning false
// keeps the exact token as NumericText and prevents strict LP round-tripping.
type NumericKindResolver interface {
	ResolveNumeric(NumericContext) (ScalarKind, bool)
}

type NumericKindResolverFunc func(NumericContext) (ScalarKind, bool)

func (f NumericKindResolverFunc) ResolveNumeric(ctx NumericContext) (ScalarKind, bool) {
	return f(ctx)
}

func scalarFromValue(value any, kind ScalarKind) (TypedScalar, error) {
	switch typed := value.(type) {
	case nil:
		return TypedScalar{Kind: ScalarNull}, nil
	case string:
		return TypedScalar{Kind: ScalarString, StringValue: typed}, nil
	case bool:
		copy := typed
		return TypedScalar{Kind: ScalarBoolean, BooleanValue: &copy}, nil
	case json.Number:
		return numericScalar(typed.String(), kind)
	default:
		return TypedScalar{}, fmt.Errorf("unsupported JSON scalar type %T", value)
	}
}

func numericScalar(raw string, kind ScalarKind) (TypedScalar, error) {
	switch kind {
	case ScalarTimestampNS, ScalarInt64:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return TypedScalar{}, fmt.Errorf("invalid signed 64-bit decimal")
		}
		return TypedScalar{Kind: kind, DecimalText: strconv.FormatInt(value, 10)}, nil
	case ScalarUint64:
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return TypedScalar{}, fmt.Errorf("invalid unsigned 64-bit decimal")
		}
		return TypedScalar{Kind: kind, DecimalText: strconv.FormatUint(value, 10)}, nil
	case ScalarFloat64:
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return TypedScalar{}, fmt.Errorf("invalid finite binary64 value")
		}
		return TypedScalar{Kind: kind, DecimalText: raw}, nil
	case ScalarNumericText:
		return TypedScalar{Kind: kind, DecimalText: raw}, nil
	default:
		return TypedScalar{}, fmt.Errorf("invalid numeric scalar kind %q", kind)
	}
}
