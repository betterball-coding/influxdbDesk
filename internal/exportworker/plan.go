package exportworker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/influxdesk/influxdesk/internal/exportlane"
	"github.com/influxdesk/influxdesk/internal/transport"
)

const maxPlanSlices = 1_000_000

const (
	tagKeysQuantumID   = "metadata/tag-keys"
	fieldKeysQuantumID = "metadata/field-keys"
)

var (
	ErrInvalidPlan       = errors.New("INVALID_EXPORT_PLAN")
	ErrPlanNotRegistered = errors.New("EXPORT_PLAN_NOT_REGISTERED")
	ErrQuantumMismatch   = errors.New("EXPORT_QUANTUM_MISMATCH")
)

// MeasurementSpec describes one fully-qualified measurement logical export.
// StartNS and EndNS are an exact half-open nanosecond range. SliceWidthNS is a
// positive decimal; an empty value creates one slice for the complete range.
type MeasurementSpec struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retentionPolicy"`
	Measurement     string `json:"measurement"`
	StartNS         string `json:"startNs"`
	EndNS           string `json:"endNs"`
	SliceWidthNS    string `json:"sliceWidthNs,omitempty"`
	OutputDirectory string `json:"outputDirectory"`
	TypePreserving  bool   `json:"typePreserving"`
	Lossy           bool   `json:"lossy"`
}

type TimeSlice struct {
	Ordinal    string `json:"ordinal"`
	QuantumID  string `json:"quantumId"`
	FragmentID string `json:"fragmentId"`
	StartNS    string `json:"startNs"`
	EndNS      string `json:"endNs"`
	PartPath   string `json:"partPath"`
	FinalPath  string `json:"finalPath"`
}

type Plan struct {
	JobID              string                   `json:"jobId"`
	Generation         exportlane.GenerationKey `json:"generation"`
	Spec               MeasurementSpec          `json:"spec"`
	Quanta             []exportlane.Quantum     `json:"quanta"`
	Slices             []TimeSlice              `json:"slices"`
	QueryDigestSHA256  string                   `json:"queryDigestSha256"`
	ManifestFragmentID string                   `json:"manifestFragmentId"`
	ManifestPartPath   string                   `json:"manifestPartPath"`
	ManifestFinalPath  string                   `json:"manifestFinalPath"`
}

func BuildPlan(jobID string, generation exportlane.GenerationKey, spec MeasurementSpec) (Plan, error) {
	if uuid.Validate(jobID) != nil || strings.TrimSpace(generation.ConnectionID) == "" ||
		strings.TrimSpace(generation.Generation) == "" || !validName(spec.Database) ||
		!validName(spec.RetentionPolicy) || !validName(spec.Measurement) ||
		strings.TrimSpace(spec.OutputDirectory) == "" {
		return Plan{}, ErrInvalidPlan
	}
	canonicalGeneration, _, err := canonicalPositive(generation.Generation)
	if err != nil {
		return Plan{}, err
	}
	generation.Generation = canonicalGeneration
	start, startNumber, err := canonicalSigned(spec.StartNS)
	if err != nil {
		return Plan{}, err
	}
	end, endNumber, err := canonicalSigned(spec.EndNS)
	if err != nil || startNumber.Cmp(endNumber) >= 0 {
		return Plan{}, ErrInvalidPlan
	}
	width := new(big.Int).Sub(endNumber, startNumber)
	if spec.SliceWidthNS != "" {
		canonical, number, widthErr := canonicalPositive(spec.SliceWidthNS)
		if widthErr != nil {
			return Plan{}, widthErr
		}
		spec.SliceWidthNS = canonical
		width = number
	}
	output, err := filepath.Abs(spec.OutputDirectory)
	if err != nil {
		return Plan{}, ErrInvalidPlan
	}
	spec.StartNS, spec.EndNS, spec.OutputDirectory = start, end, output

	qualified := quoteIdentifier(spec.Database) + "." + quoteIdentifier(spec.RetentionPolicy) + "." + quoteIdentifier(spec.Measurement)
	queries := []string{
		"SHOW TAG KEYS FROM " + qualified,
		"SHOW FIELD KEYS FROM " + qualified,
	}
	quanta := []exportlane.Quantum{
		metadataQuantum(tagKeysQuantumID, spec, queries[0]),
		metadataQuantum(fieldKeysQuantumID, spec, queries[1]),
	}

	slices := make([]TimeSlice, 0)
	for cursor, ordinal := new(big.Int).Set(startNumber), 0; cursor.Cmp(endNumber) < 0; ordinal++ {
		if ordinal >= maxPlanSlices {
			return Plan{}, ErrInvalidPlan
		}
		next := new(big.Int).Add(cursor, width)
		if next.Cmp(endNumber) > 0 {
			next.Set(endNumber)
		}
		ordinalText := fmt.Sprintf("%d", ordinal)
		quantumID := fmt.Sprintf("slice/%06d", ordinal)
		startText, endText := cursor.String(), next.String()
		fileName := fmt.Sprintf("data-%06d.lp.gz", ordinal)
		finalPath := filepath.Join(output, fileName)
		slice := TimeSlice{
			Ordinal: ordinalText, QuantumID: quantumID,
			FragmentID: deterministicUUID(jobID + "\x00DATA\x00" + ordinalText),
			StartNS:    startText, EndNS: endText,
			PartPath: finalPath + ".part", FinalPath: finalPath,
		}
		queryText := "SELECT * FROM " + qualified + " WHERE time >= " + startText +
			" AND time < " + endText + " GROUP BY *"
		queries = append(queries, queryText)
		quanta = append(quanta, exportlane.Quantum{
			ID: quantumID, Kind: exportlane.QuantumTimeSlice,
			Request: transport.AuthorizedReadQuery{
				Database: spec.Database, RetentionPolicy: spec.RetentionPolicy, Query: queryText,
			},
		})
		slices = append(slices, slice)
		cursor = next
	}
	manifestPath := filepath.Join(output, "manifest.json")
	return Plan{
		JobID: jobID, Generation: generation, Spec: spec, Quanta: quanta, Slices: slices,
		QueryDigestSHA256:  digestQueries(queries),
		ManifestFragmentID: deterministicUUID(jobID + "\x00MANIFEST"),
		ManifestPartPath:   manifestPath + ".part", ManifestFinalPath: manifestPath,
	}, nil
}

func (p Plan) LaneRequest() (exportlane.JobRequest, error) {
	if uuid.Validate(p.JobID) != nil || len(p.Quanta) == 0 || len(p.Slices) == 0 {
		return exportlane.JobRequest{}, ErrInvalidPlan
	}
	return exportlane.JobRequest{ID: p.JobID, Generation: p.Generation, Quanta: append([]exportlane.Quantum(nil), p.Quanta...)}, nil
}

func metadataQuantum(id string, spec MeasurementSpec, statement string) exportlane.Quantum {
	return exportlane.Quantum{
		ID: id, Kind: exportlane.QuantumMetadata,
		Request: transport.AuthorizedReadQuery{
			Database: spec.Database, RetentionPolicy: spec.RetentionPolicy, Query: statement,
		},
	}
}

func validName(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

func quoteIdentifier(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

func canonicalSigned(value string) (string, *big.Int, error) {
	if value == "" || strings.HasPrefix(value, "+") || value == "-0" ||
		(len(value) > 1 && value[0] == '0') || (len(value) > 2 && strings.HasPrefix(value, "-0")) {
		return "", nil, ErrInvalidPlan
	}
	number := new(big.Int)
	if _, ok := number.SetString(value, 10); !ok || !number.IsInt64() {
		return "", nil, ErrInvalidPlan
	}
	return number.String(), number, nil
}

func canonicalPositive(value string) (string, *big.Int, error) {
	if value == "" || strings.HasPrefix(value, "+") || (len(value) > 1 && value[0] == '0') {
		return "", nil, ErrInvalidPlan
	}
	number := new(big.Int)
	if _, ok := number.SetString(value, 10); !ok || number.Sign() <= 0 {
		return "", nil, ErrInvalidPlan
	}
	return number.String(), number, nil
}

func deterministicUUID(value string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(value)).String()
}

func digestQueries(queries []string) string {
	hash := sha256.New()
	for _, queryText := range queries {
		_, _ = fmt.Fprintf(hash, "%d:", len(queryText))
		_, _ = hash.Write([]byte(queryText))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
