package exportworker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdesk/influxdesk/internal/exportjob"
	"github.com/influxdesk/influxdesk/internal/query"
	"github.com/influxdesk/influxdesk/internal/transfer"
)

const ManifestSchemaV1 = "influxdesk.logical-export.v1"

type Manifest struct {
	SchemaVersion      string             `json:"schemaVersion"`
	ExportKind         string             `json:"exportKind"`
	Database           string             `json:"database"`
	RetentionPolicy    string             `json:"retentionPolicy"`
	Measurement        string             `json:"measurement"`
	Range              ManifestRange      `json:"range"`
	Schema             ManifestSchema     `json:"schema"`
	QueryDigestSHA256  string             `json:"queryDigestSha256"`
	ChecksumAlgorithm  string             `json:"checksumAlgorithm"`
	SnapshotConsistent bool               `json:"snapshotConsistent"`
	TypePreserving     bool               `json:"typePreserving"`
	Lossy              bool               `json:"lossy"`
	Warnings           []string           `json:"warnings"`
	Fragments          []ManifestFragment `json:"fragments"`
}

type ManifestRange struct {
	StartNS string `json:"startNs"`
	EndNS   string `json:"endNs"`
}

type ManifestSchema struct {
	TagKeys []string        `json:"tagKeys"`
	Fields  []ManifestField `json:"fields"`
}

type ManifestField struct {
	Name string           `json:"name"`
	Kind query.ScalarKind `json:"kind"`
}

type ManifestFragment struct {
	File             string `json:"file"`
	StartNS          string `json:"startNs"`
	EndNS            string `json:"endNs"`
	CompressedSize   string `json:"compressedSize"`
	UncompressedSize string `json:"uncompressedSize"`
	SHA256           string `json:"sha256"`
	PointCount       string `json:"pointCount,omitempty"`
}

func (w *Worker) recordCompleted(
	ctx context.Context,
	jobID string,
	plan Plan,
	slice TimeSlice,
	fragment ManifestFragment,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) error {
	w.mu.Lock()
	runtime := w.jobs[jobID]
	if runtime == nil {
		w.mu.Unlock()
		return ErrPlanNotRegistered
	}
	runtime.completed[slice.FragmentID] = fragment
	if len(runtime.completed) != len(plan.Slices) || runtime.manifest != nil {
		w.mu.Unlock()
		return nil
	}
	fragments := make([]ManifestFragment, 0, len(plan.Slices))
	for _, planned := range plan.Slices {
		completed, found := runtime.completed[planned.FragmentID]
		if !found {
			w.mu.Unlock()
			return ErrArtifactState
		}
		fragments = append(fragments, completed)
	}
	manifest := buildManifest(plan, fragments, tagKeys, fieldTypes)
	w.mu.Unlock()

	meta, err := w.writeManifest(ctx, jobID, plan, manifest)
	if err != nil {
		return err
	}
	w.mu.Lock()
	if runtime = w.jobs[jobID]; runtime != nil {
		manifestCopy, metaCopy := cloneManifest(manifest), meta
		runtime.manifest, runtime.manifestMeta = &manifestCopy, &metaCopy
	}
	w.mu.Unlock()
	return nil
}

func buildManifest(
	plan Plan,
	fragments []ManifestFragment,
	tagKeys map[string]struct{},
	fieldTypes map[string]query.ScalarKind,
) Manifest {
	tags := make([]string, 0, len(tagKeys))
	for key := range tagKeys {
		tags = append(tags, key)
	}
	sort.Strings(tags)
	fields := make([]ManifestField, 0, len(fieldTypes))
	for name, kind := range fieldTypes {
		fields = append(fields, ManifestField{Name: name, Kind: kind})
	}
	sort.Slice(fields, func(left, right int) bool { return fields[left].Name < fields[right].Name })
	return Manifest{
		SchemaVersion: ManifestSchemaV1, ExportKind: "LOGICAL_EXPORT",
		Database: plan.Spec.Database, RetentionPolicy: plan.Spec.RetentionPolicy,
		Measurement:       plan.Spec.Measurement,
		Range:             ManifestRange{StartNS: plan.Spec.StartNS, EndNS: plan.Spec.EndNS},
		Schema:            ManifestSchema{TagKeys: tags, Fields: fields},
		QueryDigestSHA256: plan.QueryDigestSHA256, ChecksumAlgorithm: "SHA-256",
		SnapshotConsistent: false, TypePreserving: plan.Spec.TypePreserving, Lossy: plan.Spec.Lossy,
		Warnings:  []string{"Logical export is not a backup or snapshot and is not point-in-time consistent."},
		Fragments: fragments,
	}
}

func (w *Worker) writeManifest(
	ctx context.Context,
	jobID string,
	plan Plan,
	manifest Manifest,
) (meta transfer.ArtifactMeta, err error) {
	if err := validateManifest(manifest, plan); err != nil {
		return transfer.ArtifactMeta{}, err
	}
	existing, getErr := w.fragments.GetFragment(ctx, plan.ManifestFragmentID)
	if getErr == nil {
		if existing.State != exportjob.FragmentCorrupt {
			return transfer.ArtifactMeta{}, ErrArtifactState
		}
		_ = os.Remove(plan.ManifestPartPath)
		if _, err := w.fragments.RetryCorruptFragment(ctx, plan.ManifestFragmentID, plan.ManifestPartPath); err != nil {
			return transfer.ArtifactMeta{}, err
		}
	} else if errors.Is(getErr, exportjob.ErrFragmentNotFound) {
		if _, err := w.fragments.BeginFragment(ctx, exportjob.BeginFragmentRequest{
			FragmentID: plan.ManifestFragmentID, JobID: jobID,
			Ordinal: strconv.Itoa(len(plan.Slices)), Kind: exportjob.FragmentManifest,
			PartPath: plan.ManifestPartPath,
		}); err != nil {
			return transfer.ArtifactMeta{}, err
		}
	} else {
		return transfer.ArtifactMeta{}, getErr
	}
	committed := false
	defer func() {
		if err != nil && !committed {
			_ = w.reservation.Abort(context.WithoutCancel(ctx), jobID, plan.ManifestFragmentID)
		}
	}()

	data, err := json.Marshal(manifest)
	if err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	if err := w.reservation.Reserve(ctx, jobID, plan.ManifestFragmentID, transfer.ReservationExtentBytes); err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	partAbs, err := filepath.Abs(plan.ManifestPartPath)
	if err != nil {
		return transfer.ArtifactMeta{}, err
	}
	finalAbs, err := filepath.Abs(plan.ManifestFinalPath)
	if err != nil {
		return transfer.ArtifactMeta{}, err
	}
	if _, err := os.Lstat(finalAbs); err == nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, ErrArtifactState
	} else if !errors.Is(err, os.ErrNotExist) {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	file, err := os.OpenFile(partAbs, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	renamed := false
	defer func() {
		if file != nil {
			_ = file.Close()
		}
		if err != nil && !renamed {
			_ = os.Remove(partAbs)
		}
	}()
	buffered := bufio.NewWriter(file)
	if _, err = buffered.Write(data); err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	if err = buffered.Flush(); err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	if err = file.Sync(); err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	if err = file.Close(); err != nil {
		file = nil
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	file = nil

	validated, err := os.ReadFile(partAbs)
	if err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	var decoded Manifest
	if err := json.Unmarshal(validated, &decoded); err != nil || !reflect.DeepEqual(decoded, manifest) {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, ErrArtifactState
	}
	digest := sha256.Sum256(validated)
	meta = transfer.ArtifactMeta{
		PartPath: partAbs, FinalPath: finalAbs,
		CompressedSize: int64(len(validated)), UncompressedSize: int64(len(validated)),
		SHA256: hex.EncodeToString(digest[:]),
	}
	if _, err = w.fragments.MarkFragmentFinalizing(ctx, exportjob.FinalizeFragmentRequest{
		FragmentID: plan.ManifestFragmentID, PartPath: partAbs, FinalPath: finalAbs,
		CompressedSize:   strconv.FormatInt(meta.CompressedSize, 10),
		UncompressedSize: strconv.FormatInt(meta.UncompressedSize, 10), ChecksumSHA256: meta.SHA256,
	}); err != nil {
		w.markWritingCorrupt(ctx, plan.ManifestFragmentID)
		return transfer.ArtifactMeta{}, err
	}
	if err = transfer.CommitSameVolumeRename(partAbs, finalAbs); err != nil {
		return transfer.ArtifactMeta{}, err
	}
	renamed = true
	committed = true
	if _, err = w.fragments.CompleteFragment(ctx, plan.ManifestFragmentID); err != nil {
		return meta, err
	}
	if err = w.reservation.Settle(ctx, jobID, plan.ManifestFragmentID, meta.CompressedSize); err != nil {
		return meta, err
	}
	return meta, nil
}

func validateManifest(manifest Manifest, plan Plan) error {
	if manifest.SchemaVersion != ManifestSchemaV1 || manifest.ExportKind != "LOGICAL_EXPORT" ||
		manifest.Database != plan.Spec.Database || manifest.RetentionPolicy != plan.Spec.RetentionPolicy ||
		manifest.Measurement != plan.Spec.Measurement || manifest.SnapshotConsistent ||
		manifest.Range.StartNS != plan.Spec.StartNS || manifest.Range.EndNS != plan.Spec.EndNS ||
		manifest.TypePreserving != plan.Spec.TypePreserving || manifest.Lossy != plan.Spec.Lossy ||
		manifest.QueryDigestSHA256 != plan.QueryDigestSHA256 || manifest.ChecksumAlgorithm != "SHA-256" ||
		len(manifest.Fragments) != len(plan.Slices) {
		return ErrArtifactState
	}
	for index, fragment := range manifest.Fragments {
		planned := plan.Slices[index]
		if fragment.File != filepath.Base(planned.FinalPath) || fragment.StartNS != planned.StartNS ||
			fragment.EndNS != planned.EndNS || !validSHA256(fragment.SHA256) ||
			!validUnsigned(fragment.CompressedSize) || !validUnsigned(fragment.UncompressedSize) ||
			(fragment.PointCount != "" && !validUnsigned(fragment.PointCount)) {
			return ErrArtifactState
		}
	}
	return nil
}

func manifestFragmentFromStored(fragment exportjob.Fragment) (ManifestFragment, error) {
	if fragment.FinalPath == nil || fragment.StartNS == nil || fragment.EndNS == nil ||
		fragment.CompressedSize == nil || fragment.UncompressedSize == nil || fragment.ChecksumSHA256 == nil {
		return ManifestFragment{}, ErrArtifactState
	}
	return ManifestFragment{
		File: filepath.Base(*fragment.FinalPath), StartNS: *fragment.StartNS, EndNS: *fragment.EndNS,
		CompressedSize: *fragment.CompressedSize, UncompressedSize: *fragment.UncompressedSize,
		SHA256: *fragment.ChecksumSHA256,
	}, nil
}

func cloneManifest(value Manifest) Manifest {
	encoded, _ := json.Marshal(value)
	var result Manifest
	_ = json.Unmarshal(encoded, &result)
	return result
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validUnsigned(value string) bool {
	if value == "" || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") ||
		(len(value) > 1 && value[0] == '0') {
		return false
	}
	number := new(big.Int)
	_, ok := number.SetString(value, 10)
	return ok
}
