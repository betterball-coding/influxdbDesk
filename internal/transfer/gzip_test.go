package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWriteDurableGzipFinalizesAndValidatesBeforeRename(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	part := filepath.Join(dir, "chunk.lp.gz.part")
	final := filepath.Join(dir, "chunk.lp.gz")
	var order []string
	meta, err := WriteDurableGzip(context.Background(), part, final, func(w io.Writer) error {
		_, err := io.WriteString(w, "cpu,host=a value=1i 1700000000123456789\n")
		return err
	}, FinalizeHooks{
		BeforeRename: func(_ context.Context, got ArtifactMeta) error {
			order = append(order, "FINALIZING")
			if _, err := os.Stat(part); err != nil {
				t.Fatalf("part missing before rename: %v", err)
			}
			if got.UncompressedSize == 0 || got.SHA256 == "" {
				t.Fatalf("metadata=%+v", got)
			}
			return nil
		},
		AfterRename: func(_ context.Context, _ ArtifactMeta) error {
			order = append(order, "COMPLETE")
			_, err := os.Stat(final)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"FINALIZING", "COMPLETE"}) {
		t.Fatalf("order=%v", order)
	}
	validated, err := ValidateGzipArtifact(final)
	if err != nil {
		t.Fatal(err)
	}
	if validated.SHA256 != meta.SHA256 || validated.UncompressedSize != meta.UncompressedSize {
		t.Fatalf("validated=%+v meta=%+v", validated, meta)
	}
}

func TestValidateGzipRejectsTrailingData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	part := filepath.Join(dir, "chunk.part")
	final := filepath.Join(dir, "chunk.gz")
	if _, err := WriteDurableGzip(context.Background(), part, final,
		func(w io.Writer) error { _, err := io.WriteString(w, "value"); return err }, FinalizeHooks{}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(final, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("trailing")); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, err := ValidateGzipArtifact(final); !errors.Is(err, ErrInvalidGzip) {
		t.Fatalf("trailing data error=%v", err)
	}
}

func TestBeforeRenameFailureRemovesPart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	part := filepath.Join(dir, "chunk.part")
	final := filepath.Join(dir, "chunk.gz")
	want := errors.New("persist FINALIZING failed")
	_, err := WriteDurableGzip(context.Background(), part, final,
		func(w io.Writer) error { _, err := io.WriteString(w, "value"); return err },
		FinalizeHooks{BeforeRename: func(context.Context, ArtifactMeta) error { return want }})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(part); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part remains after failure: %v", err)
	}
}

func TestCommitSameVolumeReplaceReplacesExistingFile(t *testing.T) {
	directory := t.TempDir()
	partPath := filepath.Join(directory, "result.csv.part")
	finalPath := filepath.Join(directory, "result.csv")
	if err := os.WriteFile(partPath, []byte("new result"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, []byte("old result"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := CommitSameVolumeReplace(partPath, finalPath); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "new result" {
		t.Fatalf("content=%q", content)
	}
	if _, err := os.Stat(partPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part path still exists: %v", err)
	}
}
