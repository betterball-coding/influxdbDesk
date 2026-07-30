package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestInspectImportSourceStreamsExactIdentity(t *testing.T) {
	payload := []byte("cpu,host=edge value=9007199254740993i 1735689600000000001\n")
	path := filepath.Join(t.TempDir(), "metrics.lp")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	app := NewApp()
	app.ctx = context.Background()
	app.ready = true

	got, err := app.InspectImportSource(path)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(payload)
	if got.SourcePath != path || got.DisplayName != "metrics.lp" ||
		got.SizeBytes != strconv.Itoa(len(payload)) || got.SHA256 != hex.EncodeToString(want[:]) {
		t.Fatalf("unexpected inspection: %+v", got)
	}
}

func TestInspectImportSourceRejectsNonCanonicalPathAndCanceledContext(t *testing.T) {
	app := NewApp()
	app.ctx = context.Background()
	app.ready = true
	if _, err := app.InspectImportSource("relative.lp"); err == nil || err.Error() != "IMPORT_SOURCE_PATH_INVALID" {
		t.Fatalf("relative path error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "metrics.lp")
	if err := os.WriteFile(path, []byte("m f=1i 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app.ctx = ctx
	if _, err := app.InspectImportSource(path); err == nil || err != context.Canceled {
		t.Fatalf("canceled inspection error = %v", err)
	}
}
