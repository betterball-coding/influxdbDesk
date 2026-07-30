//go:build windows

package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenUsesAValidWindowsFileURI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "influxdesk.db")
	database, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database was not created at requested path: %v", err)
	}
}
