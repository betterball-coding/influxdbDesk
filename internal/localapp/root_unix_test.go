//go:build !windows

package localapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreparePrivateSubdirRejectsExistingFileWithoutChangingMode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "logs")
	if err := os.WriteFile(path, []byte("do not modify"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PreparePrivateSubdir(root, "logs"); err == nil {
		t.Fatal("expected existing file rejection")
	}
	after, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode() != after.Mode() {
		t.Fatalf("file mode changed from %v to %v", before.Mode(), after.Mode())
	}
}
