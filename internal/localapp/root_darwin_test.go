//go:build darwin

package localapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareDarwinRootUsesPrivateApplicationSupportDirectory(t *testing.T) {
	base := t.TempDir()
	root, err := prepareDarwinRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	if root != filepath.Join(base, "InfluxDesk") {
		t.Fatalf("unexpected root %q", root)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("root mode is %v", info.Mode())
	}
}

func TestPrepareDarwinRootRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(base, "InfluxDesk")); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareDarwinRoot(base); err == nil {
		t.Fatal("expected symlink rejection")
	}
}
