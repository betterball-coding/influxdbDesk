//go:build !windows

package localapp

import (
	"errors"
	"os"
	"path/filepath"
)

var ErrUnsupportedPlatform = errors.New("InfluxDesk production storage is supported only on Windows")

func PreparePrivateRoot() (string, error) {
	return "", ErrUnsupportedPlatform
}

// PreparePrivateSubdir exists for tests and tooling. Production initialization
// still fails closed in PreparePrivateRoot on non-Windows systems.
func PreparePrivateSubdir(root, name string) (string, error) {
	if filepath.Base(name) != name || name == "." || name == "" {
		return "", errors.New("invalid private subdirectory name")
	}
	path := filepath.Join(root, name)
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("private subdirectory cannot be a symlink")
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return "", errors.New("private data path is not a directory")
	}
	return path, nil
}
