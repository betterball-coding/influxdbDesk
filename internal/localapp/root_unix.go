//go:build !windows

package localapp

import (
	"errors"
	"os"
	"path/filepath"
)

func PreparePrivateSubdir(root, name string) (string, error) {
	if filepath.Base(name) != name || name == "." || name == "" {
		return "", errors.New("invalid private subdirectory name")
	}
	return preparePrivateDirectory(filepath.Join(root, name))
}

func preparePrivateDirectory(path string) (string, error) {
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("private data path cannot be a symlink")
	}
	if err == nil && !info.IsDir() {
		return "", errors.New("private data path is not a directory")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", err
	}
	info, err = os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("private data path is not a directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("private data path permissions are too broad")
	}
	return path, nil
}
