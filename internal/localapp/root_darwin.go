//go:build darwin

package localapp

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func PreparePrivateRoot() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil || base == "" || !filepath.IsAbs(base) {
		return "", errors.New("macOS Application Support directory is unavailable")
	}
	return prepareDarwinRoot(base)
}

func prepareDarwinRoot(applicationSupport string) (string, error) {
	if applicationSupport == "" || !filepath.IsAbs(applicationSupport) {
		return "", errors.New("macOS Application Support directory is invalid")
	}
	root, err := preparePrivateDirectory(filepath.Join(applicationSupport, "InfluxDesk"))
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return "", errors.New("macOS private data root has an unexpected owner")
	}
	return root, nil
}
