//go:build !windows

package transfer

import (
	"os"
	"path/filepath"
)

func durableRename(from, to string) error {
	return durableReplace(from, to)
}

func durableReplace(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(to))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
