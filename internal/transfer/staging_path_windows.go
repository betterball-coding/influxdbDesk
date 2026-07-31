//go:build windows

package transfer

import (
	"errors"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func sameStagingPath(left, right string) bool {
	left, leftErr := longStagingPath(left)
	right, rightErr := longStagingPath(right)
	return leftErr == nil && rightErr == nil &&
		strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}

// GetLongPathName expands 8.3 path segments without following junctions. This
// keeps the reparse-point check strict while accepting Windows runner paths
// such as C:\Users\RUNNER~1 and their equivalent long form.
func longStagingPath(value string) (string, error) {
	pointer, err := windows.UTF16PtrFromString(value)
	if err != nil {
		return "", err
	}
	size := uint32(windows.MAX_PATH)
	for {
		buffer := make([]uint16, size)
		length, err := windows.GetLongPathName(pointer, &buffer[0], size)
		if err != nil {
			return "", err
		}
		if length == 0 {
			return "", errors.New("empty Windows long path")
		}
		if length < size {
			return windows.UTF16ToString(buffer[:length]), nil
		}
		size = length + 1
	}
}
