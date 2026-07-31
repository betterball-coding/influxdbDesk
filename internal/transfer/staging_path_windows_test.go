//go:build windows

package transfer

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestSameStagingPathAcceptsWindowsShortName(t *testing.T) {
	directory := t.TempDir()
	pointer, err := windows.UTF16PtrFromString(directory)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]uint16, 32768)
	length, err := windows.GetShortPathName(pointer, &buffer[0], uint32(len(buffer)))
	if err != nil || length == 0 || length >= uint32(len(buffer)) {
		t.Skip("Windows volume does not expose an 8.3 short path")
	}
	shortPath := windows.UTF16ToString(buffer[:length])
	if strings.EqualFold(filepath.Clean(shortPath), filepath.Clean(directory)) {
		t.Skip("Windows volume did not shorten the temporary path")
	}
	evaluated, err := filepath.EvalSymlinks(shortPath)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStagingPath(evaluated, shortPath) {
		t.Fatalf("short path %q did not match evaluated path %q", shortPath, evaluated)
	}
}
