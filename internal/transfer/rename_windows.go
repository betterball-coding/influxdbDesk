//go:build windows

package transfer

import "golang.org/x/sys/windows"

func durableRename(from, to string) error {
	return moveFile(from, to, windows.MOVEFILE_WRITE_THROUGH)
}

func durableReplace(from, to string) error {
	return moveFile(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func moveFile(from, to string, flags uint32) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(fromPtr, toPtr, flags)
}
