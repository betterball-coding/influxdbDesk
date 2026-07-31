//go:build windows

package localapp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

const privateDirectoryFileAllAccess windows.ACCESS_MASK = 0x1f01ff

// PreparePrivateRoot creates and verifies %LOCALAPPDATA%\InfluxDesk before any sensitive file is opened.
func PreparePrivateRoot() (string, error) {
	base := os.Getenv("LOCALAPPDATA")
	if base == "" {
		return "", errors.New("LOCALAPPDATA is not set")
	}
	root := filepath.Join(base, "InfluxDesk")
	if err := rejectReparsePoint(root); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := rejectReparsePoint(root); err != nil {
		return "", err
	}
	if err := requireDirectory(root); err != nil {
		return "", err
	}

	if err := applyPrivateDACL(root); err != nil {
		return "", err
	}
	return root, nil
}

func applyPrivateDACL(path string) error {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return err
	}
	userSID := user.User.Sid.String()
	sddl := fmt.Sprintf("D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;%s)", userSID)
	descriptor, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	flags := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION)
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, flags, nil, nil, dacl, nil); err != nil {
		return err
	}
	return verifyPrivateDACL(path, user.User.Sid)
}

func PreparePrivateSubdir(root, name string) (string, error) {
	if filepath.Base(name) != name || name == "." || name == "" {
		return "", errors.New("invalid private subdirectory name")
	}
	if err := rejectReparsePoint(root); err != nil {
		return "", err
	}
	path := filepath.Join(root, name)
	if err := rejectReparsePoint(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := rejectReparsePoint(path); err != nil {
		return "", err
	}
	if err := requireDirectory(path); err != nil {
		return "", err
	}
	if err := applyPrivateDACL(path); err != nil {
		return "", err
	}
	return path, nil
}

func rejectReparsePoint(path string) error {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(ptr)
	if err != nil {
		return err
	}
	if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("private data root cannot be a reparse point or junction")
	}
	return nil
}

func requireDirectory(path string) error {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(ptr)
	if err != nil {
		return err
	}
	if attributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return errors.New("private data path is not a directory")
	}
	return nil
}

func verifyPrivateDACL(path string, userSID *windows.SID) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	if descriptor == nil {
		return errors.New("private data root has no security descriptor")
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("private data root DACL verification failed")
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || dacl == nil || defaulted || dacl.AceCount != 2 {
		return errors.New("private data root DACL verification failed")
	}
	systemSID, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	var userAllowed, systemAllowed bool
	for index := uint16(0); index < dacl.AceCount; index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			return err
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags != windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE ||
			ace.Mask != privateDirectoryFileAllAccess {
			return errors.New("private data root DACL verification failed")
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		switch {
		case sid.Equals(userSID) && !userAllowed:
			userAllowed = true
		case sid.Equals(systemSID) && !systemAllowed:
			systemAllowed = true
		default:
			return errors.New("private data root DACL verification failed")
		}
	}
	if !userAllowed || !systemAllowed {
		return errors.New("private data root DACL verification failed")
	}
	return rejectReparsePoint(path)
}
