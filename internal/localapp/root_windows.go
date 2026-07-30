//go:build windows

package localapp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

const privateDirectorySDDLPrefix = "D:P"

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
	return verifyPrivateDACL(path, userSID)
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

func verifyPrivateDACL(path, userSID string) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	if descriptor == nil {
		return errors.New("private data root has no security descriptor")
	}
	sddl := descriptor.String()
	if !strings.HasPrefix(sddl, privateDirectorySDDLPrefix) || !strings.Contains(sddl, ";;;SY)") || !strings.Contains(sddl, ";;;"+userSID+")") {
		return errors.New("private data root DACL verification failed")
	}
	return rejectReparsePoint(path)
}
