package architecture_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestInfluxNetworkBoundary prevents new packages from bypassing the sealed
// transport request types. Update signing code is intentionally network-free.
func TestInfluxNetworkBoundary(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate architecture test")
	}
	internalRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), ".."))
	err := filepath.WalkDir(internalRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(internalRoot, path)
		if err != nil {
			return err
		}
		if firstPathElement(relative) == "transport" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			if imported.Path.Value == `"net/http"` {
				t.Errorf("%s imports net/http outside internal/transport", relative)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func firstPathElement(path string) string {
	parts := strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}
