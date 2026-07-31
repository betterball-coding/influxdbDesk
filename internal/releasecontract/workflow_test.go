package releasecontract_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCIUsesSharedAndNativePlatformGates(t *testing.T) {
	workflow := read(t, filepath.Join(repositoryRoot(t), ".github", "workflows", "ci.yml"))
	for _, required := range []string{
		"branches:\n      - main",
		"runs-on: ubuntu-24.04",
		"runs-on: windows-2022",
		"runs-on: macos-14",
		"npm run test:e2e",
		"go test -race",
		"GOOS=windows GOARCH=amd64",
		"GOOS=darwin GOARCH=arm64",
		"INFLUXDESK_MACOS_KEYCHAIN_TEST",
		"darwin/universal",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("CI workflow is missing %s", required)
		}
	}
	if strings.Contains(workflow, "codex/") {
		t.Fatal("CI must not depend on a temporary platform branch")
	}
}

func TestProductionReleaseUsesOneImmutableTaggedSource(t *testing.T) {
	workflow := read(t, filepath.Join(repositoryRoot(t), ".github", "workflows", "release.yml"))
	for _, required := range []string{
		"tags:\n      - 'v*.*.*'",
		`source_version="$(node -p "require('./wails.json').info.productVersion")"`,
		`git merge-base --is-ancestor "$GITHUB_SHA" origin/main`,
		"runs-on: windows-2022",
		"runs-on: macos-14",
		"environment: production",
		"SIGN_COMMAND",
		"UPDATE_PRIVATE_KEY_BASE64",
		"MACOS_CERTIFICATE_P12_BASE64",
		"INFLUXDESK_REQUIRE_PRODUCTION_SIGNING",
		"sign-windows-artifact.ps1",
		"spctl --assess",
		"--verify-tag",
		"--draft",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("production release workflow is missing %s", required)
		}
	}
	for _, forbidden := range []string{"codex/", "v1-release", "test \"$RELEASE_VERSION\" = \"1.0.0\"", "signing=ad-hoc"} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("production release workflow contains forbidden temporary contract %s", forbidden)
		}
	}
}

func TestLegacyPlatformReleaseWorkflowsAreRemoved(t *testing.T) {
	root := filepath.Join(repositoryRoot(t), ".github", "workflows")
	for _, name := range []string{"v1-release.yml", "preview-release.yml", "windows-release.yml", "macos-release.yml"} {
		_, err := os.Stat(filepath.Join(root, name))
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Errorf("legacy workflow must be removed: %s", name)
		}
	}
}

func TestWailsPlatformOptionsAreBuildTagged(t *testing.T) {
	root := repositoryRoot(t)
	mainSource := read(t, filepath.Join(root, "main.go"))
	if strings.Contains(mainSource, "windowsRuntimeOptions") || strings.Contains(mainSource, "options/mac") {
		t.Fatal("shared main.go contains platform-specific Wails options")
	}

	contracts := map[string][]string{
		"platform_windows.go": {"//go:build windows", "func configurePlatform", "result.Windows = windowsRuntimeOptions()"},
		"platform_darwin.go":  {"//go:build darwin", "func configurePlatform", "result.Mac = &mac.Options"},
		"platform_other.go":   {"//go:build !windows && !darwin", "func configurePlatform"},
	}
	for name, requiredValues := range contracts {
		source := read(t, filepath.Join(root, name))
		for _, required := range requiredValues {
			if !strings.Contains(source, required) {
				t.Errorf("%s is missing %s", name, required)
			}
		}
	}
}
