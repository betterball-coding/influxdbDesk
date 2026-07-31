package releasecontract_test

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestV1ReleaseWorkflowBuildsDistinctNativePackages(t *testing.T) {
	root := repositoryRoot(t)
	workflow := read(t, filepath.Join(root, ".github", "workflows", "v1-release.yml"))
	for _, required := range []string{
		"runs-on: windows-2022",
		"runs-on: macos-14",
		"-webview2 embed",
		"tags:",
		"- v1.0.0",
		"InfluxDesk-${{ inputs.version || '1.0.0' }}-windows-installers",
		"win10-x64-installer.exe",
		"win11-x64-installer.msi",
		`Get-ChildItem .\build\windows\wix\bin -Filter *.msi -Recurse`,
		"build-macos.sh universal",
		"macos-universal.dmg",
		"macos-universal.zip",
		"sha256sum -c SHA256SUMS.txt",
		"gh release create",
		"--latest",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("v1 release workflow is missing %s", required)
		}
	}
	if strings.Contains(workflow, "--prerelease") {
		t.Fatal("v1 release must not be marked as a prerelease")
	}
}
func TestV1ReleaseNotesDiscloseSigningBoundaries(t *testing.T) {
	notes := read(t, filepath.Join(repositoryRoot(t), "docs", "releases", "v1.0.0.md"))
	for _, required := range []string{
		"Windows EXE/MSI 未做 Authenticode 签名",
		"仅做 ad-hoc 签名",
		"未提交 Apple 公证",
		"浏览器测试不等同于 Windows/macOS 原生运行验收",
	} {
		if !strings.Contains(notes, required) {
			t.Errorf("v1 release notes are missing boundary %s", required)
		}
	}
}
