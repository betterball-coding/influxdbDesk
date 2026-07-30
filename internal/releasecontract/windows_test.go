package releasecontract_test

import (
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const stableUpgradeCode = "{7F456FFB-A2DC-49D2-AF8C-F6438070B516}"

func TestWiXProductionContract(t *testing.T) {
	root := repositoryRoot(t)
	wxs := read(t, filepath.Join(root, "build", "windows", "wix", "Package.wxs"))
	decoder := xml.NewDecoder(strings.NewReader(wxs))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Package.wxs is not well-formed XML: %v", err)
		}
	}
	for _, required := range []string{
		stableUpgradeCode,
		`Scope="perMachine"`,
		`Platform="x64"`,
		`AllowSameVersionUpgrades="no"`,
		`Schedule="afterInstallInitialize"`,
		`ProgramFiles64Folder`,
		`WEBVIEW2_RUNTIME_VERSION`,
		`{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}`,
		`Bitness="always32"`,
	} {
		if !strings.Contains(wxs, required) {
			t.Errorf("Package.wxs is missing %s", required)
		}
	}
	if strings.Contains(strings.ToLower(wxs), "customaction") {
		t.Error("WebView2 or shutdown policy must not use an MSI custom action")
	}
}

func TestReleaseWorkflowRequiresRealSignedArtifacts(t *testing.T) {
	workflow := read(t, filepath.Join(repositoryRoot(t), ".github", "workflows", "windows-release.yml"))
	for _, required := range []string{
		"steps.locate-msi.outputs.path",
		"release/manifest.json",
		"release/manifest.sig",
		"if-no-files-found: error",
		"cmd\\releasetool verify",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("release workflow is missing %s", required)
		}
	}
	verificationScript := read(t, filepath.Join(repositoryRoot(t), "scripts", "verify-release.ps1"))
	if !strings.Contains(verificationScript, "Get-AuthenticodeSignature") || !strings.Contains(verificationScript, "TimeStamperCertificate") {
		t.Error("release verification does not check Authenticode and timestamping")
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate release contract test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func read(t *testing.T, path string) string {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}
