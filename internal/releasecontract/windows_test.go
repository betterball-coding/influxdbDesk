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
		`WindowsBuild &gt;= 22000`,
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

func TestWindows10NSISContract(t *testing.T) {
	root := repositoryRoot(t)
	project := read(t, filepath.Join(root, "build", "windows", "installer", "project.nsi"))
	tools := read(t, filepath.Join(root, "build", "windows", "installer", "wails_tools.nsh"))
	buildScript := read(t, filepath.Join(root, "scripts", "build-win10.sh"))

	for _, required := range []string{
		`INFLUXDESK_MIN_WINDOWS_BUILD 16299`,
		`AtLeastBuild`,
		`INFLUXDESK_MIN_WEBVIEW2_VERSION "94.0.992.31"`,
		`VersionCompare`,
		`influxdesk.webview2runtime`,
		`ExecWait`,
		`MUI_LANGUAGE "SimpChinese"`,
		`win10-x64-installer.exe`,
	} {
		if !strings.Contains(project, required) {
			t.Errorf("project.nsi is missing %s", required)
		}
	}
	for _, required := range []string{`${AtLeastWin10}`, `${IsNativeAMD64}`} {
		if !strings.Contains(tools, required) {
			t.Errorf("wails_tools.nsh is missing %s", required)
		}
	}
	for _, required := range []string{
		`-platform windows/amd64`,
		`-webview2 embed`,
		`-o InfluxDesk-win10-x64.exe`,
	} {
		if !strings.Contains(buildScript, required) {
			t.Errorf("build-win10.sh is missing %s", required)
		}
	}
}

func TestWindowsManifestDeclaresWindows10Compatibility(t *testing.T) {
	manifest := read(t, filepath.Join(repositoryRoot(t), "build", "windows", "wails.exe.manifest"))
	decoder := xml.NewDecoder(strings.NewReader(manifest))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("wails.exe.manifest is not well-formed XML: %v", err)
		}
	}
	if !strings.Contains(manifest, `{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}`) {
		t.Fatal("Windows 10/11 supportedOS GUID is missing")
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
