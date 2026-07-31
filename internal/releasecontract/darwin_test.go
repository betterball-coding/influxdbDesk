package releasecontract_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDarwinBundleContract(t *testing.T) {
	root := repositoryRoot(t)
	for _, name := range []string{"Info.plist", "Info.dev.plist"} {
		plist := read(t, filepath.Join(root, "build", "darwin", name))
		for _, required := range []string{
			"com.betterballcoding.influxdesk",
			"<string>12.0</string>",
			"public.app-category.developer-tools",
			"NSHighResolutionCapable",
			"NSAllowsLocalNetworking",
		} {
			if !strings.Contains(plist, required) {
				t.Errorf("%s is missing %s", name, required)
			}
		}
	}
	icon, err := os.Stat(filepath.Join(root, "build", "appicon.png"))
	if err != nil || icon.Size() == 0 {
		t.Fatal("macOS source icon is missing")
	}
}

func TestDarwinBuildAndCIContract(t *testing.T) {
	root := repositoryRoot(t)
	scriptPath := filepath.Join(root, "scripts", "build-macos.sh")
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatal("build-macos.sh is not executable")
	}
	script := read(t, scriptPath)
	for _, required := range []string{
		`"$(uname -s)" != "Darwin"`,
		`darwin/$target_arch`,
		"universal|arm64|amd64",
		"lipo -archs",
		"codesign --verify",
		"notarytool submit",
		"ditto -c -k",
		"hdiutil create",
		"SHA256SUMS.txt",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("macOS build script is missing %s", required)
		}
	}
	workflow := read(t, filepath.Join(root, ".github", "workflows", "macos-release.yml"))
	for _, required := range []string{
		"runs-on: macos-14",
		"build-macos.sh universal",
		"INFLUXDESK_MACOS_KEYCHAIN_TEST",
		"release/macos/*.dmg",
		"release/macos/*.zip",
		"if-no-files-found: error",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("macOS workflow is missing %s", required)
		}
	}
	if strings.Contains(strings.ToLower(workflow), "webview2") || strings.Contains(strings.ToLower(workflow), ".msi") {
		t.Fatal("macOS workflow contains Windows packaging assumptions")
	}
}

func TestDarwinSecurityContractUsesApplicationSupportAndKeychain(t *testing.T) {
	root := repositoryRoot(t)
	localRoot := read(t, filepath.Join(root, "internal", "localapp", "root_darwin.go"))
	if !strings.Contains(localRoot, "os.UserConfigDir()") || !strings.Contains(localRoot, `"InfluxDesk"`) {
		t.Fatal("macOS private root does not use Application Support")
	}
	keychain := read(t, filepath.Join(root, "internal", "credential", "store_darwin.go"))
	for _, required := range []string{
		"-framework Security",
		"SecKeychainAddGenericPassword",
		"SecKeychainItemModifyAttributesAndData",
		"SecKeychainItemDelete",
		"influxdesk_secure_free",
		"com.betterballcoding.influxdesk",
	} {
		if !strings.Contains(keychain, required) {
			t.Errorf("macOS Keychain backend is missing %s", required)
		}
	}
}
