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
		"INFLUXDESK_REQUIRE_PRODUCTION_SIGNING",
		"MACOS_NOTARY_KEYCHAIN",
		"stapler validate",
		"spctl --assess",
		"ditto -c -k",
		"hdiutil create",
		"SHA256SUMS.txt",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("macOS build script is missing %s", required)
		}
	}
	workflow := read(t, filepath.Join(root, ".github", "workflows", "ci.yml"))
	for _, required := range []string{
		"runs-on: macos-14",
		"wails build -platform darwin/universal",
		"INFLUXDESK_MACOS_KEYCHAIN_TEST",
		"build/bin/InfluxDesk.app",
		"if-no-files-found: error",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("macOS workflow is missing %s", required)
		}
	}
	releaseWorkflow := read(t, filepath.Join(root, ".github", "workflows", "release.yml"))
	for _, required := range []string{
		"MACOS_CERTIFICATE_P12_BASE64",
		"MACOS_SIGN_IDENTITY",
		"APPLE_APP_PASSWORD",
		"INFLUXDESK_REQUIRE_PRODUCTION_SIGNING",
		"notarization=accepted and stapled",
		"scripts/build-macos.sh universal",
		"release/macos/*",
	} {
		if !strings.Contains(releaseWorkflow, required) {
			t.Errorf("production macOS workflow is missing %s", required)
		}
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
