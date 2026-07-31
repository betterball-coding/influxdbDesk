package main

import (
	"strings"
	"testing"

	"github.com/wailsapp/wails/v2/pkg/options/windows"
)

func TestWindowsRuntimeOptionsKeepWin10Contract(t *testing.T) {
	options := windowsRuntimeOptions()
	if options.BackdropType != windows.None {
		t.Fatalf("BackdropType = %d, want windows.None", options.BackdropType)
	}
	if options.WebviewIsTransparent || options.WindowIsTranslucent {
		t.Fatal("Win10 build must not enable translucent Win11 presentation paths")
	}
	if options.Messages == nil {
		t.Fatal("WebView2 messages must be configured")
	}

	for name, message := range map[string]string{
		"InstallationRequired": options.Messages.InstallationRequired,
		"UpdateRequired":       options.Messages.UpdateRequired,
		"FailedToInstall":      options.Messages.FailedToInstall,
		"DownloadPage":         options.Messages.DownloadPage,
		"ContactAdmin":         options.Messages.ContactAdmin,
	} {
		if strings.TrimSpace(message) == "" {
			t.Errorf("%s must not be empty", name)
		}
	}
	if !strings.Contains(options.Messages.InstallationRequired, minimumWebView2Runtime) ||
		!strings.Contains(options.Messages.UpdateRequired, minimumWebView2Runtime) {
		t.Fatalf("WebView2 minimum %s must be visible in install and update prompts", minimumWebView2Runtime)
	}
}
