//go:build darwin

package main

import (
	"testing"

	"github.com/wailsapp/wails/v2/pkg/menu"
)

func TestDarwinApplicationOptionsKeepNativeChromeAndMenus(t *testing.T) {
	result := applicationOptions(NewApp())
	if result.Frameless {
		t.Fatal("macOS must retain native window chrome")
	}
	if result.Mac == nil || result.Mac.TitleBar == nil || result.Mac.TitleBar.HideTitleBar {
		t.Fatal("macOS native title bar is not configured")
	}
	if result.Mac.About == nil || result.Mac.About.Title != "InfluxDesk" || len(result.Mac.About.Icon) == 0 {
		t.Fatal("macOS About metadata is incomplete")
	}
	if result.Menu == nil || len(result.Menu.Items) != 5 {
		t.Fatal("macOS application menu is incomplete")
	}
	if result.Menu.Items[0].Role != menu.AppMenuRole || result.Menu.Items[3].Role != menu.EditMenuRole || result.Menu.Items[4].Role != menu.WindowMenuRole {
		t.Fatal("macOS system menu roles were not preserved")
	}
	query := result.Menu.Items[1]
	if query.Label != "查询" || query.SubMenu == nil || len(query.SubMenu.Items) != 2 {
		t.Fatal("macOS query menu is incomplete")
	}
	if accelerator := query.SubMenu.Items[1].Accelerator; accelerator == nil || accelerator.Key != "\r" {
		t.Fatal("macOS execute accelerator must use Cocoa's return key equivalent")
	}
	navigation := result.Menu.Items[2]
	if navigation.Label != "导航" || navigation.SubMenu == nil || len(navigation.SubMenu.Items) != 5 {
		t.Fatal("macOS navigation menu is incomplete")
	}
}
