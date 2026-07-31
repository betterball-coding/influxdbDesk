//go:build darwin

package main

import (
	_ "embed"

	"github.com/wailsapp/wails/v2/pkg/menu"
	"github.com/wailsapp/wails/v2/pkg/menu/keys"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

//go:embed build/appicon.png
var darwinAboutIcon []byte

const appCommandEvent = "app.command.v1"

func configurePlatform(result *options.App, app *App) {
	result.Mac = &mac.Options{
		TitleBar:                     mac.TitleBarDefault(),
		Appearance:                   mac.DefaultAppearance,
		DisableEscapeExitsFullscreen: true,
		About: &mac.AboutInfo{
			Title:   "InfluxDesk",
			Message: "InfluxDB 1.x 中文桌面工作台",
			Icon:    darwinAboutIcon,
		},
	}
	result.Menu = darwinApplicationMenu(app)
}

func darwinApplicationMenu(app *App) *menu.Menu {
	queryMenu := menu.NewMenu()
	queryMenu.AddText("新建查询", keys.CmdOrCtrl("n"), emitAppCommand(app, "NEW_QUERY"))
	queryMenu.AddText("执行查询", keys.CmdOrCtrl("\r"), emitAppCommand(app, "EXECUTE_QUERY"))

	navigationMenu := menu.NewMenu()
	navigationMenu.AddText("查询工作台", keys.CmdOrCtrl("1"), emitAppCommand(app, "SHOW_QUERY"))
	navigationMenu.AddText("连接管理", keys.CmdOrCtrl("2"), emitAppCommand(app, "SHOW_CONNECTIONS"))
	navigationMenu.AddText("数据传输", keys.CmdOrCtrl("3"), emitAppCommand(app, "SHOW_TASKS"))
	navigationMenu.AddSeparator()
	navigationMenu.AddText("设置", keys.CmdOrCtrl(","), emitAppCommand(app, "SHOW_SETTINGS"))

	return menu.NewMenuFromItems(
		menu.AppMenu(),
		menu.SubMenu("查询", queryMenu),
		menu.SubMenu("导航", navigationMenu),
		menu.EditMenu(),
		menu.WindowMenu(),
	)
}

func emitAppCommand(app *App, command string) menu.Callback {
	return func(_ *menu.CallbackData) {
		app.mu.RLock()
		ctx := app.ctx
		app.mu.RUnlock()
		if ctx != nil {
			wailsruntime.EventsEmit(ctx, appCommandEvent, command)
		}
	}
}
