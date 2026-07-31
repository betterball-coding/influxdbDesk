package main

import (
	"embed"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	app := NewApp()

	err := wails.Run(applicationOptions(app))

	if err != nil {
		println("Error:", err.Error())
	}
}

func applicationOptions(app *App) *options.App {
	result := &options.App{
		Title:     "InfluxDesk",
		Width:     1440,
		Height:    900,
		MinWidth:  1180,
		MinHeight: 720,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 246, G: 247, B: 249, A: 1},
		Windows:          windowsRuntimeOptions(),
		OnStartup:        app.startup,
		OnShutdown:       app.shutdown,
		Bind: []interface{}{
			app,
		},
	}
	configurePlatform(result, app)
	return result
}
