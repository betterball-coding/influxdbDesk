//go:build !windows && !darwin

package main

import "github.com/wailsapp/wails/v2/pkg/options"

func configurePlatform(_ *options.App, _ *App) {}
