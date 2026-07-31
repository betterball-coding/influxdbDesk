//go:build windows

package main

import (
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/windows"
)

const minimumWebView2Runtime = "94.0.992.31"

func configurePlatform(result *options.App, _ *App) {
	result.Windows = windowsRuntimeOptions()
}

func windowsRuntimeOptions() *windows.Options {
	return &windows.Options{
		// Keep the window on the Win10-compatible path even when built on Win11.
		BackdropType: windows.None,
		Messages: &windows.Messages{
			InstallationRequired: "运行 InfluxDesk 需要 Microsoft Edge WebView2 Runtime（最低版本 " + minimumWebView2Runtime + "）。",
			UpdateRequired:       "当前 WebView2 Runtime 版本过旧；InfluxDesk 最低要求 " + minimumWebView2Runtime + "。",
			MissingRequirements:  "缺少运行环境",
			Webview2NotInstalled: "未安装可用的 WebView2 Runtime",
			Error:                "运行环境错误",
			FailedToInstall:      "WebView2 Runtime 安装或更新失败。请检查网络和管理员策略后重试。",
			DownloadPage:         "InfluxDesk 需要 WebView2 Runtime。点击“确定”打开 Microsoft 下载页面。最低版本：",
			PressOKToInstall:     "点击“确定”后将从 Microsoft 下载并静默安装，请稍候。",
			ContactAdmin:         "InfluxDesk 需要 WebView2 Runtime，请联系系统管理员安装或更新。",
			InvalidFixedWebview2: "指定的 WebView2 Runtime 无效，请检查目录和最低版本。",
			WebView2ProcessCrash: "WebView2 进程已崩溃，请重新启动 InfluxDesk。",
		},
	}
}
