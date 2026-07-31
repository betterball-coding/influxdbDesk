# macOS 构建与发布

InfluxDesk 的 macOS 目标使用 Wails v2.13.0、WKWebView 和原生 Keychain Services。应用保留现有中文 InfluxDB 1.x 编辑器工作流，运行数据位于 `~/Library/Application Support/InfluxDesk`，连接认证信息不会写入 SQLite 或普通日志。

## 支持范围

- macOS 12 Monterey 或更高版本。
- Apple Silicon `arm64`、Intel `amd64`，默认产出同时包含两种架构的 universal 应用。
- `.app`、ZIP 和 DMG。DMG 包含 `InfluxDesk.app` 与 `/Applications` 快捷入口。
- Wails 使用仓库根目录的 `build/appicon.png` 生成 `iconfile.icns`。
- macOS 使用 WKWebView，不依赖或打包 Microsoft WebView2。

当前 Windows Ed25519 更新清单绑定 `windows-x64` 和 MSI。macOS 构建不会复用该安装/更新协议，也没有未接线的自动更新入口；升级方式是安装新 `.app`，本地数据库和 Keychain 条目保持在用户目录中。

## 前置条件

在真实 Mac 上安装并选择 Xcode Command Line Tools：

```bash
xcode-select --install
xcode-select -p
```

工具链版本与仓库一致：

```text
Go 1.25.12
Node.js 22.22.2
Wails 2.13.0
```

安装固定版本 Wails CLI：

```bash
go install github.com/wailsapp/wails/v2/cmd/wails@v2.13.0
```

## 本机构建

默认构建 universal 包，并运行前端测试、前端生产构建、Go 测试和 `go vet`：

```bash
scripts/build-macos.sh universal
```

也可构建单架构包：

```bash
scripts/build-macos.sh arm64
scripts/build-macos.sh amd64
```

产物写入：

```text
build/bin/InfluxDesk.app
release/macos/InfluxDesk-<version>-macos-<arch>.zip
release/macos/InfluxDesk-<version>-macos-<arch>.dmg
release/macos/SHA256SUMS.txt
release/macos/BUILD_INFO.txt
```

版本来自 `wails.json` 的 `info.productVersion`。脚本会检查 bundle identifier、plist、目标架构、代码签名和校验和。未设置签名身份时只做 ad-hoc 签名，不能作为面向用户的 Developer ID 发布包。

## Developer ID 与公证

先把 `Developer ID Application` 证书及私钥导入当前登录 Keychain。签名构建：

```bash
MACOS_SIGN_IDENTITY='Developer ID Application: Example Company (TEAMID)' \
  scripts/build-macos.sh universal
```

用 `notarytool` 在 Keychain 中保存公证凭据，命令会交互式读取凭据：

```bash
xcrun notarytool store-credentials influxdesk-notary
```

签名、公证并 stapling：

```bash
MACOS_SIGN_IDENTITY='Developer ID Application: Example Company (TEAMID)' \
MACOS_NOTARY_PROFILE='influxdesk-notary' \
  scripts/build-macos.sh universal
```

正式发布前还应独立执行：

```bash
codesign --verify --deep --strict --verbose=2 build/bin/InfluxDesk.app
spctl --assess --type execute --verbose=4 build/bin/InfluxDesk.app
xcrun stapler validate build/bin/InfluxDesk.app
lipo -archs build/bin/InfluxDesk.app/Contents/MacOS/InfluxDesk
```

## CI

`.github/workflows/ci.yml` 在每个 PR 和 `main` 提交上运行 macOS 原生测试、临时 Keychain 回归和 universal `.app` 构建。该产物只做 ad-hoc 签名并保留 7 天，仅用于诊断，不是发行包。

`.github/workflows/release.yml` 只接受已经进入 `main` 的 `vX.Y.Z` 标签。它从受保护的 `production` environment 读取 Developer ID 和 Apple 公证凭据，要求应用与 DMG 完成签名、公证、stapling 和 Gatekeeper 验证，然后与签名 Windows 包一起创建 Release 草稿。任何凭据或验证缺失都会终止，不会降级发布 ad-hoc 包。

浏览器 mock、Linux 静态检查或 `CGO_ENABLED=0` 交叉编译都不能证明 WKWebView、Cocoa 文件对话框、Keychain 授权、Gatekeeper、签名或公证真实通过；公开草稿前仍需真实 Apple Silicon/Intel 设备验收。
