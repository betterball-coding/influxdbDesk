# Windows 10 x64 兼容性契约

## 支持边界

- 首要交付与待实机验收基线：Windows 10 22H2 x64（build 19045）。
- 技术最低版本：Windows 10 1709 x64（build 16299）。NSIS 同时检查最低 build 与原生 AMD64 架构。
- WebView2 最低版本：94.0.992.31，与 Wails v2.13.0 的运行时检查保持一致。
- Windows 11 可以运行，但应用显式使用 `windows.None` 背景类型，不依赖 Mica、Acrylic 或圆角等 Win11 专属表现。

技术最低版本由当前依赖的交集确定：Go 1.25 要求 Windows 10 / Server 2016 或更高版本；Microsoft 当前列出的 Edge/WebView2 Windows 客户端支持从 Windows 10 SAC 1709 起。22H2 是本分支的首要支持目标，较早 SAC 版本不代表已完成产品验收。

## Windows API 审计

- Credential Manager：`CredWriteW`、`CredReadW`、`CredDeleteW`、`CredFree`。
- DPAPI：`CryptProtectData`、`CryptUnprotectData`、`LocalFree`。
- 文件与卷：`GetVolumePathNameW`、`GetVolumeNameForVolumeMountPointW`、`GetDiskFreeSpaceExW`、`MoveFileExW`。
- 本地数据目录安全：进程 Token、DACL、文件属性与 reparse-point 检查。

这些调用均不要求 Windows 11。Wails 对较新的 DPI、暗色标题栏和 backdrop 能力采用动态探测；本项目另行禁用了 backdrop 路径。

## WebView2 与安装器

`scripts/build-win10.sh` 固定使用 `windows/amd64` 和 `-webview2 embed`。NSIS 会检查机器级和当前用户级 Evergreen Runtime：缺失或低于 94.0.992.31 时运行随安装器携带的 Microsoft bootstrapper，并在返回失败或复检不通过时以中文提示并中止。应用启动时由 Wails 再次通过 WebView2 Loader API 检查版本，覆盖便携版和安装后运行时被移除或降级的情况。

WiX 工程仍是发布骨架：它只检查机器级 WebView2 注册，不负责安装或更新运行时，因此不是本分支生成的 Win10 交付产物。

Win10 NSIS 与 Win11 MSI 使用不同的安装器和升级机制。当前 Ed25519 自动更新 manifest 只绑定 Win11 MSI；Win10 版本不嵌入该通道公钥，升级时下载新的 NSIS 安装包并人工确认。实现独立的 NSIS 签名更新协议前，不得把 Win11 MSI manifest 复用于 Win10。

## 尚未验证

当前 WSL 工具链只能验证交叉编译、PE/NSIS 结构和自动化测试。仍需在干净 Windows 10 22H2 x64 VM 上覆盖 WebView2 缺失、过旧、离线、企业策略阻断、安装/卸载、Credential Manager、DPAPI、DACL、Wails IPC 和多显示器 DPI；产物还需要 Authenticode 签名。
