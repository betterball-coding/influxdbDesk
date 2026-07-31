# 跨平台维护规范

InfluxDesk 采用单仓库、单主线、共享核心和平台适配层。Windows 10、Windows 11 和 macOS 是同一版本源代码的不同交付目标，不是三套独立产品代码。

## 唯一事实源

- `main` 是唯一长期开发分支。功能分支和平台分支必须短期存在，通过 PR 合并后删除。
- 一个版本标签必须指向 `main` 中的提交，`wails.json` 的 `info.productVersion` 必须与标签一致。
- 同一标签同时生成 Win10 EXE、Win11 MSI、macOS universal DMG/ZIP。禁止在打包阶段修改业务源码或临时改版本号。
- 平台修复优先提炼共享契约，再修改对应适配器；不得复制整套前端、查询或传输逻辑。

## 代码边界

| 范围 | 位置 | 维护规则 |
| --- | --- | --- |
| 共享前端 | `frontend/src` | 默认不读取操作系统 API，通过 Wails bridge 或小型平台工具获取能力 |
| 共享业务 | `app.go`、`internal/*` | 不包含安装器、窗口或凭据系统的分支逻辑 |
| Wails 平台入口 | `platform_windows.go`、`platform_darwin.go`、`platform_other.go` | 使用 Go build tag，每个平台只实现同名入口 |
| 系统凭据 | `internal/credential/store_*.go` | Windows Credential Manager、macOS Keychain；不支持的平台 fail-closed |
| 私有目录 | `internal/localapp/root_*.go` | 平台原生目录、所有权、权限和链接检查 |
| 文件系统语义 | `internal/transfer/*_windows.go`、`*_unix.go` | 只隔离卷、rename、路径等真实系统差异 |
| 打包 | `build/windows`、`build/darwin`、`scripts/build-*` | 不承载业务行为 |

新增平台能力时，应先定义共享接口或同名平台函数，再增加 `*_windows.go` / `*_darwin.go`。如果一个条件分支开始出现在多个共享文件中，应将它收口为平台适配器。

## CI 分层

`.github/workflows/ci.yml` 是所有 PR 和 `main` 的必过门禁：

1. Ubuntu 执行前端单元测试、浏览器交互测试、Go 全量测试、vet、race 和跨平台编译契约。
2. Windows runner 执行原生 Go 测试并生成未签名 Windows x64 CI 可执行文件。
3. macOS runner 执行 Keychain 回归、原生 Go 测试并生成 ad-hoc 签名的 universal CI 应用。

CI 产物仅用于诊断，保留 7 天，不得作为正式发行包。

## 正式发布

`.github/workflows/release.yml` 只响应 `vX.Y.Z` 标签，并执行以下 fail-closed 门禁：

1. 标签版本必须与 `wails.json` 一致，且标签提交必须已经进入远端 `main`。
2. Windows EXE、NSIS 安装器、NSIS 卸载器和 MSI 必须通过 Authenticode 与 RFC3161 时间戳验证。
3. Win11 MSI 更新 manifest 使用 Ed25519 私钥签名，并重新用公开密钥校验 MSI 长度和 SHA-256；Win10 NSIS 当前采用手动安装升级，不复用 MSI 更新通道。
4. macOS universal 应用必须使用 Developer ID 签名，通过 Apple 公证并完成 stapling；DMG 也必须公证。
5. 两个平台成功后才创建 GitHub Release 草稿。人工核对安装生命周期和发布说明后再公开。

生产环境需要配置受保护的 GitHub `production` environment，并限制审批人。所需配置：

| 名称 | 类型 | 用途 |
| --- | --- | --- |
| `SIGN_COMMAND` | Secret | 对 `{file}` 执行企业 Authenticode/RFC3161 签名 |
| `UPDATE_PRIVATE_KEY_BASE64` | Secret | Ed25519 更新清单私钥 |
| `UPDATE_PUBLIC_KEY_BASE64` | Variable | 内置和发布校验使用的 Ed25519 公钥 |
| `MACOS_CERTIFICATE_P12_BASE64` | Secret | Developer ID 证书和私钥 |
| `MACOS_CERTIFICATE_PASSWORD` | Secret | P12 密码 |
| `MACOS_SIGN_IDENTITY` | Secret | Developer ID Application 身份名称 |
| `APPLE_ID`、`APPLE_TEAM_ID`、`APPLE_APP_PASSWORD` | Secret | Apple 公证凭据 |

任何凭据缺失、签名无效、缺少时间戳、公证失败或产物名称不一致都会终止发布，不允许降级为未签名正式包。

## 日常变更流程

1. 从最新 `main` 创建短期功能分支。
2. 共享功能只修改共享目录；平台差异只修改适配器和对应契约测试。
3. 本地运行能够执行的共享测试。平台原生行为由 CI 的 Windows/macOS runner 补齐。
4. PR 必须通过三个 CI job，至少一人审查平台边界、安全和数据兼容性。
5. 合并后删除短期分支。禁止在 Win10、Win11、macOS 长期分支上继续开发。

紧急修复从发布标签创建短期 hotfix 分支，修复后先合回 `main`，再从 `main` 打补丁版本标签。不得只修某个安装包对应的分支。

## 新增平台检查表

- 增加 Wails 平台入口以及明确的 build tag。
- 为凭据、私有目录、权限、原子 rename、可用空间和文件选择器提供原生实现或明确拒绝。
- 增加原生 CI runner，不能只依赖交叉编译。
- 定义支持的 OS/架构、最低版本、安装器、升级和卸载契约。
- 增加签名、公证或平台等价供应链验证。
- 在真实设备或认证 VM 上完成启动、休眠恢复、权限、代理、显示缩放和安装生命周期验收。

满足以上条件前，只能标记为实验性目标，不能进入正式支持矩阵。
