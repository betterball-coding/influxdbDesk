# PLAN 39 实施状态

更新日期：2026-07-31

本文记录当前仓库的**真实可执行覆盖**。这里的“已实现并已测试”表示代码已接入应用路径，并有仓库内自动化测试覆盖关键契约；它不表示已经在 Windows 认证 VM、真实 InfluxDB 或生产签名基础设施上通过发布验收。

当前结论：仓库已形成可编译实施基线和多个纵向闭环，但仍不是签名生产版本。

## 已实现并已测试

### Query、Task 与精确数值

- Query 已接入 SQLite 持久层：创建幂等、并发准入、Cancel/Close `CommandEnvelope`、重启恢复和安全错误快照均有测试。
- 结果空闲 2 小时后删除，终态 24 小时后转换为最小 tombstone，Query tombstone/创建账本/命令账本在 90 天边界一并清理；24 至 90 天 replay 不重新发包。
- 交互查询的 60 秒 deadline 从 Dispatcher 实际 `BeginRoundTrip` barrier 后开始，并覆盖响应 body 的完整解码。
- chunk 响应使用单一 `json.Decoder` 和 `UseNumber()`；Tagged `decimalText`、类型漂移/歧义、加密 spill、截断、HMAC cursor 和 chunk statement 对账已有测试。
- Task 快照、双 revision、事件日志和全局 `eventSeq` 同事务提交。`eventSeq` 使用规范十进制 TEXT 和十进制排序/递增，不依赖 SQLite `INTEGER`、Go `int64` 或 JavaScript `number`，并已测试越过 uint64 上限。
- 四槽 InteractiveLane、一个独立 TransferLane、队列上限、取消及 generation 隔离已有调度测试。

### Protection 与 Mutation

- 三态保护、10 分钟内存 lease、单调 connection generation 和独立 protection revision 已实现。
- Protection 命令账本按 generation/kind/request ID 隔离；确定成功/拒绝 replay、并发单提交者、generation 关闭后的保留窗口和重启恢复已有测试。
- Writer-preferred `dispatchGate`、`lockPending`、支配性 Lock 和底层 RoundTrip start barrier 已接入 mutation 与 Import 派发。
- Mutation preview token reserve/consume、Operation 创建幂等、`DISPATCHING + DISPATCH_ATTEMPTED` 耐久边界、取消和 `OUTCOME_UNKNOWN` 已实现并有并发/崩溃边界测试。

### Import

- `PreflightImport` 已接入持久 `PREFLIGHTING -> STAGING -> READY` 流程，支持规范 LP/TXT、CSV 显式映射、Typed JSONL 和 GZ；校验显式 ns 时间戳、5 MiB 单点、GZ CRC/解压上限、source/staging SHA-256 和路径逃逸。
- Preflight 先查幂等账本，再原子执行 retained global/profile 准入和私有 staging reservation；文件按已持久 reservation 写入并按实际逻辑长度结算。
- START/RESUME/RESOLVE 的 `ImportRunGrant` 绑定 lease、profile/generation/revision、checkpoint、spec/target 和 source/staging digest；Grant reserve/consume 与命令账本优先 replay 已实现。
- Connection Manager 已运行连续 Import runner：每次只允许一个 batch，在 settlement 与 Permit checkpoint CAS 成功后才派发下一批；锁定、关闭、取消和 incident 会停止 runner。
- 真实 `/write?precision=ns` worker 已实现 5000 点/5 MiB batching、单次 start barrier、ACK、普通 413 batch limit transition、明确拒绝、partial 和 unknown settlement。
- Checkpoint parent/child chain、runSegment ownership、Permit CAS 失败后的 fail-closed 暂停、旧 callback 拒绝和重启恢复已有测试。
- `REPLAY_EXACT`、`ASSUME_COMMITTED`、`ACCEPT_PARTIAL` 的 PAUSE/CONTINUE 分支，以及 PARTIAL/UNKNOWN incident lineage 已实现。
- ABORT 在一个 FULL 事务内关闭 OPEN incident 和 segment、撤销 Permit、写 job/audit/event，且不推进 checkpoint；精确 replay 与并发 revision 边界已有测试。
- `CancelImport` 已覆盖无在途和在途 settlement；`CleanupImport` 先进入 CLEANING、确认删除受保护 staging，再原子释放 reservation/retained slot。失败保持 CLEANING 并可由恢复路径继续。

### Export

- 当前实现范围是**单个 fully-qualified measurement 的严格逻辑导出**。Export Plan 生成 `SHOW TAG KEYS`、`SHOW FIELD KEYS` 和确定性、无重叠的 `[startNs,endNs)` `SELECT * ... GROUP BY *` quantum。
- Export 使用真实、generation 隔离的 TransferLane；每个 generation 仅一个在途请求，多个 Job 按 quantum 公平轮转，取消期间保持 lane 直至 worker 清理完成。
- metadata 和数据请求均经过 Dispatcher。严格校验 tag 只来自 `series.tags`、field 只来自 FIELD KEYS，并拒绝 tag/field 冲突、未知列、类型变化和 Schema 漂移。
- StartExport 实现 ledger-first replay、global/profile retained admission、每 generation 最多 8 个可调度任务，以及目标卷身份/空间和 artifact extent reservation。
- 数据分片写为确定性 `.lp.gz`：gzip footer、buffer flush、文件 sync/close、完整压缩 SHA-256、解压到 EOF 的 CRC/ISIZE 验证、FINALIZING、同卷 rename 和 COMPLETE 顺序已有实现与故障测试。
- `manifest.json` 包含逻辑导出范围、Schema、query digest、分片 size/checksum、`snapshotConsistent=false`、`typePreserving`、`lossy` 和 warnings；同步前重新解析校验。
- Export 持久任务/fragment repository、Restart/Cancel/Cleanup 命令账本、COMPLETE fragment 复验、WRITING `.part` 清理、quota reservation takeover 和启动后 `PAUSED_RESTARTABLE` 恢复已有测试。
- Export worker/service 的仓库测试覆盖真实 TransferLane 调度、header timeout、取消清理、gzip/manifest 提交、确定性 plan、quota/restart 和 connection close。

### Frontend

- Schema 初始加载把每个 database 的 Retention Policies 与 Measurements 合并为单次多 statement 请求，并在应用级最多并发 2 路，避免多个刷新占满 4 个 InteractiveLane；连接 generation 变化时丢弃整份旧快照。
- 左侧 Measurement 目录使用 TanStack Virtual，只渲染视口附近节点；64,149 个名称的回归测试验证实际节点少于 100 个。筛选使用 deferred value，Field/Tag 详情按 connection generation 合并并缓存，快速切换后不重复读取已完成的详情。
- Import/Export 任务面板已接入真实 Wails facade，不接受前端提供可信 staging 路径、staging digest 或容量计费值。
- Transfer 任务先订阅 `task.changed.v1`，再读取 event head，并通过 `GetTaskChanges` 补拉断号；同时有定时 head 核对和窗口 focus 核对。事件 sequence 全程保持十进制字符串。
- 表格保留 tagged `decimalText`。图表对 timestamp/int64/uint64 使用共同 BigInt 原点，仅在 delta 不超过 `2^53-1` 时生成精确 number 投影。
- 无法安全投影时默认阻止绘制；只有用户显式开启近似模式才继续，并持续显示精度提示。Tooltip 始终回显原始 `decimalText`。这些路径有 Vitest 组件和投影测试。
- 工作台、Monaco、任务抽屉和结果视图已覆盖 1440x900、1180x720 的 Playwright 浏览器验收基线；浏览器 E2E 使用 mock，不代替原生 Windows Wails 验收。

### 本地安全与发布骨架

- SQLite WAL/FULL、Credential Manager 引用、DPAPI/AES-GCM、受保护 DACL root、reparse 拒绝和 secret-free 日志/事件边界已有代码及可在当前环境运行的测试。
- Ed25519 manifest 原始字节验签、HTTPS/版本/平台约束、MSI length/SHA-256 校验、内置 channel key 和 Authenticode 发布检查已实现。
- WiX 6.0.2 x64 per-machine 工程和 Windows release workflow 已入库；它们仍需真实 Windows、证书和安装生命周期执行，不能据此宣称 MSI 已发布。
- macOS 已接入 `~/Library/Application Support/InfluxDesk` 私有目录、Keychain Services、标准 Cocoa 标题栏与菜单、Command 快捷键、固定 bundle identifier，以及 arm64/amd64 universal `.app`、ZIP、DMG 构建脚本和 CI 契约。

## 已知限制与剩余工作

以下项目仍是 PLAN 39 发布或完整产品范围的缺口：

- **Windows 与 MSI 实测**：在干净 Windows 10 Enterprise 22H2 x64（build 19045）和 Windows 11 VM 上执行 WebView2 缺失/过旧、Credential Manager、DPAPI、DACL、Wails IPC/CDP、EXE/MSI 签名、WiX/NSIS 安装、升级、回滚和卸载门禁。
- **macOS 原生与发布实测**：需在真实 Apple Silicon 与 Intel Mac 上执行 WKWebView、Cocoa 菜单/文件对话框、Keychain ACL、睡眠恢复、Gatekeeper、Developer ID、notarytool、stapling、DMG 安装和升级验收；当前工作流只上传 ad-hoc 签名包。
- **FINALIZING 恢复优化**：Export 在 rename 已成功但 COMPLETE 事务未提交的崩溃点，尚需启动时直接复验最终文件并补写 COMPLETE；当前恢复保守地暂停并要求 Restart/重新协调。
- **真正逐 chunk 大导出**：当前一个 quantum 会先完整解码受限响应再写 artifact，尚不是将每个合法 chunk 持续写入 `.part` 的端到端大数据流；仍受单 quantum 响应上限约束。
- **Export 范围**：尚未支持一个任务内的多 measurement 编排，也未支持 `NumericText`/类型冲突的用户显式宽松映射。当前只支持单 measurement 严格模式。
- **连接 Transport**：自定义 CA、mTLS、单跳 SSH/host key 和系统/显式 HTTP/SOCKS5 proxy 尚未完整接入 profile、资源生命周期和 UI。
- **Schema 与管理面**：惰性 Field/Tag/Tag Value 分页、实例级 CQ/Users/Grants/Running Queries/Diagnostics，以及完整受控管理 UI 尚未闭环。
- **超大 Schema 分页**：当前仍一次返回完整 Measurement 名称，并复用通用 Query 的 100,000 行/128 MiB 上限；超过该规模需实现后端 `GetSchemaChildren` 分页，当前 64,149 条真实目录不受此限制。
- **完整私有配额恢复**：已有 admission、extent reservation 和任务文件 reconciliation 基础，但 `%LOCALAPPDATA%\InfluxDesk` 全目录扫描、孤儿/隔离文件 fail-closed 计费和用户清理 UI 尚未完成。
- **Windows session lock**：会话锁定触发 dominant Lock、lease/Grant/Permit 失效的原生事件接线和 Windows 验收尚未完成。
- **本地产品功能**：加密历史/收藏 repository 与清理策略、诊断 ZIP allowlist 预览、SBOM/漏洞流水线，以及已验签更新包的下载、用户确认和安装流程尚未完成。
- **Query 多 series 完整浏览**：后端支持 series 列表和 cursor page，但当前原生前端桥只加载 statement 0 的首个 series、首个最多 5000 行结果页；仍需完整 statement/series 导航和连续分页。
- **真实 InfluxDB 契约**：尚未对 InfluxDB OSS 1.8.10、1.12.4 执行完整兼容、超时、partial、413、故障注入和逐点回导测试；1.7.10 夜间兼容也未运行。

## 工具链偏差

PLAN 39 的默认假设写有 Go 1.24.7，但 Wails 2.13.0 发布模块自身声明 `go 1.25.0`。仓库因此明确锁定：

```text
go 1.25.0
toolchain go1.25.12
github.com/wailsapp/wails/v2 v2.13.0
```

这不是无意漂移，而是满足固定 Wails 版本依赖下限所需的可复现构建决策。

## 建议验证命令

Linux/WSL 仓库门禁：

```bash
go test ./...
go vet ./...
go test -race \
  ./internal/transport ./internal/query ./internal/store ./internal/tasks \
  ./internal/protection ./internal/operation ./internal/transfer \
  ./internal/importworker ./internal/exportlane ./internal/exportjob \
  ./internal/exportworker ./internal/exportservice

cd frontend
npm ci
npm test
npm run build
npm run test:e2e
npm audit --audit-level=moderate
```

Windows 可编译性门禁：

```bash
wails build -platform windows/amd64 -clean -m -nopackage -webview2 error -nocolour
```

最后一条仅证明能够生成未签名 Windows 产物；正式完成仍以认证 VM 上的真实 WiX MSI、Authenticode、升级/回滚/卸载和 InfluxDB 契约结果为准。

macOS 原生门禁只能在真实 Mac 上运行：

```bash
INFLUXDESK_MACOS_KEYCHAIN_TEST=1 scripts/build-macos.sh universal
```

Linux/WSL 上的前端浏览器测试和 Darwin 无 CGO 交叉编译只覆盖静态契约，不能替代 WKWebView、Keychain、签名和公证验证。
