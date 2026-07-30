# InfluxDesk

InfluxDesk 是面向 Windows 11 x64 和 InfluxDB 1.x 的 InfluxQL 桌面客户端。仓库当前是 PLAN 39 的**可编译实施基线和可测试纵向闭环**，不是已经签名、通过生产环境认证的发布版本。

产品范围包括连接与查询工作台、精确数值结果、受保护 mutation，以及逻辑导入/导出。它不提供服务端备份、快照恢复，也不承诺 exactly-once 迁移。

## 目标范围

- 正式目标：Windows 11 Enterprise x64 23H2/24H2，且仅覆盖微软支持期。
- InfluxDB 目标：OSS 1.8.10、1.12.4；1.7.10 仅作为夜间兼容目标。
- 查询语言：仅 InfluxQL；Flux、SQL、InfluxDB 2.x/3.x 不在本版本范围内。

## 当前可执行闭环

- Query：持久创建/命令幂等、60 秒真实 RoundTrip deadline、严格 chunk 协议、`UseNumber` 精确类型、加密 spill、HMAC cursor、2 小时结果过期、24 小时 tombstone 和 90 天清理边界。
- Task：双 revision、SQLite 快照/事件同事务提交，以及不依赖 SQLite `INTEGER`/Go `int64` 的任意精度十进制 `eventSeq`。
- Protection 与 Mutation：三态保护、generation 级命令账本、writer-preferred `dispatchGate`、支配性 Lock、preview reserve、单次 start barrier、Operation 幂等/取消/未知结果。
- Import：Preflight 和规范 staging、START/RESUME Grant、连续单 batch runner、checkpoint/Permit CAS、partial/unknown incident、人工 Resolve/ABORT、Cancel/Cleanup 和重启恢复。
- Export：单 measurement 严格模式、确定性半开时间分片、真实独立 TransferLane、目标卷 extent reservation、`.lp.gz` 耐久提交、manifest、重启复验与 Restart/Cancel/Cleanup。
- Frontend：Wails task event 订阅与断号补拉、Import/Export 任务面板，以及基于 `decimalText`/BigInt 原点的图表精度保护和显式近似模式。

以上能力由仓库内单元测试和集成测试覆盖；尚未完成的生产发布门禁和产品范围见 [PLAN 39 实施状态](docs/IMPLEMENTATION_STATUS.md)。

## 仓库结构

- `app.go`：Wails facade 和启动恢复顺序。
- `frontend/`：工作台、精确结果表格/图表、任务事件同步和传输面板。
- `internal/transport`：InfluxDB 唯一 HTTP 出口和强类型 `/ping`、`/query`、`/write` 请求。
- `internal/influxql`：版本锁定的 fail-closed AST 分类。
- `internal/query`：Query 任务、chunk 协议、精确类型、cursor、spill 和持久保留期。
- `internal/tasks`：任务快照、revision、精确 `eventSeq`、幂等/命令账本和恢复。
- `internal/protection`、`internal/operation`：保护命令、lease/gate、preview token 和 mutation 派发。
- `internal/transfer`、`internal/importworker`：Import staging、配额、Grant/Permit、checkpoint/incident、runner 控制和耐久 gzip。
- `internal/exportlane`、`internal/exportjob`、`internal/exportworker`、`internal/exportservice`：Export 调度、持久任务、严格导出 worker 和应用服务。
- `internal/credential`、`internal/secure`、`internal/localapp`：Credential Manager、DPAPI/GCM 和受保护本地目录。
- `build/windows/wix`：WiX 7 MSI 工程；Wails NSIS 不是正式发布产物。

## 工具链说明

PLAN 39 的默认环境记录为 Go 1.24.7，但 Wails 2.13.0 自身的 `go.mod` 要求 Go 1.25.0。为保证依赖图可构建和结果可复现，本仓库因此锁定：

- Go language version：`1.25.0`
- Go toolchain：`go1.25.12`
- Wails：`2.13.0`
- Node.js：`22.22.2`

这是一项有意记录的实现偏差，不能把仓库降回 Go 1.24.7 而仍声称使用未修改的 Wails 2.13.0。

## 开发与验证

后端基础门禁：

```bash
go test ./...
go vet ./...
go test -race \
  ./internal/transport ./internal/query ./internal/store ./internal/tasks \
  ./internal/protection ./internal/operation ./internal/transfer \
  ./internal/importworker ./internal/exportlane ./internal/exportjob \
  ./internal/exportworker ./internal/exportservice
```

前端门禁：

```bash
cd frontend
npm ci
npm test
npm run build
npm run test:e2e
npm audit --audit-level=moderate
```

Windows x64 可编译性基线：

```bash
wails build -platform windows/amd64 -clean -m -nopackage -webview2 error -nocolour
```

该命令产生的未签名交叉编译产物只适合开发/CI 验证，不等同于 WiX MSI、Authenticode 或 Windows VM 验收。

浏览器工作台可用以下命令启动：

```bash
cd frontend
npm run dev -- --host 127.0.0.1
```

浏览器模式使用确定性 mock 数据；原生 Wails 模式使用生成绑定，后端错误不会静默回退到 mock。

## 安全与发布边界

- SQLite 使用 WAL 和 `synchronous=FULL`；抢占准入和状态边界使用 `BEGIN IMMEDIATE`。
- 凭据、LP、查询结果、preview token 和 Import Grant 不写入任务账本、事件或审计正文。
- 所有数值单元格通过 Wails 时保持 tagged `decimalText`；图表只生成非权威投影。
- `%LOCALAPPDATA%\InfluxDesk` 的 Windows DACL/reparse fail-closed、Credential Manager 和 DPAPI 实现已入库，但仍需在认证 Windows VM 上执行门禁。
- Ed25519 manifest 验签、artifact hash 检查、WiX 源码和发布检查已入库；下载/安装链、真实签名及升级/回滚/卸载尚未完成生产验收。

生产发布至少还需 Windows 11 23H2/24H2 VM、真实 WiX MSI 和签名、InfluxDB 1.8.10/1.12.4 契约测试、故障注入、安全扫描与完整安装生命周期验证。完整清单见 [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md)。
