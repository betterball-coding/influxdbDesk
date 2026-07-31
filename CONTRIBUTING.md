# 贡献指南

开发和发布规则以 `docs/CROSS_PLATFORM_MAINTENANCE.md` 为准。

## 分支与提交

- 从 `main` 创建短期分支，保持变更聚焦。
- 不创建长期 Win10、Win11 或 macOS 产品分支。
- 数据格式、Wails bridge、持久化 schema 和任务状态变化必须包含向后兼容说明与回归测试。
- 生成文件只有在其源码契约同步变化时才提交。

## 本地门禁

```bash
cd frontend
npm ci
npm audit --audit-level=moderate
npm test
npm run build
npm run test:e2e

cd ..
go test ./...
go vet ./...
go test -race ./internal/transport ./internal/query ./internal/store \
  ./internal/tasks ./internal/protection ./internal/operation ./internal/transfer
```

Linux/WSL 不能代替 Windows Credential Manager、DPAPI、WebView2 或 macOS Keychain、WKWebView、Gatekeeper 的原生测试。PR 合并以 GitHub Actions 的 Linux、Windows、macOS 三组门禁为准。

## 发布

版本号只在 `wails.json` 中维护。版本提交进入 `main` 并通过 CI 后再创建匹配的 `vX.Y.Z` 标签。正式流水线只生成签名和公证后的 Release 草稿；发布者必须完成安装、升级、回滚、卸载和校验和复核后才能公开。
