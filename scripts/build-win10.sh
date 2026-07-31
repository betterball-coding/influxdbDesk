#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"

wails_bin=${INFLUXDESK_WAILS_BIN:-}
if [[ -z "$wails_bin" ]]; then
    wails_bin=$(command -v wails || true)
fi
if [[ -z "$wails_bin" ]]; then
    gopath_bin=$(go env GOPATH)/bin/wails
    if [[ -x "$gopath_bin" ]]; then
        wails_bin=$gopath_bin
    fi
fi
if [[ -z "$wails_bin" || ! -x "$wails_bin" ]]; then
    echo "Wails v2 executable not found; set INFLUXDESK_WAILS_BIN." >&2
    exit 1
fi

"$wails_bin" build \
    -platform windows/amd64 \
    -clean \
    -m \
    -nsis \
    -webview2 embed \
    -o InfluxDesk-win10-x64.exe \
    -nocolour
