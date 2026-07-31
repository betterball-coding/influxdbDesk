#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
target_arch="${1:-universal}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "build-macos.sh must run on macOS with Xcode Command Line Tools installed." >&2
  exit 2
fi

case "$target_arch" in
  universal|arm64|amd64) ;;
  *)
    echo "usage: scripts/build-macos.sh [universal|arm64|amd64]" >&2
    exit 2
    ;;
esac

for required_command in go node npm wails xcode-select lipo codesign ditto hdiutil shasum; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "required command is unavailable: $required_command" >&2
    exit 2
  fi
done

if ! xcode-select -p >/dev/null 2>&1; then
  echo "Xcode Command Line Tools are not configured. Run: xcode-select --install" >&2
  exit 2
fi

product_version="$(node -e 'const fs=require("fs"); const config=JSON.parse(fs.readFileSync(process.argv[1], "utf8")); process.stdout.write(config.info.productVersion)' "$project_dir/wails.json")"
if [[ ! "$product_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "wails.json info.productVersion must contain exactly three numeric components." >&2
  exit 2
fi

cd "$project_dir/frontend"
npm ci
npm test
npm run build

cd "$project_dir"
go test ./...
go vet ./...
wails build \
  -platform "darwin/$target_arch" \
  -clean \
  -m \
  -nocolour \
  -trimpath \
  -s \
  -skipbindings

app_path="$project_dir/build/bin/InfluxDesk.app"
binary_path="$app_path/Contents/MacOS/InfluxDesk"
plist_path="$app_path/Contents/Info.plist"
if [[ ! -x "$binary_path" || ! -f "$plist_path" ]]; then
  echo "Wails completed without a valid InfluxDesk.app bundle." >&2
  exit 1
fi

plutil -lint "$plist_path"
bundle_version="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$plist_path")"
bundle_identifier="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$plist_path")"
if [[ "$bundle_version" != "$product_version" || "$bundle_identifier" != "com.betterballcoding.influxdesk" ]]; then
  echo "application bundle metadata does not match the release contract." >&2
  exit 1
fi

binary_arches="$(lipo -archs "$binary_path")"
case "$target_arch" in
  universal)
    if [[ "$binary_arches" != *"arm64"* || "$binary_arches" != *"x86_64"* ]]; then
      echo "universal build is missing arm64 or x86_64: $binary_arches" >&2
      exit 1
    fi
    ;;
  arm64)
    [[ "$binary_arches" == "arm64" ]] || { echo "expected arm64, got $binary_arches" >&2; exit 1; }
    ;;
  amd64)
    [[ "$binary_arches" == "x86_64" ]] || { echo "expected x86_64, got $binary_arches" >&2; exit 1; }
    ;;
esac

xattr -cr "$app_path"
sign_identity="${MACOS_SIGN_IDENTITY:--}"
require_production_signing="${INFLUXDESK_REQUIRE_PRODUCTION_SIGNING:-0}"
notary_profile="${MACOS_NOTARY_PROFILE:-}"
notary_keychain="${MACOS_NOTARY_KEYCHAIN:-}"
if [[ "$require_production_signing" == "1" && ( "$sign_identity" == "-" || -z "$notary_profile" ) ]]; then
  echo "production macOS builds require MACOS_SIGN_IDENTITY and MACOS_NOTARY_PROFILE." >&2
  exit 2
fi
if [[ "$sign_identity" == "-" ]]; then
  codesign --force --deep --sign - --timestamp=none "$app_path"
  signing_status="ad-hoc"
else
  codesign --force --deep --options runtime --timestamp --sign "$sign_identity" "$app_path"
  signing_status="Developer ID: $sign_identity"
fi
codesign --verify --deep --strict --verbose=2 "$app_path"

release_dir="$project_dir/release/macos"
mkdir -p "$release_dir"
artifact_base="InfluxDesk-$product_version-macos-$target_arch"
zip_path="$release_dir/$artifact_base.zip"
dmg_path="$release_dir/$artifact_base.dmg"
rm -f "$zip_path" "$dmg_path" "$release_dir/SHA256SUMS.txt" "$release_dir/BUILD_INFO.txt"

ditto -c -k --sequesterRsrc --keepParent "$app_path" "$zip_path"

notarization_status="not submitted"
if [[ -n "$notary_profile" ]]; then
  if [[ "$sign_identity" == "-" ]]; then
    echo "MACOS_NOTARY_PROFILE requires a Developer ID MACOS_SIGN_IDENTITY." >&2
    exit 2
  fi
	if ! command -v xcrun >/dev/null 2>&1; then
    echo "xcrun is required for notarization." >&2
    exit 2
  fi
  notary_args=(--keychain-profile "$notary_profile")
  if [[ -n "$notary_keychain" ]]; then
    notary_args+=(--keychain "$notary_keychain")
  fi
  xcrun notarytool submit "$zip_path" "${notary_args[@]}" --wait
  xcrun stapler staple "$app_path"
  xcrun stapler validate "$app_path"
  rm -f "$zip_path"
  ditto -c -k --sequesterRsrc --keepParent "$app_path" "$zip_path"
  notarization_status="accepted and stapled"
fi

dmg_staging="$(mktemp -d "${TMPDIR:-/tmp}/influxdesk-dmg.XXXXXX")"
cleanup() {
  rm -rf "$dmg_staging"
}
trap cleanup EXIT
ditto "$app_path" "$dmg_staging/InfluxDesk.app"
ln -s /Applications "$dmg_staging/Applications"
hdiutil create -volname "InfluxDesk" -srcfolder "$dmg_staging" -ov -format UDZO "$dmg_path"

if [[ "$sign_identity" != "-" ]]; then
  codesign --force --timestamp --sign "$sign_identity" "$dmg_path"
  codesign --verify --verbose=2 "$dmg_path"
fi
if [[ -n "$notary_profile" ]]; then
  xcrun notarytool submit "$dmg_path" "${notary_args[@]}" --wait
  xcrun stapler staple "$dmg_path"
  xcrun stapler validate "$dmg_path"
fi

if [[ "$require_production_signing" == "1" ]]; then
  [[ "$signing_status" == Developer\ ID:* ]] || { echo "Developer ID signing verification failed." >&2; exit 1; }
  [[ "$notarization_status" == "accepted and stapled" ]] || { echo "notarization verification failed." >&2; exit 1; }
  spctl --assess --type execute --verbose=4 "$app_path"
fi

(
  cd "$release_dir"
  shasum -a 256 "$(basename "$zip_path")" "$(basename "$dmg_path")" > SHA256SUMS.txt
)

{
  echo "product=InfluxDesk"
  echo "version=$product_version"
  echo "target=darwin/$target_arch"
  echo "architectures=$binary_arches"
  echo "bundleIdentifier=$bundle_identifier"
  echo "signing=$signing_status"
  echo "notarization=$notarization_status"
  echo "zipBytes=$(wc -c < "$zip_path" | tr -d ' ')"
  echo "dmgBytes=$(wc -c < "$dmg_path" | tr -d ' ')"
} > "$release_dir/BUILD_INFO.txt"

echo "macOS artifacts:"
echo "  $zip_path"
echo "  $dmg_path"
echo "  $release_dir/SHA256SUMS.txt"
echo "  signing: $signing_status"
echo "  notarization: $notarization_status"
