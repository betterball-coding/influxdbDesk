param(
    [Parameter(Mandatory = $true)][string]$Version,
    [Parameter(Mandatory = $true)][string]$ExePath,
    [Parameter(Mandatory = $true)][string]$MsiPath,
    [Parameter(Mandatory = $true)][string]$ManifestPath,
    [Parameter(Mandatory = $true)][string]$ManifestSignaturePath
)

$ErrorActionPreference = 'Stop'

if ($Version -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$') {
    throw "Version must contain exactly three numeric components."
}

foreach ($path in @($ExePath, $MsiPath, $ManifestPath, $ManifestSignaturePath)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Required release artifact is missing: $path"
    }
}

foreach ($path in @($ExePath, $MsiPath)) {
    $signature = Get-AuthenticodeSignature -LiteralPath $path
    if ($signature.Status -ne 'Valid') {
        throw "Authenticode signature is not valid for $path (status: $($signature.Status))."
    }
    if ($null -eq $signature.TimeStamperCertificate) {
        throw "RFC3161 timestamp countersignature is missing for $path."
    }
}

$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
if ($manifest.platform -ne 'windows-x64' -or $manifest.version -ne $Version) {
    throw "Manifest platform or version does not match the release."
}
$msi = Get-Item -LiteralPath $MsiPath
$hash = (Get-FileHash -LiteralPath $MsiPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ([int64]$manifest.msiLength -ne $msi.Length -or $manifest.msiSha256 -ne $hash) {
    throw "Manifest MSI length or SHA-256 does not match the signed artifact."
}

Write-Host "Release artifacts and Authenticode signatures verified."
