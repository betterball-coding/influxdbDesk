param(
    [Parameter(Mandatory = $true)][string]$Path
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($env:SIGN_COMMAND)) {
    throw 'SIGN_COMMAND is required.'
}
if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
    throw "Signing target does not exist: $Path"
}

$resolvedPath = (Resolve-Path -LiteralPath $Path).Path
$command = $env:SIGN_COMMAND -replace '\{file\}', $resolvedPath
& powershell -NoProfile -Command $command
if ($LASTEXITCODE -ne 0) {
    throw "Signing command failed for $resolvedPath."
}

$signature = Get-AuthenticodeSignature -LiteralPath $resolvedPath
if ($signature.Status -ne 'Valid') {
    throw "Authenticode signature is not valid for $resolvedPath (status: $($signature.Status))."
}
if ($null -eq $signature.TimeStamperCertificate) {
    throw "RFC3161 timestamp countersignature is missing for $resolvedPath."
}

Write-Host "Verified Authenticode signature: $resolvedPath"
