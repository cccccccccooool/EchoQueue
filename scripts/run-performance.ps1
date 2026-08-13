param(
    [string]$RedisAddress = "127.0.0.1:6380"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$previousLocation = Get-Location

try {
    Set-Location -LiteralPath $repoRoot
    $env:ECHOQUEUE_REDIS_ADDR = $RedisAddress
    go run ./scripts/perf_harness
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Set-Location -LiteralPath $previousLocation
}
