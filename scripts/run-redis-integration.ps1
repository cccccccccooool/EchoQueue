param(
    [string]$RedisAddress = "127.0.0.1:6380"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$previousLocation = Get-Location
$testExitCode = 0

try {
    Set-Location -LiteralPath $repoRoot
    $env:ECHOQUEUE_REDIS_ADDR = $RedisAddress

    $packages = @(go list -tags=integration ./...)
    if ($LASTEXITCODE -ne 0 -or $packages.Count -eq 0) {
        throw "integration build tag resolved no Go packages"
    }

    go test -tags=integration ./...
    $testExitCode = $LASTEXITCODE
}
finally {
    Set-Location -LiteralPath $previousLocation
}

if ($testExitCode -ne 0) {
    exit $testExitCode
}
