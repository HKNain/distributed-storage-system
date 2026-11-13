
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $ProjectRoot ".env"

if (!(Test-Path $EnvFile)) {
    throw ".env file not found: $EnvFile"
}

$config = @{}

Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()

    if (!$line -or $line.StartsWith("#")) {
        return
    }

    $separator = $line.IndexOf("=")
    if ($separator -lt 1) {
        return
    }

    $key = $line.Substring(0, $separator).Trim()
    $value = $line.Substring($separator + 1).Trim()

    if (
        ($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))
    ) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    $config[$key] = $value
}

$count = if ($config.ContainsKey("MASTER_NODE_COUNT")) {
    [int]$config["MASTER_NODE_COUNT"]
} else { 3 }

$httpBasePort = if ($config.ContainsKey("MASTER_HTTP_BASE_PORT")) {
    [int]$config["MASTER_HTTP_BASE_PORT"]
} else { 3001 }

$grpcBasePort = if ($config.ContainsKey("MASTER_GRPC_BASE_PORT")) {
    [int]$config["MASTER_GRPC_BASE_PORT"]
} else { 4001 }

$raftBasePort = if ($config.ContainsKey("MASTER_RAFT_BASE_PORT")) {
    [int]$config["MASTER_RAFT_BASE_PORT"]
} else { 7001 }

$ports = @()

for ($i = 0; $i -lt $count; $i++) {
    $ports += $httpBasePort + $i
    $ports += $grpcBasePort + $i
    $ports += $raftBasePort + $i
}

$ports = $ports | Select-Object -Unique

Write-Host ""
Write-Host "Stopping master nodes..." -ForegroundColor Cyan

foreach ($port in $ports) {
    $connections = @(
        Get-NetTCPConnection -LocalPort $port -State Listen `
            -ErrorAction SilentlyContinue
    )

    if ($connections.Count -eq 0) {
        Write-Host "No master process listening on port $port." `
            -ForegroundColor Yellow
        continue
    }

    $processIds = $connections |
        Select-Object -ExpandProperty OwningProcess -Unique

    foreach ($processId in $processIds) {
        $process = Get-Process -Id $processId `
            -ErrorAction SilentlyContinue

        if ($process) {
            Write-Host "Stopping $($process.ProcessName) (PID: $processId) on port $port..." `
                -ForegroundColor Yellow

            Stop-Process -Id $processId -Force
        }
    }
}

Write-Host ""
Write-Host "Master shutdown completed." -ForegroundColor Green
