
$ErrorActionPreference = "Stop"

# Project root = parent directory of scripts/
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $ProjectRoot ".env"

if (!(Test-Path $EnvFile)) {
    throw ".env file not found at: $EnvFile"
}

# Read .env
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

# Configuration defaults
$count = 1
$basePort = 3001

if ($config.ContainsKey("STORAGE_NODE_COUNT")) {
    $count = [int]$config["STORAGE_NODE_COUNT"]
}

if ($config.ContainsKey("STORAGE_NODE_BASE_PORT")) {
    $basePort = [int]$config["STORAGE_NODE_BASE_PORT"]
}

if ($count -lt 1) {
    throw "STORAGE_NODE_COUNT must be at least 1."
}

if ($basePort -lt 1 -or ($basePort + $count - 1) -gt 65535) {
    throw "Invalid storage node port range."
}

Write-Host ""
Write-Host "Stopping storage nodes..." -ForegroundColor Cyan
Write-Host ""

for ($i = 1; $i -le $count; $i++) {
    $port = $basePort + $i - 1

    # Find the process listening on this node's port.
    $connections = @(
        Get-NetTCPConnection -LocalPort $port -State Listen `
            -ErrorAction SilentlyContinue
    )

    if ($connections.Count -eq 0) {
        Write-Host "No process found on port $port." `
            -ForegroundColor Yellow
        continue
    }

    $processIds = $connections |
        Select-Object -ExpandProperty OwningProcess -Unique

    foreach ($processId in $processIds) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue

        if ($process) {
            Write-Host "Stopping $($process.ProcessName) (PID: $processId) on port $port..." `
                -ForegroundColor Yellow

            Stop-Process -Id $processId -Force
        }
    }
}

Write-Host ""
Write-Host "Storage node shutdown completed." -ForegroundColor Green
