
$ErrorActionPreference = "Stop"

# Project root = parent directory of scripts/
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $ProjectRoot ".env"

if (!(Test-Path $EnvFile)) {
    throw ".env file not found at: $EnvFile"
}

# Read .env without overwriting existing process environment variables.
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

    # Remove optional surrounding quotes.
    if (
        ($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))
    ) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    $config[$key] = $value
}

# Read configuration.
$count = 1
$basePort = 3001
$hostAddress = "localhost"
$dataDir = "data/nodes"
$masterAddress = "localhost:4000"

if ($config.ContainsKey("STORAGE_NODE_COUNT")) {
    $count = [int]$config["STORAGE_NODE_COUNT"]
}

if ($config.ContainsKey("STORAGE_NODE_BASE_PORT")) {
    $basePort = [int]$config["STORAGE_NODE_BASE_PORT"]
}

if ($config.ContainsKey("STORAGE_NODE_HOST")) {
    $hostAddress = $config["STORAGE_NODE_HOST"]
}

if ($config.ContainsKey("STORAGE_NODE_DATA_DIR")) {
    $dataDir = $config["STORAGE_NODE_DATA_DIR"]
}

if ($config.ContainsKey("MASTER_GRPC_ADDRESS")) {
    $masterAddress = $config["MASTER_GRPC_ADDRESS"]
}

# Validate configuration.
if ($count -lt 1) {
    throw "STORAGE_NODE_COUNT must be at least 1."
}

if ($basePort -lt 1 -or ($basePort + $count - 1) -gt 65535) {
    throw "Invalid storage node port range."
}

Write-Host ""
Write-Host "Starting $count storage nodes..." -ForegroundColor Cyan
Write-Host "Project: $ProjectRoot"
Write-Host "Master:  $masterAddress"
Write-Host ""

for ($i = 1; $i -le $count; $i++) {
    $nodeID = "storage-node-$i"
    $port = $basePort + $i - 1
    $address = "${hostAddress}:$port"
    $storageDir = Join-Path $dataDir "node$i"

    # Create an independent directory for each node.
    $absoluteStorageDir = Join-Path $ProjectRoot $storageDir
    New-Item -ItemType Directory -Force -Path $absoluteStorageDir |
        Out-Null

    # Build the command executed in the node's own PowerShell window.
    $commands = @(
        "Set-Location -LiteralPath '$($ProjectRoot.Replace("'","''"))'"
        "`$env:STORAGE_NODE_ID = '$nodeID'"
        "`$env:STORAGE_NODE_PORT = '$port'"
        "`$env:STORAGE_NODE_HOST = '$hostAddress'"
        "`$env:STORAGE_NODE_ADDRESS = '$address'"
        "`$env:STORAGE_DIR = '$storageDir'"
        "`$env:MASTER_GRPC_ADDRESS = '$masterAddress'"
        "Write-Host 'Starting $nodeID at $address' -ForegroundColor Green"
        "go run ./cmd/storage-node"
    )

    # Use EncodedCommand to safely handle paths containing spaces.
    $command = $commands -join "`r`n"
    $encodedCommand = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($command)
    )

    Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @("-NoExit", "-EncodedCommand", $encodedCommand) `
        -WorkingDirectory $ProjectRoot

    Write-Host "Started $nodeID -> $address | $storageDir" `
        -ForegroundColor Green
}

Write-Host ""
Write-Host "All storage node processes have been launched." `
    -ForegroundColor Cyan
