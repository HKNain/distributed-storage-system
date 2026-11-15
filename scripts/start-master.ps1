
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

$hostAddress = if ($config.ContainsKey("MASTER_HOST")) {
    $config["MASTER_HOST"]
} else { "127.0.0.1" }

$httpBasePort = if ($config.ContainsKey("MASTER_HTTP_BASE_PORT")) {
    [int]$config["MASTER_HTTP_BASE_PORT"]
} else { 3001 }

$grpcBasePort = if ($config.ContainsKey("MASTER_GRPC_BASE_PORT")) {
    [int]$config["MASTER_GRPC_BASE_PORT"]
} else { 4001 }

$raftBasePort = if ($config.ContainsKey("MASTER_RAFT_BASE_PORT")) {
    [int]$config["MASTER_RAFT_BASE_PORT"]
} else { 7001 }

$dataDirBase = if ($config.ContainsKey("MASTER_DATA_DIR_BASE")) {
    $config["MASTER_DATA_DIR_BASE"]
} else { "data/masters" }

$bootstrapEnabled = if ($config.ContainsKey("MASTER_RAFT_BOOTSTRAP")) {
    [bool]::Parse($config["MASTER_RAFT_BOOTSTRAP"])
} else {
    $true
}

if ($count -lt 3) {
    throw "MASTER_NODE_COUNT must be at least 3."
}

foreach ($basePort in @($httpBasePort, $grpcBasePort, $raftBasePort)) {
    if ($basePort -lt 1 -or ($basePort + $count - 1) -gt 65535) {
        throw "Invalid master port range."
    }
}

# Generate identical peer configuration for every master.
$peers = @(
    for ($i = 1; $i -le $count; $i++) {
        $id = "master-$i"
        $raftPort = $raftBasePort + $i - 1
        "$id=$($hostAddress):$raftPort"
    }
) -join ","

$httpPeers = @(
    for ($i = 1; $i -le $count; $i++) {
        $id = "master-$i"
        $httpPort = $httpBasePort + $i - 1
        "$id=$($hostAddress):$httpPort"
    }
) -join ","

Write-Host ""
Write-Host "Starting $count master nodes..." -ForegroundColor Cyan
Write-Host "Raft peers: $peers"
Write-Host ""

for ($i = 1; $i -le $count; $i++) {
    $nodeID = "master-$i"
    $nodeBootstrap = "false"

    if ($i -eq 1 -and $bootstrapEnabled) {
        $nodeBootstrap = "true"
    }

    $httpPort = $httpBasePort + $i - 1
    $grpcPort = $grpcBasePort + $i - 1
    $raftPort = $raftBasePort + $i - 1

    $dataDir = "$dataDirBase/master-$i"

    $absoluteDataDir = Join-Path $ProjectRoot $dataDir
    New-Item -ItemType Directory -Force -Path $absoluteDataDir |
        Out-Null

    $commands = @(
        "Set-Location -LiteralPath '$($ProjectRoot.Replace("'","''"))'"
        "`$env:MASTER_NODE_ID = '$nodeID'"
        "`$env:MASTER_HTTP_ADDRESS = ':$httpPort'"
        "`$env:MASTER_GRPC_ADDRESS = ':$grpcPort'"
        "`$env:MASTER_RAFT_BIND_ADDRESS = '$($hostAddress):$raftPort'"
        "`$env:MASTER_RAFT_ADVERTISE_ADDRESS = '$($hostAddress):$raftPort'"
        "`$env:MASTER_RAFT_PEERS = '$peers'"
        "`$env:MASTER_HTTP_PEERS = '$httpPeers'"
        "`$env:MASTER_RAFT_BOOTSTRAP = '$nodeBootstrap'"
        "`$env:MASTER_DATA_DIR = '$dataDir'"
        "Write-Host 'Starting $nodeID' -ForegroundColor Green"
        "go run ./cmd/master"
    )

    $command = $commands -join "`r`n"

    $encodedCommand = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($command)
    )

    Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @("-NoExit", "-EncodedCommand", $encodedCommand) `
        -WorkingDirectory $ProjectRoot

    Write-Host "Started $nodeID | HTTP: $httpPort | gRPC: $grpcPort | Raft: $raftPort" `
        -ForegroundColor Green
}

Write-Host ""
Write-Host "All master processes launched." -ForegroundColor Cyan
