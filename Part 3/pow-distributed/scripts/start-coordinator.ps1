param(
    [ValidateRange(1,65535)][int]$HttpPort = 8080,
    [ValidateRange(1,65535)][int]$GrpcPort = 50051,
    [ValidateRange(1,[long]::MaxValue)][long]$ChunkSize = 1000000,
    [ValidatePattern('^\d+(ms|s|m)$')][string]$HeartbeatTimeout = '3s',
    [ValidatePattern('^\d+(ms|s|m)$')][string]$AssignmentTimeout = '2m'
)
. "$PSScriptRoot/processes.ps1"
$lock = Enter-LaunchLock
try {
    foreach ($record in @(Read-Records 'coordinator')) {
        if (Test-OwnedProcess $record) { throw "Coordinator already running (PID $($record.Pid))." }
    }
    Assert-FreePort $HttpPort
    Assert-FreePort $GrpcPort
    Push-Location $ProjectRoot
    try { go build -o "$BinDir/pow-coordinator.exe" ./cmd/coordinator; if ($LASTEXITCODE -ne 0) { throw 'Coordinator build failed.' } }
    finally { Pop-Location }
    $process = Start-Process -FilePath "$BinDir/pow-coordinator.exe" -ArgumentList @('-http',"127.0.0.1:$HttpPort",'-grpc',"127.0.0.1:$GrpcPort",'-chunk-size',$ChunkSize,'-heartbeat-timeout',$HeartbeatTimeout,'-assignment-timeout',$AssignmentTimeout) -WorkingDirectory $ProjectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput "$RunDir/coordinator.out.log" -RedirectStandardError "$RunDir/coordinator.err.log"
    $record = Process-Record $process 'coordinator' "$BinDir/pow-coordinator.exe"
    Save-Records 'coordinator' @($record)
    Wait-LocalPort $process $HttpPort
    Write-Host "Coordinator PID $($process.Id): http://localhost:$HttpPort/ (gRPC $GrpcPort)"
    Write-Host 'Logs: Get-Content .run/coordinator.err.log -Wait'
} finally { $lock.Dispose() }
