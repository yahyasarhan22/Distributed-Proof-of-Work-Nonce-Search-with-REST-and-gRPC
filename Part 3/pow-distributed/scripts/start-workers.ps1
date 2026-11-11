param(
    [ValidateRange(1,128)][int]$Count = 4,
    [ValidateRange(1,65535)][int]$BasePort = 50052,
    [ValidatePattern('^\S+:\d+$')][string]$Coordinator = '127.0.0.1:50051',
    [ValidatePattern('^\d+(ms|s|m)$')][string]$HeartbeatInterval = '500ms'
)
. "$PSScriptRoot/processes.ps1"
$lock = Enter-LaunchLock
$records = @()
$launchStarted = $false
try {
    if ($BasePort + $Count - 1 -gt 65535) { throw 'Worker port sequence exceeds 65535.' }
    foreach ($record in @(Read-Records 'workers')) {
        if (Test-OwnedProcess $record) { throw "Workers already running. Stop them with scripts/stop-workers.ps1 before launching a new group." }
    }
    for ($i=0; $i -lt $Count; $i++) { Assert-FreePort ($BasePort+$i) }
    Push-Location $ProjectRoot
    try { go build -o "$BinDir/pow-worker.exe" ./cmd/worker; if ($LASTEXITCODE -ne 0) { throw 'Worker build failed.' } }
    finally { Pop-Location }
    $launchStarted = $true
    for ($i=0; $i -lt $Count; $i++) {
        $workerId = "worker-$($i+1)"
        $workerPort = $BasePort+$i
        $process = Start-Process -FilePath "$BinDir/pow-worker.exe" -ArgumentList @('-id',$workerId,'-port',$workerPort,'-coordinator',$Coordinator,'-heartbeat-interval',$HeartbeatInterval) -WorkingDirectory $ProjectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput "$RunDir/$workerId.out.log" -RedirectStandardError "$RunDir/$workerId.err.log"
        $records += Process-Record $process $workerId "$BinDir/pow-worker.exe"
        Save-Records 'workers' $records
        Wait-LocalPort $process $workerPort
        Write-Host "$workerId PID $($process.Id), port $workerPort"
    }
    Write-Host 'Logs: Get-Content .run/worker-1.err.log -Wait'
} catch {
    if ($launchStarted) {
        foreach ($record in $records) { if (Test-OwnedProcess $record) { Stop-Process -Id $record.Pid -Force } }
        Save-Records 'workers' @()
    }
    throw
} finally { $lock.Dispose() }
