# Functional count/configuration check; no scalability measurements.
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
. "$root/scripts/processes.ps1"
foreach ($existing in (@(Read-Records 'coordinator') + @(Read-Records 'workers'))) {
    if (Test-OwnedProcess $existing) { throw 'Stop the existing script-launched system before running this test.' }
}
$base = 'http://127.0.0.1:18080'
try {
    & "$root/scripts/start-coordinator.ps1" -HttpPort 18080 -GrpcPort 51051 -ChunkSize 12345 -HeartbeatTimeout '2s'
    foreach ($count in @(1,2,4,8)) {
        & "$root/scripts/start-workers.ps1" -Count $count -BasePort 51052 -Coordinator '127.0.0.1:51051' -HeartbeatInterval '100ms'
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        do {
            $snapshot = Invoke-RestMethod "$base/workers"
            if (@($snapshot | Where-Object alive).Count -eq $count) { break }
            Start-Sleep -Milliseconds 50
        } while ([DateTime]::UtcNow -lt $deadline)
        if (@($snapshot | Where-Object alive).Count -ne $count) { throw "Count $count did not register." }
        if (@(Read-Records 'workers').Count -ne $count) { throw 'PID record count mismatch.' }
        Write-Output "PASS count=$count independent registered processes"
        & "$root/scripts/stop-workers.ps1"
        $deadline = [DateTime]::UtcNow.AddSeconds(10)
        do {
            $snapshot = Invoke-RestMethod "$base/workers"
            if (@($snapshot | Where-Object alive).Count -eq 0) { break }
            Start-Sleep -Milliseconds 50
        } while ([DateTime]::UtcNow -lt $deadline)
        if (@($snapshot | Where-Object alive).Count -ne 0) { throw 'Stopped workers retained live leases.' }
    }
} finally {
    & "$root/scripts/stop-system.ps1"
}
