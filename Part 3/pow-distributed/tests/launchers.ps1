# Runs real processes on alternate ports; requires no existing script-launched group.
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
. "$root/scripts/processes.ps1"
foreach ($existing in (@(Read-Records 'coordinator') + @(Read-Records 'workers'))) {
    if (Test-OwnedProcess $existing) { throw 'Stop the existing script-launched system before running this test.' }
}
try {
    & "$root/scripts/start-coordinator.ps1" -HttpPort 18080 -GrpcPort 51051
    & "$root/scripts/start-workers.ps1" -Count 4 -BasePort 51052 -Coordinator '127.0.0.1:51051'
    $records = @(Read-Records 'workers')
    if ($records.Count -ne 4 -or @($records | Select-Object -ExpandProperty Pid -Unique).Count -ne 4) { throw 'Workers are not independent processes.' }
    foreach ($record in $records) { if (-not (Test-OwnedProcess $record)) { throw 'Missing process identity.' } }
    $before = Get-Content "$RunDir/workers.json" -Raw
    $rejected = $false
    try { & "$root/scripts/start-workers.ps1" -Count 4 -BasePort 51052 -Coordinator '127.0.0.1:51051' } catch { $rejected = $true }
    if (-not $rejected -or (Get-Content "$RunDir/workers.json" -Raw) -ne $before) { throw 'Duplicate launch was not safely rejected.' }
    $base = 'http://127.0.0.1:18080'
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    do { $workers = Invoke-RestMethod "$base/workers"; if (@($workers | Where-Object alive).Count -eq 4) { break }; Start-Sleep -Milliseconds 100 } while ([DateTime]::UtcNow -lt $deadline)
    if (@($workers | Where-Object alive).Count -ne 4) { throw 'Not all workers registered.' }
    $j = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"hello","difficulty":4}'
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do { $result = Invoke-RestMethod "$base/jobs/$($j.job_id)"; if ($result.status -eq 'FOUND') { break }; Start-Sleep -Milliseconds 50 } while ([DateTime]::UtcNow -lt $deadline)
    if ($result.status -ne 'FOUND') { throw 'Search did not finish.' }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes("hello$($result.nonce)"))).Replace('-','').ToLower() } finally { $sha.Dispose() }
    if ($hash -ne $result.hash -or -not $hash.StartsWith('0000')) { throw 'Invalid independent proof.' }
    Write-Output "PASS real 4-process proof: nonce=$($result.nonce), hash=$hash"
    & "$root/scripts/stop-workers.ps1" -Id worker-2
    if (@(Read-Records 'workers').Count -ne 3) { throw 'Selective stopping failed.' }
    Write-Output 'PASS duplicate prevention, process identities, registration and selective stopping.'
} finally {
    & "$root/scripts/stop-system.ps1"
}
foreach ($record in $records) { if (Test-OwnedProcess $record) { throw 'Worker remained after stop-system.' } }
Write-Output 'PASS stopping and cleanup.'
