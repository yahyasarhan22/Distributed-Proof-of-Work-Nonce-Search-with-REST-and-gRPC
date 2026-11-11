# Run against the four-worker group launched by the project scripts.
param([string]$BaseUrl = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
. "$root/scripts/processes.ps1"
$j = Invoke-RestMethod "$BaseUrl/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"fault-demo","difficulty":6}'
try {
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    do {
        $workers = Invoke-RestMethod "$BaseUrl/workers"
        $victim = $workers | Where-Object { $_.worker_id -eq 'worker-2' -and $_.current_assignment.job_id -eq $j.job_id } | Select-Object -First 1
        if ($victim) { break }
        Start-Sleep -Milliseconds 10
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $victim) { throw 'No active worker-2 range observed; retry with higher difficulty if the job finished immediately.' }
    $range = $victim.current_assignment
    & "$root/scripts/stop-workers.ps1" -Id worker-2
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    do {
        $snapshot = Invoke-RestMethod "$BaseUrl/workers"
        $dead = @($snapshot | Where-Object { $_.worker_id -eq 'worker-2' -and -not $_.alive })
        if ($dead) { break }
        Start-Sleep -Milliseconds 25
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $dead) { throw 'Failed worker not marked offline.' }
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        $result = Invoke-RestMethod "$BaseUrl/jobs/$($j.job_id)"
        if ($result.status -eq 'FOUND') { break }
        Start-Sleep -Milliseconds 50
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($result.status -ne 'FOUND') { throw 'No proof within the bounded test deadline; the job is being cancelled.' }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes("fault-demo$($result.nonce)"))).Replace('-','').ToLower() } finally { $sha.Dispose() }
    if ($hash -ne $result.hash -or -not $hash.StartsWith('000000')) { throw 'Invalid proof after failure.' }
    $lines = @(Get-Content "$RunDir/coordinator.err.log" | Where-Object { $_.Contains("job=$($j.job_id)") -and $_.Contains("start=$($range.start) end=$($range.end)") })
    if (@($lines | Where-Object { $_.Contains('range requeued') }).Count -lt 1 -or @($lines | Where-Object { $_.Contains('range assigned') }).Count -lt 2) { throw 'Missing range requeue/reassignment evidence.' }
    Write-Output "PASS forced worker-2 crash, offline detection, reassignment [$($range.start),$($range.end)), and valid completion."
    Write-Output "nonce=$($result.nonce) hash=$hash"
    $lines | Write-Output
} finally {
    $latest = Invoke-RestMethod "$BaseUrl/jobs/$($j.job_id)"
    if ($latest.status -eq 'RUNNING') { Invoke-RestMethod "$BaseUrl/jobs/$($j.job_id)" -Method Delete | Out-Null }
}
