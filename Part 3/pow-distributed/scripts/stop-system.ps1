. "$PSScriptRoot/processes.ps1"
$lock = Enter-LaunchLock
try {
    Stop-WorkerRecords
    foreach ($record in @(Read-Records 'coordinator')) {
        if (Test-OwnedProcess $record) { Stop-Process -Id $record.Pid -Force; Write-Host "Stopped coordinator, PID $($record.Pid)" }
    }
    Save-Records 'coordinator' @()
} finally { $lock.Dispose() }
