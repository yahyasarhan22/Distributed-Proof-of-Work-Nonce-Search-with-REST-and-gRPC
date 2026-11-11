param([string]$Id = '')
. "$PSScriptRoot/processes.ps1"
$lock = Enter-LaunchLock
try {
    Stop-WorkerRecords $Id
} finally { $lock.Dispose() }
