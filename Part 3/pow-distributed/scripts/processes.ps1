. "$PSScriptRoot/env.ps1"
$script:RunDir = Join-Path $ProjectRoot '.run'
$script:BinDir = Join-Path $ProjectRoot 'bin'
New-Item -ItemType Directory -Force $RunDir, $BinDir | Out-Null

function Read-Records([string]$Name) {
    $file = Join-Path $RunDir "$Name.json"
    if (Test-Path -LiteralPath $file) {
        $decoded = Get-Content -LiteralPath $file -Raw | ConvertFrom-Json
        foreach ($record in $decoded) { Write-Output $record }
    }
}
function Test-OwnedProcess($Record) {
    $p = Get-Process -Id $Record.Pid -ErrorAction SilentlyContinue
    if (-not $p) { return $false }
    try { return ($p.Path -eq $Record.Path -and $p.StartTime.ToUniversalTime().Ticks.ToString() -eq $Record.StartedTicks) }
    catch { return $false }
}
function Save-Records([string]$Name, [object[]]$Records) {
    $json = ConvertTo-Json -InputObject @($Records) -Depth 4
    [IO.File]::WriteAllText((Join-Path $RunDir "$Name.json"), $json)
}
function Process-Record($Process, [string]$Id, [string]$ExecutablePath) {
    $Process.Refresh()
    [pscustomobject]@{ Id=$Id; Pid=$Process.Id; Path=[IO.Path]::GetFullPath($ExecutablePath); StartedTicks=$Process.StartTime.ToUniversalTime().Ticks.ToString() }
}
function Assert-FreePort([int]$Port) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $Port)
    try { $listener.Start() } catch { throw "Port $Port is already in use. Stop the existing process or choose another port." }
    finally { $listener.Stop() }
}
function Wait-LocalPort($Process, [int]$Port) {
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while ([DateTime]::UtcNow -lt $deadline) {
        $Process.Refresh()
        if ($Process.HasExited) { throw "Process $($Process.Id) exited. Check .run/*.err.log." }
        $client = [Net.Sockets.TcpClient]::new()
        try { $client.Connect('127.0.0.1', $Port); return } catch { Start-Sleep -Milliseconds 100 }
        finally { $client.Dispose() }
    }
    throw "Process did not listen on port $Port within 10 seconds."
}
function Enter-LaunchLock {
    try { return [IO.File]::Open((Join-Path $RunDir 'launcher.lock'), 'OpenOrCreate', 'ReadWrite', 'None') }
    catch { throw 'Another startup/stop script is running. Try again when it finishes.' }
}

# Caller owns launcher.lock, so shutdown excludes concurrent startups throughout.
function Stop-WorkerRecords([string]$Id = '') {
    $keep = @()
    foreach ($record in @(Read-Records 'workers')) {
        if ($Id -and $record.Id -ne $Id) { $keep += $record; continue }
        if (Test-OwnedProcess $record) {
            Stop-Process -Id $record.Pid -Force
            Write-Host "Stopped $($record.Id), PID $($record.Pid)"
        }
    }
    Save-Records 'workers' $keep
}
