$ErrorActionPreference = 'Stop'
$script:ProjectRoot = Split-Path $PSScriptRoot -Parent
$fallback = Join-Path $ProjectRoot '../../Part 2/.tools'
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    if (-not (Test-Path "$fallback/go/bin/go.exe")) { throw 'Install Go and add it to PATH.' }
    $env:PATH = "$fallback/go/bin;$env:PATH"
}
# Keep all caches written by Part 3 inside Part 3.
$env:GOCACHE = Join-Path $ProjectRoot '.tools/gocache'
$env:GOPATH = Join-Path $ProjectRoot '.tools/gopath'
$env:GOMODCACHE = Join-Path $env:GOPATH 'pkg/mod'
if (Test-Path "$ProjectRoot/.tools/w64devkit/bin/gcc.exe") {
    $env:PATH = "$ProjectRoot/.tools/w64devkit/bin;$env:PATH"
    $env:CC = 'gcc'
    $env:CGO_ENABLED = '1'
}
