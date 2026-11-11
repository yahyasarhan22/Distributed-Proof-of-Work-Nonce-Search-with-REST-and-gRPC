. "$PSScriptRoot/env.ps1"
$fallback = Join-Path $ProjectRoot '../../Part 2/.tools'
if (Test-Path "$fallback/bin/protoc-gen-go.exe") { $env:PATH = "$fallback/bin;$env:PATH" }
$protoc = Get-Command protoc -ErrorAction SilentlyContinue
if ($protoc) { $compiler = $protoc.Source } elseif (Test-Path "$fallback/protoc/bin/protoc.exe") { $compiler = "$fallback/protoc/bin/protoc.exe" } else { throw 'Install protoc and the Go plugins first; see README.' }
Push-Location $ProjectRoot
try {
    & $compiler --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/pow.proto
    if ($LASTEXITCODE -ne 0) { throw 'Protocol generation failed.' }
} finally { Pop-Location }
