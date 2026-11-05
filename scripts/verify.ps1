# scripts/verify.ps1 - run the A5 verification suite with exit-code checks.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\verify.ps1 [-Race] [-NoRace]
# Default race decision comes from .agent/ENV.md (race: AVAILABLE => on).

param(
    [switch]$Race,
    [switch]$NoRace
)

Set-Location (Join-Path $PSScriptRoot '..')
# Native tools (go, gcc) write progress to stderr; do NOT stop on stderr lines.
# Exit codes are checked explicitly via $LASTEXITCODE.
$ErrorActionPreference = 'Continue'

$fail = 0

function Run-Step {
    param([string]$Name, [scriptblock]$Cmd, [string]$Log)
    Write-Host "== $Name"
    $out = & $Cmd 2>&1
    $code = $LASTEXITCODE
    if ($Log) {
        $dir = Join-Path '.agent' 'logs'
        if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
        $out | Out-File -FilePath (Join-Path $dir $Log) -Encoding utf8
    }
    if ($code -ne 0) {
        Write-Host "FAIL: $Name (exit $code)"
        $out | Select-Object -Last 40 | ForEach-Object { Write-Host $_ }
        $script:fail = 1
    } else {
        Write-Host "ok: $Name"
    }
}

# gofmt -l exits 0 even when listing files: fail on any output.
$gofmtOut = gofmt -l .
if ($LASTEXITCODE -ne 0 -or $gofmtOut) {
    Write-Host 'FAIL: gofmt'
    $gofmtOut | ForEach-Object { Write-Host $_ }
    $script:fail = 1
} else {
    Write-Host 'ok: gofmt'
}

Run-Step 'go vet' { go vet ./... } 'vet.log'
Run-Step 'go test' { go test ./... -count=1 -timeout 300s } 'test.log'

# race gating from .agent/ENV.md
$raceWanted = $true
if ($NoRace) {
    $raceWanted = $false
} elseif (-not $Race) {
    $envMd = Join-Path '.agent' 'ENV.md'
    if (Test-Path $envMd) {
        $content = Get-Content $envMd -Raw
        if ($content -match 'Race detector:\s*UNAVAILABLE') { $raceWanted = $false }
    }
}

if ($raceWanted) {
    Run-Step 'go test -race' { go test ./... -race -count=1 -timeout 600s } 'test-race.log'
} else {
    Write-Host 'skip: go test -race (unavailable per ENV.md); run concurrency tests with -count=20'
    Run-Step 'go test count=20 (race fallback)' { go test ./... -count=20 -timeout 900s } 'test-count20.log'
}

if ($fail -ne 0) { exit 1 } else { Write-Host 'ALL OK'; exit 0 }
