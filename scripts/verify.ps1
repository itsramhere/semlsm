# scripts/verify.ps1 - run the verification suite with exit-code checks.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\verify.ps1 [-Race] [-NoRace]
# Race is on by default; -NoRace or CGO_ENABLED=0 selects ordinary tests.

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
    param([string]$Name, [scriptblock]$Cmd)
    Write-Host "== $Name"
    $out = & $Cmd 2>&1
    $code = $LASTEXITCODE
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

Run-Step 'go vet' { go vet ./... }
Run-Step 'go test' { go test ./... -count=1 -timeout 300s }

# -Race is accepted for backwards compatibility; -NoRace takes precedence.
$raceWanted = -not $NoRace
$cgoEnabled = go env CGO_ENABLED
if ($LASTEXITCODE -ne 0) {
    Write-Host 'FAIL: go env CGO_ENABLED'
    $fail = 1
    $raceWanted = $false
} elseif ($cgoEnabled.Trim() -eq '0') {
    Write-Host 'skip: go test -race (CGO_ENABLED=0; ordinary tests ran above)'
    $raceWanted = $false
} elseif ($NoRace) {
    Write-Host 'skip: go test -race (-NoRace; ordinary tests ran above)'
}

if ($raceWanted) {
    Run-Step 'go test -race' { go test ./... -race -count=1 -timeout 600s }
}

if ($fail -ne 0) { exit 1 } else { Write-Host 'ALL OK'; exit 0 }
