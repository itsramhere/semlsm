# scripts/verify.ps1 - run the A5 verification suite with exit-code checks.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\verify.ps1 [-Race:$true|:$false] [-Full]
# Default race decision comes from .agent/ENV.md (race: AVAILABLE => on).

param(
    [switch]$Race,
    [switch]$NoRace,
    [switch]$Full  # run all packages with -count=1 (gate mode); default is affected-package mode handled by caller
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

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
        $tail = $out | Select-Object -Last 40
        $tail | ForEach-Object { Write-Host $_ }
        $script:fail = 1
    } else {
        Write-Host "ok: $Name"
    }
}

Run-Step 'gofmt' { gofmt -l . } 'gofmt.log'
# gofmt -l exits 0 even when listing files; treat any output line as failure
$gofmtOut = gofmt -l .
if ($gofmtOut) { Write-Host 'FAIL: gofmt reported unformatted files:'; $gofmtOut; $script:fail = 1 }

Run-Step 'go vet' { go vet ./... } 'vet.log'

if ($Full) {
    Run-Step 'go test' { go test ./... -count=1 -timeout 300s } 'test.log'
} else {
    Run-Step 'go test' { go test ./... -count=1 -timeout 300s } 'test.log'
}

# race gating from .agent/ENV.md
$raceWanted = $true
if (-not ($Race -or $NoRace)) {
    $envMd = Join-Path '.agent' 'ENV.md'
    if (Test-Path $envMd) {
        $content = Get-Content $envMd -Raw
        if ($content -match 'Race detector:\s*UNAVAILABLE') { $raceWanted = $false }
    }
}
if ($NoRace) { $raceWanted = $false }

if ($raceWanted) {
    Run-Step 'go test -race' { go test ./... -race -count=1 -timeout 600s } 'test-race.log'
} else {
    Write-Host 'skip: go test -race (unavailable per ENV.md); run concurrency tests with -count=20'
    Run-Step 'go test count=20 (race fallback)' { go test ./... -count=20 -timeout 900s } 'test-count20.log'
}

if ($fail -ne 0) { exit 1 } else { Write-Host 'ALL OK'; exit 0 }
