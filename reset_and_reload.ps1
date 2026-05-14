$ErrorActionPreference = "Stop"

$RepoDir = "C:\Users\lande\Documents\GitHub\twincitiesscrabble"
$DataDir = "C:\Users\lande\Documents\ScrabbleData"

$DayCsv = Join-Path $DataDir "Daytime Scrabble 2026 - May 7.csv"
$NmCsv = Join-Path $DataDir "North Metro Scrabble 2026 - May 7.csv"
$Payload = Join-Path $DataDir "combined_payload.json"
$LoadSql = Join-Path $DataDir "combined_load.sql"

function Stop-WithMessage {
    param([string]$Message)
    Write-Host $Message
    exit 1
}

function Stop-IfFailed {
    param([string]$StepName)
    if ($LASTEXITCODE -ne 0) {
        Stop-WithMessage "$StepName failed."
    }
}

function Require-File {
    param(
        [string]$Path,
        [string]$Description
    )
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        Stop-WithMessage "Missing $Description`: $Path"
    }
}

Write-Host "Twin Cities Scrabble D1 reload"
Write-Host "Repo folder: $RepoDir"
Write-Host "Data folder: $DataDir"

if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) {
    Write-Host "Creating data folder..."
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
}

Require-File $DayCsv "Daytime CSV"
Require-File $NmCsv "North Metro CSV"

Push-Location $RepoDir
try {
    Require-File (Join-Path $RepoDir "make_import_payload.py") "make_import_payload.py"
    Require-File (Join-Path $RepoDir "generate_load_sql.py") "generate_load_sql.py"
    Require-File (Join-Path $RepoDir "wrangler.toml") "wrangler.toml"

    Write-Host "Building payload from CSV files..."
    Write-Host "  DAY: $DayCsv"
    Write-Host "  NM:  $NmCsv"
    Write-Host "  Out: $Payload"
    python make_import_payload.py --club "DAY=$DayCsv" --club "NM=$NmCsv" --out "$Payload"
    Stop-IfFailed "make_import_payload.py"

    Write-Host "Generating SQL load file..."
    Write-Host "  In:  $Payload"
    Write-Host "  Out: $LoadSql"
    python generate_load_sql.py "$Payload" "$LoadSql"
    Stop-IfFailed "generate_load_sql.py"

    Write-Host "Checking Wrangler login..."
    wrangler whoami
    Stop-IfFailed "Wrangler login check"

    Write-Host "Resetting D1 tables in foreign-key-safe order..."
    wrangler d1 execute tcscrabble-db --remote --command "DELETE FROM games;"
    Stop-IfFailed "Deleting games"

    wrangler d1 execute tcscrabble-db --remote --command "DELETE FROM players;"
    Stop-IfFailed "Deleting players"

    wrangler d1 execute tcscrabble-db --remote --command "DELETE FROM clubs;"
    Stop-IfFailed "Deleting clubs"

    Write-Host "Loading generated SQL into D1..."
    Write-Host "  File: $LoadSql"
    wrangler d1 execute tcscrabble-db --remote --file "$LoadSql"
    Stop-IfFailed "D1 load"

    Write-Host "Reload complete."
    Write-Host "Generated files are in: $DataDir"
}
finally {
    Pop-Location
}
