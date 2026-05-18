$ErrorActionPreference = "Stop"

$RepoDir = "C:\Users\lande\Documents\GitHub\twincitiesscrabble"
$DataDir = "C:\Users\lande\Documents\ScrabbleData"

$DayCsv = Join-Path $DataDir "Daytime Scrabble 2026 - May 7.csv"
$NmCsv = Join-Path $DataDir "North Metro Scrabble 2026 - May 7.csv"
$Payload = Join-Path $DataDir "combined_payload.json"
$RatingsPayload = Join-Path $DataDir "ratings_payload.json"
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
    Require-File (Join-Path $RepoDir "ratings_refresh.py") "ratings_refresh.py"
    Require-File (Join-Path $RepoDir "player_external_ids.csv") "player_external_ids.csv"
    Require-File (Join-Path $RepoDir "generate_load_sql.py") "generate_load_sql.py"
    Require-File (Join-Path $RepoDir "wrangler.toml") "wrangler.toml"

    Write-Host "📦 Building game payload..."
    Write-Host "  DAY: $DayCsv"
    Write-Host "  NM:  $NmCsv"
    Write-Host "  Out: $Payload"
    python make_import_payload.py --club "DAY=$DayCsv" --club "NM=$NmCsv" --out "$Payload"
    Stop-IfFailed "make_import_payload.py"

    Write-Host "⭐ Refreshing external ratings..."
    Write-Host "  Out: $RatingsPayload"
    python ratings_refresh.py --external-ids "player_external_ids.csv" --players-json "$Payload" --out "$RatingsPayload"
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "ratings_refresh.py failed. Continuing with an empty ratings payload so game stats can still load."
        '{"ratings":[],"warnings":["ratings_refresh.py failed during reset_and_reload.ps1; game stats load continued"]}' | Set-Content -LiteralPath $RatingsPayload -Encoding UTF8
    }

    Write-Host "🧾 Generating SQL..."
    Write-Host "  In:  $Payload"
    Write-Host "  Ratings: $RatingsPayload"
    Write-Host "  Out: $LoadSql"
    python generate_load_sql.py "$Payload" "$LoadSql" --ratings "$RatingsPayload"
    Stop-IfFailed "generate_load_sql.py"

    Write-Host "Checking Wrangler login..."
    wrangler whoami
    Stop-IfFailed "Wrangler login check"

    Write-Host "🧹 Resetting + loading DB..."
    Write-Host "Ensuring player_ratings table exists..."
    wrangler d1 execute tcscrabble-db --remote --command "CREATE TABLE IF NOT EXISTS player_ratings (player_id INTEGER PRIMARY KEY, naspa_rating INTEGER, wgpo_rating INTEGER, wgpo_wow_rating INTEGER, cross_tables_rating INTEGER, naspa_url TEXT, wgpo_url TEXT, cross_tables_url TEXT, rating_source_notes TEXT, ratings_updated_at TEXT NOT NULL, FOREIGN KEY (player_id) REFERENCES players(player_id));"
    Stop-IfFailed "Ensuring player_ratings table"

    Write-Host "Resetting D1 tables in foreign-key-safe order..."
    wrangler d1 execute tcscrabble-db --remote --command "DELETE FROM games;"
    Stop-IfFailed "Deleting games"

    wrangler d1 execute tcscrabble-db --remote --command "DELETE FROM player_ratings;"
    Stop-IfFailed "Deleting player_ratings"

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
