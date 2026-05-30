$ErrorActionPreference = "Stop"

$RepoDir = "C:\Users\lande\Documents\GitHub\twincitiesscrabble"
$DataDir = "C:\Users\lande\Documents\ScrabbleData"

$SheetGid = "411563466"
$DaySpreadsheetId = "1oYLaK2QGK7lOmzWtaNNa1pMSVTW-leyG7O_FjNM-xeQ"
$NmSpreadsheetId = "1vqbccA2TYLCRi6vcu2xfvVLyZJTpPpxYRUhrA6kCCMg"
$DayCsv = Join-Path $DataDir "Daytime Scrabble 2026 - latest.csv"
$NmCsv = Join-Path $DataDir "North Metro Scrabble 2026 - latest.csv"
$Payload = Join-Path $DataDir "combined_payload.json"
$RatingsPayload = Join-Path $DataDir "ratings_payload.json"
$CrossTablesHighlights = Join-Path $DataDir "cross_tables_highlights.json"
$CrossTablesReport = Join-Path $DataDir "cross_tables_highlights.txt"
$CrossTablesWarnings = Join-Path $DataDir "cross_tables_highlights_warnings.txt"
$LoadSql = Join-Path $DataDir "combined_load.sql"
$AcceptedMismatches = Join-Path $RepoDir "accepted_mismatches.txt"

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

function Download-GoogleSheetCsv {
    param(
        [string]$SpreadsheetId,
        [string]$Gid,
        [string]$OutPath,
        [string]$Description
    )

    $Url = "https://docs.google.com/spreadsheets/d/$SpreadsheetId/export?format=csv&gid=$Gid"
    $TempPath = "$OutPath.download"

    Write-Host "Downloading $Description CSV..."
    Write-Host "  URL: $Url"
    Write-Host "  Out: $OutPath"

    try {
        Invoke-WebRequest -Uri $Url -OutFile $TempPath -UseBasicParsing
    }
    catch {
        Stop-WithMessage "Could not download $Description CSV. Confirm the sheet is shared for link access. $($_.Exception.Message)"
    }

    $FirstLine = ""
    if (Test-Path -LiteralPath $TempPath -PathType Leaf) {
        $FirstLine = (Get-Content -LiteralPath $TempPath -TotalCount 1 -ErrorAction SilentlyContinue) -join ""
    }

    if (-not (Test-Path -LiteralPath $TempPath -PathType Leaf) -or ((Get-Item -LiteralPath $TempPath).Length -eq 0)) {
        Remove-Item -LiteralPath $TempPath -Force -ErrorAction SilentlyContinue
        Stop-WithMessage "Downloaded $Description CSV was empty."
    }

    if ($FirstLine -match '<!doctype|<html|ServiceLogin|accounts\.google\.com|Sign in') {
        Remove-Item -LiteralPath $TempPath -Force -ErrorAction SilentlyContinue
        Stop-WithMessage "Google returned an HTML page instead of $Description CSV. The sheet likely requires authenticated access or link sharing is not enabled."
    }

    Move-Item -LiteralPath $TempPath -Destination $OutPath -Force
}

Write-Host "Twin Cities Scrabble D1 reload"
Write-Host "Repo folder: $RepoDir"
Write-Host "Data folder: $DataDir"

if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) {
    Write-Host "Creating data folder..."
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
}

Download-GoogleSheetCsv $DaySpreadsheetId $SheetGid $DayCsv "Daytime"
Download-GoogleSheetCsv $NmSpreadsheetId $SheetGid $NmCsv "North Metro"

Require-File $DayCsv "Daytime CSV"
Require-File $NmCsv "North Metro CSV"

Push-Location $RepoDir
try {
    Require-File (Join-Path $RepoDir "make_import_payload.py") "make_import_payload.py"
    Require-File (Join-Path $RepoDir "ratings_refresh.py") "ratings_refresh.py"
    Require-File (Join-Path $RepoDir "scan_cross_tables_highlights.py") "scan_cross_tables_highlights.py"
    Require-File (Join-Path $RepoDir "player_external_ids.csv") "player_external_ids.csv"
    Require-File (Join-Path $RepoDir "generate_load_sql.py") "generate_load_sql.py"
    Require-File $AcceptedMismatches "accepted_mismatches.txt"
    Require-File (Join-Path $RepoDir "wrangler.toml") "wrangler.toml"

    Write-Host "Building game payload..."
    Write-Host "  DAY: $DayCsv"
    Write-Host "  NM:  $NmCsv"
    Write-Host "  Out: $Payload"
    Write-Host "  Accepted mismatches: $AcceptedMismatches"
    python make_import_payload.py --club "DAY=$DayCsv" --club "NM=$NmCsv" --accepted-mismatches "$AcceptedMismatches" --out "$Payload"
    Stop-IfFailed "make_import_payload.py"

    Write-Host "Refreshing external ratings..."
    Write-Host "  Out: $RatingsPayload"
    python ratings_refresh.py --external-ids "player_external_ids.csv" --players-json "$Payload" --out "$RatingsPayload"
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "ratings_refresh.py failed. Continuing with an empty ratings payload so game stats can still load."
        '{"ratings":[],"warnings":["ratings_refresh.py failed during reset_and_reload.ps1; game stats load continued"]}' | Set-Content -LiteralPath $RatingsPayload -Encoding UTF8
    }

    Write-Host "Scanning Cross-tables highlights..."
    Write-Host "  Out: $CrossTablesHighlights"
    Write-Host "  Report: $CrossTablesReport"
    python scan_cross_tables_highlights.py --players-json "$Payload" --external-ids "player_external_ids.csv" --out "$CrossTablesHighlights" --report "$CrossTablesReport" --warnings "$CrossTablesWarnings" --email-to "lande_hall@yahoo.com" --email-to "dustydame@gmail.com"
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "scan_cross_tables_highlights.py failed. Continuing so game stats can still load."
        '{"scanned_at":"","tournaments_scanned":[],"highlights":[],"warnings":["scan_cross_tables_highlights.py failed during reset_and_reload.ps1; game stats load continued"]}' | Set-Content -LiteralPath $CrossTablesHighlights -Encoding UTF8
        'scan_cross_tables_highlights.py failed during reset_and_reload.ps1; game stats load continued' | Set-Content -LiteralPath $CrossTablesWarnings -Encoding UTF8
        'Cross-tables highlight scan failed; game stats load continued.' | Set-Content -LiteralPath $CrossTablesReport -Encoding UTF8
    }
    elseif ((Test-Path -LiteralPath $CrossTablesWarnings -PathType Leaf) -and ((Get-Content -LiteralPath $CrossTablesWarnings -Raw).Trim().Length -gt 0)) {
        Write-Warning "Cross-tables highlight scan completed with warnings. See $CrossTablesWarnings"
    }

    Write-Host "Generating SQL..."
    Write-Host "  In:  $Payload"
    Write-Host "  Ratings: $RatingsPayload"
    Write-Host "  Out: $LoadSql"
    python generate_load_sql.py "$Payload" "$LoadSql" --ratings "$RatingsPayload"
    Stop-IfFailed "generate_load_sql.py"

    Write-Host "Checking Wrangler login..."
    wrangler whoami
    Stop-IfFailed "Wrangler login check"

    Write-Host "Resetting + loading DB..."
    Write-Host "Ensuring player_ratings table exists..."
    $CreateRatingsSql = 'CREATE TABLE IF NOT EXISTS player_ratings (player_id INTEGER PRIMARY KEY, naspa_rating INTEGER, wgpo_rating INTEGER, wgpo_wow_rating INTEGER, cross_tables_rating INTEGER, naspa_url TEXT, wgpo_url TEXT, cross_tables_url TEXT, rating_source_notes TEXT, ratings_updated_at TEXT NOT NULL, FOREIGN KEY (player_id) REFERENCES players(player_id));'
    wrangler d1 execute tcscrabble-db --remote --command $CreateRatingsSql
    Stop-IfFailed "Ensuring player_ratings table"

    Write-Host "Resetting D1 tables in foreign-key-safe order..."
    $DeleteGamesSql = 'DELETE FROM games;'
    wrangler d1 execute tcscrabble-db --remote --command $DeleteGamesSql
    Stop-IfFailed "Deleting games"

    $DeleteRatingsSql = 'DELETE FROM player_ratings;'
    wrangler d1 execute tcscrabble-db --remote --command $DeleteRatingsSql
    Stop-IfFailed "Deleting player_ratings"

    $DeletePlayersSql = 'DELETE FROM players;'
    wrangler d1 execute tcscrabble-db --remote --command $DeletePlayersSql
    Stop-IfFailed "Deleting players"

    $DeleteClubsSql = 'DELETE FROM clubs;'
    wrangler d1 execute tcscrabble-db --remote --command $DeleteClubsSql
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
