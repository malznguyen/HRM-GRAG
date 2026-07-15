#Requires -Version 5.1

[CmdletBinding()]
param(
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ApiRoot = Join-Path $Root "gmrag_api"
$DatabaseUrl = "postgres://gmrag_user:change_me@127.0.0.1:5432/gmrag"
$CollectionName = "gmrag_document_chunks"

function Assert-CommandAvailable {
    param([Parameter(Mandatory)][string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command is unavailable: $Name"
    }
}

function Invoke-SqlxMigrationInfo {
    Push-Location $ApiRoot
    try {
        $previousDatabaseUrl = $env:DATABASE_URL
        $env:DATABASE_URL = $DatabaseUrl
        & sqlx migrate info --source migrations
        if ($LASTEXITCODE -ne 0) {
            throw "sqlx migrate info failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        $env:DATABASE_URL = $previousDatabaseUrl
        Pop-Location
    }
}

function Write-ManualHandoff {
    Write-Host @'
# 1) Full stack
docker compose up -d --build
docker compose ps
docker compose logs --tail 100 ingestion-worker   # confirm it passes the startup probe, no crash-loop

# 2) API on host (separate terminal, stays foreground)
Set-Location 'C:\Users\admin\Desktop\Project\GMRAG\gmrag_api'
$env:QDRANT_URL='http://127.0.0.1:6333'; $env:QDRANT_VECTOR_SIZE='1024'
$env:OLLAMA_EMBED_URL='http://127.0.0.1:11434/api/embed'
$env:OLLAMA_EMBED_MODEL='AITeamVN/Vietnamese_Embedding'
cargo run --locked

# 3) Token: login on frontend -> F12 Network -> copy the Authorization: Bearer value
#    Need a workspace where that user is owner/admin (create at /workspaces, copy UUID)

# 4) Smoke (third terminal)
$env:GMRAG_SMOKE_WORKSPACE_ID='<uuid>'; $env:GMRAG_SMOKE_BEARER_TOKEN='<token>'
.\scripts\run-local-ingestion-smoke.ps1
# expect: Local ingestion smoke: PASS
'@
}

Assert-CommandAvailable -Name "sqlx"

if (-not $Execute) {
    Write-Host "PREVIEW ONLY - no Qdrant collection or database migration will be changed."
    Write-Host "Would delete Qdrant collection: $CollectionName"
    Invoke-SqlxMigrationInfo
    Write-Host "docker compose up -d --build"
    exit 0
}

if ($PSVersionTable.PSVersion.Major -lt 7) {
    $pwsh = Get-Command "pwsh" -ErrorAction SilentlyContinue
    if ($null -eq $pwsh) {
        throw "-Execute requires pwsh 7+ because scripts/lib/gmrag-local-ops.ps1 requires PowerShell 7."
    }
    & $pwsh.Source -NoProfile -File $PSCommandPath -Execute
    exit $LASTEXITCODE
}

Assert-CommandAvailable -Name "docker"
. (Join-Path $PSScriptRoot "lib/gmrag-local-ops.ps1")

Remove-ComposeQdrantCollection `
    -Root $Root `
    -CollectionName $CollectionName `
    -AllowSharedResetCollection

Push-Location $ApiRoot
try {
    $previousDatabaseUrl = $env:DATABASE_URL
    $env:DATABASE_URL = $DatabaseUrl
    & sqlx migrate run --source migrations
    $migrationExitCode = $LASTEXITCODE
}
finally {
    $env:DATABASE_URL = $previousDatabaseUrl
    Pop-Location
}

if ($migrationExitCode -ne 0) {
    exit $migrationExitCode
}

Write-ManualHandoff
