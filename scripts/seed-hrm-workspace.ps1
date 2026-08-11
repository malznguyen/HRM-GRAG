#Requires -Version 5.1

[CmdletBinding()]
param(
    [string]$TenantId,
    [string]$WorkspaceId,
    [string]$TenantName = "HRM",
    [string]$WorkspaceName = "HRM"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Import-RepositoryEnvironment {
    param([Parameter(Mandatory)][string]$RepositoryRoot)

    foreach ($path in @((Join-Path $RepositoryRoot ".env"), (Join-Path $RepositoryRoot "gmrag_api/.env"))) {
        if (-not (Test-Path -LiteralPath $path)) { continue }
        foreach ($line in Get-Content -LiteralPath $path) {
            if ($line -notmatch '^([^#=\s]+)=(.*)$') { continue }
            $name = $matches[1]
            if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, "Process"))) {
                [Environment]::SetEnvironmentVariable($name, $matches[2], "Process")
            }
        }
    }
}

function Get-RequiredEnvironmentValue {
    param([Parameter(Mandatory)][string]$Name)

    $value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value)) { throw "$Name must be set." }
    return $value
}

function ConvertTo-CanonicalUuid {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Value
    )

    $parsed = [Guid]::Empty
    if (-not [Guid]::TryParse($Value, [ref]$parsed)) { throw "$Name must be a UUID." }
    return $parsed.ToString()
}

function Get-AllOpenFgaTuples {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreId,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    $continuationToken = $null
    $tuples = @()
    do {
        $body = @{ page_size = 100 }
        if ($continuationToken) { $body.continuation_token = $continuationToken }
        $response = Invoke-RestMethod `
            -Method Post `
            -Uri "$ApiUrl/stores/$StoreId/read" `
            -Headers $Headers `
            -ContentType "application/json" `
            -Body ($body | ConvertTo-Json -Compress)
        $tuples += @($response.tuples)
        $continuationToken = $response.continuation_token
    } while ($continuationToken)
    return $tuples
}

Import-RepositoryEnvironment -RepositoryRoot $Root

if ([string]::IsNullOrWhiteSpace($TenantId)) {
    $TenantId = Get-RequiredEnvironmentValue -Name "HRM_TENANT_ID"
}
if ([string]::IsNullOrWhiteSpace($WorkspaceId)) {
    $WorkspaceId = Get-RequiredEnvironmentValue -Name "HRM_WORKSPACE_ID"
}
$TenantId = ConvertTo-CanonicalUuid -Name "TenantId" -Value $TenantId
$WorkspaceId = ConvertTo-CanonicalUuid -Name "WorkspaceId" -Value $WorkspaceId

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker must be installed and available on PATH."
}

$postgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "hrm_rag_user" }
$postgresDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "hrm_rag" }
$escapedTenantName = $TenantName.Replace("'", "''")
$escapedWorkspaceName = $WorkspaceName.Replace("'", "''")

$sql = @"
BEGIN;
INSERT INTO tenants (id, name)
VALUES ('$TenantId', '$escapedTenantName')
ON CONFLICT (id) DO NOTHING;
INSERT INTO workspaces (id, tenant_id, name)
VALUES ('$WorkspaceId', '$TenantId', '$escapedWorkspaceName')
ON CONFLICT (id) DO NOTHING;
COMMIT;
SELECT
    (SELECT COUNT(*) FROM tenants WHERE id = '$TenantId')::text || '|' ||
    (SELECT COUNT(*) FROM workspaces WHERE id = '$WorkspaceId' AND tenant_id = '$TenantId')::text;
"@

Push-Location $Root
try {
    $sqlOutput = @(& docker compose -p hrm-rag exec -T postgres `
        psql -v ON_ERROR_STOP=1 -qAt -U $postgresUser -d $postgresDb -c $sql 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL seed failed: $($sqlOutput -join [Environment]::NewLine)"
    }
}
finally {
    Pop-Location
}

$verification = @($sqlOutput | Where-Object { $_ -match '^\d+\|\d+$' })[-1]
if ($verification -ne "1|1") {
    throw "SQL seed verification failed (tenant|workspace=$verification). The workspace ID may already belong to another tenant."
}

$apiUrl = (Get-RequiredEnvironmentValue -Name "OPENFGA_API_URL").TrimEnd('/')
$storeId = Get-RequiredEnvironmentValue -Name "OPENFGA_STORE_ID"
$null = Get-RequiredEnvironmentValue -Name "OPENFGA_MODEL_ID"
$headers = @{ Authorization = "Bearer $(Get-RequiredEnvironmentValue -Name 'OPENFGA_API_TOKEN')" }

# These are the two structural tuples written by create_tenant/create_workspace.
# User admin/member tuples are intentionally not seeded: HRM request provisioning
# derives them from the signed role claim and keeps role changes synchronized.
$requiredTuples = @(
    [ordered]@{
        user = "platform:system"
        relation = "platform"
        object = "tenant:$TenantId"
    },
    [ordered]@{
        user = "tenant:$TenantId"
        relation = "tenant"
        object = "workspace:$WorkspaceId"
    }
)

$existingKeys = @{}
foreach ($entry in @(Get-AllOpenFgaTuples -ApiUrl $apiUrl -StoreId $storeId -Headers $headers)) {
    $key = $entry.key
    $existingKeys["$($key.user)|$($key.relation)|$($key.object)"] = $true
}

$missing = @($requiredTuples | Where-Object {
    -not $existingKeys.ContainsKey("$($_.user)|$($_.relation)|$($_.object)")
})
if ($missing.Count -gt 0) {
    $payload = @{ writes = @{ tuple_keys = $missing } }
    Invoke-RestMethod `
        -Method Post `
        -Uri "$apiUrl/stores/$storeId/write" `
        -Headers $headers `
        -ContentType "application/json" `
        -Body ($payload | ConvertTo-Json -Depth 8 -Compress) | Out-Null
}

$actualKeys = @{}
foreach ($entry in @(Get-AllOpenFgaTuples -ApiUrl $apiUrl -StoreId $storeId -Headers $headers)) {
    $key = $entry.key
    $actualKeys["$($key.user)|$($key.relation)|$($key.object)"] = $true
}
foreach ($tuple in $requiredTuples) {
    $key = "$($tuple.user)|$($tuple.relation)|$($tuple.object)"
    if (-not $actualKeys.ContainsKey($key)) { throw "OpenFGA tuple verification failed: $key" }
}

Write-Output "HRM_TENANT_ID=$TenantId"
Write-Output "HRM_WORKSPACE_ID=$WorkspaceId"
Write-Output "OPENFGA_STRUCTURAL_TUPLES=2"
