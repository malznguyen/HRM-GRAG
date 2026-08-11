#Requires -Version 5.1

[CmdletBinding()]
param(
    [string]$StoreName = "hrm-rag-dev",
    [string]$ApiUrl
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ModelPath = Join-Path $Root "gmrag_api/openfga/model.fga"
$CliImage = "openfga/cli:v0.7.15"

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

function Get-OpenFgaHeaders {
    return @{ Authorization = "Bearer $(Get-RequiredEnvironmentValue -Name 'OPENFGA_API_TOKEN')" }
}

function Get-OpenFgaStores {
    param([Parameter(Mandatory)][string]$BaseUrl)

    $token = $null
    $stores = @()
    do {
        $uri = "$BaseUrl/stores?page_size=100"
        if ($token) { $uri += "&continuation_token=$([Uri]::EscapeDataString($token))" }
        $response = Invoke-RestMethod -Method Get -Uri $uri -Headers (Get-OpenFgaHeaders)
        $stores += @($response.stores)
        $token = $response.continuation_token
    } while ($token)
    return $stores
}

Import-RepositoryEnvironment -RepositoryRoot $Root

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker must be installed and available on PATH."
}
if (-not (Test-Path -LiteralPath $ModelPath -PathType Leaf)) {
    throw "OpenFGA model file not found: $ModelPath"
}
if ($StoreName -notmatch '^[a-zA-Z0-9._-]+$') {
    throw "StoreName may contain only letters, numbers, dot, underscore, and hyphen."
}

if ([string]::IsNullOrWhiteSpace($ApiUrl)) {
    $ApiUrl = Get-RequiredEnvironmentValue -Name "OPENFGA_API_URL"
}
$ApiUrl = $ApiUrl.TrimEnd('/')

# Compose enables preshared authentication. Requiring the token here prevents a
# misleading 401 and ensures every store/model request carries the bearer header.
$null = Get-RequiredEnvironmentValue -Name "OPENFGA_API_TOKEN"
$Headers = Get-OpenFgaHeaders

# New-TestOpenFgaStore clones a model from an existing source store. Recovery on
# a blank machine has no source store, so bootstrap compiles model.fga directly.
$existing = @(Get-OpenFgaStores -BaseUrl $ApiUrl | Where-Object { $_.name -eq $StoreName })
if ($existing.Count -gt 0) {
    $ids = ($existing | ForEach-Object { $_.id }) -join ', '
    throw "OpenFGA store '$StoreName' already exists (id: $ids). Refusing to create a duplicate; reuse it deliberately or choose a different -StoreName."
}

$store = Invoke-RestMethod `
    -Method Post `
    -Uri "$ApiUrl/stores" `
    -Headers $Headers `
    -ContentType "application/json" `
    -Body (@{ name = $StoreName } | ConvertTo-Json -Compress)

try {
    $mount = "type=bind,source=$Root,target=/work,readonly"
    $jsonLines = @(& docker run --rm --mount $mount -w /work $CliImage `
        model transform `
        --file /work/gmrag_api/openfga/model.fga `
        --input-format fga `
        --output-format json 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "FGA model transform failed: $($jsonLines -join [Environment]::NewLine)"
    }

    $compiled = ($jsonLines -join [Environment]::NewLine) | ConvertFrom-Json
    $modelBody = [ordered]@{
        schema_version = $compiled.schema_version
        type_definitions = @($compiled.type_definitions)
    }
    if ($compiled.PSObject.Properties.Name -contains "conditions" -and $null -ne $compiled.conditions) {
        $modelBody.conditions = $compiled.conditions
    }

    $model = Invoke-RestMethod `
        -Method Post `
        -Uri "$ApiUrl/stores/$($store.id)/authorization-models" `
        -Headers $Headers `
        -ContentType "application/json" `
        -Body ($modelBody | ConvertTo-Json -Depth 100 -Compress)
}
catch {
    try {
        Invoke-RestMethod -Method Delete -Uri "$ApiUrl/stores/$($store.id)" -Headers $Headers | Out-Null
    }
    catch {
        Write-Warning "Bootstrap failed and cleanup of store $($store.id) also failed."
    }
    throw
}

Write-Output "OPENFGA_STORE_ID=$($store.id)"
Write-Output "OPENFGA_MODEL_ID=$($model.authorization_model_id)"
