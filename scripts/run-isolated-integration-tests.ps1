#Requires -Version 7

[CmdletBinding()]
param(
    [switch]$VerifyPanicCleanup,
    [switch]$SweepOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
. (Join-Path $PSScriptRoot "lib/gmrag-local-ops.ps1")
Import-GmragEnvironment -Root $Root

function Get-DevStoreSnapshot {
    param(
        [Parameter(Mandatory)][string]$Database,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$FgaUrl,
        [Parameter(Mandatory)][string]$FgaStoreId
    )

    $tables = @(
        "audit_events", "authz_outbox", "chat_messages", "chat_sessions",
        "document_chunks", "document_shares", "documents", "graph_edge_sources",
        "graph_edges", "graph_node_sources", "graph_nodes", "ingestion_jobs",
        "qdrant_outbox", "storage_outbox", "tenant_members", "tenants", "users",
        "workspace_members", "workspaces"
    )
    $sqlParts = $tables | ForEach-Object { "SELECT '$_' AS table_name, count(*)::text AS row_count FROM $_" }
    $databaseCounts = @(Invoke-PostgresSql -Root $Root -Database $Database -Sql (($sqlParts -join " UNION ALL ") + " ORDER BY table_name"))
    $qdrantCounts = [ordered]@{}
    foreach ($collection in @(Get-ComposeQdrantCollections -Root $Root | Where-Object { $_ -in @("gmrag_document_chunks", "gmrag_document_chunks_test") })) {
        $qdrantCounts[$collection] = Get-ComposeQdrantPointCount -Root $Root -CollectionName $collection
    }
    return [ordered]@{
        postgres = @($databaseCounts)
        qdrant = $qdrantCounts
        storage_objects = Get-MinioObjectCount -Root $Root -Bucket $Bucket
        openfga_tuples = @(Get-OpenFgaTuples -ApiUrl $FgaUrl -StoreId $FgaStoreId).Count
    }
}

function Remove-StaleTestNamespaces {
    param(
        [Parameter(Mandatory)][string]$FgaUrl
    )

    $removedDatabases = 0
    foreach ($database in @(Get-TestDatabaseNames -Root $Root)) {
        Remove-TestDatabase -Root $Root -DatabaseName $database
        $removedDatabases++
    }

    $removedBuckets = 0
    foreach ($bucket in @(Get-MinioBuckets -Root $Root | Where-Object { $_ -like "gmrag-test-*" })) {
        Remove-TestMinioBucket -Root $Root -Bucket $bucket
        $removedBuckets++
    }

    $removedStores = 0
    foreach ($store in @(Get-OpenFgaStores -ApiUrl $FgaUrl | Where-Object { $_.name -like "gmrag-test-*" })) {
        Remove-TestOpenFgaStore -ApiUrl $FgaUrl -StoreId $store.id -ExpectedName $store.name
        $removedStores++
    }

    $removedQdrant = 0
    foreach ($container in @(& docker ps -a --format "{{.Names}}" | Where-Object { $_ -like "gmrag-test-qdrant-*" })) {
        Remove-TestQdrantContainer -ContainerName $container
        $removedQdrant++
    }
    foreach ($collection in @(Get-ComposeQdrantCollections -Root $Root | Where-Object { $_ -match '^gmrag_test_[a-z0-9_]+$' })) {
        Remove-ComposeQdrantCollection -Root $Root -CollectionName $collection
        $removedQdrant++
    }

    if (@(Get-TestDatabaseNames -Root $Root).Count -ne 0) { throw "Stale PostgreSQL test namespace sweep was incomplete." }
    if (@(Get-MinioBuckets -Root $Root | Where-Object { $_ -like "gmrag-test-*" }).Count -ne 0) { throw "Stale MinIO test namespace sweep was incomplete." }
    if (@(Get-OpenFgaStores -ApiUrl $FgaUrl | Where-Object { $_.name -like "gmrag-test-*" }).Count -ne 0) { throw "Stale OpenFGA test namespace sweep was incomplete." }
    if (@(& docker ps -a --format "{{.Names}}" | Where-Object { $_ -like "gmrag-test-qdrant-*" }).Count -ne 0) { throw "Stale Qdrant container sweep was incomplete." }
    if (@(Get-ComposeQdrantCollections -Root $Root | Where-Object { $_ -match '^gmrag_test_[a-z0-9_]+$' }).Count -ne 0) { throw "Stale shared-Qdrant collection sweep was incomplete." }

    Write-Host "Sweep complete: postgres=$removedDatabases qdrant=$removedQdrant storage=$removedBuckets openfga=$removedStores"
}

function Assert-RuntimeIsolationEnvironment {
    param(
        [Parameter(Mandatory)][string]$TestDatabaseUrl,
        [Parameter(Mandatory)][string]$DevDatabaseUrl,
        [Parameter(Mandatory)][string]$TestCollection,
        [Parameter(Mandatory)][string]$DevCollection,
        [Parameter(Mandatory)][string]$TestBucket,
        [Parameter(Mandatory)][string]$DevBucket,
        [Parameter(Mandatory)][string]$TestStoreId,
        [Parameter(Mandatory)][string]$DevStoreId
    )

    if ($env:DATABASE_URL -ne $TestDatabaseUrl -or $env:TEST_DATABASE_URL -ne $TestDatabaseUrl -or $TestDatabaseUrl -eq $DevDatabaseUrl) { throw "PostgreSQL isolation environment guard failed." }
    if ($env:QDRANT_COLLECTION -ne $TestCollection -or $env:TEST_QDRANT_COLLECTION -ne $TestCollection -or $TestCollection -eq $DevCollection) { throw "Qdrant isolation environment guard failed." }
    if ($env:S3_BUCKET -ne $TestBucket -or $env:TEST_S3_BUCKET -ne $TestBucket -or $TestBucket -eq $DevBucket) { throw "MinIO isolation environment guard failed." }
    if ($env:OPENFGA_STORE_ID -ne $TestStoreId -or $env:TEST_OPENFGA_STORE_ID -ne $TestStoreId -or $TestStoreId -eq $DevStoreId) { throw "OpenFGA isolation environment guard failed." }
}

if ($env:APP_ENV -ne "test") { throw "Refusing integration runner: APP_ENV must be exactly test." }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "docker is required." }
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { throw "cargo is required." }

$devDatabaseUrl = Get-RequiredEnvironmentValue -Name "DATABASE_URL"
$devDatabase = Get-DatabaseTarget -DatabaseUrl $devDatabaseUrl
if (-not (Test-LocalHostName $devDatabase.Host)) { throw "DATABASE_URL must target local PostgreSQL." }
if ($devDatabase.Database -match '^gmrag_test_') { throw "DATABASE_URL must identify the dev database, not a test namespace." }

$devCollection = if ($env:QDRANT_COLLECTION) { $env:QDRANT_COLLECTION } else { "gmrag_document_chunks" }
if ($devCollection -match '^gmrag_test_') { throw "Configured dev Qdrant collection uses the reserved test prefix." }
$devQdrantUrl = if ($env:QDRANT_URL) { $env:QDRANT_URL } else { "http://qdrant:6333" }
Assert-LocalUrl -Name "QDRANT_URL" -Value $devQdrantUrl | Out-Null
$s3Endpoint = Get-RequiredEnvironmentValue -Name "S3_ENDPOINT_URL"
Assert-LocalUrl -Name "S3_ENDPOINT_URL" -Value $s3Endpoint | Out-Null
$devBucket = Get-RequiredEnvironmentValue -Name "S3_BUCKET"
if ($devBucket -like "gmrag-test-*") { throw "Configured dev bucket uses the reserved test prefix." }
$fgaUrl = Get-RequiredEnvironmentValue -Name "OPENFGA_API_URL"
Assert-LocalUrl -Name "OPENFGA_API_URL" -Value $fgaUrl | Out-Null
$devStoreId = Get-RequiredEnvironmentValue -Name "OPENFGA_STORE_ID"
$devStore = Invoke-RestMethod -Method Get -Uri "$($fgaUrl.TrimEnd('/'))/stores/$devStoreId"
if ($devStore.name -like "gmrag-test-*") { throw "Configured dev OpenFGA store uses the reserved test prefix." }

$lockPath = Join-Path ([IO.Path]::GetTempPath()) "gmrag-test-isolation.lock"
try {
    $lockStream = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
}
catch { throw "Another isolated integration run is active. Refusing concurrent namespace sweep." }

$workersToRestore = @()
$databaseCreated = $false
$bucketCreated = $false
$fgaNamespace = $null
$qdrantNamespace = $null
$cleanupErrors = [System.Collections.Generic.List[string]]::new()
$runError = $null
$testExitCode = 0
$expectedPanicObserved = $false
$baselineJson = $null

$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$random = [Guid]::NewGuid().ToString("N").Substring(0, 10)
$databaseName = "gmrag_test_${timestamp}_$random"
$collectionName = "gmrag_test_${timestamp}_$random"
$bucketName = "gmrag-test-$timestamp-$random"
$storeName = "gmrag-test-$timestamp-$random"
$containerName = "gmrag-test-qdrant-$timestamp-$random"
$testDatabaseUrl = New-DatabaseUrl -BaseUrl $devDatabaseUrl -DatabaseName $databaseName

if ($databaseName -eq $devDatabase.Database) { throw "Generated test database collides with dev." }
if ($collectionName -eq $devCollection) { throw "Generated test collection collides with dev." }
if ($bucketName -eq $devBucket) { throw "Generated test bucket collides with dev." }

Write-Host "APP_ENV=$($env:APP_ENV)"
Write-Host "dev_postgres=$($devDatabase.Host):$($devDatabase.Port)/$($devDatabase.Database)"
Write-Host "test_postgres=$($devDatabase.Host):$($devDatabase.Port)/$databaseName"
Write-Host "dev_qdrant_collection=$devCollection"
Write-Host "test_qdrant_collection=$collectionName"
Write-Host "dev_storage_bucket=$devBucket"
Write-Host "test_storage_bucket=$bucketName"
Write-Host "dev_openfga_store=$devStoreId"
Write-Host "test_openfga_store_name=$storeName"

try {
    $runningServices = @()
    Push-Location $Root
    try { $runningServices = @(& docker compose ps --status running --services) } finally { Pop-Location }
    foreach ($service in @("postgres", "minio", "openfga", "qdrant")) {
        if ($runningServices -notcontains $service) { throw "Required Docker Compose service is not running: $service" }
    }
    $workersToRestore = @($runningServices | Where-Object { $_ -in @("process-authz-outbox", "process-qdrant-outbox", "storage-orphan-scan") })
    if ($workersToRestore.Count -gt 0) {
        Push-Location $Root
        try { & docker compose stop @workersToRestore | Out-Null; if ($LASTEXITCODE -ne 0) { throw "Could not pause background workers." } } finally { Pop-Location }
    }

    Remove-StaleTestNamespaces -FgaUrl $fgaUrl
    if ($SweepOnly) { return }

    $baselineJson = (Get-DevStoreSnapshot -Database $devDatabase.Database -Bucket $devBucket -FgaUrl $fgaUrl -FgaStoreId $devStoreId | ConvertTo-Json -Depth 10 -Compress)

    New-TestDatabase -Root $Root -DatabaseName $databaseName
    $databaseCreated = $true
    New-TestMinioBucket -Root $Root -Bucket $bucketName
    $bucketCreated = $true
    $fgaNamespace = New-TestOpenFgaStore -ApiUrl $fgaUrl -StoreName $storeName -SourceStoreId $devStoreId -SourceModelId $env:OPENFGA_MODEL_ID
    if ($fgaNamespace.StoreId -eq $devStoreId) { throw "Generated OpenFGA store collides with dev." }
    $qdrantNamespace = New-TestQdrantContainer -ContainerName $containerName -CollectionName $collectionName

    $env:GMRAG_TEST_RUN_ID = $databaseName
    $env:DEV_DATABASE_URL = $devDatabaseUrl
    $env:TEST_DATABASE_URL = $testDatabaseUrl
    $env:DATABASE_URL = $testDatabaseUrl
    $env:DEV_QDRANT_COLLECTION = $devCollection
    $env:DEV_QDRANT_URL = $devQdrantUrl
    $env:TEST_QDRANT_URL = $qdrantNamespace.Url
    $env:TEST_QDRANT_COLLECTION = $collectionName
    $env:QDRANT_URL = $qdrantNamespace.Url
    $env:QDRANT_COLLECTION = $collectionName
    $env:DEV_S3_BUCKET = $devBucket
    $env:TEST_S3_BUCKET = $bucketName
    $env:S3_BUCKET = $bucketName
    $env:DEV_OPENFGA_STORE_ID = $devStoreId
    $env:TEST_OPENFGA_STORE_ID = $fgaNamespace.StoreId
    $env:TEST_OPENFGA_STORE_NAME = $storeName
    $env:OPENFGA_STORE_ID = $fgaNamespace.StoreId
    $env:OPENFGA_MODEL_ID = $fgaNamespace.ModelId
    $env:CARGO_INCREMENTAL = "0"
    $env:RUST_TEST_THREADS = "1"

    Assert-RuntimeIsolationEnvironment -TestDatabaseUrl $testDatabaseUrl -DevDatabaseUrl $devDatabaseUrl -TestCollection $collectionName -DevCollection $devCollection -TestBucket $bucketName -DevBucket $devBucket -TestStoreId $fgaNamespace.StoreId -DevStoreId $devStoreId

    Push-Location (Join-Path $Root "gmrag_api")
    try {
        if ($VerifyPanicCleanup) {
            $env:GMRAG_TEST_FORCE_PANIC = "1"
            & cargo test --locked --test test_isolation_panic_probe -- --nocapture
            $testExitCode = $LASTEXITCODE
            if ($testExitCode -eq 0) { throw "Forced-panic probe unexpectedly passed." }
            $expectedPanicObserved = $true
        }
        else {
            Remove-Item Env:GMRAG_TEST_FORCE_PANIC -ErrorAction SilentlyContinue
            & cargo test --locked
            $testExitCode = $LASTEXITCODE
        }
    }
    finally { Pop-Location }
}
catch { $runError = $_ }
finally {
    if ($qdrantNamespace) {
        try { Remove-TestQdrantContainer -ContainerName $qdrantNamespace.ContainerName } catch { $cleanupErrors.Add("qdrant: $($_.Exception.Message)") }
    }
    if ($fgaNamespace) {
        try { Remove-TestOpenFgaStore -ApiUrl $fgaUrl -StoreId $fgaNamespace.StoreId -ExpectedName $storeName } catch { $cleanupErrors.Add("openfga: $($_.Exception.Message)") }
    }
    if ($bucketCreated) {
        try { Remove-TestMinioBucket -Root $Root -Bucket $bucketName } catch { $cleanupErrors.Add("storage: $($_.Exception.Message)") }
    }
    if ($databaseCreated) {
        try { Remove-TestDatabase -Root $Root -DatabaseName $databaseName } catch { $cleanupErrors.Add("postgres: $($_.Exception.Message)") }
    }

    try {
        if (@(Get-TestDatabaseNames -Root $Root) -contains $databaseName) { throw "database still exists" }
    } catch { $cleanupErrors.Add("postgres verification: $($_.Exception.Message)") }
    try {
        if (@(Get-MinioBuckets -Root $Root) -contains $bucketName) { throw "bucket still exists" }
    } catch { $cleanupErrors.Add("storage verification: $($_.Exception.Message)") }
    if ($fgaNamespace) {
        try {
            if (@(Get-OpenFgaStores -ApiUrl $fgaUrl | Where-Object { $_.id -eq $fgaNamespace.StoreId }).Count -ne 0) { throw "store still exists" }
        } catch { $cleanupErrors.Add("openfga verification: $($_.Exception.Message)") }
    }
    try {
        if (@(& docker ps -a --format "{{.Names}}") -contains $containerName) { throw "container still exists" }
    } catch { $cleanupErrors.Add("qdrant verification: $($_.Exception.Message)") }

    if ($baselineJson) {
        try {
            $afterJson = (Get-DevStoreSnapshot -Database $devDatabase.Database -Bucket $devBucket -FgaUrl $fgaUrl -FgaStoreId $devStoreId | ConvertTo-Json -Depth 10 -Compress)
            if ($afterJson -ne $baselineJson) { throw "Dev stores changed during the isolated test run." }
            Write-Host "Dev-store invariant: unchanged across PostgreSQL, Qdrant, MinIO, and OpenFGA."
        }
        catch { $cleanupErrors.Add("dev invariant: $($_.Exception.Message)") }
    }

    if ($workersToRestore.Count -gt 0) {
        try {
            Push-Location $Root
            try { & docker compose start @workersToRestore | Out-Null; if ($LASTEXITCODE -ne 0) { throw "Could not restore background workers." } } finally { Pop-Location }
        }
        catch { $cleanupErrors.Add("worker restore: $($_.Exception.Message)") }
    }
    $lockStream.Dispose()
}

if ($cleanupErrors.Count -gt 0) {
    $cleanupErrors | ForEach-Object { Write-Error $_ }
    exit 3
}
if ($runError) { throw $runError }
if ($VerifyPanicCleanup) {
    if (-not $expectedPanicObserved) { throw "Forced-panic probe was not observed." }
    Write-Host "Forced-panic teardown verified for PostgreSQL, Qdrant, MinIO, and OpenFGA."
    exit 0
}
exit $testExitCode
