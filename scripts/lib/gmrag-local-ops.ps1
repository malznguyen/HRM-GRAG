#Requires -Version 7

Set-StrictMode -Version Latest

function Import-GmragEnvironment {
    param([Parameter(Mandatory)][string]$Root)

    foreach ($path in @((Join-Path $Root ".env"), (Join-Path $Root "gmrag_api/.env"))) {
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

function Test-LocalHostName {
    param([Parameter(Mandatory)][string]$HostName)

    return $HostName.ToLowerInvariant() -in @("localhost", "127.0.0.1", "::1", "postgres", "minio", "openfga", "qdrant")
}

function Assert-LocalUrl {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Value
    )

    try { $uri = [Uri]$Value } catch { throw "$Name must be a valid URL." }
    if (-not (Test-LocalHostName $uri.Host)) { throw "$Name must target a local host; resolved host=$($uri.Host)." }
    return $uri
}

function Get-DatabaseTarget {
    param([Parameter(Mandatory)][string]$DatabaseUrl)

    try { $uri = [Uri]$DatabaseUrl } catch { throw "DATABASE_URL must be a valid PostgreSQL URL." }
    $databaseName = $uri.AbsolutePath.TrimStart('/')
    if ([string]::IsNullOrWhiteSpace($databaseName)) { throw "DATABASE_URL must include a database name." }
    return [pscustomobject]@{
        Host = $uri.Host
        Port = $uri.Port
        Database = $databaseName
    }
}

function New-DatabaseUrl {
    param(
        [Parameter(Mandatory)][string]$BaseUrl,
        [Parameter(Mandatory)][string]$DatabaseName
    )

    if ($DatabaseName -notmatch '^[a-z0-9_]+$') { throw "Unsafe database name: $DatabaseName" }
    $builder = [UriBuilder]$BaseUrl
    $builder.Path = "/$DatabaseName"
    return $builder.Uri.AbsoluteUri
}

function Invoke-PostgresSql {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Sql,
        [string]$Database = "postgres"
    )

    $postgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "gmrag_user" }
    Push-Location $Root
    try {
        $output = @(& docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U $postgresUser -d $Database -At -c $Sql 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL command failed: $($output -join [Environment]::NewLine)" }
        return @($output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    }
    finally { Pop-Location }
}

function Get-TestDatabaseNames {
    param([Parameter(Mandatory)][string]$Root)

    return @(Invoke-PostgresSql -Root $Root -Sql "SELECT datname FROM pg_database WHERE datname LIKE 'gmrag_test_%' ORDER BY datname")
}

function New-TestDatabase {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$DatabaseName
    )

    if ($DatabaseName -notmatch '^gmrag_test_[a-z0-9_]+$') { throw "Refusing unsafe test database name: $DatabaseName" }
    Invoke-PostgresSql -Root $Root -Sql "CREATE DATABASE `"$DatabaseName`" TEMPLATE template0" | Out-Null
}

function Remove-TestDatabase {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$DatabaseName
    )

    if ($DatabaseName -notmatch '^gmrag_test_[a-z0-9_]+$') { throw "Refusing unsafe test database name: $DatabaseName" }
    $escaped = $DatabaseName.Replace("'", "''")
    Invoke-PostgresSql -Root $Root -Sql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$escaped' AND pid <> pg_backend_pid()" | Out-Null
    Invoke-PostgresSql -Root $Root -Sql "DROP DATABASE IF EXISTS `"$DatabaseName`"" | Out-Null
}

function Invoke-MinioCommand {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Command
    )

    $script = 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && ' + $Command
    Push-Location $Root
    try {
        $output = @(& docker compose run --rm --no-deps --entrypoint /bin/sh minio-init -c $script 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "MinIO command failed: $($output -join [Environment]::NewLine)" }
        return $output
    }
    finally { Pop-Location }
}

function Get-MinioBuckets {
    param([Parameter(Mandatory)][string]$Root)

    $lines = Invoke-MinioCommand -Root $Root -Command 'mc ls --json local'
    $buckets = foreach ($line in $lines) {
        if ($line -notmatch '^\s*\{') { continue }
        $record = $line | ConvertFrom-Json
        if ($record.type -eq "folder") { $record.key.TrimEnd('/') }
    }
    return @($buckets | Sort-Object -Unique)
}

function New-TestMinioBucket {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Bucket
    )

    if ($Bucket -notmatch '^gmrag-test-[a-z0-9-]+$') { throw "Refusing unsafe test bucket: $Bucket" }
    Invoke-MinioCommand -Root $Root -Command "mc mb --ignore-existing 'local/$Bucket' >/dev/null && mc anonymous set private 'local/$Bucket' >/dev/null" | Out-Null
}

function Remove-TestMinioBucket {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Bucket
    )

    if ($Bucket -notmatch '^gmrag-test-[a-z0-9-]+$') { throw "Refusing unsafe test bucket: $Bucket" }
    $existing = Get-MinioBuckets -Root $Root
    if ($existing -contains $Bucket) {
        Invoke-MinioCommand -Root $Root -Command "mc rb --force 'local/$Bucket' >/dev/null" | Out-Null
    }
}

function Get-MinioObjectCount {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$Bucket,
        [string]$Prefix = ""
    )

    if ($Bucket -notmatch '^[a-z0-9][a-z0-9.-]+$') { throw "Unsafe bucket name: $Bucket" }
    if ($Prefix -notmatch '^[a-zA-Z0-9_./-]*$') { throw "Unsafe object prefix: $Prefix" }
    $path = if ($Prefix) { "local/$Bucket/$Prefix" } else { "local/$Bucket" }
    $output = Invoke-MinioCommand -Root $Root -Command "mc ls --recursive '$path' | wc -l"
    return [int](@($output)[-1].Trim())
}

function Get-OpenFgaStores {
    param([Parameter(Mandatory)][string]$ApiUrl)

    $base = $ApiUrl.TrimEnd('/')
    $token = $null
    $stores = @()
    do {
        $uri = "$base/stores?page_size=100"
        if ($token) { $uri += "&continuation_token=$([Uri]::EscapeDataString($token))" }
        $response = Invoke-RestMethod -Method Get -Uri $uri
        $stores += @($response.stores)
        $token = $response.continuation_token
    } while ($token)
    return $stores
}

function Get-OpenFgaModel {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreId,
        [string]$ModelId
    )

    $base = $ApiUrl.TrimEnd('/')
    if ($ModelId) {
        return (Invoke-RestMethod -Method Get -Uri "$base/stores/$StoreId/authorization-models/$ModelId").authorization_model
    }
    $response = Invoke-RestMethod -Method Get -Uri "$base/stores/$StoreId/authorization-models?page_size=1"
    $models = @($response.authorization_models)
    if ($models.Count -ne 1) { throw "Could not resolve exactly one source OpenFGA model." }
    return $models[0]
}

function New-TestOpenFgaStore {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreName,
        [Parameter(Mandatory)][string]$SourceStoreId,
        [string]$SourceModelId
    )

    if ($StoreName -notmatch '^gmrag-test-[a-z0-9-]+$') { throw "Refusing unsafe test OpenFGA store name: $StoreName" }
    $base = $ApiUrl.TrimEnd('/')
    $store = Invoke-RestMethod -Method Post -Uri "$base/stores" -ContentType "application/json" -Body (@{ name = $StoreName } | ConvertTo-Json -Compress)
    try {
        $sourceModel = Get-OpenFgaModel -ApiUrl $base -StoreId $SourceStoreId -ModelId $SourceModelId
        $modelBody = @{
            schema_version = $sourceModel.schema_version
            type_definitions = @($sourceModel.type_definitions)
            conditions = $sourceModel.conditions
        }
        $model = Invoke-RestMethod -Method Post -Uri "$base/stores/$($store.id)/authorization-models" -ContentType "application/json" -Body ($modelBody | ConvertTo-Json -Depth 100 -Compress)
        return [pscustomobject]@{
            StoreId = $store.id
            StoreName = $store.name
            ModelId = $model.authorization_model_id
        }
    }
    catch {
        try { Invoke-RestMethod -Method Delete -Uri "$base/stores/$($store.id)" | Out-Null } catch {}
        throw
    }
}

function Remove-TestOpenFgaStore {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreId,
        [Parameter(Mandatory)][string]$ExpectedName
    )

    if ($ExpectedName -notmatch '^gmrag-test-[a-z0-9-]+$') { throw "Refusing unsafe test OpenFGA store name: $ExpectedName" }
    $base = $ApiUrl.TrimEnd('/')
    try { $store = Invoke-RestMethod -Method Get -Uri "$base/stores/$StoreId" } catch {
        if ($_.Exception.Response.StatusCode -eq 404) { return }
        throw
    }
    if ($store.name -ne $ExpectedName) { throw "Refusing to delete OpenFGA store $StoreId because its name is '$($store.name)', not '$ExpectedName'." }
    Invoke-RestMethod -Method Delete -Uri "$base/stores/$StoreId" | Out-Null
}

function Get-OpenFgaTuples {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreId
    )

    $base = $ApiUrl.TrimEnd('/')
    $token = $null
    $tuples = @()
    do {
        $body = @{ page_size = 100 }
        if ($token) { $body.continuation_token = $token }
        $response = Invoke-RestMethod -Method Post -Uri "$base/stores/$StoreId/read" -ContentType "application/json" -Body ($body | ConvertTo-Json -Compress)
        $tuples += @($response.tuples)
        $token = $response.continuation_token
    } while ($token)
    return $tuples
}

function Remove-AllOpenFgaTuples {
    param(
        [Parameter(Mandatory)][string]$ApiUrl,
        [Parameter(Mandatory)][string]$StoreId
    )

    $base = $ApiUrl.TrimEnd('/')
    $tuples = @(Get-OpenFgaTuples -ApiUrl $base -StoreId $StoreId)
    for ($offset = 0; $offset -lt $tuples.Count; $offset += 100) {
        $last = [Math]::Min($offset + 99, $tuples.Count - 1)
        $keys = @($tuples[$offset..$last] | ForEach-Object {
            @{ user = $_.key.user; relation = $_.key.relation; object = $_.key.object }
        })
        Invoke-RestMethod -Method Post -Uri "$base/stores/$StoreId/write" -ContentType "application/json" -Body (@{ deletes = @{ tuple_keys = $keys } } | ConvertTo-Json -Depth 8 -Compress) | Out-Null
    }
    return $tuples.Count
}

function Invoke-ComposeQdrantRequest {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][ValidateSet("GET", "PUT", "POST", "DELETE")][string]$Method,
        [Parameter(Mandatory)][string]$Path,
        [string]$Body = ""
    )

    if ($Path -notmatch '^/[a-zA-Z0-9_/?=&.-]+$') { throw "Unsafe Qdrant request path: $Path" }
    if ($Body.Contains("'")) { throw "Qdrant request body contains an unsupported quote." }
    $template = @'
body='__BODY__'
exec 3<>/dev/tcp/127.0.0.1/6333
printf '%s\r\n' '__METHOD__ __PATH__ HTTP/1.1' 'Host: localhost' 'Content-Type: application/json' "Content-Length: ${#body}" 'Connection: close' '' "$body" >&3
cat <&3
'@
    $command = $template.Replace('__BODY__', $Body).Replace('__METHOD__', $Method).Replace('__PATH__', $Path)
    Push-Location $Root
    try {
        $output = @(& docker compose exec -T qdrant bash -lc $command 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "Qdrant request transport failed: $($output -join [Environment]::NewLine)" }
    }
    finally { Pop-Location }

    $raw = $output -join "`n"
    if ($raw -notmatch '^HTTP/1\.1\s+(\d{3})') { throw "Qdrant returned an invalid HTTP response." }
    $status = [int]$matches[1]
    $parts = [regex]::Split($raw, "\r?\n\r?\n", 2)
    $responseBody = if ($parts.Count -eq 2) { $parts[1].Trim() } else { "" }
    if ($status -lt 200 -or $status -ge 300) { throw "Qdrant request failed with HTTP $status." }
    if (-not $responseBody) { return $null }
    return $responseBody | ConvertFrom-Json -Depth 100
}

function Get-ComposeQdrantCollections {
    param([Parameter(Mandatory)][string]$Root)

    $response = Invoke-ComposeQdrantRequest -Root $Root -Method GET -Path "/collections"
    return @($response.result.collections | ForEach-Object { $_.name } | Sort-Object -Unique)
}

function Remove-ComposeQdrantCollection {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$CollectionName,
        [switch]$AllowSharedResetCollection
    )

    $safeTestName = $CollectionName -match '^gmrag_test_[a-z0-9_]+$'
    $safeResetName = $AllowSharedResetCollection -and $CollectionName -in @("gmrag_document_chunks", "gmrag_document_chunks_test")
    if (-not $safeTestName -and -not $safeResetName) { throw "Refusing unsafe Qdrant collection delete: $CollectionName" }
    $existing = Get-ComposeQdrantCollections -Root $Root
    if ($existing -contains $CollectionName) {
        Invoke-ComposeQdrantRequest -Root $Root -Method DELETE -Path "/collections/$CollectionName" | Out-Null
    }
}

function Get-ComposeQdrantPointCount {
    param(
        [Parameter(Mandatory)][string]$Root,
        [Parameter(Mandatory)][string]$CollectionName
    )

    if ($CollectionName -notmatch '^[a-zA-Z0-9_-]+$') { throw "Unsafe Qdrant collection name: $CollectionName" }
    $response = Invoke-ComposeQdrantRequest -Root $Root -Method GET -Path "/collections/$CollectionName"
    return [int64]$response.result.points_count
}

function New-TestQdrantContainer {
    param(
        [Parameter(Mandatory)][string]$ContainerName,
        [Parameter(Mandatory)][string]$CollectionName
    )

    if ($ContainerName -notmatch '^gmrag-test-qdrant-[a-z0-9-]+$') { throw "Refusing unsafe Qdrant container name: $ContainerName" }
    if ($CollectionName -notmatch '^gmrag_test_[a-z0-9_]+$') { throw "Refusing unsafe Qdrant collection name: $CollectionName" }
    $containerId = (& docker run -d --name $ContainerName -p "127.0.0.1::6333" qdrant/qdrant:v1.18.2).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $containerId) { throw "Could not start isolated Qdrant container." }
    try {
        $portOutput = (& docker port $ContainerName 6333/tcp).Trim()
        if ($LASTEXITCODE -ne 0 -or $portOutput -notmatch ':(\d+)$') { throw "Could not resolve isolated Qdrant port." }
        $url = "http://127.0.0.1:$($matches[1])"
        $ready = $false
        for ($attempt = 0; $attempt -lt 60; $attempt++) {
            try { Invoke-RestMethod -Method Get -Uri "$url/collections" -TimeoutSec 2 | Out-Null; $ready = $true; break } catch { Start-Sleep -Milliseconds 500 }
        }
        if (-not $ready) { throw "Isolated Qdrant did not become ready." }
        [int]$vectorSize = 1024
        if ($env:QDRANT_VECTOR_SIZE -and -not [int]::TryParse($env:QDRANT_VECTOR_SIZE, [ref]$vectorSize)) {
            throw "QDRANT_VECTOR_SIZE must be a positive integer."
        }
        if ($vectorSize -le 0) { throw "QDRANT_VECTOR_SIZE must be a positive integer." }
        $collectionBody = @{ vectors = @{ size = $vectorSize; distance = "Cosine" }; on_disk_payload = $true } | ConvertTo-Json -Depth 5 -Compress
        Invoke-RestMethod -Method Put -Uri "$url/collections/$CollectionName" -ContentType "application/json" -Body $collectionBody | Out-Null
        foreach ($field in @("workspace_id", "document_id")) {
            $indexBody = @{ field_name = $field; field_schema = "keyword" } | ConvertTo-Json -Compress
            Invoke-RestMethod -Method Put -Uri "$url/collections/$CollectionName/index?wait=true" -ContentType "application/json" -Body $indexBody | Out-Null
        }
        return [pscustomobject]@{ ContainerName = $ContainerName; CollectionName = $CollectionName; Url = $url }
    }
    catch {
        & docker rm -f $ContainerName 2>&1 | Out-Null
        throw
    }
}

function Remove-TestQdrantContainer {
    param([Parameter(Mandatory)][string]$ContainerName)

    if ($ContainerName -notmatch '^gmrag-test-qdrant-[a-z0-9-]+$') { throw "Refusing unsafe Qdrant container name: $ContainerName" }
    $existing = @(& docker ps -a --filter "name=^/$ContainerName`$" --format "{{.Names}}")
    if ($existing -contains $ContainerName) {
        & docker rm -f $ContainerName 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Could not remove isolated Qdrant container $ContainerName." }
    }
}

function Get-QdrantPointCount {
    param(
        [Parameter(Mandatory)][string]$Url,
        [Parameter(Mandatory)][string]$CollectionName
    )

    $response = Invoke-RestMethod -Method Get -Uri "$($Url.TrimEnd('/'))/collections/$CollectionName"
    return [int64]$response.result.points_count
}
