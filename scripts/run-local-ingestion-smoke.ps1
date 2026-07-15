[CmdletBinding()]
param(
    [string]$ApiBaseUrl = $env:GMRAG_SMOKE_API_BASE_URL,
    [string]$WorkspaceId = $env:GMRAG_SMOKE_WORKSPACE_ID,
    [string]$BearerToken = $env:GMRAG_SMOKE_BEARER_TOKEN,
    [string]$QdrantUrl = $env:QDRANT_URL,
    [string]$QdrantCollection = $env:QDRANT_COLLECTION,
    [string]$OllamaUrl = $env:OLLAMA_EMBED_URL,
    [string]$OllamaModel = $env:OLLAMA_EMBED_MODEL,
    [int]$TimeoutSec = 600
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) { $ApiBaseUrl = "http://127.0.0.1:8083" }
if ([string]::IsNullOrWhiteSpace($QdrantUrl)) { $QdrantUrl = "http://127.0.0.1:6333" }
if ([string]::IsNullOrWhiteSpace($QdrantCollection)) { $QdrantCollection = "gmrag_document_chunks" }
if ([string]::IsNullOrWhiteSpace($OllamaUrl)) { $OllamaUrl = "http://127.0.0.1:11434/api/embed" }
if ([string]::IsNullOrWhiteSpace($OllamaModel)) { $OllamaModel = "AITeamVN/Vietnamese_Embedding" }

$ApiBaseUrl = $ApiBaseUrl.TrimEnd("/")
$QdrantUrl = $QdrantUrl.TrimEnd("/")

if ([string]::IsNullOrWhiteSpace($WorkspaceId)) { throw "GMRAG_SMOKE_WORKSPACE_ID must be set." }
if ([string]::IsNullOrWhiteSpace($BearerToken)) { throw "GMRAG_SMOKE_BEARER_TOKEN must be set." }
$BearerToken = $BearerToken.Trim()
if ($OllamaModel -ne "AITeamVN/Vietnamese_Embedding") {
    throw "OLLAMA_EMBED_MODEL must equal AITeamVN/Vietnamese_Embedding exactly."
}
if ($TimeoutSec -lt 30) { throw "TimeoutSec must be at least 30 seconds." }

$ApiHeaders = @{ Authorization = "Bearer $BearerToken" }
$QdrantHeaders = @{}
if (-not [string]::IsNullOrWhiteSpace($env:QDRANT_API_KEY)) {
    $QdrantHeaders["api-key"] = $env:QDRANT_API_KEY
}

function Invoke-JsonRequest {
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        [hashtable]$Headers = @{},
        $Body = $null
    )

    $parameters = @{
        Method = $Method
        Uri = $Uri
        Headers = $Headers
    }
    if ($null -ne $Body) {
        $parameters.ContentType = "application/json"
        $parameters.Body = $Body | ConvertTo-Json -Depth 30 -Compress
    }
    Invoke-RestMethod @parameters
}

function New-TextLayerPdf {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Text
    )

    $safeText = $Text.Replace("\", "\\").Replace("(", "\(").Replace(")", "\)")
    $stream = "BT /F1 18 Tf 72 720 Td ($safeText) Tj ET`n"
    $objects = @(
        "1 0 obj`n<< /Type /Catalog /Pages 2 0 R >>`nendobj`n",
        "2 0 obj`n<< /Type /Pages /Kids [3 0 R] /Count 1 >>`nendobj`n",
        "3 0 obj`n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>`nendobj`n",
        "4 0 obj`n<< /Length $([Text.Encoding]::ASCII.GetByteCount($stream)) >>`nstream`n$stream" + "endstream`nendobj`n",
        "5 0 obj`n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>`nendobj`n"
    )

    $content = "%PDF-1.4`n"
    $offsets = [System.Collections.Generic.List[int]]::new()
    foreach ($object in $objects) {
        $offsets.Add([Text.Encoding]::ASCII.GetByteCount($content))
        $content += $object
    }
    $xrefOffset = [Text.Encoding]::ASCII.GetByteCount($content)
    $content += "xref`n0 6`n0000000000 65535 f `n"
    foreach ($offset in $offsets) {
        $content += $offset.ToString("0000000000") + " 00000 n `n"
    }
    $content += "trailer`n<< /Size 6 /Root 1 0 R >>`nstartxref`n$xrefOffset`n%%EOF`n"
    [IO.File]::WriteAllBytes($Path, [Text.Encoding]::ASCII.GetBytes($content))
}

function New-Docx {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Text
    )

    Add-Type -AssemblyName System.IO.Compression
    $stream = [IO.File]::Open($Path, [IO.FileMode]::Create)
    try {
        $archive = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Create, $false)
        try {
            $entries = @{
                "[Content_Types].xml" = '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>'
                "_rels/.rels" = '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>'
                "word/document.xml" = '<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>' + [Security.SecurityElement]::Escape($Text) + '</w:t></w:r></w:p></w:body></w:document>'
            }
            foreach ($name in $entries.Keys) {
                $entry = $archive.CreateEntry($name)
                $writer = [IO.StreamWriter]::new($entry.Open(), [Text.UTF8Encoding]::new($false))
                try { $writer.Write($entries[$name]) } finally { $writer.Dispose() }
            }
        }
        finally { $archive.Dispose() }
    }
    finally { $stream.Dispose() }
}

function Upload-SmokeDocument {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Marker
    )

    $response = Invoke-RestMethod -Method Post `
        -Uri "$ApiBaseUrl/workspaces/$WorkspaceId/documents/upload" `
        -Headers $ApiHeaders `
        -Form @{ file = Get-Item -LiteralPath $Path; access_mode = "workspace_default" }
    $document = @($response.documents)[0]
    if ($null -eq $document -or [string]::IsNullOrWhiteSpace($document.document_id)) {
        throw "Upload did not return a document id for $Path."
    }
    [pscustomobject]@{
        id = $document.document_id.ToString()
        filename = $document.filename
        marker = $Marker
    }
}

function Wait-ForCompletedDocuments {
    param([Parameter(Mandatory)][array]$Documents)

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSec)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $response = Invoke-JsonRequest -Method GET -Uri "$ApiBaseUrl/workspaces/$WorkspaceId/documents?limit=100&offset=0" -Headers $ApiHeaders
        $rows = @($response.documents)
        $pending = 0
        foreach ($document in $Documents) {
            $row = $rows | Where-Object { $_.id.ToString() -eq $document.id } | Select-Object -First 1
            if ($null -eq $row) {
                $pending++
                continue
            }
            if ($row.status -eq "FAILED") {
                throw "Document $($document.id) failed: $($row.failure_code) $($row.failure_message)"
            }
            if ($row.status -ne "COMPLETED" -or $row.processing_stage -ne "DONE") {
                $pending++
            }
        }
        if ($pending -eq 0) { return }
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for all smoke documents to reach COMPLETED/DONE."
}

function Get-QdrantVectorSize {
    $collection = Invoke-JsonRequest -Method GET -Uri "$QdrantUrl/collections/$QdrantCollection" -Headers $QdrantHeaders
    $vectors = $collection.result.config.params.vectors
    if ($null -ne $vectors.size) { return [int]$vectors.size }
    throw "Qdrant collection does not expose a single dense vector size."
}

function Assert-DocumentPoints {
    param([Parameter(Mandatory)]$Document)

    $body = @{
        filter = @{ must = @(@{ key = "document_id"; match = @{ value = $Document.id } }) }
        limit = 100
        with_payload = $true
        with_vector = $true
    }
    $response = Invoke-JsonRequest -Method POST -Uri "$QdrantUrl/collections/$QdrantCollection/points/scroll" -Headers $QdrantHeaders -Body $body
    $points = @($response.result.points)
    if ($points.Count -eq 0) { throw "No Qdrant points found for document $($Document.id)." }
    foreach ($point in $points) {
        if (@($point.vector).Count -ne 1024) {
            throw "Point $($point.id) has vector length $(@($point.vector).Count), expected 1024."
        }
    }
}

function Assert-RetrievalContent {
    param([Parameter(Mandatory)]$Document)

    $embeddingResponse = Invoke-JsonRequest -Method POST -Uri $OllamaUrl -Body @{
        model = $OllamaModel
        input = @($Document.marker)
    }
    $queryVector = @($embeddingResponse.embeddings)[0]
    if (@($queryVector).Count -ne 1024) {
        throw "Ollama returned $(@($queryVector).Count) dimensions, expected 1024."
    }

    $searchBody = @{
        vector = $queryVector
        limit = 5
        with_payload = $true
        with_vector = $false
        filter = @{ must = @(
            @{ key = "workspace_id"; match = @{ value = $WorkspaceId } },
            @{ key = "document_id"; match = @{ value = $Document.id } }
        ) }
    }
    $search = Invoke-JsonRequest -Method POST -Uri "$QdrantUrl/collections/$QdrantCollection/points/search" -Headers $QdrantHeaders -Body $searchBody
    $point = @($search.result)[0]
    if ($null -eq $point) { throw "Qdrant retrieval returned no point for document $($Document.id)." }

    $chunk = Invoke-JsonRequest -Method GET -Uri "$ApiBaseUrl/workspaces/$WorkspaceId/chunks/$($point.id)" -Headers $ApiHeaders
    if ($chunk.original_text -notlike "*$($Document.marker)*") {
        throw "Retrieved chunk $($point.id) does not contain marker $($Document.marker)."
    }
}

$runId = [Guid]::NewGuid().ToString("N").Substring(0, 12)
$tempDirectory = Join-Path ([IO.Path]::GetTempPath()) "gmrag-ingestion-smoke-$runId"
[IO.Directory]::CreateDirectory($tempDirectory) | Out-Null

try {
    $fixtures = @(
        @{ extension = "pdf"; marker = "GMRAG_SMOKE_PDF_$runId" },
        @{ extension = "docx"; marker = "GMRAG_SMOKE_DOCX_$runId" },
        @{ extension = "txt"; marker = "GMRAG_SMOKE_TXT_$runId" },
        @{ extension = "md"; marker = "GMRAG_SMOKE_MD_$runId" }
    )

    foreach ($fixture in $fixtures) {
        $fixture.path = Join-Path $tempDirectory "smoke-$runId.$($fixture.extension)"
        switch ($fixture.extension) {
            "pdf" { New-TextLayerPdf -Path $fixture.path -Text "$($fixture.marker) local ingestion smoke text layer with sufficient searchable content" }
            "docx" { New-Docx -Path $fixture.path -Text $fixture.marker }
            "txt" { [IO.File]::WriteAllText($fixture.path, $fixture.marker, [Text.UTF8Encoding]::new($false)) }
            "md" { [IO.File]::WriteAllText($fixture.path, "# Smoke`n`n$($fixture.marker)", [Text.UTF8Encoding]::new($false)) }
        }
    }

    $documents = @($fixtures | ForEach-Object { Upload-SmokeDocument -Path $_.path -Marker $_.marker })
    Wait-ForCompletedDocuments -Documents $documents

    $vectorSize = Get-QdrantVectorSize
    if ($vectorSize -ne 1024) { throw "Qdrant collection size is $vectorSize, expected 1024." }

    foreach ($document in $documents) {
        $preview = Invoke-JsonRequest -Method GET -Uri "$ApiBaseUrl/workspaces/$WorkspaceId/documents/$($document.id)/preview" -Headers $ApiHeaders
        if ($preview.content -notlike "*$($document.marker)*") {
            throw "Preview for $($document.id) does not contain marker $($document.marker)."
        }
        Assert-DocumentPoints -Document $document
        Assert-RetrievalContent -Document $document
    }

    Write-Host "Local ingestion smoke: PASS"
    Write-Host "workspace_id=$WorkspaceId collection=$QdrantCollection vector_size=$vectorSize"
    foreach ($document in $documents) { Write-Host "document_id=$($document.id) filename=$($document.filename)" }
}
finally {
    if (Test-Path -LiteralPath $tempDirectory) {
        Remove-Item -LiteralPath $tempDirectory -Recurse -Force
    }
}
