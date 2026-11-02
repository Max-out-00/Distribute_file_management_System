$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

docker compose up --build -d
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose exec -T metadata python -m scripts.demo wait-nodes
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$uploadOutput = docker compose run --rm -T client python -m client.cli upload sample.bin
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$uploadOutput | Write-Output
$match = [regex]::Match(($uploadOutput -join "`n"), "Uploaded\. file_id: ([A-Za-z0-9_-]+)")
if (-not $match.Success) { throw "Could not read uploaded file ID" }
$fileId = $match.Groups[1].Value

docker compose exec -T metadata python -m scripts.demo placement $fileId
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose stop node2
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker compose exec -T metadata python -m scripts.demo wait-recovery $fileId
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose exec -T metadata python -m scripts.demo placement $fileId
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose run --rm -T --no-deps client python -m client.cli download $fileId sample_out.bin
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$sourceHash = (Get-FileHash sample.bin -Algorithm SHA256).Hash
$downloadHash = (Get-FileHash sample_out.bin -Algorithm SHA256).Hash
Write-Output "original SHA-256: $sourceHash"
Write-Output "download SHA-256: $downloadHash"
if ($sourceHash -ne $downloadHash) { throw "SHA-256 mismatch after recovery" }
