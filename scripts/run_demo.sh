#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up --build -d
docker compose exec -T metadata python -m scripts.demo wait-nodes
upload_output=$(docker compose run --rm -T client python -m client.cli upload sample.bin)
printf '%s\n' "$upload_output"
file_id=$(printf '%s\n' "$upload_output" | sed -n 's/^Uploaded\. file_id: //p')
if [ -z "$file_id" ]; then
    echo "Could not read uploaded file ID" >&2
    exit 1
fi

docker compose exec -T metadata python -m scripts.demo placement "$file_id"
docker compose stop node2
docker compose exec -T metadata python -m scripts.demo wait-recovery "$file_id"
docker compose exec -T metadata python -m scripts.demo placement "$file_id"
docker compose run --rm -T --no-deps client python -m client.cli download "$file_id" sample_out.bin

source_hash=$(sha256sum sample.bin | awk '{print $1}')
download_hash=$(sha256sum sample_out.bin | awk '{print $1}')
printf 'original SHA-256: %s\ndownload SHA-256: %s\n' "$source_hash" "$download_hash"
test "$source_hash" = "$download_hash"