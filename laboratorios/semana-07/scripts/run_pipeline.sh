#!/usr/bin/env bash
# Tubería completa: Extract+Load (Python) -> Transform (dbt build = run + test).
# La usan tanto Kestra como `docker compose run --rm pipeline`.
set -euo pipefail

PY="${PIPELINE_PY:-python}"
DBT="${DBT_BIN:-dbt}"

echo "==> [1/2] Ingesta NYC Yellow Taxi ${START_MONTH} .. ${END_MONTH} -> Snowflake RAW"
"$PY" /lab/ingest/ingest.py "$@"

echo "==> [2/2] dbt: seeds + bronze -> silver -> gold + tests"
cd "${DBT_PROJECT_DIR:-/lab/dbt}"
"$DBT" deps --quiet
"$DBT" build --fail-fast
echo "==> Tubería completada"
