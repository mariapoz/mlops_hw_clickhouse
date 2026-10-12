#!/usr/bin/env bash
# Выполняет запрос п.3 в ClickHouse и сохраняет результат в CSV.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T clickhouse clickhouse-client --format CSVWithNames \
  < sql/03_top_category_by_state.sql \
  > results/top_category_by_state.csv

echo "Saved results/top_category_by_state.csv ($(($(wc -l < results/top_category_by_state.csv) - 1)) rows)"
