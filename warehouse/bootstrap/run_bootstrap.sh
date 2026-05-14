#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-olist-postgres}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SQL_FILES=(
  "001_schema.sql"
  "002_dim_tables.sql"
  "003_fact_tables.sql"
  "004_indexes.sql"
  "005_constraints.sql"
  "006_optional_dims.sql"
  "007_seed_optional_dims.sql"
)

echo "Using container=$CONTAINER_NAME db=$DB_NAME user=$DB_USER"

for f in "${SQL_FILES[@]}"; do
  path="$SCRIPT_DIR/$f"
  if [[ ! -f "$path" ]]; then
    echo "Missing SQL file: $path" >&2
    exit 1
  fi

  echo "Applying $f ..."
  docker exec -i "$CONTAINER_NAME" psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" < "$path"
  echo "Applied $f"
  echo "-----------------------------"
done

echo "Bootstrap complete."
echo "Verifying mart tables..."
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "\\dt mart.*"
