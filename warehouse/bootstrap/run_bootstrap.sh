#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-postgres}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
INCLUDE_DEV_SQL="${INCLUDE_DEV_SQL:-0}"

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

echo "Using host=$PGHOST port=$PGPORT db=$PGDATABASE user=$PGUSER"

for f in "${SQL_FILES[@]}"; do
  path="$SCRIPT_DIR/$f"
  if [[ ! -f "$path" ]]; then
    echo "Missing SQL file: $path" >&2
    exit 1
  fi

  echo "Applying $f ..."
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$path"
  echo "Applied $f"
  echo "-----------------------------"
done

if [[ "$INCLUDE_DEV_SQL" == "1" ]]; then
  dev_path="$SCRIPT_DIR/008_dev_relax_fact_order_events_fk.sql"
  if [[ -f "$dev_path" ]]; then
    echo "Applying 008_dev_relax_fact_order_events_fk.sql ..."
    psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$dev_path"
    echo "Applied 008_dev_relax_fact_order_events_fk.sql"
    echo "-----------------------------"
  fi
fi

echo "Bootstrap complete."
echo "Verifying mart tables..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "\\dt mart.*"
