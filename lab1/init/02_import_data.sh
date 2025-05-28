#!/bin/bash
set -e

echo "Waiting for PostgreSQL to start..."
while ! pg_isready -U $POSTGRES_USER -d $POSTGRES_DB; do
    sleep 1
done

echo "Importing CSV files..."
for f in /data/*.csv; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        COPY mock_data_raw FROM '$f' DELIMITER ',' CSV HEADER;
EOSQL
done