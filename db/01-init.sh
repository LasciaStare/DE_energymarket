#!/bin/sh
set -eu
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
  -v reader_password="$READER_PASSWORD" -v loader_password="$LOADER_PASSWORD" <<'SQL'
CREATE ROLE market_reader LOGIN PASSWORD :'reader_password';
CREATE ROLE market_loader LOGIN PASSWORD :'loader_password';
ALTER ROLE market_reader SET default_transaction_read_only = on;
SQL
