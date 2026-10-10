#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "$POSTGRES_DB" \
  -v app_password="$POSTGRES_APP_PASSWORD" <<-EOSQL
    CREATE ROLE outbox_app WITH LOGIN PASSWORD :'app_password';
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO outbox_app;
    GRANT USAGE, CREATE ON SCHEMA public TO outbox_app;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO outbox_app;
EOSQL