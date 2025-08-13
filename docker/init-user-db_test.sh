#!/bin/bash
set -e

# Variables desde .env
DB_ADMIN_USER="${POSTGRES_USER:-postgres}"
DB_ADMIN_PASS="${POSTGRES_PASSWORD:-changeme}"

DB_USER_TEST="${DB_USER_TEST:-georef_test}"
DB_PASS_TEST="${DB_PASSWORD_TEST:-changeme_test}"
DB_NAME_TEST="${DB_NAME_TEST:-georef_ar_etl_test}"

echo "Creando base de datos de test: $DB_NAME_TEST con usuario: $DB_USER_TEST"

# Crear el usuario de test si no existe
psql -U "$DB_ADMIN_USER" -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '$DB_USER_TEST'" | grep -q 1 || \
    psql -U "$DB_ADMIN_USER" -d postgres -c "CREATE USER $DB_USER_TEST WITH PASSWORD '$DB_PASS_TEST';"

# Crear la base de datos de test si no existe
psql -U "$DB_ADMIN_USER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME_TEST'" | grep -q 1 || \
    psql -U "$DB_ADMIN_USER" -d postgres -c "CREATE DATABASE $DB_NAME_TEST OWNER $DB_USER_TEST;"

# Habilitar PostGIS
psql -U "$DB_ADMIN_USER" -d "$DB_NAME_TEST" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Base de datos $DB_NAME_TEST creada y PostGIS habilitado."
