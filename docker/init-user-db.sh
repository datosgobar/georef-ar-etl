#!/bin/bash
set -e

# Variables desde .env
DB_ADMIN_USER="${POSTGRES_USER:-postgres}"
DB_ADMIN_PASS="${POSTGRES_PASSWORD:-changeme}"

DB_USER="${DB_USER:-georef}"
DB_PASS="${DB_PASSWORD:-changeme}"
DB_NAME="${DB_NAME:-georef_ar_etl}"

echo "Creando base de datos: $DB_NAME con usuario: $DB_USER"

# Crear el usuario si no existe
psql -U "$DB_ADMIN_USER" -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '$DB_USER'" | grep -q 1 || \
    psql -U "$DB_ADMIN_USER" -d postgres -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';"

# Crear la base de datos si no existe
psql -U "$DB_ADMIN_USER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    psql -U "$DB_ADMIN_USER" -d postgres -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"

# Habilitar PostGIS
psql -U "$DB_ADMIN_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Base de datos $DB_NAME creada y PostGIS habilitado."
