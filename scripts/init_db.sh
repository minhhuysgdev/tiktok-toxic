#!/bin/bash
# ===================================================================
# Initialize PostgreSQL Database Schema
# ===================================================================

set -e

echo "=========================================="
echo "Initializing PostgreSQL Database"
echo "=========================================="

# Database connection info
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="tiktok_toxicity"
DB_USER="tiktok_user"
DB_PASS="tiktok_pass"

# SQL script path
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_SCRIPT="$PROJECT_ROOT/sql/serving_views.sql"

echo "Database: $DB_NAME"
echo "SQL Script: $SQL_SCRIPT"
echo ""

# Check if PostgreSQL is running
if ! pg_isready -h "$DB_HOST" -p "$DB_PORT" > /dev/null 2>&1; then
    echo "ERROR: PostgreSQL is not running!"
    echo "Please start PostgreSQL first (docker-compose up -d postgres)"
    exit 1
fi

echo "✓ PostgreSQL is running"

# Execute SQL script
echo "Executing SQL script..."
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_SCRIPT"

echo ""
echo "=========================================="
echo "✓ Database initialized successfully"
echo "=========================================="

