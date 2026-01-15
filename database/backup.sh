#!/bin/bash

# LicenseIQ Database Backup Script
# Creates timestamped backups of the database

set -e

echo "💾 LicenseIQ Database Backup"
echo "============================"
echo ""

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "❌ Error: DATABASE_URL environment variable is not set"
    exit 1
fi

# Create timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="database/backups"

echo "📊 Creating database backups..."
echo ""

# Create full backup
echo "1/3 Creating full backup..."
pg_dump $DATABASE_URL > "${BACKUP_DIR}/full_backup.sql"
pg_dump $DATABASE_URL > "${BACKUP_DIR}/full_backup_${TIMESTAMP}.sql"
echo "   ✅ Full backup created"

# Create schema-only backup
echo "2/3 Creating schema backup..."
pg_dump $DATABASE_URL --schema-only > "${BACKUP_DIR}/schema_backup.sql"
echo "   ✅ Schema backup created"

# Create data-only backup
echo "3/3 Creating data backup..."
pg_dump $DATABASE_URL --data-only --inserts > "${BACKUP_DIR}/data_backup.sql"
echo "   ✅ Data backup created"

echo ""
echo "📦 Backup files created:"
ls -lh ${BACKUP_DIR}/*.sql | grep -v "_${TIMESTAMP}" | tail -3

echo ""
echo "🎉 Database backup completed successfully!"
echo ""
echo "Backup location: ${BACKUP_DIR}/"
echo "Latest backups: full_backup.sql, schema_backup.sql, data_backup.sql"
echo "Timestamped backup: full_backup_${TIMESTAMP}.sql"
echo ""
