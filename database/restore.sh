#!/bin/bash

# LicenseIQ Database Restore Script
# This script restores the database from backup files

set -e

echo "🔄 LicenseIQ Database Restore"
echo "=============================="
echo ""

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "❌ Error: DATABASE_URL environment variable is not set"
    echo "   Please ensure you have a PostgreSQL database configured in Replit"
    exit 1
fi

echo "📊 Database URL detected"
echo ""

# Ask user which backup to restore
echo "Select backup to restore:"
echo "1) Full backup (schema + data)"
echo "2) Schema only"
echo "3) Data only"
echo ""
read -p "Enter choice (1-3): " choice

case $choice in
    1)
        echo "📦 Restoring full backup..."
        psql $DATABASE_URL < database/backups/full_backup.sql
        echo "✅ Full database restored successfully!"
        ;;
    2)
        echo "📐 Restoring schema only..."
        psql $DATABASE_URL < database/backups/schema_backup.sql
        echo "✅ Database schema restored successfully!"
        ;;
    3)
        echo "📊 Restoring data only..."
        psql $DATABASE_URL < database/backups/data_backup.sql
        echo "✅ Database data restored successfully!"
        ;;
    *)
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "🎉 Database restoration completed!"
echo ""
echo "Next steps:"
echo "1. Run 'npm run dev' to start the application"
echo "2. Login with existing user credentials from the backup"
echo ""
