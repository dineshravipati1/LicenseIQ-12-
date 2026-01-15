#!/bin/bash

# Test database connection and backup files
# This script helps verify your database setup before restoring

echo "🔍 LicenseIQ Database Verification"
echo "==================================="
echo ""

# Check DATABASE_URL
if [ -z "$DATABASE_URL" ]; then
    echo "❌ DATABASE_URL is not set"
    echo "   Please create a PostgreSQL database in Replit (Tools → Database)"
    exit 1
else
    echo "✅ DATABASE_URL is configured"
fi

# Test database connection
echo ""
echo "Testing database connection..."
if psql $DATABASE_URL -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✅ Database connection successful"
else
    echo "❌ Cannot connect to database"
    exit 1
fi

# Check backup files
echo ""
echo "Checking backup files..."
if [ -f "database/backups/full_backup.sql" ]; then
    SIZE=$(du -h database/backups/full_backup.sql | cut -f1)
    echo "✅ full_backup.sql exists (${SIZE})"
else
    echo "❌ full_backup.sql not found"
fi

if [ -f "database/backups/schema_backup.sql" ]; then
    SIZE=$(du -h database/backups/schema_backup.sql | cut -f1)
    echo "✅ schema_backup.sql exists (${SIZE})"
else
    echo "❌ schema_backup.sql not found"
fi

if [ -f "database/backups/data_backup.sql" ]; then
    SIZE=$(du -h database/backups/data_backup.sql | cut -f1)
    echo "✅ data_backup.sql exists (${SIZE})"
else
    echo "❌ data_backup.sql not found"
fi

# Check current tables
echo ""
echo "Current database tables:"
psql $DATABASE_URL -c "\dt" 2>/dev/null || echo "No tables found (this is OK for new installations)"

echo ""
echo "✅ Verification complete!"
echo ""
echo "To restore the database, run:"
echo "  bash database/restore.sh"
echo ""
