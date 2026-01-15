#!/bin/bash

# LicenseIQ Database Restore Script for Hostinger VPS
# Usage: ./restore-hostinger.sh <backup_file.sql>

# Configuration for Hostinger VPS
DB_USER="postgres"
DB_NAME="licenseiq_db"
DB_HOST="localhost"

echo "🔄 LicenseIQ Database Restore"
echo "=============================="
echo ""

# Check if backup file was provided
if [ -z "$1" ]; then
    echo "❌ Error: No backup file specified"
    echo ""
    echo "Usage: ./restore-hostinger.sh <backup_file.sql>"
    echo ""
    echo "Examples:"
    echo "  ./restore-hostinger.sh backups/full_backup_20251108_182949.sql"
    echo "  ./restore-hostinger.sh backups/full_backup.sql"
    echo ""
    echo "Available backups:"
    ls -lht /home/licenseiq-qa/htdocs/qa.licenseiq.ai/database/backups/full_backup_*.sql 2>/dev/null | head -5
    exit 1
fi

BACKUP_FILE=$1

# Check if file exists
if [ ! -f "$BACKUP_FILE" ]; then
    echo "❌ Error: Backup file not found: $BACKUP_FILE"
    exit 1
fi

# Get file size for display
FILE_SIZE=$(ls -lh $BACKUP_FILE | awk '{print $5}')

echo "⚠️  WARNING: This will REPLACE ALL DATA in database '$DB_NAME'"
echo ""
echo "📦 Backup Details:"
echo "   File: $BACKUP_FILE"
echo "   Size: $FILE_SIZE"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Restore cancelled."
    exit 0
fi

echo ""
echo "🔄 Starting database restore..."
echo ""

# Step 1: Stop application
echo "Step 1/5: Stopping application..."
pm2 stop licenseiq 2>/dev/null || echo "   (Application not running via PM2)"

# Step 2: Drop and recreate database
echo "Step 2/5: Dropping and recreating database..."
sudo -u postgres psql << EOF
-- Disconnect all users from the database
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();

-- Drop the database
DROP DATABASE IF EXISTS $DB_NAME;

-- Recreate the database
CREATE DATABASE $DB_NAME;

-- Grant permissions to user
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;

-- Connect to the new database
\c $DB_NAME

-- Grant schema permissions (PostgreSQL 15+)
GRANT ALL ON SCHEMA public TO $DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;
EOF

if [ $? -ne 0 ]; then
    echo "❌ Failed to recreate database"
    exit 1
fi

echo "   ✅ Database recreated"

# Step 3: Restore the backup
echo "Step 3/5: Restoring backup..."

# Set password if available in environment
if [ -z "$PGPASSWORD" ]; then
    echo "   Note: Enter database password when prompted"
fi

psql -h $DB_HOST -U $DB_USER -d $DB_NAME < $BACKUP_FILE

if [ $? -eq 0 ]; then
    echo "   ✅ Backup restored successfully"
else
    echo "   ❌ Restore failed!"
    exit 1
fi

# Step 4: Verify restoration
echo "Step 4/5: Verifying restoration..."
RECORD_COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)

if [ ! -z "$RECORD_COUNT" ] && [ "$RECORD_COUNT" -gt 0 ]; then
    echo "   ✅ Verified: $RECORD_COUNT tables restored"
else
    echo "   ⚠️  Warning: Could not verify table count"
fi

# Step 5: Restart application
echo "Step 5/5: Restarting application..."
pm2 start licenseiq 2>/dev/null || echo "   (Start application manually with: pm2 start ecosystem.config.js)"

echo ""
echo "🎉 Database restore complete!"
echo ""
echo "📋 Next Steps:"
echo "   1. Check application: https://qa.licenseiq.ai"
echo "   2. Login and verify your data"
echo "   3. Check logs: pm2 logs licenseiq"
echo ""
