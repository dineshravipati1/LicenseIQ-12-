#!/bin/bash

# LicenseIQ Database Backup Script for Hostinger VPS
# Automatically creates timestamped backups

# Configuration for Hostinger VPS
DB_USER="postgres"
DB_NAME="licenseiq_db"
DB_HOST="localhost"
BACKUP_DIR="/home/licenseiq-qa/htdocs/qa.licenseiq.ai/database/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/full_backup_$TIMESTAMP.sql"

echo "💾 LicenseIQ Database Backup"
echo "============================"
echo ""

# Create backup directory if it doesn't exist
mkdir -p $BACKUP_DIR

# Set password from environment variable or prompt
# For automated backups, set PGPASSWORD in environment or .env file
if [ -z "$PGPASSWORD" ]; then
    echo "⚠️  Note: Set PGPASSWORD environment variable to avoid password prompt"
    echo ""
fi

# Create backup
echo "📊 Creating database backup..."
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "Host: $DB_HOST"
echo ""

pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME > $BACKUP_FILE

# Check if backup was successful
if [ $? -eq 0 ]; then
    echo "✅ Backup created successfully!"
    echo ""
    
    # Get file size
    FILE_SIZE=$(ls -lh $BACKUP_FILE | awk '{print $5}')
    echo "📦 Backup Details:"
    echo "   File: $BACKUP_FILE"
    echo "   Size: $FILE_SIZE"
    echo ""
    
    # Delete backups older than 30 days (keep last 30 days)
    DELETED_COUNT=$(find $BACKUP_DIR -name "full_backup_*.sql" -mtime +30 -delete -print | wc -l)
    if [ $DELETED_COUNT -gt 0 ]; then
        echo "🧹 Cleaned up $DELETED_COUNT old backup(s) (kept last 30 days)"
    fi
    
    echo ""
    echo "🎉 Backup complete!"
    echo ""
    echo "Latest backups:"
    ls -lht $BACKUP_DIR/full_backup_*.sql | head -5
else
    echo "❌ Backup failed!"
    echo "   Please check database connection and credentials"
    exit 1
fi
