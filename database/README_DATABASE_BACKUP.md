# LicenseIQ Database Backup Guide

## 📦 Latest Backup File

**File:** `licenseiq_backup_20251125_185452.sql`  
**Size:** 761 KB  
**Date:** November 25, 2025 at 6:54 PM  
**Format:** PostgreSQL dump with complete schema and data

**Previous Backups:**
- `licenseiq_backup_20251125_184941.sql` (767 KB) - Before category CRUD implementation

---

## 📊 What's Included

This backup contains:

✅ **Complete Database Schema**
- All 40+ tables with proper relationships
- Primary keys, foreign keys, and constraints
- Indexes for performance optimization
- pgvector extension configuration

✅ **All Data**
- User accounts (including admin user)
- Contracts and AI analysis results
- Navigation categories and preferences
- Master data mappings
- ERP and LicenseIQ schema catalogs
- Royalty calculation rules
- System configuration settings

✅ **Clean Restore**
- Includes `DROP TABLE IF EXISTS` statements
- Safe to restore over existing database
- No ownership or ACL conflicts

---

## 🔧 How to Restore the Backup

### Method 1: Restore to Development Database (Recommended)

```bash
# Navigate to project root
cd /path/to/licenseiq

# Restore using environment variables
PGPASSWORD=$PGPASSWORD psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f database/licenseiq_backup_20251125_185452.sql
```

### Method 2: Restore to Custom Database

```bash
# Replace with your database credentials
psql -h your-host -p 5432 -U your-user -d your-database -f database/licenseiq_backup_20251125_185452.sql
```

### Method 3: Restore via pgAdmin

1. Open pgAdmin
2. Right-click on your database → **Restore**
3. Select the `.sql` file
4. Choose **Plain** format
5. Click **Restore**

---

## ⚠️ Important Notes

### Before Restoring

1. **Backup Current Database** (if you have important data):
   ```bash
   pg_dump -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE > my_backup_$(date +%Y%m%d).sql
   ```

2. **Close All Connections** to the database (stop the app):
   ```bash
   # Stop the workflow or press Ctrl+C
   ```

3. **Verify pgvector Extension** is installed:
   ```sql
   CREATE EXTENSION IF NOT EXISTS vector;
   ```

### After Restoring

1. **Restart the Application**:
   ```bash
   npm run dev
   ```

2. **Verify Data**:
   - Login with admin credentials: `admin` / `Admin@123!`
   - Check contracts are visible
   - Verify navigation categories display correctly

3. **Run Migrations** (if schema changed):
   ```bash
   npm run db:push
   ```

---

## 🔑 Default Admin Credentials

After restoring this backup, use these credentials:

- **Username:** `admin`
- **Email:** `admin@licenseiq.com`
- **Password:** `Admin@123!`

⚠️ **Change the password immediately in production!**

---

## 📋 Backup Contents Summary

### Users & Authentication
- Default admin user with hashed password
- Session management tables

### Navigation System
- 6 navigation categories with intelligent grouping
- 20 navigation items properly categorized
- User-specific category preferences and state

### Contract Management
- Contract metadata and AI analysis
- Document embeddings for semantic search
- Royalty calculation rules

### Master Data
- ERP catalog entries (28 entities)
- LicenseIQ schema catalog (28 entities)
- Master data mappings

### System Configuration
- Theme preferences (12 color schemes)
- User-organization role assignments
- Audit logs and activity tracking

---

## 🚀 Quick Restore & Run

One-liner to restore and start the app:

```bash
PGPASSWORD=$PGPASSWORD psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f database/licenseiq_backup_20251125_185452.sql && npm run dev
```

---

## 📝 Creating New Backups

To create a fresh backup:

```bash
# Auto-timestamped filename
PGPASSWORD=$PGPASSWORD pg_dump -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --clean --if-exists --inserts --no-owner --no-acl > database/licenseiq_backup_$(date +%Y%m%d_%H%M%S).sql
```

**Backup Options Explained:**
- `--clean`: Add DROP statements before CREATE
- `--if-exists`: Add IF EXISTS to DROP statements (safe)
- `--inserts`: Use INSERT statements (more portable)
- `--no-owner`: Don't set ownership (avoids permission issues)
- `--no-acl`: Don't dump access privileges

---

## 🔍 Troubleshooting

### Error: "extension vector does not exist"

```sql
-- Connect to database and run:
CREATE EXTENSION IF NOT EXISTS vector;
```

### Error: "permission denied"

Make sure you have proper database credentials and the user has CREATE/DROP permissions.

### Error: "database is being accessed by other users"

Stop the application workflow before restoring.

---

## 📞 Need Help?

If you encounter issues:

1. Check the application logs: `npm run dev`
2. Verify database connection: `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "SELECT version();"`
3. Review the backup file: `head -100 database/licenseiq_backup_20251125_185452.sql`

---

**Last Updated:** November 25, 2025  
**Database Version:** PostgreSQL 15+ with pgvector extension
