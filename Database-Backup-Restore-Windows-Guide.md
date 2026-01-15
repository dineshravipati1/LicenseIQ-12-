# PostgreSQL Database Backup & Restore Guide for Windows
## Licence IQ Research Platform

### Complete guide for backing up and restoring the Licence IQ database on Windows systems

---

## 📋 **Table of Contents**
- [Prerequisites](#prerequisites)
- [Method 1: Using pgAdmin (GUI - Recommended for Windows)](#method-1-using-pgadmin-gui---recommended-for-windows)
- [Method 2: Command Line Tools](#method-2-command-line-tools)
- [Method 3: Using Windows PowerShell](#method-3-using-windows-powershell)
- [Method 4: Docker Desktop on Windows](#method-4-docker-desktop-on-windows)
- [Troubleshooting](#troubleshooting)
- [Automation Scripts](#automation-scripts)

---

## 🛠️ **Prerequisites**

### **Required Software:**
1. **PostgreSQL** installed on Windows
   - Download from: https://www.postgresql.org/download/windows/
   - Recommended: PostgreSQL 14+ with pgAdmin included

2. **pgAdmin** (usually included with PostgreSQL)
   - Standalone download: https://www.pgadmin.org/download/pgadmin-4-windows/

3. **Database backup file**: `database_backup.sql`

### **System Requirements:**
- Windows 10/11
- Administrator privileges (for PostgreSQL installation)
- Minimum 2GB free disk space
- Internet connection (for initial setup)

---

## 🎯 **Method 1: Using pgAdmin (GUI - Recommended for Windows)**

### **Step 1: Open pgAdmin**
1. **Launch pgAdmin** from Start Menu or Desktop
2. **Enter Master Password** (set during installation)
3. **Connect to PostgreSQL Server**
   - Right-click "Servers" → "Create" → "Server"
   - **Name:** Local PostgreSQL
   - **Host:** localhost or 127.0.0.1
   - **Port:** 5432 (default)
   - **Username:** postgres
   - **Password:** [your postgres password]

### **Step 2: Create Target Database**
1. **Right-click "Databases"** → "Create" → "Database..."
2. **Database Name:** `licence_iq_local`
3. **Owner:** postgres
4. **Click "Save"**

### **Step 3: Restore from Backup File**
1. **Right-click** the new database `licence_iq_local`
2. **Select "Restore..."**
3. **Configure Restore Settings:**
   - **Format:** Plain (for .sql files)
   - **Filename:** Browse and select `database_backup.sql`
   - **Role name:** postgres
   - **Options:**
     - ☑️ Clean before restore
     - ☑️ Create the database
     - ☑️ Only data
     - ☑️ Only schema (uncheck if you want both)

4. **Click "Restore"**
5. **Monitor Progress** in the background processes panel
6. **Check Messages** for any errors

### **Step 4: Verify Restoration**
1. **Refresh Database** (F5 or right-click → Refresh)
2. **Expand Tables** under `licence_iq_local`
3. **Expected Tables:**
   - `users` (9 records)
   - `contracts` (9 records) 
   - `contract_analysis` (9 records)
   - `audit_trail` (1,091 records)
   - `financial_analysis` (1 record)
   - And 8 additional tables

4. **Verify Data:**
   - Right-click any table → "View/Edit Data" → "All Rows"
   - Check that data is populated correctly

---

## 💻 **Method 2: Command Line Tools**

### **Step 1: Open Command Prompt as Administrator**
```cmd
# Press Win + R, type "cmd", press Ctrl+Shift+Enter
```

### **Step 2: Navigate to PostgreSQL bin Directory**
```cmd
# Default installation path (adjust version number as needed)
cd "C:\Program Files\PostgreSQL\15\bin"

# Or add to PATH permanently via System Environment Variables
```

### **Step 3: Create Database**
```cmd
# Connect and create database
psql -U postgres -c "CREATE DATABASE licence_iq_local;"
```

### **Step 4: Restore Backup**
```cmd
# Method A: Direct restore
psql -U postgres -d licence_iq_local -f "C:\path\to\database_backup.sql"

# Method B: Using input redirection
psql -U postgres licence_iq_local < "C:\path\to\database_backup.sql"

# Method C: With connection details
psql -h localhost -U postgres -d licence_iq_local -f "C:\path\to\database_backup.sql"
```

### **Step 5: Verify Installation**
```cmd
# Connect to database
psql -U postgres -d licence_iq_local

# List tables
\dt

# Check record counts
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM contracts;

# Exit
\q
```

---

## 🔵 **Method 3: Using Windows PowerShell**

### **Step 1: Open PowerShell as Administrator**
```powershell
# Press Win + X, select "Windows PowerShell (Admin)"
```

### **Step 2: Set Execution Policy (if needed)**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### **Step 3: Create and Run Restore Script**
```powershell
# Create restoration script
$pgPath = "C:\Program Files\PostgreSQL\15\bin"
$backupFile = "C:\path\to\database_backup.sql"
$dbName = "licence_iq_local"

# Add PostgreSQL to PATH for this session
$env:PATH += ";$pgPath"

# Create database
& psql -U postgres -c "DROP DATABASE IF EXISTS $dbName;"
& psql -U postgres -c "CREATE DATABASE $dbName;"

# Restore backup
& psql -U postgres -d $dbName -f $backupFile

Write-Host "✅ Database restoration completed!" -ForegroundColor Green
```

### **Step 4: Save as PowerShell Script**
```powershell
# Save the above as restore-database.ps1
# Run with: .\restore-database.ps1
```

---

## 🐳 **Method 4: Docker Desktop on Windows**

### **Step 1: Install Docker Desktop**
1. Download from: https://www.docker.com/products/docker-desktop/
2. Install and restart Windows
3. Ensure Docker is running

### **Step 2: Run PostgreSQL Container**
```cmd
# Pull and run PostgreSQL
docker run --name postgres-local -e POSTGRES_PASSWORD=yourpassword -p 5432:5432 -d postgres:15

# Wait for container to start (30 seconds)
timeout /t 30
```

### **Step 3: Copy Backup File to Container**
```cmd
# Copy backup file
docker cp database_backup.sql postgres-local:/tmp/database_backup.sql
```

### **Step 4: Restore Database**
```cmd
# Create database and restore
docker exec postgres-local psql -U postgres -c "CREATE DATABASE licence_iq_local;"
docker exec postgres-local psql -U postgres -d licence_iq_local -f /tmp/database_backup.sql
```

### **Step 5: Connect via pgAdmin**
- **Host:** localhost
- **Port:** 5432
- **Username:** postgres
- **Password:** yourpassword

---

## 🚨 **Troubleshooting**

### **Common Issues and Solutions:**

#### **1. "psql is not recognized as internal or external command"**
**Solution:**
```cmd
# Add PostgreSQL to PATH
set PATH=%PATH%;"C:\Program Files\PostgreSQL\15\bin"

# Or use full path
"C:\Program Files\PostgreSQL\15\bin\psql" -U postgres
```

#### **2. "Permission denied" or "Access denied"**
**Solutions:**
- Run Command Prompt as Administrator
- Check PostgreSQL service is running:
```cmd
# Check service status
sc query postgresql-x64-15

# Start service if stopped
net start postgresql-x64-15
```

#### **3. "Database already exists" errors**
**Solution:**
```cmd
# Drop and recreate database
psql -U postgres -c "DROP DATABASE IF EXISTS licence_iq_local;"
psql -U postgres -c "CREATE DATABASE licence_iq_local;"
```

#### **4. "Connection refused" or "Server not found"**
**Solutions:**
- Verify PostgreSQL is installed and running
- Check Windows Firewall settings
- Verify connection parameters:
```cmd
# Test connection
psql -h localhost -U postgres -l
```

#### **5. pgAdmin cannot connect to server**
**Solutions:**
- Reset pgAdmin configuration:
  - Delete: `%APPDATA%\pgAdmin`
  - Restart pgAdmin
- Check PostgreSQL authentication in `pg_hba.conf`

#### **6. Backup file path issues**
**Solutions:**
```cmd
# Use quotes for paths with spaces
psql -U postgres -d licence_iq_local -f "C:\My Documents\database_backup.sql"

# Use forward slashes
psql -U postgres -d licence_iq_local -f "C:/path/to/database_backup.sql"

# Copy to simple path
copy database_backup.sql C:\temp\
psql -U postgres -d licence_iq_local -f "C:\temp\database_backup.sql"
```

---

## 🤖 **Automation Scripts**

### **Batch Script for Easy Restoration**

Create `restore-database.bat`:
```batch
@echo off
echo 🚀 Licence IQ Database Restoration Script
echo ==========================================

set PGPATH=C:\Program Files\PostgreSQL\15\bin
set BACKUP_FILE=%~dp0database_backup.sql
set DB_NAME=licence_iq_local

echo 📍 PostgreSQL Path: %PGPATH%
echo 📍 Backup File: %BACKUP_FILE%
echo 📍 Database Name: %DB_NAME%

if not exist "%BACKUP_FILE%" (
    echo ❌ Error: Backup file not found!
    echo    Please ensure database_backup.sql is in the same folder as this script.
    pause
    exit /b 1
)

echo.
echo 🗃️ Creating database...
"%PGPATH%\psql" -U postgres -c "DROP DATABASE IF EXISTS %DB_NAME%;"
"%PGPATH%\psql" -U postgres -c "CREATE DATABASE %DB_NAME%;"

echo.
echo 📦 Restoring backup...
"%PGPATH%\psql" -U postgres -d %DB_NAME% -f "%BACKUP_FILE%"

if %errorlevel% equ 0 (
    echo.
    echo ✅ Database restoration completed successfully!
    echo.
    echo 🔍 Verifying installation...
    "%PGPATH%\psql" -U postgres -d %DB_NAME% -c "SELECT COUNT(*) as total_users FROM users;"
    "%PGPATH%\psql" -U postgres -d %DB_NAME% -c "SELECT COUNT(*) as total_contracts FROM contracts;"
    echo.
    echo 🎉 Setup complete! Your Licence IQ database is ready.
    echo.
    echo 🔗 Connection Details:
    echo    Host: localhost
    echo    Port: 5432
    echo    Database: %DB_NAME%
    echo    Username: postgres
) else (
    echo ❌ Error occurred during restoration!
    echo Please check the error messages above.
)

echo.
pause
```

### **PowerShell Script with Error Handling**

Create `Restore-LicenceIQDatabase.ps1`:
```powershell
param(
    [string]$BackupFile = "database_backup.sql",
    [string]$DatabaseName = "licence_iq_local",
    [string]$PostgreSQLPath = "C:\Program Files\PostgreSQL\15\bin"
)

function Write-ColoredOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

Write-ColoredOutput "🚀 Licence IQ Database Restoration Script" "Cyan"
Write-ColoredOutput "=========================================" "Cyan"

# Validate prerequisites
if (!(Test-Path $PostgreSQLPath)) {
    Write-ColoredOutput "❌ PostgreSQL not found at: $PostgreSQLPath" "Red"
    Write-ColoredOutput "Please update the PostgreSQLPath parameter or install PostgreSQL." "Yellow"
    exit 1
}

if (!(Test-Path $BackupFile)) {
    Write-ColoredOutput "❌ Backup file not found: $BackupFile" "Red"
    Write-ColoredOutput "Please ensure the backup file exists in the current directory." "Yellow"
    exit 1
}

# Add PostgreSQL to PATH
$env:PATH += ";$PostgreSQLPath"

try {
    Write-ColoredOutput "🗃️ Creating database: $DatabaseName" "Yellow"
    
    # Drop existing database
    $dropResult = & psql -U postgres -c "DROP DATABASE IF EXISTS $DatabaseName;" 2>&1
    
    # Create new database
    $createResult = & psql -U postgres -c "CREATE DATABASE $DatabaseName;" 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create database: $createResult"
    }

    Write-ColoredOutput "📦 Restoring backup file..." "Yellow"
    
    # Restore backup
    $restoreResult = & psql -U postgres -d $DatabaseName -f $BackupFile 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to restore backup: $restoreResult"
    }

    Write-ColoredOutput "🔍 Verifying restoration..." "Yellow"
    
    # Verify data
    $userCount = & psql -U postgres -d $DatabaseName -t -c "SELECT COUNT(*) FROM users;" 2>&1
    $contractCount = & psql -U postgres -d $DatabaseName -t -c "SELECT COUNT(*) FROM contracts;" 2>&1
    
    Write-ColoredOutput "✅ Database restoration completed successfully!" "Green"
    Write-ColoredOutput "" "White"
    Write-ColoredOutput "📊 Verification Results:" "Cyan"
    Write-ColoredOutput "   Users: $($userCount.Trim())" "White"
    Write-ColoredOutput "   Contracts: $($contractCount.Trim())" "White"
    Write-ColoredOutput "" "White"
    Write-ColoredOutput "🔗 Connection Details:" "Cyan"
    Write-ColoredOutput "   Host: localhost" "White"
    Write-ColoredOutput "   Port: 5432" "White"
    Write-ColoredOutput "   Database: $DatabaseName" "White"
    Write-ColoredOutput "   Username: postgres" "White"
    Write-ColoredOutput "" "White"
    Write-ColoredOutput "🎉 Your Licence IQ database is ready for local development!" "Green"

} catch {
    Write-ColoredOutput "❌ Error during restoration:" "Red"
    Write-ColoredOutput $_.Exception.Message "Red"
    exit 1
}
```

---

## 📝 **Quick Reference Commands**

### **Essential Commands:**
```cmd
# Check PostgreSQL service status
sc query postgresql-x64-15

# Start PostgreSQL service
net start postgresql-x64-15

# Connect to PostgreSQL
psql -U postgres

# List databases
\l

# Connect to specific database
\c licence_iq_local

# List tables
\dt

# Exit psql
\q
```

### **Environment Setup:**
```cmd
# Add PostgreSQL to PATH (temporary)
set PATH=%PATH%;"C:\Program Files\PostgreSQL\15\bin"

# Set PGUSER environment variable
set PGUSER=postgres

# Set PGDATABASE environment variable
set PGDATABASE=licence_iq_local
```

---

## 🎯 **Best Practices**

1. **Always backup before major changes**
2. **Use descriptive database names** (e.g., `licence_iq_local`, `licence_iq_test`)
3. **Test restoration in development** before production
4. **Keep backups in version control** (if size permits)
5. **Document any custom configurations**
6. **Use Windows Task Scheduler** for automated backups
7. **Verify restoration** with sample queries
8. **Monitor disk space** during restoration

---

## 📞 **Support Information**

### **PostgreSQL Resources:**
- **Documentation:** https://www.postgresql.org/docs/
- **Windows Installation:** https://www.postgresql.org/download/windows/
- **pgAdmin Documentation:** https://www.pgadmin.org/docs/

### **Licence IQ Support:**
- **Database Schema:** See `shared/schema.ts` in the project
- **Environment Variables:** Check `.env.example`
- **Application Configuration:** Refer to project documentation

---

*This guide covers comprehensive database backup and restoration procedures for the Licence IQ Research Platform on Windows systems. Keep this document updated as the database schema evolves.*