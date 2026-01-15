# 🚀 LicenseIQ Database Restoration Guide - Hostinger VPS

This guide provides step-by-step instructions for restoring your LicenseIQ database backup to a Hostinger VPS running local PostgreSQL.

---

## ✅ Compatibility Confirmed

Your backup file (`licenseiq_backup_20251117_160417.sql`) **will work** on Hostinger VPS!

**Requirements:**
- ✅ PostgreSQL 14 or higher
- ✅ pgvector extension (manually installed)
- ✅ 613 KB storage space (backup size)
- ✅ Standard PostgreSQL features (all included)

---

## 📋 Prerequisites

1. **Hostinger VPS with root access**
2. **Ubuntu 22.04 or 24.04** (recommended)
3. **At least 4GB RAM** (recommended for production)

---

## 🔧 Method 1: Automated Setup (Recommended)

### Step 1: Upload Setup Script

```bash
# From your local machine
scp database/backups/hostinger_setup.sh root@YOUR_VPS_IP:/root/
```

### Step 2: Run Setup Script

```bash
# SSH to VPS
ssh root@YOUR_VPS_IP

# Make executable
chmod +x /root/hostinger_setup.sh

# Run setup
sudo ./hostinger_setup.sh
```

The script will:
- ✅ Install PostgreSQL 14
- ✅ Install pgvector extension
- ✅ Create `licenseiq` database
- ✅ Create database user
- ✅ Configure firewall
- ✅ Enable remote access

### Step 3: Upload Backup File

```bash
# From your local machine
scp database/backups/licenseiq_backup_20251117_160417.sql root@YOUR_VPS_IP:/tmp/
```

### Step 4: Restore Backup

```bash
# SSH to VPS
ssh root@YOUR_VPS_IP

# Restore database
sudo -u postgres psql licenseiq < /tmp/licenseiq_backup_20251117_160417.sql

# Verify restoration
sudo -u postgres psql licenseiq -c "SELECT COUNT(*) FROM users;"
```

---

## 🛠️ Method 2: Manual Setup

### Step 1: Install PostgreSQL

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install PostgreSQL 14
sudo apt install -y postgresql-14 postgresql-contrib-14

# Start PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### Step 2: Install pgvector Extension

```bash
# Install pgvector from package
sudo apt install -y postgresql-14-pgvector

# Verify installation
dpkg -l | grep pgvector
```

### Step 3: Create Database

```bash
# Switch to postgres user
sudo -u postgres psql

# In PostgreSQL prompt:
CREATE DATABASE licenseiq;
\c licenseiq
CREATE EXTENSION IF NOT EXISTS vector;
\dx  # Verify extension installed
\q   # Exit
```

### Step 4: Create Database User

```bash
sudo -u postgres psql

# In PostgreSQL prompt:
CREATE USER licenseiq WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE licenseiq TO licenseiq;
\c licenseiq
GRANT ALL ON SCHEMA public TO licenseiq;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO licenseiq;
\q
```

### Step 5: Upload and Restore Backup

```bash
# Upload backup
scp licenseiq_backup_20251117_160417.sql root@YOUR_VPS_IP:/tmp/

# SSH to VPS
ssh root@YOUR_VPS_IP

# Restore
sudo -u postgres psql licenseiq < /tmp/licenseiq_backup_20251117_160417.sql
```

---

## 🔐 Configure Remote Access (Optional)

### Edit PostgreSQL Configuration

```bash
# Edit postgresql.conf
sudo nano /etc/postgresql/14/main/postgresql.conf

# Change:
# listen_addresses = 'localhost'
# To:
listen_addresses = '*'

# Save and exit (Ctrl+X, Y, Enter)
```

### Edit pg_hba.conf

```bash
# Edit pg_hba.conf
sudo nano /etc/postgresql/14/main/pg_hba.conf

# Add at the end:
host    all             all             0.0.0.0/0               md5

# Save and exit
```

### Configure Firewall

```bash
# Allow PostgreSQL port
sudo ufw allow 5432/tcp
sudo ufw reload

# Restart PostgreSQL
sudo systemctl restart postgresql
```

---

## ✅ Verify Installation

### Check PostgreSQL Status

```bash
sudo systemctl status postgresql
```

### Check pgvector Extension

```bash
sudo -u postgres psql licenseiq -c "\dx"
```

Expected output:
```
                                      List of installed extensions
  Name   | Version |   Schema   |                         Description
---------+---------+------------+--------------------------------------------------------------
 plpgsql | 1.0     | pg_catalog | PL/pgSQL procedural language
 vector  | 0.5.1   | public     | vector data type and ivfflat and hnsw access methods
```

### Check Tables

```bash
sudo -u postgres psql licenseiq -c "\dt"
```

Should show all 48 tables from the backup.

### Check User Count

```bash
sudo -u postgres psql licenseiq -c "SELECT COUNT(*) FROM users;"
```

---

## 🔗 Connection String

After successful restoration, use this connection string:

```
postgresql://licenseiq:YOUR_PASSWORD@YOUR_VPS_IP:5432/licenseiq
```

**For application configuration:**
```bash
export DATABASE_URL="postgresql://licenseiq:YOUR_PASSWORD@YOUR_VPS_IP:5432/licenseiq"
```

---

## 📊 What Gets Restored

Your backup includes:

### 48 Tables:
- ✅ All user data and authentication
- ✅ All contract documents and analysis
- ✅ All payment rules and calculations
- ✅ All AI embeddings and knowledge graphs
- ✅ All ERP integrations and mappings
- ✅ All sales data and imports
- ✅ All system configurations

### Extensions:
- ✅ pgvector (for AI embeddings)

### Data Size:
- ✅ 613 KB complete backup

---

## ⚠️ Troubleshooting

### Error: "extension vector does not exist"

**Solution:**
```bash
sudo apt install postgresql-14-pgvector
sudo -u postgres psql licenseiq -c "CREATE EXTENSION vector;"
```

### Error: "role licenseiq does not exist"

**Solution:**
```bash
sudo -u postgres psql -c "CREATE USER licenseiq WITH PASSWORD 'password';"
```

### Error: "database licenseiq does not exist"

**Solution:**
```bash
sudo -u postgres psql -c "CREATE DATABASE licenseiq;"
```

### Connection Refused

**Check if PostgreSQL is running:**
```bash
sudo systemctl status postgresql
sudo systemctl start postgresql
```

**Check firewall:**
```bash
sudo ufw status
sudo ufw allow 5432/tcp
```

---

## 🔒 Security Best Practices

### 1. Restrict Remote Access

Instead of `0.0.0.0/0`, use specific IPs:

```bash
# In pg_hba.conf
host    licenseiq    licenseiq    123.456.789.0/24    md5
```

### 2. Enable SSL

```bash
# Generate SSL certificate
sudo openssl req -new -x509 -days 365 -nodes -text -out /etc/postgresql/14/main/server.crt -keyout /etc/postgresql/14/main/server.key

# Set permissions
sudo chmod 600 /etc/postgresql/14/main/server.key
sudo chown postgres:postgres /etc/postgresql/14/main/server.*

# Edit postgresql.conf
ssl = on
ssl_cert_file = '/etc/postgresql/14/main/server.crt'
ssl_key_file = '/etc/postgresql/14/main/server.key'

# Restart
sudo systemctl restart postgresql
```

### 3. Regular Backups

```bash
# Create backup script
cat > /root/backup_licenseiq.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/root/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
mkdir -p $BACKUP_DIR
pg_dump -U postgres licenseiq > "$BACKUP_DIR/licenseiq_$TIMESTAMP.sql"
# Keep only last 7 backups
ls -t $BACKUP_DIR/licenseiq_*.sql | tail -n +8 | xargs rm -f
EOF

chmod +x /root/backup_licenseiq.sh

# Add to crontab (daily at 2 AM)
(crontab -l ; echo "0 2 * * * /root/backup_licenseiq.sh") | crontab -
```

---

## 📞 Need Help?

Common issues:
1. **pgvector not found**: Ensure PostgreSQL 14+ and pgvector package installed
2. **Permission denied**: Run commands with `sudo` or as `postgres` user
3. **Connection refused**: Check firewall and PostgreSQL listen_addresses

---

## ✅ Success Checklist

- [ ] PostgreSQL 14+ installed
- [ ] pgvector extension installed
- [ ] Database `licenseiq` created
- [ ] User `licenseiq` created with password
- [ ] Backup file uploaded to VPS
- [ ] Backup successfully restored
- [ ] All 48 tables present
- [ ] User data accessible
- [ ] Application can connect

---

**Your LicenseIQ database is now ready on Hostinger VPS!** 🎉
