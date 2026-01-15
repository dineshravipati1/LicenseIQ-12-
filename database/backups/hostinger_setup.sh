#!/bin/bash

################################################################################
# LicenseIQ Database Setup Script for Hostinger VPS
################################################################################
#
# This script sets up PostgreSQL with pgvector extension on Hostinger VPS
# and prepares it for LicenseIQ backup restoration
#
# Usage:
#   1. Upload this script to your Hostinger VPS
#   2. Make it executable: chmod +x hostinger_setup.sh
#   3. Run as root: sudo ./hostinger_setup.sh
#
################################################################################

set -e  # Exit on any error

echo "=================================="
echo "LicenseIQ PostgreSQL Setup"
echo "Hostinger VPS Configuration"
echo "=================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
  echo "❌ Please run as root (use: sudo ./hostinger_setup.sh)"
  exit 1
fi

echo "📦 Step 1: Installing PostgreSQL 14..."
echo "--------------------------------------"
apt update -y
apt install -y postgresql-14 postgresql-contrib-14 postgresql-14-pgvector

echo ""
echo "✅ PostgreSQL 14 installed successfully!"
echo ""

echo "🔧 Step 2: Configuring PostgreSQL..."
echo "--------------------------------------"

# Start PostgreSQL service
systemctl start postgresql
systemctl enable postgresql

echo ""
echo "✅ PostgreSQL service started and enabled!"
echo ""

echo "🗄️  Step 3: Creating LicenseIQ database..."
echo "--------------------------------------"

# Create database and enable extensions
sudo -u postgres psql <<EOF
-- Create database
CREATE DATABASE licenseiq;

-- Connect to database
\c licenseiq

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;

-- Verify extensions
\dx

-- Create database info
SELECT 'Database: licenseiq created successfully' as status;
SELECT version() as postgresql_version;
SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';
EOF

echo ""
echo "✅ Database created and pgvector enabled!"
echo ""

echo "🔐 Step 4: Creating database user..."
echo "--------------------------------------"

# Prompt for password
read -s -p "Enter password for 'licenseiq' database user: " DB_PASSWORD
echo ""
read -s -p "Confirm password: " DB_PASSWORD_CONFIRM
echo ""

if [ "$DB_PASSWORD" != "$DB_PASSWORD_CONFIRM" ]; then
    echo "❌ Passwords don't match!"
    exit 1
fi

# Create user
sudo -u postgres psql <<EOF
CREATE USER licenseiq WITH PASSWORD '$DB_PASSWORD';
GRANT ALL PRIVILEGES ON DATABASE licenseiq TO licenseiq;
\c licenseiq
GRANT ALL ON SCHEMA public TO licenseiq;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO licenseiq;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO licenseiq;
EOF

echo ""
echo "✅ Database user 'licenseiq' created!"
echo ""

echo "🌐 Step 5: Configuring remote access..."
echo "--------------------------------------"

# Backup original config files
cp /etc/postgresql/14/main/postgresql.conf /etc/postgresql/14/main/postgresql.conf.backup
cp /etc/postgresql/14/main/pg_hba.conf /etc/postgresql/14/main/pg_hba.conf.backup

# Configure PostgreSQL to listen on all interfaces
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/14/main/postgresql.conf

# Allow password authentication from any IP (you can restrict this later)
echo "host    all             all             0.0.0.0/0               md5" >> /etc/postgresql/14/main/pg_hba.conf

# Restart PostgreSQL
systemctl restart postgresql

echo ""
echo "✅ PostgreSQL configured for remote access!"
echo ""

echo "🔥 Step 6: Configuring firewall..."
echo "--------------------------------------"

# Install and configure UFW if not already installed
if ! command -v ufw &> /dev/null; then
    apt install -y ufw
fi

# Allow PostgreSQL port
ufw allow 5432/tcp
ufw allow 22/tcp  # Ensure SSH remains allowed
ufw --force enable

echo ""
echo "✅ Firewall configured (Port 5432 opened)!"
echo ""

echo "=================================="
echo "✅ Setup Complete!"
echo "=================================="
echo ""
echo "📋 Connection Details:"
echo "--------------------------------------"
echo "Database:  licenseiq"
echo "User:      licenseiq"
echo "Password:  [your password]"
echo "Host:      $(hostname -I | awk '{print $1}')"
echo "Port:      5432"
echo ""
echo "📝 Connection String:"
echo "--------------------------------------"
echo "postgresql://licenseiq:YOUR_PASSWORD@$(hostname -I | awk '{print $1}'):5432/licenseiq"
echo ""
echo "🔄 Next Steps:"
echo "--------------------------------------"
echo "1. Upload your backup file:"
echo "   scp licenseiq_backup_20251117_160417.sql root@$(hostname -I | awk '{print $1}'):/tmp/"
echo ""
echo "2. Restore the backup:"
echo "   sudo -u postgres psql licenseiq < /tmp/licenseiq_backup_20251117_160417.sql"
echo ""
echo "3. Verify restoration:"
echo "   sudo -u postgres psql licenseiq -c 'SELECT COUNT(*) FROM users;'"
echo ""
echo "⚠️  Security Recommendations:"
echo "--------------------------------------"
echo "1. Restrict pg_hba.conf to specific IP addresses"
echo "2. Enable SSL/TLS for PostgreSQL connections"
echo "3. Set up regular automated backups"
echo "4. Monitor database logs regularly"
echo ""
echo "=================================="
