# LicenseIQ - Hostinger VPS Deployment Guide

Complete step-by-step guide to deploy LicenseIQ application on Hostinger VPS with Ubuntu, PostgreSQL, Node.js, PM2, and Nginx.

---

## 🆕 **Latest Updates - November 2025**

**Critical Production Updates:**
- ✅ **Enhanced Product Matching** - Keyword-based semantic matching for License Fee Calculator
- ✅ **Favicon Update** - LicenseIQ logo symbol added as favicon
- ✅ **Fresh Database Backup** - `database_backup_production_20251112_172901.sql` (613KB)
- ✅ **Admin User Setup** - SQL script for admin@licenseiq.ai with password: admin123

**Files Changed:**
- `server/routes.ts` - Enhanced product matching algorithm (lines 1773-1811)
- `client/public/favicon.png` - LicenseIQ logo symbol
- `client/public/apple-touch-icon.png` - iOS touch icon
- `client/index.html` - Favicon meta tags

**Database Backup Location:** `database_backup_production_20251112_172901.sql`

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Purchase & Setup Hostinger VPS](#2-purchase--setup-hostinger-vps)
3. [Initial Server Configuration](#3-initial-server-configuration)
4. [Install Required Software](#4-install-required-software)
5. [Setup PostgreSQL Database](#5-setup-postgresql-database)
6. [Deploy Application Code](#6-deploy-application-code)
7. [Configure Environment Variables](#7-configure-environment-variables)
8. [Build Application](#8-build-application)
9. [Setup PM2 Process Manager](#9-setup-pm2-process-manager)
10. [Configure Nginx Reverse Proxy](#10-configure-nginx-reverse-proxy)
11. [Configure Domain & DNS](#11-configure-domain--dns)
12. [Install SSL Certificate](#12-install-ssl-certificate)
13. [Configure Firewall](#13-configure-firewall)
14. [Testing & Verification](#14-testing--verification)
15. [Update & Maintenance](#15-update--maintenance)
16. [Troubleshooting](#16-troubleshooting)

---

## 1. Prerequisites

Before starting, ensure you have:

- **Hostinger Account** with payment method
- **Domain Name** (optional but recommended)
- **Basic SSH knowledge**
- **Git repository** of your LicenseIQ application
- **Required API Keys**: OPENAI_API_KEY, GROQ_API_KEY, HUGGINGFACE_API_KEY

**Estimated Time**: 1-2 hours  
**Estimated Cost**: $4.99-$19.99/month (depending on VPS plan)

---

## 2. Purchase & Setup Hostinger VPS

### Step 2.1: Purchase VPS

1. Go to [Hostinger VPS Hosting](https://www.hostinger.com/vps-hosting)
2. Select a VPS plan:
   - **KVM 1** ($4.99/mo): 1 vCPU, 4GB RAM, 50GB NVMe - Good for testing
   - **KVM 2** ($8.99/mo): 2 vCPU, 8GB RAM, 100GB NVMe - **Recommended for production**
   - **KVM 4** ($12.99/mo): 4 vCPU, 16GB RAM, 200GB NVMe - For high traffic
3. Click **"Add to Cart"** → **"Checkout"**
4. Complete payment

### Step 2.2: Configure VPS

1. Login to **hPanel** (Hostinger control panel)
2. Navigate to **"VPS"** section
3. Click **"Setup"** on your new VPS
4. Configure:
   - **Location**: Choose closest to your users (US, EU, Asia)
   - **Operating System**: **Ubuntu 22.04** or **24.04 64-bit**
   - **Hostname**: `licenseiq-prod` (or your preference)
   - **Root Password**: Create a **strong password** (save it securely!)
5. Click **"Finish Setup"**
6. Wait 2-5 minutes for VPS to be ready

### Step 2.3: Get VPS Access Details

After setup completes, note down:
- **IP Address**: e.g., `123.45.67.89`
- **Root Username**: `root`
- **Root Password**: Your chosen password
- **SSH Port**: `22` (default)

---

## 3. Initial Server Configuration

### Step 3.1: Connect to VPS via SSH

**Option A: Using Hostinger Browser Terminal**
1. In hPanel → **VPS** → Click **"Browser Terminal"**
2. Enter root password

**Option B: Using Local Terminal/PuTTY**
```bash
ssh root@your_vps_ip
# Enter password when prompted
```

### Step 3.2: Update System

```bash
# Update package lists and upgrade installed packages
sudo apt update && sudo apt upgrade -y

# Install essential utilities
sudo apt install -y curl wget git vim ufw build-essential
```

### Step 3.3: Create Non-Root User (Security Best Practice)

```bash
# Create new user
adduser licenseiq
# Enter password and user details when prompted

# Add user to sudo group
usermod -aG sudo licenseiq

# Switch to new user
su - licenseiq
```

### Step 3.4: Configure SSH (Optional but Recommended)

```bash
# Exit to root user
exit

# Edit SSH config
sudo nano /etc/ssh/sshd_config

# Make these changes:
# PermitRootLogin no          # Disable root SSH login
# PasswordAuthentication yes  # Or use SSH keys for better security

# Restart SSH
sudo systemctl restart sshd
```

**⚠️ Test new user login before logging out as root!**

---

## 4. Install Required Software

### Step 4.1: Install Node.js 20.x (LTS)

```bash
# Add NodeSource repository
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -

# Install Node.js and npm
sudo apt install -y nodejs

# Verify installation
node --version    # Should show v20.x.x
npm --version     # Should show 10.x.x
```

### Step 4.2: Install PostgreSQL 14+

```bash
# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Verify installation
sudo systemctl status postgresql
# Should show "active (running)"
```

### Step 4.3: Install PM2 Process Manager

```bash
# Install PM2 globally
sudo npm install -g pm2

# Verify installation
pm2 --version
```

### Step 4.4: Install Nginx Web Server

```bash
# Install Nginx
sudo apt install -y nginx

# Start Nginx
sudo systemctl start nginx
sudo systemctl enable nginx

# Verify installation
sudo systemctl status nginx
nginx -v    # Should show nginx version
```

---

## 5. Setup PostgreSQL Database

### Step 5.1: Create Database and User

```bash
# Switch to postgres user
sudo -i -u postgres

# Open PostgreSQL prompt
psql

# Execute these SQL commands:
CREATE DATABASE licenseiq_db;
CREATE USER licenseiq_user WITH ENCRYPTED PASSWORD 'YourSecurePassword123!';
ALTER ROLE licenseiq_user SET client_encoding TO 'utf8';
ALTER ROLE licenseiq_user SET default_transaction_isolation TO 'read committed';
ALTER ROLE licenseiq_user SET timezone TO 'UTC';
GRANT ALL PRIVILEGES ON DATABASE licenseiq_db TO licenseiq_user;

# Grant schema permissions (PostgreSQL 15+)
\c licenseiq_db
GRANT ALL ON SCHEMA public TO licenseiq_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO licenseiq_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO licenseiq_user;

# Exit psql
\q

# Exit postgres user
exit
```

### Step 5.2: Enable pgvector Extension

LicenseIQ uses vector embeddings for semantic search.

```bash
# Install PostgreSQL development files
sudo apt install -y postgresql-server-dev-14

# Clone and install pgvector
cd /tmp
git clone https://github.com/pgvector/pgvector.git
cd pgvector
sudo make
sudo make install

# Connect to database and enable extension
sudo -u postgres psql -d licenseiq_db -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Verify installation
sudo -u postgres psql -d licenseiq_db -c "\dx"
# Should show "vector" in the list
```

### Step 5.3: Configure Remote Access (if needed)

**⚠️ Only if you need to connect from external tools (pgAdmin, etc.)**

```bash
# Edit PostgreSQL config
sudo nano /etc/postgresql/14/main/postgresql.conf

# Find and change:
listen_addresses = '*'

# Edit client authentication
sudo nano /etc/postgresql/14/main/pg_hba.conf

# Add at the end:
host    all             all             0.0.0.0/0               md5

# Restart PostgreSQL
sudo systemctl restart postgresql
```

---

## 6. Deploy Application Code

### Step 6.1: Create Application Directory

```bash
# Create directory
sudo mkdir -p /var/www/licenseiq
sudo chown -R $USER:$USER /var/www/licenseiq
cd /var/www/licenseiq
```

### Step 6.2: Clone Repository

**Option A: From GitHub (Public Repo)**
```bash
git clone https://github.com/yourusername/licenseiq.git .
```

**Option B: From GitHub (Private Repo)**
```bash
# Generate SSH key on server
ssh-keygen -t ed25519 -C "your_email@example.com"
# Press Enter for all prompts

# Display public key
cat ~/.ssh/id_ed25519.pub

# Copy the output and add to GitHub:
# GitHub → Settings → SSH and GPG keys → New SSH key

# Clone repo
git clone git@github.com:yourusername/licenseiq.git .
```

**Option C: Upload via SCP (from local machine)**
```bash
# Run this from your LOCAL machine:
scp -r /path/to/licenseiq licenseiq@your_vps_ip:/var/www/licenseiq
```

### Step 6.3: Install Dependencies

```bash
cd /var/www/licenseiq

# Install all dependencies
npm install

# This installs both client and server dependencies
```

---

## 7. Configure Environment Variables

### Step 7.1: Create .env File

```bash
cd /var/www/licenseiq
nano .env
```

### Step 7.2: Add Environment Variables

Copy and paste the following, **replacing with your actual values**:

```env
# Database Configuration
DATABASE_URL=postgresql://licenseiq_user:YourSecurePassword123!@localhost:5432/licenseiq_db
PGHOST=localhost
PGPORT=5432
PGDATABASE=licenseiq_db
PGUSER=licenseiq_user
PGPASSWORD=YourSecurePassword123!

# Server Configuration
NODE_ENV=production
PORT=5000

# Session Secret (generate random string)
SESSION_SECRET=your-super-secret-session-key-change-this-to-random-string

# AI Service API Keys
OPENAI_API_KEY=sk-your-openai-api-key-here
GROQ_API_KEY=gsk_your-groq-api-key-here
HUGGINGFACE_API_KEY=hf_your-huggingface-api-key-here

# Application URL (update after domain setup)
APP_URL=http://your-vps-ip:5000
# Later change to: https://yourdomain.com
```

**Save and exit**: Press `Ctrl+X`, then `Y`, then `Enter`

### Step 7.3: Secure .env File

```bash
# Restrict permissions (only owner can read/write)
chmod 600 .env

# Verify it's not tracked by git
echo ".env" >> .gitignore
```

### Step 7.4: ⚠️ **CRITICAL: Update Database Configuration Files**

Since you're using **standalone PostgreSQL** (not Neon serverless), you must modify these files:

#### 📝 **File 1: `server/db.ts`**

**Current (Development - Neon Serverless):**
```typescript
import { Pool, neonConfig } from '@neondatabase/serverless';
import { drizzle } from 'drizzle-orm/neon-serverless';
import ws from "ws";
import * as schema from "@shared/schema";

neonConfig.webSocketConstructor = ws;

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });
export const db = drizzle({ client: pool, schema });
```

**Change to (Production - Standalone PostgreSQL):**
```typescript
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import * as schema from "@shared/schema";

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

export const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  ssl: false // Set to true if using SSL
});
export const db = drizzle(pool, { schema });
```

**Key Changes:**
1. ✅ Import `Pool` from `'pg'` instead of `'@neondatabase/serverless'`
2. ✅ Import `drizzle` from `'drizzle-orm/node-postgres'` instead of `'drizzle-orm/neon-serverless'`
3. ✅ Remove `neonConfig` and `ws` imports
4. ✅ Remove `neonConfig.webSocketConstructor = ws;`
5. ✅ Use `drizzle(pool, { schema })` instead of `drizzle({ client: pool, schema })`
6. ✅ Add `ssl: false` option for local PostgreSQL

#### 📝 **File 2: `server/index.ts`**

**No changes required** - This file already uses standard Drizzle ORM syntax that works with both Neon and standalone PostgreSQL.

The database initialization code at lines 40-69 is compatible with both:
```typescript
await db.execute(sql`CREATE EXTENSION IF NOT EXISTS vector;`);
// This works with both Neon and standalone PostgreSQL
```

#### 📦 **Update package.json Dependencies**

The `pg` library should already be installed. Verify with:
```bash
cd /var/www/licenseiq
npm list pg
```

If not installed:
```bash
npm install pg
npm install --save-dev @types/node
```

#### 🔄 **Summary of Changes**

| File | What to Change | Why |
|------|---------------|-----|
| `server/db.ts` | Replace Neon imports with `pg` | Standalone PostgreSQL uses standard pg library |
| `server/db.ts` | Change drizzle import path | Different adapter for node-postgres |
| `server/db.ts` | Update Pool and drizzle() calls | Different syntax for standard pg |
| `server/index.ts` | ✅ No changes needed | Already compatible |

---

## 8. Build Application

### Step 8.1: Run Database Migrations

```bash
cd /var/www/licenseiq

# Push schema to database
npm run db:push

# If you get a data-loss warning:
npm run db:push --force
```

You should see output like:
```
✓ pgvector extension enabled
✓ Database schema synced successfully
```

### Step 8.2: Build Frontend

```bash
# Build the React frontend
npm run build

# This creates optimized production files
# The build output is already configured in the app
```

---

## 9. Setup PM2 Process Manager

### Step 9.1: Create PM2 Ecosystem File

```bash
cd /var/www/licenseiq
nano ecosystem.config.js
```

Add the following configuration:

```javascript
module.exports = {
  apps: [{
    name: 'licenseiq',
    script: 'npm',
    args: 'start',
    cwd: '/var/www/licenseiq',
    instances: 2,  // Use 2 instances for load balancing
    exec_mode: 'cluster',
    watch: false,  // Don't watch in production
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production',
      PORT: 5000
    },
    error_file: '/var/www/licenseiq/logs/pm2-error.log',
    out_file: '/var/www/licenseiq/logs/pm2-out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true
  }]
};
```

### Step 9.2: Create Logs Directory

```bash
mkdir -p /var/www/licenseiq/logs
```

### Step 9.3: Start Application with PM2

```bash
cd /var/www/licenseiq

# Start the application
pm2 start ecosystem.config.js

# Save PM2 process list
pm2 save

# Setup PM2 to auto-start on system reboot
pm2 startup systemd -u $USER --hp $HOME
# Copy and run the command it outputs (starts with 'sudo env...')

# Verify application is running
pm2 status
pm2 logs licenseiq
```

You should see output showing:
```
┌─────┬─────────────┬─────────┬─────────┬─────────┬──────────┐
│ id  │ name        │ mode    │ ↺       │ status  │ cpu      │
├─────┼─────────────┼─────────┼─────────┼─────────┼──────────┤
│ 0   │ licenseiq   │ cluster │ 0       │ online  │ 0%       │
│ 1   │ licenseiq   │ cluster │ 0       │ online  │ 0%       │
└─────┴─────────────┴─────────┴─────────┴─────────┴──────────┘
```

### Step 9.4: Useful PM2 Commands

```bash
pm2 list                    # List all processes
pm2 logs licenseiq          # View logs
pm2 logs licenseiq --lines 100  # View last 100 lines
pm2 restart licenseiq       # Restart app
pm2 stop licenseiq          # Stop app
pm2 delete licenseiq        # Remove app from PM2
pm2 monit                   # Monitor resources in real-time
pm2 flush                   # Clear logs
```

---

## 10. Configure Nginx Reverse Proxy

### Step 10.1: Create Nginx Configuration

```bash
sudo nano /etc/nginx/sites-available/licenseiq
```

### Step 10.2: Add Configuration (HTTP Only - SSL in next section)

```nginx
server {
    listen 80;
    listen [::]:80;
    
    # Replace with your domain or VPS IP
    server_name your-domain.com www.your-domain.com;
    # Or use: server_name 123.45.67.89;
    
    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    
    # Proxy to Node.js application
    location / {
        proxy_pass http://localhost:5000;
        proxy_http_version 1.1;
        
        # WebSocket support
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        
        # Standard proxy headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeout settings
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
        
        # Buffer settings
        proxy_buffering off;
        proxy_cache_bypass $http_upgrade;
    }
    
    # Logs
    access_log /var/log/nginx/licenseiq_access.log;
    error_log /var/log/nginx/licenseiq_error.log;
}
```

### Step 10.3: Enable Site

```bash
# Create symbolic link to enable site
sudo ln -s /etc/nginx/sites-available/licenseiq /etc/nginx/sites-enabled/

# Remove default nginx site (optional)
sudo rm /etc/nginx/sites-enabled/default

# Test nginx configuration
sudo nginx -t

# If test passes, restart nginx
sudo systemctl restart nginx

# Enable nginx to start on boot
sudo systemctl enable nginx
```

---

## 11. Configure Domain & DNS

### Step 11.1: Point Domain to VPS (if using domain)

**In Hostinger hPanel:**

1. Navigate to **"Domains"** → Select your domain
2. Click **"DNS / Name Servers"**
3. Click **"Manage"**

**Add/Update A Records:**

| Type | Name | Points to        | TTL       |
|------|------|------------------|-----------|
| A    | @    | your_vps_ip      | Automatic |
| A    | www  | your_vps_ip      | Automatic |

4. Click **"Add Record"** or **"Save"**
5. Wait 5-30 minutes for DNS propagation

### Step 11.2: Verify DNS Propagation

```bash
# From your VPS or local machine
dig your-domain.com
dig www.your-domain.com

# Or use online tool:
# https://www.whatsmydns.net
```

Should show your VPS IP address.

---

## 12. Install SSL Certificate

### Step 12.1: Install Certbot

```bash
# Install Certbot and Nginx plugin
sudo apt install -y certbot python3-certbot-nginx
```

### Step 12.2: Obtain SSL Certificate

**⚠️ Ensure DNS is fully propagated before running this!**

```bash
# Obtain and install certificate
sudo certbot --nginx -d your-domain.com -d www.your-domain.com

# Follow the prompts:
# 1. Enter email address
# 2. Agree to Terms of Service (Y)
# 3. Choose whether to redirect HTTP to HTTPS (recommended: Yes)
```

Certbot will automatically:
- Generate SSL certificates
- Update your Nginx configuration
- Configure HTTPS
- Setup HTTP to HTTPS redirect

### Step 12.3: Verify SSL Installation

```bash
# List certificates
sudo certbot certificates

# Test renewal (dry run)
sudo certbot renew --dry-run
```

Visit `https://your-domain.com` in a browser - you should see a padlock icon!

### Step 12.4: Auto-Renewal Setup

Certbot installs a systemd timer for automatic renewal:

```bash
# Check timer status
sudo systemctl status certbot.timer

# Enable timer (should already be enabled)
sudo systemctl enable certbot.timer
```

Certificates will automatically renew every 90 days.

---

## 13. Configure Firewall

### Step 13.1: Setup UFW Firewall

```bash
# Allow SSH (IMPORTANT: Do this first!)
sudo ufw allow OpenSSH

# Allow HTTP and HTTPS
sudo ufw allow 'Nginx Full'

# Enable firewall
sudo ufw enable

# Verify status
sudo ufw status verbose
```

Should show:
```
Status: active

To                         Action      From
--                         ------      ----
OpenSSH                    ALLOW       Anywhere
Nginx Full                 ALLOW       Anywhere
```

### Step 13.2: Additional Security (Optional)

```bash
# Rate limiting for SSH (prevent brute force)
sudo ufw limit OpenSSH

# Allow specific IP only for SSH (replace with your IP)
# sudo ufw allow from your_ip_address to any port 22
```

---

## 14. Testing & Verification

### Step 14.1: Check All Services

```bash
# Check PostgreSQL
sudo systemctl status postgresql

# Check PM2
pm2 status
pm2 logs licenseiq --lines 50

# Check Nginx
sudo systemctl status nginx
sudo nginx -t

# Check if port 5000 is listening
sudo netstat -tulpn | grep :5000

# Test application directly
curl http://localhost:5000
```

### Step 14.2: Access Application

**Without SSL:**
```
http://your-domain.com
or
http://your_vps_ip
```

**With SSL:**
```
https://your-domain.com
```

### Step 14.3: Create Admin User

1. Visit your application URL
2. Register a new account
3. Update user role to 'owner' in database:

```bash
# Connect to database
sudo -u postgres psql -d licenseiq_db

# List users
SELECT id, username, email, role FROM users;

# Update role to owner
UPDATE users SET role = 'owner' WHERE email = 'your@email.com';

# Verify
SELECT id, username, email, role FROM users;

# Exit
\q
```

4. Logout and login again to see all features

### Step 14.4: Seed Initial Data

1. Login as owner
2. Navigate to **Data Management**
3. Click **"Seed All 25 Standard Entities"** button
4. Click **"Seed Standard Fields"** button
5. Verify entities and fields are created

---

## 15. Update & Maintenance

### Step 15.1: Deploy Updates

```bash
# Navigate to app directory
cd /var/www/licenseiq

# Pull latest code
git pull origin main

# Install any new dependencies
npm install

# Run database migrations (if schema changed)
npm run db:push

# Rebuild application
npm run build

# Restart PM2
pm2 restart licenseiq

# Reload Nginx (if config changed)
sudo nginx -t && sudo systemctl reload nginx
```

### Step 15.2: Database Backup

```bash
# Create backup script
nano ~/backup-db.sh
```

Add:
```bash
#!/bin/bash
BACKUP_DIR="/var/backups/licenseiq"
DATE=$(date +%Y%m%d_%H%M%S)
mkdir -p $BACKUP_DIR

# Backup database
sudo -u postgres pg_dump licenseiq_db | gzip > $BACKUP_DIR/licenseiq_db_$DATE.sql.gz

# Keep only last 7 days of backups
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: licenseiq_db_$DATE.sql.gz"
```

Make executable and add to crontab:
```bash
chmod +x ~/backup-db.sh

# Add to crontab (daily at 2 AM)
crontab -e
# Add this line:
0 2 * * * /home/licenseiq/backup-db.sh >> /var/log/backup.log 2>&1
```

### Step 15.3: Restore Database Backup

**📦 Latest Production Backup Available:**
- File: `database_backup_production_20251112_172901.sql`
- Size: 613KB
- Date: November 12, 2025
- Includes: All demo contracts, rules, sales data, and system knowledge base

**Option A: Restore from Development Backup (Fresh Setup)**

```bash
# Upload the backup file to server (run from LOCAL machine)
scp database_backup_production_20251112_172901.sql licenseiq@your_vps_ip:/tmp/

# SSH into server
ssh licenseiq@your_vps_ip

# Restore database
sudo -u postgres psql licenseiq_db < /tmp/database_backup_production_20251112_172901.sql

# Clean up
rm /tmp/database_backup_production_20251112_172901.sql

# Restart application
pm2 restart licenseiq
```

**Option B: Restore from Server Backup (Regular Maintenance)**

```bash
# List backups
ls -lh /var/backups/licenseiq/

# Restore from gzipped backup
gunzip < /var/backups/licenseiq/licenseiq_db_YYYYMMDD_HHMMSS.sql.gz | sudo -u postgres psql licenseiq_db

# Restart application
pm2 restart licenseiq
```

### Step 15.4: Monitor Logs

```bash
# PM2 logs
pm2 logs licenseiq --lines 100

# Nginx access logs
sudo tail -f /var/log/nginx/licenseiq_access.log

# Nginx error logs
sudo tail -f /var/log/nginx/licenseiq_error.log

# System logs
journalctl -u nginx -f
```

### Step 15.5: Monitor Resources

```bash
# Real-time monitoring
pm2 monit

# Disk usage
df -h

# Memory usage
free -h

# CPU and process info
htop
# Install if not available: sudo apt install htop
```

---

## 16. Troubleshooting

### Issue: 502 Bad Gateway

**Cause**: Node.js application not running or wrong port

**Solution**:
```bash
# Check if app is running
pm2 status

# Check logs
pm2 logs licenseiq

# Restart app
pm2 restart licenseiq

# Verify port
sudo netstat -tulpn | grep :5000
```

### Issue: Database Connection Error

**Cause**: Wrong credentials or PostgreSQL not running

**Solution**:
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Verify DATABASE_URL in .env file
cat .env | grep DATABASE_URL

# Test connection
sudo -u postgres psql -d licenseiq_db -c "SELECT version();"

# Check logs
sudo tail -f /var/log/postgresql/postgresql-14-main.log
```

### Issue: PM2 Not Starting After Reboot

**Cause**: PM2 startup not configured

**Solution**:
```bash
# Remove old startup script
pm2 unstartup

# Create new startup script
pm2 startup systemd -u $USER --hp $HOME
# Run the command it outputs

# Save PM2 list
pm2 save

# Reboot and test
sudo reboot
# After reboot:
pm2 list
```

### Issue: SSL Certificate Error

**Cause**: Certificate expired or DNS issues

**Solution**:
```bash
# Check certificate status
sudo certbot certificates

# Force renewal
sudo certbot renew --force-renewal

# Check DNS
dig your-domain.com
```

### Issue: Out of Memory

**Cause**: Application using too much RAM

**Solution**:
```bash
# Check memory usage
free -h
pm2 monit

# Reduce PM2 instances
pm2 scale licenseiq 1

# Add swap space
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

### Issue: Nginx Configuration Error

**Cause**: Syntax error in nginx config

**Solution**:
```bash
# Test configuration
sudo nginx -t

# Check error logs
sudo tail -f /var/log/nginx/error.log

# Restart nginx
sudo systemctl restart nginx
```

### Issue: Port 5000 Already in Use

**Cause**: Another process using port 5000

**Solution**:
```bash
# Find process using port
sudo lsof -i :5000

# Kill process
sudo kill -9 <PID>

# Or change port in .env and ecosystem.config.js
```

---

## 🎉 Deployment Complete!

Your LicenseIQ application is now live on Hostinger VPS!

### Quick Reference Commands

```bash
# Application Management
pm2 list                    # List all processes
pm2 restart licenseiq       # Restart app
pm2 logs licenseiq         # View logs
pm2 monit                  # Monitor resources

# Database
sudo -u postgres psql -d licenseiq_db  # Connect to DB

# Nginx
sudo nginx -t              # Test config
sudo systemctl restart nginx  # Restart nginx
sudo tail -f /var/log/nginx/licenseiq_error.log  # View logs

# System
sudo ufw status            # Check firewall
df -h                      # Disk space
free -h                    # Memory usage
htop                       # Process monitor
```

### Issue: Database Configuration Error - "Cannot find module '@neondatabase/serverless'"

**Cause**: Forgot to change `server/db.ts` from Neon to standalone PostgreSQL

**Solution**:
```bash
# Edit server/db.ts
nano /var/www/licenseiq/server/db.ts

# Replace the imports and configuration as shown in Step 7.4
# Then rebuild and restart
npm run build
pm2 restart licenseiq
```

**Quick Fix - Replace entire file:**
```typescript
// /var/www/licenseiq/server/db.ts
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import * as schema from "@shared/schema";

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

export const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  ssl: false
});
export const db = drizzle(pool, { schema });
```

### Issue: "Module not found: Error: Can't resolve 'drizzle-orm/neon-serverless'"

**Cause**: Same as above - Neon imports still present

**Solution**: See Step 7.4 in this guide for complete database configuration changes.

---

### Next Steps

1. ✅ Set up automated backups
2. ✅ Configure monitoring (Uptime Robot, etc.)
3. ✅ Setup error tracking (Sentry, LogRocket)
4. ✅ Configure CDN (Cloudflare) for better performance
5. ✅ Setup staging environment

### Support Resources

- **Hostinger Support**: https://support.hostinger.com
- **LicenseIQ Documentation**: Check your repository README.md
- **Community Forums**: Stack Overflow, Reddit r/webdev

---

---

## Quick Reference: Database Configuration Changes

**Must change before deploying to Hostinger VPS with standalone PostgreSQL:**

### server/db.ts Changes

| Component | Development (Neon) | Production (Standalone PostgreSQL) |
|-----------|-------------------|-------------------------------------|
| Import Pool | `from '@neondatabase/serverless'` | `from 'pg'` |
| Import drizzle | `from 'drizzle-orm/neon-serverless'` | `from 'drizzle-orm/node-postgres'` |
| WebSocket config | `neonConfig.webSocketConstructor = ws;` | ❌ Remove this |
| Pool creation | `new Pool({ connectionString: ... })` | `new Pool({ connectionString: ..., ssl: false })` |
| Drizzle init | `drizzle({ client: pool, schema })` | `drizzle(pool, { schema })` |

### server/index.ts
✅ No changes needed - already compatible with both

**See Step 7.4 for complete code examples.**

---

**Document Version**: 2.0  
**Last Updated**: November 12, 2025  
**Application**: LicenseIQ  
**Author**: LicenseIQ Development Team
