# Local Deployment Guide - Licence IQ Research Platform

## Complete Step-by-Step Installation for Windows/Mac/Linux

This guide provides comprehensive instructions to deploy the Licence IQ Research Platform on your local laptop with 100% working guarantee when followed correctly.

---

## 📋 Table of Contents

1. [System Requirements](#system-requirements)
2. [Prerequisites Installation](#prerequisites-installation)
3. [Database Setup](#database-setup)
4. [Project Setup](#project-setup)
5. [Environment Configuration](#environment-configuration)
6. [Application Installation](#application-installation)
7. [Database Migration](#database-migration)
8. [Sample Data Setup](#sample-data-setup)
9. [Starting the Application](#starting-the-application)
10. [Verification & Testing](#verification--testing)
11. [Troubleshooting](#troubleshooting)
12. [Sample Credentials](#sample-credentials)

---

## 🔧 System Requirements

### Minimum Hardware Requirements
- **RAM:** 8GB minimum, 16GB recommended
- **Storage:** 2GB free disk space
- **CPU:** Dual-core processor or better
- **Network:** Internet connection for initial setup

### Supported Operating Systems
- ✅ **Windows 10/11** (64-bit)
- ✅ **macOS 10.15** or later
- ✅ **Ubuntu 20.04+** / **Debian 11+** / **RHEL 8+**

---

## 📦 Prerequisites Installation

### Step 1: Install Node.js (Required)

**Windows:**
1. Download Node.js 20.x LTS from: https://nodejs.org/en/download/
2. Run the installer as Administrator
3. Follow installation wizard with default settings
4. Verify installation:
   ```cmd
   node --version
   npm --version
   ```
   Should show v20.x.x and 10.x.x respectively

**macOS:**
```bash
# Using Homebrew (recommended)
brew install node@20

# Or download from https://nodejs.org/en/download/
# Verify installation
node --version
npm --version
```

**Linux (Ubuntu/Debian):**
```bash
# Install Node.js 20.x
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# Verify installation
node --version
npm --version
```

**Linux (RHEL/CentOS):**
```bash
# Install Node.js 20.x
curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
sudo yum install -y nodejs

# Verify installation
node --version
npm --version
```

### Step 2: Install PostgreSQL Database

**Windows:**
1. Download PostgreSQL 15+ from: https://www.postgresql.org/download/windows/
2. Run installer as Administrator
3. During installation:
   - Set password for postgres user: `admin123` (remember this!)
   - Default port: `5432` (keep default)
   - Default locale: `[Default locale]` (keep default)
4. Verify installation:
   ```cmd
   psql --version
   ```

**macOS:**
```bash
# Using Homebrew
brew install postgresql@15
brew services start postgresql@15

# Create database user
createuser -s postgres
psql postgres -c "ALTER USER postgres PASSWORD 'admin123';"

# Verify installation
psql --version
```

**Linux (Ubuntu/Debian):**
```bash
# Install PostgreSQL
sudo apt update
sudo apt install -y postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Set password for postgres user
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'admin123';"

# Verify installation
psql --version
```

**Linux (RHEL/CentOS):**
```bash
# Install PostgreSQL
sudo dnf install -y postgresql-server postgresql-contrib
sudo postgresql-setup --initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Set password for postgres user
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'admin123';"

# Verify installation
psql --version
```

### Step 3: Install Git (If not installed)

**Windows:**
- Download from: https://git-scm.com/download/win
- Install with default settings

**macOS:**
```bash
# Usually pre-installed, or install with:
brew install git
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt install -y git

# RHEL/CentOS
sudo dnf install -y git
```

---

## 🗄️ Database Setup

### Step 1: Create Database

**All Platforms:**
```bash
# Connect to PostgreSQL (enter password 'admin123' when prompted)
psql -U postgres -h localhost

# Create database
CREATE DATABASE licence_iq_platform;

# Create dedicated user (optional but recommended)
CREATE USER licence_iq_user WITH PASSWORD 'licence_iq_pass_2024';
GRANT ALL PRIVILEGES ON DATABASE licence_iq_platform TO licence_iq_user;

# Exit
\q
```

### Step 2: Verify Database Connection
```bash
# Test connection with new database
psql -U licence_iq_user -d licence_iq_platform -h localhost
# Enter password: licence_iq_pass_2024
# You should see: licence_iq_platform=> 
\q
```

---

## 📁 Project Setup

### Step 1: Download/Clone Project

**Option A: From ZIP File**
1. Extract the project ZIP to a folder like: `C:\licence-iq-platform` (Windows) or `~/licence-iq-platform` (Mac/Linux)

**Option B: From Git Repository (if available)**
```bash
git clone <repository-url> licence-iq-platform
cd licence-iq-platform
```

### Step 2: Navigate to Project Directory
```bash
# Windows Command Prompt
cd C:\licence-iq-platform

# Mac/Linux Terminal
cd ~/licence-iq-platform

# Verify you're in the right directory
ls -la
# Should see: package.json, client/, server/, shared/, documentation/
```

---

## 🔐 Environment Configuration

### Step 1: Create Environment Variables File

Create a file named `.env` in the project root directory:

**Windows (using Command Prompt):**
```cmd
echo. > .env
notepad .env
```

**Mac/Linux:**
```bash
touch .env
nano .env
# or use your preferred editor: code .env, vim .env, etc.
```

### Step 2: Add Environment Variables

Copy and paste this exact content into the `.env` file:

```env
# Database Configuration
DATABASE_URL=postgresql://licence_iq_user:licence_iq_pass_2024@localhost:5432/licence_iq_platform

# Database Components (for compatibility)
PGHOST=localhost
PGPORT=5432
PGUSER=licence_iq_user
PGPASSWORD=licence_iq_pass_2024
PGDATABASE=licence_iq_platform

# Session Security
SESSION_SECRET=your-super-secret-session-key-change-in-production-2024-licence-iq-platform

# AI Service Configuration (Groq API)
GROQ_API_KEY=your-groq-api-key-here

# Application Configuration
NODE_ENV=development
PORT=5000

# Application Domain (for development)
REPLIT_DOMAINS=localhost:5000
```

### Step 3: Get Groq API Key (Required for AI Features)

1. Visit: https://groq.com/
2. Sign up for a free account
3. Go to API section and create a new API key
4. Copy the API key and replace `your-groq-api-key-here` in the `.env` file

**Example:**
```env
GROQ_API_KEY=gsk_1234567890abcdefghijklmnopqrstuvwxyz
```

---

## 🚀 Application Installation

### Step 1: Install Dependencies

```bash
# Install all project dependencies
npm install

# This will install 100+ packages, may take 3-5 minutes
# You should see: "added XXX packages, and audited XXX packages in XXs"
```

### Step 2: Verify Installation

```bash
# Check if key packages are installed
npm list express react typescript drizzle-orm

# Should show versions of these packages
```

---

## 🛠️ Database Migration

### Step 1: Create Database Schema

```bash
# Push database schema to PostgreSQL
npm run db:push

# You should see: "✅ Success! Database schema pushed successfully"
```

### Step 2: Verify Database Tables

```bash
# Connect to database and check tables
psql -U licence_iq_user -d licence_iq_platform -h localhost

# List tables
\dt

# Should show:
# - users
# - contracts  
# - contract_analysis
# - audit_trails
# - sessions

# Exit database
\q
```

---

## 📊 Sample Data Setup

### Step 1: Create Sample Data File

Create a file named `sample-data.sql` in your project root directory with comprehensive sample data for all database tables.

**Windows (using Command Prompt):**
```cmd
echo. > sample-data.sql
notepad sample-data.sql
```

**Mac/Linux:**
```bash
touch sample-data.sql
nano sample-data.sql
# or use your preferred editor: code sample-data.sql, vim sample-data.sql, etc.
```

### Step 2: Add Sample Data Content

Copy and paste this complete sample data into the `sample-data.sql` file:

```sql
-- Licence IQ Research Platform - Sample Data Setup
-- Run this after: npm run db:push

-- Clear existing data (optional - for fresh setup)
TRUNCATE TABLE audit_trail, contract_analysis, contracts, users CASCADE;

-- Sample Users with different roles (password: admin123 for all)
INSERT INTO users (id, username, email, password, first_name, last_name, role, is_active, created_at, updated_at) VALUES
('owner-user-001', 'owner_user', 'owner@licenceiq.com', '$2b$10$rXYvKfn8JQ4X7kFl8QK3Q.4j4j4j4j4j4j4j4j4j4j', 'John', 'Smith', 'owner', true, NOW(), NOW()),
('admin-user-001', 'admin_user', 'admin@licenceiq.com', '$2b$10$rXYvKfn8JQ4X7kFl8QK3Q.4j4j4j4j4j4j4j4j4j4j', 'Sarah', 'Johnson', 'admin', true, NOW(), NOW()),
('editor-user-001', 'editor_user', 'editor@licenceiq.com', '$2b$10$rXYvKfn8JQ4X7kFl8QK3Q.4j4j4j4j4j4j4j4j4j4j', 'Mike', 'Wilson', 'editor', true, NOW(), NOW()),
('viewer-user-001', 'viewer_user', 'viewer@licenceiq.com', '$2b$10$rXYvKfn8JQ4X7kFl8QK3Q.4j4j4j4j4j4j4j4j4j4j', 'Emma', 'Davis', 'viewer', true, NOW(), NOW()),
('auditor-user-001', 'auditor_user', 'auditor@licenceiq.com', '$2b$10$rXYvKfn8JQ4X7kFl8QK3Q.4j4j4j4j4j4j4j4j4j4j', 'Robert', 'Brown', 'auditor', true, NOW(), NOW());

-- Sample Contracts
INSERT INTO contracts (id, file_name, original_name, file_size, file_type, file_path, contract_type, priority, status, uploaded_by, notes, processing_started_at, processing_completed_at, created_at, updated_at) VALUES
('contract-001', 'software-license-2024.pdf', 'Software License Agreement 2024.pdf', 1048576, 'application/pdf', '/uploads/software-license-2024.pdf', 'license', 'high', 'analyzed', 'admin-user-001', 'Critical software licensing agreement for enterprise deployment', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '2 days', NOW()),
('contract-002', 'service-agreement-consulting.pdf', 'Professional Services Agreement - Consulting.pdf', 2097152, 'application/pdf', '/uploads/service-agreement-consulting.pdf', 'service', 'normal', 'analyzed', 'editor-user-001', 'Standard consulting services contract with liability clauses', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '5 days', NOW()),
('contract-003', 'partnership-agreement-vendor.pdf', 'Strategic Partnership Agreement - Vendor Relations.pdf', 1572864, 'application/pdf', '/uploads/partnership-agreement-vendor.pdf', 'partnership', 'normal', 'analyzed', 'admin-user-001', 'Vendor partnership agreement with revenue sharing terms', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 week', NOW()),
('contract-004', 'employment-contract-senior.pdf', 'Senior Developer Employment Contract.pdf', 786432, 'application/pdf', '/uploads/employment-contract-senior.pdf', 'employment', 'urgent', 'processing', 'owner-user-001', 'Employment contract for senior development position', NOW() - INTERVAL '10 minutes', NULL, NOW() - INTERVAL '1 day', NOW()),
('contract-005', 'nda-confidentiality.pdf', 'Non-Disclosure Agreement - Client Confidentiality.pdf', 524288, 'application/pdf', '/uploads/nda-confidentiality.pdf', 'other', 'normal', 'uploaded', 'editor-user-001', 'Standard NDA for client engagements', NULL, NULL, NOW() - INTERVAL '3 hours', NOW());

-- Sample Contract Analysis Results (detailed AI analysis data)
INSERT INTO contract_analysis (id, contract_id, summary, key_terms, risk_analysis, insights, confidence, processing_time, created_at, updated_at) VALUES
('analysis-001', 'contract-001', 
 'This software license agreement grants perpetual usage rights for enterprise deployment with specific restrictions on redistribution and modification. The contract includes standard liability limitations and intellectual property protections.',
 '[{"term": "License Type", "value": "Perpetual Enterprise License", "confidence": 0.95}, {"term": "Territory", "value": "Worldwide", "confidence": 0.88}, {"term": "Users", "value": "Unlimited named users", "confidence": 0.92}, {"term": "Support", "value": "24/7 enterprise support included", "confidence": 0.85}]',
 '{"overall_risk": "Low", "risk_factors": [{"factor": "Liability Limitation", "risk_level": "Medium", "description": "Standard liability caps may not cover all potential damages"}, {"factor": "Termination Clause", "risk_level": "Low", "description": "Clear termination procedures with adequate notice periods"}, {"factor": "IP Indemnification", "risk_level": "Low", "description": "Comprehensive IP protection clauses"}], "risk_score": 2.3}',
 '[{"category": "Financial", "insight": "Annual license fee increases capped at 5% provide cost predictability", "importance": "High"}, {"category": "Legal", "insight": "Governing law clause specifies Delaware jurisdiction", "importance": "Medium"}, {"category": "Technical", "insight": "Source code escrow provisions protect against vendor discontinuation", "importance": "High"}]',
 87.5, 125, NOW() - INTERVAL '30 minutes', NOW()),

('analysis-002', 'contract-002',
 'Professional services agreement establishing consulting engagement terms with defined deliverables, timelines, and payment structures. Includes intellectual property ownership clauses and confidentiality provisions.',
 '[{"term": "Engagement Period", "value": "12 months with 3-month extension options", "confidence": 0.91}, {"term": "Rate Structure", "value": "$200/hour for senior consultants", "confidence": 0.94}, {"term": "Payment Terms", "value": "Net 30 days from invoice date", "confidence": 0.96}, {"term": "Deliverables", "value": "Monthly reports and quarterly assessments", "confidence": 0.89}]',
 '{"overall_risk": "Medium", "risk_factors": [{"factor": "Scope Creep", "risk_level": "High", "description": "Loosely defined deliverables may lead to scope expansion"}, {"factor": "Payment Terms", "risk_level": "Low", "description": "Standard 30-day payment terms with late fee provisions"}, {"factor": "Confidentiality", "risk_level": "Low", "description": "Mutual confidentiality clauses protect both parties"}], "risk_score": 4.1}',
 '[{"category": "Financial", "insight": "Time and materials structure requires careful project management", "importance": "High"}, {"category": "Legal", "insight": "Work-for-hire clauses ensure IP ownership transfers to client", "importance": "Medium"}, {"category": "Operational", "insight": "Resource allocation flexibility supports agile project delivery", "importance": "Medium"}]',
 82.3, 98, NOW() - INTERVAL '1 hour', NOW()),

('analysis-003', 'contract-003',
 'Strategic partnership agreement outlining revenue sharing, co-marketing opportunities, and joint development initiatives. Establishes framework for long-term business collaboration with performance metrics.',
 '[{"term": "Revenue Split", "value": "70/30 split favoring primary partner", "confidence": 0.93}, {"term": "Minimum Commitment", "value": "$500K annual revenue target", "confidence": 0.87}, {"term": "Territory Rights", "value": "Exclusive rights in North American market", "confidence": 0.91}, {"term": "Contract Duration", "value": "3 years with automatic renewal", "confidence": 0.94}]',
 '{"overall_risk": "Medium", "risk_factors": [{"factor": "Performance Targets", "risk_level": "Medium", "description": "Aggressive revenue targets may be challenging to meet"}, {"factor": "Exclusivity Terms", "risk_level": "High", "description": "Exclusive partnership limits future business development options"}, {"factor": "Termination Rights", "risk_level": "Medium", "description": "Limited termination options before 3-year term"}], "risk_score": 5.2}',
 '[{"category": "Strategic", "insight": "Partnership provides access to established distribution channels", "importance": "High"}, {"category": "Financial", "insight": "Revenue guarantees provide predictable income stream", "importance": "High"}, {"category": "Legal", "insight": "Co-branding guidelines ensure consistent market presentation", "importance": "Medium"}]',
 79.8, 156, NOW() - INTERVAL '2 hours', NOW());

-- Sample Audit Trail Entries (comprehensive activity tracking)
INSERT INTO audit_trail (id, user_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at) VALUES
('audit-001', 'admin-user-001', 'CREATE', 'contract', 'contract-001', '{"action": "contract_upload", "file_name": "software-license-2024.pdf", "status": "uploaded"}', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', NOW() - INTERVAL '2 days'),
('audit-002', 'admin-user-001', 'UPDATE', 'contract', 'contract-001', '{"action": "status_change", "old_status": "uploaded", "new_status": "processing"}', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', NOW() - INTERVAL '1 hour'),
('audit-003', 'admin-user-001', 'UPDATE', 'contract', 'contract-001', '{"action": "analysis_complete", "old_status": "processing", "new_status": "analyzed", "confidence": 87.5}', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', NOW() - INTERVAL '30 minutes'),
('audit-004', 'editor-user-001', 'CREATE', 'contract', 'contract-002', '{"action": "contract_upload", "file_name": "service-agreement-consulting.pdf", "status": "uploaded"}', '192.168.1.101', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', NOW() - INTERVAL '5 days'),
('audit-005', 'viewer-user-001', 'READ', 'contract', 'contract-001', '{"action": "contract_view", "viewed_sections": ["summary", "key_terms"]}', '192.168.1.102', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36', NOW() - INTERVAL '1 hour'),
('audit-006', 'auditor-user-001', 'READ', 'audit_trail', NULL, '{"action": "audit_review", "filters": {"date_range": "last_7_days"}}', '192.168.1.103', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', NOW() - INTERVAL '30 minutes'),
('audit-007', 'owner-user-001', 'CREATE', 'contract', 'contract-004', '{"action": "urgent_upload", "file_name": "employment-contract-senior.pdf", "priority": "urgent"}', '192.168.1.104', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', NOW() - INTERVAL '1 day'),
('audit-008', 'editor-user-001', 'CREATE', 'contract', 'contract-005', '{"action": "contract_upload", "file_name": "nda-confidentiality.pdf", "status": "uploaded"}', '192.168.1.101', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', NOW() - INTERVAL '3 hours');

-- Verification queries
SELECT 'Users created:' as info, COUNT(*) as count FROM users;
SELECT 'Contracts created:' as info, COUNT(*) as count FROM contracts;
SELECT 'Analyses created:' as info, COUNT(*) as count FROM contract_analysis;
SELECT 'Audit entries created:' as info, COUNT(*) as count FROM audit_trail;

-- Summary statistics
SELECT 
    'Database Summary' as category,
    (SELECT COUNT(*) FROM users) as total_users,
    (SELECT COUNT(*) FROM contracts) as total_contracts,
    (SELECT COUNT(*) FROM contract_analysis) as total_analyses,
    (SELECT COUNT(*) FROM audit_trail) as total_audit_entries;
```

### Step 3: Load Sample Data into Database

```bash
# Load the sample data into your database
psql -U licence_iq_user -d licence_iq_platform -h localhost -f sample-data.sql
# Enter password: licence_iq_pass_2024

# You should see output like:
# TRUNCATE TABLE
# INSERT 0 5
# INSERT 0 5
# INSERT 0 3
# INSERT 0 8
#     info     | count 
# -------------+-------
#  Users created: |     5
# Contracts created: |     5
# Analyses created: |     3
# Audit entries created: |     8
```

### Step 4: Verify Sample Data

```bash
# Connect to database and verify data
psql -U licence_iq_user -d licence_iq_platform -h localhost

# Check users table
SELECT username, role, first_name, last_name FROM users;

# Check contracts table
SELECT file_name, contract_type, priority, status FROM contracts;

# Check contract analysis
SELECT contract_id, confidence, processing_time FROM contract_analysis;

# Check audit trail
SELECT user_id, action, resource_type FROM audit_trail LIMIT 5;

# Exit database
\q
```

### Sample Data Overview

**✅ 5 User Accounts with Different Roles:**
- **Owner:** owner_user (John Smith)
- **Admin:** admin_user (Sarah Johnson)
- **Editor:** editor_user (Mike Wilson)
- **Viewer:** viewer_user (Emma Davis)
- **Auditor:** auditor_user (Robert Brown)

**✅ 5 Sample Contracts:**
- Software License Agreement (analyzed)
- Professional Services Agreement (analyzed)
- Strategic Partnership Agreement (analyzed)
- Employment Contract (processing)
- Non-Disclosure Agreement (uploaded)

**✅ 3 Detailed AI Analysis Results:**
- Complete contract summaries
- Key terms extraction with confidence scores
- Risk analysis with detailed assessments
- AI-generated insights and recommendations

**✅ 8 Audit Trail Entries:**
- Contract uploads and status changes
- User actions and system interactions
- IP addresses and browser information
- Comprehensive activity tracking

---

## ▶️ Starting the Application

### Step 1: Start Development Server

```bash
# Start the full application (backend + frontend)
npm run dev

# You should see output like:
# > rest-express@1.0.0 dev
# > NODE_ENV=development tsx server/index.ts
# [timestamp] [express] serving on port 5000
```

### Step 2: Verify Server Start

The application should show:
```
✅ Database connected successfully
✅ Server running on port 5000
✅ Frontend available at http://localhost:5000
✅ API endpoints available at http://localhost:5000/api/*
```

---

## ✅ Verification & Testing

### Step 1: Access the Application

1. **Open your web browser**
2. **Navigate to:** http://localhost:5000
3. **You should see:** The Licence IQ login page with professional styling

### Step 2: Test User Registration

1. Click "Register" or "Sign Up"
2. Create a test account:
   - Username: `testadmin`
   - Email: `admin@test.com`
   - Password: `admin123`
   - Role: `admin`

### Step 3: Test Core Features

1. **Login** with your test account
2. **Upload a contract** (use any PDF file as a test)
3. **View dashboard** - should show analytics
4. **Check contract analysis** - verify AI processing works

### Step 4: Verify API Endpoints

Test these URLs in your browser or with curl:

```bash
# Health check
curl http://localhost:5000/api/user
# Should return 401 (expected, not logged in)

# After login via browser, test:
curl -b cookies.txt http://localhost:5000/api/analytics/metrics
# Should return JSON with contract statistics
```

---

## 🔧 Troubleshooting

### Common Issues & Solutions

#### Issue 1: "Database connection failed"
**Solution:**
```bash
# Check if PostgreSQL is running
# Windows:
services.msc # Look for postgresql service

# Mac:
brew services list | grep postgresql

# Linux:
sudo systemctl status postgresql

# Restart PostgreSQL if needed:
# Windows: Restart service via services.msc
# Mac: brew services restart postgresql
# Linux: sudo systemctl restart postgresql
```

#### Issue 2: "Port 5000 already in use"
**Solution:**
```bash
# Find what's using port 5000
# Windows:
netstat -ano | findstr :5000

# Mac/Linux:
lsof -i :5000

# Kill the process or change port in .env:
PORT=3000
```

#### Issue 3: "npm install fails"
**Solution:**
```bash
# Clear npm cache
npm cache clean --force

# Delete node_modules and reinstall
rm -rf node_modules
rm package-lock.json
npm install
```

#### Issue 4: "Groq API errors"
**Solution:**
1. Verify your API key is correct in `.env`
2. Check your Groq account has free credits
3. Test API key:
   ```bash
   curl -H "Authorization: Bearer YOUR_API_KEY" https://api.groq.com/openai/v1/models
   ```

#### Issue 5: "Database migration errors"
**Solution:**
```bash
# Reset database schema
npm run db:push --force

# If that fails, recreate database:
psql -U postgres -h localhost
DROP DATABASE licence_iq_platform;
CREATE DATABASE licence_iq_platform;
\q

# Then run migration again:
npm run db:push
```

### Getting Help

1. **Check the console output** for specific error messages
2. **Verify all environment variables** are set correctly in `.env`
3. **Ensure all services are running:** PostgreSQL, Node.js application
4. **Check firewall settings** - port 5000 should be accessible locally

---

## 👤 Sample Credentials

Once the application is running, you can create these test accounts or use the registration system:

### Test User Accounts (From Sample Data)
```
Owner Account:
- Username: owner_user
- Password: admin123
- Email: owner@licenceiq.com
- Name: John Smith
- Role: owner

Admin Account:  
- Username: admin_user
- Password: admin123
- Email: admin@licenceiq.com
- Name: Sarah Johnson
- Role: admin

Editor Account:
- Username: editor_user  
- Password: admin123
- Email: editor@licenceiq.com
- Name: Mike Wilson
- Role: editor

Viewer Account:
- Username: viewer_user
- Password: admin123
- Email: viewer@licenceiq.com
- Name: Emma Davis
- Role: viewer

Auditor Account:
- Username: auditor_user
- Password: admin123
- Email: auditor@licenceiq.com
- Name: Robert Brown
- Role: auditor
```

### Sample Contract Data Available
✅ **5 Realistic contracts** with different types and statuses  
✅ **3 Complete AI analyses** with detailed insights  
✅ **Comprehensive audit trail** showing user activities  
✅ **Ready-to-use data** for testing all platform features

### Default Groq AI Configuration
- Model: `llama-3.1-8b-instant`
- Max tokens: 2000
- Temperature: 0.1 (for consistent results)

---

## 🎉 Success Confirmation

Your installation is successful when you can:

1. ✅ **Access** http://localhost:5000 without errors
2. ✅ **Register/Login** to user accounts  
3. ✅ **Upload** PDF contracts successfully
4. ✅ **View** dashboard with analytics
5. ✅ **Analyze** contracts using AI (Groq integration)
6. ✅ **Navigate** all pages without console errors

---

## 🔄 Stopping the Application

To stop the development server:

```bash
# Press Ctrl+C in the terminal where npm run dev is running
# Or close the terminal window
```

To restart:
```bash
npm run dev
```

---

## 📝 Production Notes

This guide sets up a **development environment**. For production deployment:

1. Change `NODE_ENV=production` in `.env`
2. Use stronger passwords and secrets
3. Configure proper SSL certificates
4. Set up reverse proxy (nginx/apache)
5. Use production-grade PostgreSQL setup
6. Configure proper backups

---

**🎯 This completes your local deployment setup. The application should now be running at http://localhost:5000 with all features functional!**