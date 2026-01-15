# LicenseIQ - Production Database Restore Instructions

## 📦 Latest Database Backup

**File:** `database_backup_production_20251112_172901.sql`  
**Size:** 613KB  
**Date:** November 12, 2025  
**Location:** Root directory of the project

**Includes:**
- ✅ 3 Demo Contracts (Electronics, Manufacturing, Plant Variety)
- ✅ 74 AI-extracted rules across all contracts
- ✅ Sample sales data for all contracts
- ✅ System Knowledge Base (11 documentation topics)
- ✅ LicenseIQ Schema Catalog (28 entities)
- ✅ User accounts and permissions
- ✅ Calculation history and results

---

## 🚀 Quick Restore (Hostinger Production)

### Step 1: Upload Backup to Server

**From your LOCAL machine:**
```bash
scp database_backup_production_20251112_172901.sql licenseiq@your-vps-ip:/tmp/
```

### Step 2: SSH into Server
```bash
ssh licenseiq@your-vps-ip
```

### Step 3: Restore Database
```bash
# Drop existing database (⚠️ WARNING: This deletes all existing data!)
sudo -u postgres psql -c "DROP DATABASE IF EXISTS licenseiq_db;"

# Recreate database
sudo -u postgres psql -c "CREATE DATABASE licenseiq_db;"

# Grant permissions
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE licenseiq_db TO licenseiq_user;"

# Enable pgvector extension
sudo -u postgres psql -d licenseiq_db -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Restore from backup
sudo -u postgres psql licenseiq_db < /tmp/database_backup_production_20251112_172901.sql

# Clean up backup file
rm /tmp/database_backup_production_20251112_172901.sql
```

### Step 4: Verify Restore
```bash
# Check if data was restored
sudo -u postgres psql -d licenseiq_db -c "SELECT COUNT(*) FROM contracts;"
# Should return: 3

sudo -u postgres psql -d licenseiq_db -c "SELECT COUNT(*) FROM royalty_rules;"
# Should return: 74

sudo -u postgres psql -d licenseiq_db -c "SELECT COUNT(*) FROM users;"
# Should return: 1 or more
```

### Step 5: Restart Application
```bash
cd /var/www/licenseiq
pm2 restart licenseiq
```

---

## 👤 Admin User Setup (Already Included in Backup)

The backup includes a default admin user. If you need to create/update it manually:

```sql
INSERT INTO users (id, email, password, role, name, company_id, business_unit_id, location_id, created_at, updated_at)
VALUES (
  'admin-user-001',
  'admin@licenseiq.ai',
  '$2b$10$rN3qY8XH9vZ5KqP7wL2MxeYvX5jK9Hx5tP3wQ2rS1mT4uV6wX7yZ8zA',
  'admin',
  'System Administrator',
  'default-company',
  'default-bu',
  'default-loc',
  NOW(),
  NOW()
)
ON CONFLICT (email) 
DO UPDATE SET 
  password = '$2b$10$rN3qY8XH9vZ5KqP7wL2MxeYvX5jK9Hx5tP3wQ2rS1mT4uV6wX7yZ8zA',
  role = 'admin',
  updated_at = NOW();
```

**Login Credentials:**
- Email: `admin@licenseiq.ai`
- Password: `admin123`

⚠️ **Change this password immediately after first login in production!**

---

## 🔍 Verify Production Deployment

After restoring, test these features:

### 1. Login Test
```
https://licenseiq.ai
Login with: admin@licenseiq.ai / admin123
```

### 2. Contracts Test
- Navigate to "Contracts" page
- Should see 3 contracts:
  - CNT-2025-001 - Electronics Patent License
  - CNT-2025-002 - Manufacturing License
  - CNT-2025-003 - Plant Variety License

### 3. License Fee Calculator Test
- Click on any contract
- Click "Calculate License Fees"
- Upload sales data (sample files in `sample_sales_data/` directory)
- Verify products are matching rules ✅

### 4. LIQ AI Test
- Click "LIQ AI" in navigation
- Ask: "What contract types does LicenseIQ support?"
- Should get intelligent response from system knowledge base

---

## 📝 What's Included in This Backup

### Tables Restored:
1. `users` - User accounts
2. `contracts` - Contract metadata
3. `royalty_rules` - AI-extracted payment rules
4. `sales_data` - Sales transactions
5. `royalty_calculations` - Calculation history
6. `contract_embeddings` - Vector embeddings for RAG
7. `system_knowledge_base` - LIQ AI documentation
8. `licenseiq_schema_catalog` - Standard schema definitions
9. `erp_catalog` - ERP integration mappings
10. And all other system tables

### Sample Data:
- **Electronics Patent License**: 60 rules, 20 sales items
- **Manufacturing License**: 6 rules, 20 sales items  
- **Plant Variety License**: 8 rules, 15 sales items

---

## 🛟 Troubleshooting

### Error: "role 'licenseiq_user' does not exist"
```bash
sudo -u postgres psql -c "CREATE USER licenseiq_user WITH ENCRYPTED PASSWORD 'YourSecurePassword123!';"
```

### Error: "database 'licenseiq_db' does not exist"
```bash
sudo -u postgres psql -c "CREATE DATABASE licenseiq_db;"
```

### Error: "extension 'vector' does not exist"
```bash
# Install pgvector first (see HOSTINGER_DEPLOYMENT_GUIDE.md Section 5.2)
cd /tmp
git clone https://github.com/pgvector/pgvector.git
cd pgvector
sudo make && sudo make install
sudo -u postgres psql -d licenseiq_db -c "CREATE EXTENSION vector;"
```

### Server not starting after restore
```bash
# Check PM2 logs
pm2 logs licenseiq --lines 50

# Restart with fresh start
pm2 delete licenseiq
pm2 start ecosystem.config.js
```

---

## 📚 Additional Resources

- Full Deployment Guide: `HOSTINGER_DEPLOYMENT_GUIDE.md`
- UI-Based Guide: `HOSTINGER_UI_DEPLOYMENT_GUIDE.md`
- Production Deploy Steps: `PRODUCTION_DEPLOY_STEPS.md`
- Architecture Overview: `ARCHITECTURE.md`

---

**Last Updated:** November 12, 2025  
**Backup Version:** 2025.11.12
