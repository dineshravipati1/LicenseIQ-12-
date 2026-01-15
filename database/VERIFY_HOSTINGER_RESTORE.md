# Verify Hostinger PostgreSQL Database Restore

## 🔍 Why You Don't See Data After Restore

The backup file **DOES contain data** (1,524 INSERT statements, 761 KB).

If you restored it but don't see data, here's how to troubleshoot:

---

## 📋 Step-by-Step Verification

### 1. Check if Restore Completed Successfully

When you ran the restore command, did you see any **errors**?

```bash
psql -h YOUR_HOSTINGER_HOST -U YOUR_USERNAME -d YOUR_DATABASE -f licenseiq_backup_20251125_185452.sql
```

**Look for:**
- ✅ `INSERT 0 1` messages (successful inserts)
- ❌ `ERROR:` messages (failed inserts)
- ❌ `relation "table_name" does not exist` (wrong database)

---

### 2. Count Tables in Your Hostinger Database

Connect to your Hostinger PostgreSQL and run:

```sql
-- Count how many tables exist
SELECT COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'public';
```

**Expected Result:** Should show **40+ tables**

If you see **0 tables** or very few tables, the restore didn't work.

---

### 3. Check Specific Tables for Data

Run these queries to check if data was loaded:

```sql
-- Check users table
SELECT COUNT(*) as user_count FROM users;

-- Check contracts table
SELECT COUNT(*) as contract_count FROM contracts;

-- Check navigation_categories table
SELECT COUNT(*) as category_count FROM navigation_categories;

-- Check audit_trail table
SELECT COUNT(*) as audit_count FROM audit_trail;
```

**Expected Results:**
- **users:** 1+ rows (at least the admin user)
- **contracts:** 3+ rows
- **navigation_categories:** 6 rows
- **audit_trail:** Many rows (activity log)

---

### 4. List All Tables

```sql
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY tablename;
```

**Expected Tables (40+):**
- audit_trail
- business_units
- companies
- compliance_analysis
- contract_analysis
- contract_approvals
- contract_comparisons
- contract_documents
- contract_embeddings
- contract_graph_edges
- contract_graph_nodes
- contract_obligations
- contract_royalty_calculations
- contract_versions
- contracts
- data_import_jobs
- erp_entities
- erp_fields
- erp_systems
- extraction_runs
- financial_analysis
- human_review_tasks
- imported_erp_records
- licenseiq_entities
- licenseiq_entity_records
- licenseiq_fields
- locations
- master_data_mappings
- navigation_categories
- navigation_item_categories
- navigation_permissions
- performance_metrics
- role_navigation_permissions
- roles
- royalty_rules
- rule_definitions
- rule_validation_events
- sales_data
- sales_field_mappings
- semantic_index_entries
- sessions
- strategic_analysis
- system_embeddings
- user_category_preferences
- user_category_state
- user_navigation_overrides
- user_organization_roles
- users

---

## 🛠️ Common Issues and Fixes

### Issue 1: "Permission Denied" Errors During Restore

**Cause:** User doesn't have CREATE TABLE permissions

**Fix:** Use a superuser account or database owner account to restore

```bash
# Use the database owner account
psql -h HOST -U owner_username -d database_name -f licenseiq_backup_20251125_185452.sql
```

---

### Issue 2: "Relation Already Exists" Errors

**Cause:** Tables already exist in database

**Fix:** The backup file uses `DROP TABLE IF EXISTS` commands, so this should work. But if it doesn't:

**Option A - Clean Database First:**
```sql
-- Drop all tables in public schema (CAREFUL!)
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO YOUR_USERNAME;
```

Then restore again.

**Option B - Use a Fresh Database:**
Create a new empty database and restore into it.

---

### Issue 3: No Errors But No Data

**Cause:** Restore ran but transaction wasn't committed

**Fix:** The backup file should auto-commit. Try:

```bash
# Force autocommit mode
psql -h HOST -U USERNAME -d DATABASE --set AUTOCOMMIT=on -f licenseiq_backup_20251125_185452.sql
```

---

### Issue 4: Connected to Wrong Database

**Cause:** Multiple databases on Hostinger, restored to wrong one

**Fix:** Check which database you're connected to:

```sql
SELECT current_database();
```

Make sure it matches the database name you want.

---

## ✅ Complete Restore Command

Here's the correct command to restore the backup to Hostinger:

```bash
# Replace these with your actual Hostinger credentials
PGPASSWORD=your_password psql \
  -h your-hostinger-host.com \
  -p 5432 \
  -U your_username \
  -d your_database_name \
  -f licenseiq_backup_20251125_185452.sql
```

**Watch for:**
- ✅ `CREATE TABLE` messages
- ✅ `INSERT 0 1` messages (each insert)
- ❌ Any `ERROR:` messages

---

## 📊 After Restore - Verify Data

Once restore completes successfully, verify the data:

```sql
-- Check admin user exists
SELECT username, email, role FROM users WHERE username = 'admin';

-- Check navigation categories
SELECT category_key, category_name, default_sort_order 
FROM navigation_categories 
ORDER BY default_sort_order;

-- Check contracts
SELECT id, contract_name, status FROM contracts LIMIT 5;
```

**Expected Results:**
- Admin user: `admin`, `admin@licenseiq.com`, `admin`
- 6 navigation categories
- 3 contracts

---

## 🔧 If All Else Fails

If you still don't see data after trying everything above:

### Option 1: Download Latest Backup

Use the **FINAL** backup which was created after all fixes:

```
licenseiq_backup_FINAL_20251125_190607.sql (761 KB)
```

This is the most recent backup with all the latest changes.

### Option 2: Check Restore Logs

Save the restore output to a file to see what went wrong:

```bash
psql -h HOST -U USER -d DATABASE -f licenseiq_backup_20251125_185452.sql 2>&1 | tee restore.log
```

Then check `restore.log` for errors.

### Option 3: Manual Table-by-Table Check

Check each table individually:

```sql
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = tablename) as column_count
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
```

This shows which tables have data and which are empty.

---

## 📞 Need Help?

If you're still having issues, please provide:

1. The exact restore command you used
2. Any error messages you saw
3. Output of: `SELECT COUNT(*) FROM users;`
4. Output of: `SELECT COUNT(*) FROM pg_tables WHERE schemaname='public';`

---

**Last Updated:** November 25, 2025 at 7:10 PM
