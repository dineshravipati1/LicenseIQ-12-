# LicenseIQ Database Backups

This folder contains SQL backup files for the LicenseIQ Research Platform PostgreSQL database.

## 📁 Backup Files

All backup files follow the naming convention:
```
licenseiq_backup_YYYYMMDD_HHMMSS.sql
```

## 🔄 How to Restore a Backup

### Using psql command:
```bash
psql $DATABASE_URL < database/backups/licenseiq_backup_YYYYMMDD_HHMMSS.sql
```

### Using the Replit environment:
```bash
psql $DATABASE_URL -f database/backups/licenseiq_backup_YYYYMMDD_HHMMSS.sql
```

## 📊 Latest Backup Information

**File:** `licenseiq_backup_20251117_160417.sql`
**Size:** 613 KB
**Created:** November 17, 2025 at 16:04 UTC
**Tables:** 48 tables

### Tables Included:
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
- demo_requests
- early_access_signups
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
- market_benchmarks
- master_data_mappings
- navigation_permissions
- performance_metrics
- role_navigation_permissions
- roles
- royalty_rules
- rule_definitions
- rule_node_definitions
- rule_validation_events
- sales_data
- sales_field_mappings
- semantic_index_entries
- session
- sessions
- strategic_analysis
- system_embeddings
- user_navigation_overrides
- users

## 🔐 Security Notes

- Backup files contain sensitive data including hashed passwords
- Keep backups secure and never commit to public repositories
- Database backups are included in `.gitignore` by default

## 🛠️ Creating New Backups

To create a new backup:
```bash
BACKUP_DATE=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="database/backups/licenseiq_backup_${BACKUP_DATE}.sql"
pg_dump $DATABASE_URL > "$BACKUP_FILE"
```

## 📝 Backup Contents

Each backup includes:
- Complete database schema (CREATE TABLE statements)
- All data (INSERT statements)
- Sequences and indexes
- Constraints and foreign keys
- PostgreSQL extensions (pgvector)

## ⚠️ Important Notes

1. **Production Deployments:** When deploying to Hostinger VPS (qa.licenseiq.ai), use these backups to seed the production database
2. **Version Control:** Backups are excluded from Git to prevent sensitive data exposure
3. **Data Size:** Monitor backup file sizes - large backups may indicate need for data archival
4. **Regular Backups:** Create backups before major schema changes or deployments
