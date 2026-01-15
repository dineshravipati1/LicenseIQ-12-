-- ==========================================
-- CLEANUP SCRIPT: Remove Old Vendor-Based Royalty System
-- ==========================================
-- This script removes all old vendor-based royalty tables and data
-- The new system uses AI-driven contract-based royalty calculations

-- Drop old royalty system tables (in correct order to avoid FK constraints)
DROP TABLE IF EXISTS royalty_results CASCADE;
DROP TABLE IF EXISTS royalty_runs CASCADE;
DROP TABLE IF EXISTS sales_staging CASCADE;
DROP TABLE IF EXISTS sales_data CASCADE;
DROP TABLE IF EXISTS erp_import_jobs CASCADE;
DROP TABLE IF EXISTS product_mappings CASCADE;
DROP TABLE IF EXISTS license_rules CASCADE;
DROP TABLE IF EXISTS license_rule_sets CASCADE;
DROP TABLE IF EXISTS license_documents CASCADE;
DROP TABLE IF EXISTS erp_connections CASCADE;
DROP TABLE IF EXISTS vendors CASCADE;

-- Success message
SELECT 'Old vendor-based royalty system tables removed successfully!' AS status;
