-- ==========================================
-- CLEANUP SCRIPT: Delete All Contracts and Related Data
-- ==========================================
-- This script removes all contracts and their associated data
-- Users and system tables are preserved

-- Delete all contract-related data (order matters for foreign keys)
DELETE FROM contract_royalty_calculations;
DELETE FROM market_benchmarks;
DELETE FROM contract_comparisons;
DELETE FROM strategic_analysis;
DELETE FROM performance_metrics;
DELETE FROM contract_obligations;
DELETE FROM compliance_analysis;
DELETE FROM financial_analysis;
DELETE FROM contract_embeddings;
DELETE FROM contract_analysis;
DELETE FROM contracts;

-- Clean up audit trail entries related to contracts
DELETE FROM audit_trail WHERE resource_type = 'contract';

-- Success message
SELECT 'All contracts and related data have been deleted successfully!' AS status;
SELECT 'Users and system data preserved.' AS info;
