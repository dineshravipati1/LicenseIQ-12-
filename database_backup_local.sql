-- ===============================================
-- LICENSE IQ RESEARCH PLATFORM - DATABASE BACKUP
-- PostgreSQL Database Schema (Complete)
-- ===============================================
-- This file contains the complete database schema for the License IQ Research Platform
-- Restore this in your local PostgreSQL using pgAdmin or psql
-- 
-- RESTORE INSTRUCTIONS:
-- 1. Create a new database: CREATE DATABASE license_iq;
-- 2. Connect to the database: \c license_iq
-- 3. Run this file: \i database_backup_local.sql
-- 
-- OR using pgAdmin:
-- 1. Right-click Databases → Create → Database (name: license_iq)
-- 2. Right-click the new database → Query Tool
-- 3. Open this file and execute (F5)
-- ===============================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- ===============================================
-- CORE TABLES
-- ===============================================

-- Session storage table (AUTO-CREATED by connect-pg-simple)
-- DO NOT manually create this table - the app creates it automatically
-- with the correct naming convention to avoid conflicts
-- 
-- If you need to recreate it, run: DROP TABLE IF EXISTS "sessions" CASCADE;
-- Then restart your app and it will be auto-created
--
-- Uncomment below ONLY if you're not using the app's session store:
-- CREATE TABLE IF NOT EXISTS "sessions" (
--   "sid" VARCHAR NOT NULL PRIMARY KEY,
--   "sess" JSONB NOT NULL,
--   "expire" TIMESTAMP NOT NULL
-- );
-- CREATE INDEX IF NOT EXISTS "IDX_session_expire" ON "sessions" ("expire");

-- Users table
CREATE TABLE IF NOT EXISTS "users" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "username" VARCHAR UNIQUE NOT NULL,
  "email" VARCHAR UNIQUE,
  "password" VARCHAR NOT NULL,
  "first_name" VARCHAR,
  "last_name" VARCHAR,
  "profile_image_url" VARCHAR,
  "role" VARCHAR NOT NULL DEFAULT 'viewer', -- owner, admin, editor, viewer, auditor
  "is_active" BOOLEAN NOT NULL DEFAULT true,
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contracts table
CREATE TABLE IF NOT EXISTS "contracts" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "file_name" VARCHAR NOT NULL,
  "original_name" VARCHAR NOT NULL,
  "file_size" INTEGER NOT NULL,
  "file_type" VARCHAR NOT NULL,
  "file_path" VARCHAR NOT NULL,
  "contract_type" VARCHAR, -- license, service, partnership, employment, other
  "priority" VARCHAR NOT NULL DEFAULT 'normal', -- normal, high, urgent
  "status" VARCHAR NOT NULL DEFAULT 'uploaded', -- uploaded, processing, analyzed, failed
  "uploaded_by" VARCHAR NOT NULL REFERENCES "users"("id"),
  "notes" TEXT,
  "processing_started_at" TIMESTAMP,
  "processing_completed_at" TIMESTAMP,
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contract analysis results
CREATE TABLE IF NOT EXISTS "contract_analysis" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "summary" TEXT,
  "key_terms" JSONB, -- Array of extracted terms with confidence scores
  "risk_analysis" JSONB, -- Risk assessment results
  "insights" JSONB, -- AI-generated insights
  "confidence" DECIMAL(5, 2), -- Overall confidence score
  "processing_time" INTEGER, -- Processing time in seconds
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contract embeddings for semantic search (AI-driven RAG)
CREATE TABLE IF NOT EXISTS "contract_embeddings" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "embedding_type" VARCHAR NOT NULL, -- 'product', 'territory', 'full_contract', 'rule_description', 'summary', 'key_terms', 'insights'
  "source_text" TEXT NOT NULL, -- Original text that was embedded
  "embedding" vector(384), -- Hugging Face BAAI/bge-small-en-v1.5 produces 384 dimensions
  "metadata" JSONB, -- Additional context
  "created_at" TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "contract_embeddings_contract_idx" ON "contract_embeddings" ("contract_id");
CREATE INDEX IF NOT EXISTS "contract_embeddings_type_idx" ON "contract_embeddings" ("embedding_type");

-- Create HNSW index for fast vector similarity search
CREATE INDEX IF NOT EXISTS "contract_embeddings_vector_idx" 
  ON "contract_embeddings" 
  USING hnsw (embedding vector_cosine_ops);

-- Audit trail
CREATE TABLE IF NOT EXISTS "audit_trail" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "user_id" VARCHAR NOT NULL REFERENCES "users"("id"),
  "action" VARCHAR NOT NULL, -- login, logout, upload, analyze, view, edit, delete, etc.
  "resource_type" VARCHAR, -- contract, user, analysis, etc.
  "resource_id" VARCHAR,
  "details" JSONB, -- Additional context about the action
  "ip_address" VARCHAR,
  "user_agent" TEXT,
  "created_at" TIMESTAMP DEFAULT NOW()
);

-- ===============================================
-- ANALYSIS TABLES
-- ===============================================

-- Financial Analysis table
CREATE TABLE IF NOT EXISTS "financial_analysis" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "total_value" DECIMAL(15, 2),
  "currency" VARCHAR DEFAULT 'USD',
  "payment_schedule" JSONB, -- Array of payment dates and amounts
  "royalty_structure" JSONB, -- Royalty rates and calculation methods
  "revenue_projections" JSONB, -- Projected income over time
  "cost_impact" JSONB, -- Cost analysis and budget impact
  "currency_risk" DECIMAL(5, 2), -- Risk score 0-100
  "payment_terms" TEXT,
  "penalty_clauses" JSONB, -- Financial penalties and conditions
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Compliance Analysis table
CREATE TABLE IF NOT EXISTS "compliance_analysis" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "compliance_score" DECIMAL(5, 2), -- Overall compliance score 0-100
  "regulatory_frameworks" JSONB, -- GDPR, SOX, HIPAA, etc.
  "jurisdiction_analysis" JSONB, -- Governing law analysis
  "data_protection_compliance" BOOLEAN,
  "industry_standards" JSONB, -- Industry-specific compliance
  "risk_factors" JSONB, -- Compliance risk factors
  "recommended_actions" JSONB, -- Compliance improvement suggestions
  "last_compliance_check" TIMESTAMP DEFAULT NOW(),
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contract Obligations table
CREATE TABLE IF NOT EXISTS "contract_obligations" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "obligation_type" VARCHAR NOT NULL, -- payment, delivery, performance, reporting
  "description" TEXT NOT NULL,
  "due_date" TIMESTAMP,
  "responsible" VARCHAR, -- party responsible for obligation
  "status" VARCHAR DEFAULT 'pending', -- pending, completed, overdue, cancelled
  "priority" VARCHAR DEFAULT 'medium', -- low, medium, high, critical
  "completion_date" TIMESTAMP,
  "notes" TEXT,
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contract Performance Metrics table
CREATE TABLE IF NOT EXISTS "performance_metrics" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "performance_score" DECIMAL(5, 2), -- 0-100
  "milestone_completion" DECIMAL(5, 2), -- % completed
  "on_time_delivery" BOOLEAN DEFAULT true,
  "budget_variance" DECIMAL(10, 2), -- Over/under budget
  "quality_score" DECIMAL(5, 2), -- Quality assessment
  "client_satisfaction" DECIMAL(5, 2), -- Satisfaction rating
  "renewal_probability" DECIMAL(5, 2), -- Renewal likelihood
  "last_review_date" TIMESTAMP,
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Strategic Analysis table
CREATE TABLE IF NOT EXISTS "strategic_analysis" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "strategic_value" DECIMAL(5, 2), -- Strategic importance score
  "market_alignment" DECIMAL(5, 2), -- How well aligned with market
  "competitive_advantage" JSONB, -- Competitive benefits
  "risk_concentration" DECIMAL(5, 2), -- Risk concentration level
  "standardization_score" DECIMAL(5, 2), -- Template compliance
  "negotiation_insights" JSONB, -- Negotiation patterns and suggestions
  "benchmark_comparison" JSONB, -- Industry benchmark comparison
  "recommendations" JSONB, -- Strategic recommendations
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Contract Comparisons table
CREATE TABLE IF NOT EXISTS "contract_comparisons" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "similar_contracts" JSONB, -- Array of similar contract IDs and similarity scores
  "clause_variations" JSONB, -- Differences in key clauses
  "term_comparisons" JSONB, -- Financial and legal term comparisons
  "best_practices" JSONB, -- Identified best practices from comparisons
  "anomalies" JSONB, -- Unusual terms or conditions
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Market Benchmarks table
CREATE TABLE IF NOT EXISTS "market_benchmarks" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_type" VARCHAR NOT NULL,
  "industry" VARCHAR,
  "benchmark_data" JSONB, -- Market standard terms, rates, etc.
  "average_value" DECIMAL(15, 2),
  "standard_terms" JSONB, -- Common terms for this contract type
  "risk_factors" JSONB, -- Common risk factors
  "last_updated" TIMESTAMP DEFAULT NOW(),
  "created_at" TIMESTAMP DEFAULT NOW()
);

-- ===============================================
-- AI-DRIVEN ROYALTY CALCULATION SYSTEM
-- ===============================================

-- Sales Data (AI-Matched to Contracts)
CREATE TABLE IF NOT EXISTS "sales_data" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "matched_contract_id" VARCHAR REFERENCES "contracts"("id"),
  "match_confidence" DECIMAL(5, 2),
  "transaction_date" TIMESTAMP NOT NULL,
  "transaction_id" VARCHAR,
  "product_code" VARCHAR,
  "product_name" VARCHAR,
  "category" VARCHAR,
  "territory" VARCHAR,
  "currency" VARCHAR DEFAULT 'USD',
  "gross_amount" DECIMAL(15, 2) NOT NULL,
  "net_amount" DECIMAL(15, 2),
  "quantity" DECIMAL(12, 4),
  "unit_price" DECIMAL(15, 2),
  "custom_fields" JSONB,
  "import_job_id" VARCHAR,
  "created_at" TIMESTAMP DEFAULT NOW()
);

-- Contract-based Royalty Calculations
CREATE TABLE IF NOT EXISTS "contract_royalty_calculations" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "name" VARCHAR NOT NULL, -- e.g., "Q1 2024 Royalties"
  "period_start" TIMESTAMP,
  "period_end" TIMESTAMP,
  "status" VARCHAR DEFAULT 'pending_approval', -- pending_approval, approved, rejected, paid
  "total_sales_amount" DECIMAL(15, 2),
  "total_royalty" DECIMAL(15, 2),
  "currency" VARCHAR DEFAULT 'USD',
  "sales_count" INTEGER,
  "breakdown" JSONB, -- Detailed per-sale breakdown
  "chart_data" JSONB, -- Pre-computed chart data
  "calculated_by" VARCHAR REFERENCES "users"("id"),
  "approved_by" VARCHAR REFERENCES "users"("id"),
  "approved_at" TIMESTAMP,
  "rejected_by" VARCHAR REFERENCES "users"("id"),
  "rejected_at" TIMESTAMP,
  "rejection_reason" TEXT,
  "comments" TEXT,
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Structured Royalty Rules (Extracted from Contracts)
CREATE TABLE IF NOT EXISTS "royalty_rules" (
  "id" VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  "contract_id" VARCHAR NOT NULL REFERENCES "contracts"("id"),
  "rule_type" VARCHAR NOT NULL, -- 'percentage', 'tiered', 'minimum_guarantee', 'cap', 'deduction', 'fixed_fee'
  "rule_name" VARCHAR NOT NULL,
  "description" TEXT,
  
  -- JSON-based dynamic formula storage (NEW)
  "formula_definition" JSONB, -- Complete FormulaDefinition object with expression tree
  "formula_version" VARCHAR DEFAULT '1.0', -- Version for tracking formula changes
  
  -- LEGACY: Tabular columns (kept for backwards compatibility)
  "product_categories" TEXT[], -- Array of product categories this rule applies to
  "territories" TEXT[], -- Array of territories
  "container_sizes" TEXT[], -- e.g., ["1-gallon", "5-gallon"]
  "seasonal_adjustments" JSONB, -- e.g., {"Spring": 1.10, "Fall": 0.95, "Holiday": 1.20}
  "territory_premiums" JSONB, -- e.g., {"Secondary": 1.10, "Organic": 1.25}
  "volume_tiers" JSONB, -- [{"min": 0, "max": 4999, "rate": 1.25}, {"min": 5000, "rate": 1.10}]
  "base_rate" DECIMAL(15, 2), -- Base royalty rate
  "minimum_guarantee" DECIMAL(15, 2), -- Annual minimum
  "calculation_formula" TEXT, -- Description of how to calculate
  
  -- Metadata
  "priority" INTEGER DEFAULT 10, -- Lower number = higher priority
  "is_active" BOOLEAN DEFAULT true,
  "confidence" DECIMAL(5, 2), -- AI extraction confidence
  "source_section" VARCHAR, -- Where in contract this was found
  "source_text" TEXT, -- Original contract text
  "created_at" TIMESTAMP DEFAULT NOW(),
  "updated_at" TIMESTAMP DEFAULT NOW()
);

-- ===============================================
-- INDEXES FOR PERFORMANCE
-- ===============================================

CREATE INDEX IF NOT EXISTS "contracts_uploaded_by_idx" ON "contracts" ("uploaded_by");
CREATE INDEX IF NOT EXISTS "contracts_status_idx" ON "contracts" ("status");
CREATE INDEX IF NOT EXISTS "contract_analysis_contract_idx" ON "contract_analysis" ("contract_id");
CREATE INDEX IF NOT EXISTS "financial_analysis_contract_idx" ON "financial_analysis" ("contract_id");
CREATE INDEX IF NOT EXISTS "audit_trail_user_idx" ON "audit_trail" ("user_id");
CREATE INDEX IF NOT EXISTS "audit_trail_action_idx" ON "audit_trail" ("action");
CREATE INDEX IF NOT EXISTS "sales_data_contract_idx" ON "sales_data" ("matched_contract_id");
CREATE INDEX IF NOT EXISTS "sales_data_transaction_date_idx" ON "sales_data" ("transaction_date");
CREATE INDEX IF NOT EXISTS "royalty_calculations_contract_idx" ON "contract_royalty_calculations" ("contract_id");
CREATE INDEX IF NOT EXISTS "royalty_calculations_status_idx" ON "contract_royalty_calculations" ("status");
CREATE INDEX IF NOT EXISTS "royalty_rules_contract_idx" ON "royalty_rules" ("contract_id");
CREATE INDEX IF NOT EXISTS "royalty_rules_is_active_idx" ON "royalty_rules" ("is_active");

-- ===============================================
-- SAMPLE DATA (OPTIONAL - COMMENT OUT IF NOT NEEDED)
-- ===============================================

-- Create a default admin user (password: admin123)
-- IMPORTANT: Change this password immediately after first login!
-- Password hash for 'admin123' using bcrypt (cost factor 10)
INSERT INTO "users" ("id", "username", "email", "password", "first_name", "last_name", "role", "is_active")
VALUES (
  gen_random_uuid(),
  'admin',
  'admin@licenseiq.com',
  '$2b$10$K87wUgLj5.LJPyGGDJqmUeK8Z9ZfK1.eYJlHnGKY7kXKZ1ZfK1.e.',
  'Admin',
  'User',
  'owner',
  true
) ON CONFLICT (username) DO NOTHING;

-- ===============================================
-- COMPLETION MESSAGE
-- ===============================================

DO $$
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE 'License IQ Database Schema installed successfully!';
  RAISE NOTICE '========================================';
  RAISE NOTICE 'Total tables created: 20';
  RAISE NOTICE 'Extensions enabled: uuid-ossp, vector (pgvector)';
  RAISE NOTICE '';
  RAISE NOTICE 'IMPORTANT SECURITY NOTES:';
  RAISE NOTICE '1. Change the default admin password immediately!';
  RAISE NOTICE '2. Remove sample data in production environments';
  RAISE NOTICE '3. Configure DATABASE_URL in your .env file';
  RAISE NOTICE '';
  RAISE NOTICE 'Next steps:';
  RAISE NOTICE '1. Update DATABASE_URL environment variable';
  RAISE NOTICE '2. Run: npm run dev';
  RAISE NOTICE '3. Login with username: admin, password: admin123';
  RAISE NOTICE '========================================';
END $$;
