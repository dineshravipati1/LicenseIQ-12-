# ERP Mapping End-to-End Testing Guide

## Overview

This document provides comprehensive testing procedures for the complete ERP mapping workflow in LicenseIQ - from Oracle Fusion Cloud data upload through license fee calculation and invoice generation.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [System Configuration](#system-configuration)
3. [Phase 1: ERP Data Import](#phase-1-erp-data-import)
4. [Phase 2: Contract Upload & AI Extraction](#phase-2-contract-upload--ai-extraction)
5. [Phase 3: Master Data Mapping](#phase-3-master-data-mapping)
6. [Phase 4: ERP Mapping Rules Configuration](#phase-4-erp-mapping-rules-configuration)
7. [Phase 5: License Fee Calculation](#phase-5-license-fee-calculation)
8. [Phase 6: Invoice Generation](#phase-6-invoice-generation)
9. [Test Scenarios Matrix](#test-scenarios-matrix)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Access
- Admin or Owner role in LicenseIQ
- Access to at least one Company with Business Units and Locations configured
- Sample Oracle Fusion Cloud CSV export files
- Sample contract PDF documents

### Sample Test Data

#### Oracle Fusion CSV Format
```csv
TRANSACTION_ID,TRANSACTION_DATE,PRODUCT_CODE,PRODUCT_NAME,QUANTITY,UNIT_PRICE,GROSS_AMOUNT,CUSTOMER_ID,CUSTOMER_NAME,LOCATION_CODE
TXN001,2025-01-15,PROD-001,Widget A,100,25.00,2500.00,CUST-001,Acme Corp,LOC-001
TXN002,2025-01-16,PROD-002,Widget B,50,50.00,2500.00,CUST-002,Beta Inc,LOC-002
```

#### Contract PDF Requirements
- Standard licensing agreement with royalty/license fee terms
- Contains: parties, effective dates, territories, product definitions, payment terms
- File size: Under 10MB for optimal processing

---

## System Configuration

### Step 1: Verify Organization Context

1. Navigate to Dashboard
2. Confirm the correct Company is selected in the context switcher
3. Verify Business Unit and Location are properly set

**Validation Checkpoint:**
- [ ] Company name appears in header
- [ ] Business Unit dropdown shows available options
- [ ] Location selector functions correctly

### Step 2: Configure Calculation Approach

1. Navigate to **Data Management > ERP Mapping Rules**
2. Locate the "Calculation Approach" toggle
3. Select appropriate mode:
   - **Manual**: Traditional manual license fee rules only
   - **ERP Rules**: Automated calculations from ERP field mappings
   - **Hybrid**: Both systems active with per-contract overrides

**Validation Checkpoint:**
- [ ] Toggle updates immediately
- [ ] Success toast notification appears
- [ ] Setting persists after page refresh

---

## Phase 1: ERP Data Import

### Step 1.1: Access ERP Integration Hub

1. Navigate to **Data Management > ERP Mapping**
2. Select "Import Data" tab
3. Choose source ERP system (e.g., Oracle Fusion Cloud)

### Step 1.2: Upload ERP Data File

1. Click "Upload CSV" or drag-and-drop file
2. Review column mapping preview
3. Verify field mappings:
   - `TRANSACTION_DATE` → Sales Date
   - `PRODUCT_CODE` → Product ID
   - `GROSS_AMOUNT` → Net Revenue
   - `LOCATION_CODE` → Location

### Step 1.3: Execute Import

1. Click "Start Import" 
2. Monitor progress bar
3. Review import summary:
   - Total records processed
   - Successful imports
   - Errors/warnings

**Validation Checkpoint:**
- [ ] CSV columns correctly mapped
- [ ] Data preview shows accurate values
- [ ] Import completes without critical errors
- [ ] Records appear in Sales Data view

**Expected Output:**
- Sales records created in `sales_data` table
- Import job logged with timestamp and statistics
- Any mapping issues flagged for review

---

## Phase 2: Contract Upload & AI Extraction

### Step 2.1: Upload Contract PDF

1. Navigate to **Contracts > Upload Contract**
2. Select PDF file from local system
3. Fill in basic metadata:
   - Contract Name
   - Contract Type
   - Licensee/Licensor

### Step 2.2: AI Processing

1. Click "Upload and Process"
2. Wait for AI extraction (may take 30-60 seconds)
3. System performs:
   - Text extraction from PDF
   - Zero-shot entity recognition
   - Contract term identification
   - Key clause summarization

### Step 2.3: Review AI Analysis

1. Navigate to contract detail page
2. Review "AI Analysis" tab
3. Verify extracted data:
   - Parties identified
   - Effective/expiration dates
   - Territory definitions
   - Product categories
   - Payment terms/royalty rates

**Validation Checkpoint:**
- [ ] PDF uploads successfully
- [ ] AI processing completes without timeout
- [ ] Key entities extracted accurately
- [ ] Confidence scores displayed for each extraction
- [ ] Contract appears in contracts list

**Expected Output:**
- Contract record in `contracts` table
- AI analysis stored in `contract_analysis_data`
- Extracted entities in `contract_extracted_entities`
- Processing status: "completed"

---

## Phase 3: Master Data Mapping

### Step 3.1: Configure ERP Catalog

1. Navigate to **Data Management > ERP Catalog**
2. Verify ERP system is registered (e.g., Oracle Fusion Cloud)
3. Review available entities and fields

### Step 3.2: Configure LicenseIQ Catalog

1. Navigate to **Data Management > LicenseIQ Catalog**
2. Verify standard entities exist:
   - Products
   - Customers
   - Locations
   - Transactions

### Step 3.3: Create Field Mappings

1. Navigate to **Data Management > Master Data**
2. Select source ERP entity
3. Map fields to LicenseIQ schema:

| ERP Field (Oracle) | LicenseIQ Field | Transformation |
|-------------------|-----------------|----------------|
| PRODUCT_CODE | product_id | Direct |
| PRODUCT_NAME | product_name | Direct |
| GROSS_AMOUNT | net_revenue | Direct |
| CUSTOMER_ID | customer_id | Direct |

### Step 3.4: AI-Assisted Mapping

1. Click "AI Suggest Mappings"
2. Review AI recommendations with confidence scores
3. Accept or modify mappings
4. Confirm final mappings

**Validation Checkpoint:**
- [ ] ERP fields visible in mapping UI
- [ ] LicenseIQ target fields selectable
- [ ] AI suggestions appear with confidence %
- [ ] Mappings save successfully
- [ ] Dual terminology displays correctly (e.g., "Net Revenue (ERP: GROSS_AMOUNT)")

**Expected Output:**
- Mappings stored in `master_data_mappings` table
- Pending mappings in `pending_term_mappings` (if AI confidence < threshold)
- Confirmed mappings marked as active

---

## Phase 4: ERP Mapping Rules Configuration

### Step 4.1: Create Rule Set

1. Navigate to **Data Management > ERP Mapping Rules**
2. Click "Create Rule Set"
3. Fill in details:
   - **Name**: "Oracle Fusion License Fee Rules"
   - **Description**: "Automated calculation from Oracle sales data"
   - **Source System**: Oracle Fusion Cloud
   - **Source Entity**: Transactions
   - **Target Entity**: License Fee Calculations

### Step 4.2: Configure Rules (Future Enhancement)

*Note: Full rule editor coming in future release*

1. Open rule set for editing
2. Define calculation logic:
   - Source field: GROSS_AMOUNT
   - Calculation type: Percentage
   - Rate: 5%
   - Conditions: Territory = "North America"

### Step 4.3: Activate Rule Set

1. Return to rule sets list
2. Click "Activate" on the new rule set
3. Confirm activation

**Validation Checkpoint:**
- [ ] Rule set created successfully
- [ ] Source/target entities linked correctly
- [ ] Status changes to "Active"
- [ ] Rule set appears in statistics dashboard

**Expected Output:**
- Rule set record in `erp_mapping_rule_sets`
- Status: "active"
- Ready for calculation execution

---

## Phase 5: License Fee Calculation

### Step 5.1: Manual Rules (Traditional)

1. Navigate to **Contracts > License Fee Rules**
2. Select contract
3. Add manual rule:
   - Rule Type: Percentage of Net Revenue
   - Rate: 5%
   - Minimum: $1,000
   - Territory: All

### Step 5.2: ERP-Based Calculation

1. Navigate to **Finance > Royalty Calculator**
2. Select calculation period (date range)
3. Choose contracts to include
4. Select calculation mode:
   - Manual Rules
   - ERP Mapping Rules
   - Hybrid (both)

### Step 5.3: Execute Calculation

1. Click "Run Calculation"
2. Monitor progress
3. Review results:
   - Total license fees calculated
   - Breakdown by contract
   - Breakdown by territory
   - Source data traceability

**Validation Checkpoint:**
- [ ] Calculation completes successfully
- [ ] Results match expected values
- [ ] Source sales data linked to calculations
- [ ] Audit trail created

**Expected Output:**
- Calculation records in `contract_royalty_calculations`
- For ERP rules: execution log in `erp_rule_execution_log`
- Calculation status: "completed"
- Traceability from calculation → rule → sales record

---

## Phase 6: Invoice Generation

### Step 6.1: Review Calculations

1. Navigate to **Finance > Calculations**
2. Filter by period/contract
3. Verify calculation accuracy
4. Approve calculations for invoicing

### Step 6.2: Generate Invoice PDF

1. Select approved calculations
2. Click "Generate Invoice"
3. Configure invoice options:
   - Invoice date
   - Due date
   - Notes/terms

### Step 6.3: Download/Send Invoice

1. Preview generated PDF
2. Verify content:
   - Correct parties
   - Accurate amounts
   - Proper formatting
   - Dual terminology preserved

**Validation Checkpoint:**
- [ ] PDF generates without errors
- [ ] All line items present
- [ ] Calculations match source data
- [ ] Terms and conditions included
- [ ] Download functions correctly

**Expected Output:**
- PDF invoice document
- Invoice record in system (if applicable)
- Audit log entry for generation

---

## Test Scenarios Matrix

### Scenario 1: Happy Path - Full ERP Flow

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Upload Oracle CSV | Import successful | [ ] |
| 2 | Upload contract PDF | AI extraction complete | [ ] |
| 3 | Map ERP fields | Mappings saved | [ ] |
| 4 | Create ERP rule set | Rule set active | [ ] |
| 5 | Run calculation | Fees calculated | [ ] |
| 6 | Generate invoice | PDF created | [ ] |

### Scenario 2: Manual Fallback

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Set approach to "Manual" | Toggle updated | [ ] |
| 2 | Create manual rules | Rules saved | [ ] |
| 3 | Run calculation | Uses manual rules only | [ ] |
| 4 | Verify no ERP execution logs | Logs empty | [ ] |

### Scenario 3: Hybrid Mode

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Set approach to "Hybrid" | Toggle updated | [ ] |
| 2 | Create both rule types | Rules saved | [ ] |
| 3 | Run calculation | Both engines execute | [ ] |
| 4 | Verify combined results | All fees included | [ ] |

### Scenario 4: Per-Contract Override

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Set org to "ERP Rules" | Toggle updated | [ ] |
| 2 | Override contract to "Manual" | Contract updated | [ ] |
| 3 | Run calculation | Contract uses manual | [ ] |
| 4 | Other contracts use ERP | Mixed execution | [ ] |

### Scenario 5: Error Handling - Invalid CSV

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Upload malformed CSV | Error displayed | [ ] |
| 2 | Review error details | Column mismatch shown | [ ] |
| 3 | No partial import | Data unchanged | [ ] |

### Scenario 6: Audit Trail Verification

| Step | Action | Expected Result | Status |
|------|--------|-----------------|--------|
| 1 | Complete full workflow | All steps logged | [ ] |
| 2 | Navigate to Audit Trail | Records visible | [ ] |
| 3 | Trace calculation to source | Links work | [ ] |
| 4 | Verify user/timestamp | Accurate data | [ ] |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ERP MAPPING PROCESS FLOW                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Oracle Fusion   │     │   Contract PDF   │     │   ERP Catalog    │
│   Cloud Export   │     │     Upload       │     │   Configuration  │
│      (CSV)       │     │                  │     │                  │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Data Import    │     │  AI Extraction   │     │  LicenseIQ       │
│   Service        │     │  (Groq LLaMA)    │     │  Schema Catalog  │
│                  │     │                  │     │                  │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Master Data Mapping                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  Contract Term (LicenseIQ Entity: Master Data Value)       │ │
│  │  Example: "Net Revenue (ERP: GROSS_AMOUNT → LIQ: revenue)" │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────┬──────────────────────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         ▼                      ▼                      ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  Manual License  │   │  ERP Mapping     │   │   Hybrid Mode    │
│   Fee Rules      │   │  Rule Sets       │   │  (Both Active)   │
│                  │   │                  │   │                  │
└────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                ▼
                   ┌──────────────────────┐
                   │  Royalty Calculator  │
                   │  (Calculation Run)   │
                   └───────────┬──────────┘
                               │
                               ▼
                   ┌──────────────────────┐
                   │ contract_royalty_    │
                   │ calculations         │◄───── Shared Ledger
                   │ (Unified Results)    │       (Both Engines)
                   └───────────┬──────────┘
                               │
                               ▼
                   ┌──────────────────────┐
                   │  Invoice Generation  │
                   │  (PDF Output)        │
                   └──────────────────────┘
```

---

## Troubleshooting

### Issue: CSV Import Fails

**Symptoms:** Error message during upload, no records imported

**Solutions:**
1. Verify CSV encoding (UTF-8 required)
2. Check column headers match expected format
3. Ensure date formats are ISO (YYYY-MM-DD)
4. Remove special characters from text fields

### Issue: AI Extraction Timeout

**Symptoms:** Processing spinner indefinitely, no analysis results

**Solutions:**
1. Check PDF file size (< 10MB recommended)
2. Verify PDF is text-based (not scanned image)
3. Check Groq API key is configured
4. Review server logs for API errors

### Issue: Calculations Don't Match

**Symptoms:** License fee amounts incorrect

**Solutions:**
1. Verify rule configurations (rates, conditions)
2. Check sales data date ranges
3. Confirm territory mappings
4. Review calculation approach setting
5. Check for per-contract overrides

### Issue: Invoice Generation Error

**Symptoms:** PDF fails to generate or is blank

**Solutions:**
1. Ensure calculations are approved
2. Verify all required fields populated
3. Check server memory/resources
4. Review PDF service logs

---

## Appendix: Database Tables Reference

| Table | Purpose |
|-------|---------|
| `sales_data` | Imported ERP transaction records |
| `contracts` | Contract metadata and status |
| `contract_analysis_data` | AI extraction results |
| `contract_extracted_entities` | Parsed contract terms |
| `master_data_mappings` | ERP-to-LicenseIQ field mappings |
| `pending_term_mappings` | Unconfirmed AI mapping suggestions |
| `org_calculation_settings` | Organization calculation approach |
| `erp_mapping_rule_sets` | ERP rule set definitions |
| `erp_mapping_rules` | Individual mapping rules |
| `erp_rule_execution_log` | ERP calculation audit trail |
| `contract_royalty_calculations` | Unified calculation results |
| `contract_royalty_rules` | Manual license fee rules |

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-06 | System | Initial documentation |

