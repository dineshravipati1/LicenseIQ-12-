# ERP Mapping Rules Implementation Plan

**Document Version:** 1.0  
**Date:** January 6, 2026  
**Status:** Planning Phase  
**Platform:** LicenseIQ Research Platform

---

## Executive Summary

This document outlines the implementation plan for the **ERP Mapping Rules** system - a new parallel calculation engine that leverages confirmed ERP-to-LicenseIQ field mappings to automate license fee calculations. This system will coexist with the existing manual "Manage License Fee Rules" functionality, providing organizations with a choice between two calculation approaches.

### Key Objectives
- Enable automated rule creation based on confirmed ERP field mappings
- Maintain 100% backward compatibility with existing functionality
- Provide clear traceability from ERP terms to license fee calculations
- Support gradual adoption without disrupting current workflows

---

## 1. Current System Analysis

### 1.1 Existing Components

| Component | Purpose | Risk Level |
|-----------|---------|------------|
| `royaltyRules` | Stores manual rule definitions | Protected - No Changes |
| `ruleDefinitions` | Formula-based calculation rules | Protected - No Changes |
| `dynamicRulesEngine` | Runtime formula interpreter | Protected - No Changes |
| `salesDataParser` | Processes uploaded sales CSV | Minor Extension Only |
| `contractRoyaltyCalculations` | Stores calculation results | Shared Output Target |

### 1.2 ERP Mapping Components (Already Built)

| Component | Purpose | Integration Point |
|-----------|---------|-------------------|
| `masterDataMappings` | Confirmed ERP↔LicenseIQ field mappings | Rule source |
| `pendingTermMappings` | AI-suggested mappings awaiting confirmation | Must be confirmed first |
| `erpSystems` | ERP system catalog (Oracle, SAP, etc.) | Reference data |
| `erpEntities` | ERP entity definitions (Items, Vendors) | Source schema |
| `licenseiqEntities` | Target schema definitions | Target schema |
| `erpFields` / `licenseiqFields` | Field definitions with types | Mapping keys |

---

## 2. Proposed Architecture

### 2.1 Dual-Mode Calculation Engine

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ORGANIZATION SETTINGS                            │
│                                                                      │
│   Calculation Approach: [Manual] ←────Toggle────→ [ERP Mapping]     │
│                                                                      │
│   □ Allow per-contract override                                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────────────┐
│   MANUAL APPROACH       │     │   ERP MAPPING APPROACH              │
│   (Existing - Unchanged)│     │   (New - Parallel System)           │
├─────────────────────────┤     ├─────────────────────────────────────┤
│                         │     │                                      │
│  • User creates rules   │     │  • Rules derived from confirmed     │
│    manually             │     │    ERP field mappings               │
│  • FormulaNode JSON     │     │  • Transformation rules applied     │
│    expression trees     │     │  • Master data lookup integration   │
│  • Contract-specific    │     │  • Audit trail to source mapping    │
│    configuration        │     │                                      │
│                         │     │                                      │
└─────────────────────────┘     └─────────────────────────────────────┘
              │                               │
              │                               │
              └───────────────┬───────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SHARED OUTPUT LAYER                                │
│                                                                      │
│   contractRoyaltyCalculations table (unchanged structure)           │
│   → Same invoice generation                                          │
│   → Same reporting/analytics                                         │
│   → Same audit logging                                               │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 ERP Term Mapping Flow

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  ORACLE FUSION   │    │  MASTER DATA     │    │  LICENSEIQ       │
│  ERP FIELDS      │───▶│  MAPPING         │───▶│  SCHEMA          │
└──────────────────┘    └──────────────────┘    └──────────────────┘
                                │
    ItemNumber ─────────────────┼────────────────▶ item_number
    ItemDescription ────────────┼────────────────▶ full_legal_product_name
    ItemDescription ────────────┼────────────────▶ description (truncated)
    ItemClass ──────────────────┼────────────────▶ item_category
    UnitPrice ──────────────────┼────────────────▶ license_fee_rate
                                │
                                ▼
                   ┌──────────────────────┐
                   │  ERP MAPPING RULES   │
                   │                      │
                   │  Rule 1: Direct copy │
                   │  Rule 2: Transform   │
                   │  Rule 3: Lookup      │
                   │  Rule 4: Calculate   │
                   └──────────────────────┘
                                │
                                ▼
                   ┌──────────────────────┐
                   │  CALCULATION ENGINE  │
                   │                      │
                   │  Sales × Rate = Fee  │
                   └──────────────────────┘
```

---

## 3. Database Schema Design

### 3.1 New Tables (Additive Only - No Existing Table Modifications)

#### Table: `org_calculation_settings`
Controls which calculation approach an organization uses.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| company_id | varchar (FK) | References companies table |
| calculation_approach | enum | 'manual' or 'erp_mapping' |
| default_approach | boolean | Is this the org default? |
| allow_contract_override | boolean | Can contracts use different approach? |
| created_by | varchar (FK) | User who configured |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

#### Table: `erp_mapping_rule_sets`
Groups of related mapping rules.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| name | varchar | Display name |
| description | text | Purpose description |
| company_id | varchar (FK) | 3-level hierarchy |
| business_unit_id | varchar (FK) | 3-level hierarchy |
| location_id | varchar (FK) | 3-level hierarchy |
| source_system_id | varchar (FK) | References erpSystems |
| source_entity_id | varchar (FK) | References erpEntities |
| target_entity_id | varchar (FK) | References licenseiqEntities |
| mapping_id | varchar (FK) | References masterDataMappings |
| status | enum | 'draft', 'active', 'inactive' |
| version | integer | Version number for audit |
| effective_date | date | When rules take effect |
| expiry_date | date | When rules expire (nullable) |
| created_by | varchar (FK) | Creator user ID |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

#### Table: `erp_mapping_rules`
Individual transformation rules within a rule set.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| rule_set_id | varchar (FK) | Parent rule set |
| name | varchar | Rule display name |
| description | text | What this rule does |
| priority | integer | Execution order (1 = first) |
| source_field | varchar | ERP field name |
| source_field_id | varchar (FK) | References erpFields |
| target_field | varchar | LicenseIQ field name |
| target_field_id | varchar (FK) | References licenseiqFields |
| transformation_type | enum | 'direct', 'lookup', 'formula', 'conditional' |
| transformation_config | jsonb | Transformation parameters |
| is_active | boolean | Is rule enabled? |
| created_at | timestamp | Creation timestamp |
| updated_at | timestamp | Last update timestamp |

#### Table: `erp_mapping_conditions`
Conditions that determine when a rule applies.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| rule_id | varchar (FK) | Parent rule |
| field_name | varchar | Field to evaluate |
| operator | enum | 'equals', 'contains', 'greater_than', 'between', 'in', 'not_null' |
| value | varchar | Comparison value |
| value_list | jsonb | For 'in' operator |
| logic_operator | enum | 'AND', 'OR' for chaining |
| order_index | integer | Condition evaluation order |

#### Table: `erp_mapping_outputs`
Calculated output fields from rules.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| rule_id | varchar (FK) | Parent rule |
| output_field | varchar | Target output field |
| calculation_type | enum | 'percentage', 'fixed', 'tiered', 'formula' |
| calculation_config | jsonb | Calculation parameters |
| rounding_mode | enum | 'none', 'up', 'down', 'nearest' |
| decimal_places | integer | Precision |

#### Table: `erp_rule_execution_log`
Audit trail for rule executions.

| Column | Type | Description |
|--------|------|-------------|
| id | varchar (PK) | UUID primary key |
| rule_set_id | varchar (FK) | Which rule set ran |
| calculation_id | varchar (FK) | Resulting calculation |
| sales_record_id | varchar | Source sales record |
| input_data | jsonb | Input values used |
| output_data | jsonb | Calculated values |
| rules_applied | jsonb | Which rules fired |
| execution_time_ms | integer | Performance metric |
| status | enum | 'success', 'partial', 'failed' |
| error_message | text | If failed |
| executed_at | timestamp | Execution timestamp |

---

## 4. Risk Analysis & Mitigation

### 4.1 Risk Assessment Matrix

| Risk | Likelihood | Impact | Severity | Mitigation |
|------|------------|--------|----------|------------|
| **Incomplete Mappings** | Medium | High | High | Readiness check before activation |
| **Calculation Accuracy** | Low | Critical | High | Preview mode + test data validation |
| **Data Type Mismatch** | Medium | Medium | Medium | Type validation in transformation |
| **Performance Degradation** | Low | Medium | Low | Caching compiled rules |
| **User Confusion** | Medium | Low | Low | Clear UI indicators + documentation |
| **Rollback Needed** | Low | Medium | Low | One-click toggle to manual mode |
| **Orphaned Mappings** | Low | Low | Low | Cascade delete with confirmation |

### 4.2 Detailed Risk Mitigations

#### Risk 1: Incomplete Mappings Leading to Failed Calculations

**Problem:** User activates ERP Mapping approach but not all required fields are mapped.

**Mitigation Strategy:**
1. **Readiness Check System**
   - Before allowing activation, system validates:
     - All required target fields have source mappings
     - All transformation rules are complete
     - Sample data passes validation
   - Display checklist with ✓/✗ status

2. **Graceful Degradation**
   - If a mapping is missing during calculation, log warning
   - Fall back to default values where possible
   - Never fail silently - always surface issues

3. **Pre-Flight Validation**
   ```
   ┌─────────────────────────────────────────────────┐
   │  Activation Readiness Check                     │
   ├─────────────────────────────────────────────────┤
   │  ✓ item_number mapping complete                 │
   │  ✓ description mapping complete                 │
   │  ✗ license_fee_rate mapping MISSING             │
   │  ✓ vendor_id mapping complete                   │
   ├─────────────────────────────────────────────────┤
   │  Status: NOT READY - 1 required mapping missing │
   │  [Cannot Activate]                              │
   └─────────────────────────────────────────────────┘
   ```

#### Risk 2: Calculation Accuracy Concerns

**Problem:** ERP-derived rules may produce different results than manual rules.

**Mitigation Strategy:**
1. **Preview Mode**
   - Test rule set against sample sales data before activation
   - Compare results side-by-side with expected values
   - Show detailed breakdown of each calculation step

2. **Parallel Run Period**
   - Option to run BOTH approaches simultaneously
   - Compare outputs in report
   - Identify discrepancies before switching

3. **Audit Trail**
   - Every calculation logs which rules fired
   - Full input/output capture for debugging
   - Traceable back to source mapping

4. **Validation Rules**
   - Set acceptable variance thresholds
   - Alert if calculation exceeds expected range
   - Automatic hold for review if anomaly detected

#### Risk 3: Breaking Existing Functionality

**Problem:** Changes might affect current manual rules workflow.

**Mitigation Strategy:**
1. **Complete Isolation**
   - New tables only (no schema modifications)
   - New API endpoints only (no changes to existing)
   - Separate service layer for ERP rules

2. **Feature Flag Control**
   - Organization-level toggle (default: OFF)
   - Contract-level override option
   - Admin-only configuration access

3. **Shared Output Only**
   - Both approaches write to same `contractRoyaltyCalculations`
   - Downstream systems (invoicing, reporting) unchanged
   - Single source of truth for results

---

## 5. Calculation Accuracy Guarantees

### 5.1 How Accuracy Is Ensured

| Layer | Accuracy Mechanism |
|-------|-------------------|
| **Mapping** | AI suggests → Human confirms → System validates |
| **Transformation** | Configurable rules with type checking |
| **Calculation** | Same calculation engine as manual approach |
| **Output** | Same output format, same validation |
| **Audit** | Full traceability from source to result |

### 5.2 Calculation Flow Comparison

#### Manual Approach (Current)
```
Sales CSV → Parse → Match Contract → Apply Manual Rules → Calculate → Store Result
                                         │
                                         └─ Rules created by user
                                            May have human error
                                            Updates require manual changes
```

#### ERP Mapping Approach (New)
```
Sales CSV → Parse → Match Contract → Resolve Mappings → Apply ERP Rules → Calculate → Store Result
                                         │                    │
                                         │                    └─ Rules derived from
                                         │                       confirmed mappings
                                         │                       Consistent with ERP
                                         │
                                         └─ Uses master data for lookups
                                            Validates against known values
```

### 5.3 Data Validation Points

| Stage | Validation | Action on Failure |
|-------|------------|-------------------|
| Sales Upload | CSV format, required columns | Reject with error message |
| Field Mapping | Data type compatibility | Transformation or skip |
| Value Lookup | Master data exists | Use default or flag |
| Calculation | Numeric validity | Log warning, continue |
| Output | Result within bounds | Flag for review |

---

## 6. ERP Term Mapping Clarity

### 6.1 Dual Terminology Display Format

Throughout the system, we maintain **clear visibility** of both ERP and LicenseIQ terminology:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Field Mapping Display Format                                        │
│                                                                      │
│  Contract Term: "Net Sales Amount"                                   │
│  ERP Field: REVENUE_AMOUNT                                           │
│  LicenseIQ Field: net_revenue                                        │
│                                                                      │
│  Display: "Net Sales Amount (ERP: REVENUE_AMOUNT → LIQ: net_revenue)"│
└─────────────────────────────────────────────────────────────────────┘
```

### 6.2 Mapping Traceability

Every calculated value can trace back to its source:

```
Calculation Result: $1,250.00 License Fee

Trace:
├── Source: Sales Record #12345
│   └── Quantity: 500 units
│
├── Mapping Applied:
│   ├── ERP Field: UNIT_LICENSE_FEE → LicenseIQ: license_fee_rate
│   └── Value: $2.50 per unit
│
├── Rule Applied: "Standard License Fee Calculation"
│   ├── Rule Set: Oracle Fusion → Items Mapping v2
│   ├── Formula: quantity × license_fee_rate
│   └── Result: 500 × $2.50 = $1,250.00
│
└── Master Data Reference:
    ├── Item: ACER-AF-001 (Aurora Flame Maple)
    ├── Vendor: Green Innovation Genetics LLC
    └── Contract: PLV-2024-001
```

### 6.3 Mapping Confirmation Workflow

```
┌─────────────────────────────────────────────────────────────────────┐
│  Step 1: AI Generates Suggested Mappings                            │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ ItemNumber     →  item_number        [95% confidence] [Confirm] ││
│  │ ItemDescription→  full_legal_product_name [87%]       [Confirm] ││
│  │ ItemClass      →  item_category      [72% confidence] [Review]  ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  Step 2: Human Reviews & Confirms                                    │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ ✓ Confirmed: ItemNumber → item_number                           ││
│  │ ✓ Confirmed: ItemDescription → full_legal_product_name          ││
│  │ ✓ Modified:  ItemClass → item_category (was item_class)         ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  Step 3: System Creates ERP Mapping Rules                           │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ Rule Set "Oracle → Items" created with 3 rules                  ││
│  │ Ready for activation                                            ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

---

## 7. Implementation Phases

### Phase 1: Database Schema (2-3 days)
- [ ] Create migration for new tables
- [ ] Add Drizzle schema definitions
- [ ] Create insert/select types
- [ ] Add storage interface methods
- [ ] Write unit tests for CRUD

### Phase 2: API Layer (3-4 days)
- [ ] ERP Mapping Rule Sets CRUD endpoints
- [ ] ERP Mapping Rules CRUD endpoints
- [ ] Organization settings endpoints
- [ ] Preview/test endpoints
- [ ] Activation/deactivation endpoints
- [ ] Zod validation schemas
- [ ] API documentation

### Phase 3: UI Implementation (4-5 days)
- [ ] ERP Mapping Rules list page
- [ ] Rule Set editor page
- [ ] Rule configuration dialogs
- [ ] Transformation rule builder
- [ ] Preview/test modal
- [ ] Configuration settings panel
- [ ] Navigation integration

### Phase 4: Calculation Integration (3-4 days)
- [ ] Extend sales upload flow
- [ ] Add approach routing logic
- [ ] Implement ERP rule executor
- [ ] Add fallback handling
- [ ] Implement audit logging
- [ ] Performance optimization

### Phase 5: Testing & Validation (2-3 days)
- [ ] Unit tests for all new code
- [ ] Integration tests for calculation
- [ ] End-to-end workflow testing
- [ ] Performance testing
- [ ] User acceptance testing

---

## 8. Success Criteria

| Criterion | Measurement | Target |
|-----------|-------------|--------|
| **Backward Compatibility** | Existing workflows unchanged | 100% |
| **Calculation Accuracy** | Results match expected values | 99.9% |
| **Performance** | Calculation time | < 2 seconds |
| **Traceability** | Audit trail completeness | 100% |
| **User Adoption** | Organizations using ERP approach | Gradual |

---

## 9. Approval & Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Product Owner | | | |
| Technical Lead | | | |
| QA Lead | | | |
| Operations | | | |

---

## 10. Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| **ERP** | Enterprise Resource Planning system (Oracle, SAP, etc.) |
| **LicenseIQ** | Our internal schema for contract/license management |
| **Master Data Mapping** | Confirmed field-level mappings between ERP and LicenseIQ |
| **Rule Set** | A collection of related transformation rules |
| **Transformation** | The process of converting ERP values to LicenseIQ values |

### B. Related Documents

- Master Data Mapping User Guide
- License Fee Rules Configuration Guide
- Sales Upload Process Documentation
- API Reference Documentation

---

**Document Control:**
- Created: January 6, 2026
- Last Modified: January 6, 2026
- Next Review: Before implementation start
