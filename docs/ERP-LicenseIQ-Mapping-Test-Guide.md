# ERP to LicenseIQ Mapping - End-to-End Test Guide

This document describes how to test the complete workflow from Oracle Fusion Cloud ERP data through to the dual terminology display in contract term mappings.

---

## Overview

**Workflow Flow:**
```
Oracle Fusion Cloud ERP → LicenseIQ Schema → Contract PDF Extraction → AI Mapping → Master Data Lookup → Dual Terminology Display
```

**Expected Output Format:**
```
"Green Valley Nurseries (LicenseIQ Vendors: V-1001)"
```

---

## Pre-Configured Test Data

### 1. Oracle Fusion Cloud ERP System
- **System ID:** `03c4a661-b078-4e6c-bde2-3d32dda784c2`
- **System Name:** Oracle Fusion Cloud

### 2. Field Mappings (15 total)

| Oracle Fusion Entity | Oracle Field | LicenseIQ Entity | LicenseIQ Field |
|---------------------|--------------|------------------|-----------------|
| Items | ItemNumber | Items | item_number |
| Items | ItemDescription | Items | description |
| Items | ItemClass | Items | item_class |
| Items | ItemType | Items | item_type |
| Items | ItemStatus | Items | item_status |
| Items | PrimaryUOMCode | Items | uom |
| Items | ListPrice | Items | price_tier |
| Suppliers | SupplierName | Vendors | vendor_name |
| Suppliers | SupplierNumber | Vendors | vendor_number |
| Suppliers | SupplierType | Vendors | vendor_type |
| Suppliers | Status | Vendors | vendor_status |
| Suppliers | PaymentTermsName | Vendors | payment_terms |
| Suppliers | PaymentMethodCode | Vendors | payment_method |
| Suppliers | PaymentCurrencyCode | Vendors | currency |
| Suppliers | TaxRegistrationNumber | Vendors | tax_id_number |

### 3. Master Data (Monrovia Nursery Company)
- **Organization ID:** `monrovia-branded`
- **Company:** Monrovia Nursery Company

**Vendors (4 records):**
| Vendor Number | Vendor Name | Payment Terms |
|---------------|-------------|---------------|
| V-1001 | Green Valley Nurseries | Net 30 |
| V-1002 | Pacific Coast Growers | Net 45 |
| V-1003 | SunGro Horticulture | Net 30 |
| V-1004 | FertilizerPro Inc | Net 60 |

**Items (5 records):**
| Item Number | Description | Item Class |
|-------------|-------------|------------|
| PLANT-001 | Premium Rose Bush - Red | Live Plants |
| PLANT-002 | Dwarf Citrus Tree - Meyer Lemon | Live Plants |
| PLANT-003 | Japanese Maple - Bloodgood | Live Plants |
| SOIL-001 | Premium Potting Mix | Growing Media |
| FERT-001 | All-Purpose Plant Food | Fertilizers |

---

## Test Procedures

### Step 1: Verify ERP-to-LicenseIQ Field Mappings

**API Call:**
```bash
GET /api/erp-licenseiq-mappings?erpSystemId=03c4a661-b078-4e6c-bde2-3d32dda784c2
```

**Expected Response:**
```json
{
  "mappings": [
    {
      "erpSystemId": "03c4a661-b078-4e6c-bde2-3d32dda784c2",
      "erpSystemName": "Oracle Fusion Cloud",
      "erpEntityName": "Items",
      "erpFieldName": "ItemNumber",
      "licenseiqEntityName": "Items",
      "licenseiqFieldName": "item_number",
      "mappingType": "direct"
    }
    // ... 14 more mappings
  ],
  "count": 15
}
```

### Step 2: Test Dual Terminology Display Function

**API Call:**
```bash
POST /api/erp-licenseiq-mappings/test-dual-terminology
Content-Type: application/json

{
  "contractTerm": "Green Valley Nurseries",
  "licenseiqEntityName": "Vendors",
  "masterDataValue": "V-1001"
}
```

**Expected Response:**
```json
{
  "display": "Green Valley Nurseries (LicenseIQ Vendors: V-1001)",
  "components": {
    "contractTerm": "Green Valley Nurseries",
    "licenseiqEntityName": "Vendors",
    "masterDataValue": "V-1001",
    "erpFieldName": null
  }
}
```

### Step 3: Test Master Data Lookup with Organization Context

**API Call:**
```bash
POST /api/erp-licenseiq-mappings/lookup
Content-Type: application/json

{
  "contractTerm": "Green Valley",
  "licenseiqEntityId": "<vendors-entity-id>",
  "orgId": "monrovia-branded"
}
```

**Expected Response:**
```json
{
  "display": "Green Valley (LicenseIQ Vendors: V-1001)",
  "matched": true,
  "matchedRecord": {
    "recordId": "...",
    "matchedValue": "Green Valley Nurseries",
    "matchedField": "vendor_name",
    "recordData": {
      "vendor_number": "V-1001",
      "vendor_name": "Green Valley Nurseries",
      "vendor_type": "Wholesale",
      "payment_terms": "Net 30"
    }
  },
  "entityName": "Vendors"
}
```

### Step 4: Full End-to-End Contract Processing Test

1. **Upload a Contract PDF** containing vendor/item references
   - Example: A contract mentioning "Green Valley Nurseries" as a supplier

2. **Run AI Extraction** on the uploaded contract
   - The system extracts terms like vendor names, item descriptions

3. **View Pending Term Mappings** for the contract
   - Navigate to the contract detail page
   - Check the "ERP Field Mappings" section

4. **Verify Dual Terminology Display**
   - Contract Term column should show:
     ```
     Green Valley Nurseries
     (LicenseIQ Vendors: V-1001)
     ```
   - The green text indicates a successful master data match

---

## Configuration: Adding New Mappings

### Adding New ERP Field Mappings

Run the following SQL to add a new mapping:

```sql
INSERT INTO erp_licenseiq_field_mappings (
  id, erp_system_id, erp_entity_id, erp_field_id,
  licenseiq_entity_id, licenseiq_field_id, mapping_type
)
SELECT 
  gen_random_uuid()::text,
  '03c4a661-b078-4e6c-bde2-3d32dda784c2',  -- Oracle Fusion Cloud
  e.id,      -- ERP entity ID
  f.id,      -- ERP field ID
  le.id,     -- LicenseIQ entity ID
  lf.id,     -- LicenseIQ field ID
  'direct'
FROM erp_entities e
JOIN erp_fields f ON f.entity_id = e.id
JOIN licenseiq_entities le ON le.technical_name = 'your_entity'
JOIN licenseiq_fields lf ON lf.entity_id = le.id
WHERE e.name = 'YourErpEntity'
  AND f.field_name = 'YourErpField'
  AND lf.field_name = 'your_licenseiq_field';
```

### Adding New Master Data Records

```sql
INSERT INTO licenseiq_entity_records (
  id, entity_id, org_id, record_data, lookup_value, display_value
)
SELECT 
  gen_random_uuid()::text,
  e.id,
  'monrovia-branded',
  '{"vendor_number": "V-1005", "vendor_name": "New Vendor Inc", "payment_terms": "Net 30"}'::jsonb,
  'V-1005',
  'New Vendor Inc'
FROM licenseiq_entities e
WHERE e.technical_name = 'vendors';
```

---

## Troubleshooting

### No Dual Terminology Displayed

1. **Check LicenseIQ entity mapping exists:**
   ```sql
   SELECT * FROM erp_licenseiq_field_mappings 
   WHERE erp_system_id = '03c4a661-b078-4e6c-bde2-3d32dda784c2';
   ```

2. **Check master data exists for organization:**
   ```sql
   SELECT * FROM licenseiq_entity_records 
   WHERE org_id = 'monrovia-branded';
   ```

3. **Verify organization context is set:**
   - Ensure the user is logged in with the correct business unit context

### Master Data Not Matching

1. **Check search value format:**
   - The lookup performs case-insensitive partial matching
   - Ensure the contract term contains enough characters to match

2. **Verify org_id filter:**
   - Master data is filtered by organization
   - Cross-org lookups will return no results

---

## API Reference Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/erp-licenseiq-mappings` | GET | Get all ERP-to-LicenseIQ field mappings |
| `/api/erp-licenseiq-mappings/test-dual-terminology` | POST | Test dual terminology display format |
| `/api/erp-licenseiq-mappings/lookup` | POST | Lookup master data and generate display |

---

## Database Tables Reference

| Table | Purpose |
|-------|---------|
| `erp_systems` | ERP system definitions (Oracle Fusion, SAP, etc.) |
| `erp_entities` | ERP data entities (Items, Suppliers, etc.) |
| `erp_fields` | ERP field definitions within entities |
| `licenseiq_entities` | LicenseIQ schema entities (Items, Vendors) |
| `licenseiq_fields` | LicenseIQ field definitions |
| `erp_licenseiq_field_mappings` | Mappings between ERP and LicenseIQ fields |
| `licenseiq_entity_records` | Imported master data with org context |
| `pending_term_mappings` | AI-extracted contract terms awaiting confirmation |

---

## Frontend Step-by-Step Testing Guide

This section provides detailed instructions for testing the ERP-to-LicenseIQ mapping workflow through the application's user interface.

### Prerequisites

1. **Login Credentials:** System Admin or Company Admin account
2. **Organization Context:** Select "Monrovia Nursery Company" → "Monrovia Branded" business unit
3. **Test Contract PDF:** A PDF containing vendor names (e.g., "Green Valley Nurseries") or product names

---

### Step 1: Login and Set Organization Context

1. **Navigate to the login page**
   - URL: `https://your-app-url.replit.app`

2. **Enter credentials**
   - Username: `admin` (or your admin account)
   - Password: Your password

3. **Select organization context** (if prompted)
   - Company: `Monrovia Nursery Company`
   - Business Unit: `Monrovia Branded`
   - Location: Any available location (e.g., `Dayton OR (HQ)`)

4. **Verify context in header**
   - The navigation bar should show your current organization context

---

### Step 2: Verify Master Data Configuration

#### 2a. Check LicenseIQ Entity Records

1. **Navigate to:** `Settings` → `LicenseIQ Schema` → `Entity Data Browser`

2. **Select entity:** Choose `Vendors` from the entity dropdown

3. **Verify records exist:**
   - You should see 4 vendor records for Monrovia Branded:
     - V-1001: Green Valley Nurseries
     - V-1002: Pacific Coast Growers
     - V-1003: SunGro Horticulture
     - V-1004: FertilizerPro Inc

4. **Select entity:** Choose `Items` from the entity dropdown

5. **Verify records exist:**
   - You should see 5 item records:
     - PLANT-001, PLANT-002, PLANT-003, SOIL-001, FERT-001

#### 2b. Check ERP System Configuration

1. **Navigate to:** `Settings` → `ERP Integration Hub`

2. **Verify Oracle Fusion Cloud** is listed as a configured ERP system

3. **Click on Oracle Fusion Cloud** to view entities and field mappings

4. **Verify mappings:**
   - Items entity: 7 field mappings
   - Suppliers entity: 8 field mappings

---

### Step 3: Upload a Test Contract

1. **Navigate to:** `Contracts` → `All Contracts`

2. **Click:** `+ New Contract` or `Upload Contract` button

3. **Upload a PDF** containing vendor/supplier references
   - Example content in the PDF:
     ```
     This agreement is between Monrovia Nursery Company 
     and Green Valley Nurseries for the supply of...
     
     Products covered:
     - Premium Rose Bush (Red variety)
     - Japanese Maple - Bloodgood
     ```

4. **Fill in contract metadata:**
   - Contract Name: `Test Vendor Agreement - Green Valley`
   - Contract Type: Select appropriate type
   - Effective Date: Today's date
   - Business Unit: `Monrovia Branded`

5. **Click:** `Save` or `Upload`

---

### Step 4: Run AI Extraction

1. **Open the uploaded contract** by clicking on it in the contract list

2. **Locate the AI Analysis section** (usually on the right panel or in a tab)

3. **Click:** `Run AI Extraction` or `Analyze Contract` button

4. **Wait for processing** (typically 10-30 seconds)
   - You'll see a loading indicator while the AI processes the document

5. **Verify extraction completed:**
   - Status should change to "Completed" or show a success message
   - Extracted terms should appear in the analysis section

---

### Step 5: View and Confirm Pending Term Mappings

1. **Navigate to the contract analysis page:**
   - Go to `Contracts` → Click on a contract to open it
   - Scroll down past "AI Summary", "Extracted Terms", "Risk Analysis", and "AI Insights" sections
   - You will see the **"ERP Field Mappings"** section

2. **Review the pending mappings table:**

   | Contract Term | → | Mapped Field | Confidence | Status | Actions |
   |--------------|---|--------------|------------|--------|---------|
   | **Green Valley Nurseries** | → | SupplierName | 85% | Pending | ✓ ✗ ✎ |
   | *(LicenseIQ Vendors: V-1001)* | | Suppliers | | | |

3. **Verify dual terminology display:**
   - The Contract Term column should show:
     - **Primary text (bold):** The extracted term (e.g., "Green Valley Nurseries")
     - **Secondary text (green):** The LicenseIQ mapping with master data value
       - Format: `(LicenseIQ Vendors: V-1001)`

4. **If dual terminology is NOT showing:**
   - The term may not have matched any master data
   - Check that the vendor name in the contract matches the master data spelling

---

### Step 6: Confirm or Modify Mappings

#### 6a. Confirm a Correct Mapping

1. **Find a mapping with high confidence** (80%+ shown in green)

2. **Click the checkmark (✓) button** to confirm the mapping

3. **Verify status changes** from "Pending" to "Confirmed" (green badge)

#### 6b. Modify an Incorrect Mapping

1. **Find a mapping that needs correction**

2. **Click the edit (✎) button**

3. **A dropdown appears** - select the correct ERP field from the list

4. **Click:** `Save & Confirm`

5. **Verify the mapping updates** with the new field and status becomes "Modified"

#### 6c. Reject an Invalid Mapping

1. **Find a mapping that should not be used**

2. **Click the X button** to reject

3. **Verify status changes** to "Rejected" (red badge)

#### 6d. Bulk Confirm Multiple Mappings

1. **Check the boxes** next to multiple pending mappings

2. **Click:** `Confirm Selected (N)` button at the top

3. **All selected mappings** change to "Confirmed" status

---

### Step 7: Verify in License Fee Rules (Optional)

After confirming mappings, the dual terminology appears in license fee rules:

1. **Navigate to:** The contract's `License Fee Rules` section

2. **View or create a rule** that references the confirmed vendor/item

3. **Verify the display shows:**
   ```
   Vendor: Green Valley Nurseries (LicenseIQ Vendors: V-1001)
   ```

---

### Expected Results Summary

| Step | What to Check | Expected Result |
|------|--------------|-----------------|
| Login | Organization context | Shows "Monrovia Nursery Company" |
| Master Data | Entity records | 4 Vendors, 5 Items visible |
| ERP Config | Field mappings | 15 Oracle Fusion mappings |
| Upload | Contract saved | Appears in contract list |
| AI Extraction | Analysis status | "Completed" with extracted terms |
| Mappings | Dual terminology | Green text showing "(LicenseIQ Entity: Value)" |
| Confirm | Status change | "Pending" → "Confirmed" |

---

### Screenshots Reference

**Pending Term Mappings with Dual Terminology:**
```
┌─────────────────────────────────────────────────────────────────┐
│  ERP Field Mappings                                    [Refresh]│
├─────────────────────────────────────────────────────────────────┤
│  ☐ │ Contract Term              │ → │ Mapped Field  │ Conf │ St│
├─────────────────────────────────────────────────────────────────┤
│  ☐ │ Green Valley Nurseries     │ → │ SupplierName  │ 85%  │ ⏳│
│    │ (LicenseIQ Vendors: V-1001)│   │ Suppliers     │      │   │
├─────────────────────────────────────────────────────────────────┤
│  ☐ │ Premium Rose Bush          │ → │ ItemDesc      │ 78%  │ ⏳│
│    │ (LicenseIQ Items: PLANT-001)   │ Items         │      │   │
└─────────────────────────────────────────────────────────────────┘
```

**Color Legend:**
- 🟢 **Green text:** Successfully matched to master data
- 🔵 **Blue text:** Mapped to LicenseIQ entity but no master data match
- ⚪ **Gray text:** ERP field only (no LicenseIQ mapping)

---

### Common Issues and Solutions

| Issue | Possible Cause | Solution |
|-------|---------------|----------|
| No dual terminology shown | Master data not imported | Check Entity Data Browser for records |
| Wrong vendor matched | Partial name match | Modify mapping to correct field |
| AI extraction failed | PDF not readable | Re-upload cleaner PDF scan |
| Mappings not appearing | Wrong organization context | Switch to correct Business Unit |
| Low confidence scores | Ambiguous terms in contract | Review and manually confirm mappings |

---

## Technical Implementation Details

### How ERP Field Mappings Are Created

The ERP Field Mappings are created automatically during contract processing:

1. **Contract Upload** → PDF is saved and processing starts
2. **AI Analysis** → Groq AI extracts summary, key terms, risk analysis
3. **License Fee Rules Extraction** → AI extracts royalty/license fee rules
4. **ERP Field Mapping** → `extractWithErpMapping()` function is called:
   - Extracts vendor names, product names, and other entities from contract text
   - Maps extracted terms to configured ERP fields using AI
   - Creates records in `pending_term_mappings` table
   - Auto-confirms high-confidence matches (>85% confidence)
5. **Embeddings Generation** → For semantic search
6. **Status Update** → Contract marked as "analyzed"

### Key Code Location

```
server/routes.ts:
  - processContractAnalysis() function (line ~9650)
  - Calls extractWithErpMapping() after royalty rules are saved

server/services/zeroShotExtractionService.ts:
  - extractWithErpMapping() function
  - Maps contract terms to ERP fields using AI
  - Creates pending_term_mappings records
```

### Triggering ERP Mapping for Existing Contracts

For contracts that were analyzed BEFORE this fix was implemented:

1. **Click "Reprocess"** button on the contract analysis page
2. This will re-run the full analysis including ERP field mapping
3. Check the "ERP Field Mappings" section after reprocessing completes
