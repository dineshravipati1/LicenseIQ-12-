# 🧪 LicenseIQ Platform - Complete End-to-End Testing Guide

## 📋 Overview
This comprehensive guide walks you through **EVERY major feature** of the LicenseIQ platform in a single end-to-end scenario. You'll test the complete contract-to-payment workflow using AI-powered analysis, semantic matching, and automated calculations.

## 🎯 What You'll Test
1. ✅ **Contract Import** - Upload and AI analysis
2. ✅ **Contract Metadata Management** - Edit and approve metadata
3. ✅ **ERP Catalog Setup** - Configure ERP systems, entities, and fields
4. ✅ **Master Data Mapping** - AI field mapping with human review
   - **NEW!** Batch Auto-Map - Process multiple entities 10x faster
5. ✅ **Toggle Configuration** - Enable/disable ERP semantic matching
6. ✅ **ERP Data Import** - Import master product data with embeddings
7. ✅ **Rules Management** - Add, edit, delete royalty rules
8. ✅ **Sales Data Upload** - Traditional and ERP-enabled uploads
9. ✅ **Royalty Calculation** - Calculate payments using rules engine
10. ✅ **Contract Q&A** - RAG-powered document queries

## 📦 Test Files Provided

### **erp_master_data_sample.csv** - 15 nursery products (ERP master data)
**Columns:** productCode, productName, category, territory, description, unitCost, standardPrice

Sample row:
```
MAPLE-001,Aurora Flame Maple,Ornamental Trees,Primary,Premium ornamental maple tree...,12.50,25.00
```

### **sales_data_sample.csv** - 15 sales transactions
**Columns:** transactionDate, transactionId, productCode, productName, category, territory, containerSize, season, currency, grossAmount, netAmount, quantity, unitPrice

Sample row:
```
2024-03-15,TXN-2024-001,MAPLE-001,Aurora Flame Maple,Ornamental Trees,Primary,1-gallon,Spring,USD,30000,27000,6200,4.84
```

### **Contract PDF**
Use existing Plant Variety License contract or upload your own

## ⏱️ Estimated Time
**Total**: 30-45 minutes for complete end-to-end testing

---

# 🚀 COMPLETE END-TO-END TESTING WORKFLOW

---

## **MODULE 1: CONTRACT IMPORT & ANALYSIS** 📄

### **STEP 1.1: Upload Contract**

**Goal**: Import a contract and trigger AI analysis

**Steps:**
1. **Navigate to**: Dashboard (/) or Upload page (/upload)
2. **Click**: "Upload Contract" button (blue button, top right on Dashboard)
3. **Select File**: Choose any contract PDF
   - Use the existing "Plant Variety License & Royalty Agreement" if available
   - Or upload your own contract PDF
4. **Wait for Upload**: File uploads to server
5. **Wait for AI Analysis**: 
   - Status shows "Analyzing..."
   - Progress indicator appears
   - Analysis takes ~20-30 seconds (uses Groq LLaMA 3.1)

**Expected Results:**
```
✅ Upload successful
✅ Status changes to "Analyzed"
✅ Contract appears in dashboard
✅ AI extracted data visible:
   - Contract type
   - Parties (licensor/licensee)
   - Effective date
   - Payment terms
   - Key clauses
```

**Screenshot Location**: Dashboard - Contract cards

---

### **STEP 1.2: View Contract Analysis**

**Goal**: Review AI-extracted contract data

**Steps:**
1. **Navigate to**: Dashboard → Click on your **contract card**
2. **Review extracted data**:
   - **Parties**: Licensor and Licensee names
   - **Effective Date**: Contract start date
   - **Contract Type**: License, Service, SaaS, etc.
   - **Payment Terms**: Royalty rates, minimums, payment schedule
   - **Risk Assessment**: AI-identified risks (High/Medium/Low)
   - **AI Insights**: Key recommendations
   - **Dynamic Extraction**: Entities and relationships extracted

**Expected Results:**
```
✅ Contract analysis page loads
✅ All sections populated with AI data
✅ Parties, dates, and terms displayed
✅ Payment rules extracted (if applicable)
✅ Risk assessment shown
```

---

### **STEP 1.3: Manage Contract Metadata**

**Goal**: Review and update contract metadata with version control

**Steps:**
1. **Scroll to**: "Contract Metadata" section on contract analysis page
2. **Click**: "Manage Metadata" button
3. **Review AI-populated fields**:
   - Your Organization: (auto-populated)
   - Counterparty: (auto-populated)
   - Contract Type: (dropdown - 14 types)
   - Display Name: (auto-populated)
   - Effective Date: (auto-populated)
4. **Edit fields** (optional):
   - Update "Your Organization" if incorrect
   - Modify "Counterparty" name
   - Change contract type if needed
   - Set custom display name
5. **Click**: "Save Changes"
6. **Review version history**: See all metadata changes with editor info

**Approval Workflow (Optional):**
7. **Click**: "Submit for Approval" button
8. **Status changes**: Draft → Pending Approval
9. **Admin/Owner approves**: Pending → Approved
10. **Active metadata**: Approved version becomes active

**Expected Results:**
```
✅ Metadata form displays with AI-populated values
✅ All fields editable
✅ Save creates new version
✅ Version history shows all changes
✅ Approval workflow (if admin/owner)
✅ Active metadata displayed on contract
```

---

## **MODULE 2: ERP CATALOG SETUP** 🗄️

### **STEP 2.1: View Pre-Configured ERP Systems**

**Goal**: Understand ERP catalog structure

**Steps:**
1. **Navigate to**: Sidebar → "ERP Catalog" (💾 database icon)
2. **View "ERP Systems" tab**:
   - See 5 pre-configured systems:
     - Oracle EBS 12.2
     - Oracle Fusion Cloud
     - SAP S/4HANA
     - SAP ECC 6.0
     - NetSuite 2024.1
3. **View system details**:
   - Vendor (Oracle, SAP, NetSuite)
   - System name and version
   - Number of configured entities

**Expected Results:**
```
✅ ERP Catalog page loads
✅ 5 pre-configured ERP systems visible
✅ System details displayed (Oracle, SAP, NetSuite)
✅ No errors
```

---

### **STEP 2.2: View ERP Entities**

**Goal**: See available data entities (tables) for each ERP

**Steps:**
1. **Click**: "Entities" tab
2. **Review Oracle entities** (14 pre-configured):
   - Customers (AR_CUSTOMERS)
   - Items (MTL_SYSTEM_ITEMS_B)
   - Invoices (RA_CUSTOMER_TRX_ALL)
   - Suppliers (PO_VENDORS)
   - Purchase Orders (PO_HEADERS_ALL)
   - Sales Orders (OE_ORDER_HEADERS_ALL)
   - Inventory Transactions (MTL_MATERIAL_TRANSACTIONS)
   - GL Accounts (GL_CODE_COMBINATIONS)
   - And more...
3. **Note**: Entities are organized by ERP system

**Expected Results:**
```
✅ Entities tab shows all configured entities
✅ 14+ Oracle entities visible
✅ Each entity shows:
   - Name (e.g., "Customers")
   - Technical name (e.g., "AR_CUSTOMERS")
   - Entity type (master_data/transactional)
   - ERP system link
```

---

### **STEP 2.3: View ERP Fields**

**Goal**: See field definitions for entities

**Steps:**
1. **Click**: "Fields" tab
2. **Review Oracle customer fields** (10+ pre-configured):
   - CUSTOMER_ID (NUMBER)
   - CUSTOMER_NAME (VARCHAR2(240))
   - CUSTOMER_NUMBER (VARCHAR2(30))
   - CREATION_DATE (DATE)
   - STATUS (VARCHAR2(1))
   - And more...
3. **Use search**: Filter fields by name or entity

**Expected Results:**
```
✅ Fields tab shows all configured fields
✅ 30 pre-seeded fields across 3 entities:
   - Customers: 10 fields (PARTY_ID, PARTY_NAME, EMAIL_ADDRESS, etc.)
   - Suppliers: 10 fields (VENDOR_ID, VENDOR_NAME, TAX_ID, etc.)
   - Items: 10 fields (INVENTORY_ITEM_ID, ITEM_NUMBER, DESCRIPTION, etc.)
✅ Each field shows:
   - Field name
   - Data type
   - Entity it belongs to
   - ERP system link
   - Primary key indicator
```

---

### **STEP 2.4: Add Custom ERP Entity (Optional)**

**Goal**: Test adding new entity to catalog

**Steps:**
1. **Click**: "Entities" tab
2. **Click**: "Add Entity" button
3. **Fill form**:
   - ERP System: Select "Oracle EBS 12.2"
   - Entity Name: "Product Categories"
   - Technical Name: "MTL_CATEGORIES_B"
   - Entity Type: "master_data"
   - Description: "Product category master data"
4. **Click**: "Create Entity"
5. **Verify**: New entity appears in list

**Expected Results:**
```
✅ Add Entity dialog opens
✅ Form validation works
✅ New entity created successfully
✅ Entity appears in catalog
✅ Toast notification shown
```

---

## **MODULE 3: MASTER DATA MAPPING** 🔗

### **STEP 3.1: Generate AI Field Mapping (Single Entity)**

**Goal**: Create intelligent field mapping between one ERP entity and LicenseIQ entity

**Steps:**
1. **Navigate to**: Sidebar → "Master Data Mapping"
2. **Click**: "Single Mapping" tab (default)
3. **Select ERP System**: Oracle EBS (from dropdown)
4. **Select ERP Entity**: Items (from dropdown - shows "Items (master_data)")
5. **Wait**: Source Schema auto-populates from Items entity
6. **Select LicenseIQ Entity**: Products (Master Data) (from dropdown)
7. **Wait**: Target Schema auto-populates from Products entity
8. **Review auto-populated schemas**:
   
   **Source Schema (Oracle Items):**
   ```json
   {
     "CATEGORY_SET_NAME": "VARCHAR2(30)",
     "DESCRIPTION": "VARCHAR2(240)",
     "INVENTORY_ASSET_FLAG": "VARCHAR2(1)",
     "INVENTORY_ITEM_ID": "NUMBER",
     "ITEM_NUMBER": "VARCHAR2(40)",
     "ITEM_TYPE": "VARCHAR2(30)",
     "LIST_PRICE": "NUMBER",
     "PRIMARY_UOM_CODE": "VARCHAR2(3)",
     "STATUS": "VARCHAR2(15)",
     "UNIT_WEIGHT": "NUMBER"
   }
   ```
   
   **Target Schema (LicenseIQ Products):**
   ```json
   {
     "category": "string",
     "isActive": "boolean",
     "listPrice": "number",
     "manufacturer": "string",
     "productCode": "string",
     "productName": "string",
     "royaltyRate": "number",
     "subcategory": "string",
     "territory": "string",
     "unitOfMeasure": "string"
   }
   ```

9. **Click**: "Generate AI Mapping" (🪄 button)
10. **Wait**: AI processes (5-10 seconds)

**Expected Results:**
```
✅ AI generates field mappings
✅ Mapping table displays results:

Source Field (Oracle) → Target Field (LicenseIQ) | Confidence
──────────────────────────────────────────────────────────────
ITEM_NUMBER           → productCode              | 98% 🟢
DESCRIPTION           → productName              | 98% 🟢
CATEGORY_SET_NAME     → category                 | 95% 🟢
LIST_PRICE            → listPrice                | 100% 🟢
PRIMARY_UOM_CODE      → unitOfMeasure            | 92% 🟢
STATUS                → isActive                 | 85% 🟡
INVENTORY_ITEM_ID     → productCode              | 75% 🟡

Statistics:
High Confidence (≥90%): 5 mappings ✅
Medium Confidence (70-89%): 2 mappings ⚠️
Requires Review (<70%): 0 mappings
```

---

### **STEP 3.2: Human Review & Save Mapping**

**Goal**: Review AI mappings and save for reuse

**Steps:**
1. **Review confidence scores**:
   - ✅ Green badges (≥90%) = High confidence
   - ⚠️ Yellow badges (70-89%) = Medium confidence
   - 🔴 Red badges (<70%) = Requires manual review
2. **Manual correction** (if needed):
   - If any mappings have low confidence
   - Edit source/target schemas
   - Re-generate mapping
3. **Click**: "Save Mapping" button
4. **Fill dialog**:
   - Mapping Name: "Oracle Items to Products"
   - Notes: "Manually reviewed all mappings, verified correct"
5. **Click**: "Save"

**Expected Results:**
```
✅ Mapping saved to database
✅ Success toast notification
✅ Mapping appears in "Saved" tab
```

---

### **💡 IMPORTANT: How Mappings Work with CSV Files**

**Understanding the Connection:**

The mapping you just created (Oracle Items → Products) is a **template** that tells the system how to transform data from your ERP structure to LicenseIQ's standard schema.

**When you import `erp_master_data_sample.csv` in Module 5:**
- CSV has columns: `productCode, productName, category, territory, description, unitCost, standardPrice`
- System uses your mapping to transform these to Products schema: `productCode, productName, category, territory, listPrice, etc.`
- Example: CSV's `standardPrice` → LicenseIQ's `listPrice`

**Key Points:**
✅ Mappings are **reusable templates** (not tied to one file)
✅ CSV column names don't have to match Oracle Items fields exactly
✅ The mapping tells the system: "Column A from CSV → Field B in LicenseIQ"
✅ You'll select your saved mapping when importing the CSV

**This means:**
1. **Step 3.1-3.2**: Create mapping template (Oracle Items → Products)
2. **Step 5.1**: Use that mapping when importing `erp_master_data_sample.csv`
3. **Step 7.1**: Sales CSV matches against imported products using AI embeddings

---

### **STEP 3.3: Test Batch Auto-Map (NEW! 10x Faster)**

**Goal**: Process multiple ERP entities in bulk using AI

**Steps:**
1. **Click**: "Batch Auto-Map" tab
2. **Select ERP System**: Oracle EBS (from dropdown)
3. **Wait**: Entity list loads automatically
4. **Select entities to map** (check boxes):
   - ☑ Items (master_data)
   - ☑ Customers (master_data)
   - ☑ Suppliers (master_data)
   - ☑ GL Accounts (master_data)
   - Or click "Select All" to map all 10 Oracle entities
5. **Click**: "Generate Batch Mappings (4 entities)" button
6. **Wait**: AI processes (30-60 seconds)
   - Shows "Generating batch mappings..." with spinner
   - Processes all 4 entities in parallel
7. **Review results table**:
   
   | ERP Entity | → | LicenseIQ Entity | Confidence | Fields |
   |------------|---|------------------|------------|---------|
   | Items | → | Products | 🟢 95% | 7/10 |
   | Customers | → | Contract Terms | 🟢 92% | 6/10 |
   | Suppliers | → | Contract Terms | 🟡 88% | 5/10 |
   | GL Accounts | → | *(No match)* | 🔴 0% | 0/10 |

8. **Expand row details** (click chevron icon):
   - View AI reasoning: "Items entity contains product catalog..."
   - See field-level mappings
   - Review confidence scores per field
9. **Auto-selection**: High-confidence mappings (≥90%) are pre-selected
10. **Manual review**:
    - Uncheck "GL Accounts" (no match)
    - Keep "Items" and "Customers" (high confidence)
    - Review "Suppliers" (medium confidence - optional)
11. **Click**: "Save Selected (3)" button
12. **Verify**: 3 mappings saved to database

**Expected Results:**
```
✅ Batch generation processes 4 entities
✅ Results table shows all matches
✅ High-confidence auto-selected (Items, Customers)
✅ Medium-confidence available for review (Suppliers)
✅ No-match excluded (GL Accounts)
✅ Expandable details show AI reasoning
✅ Save button shows count (3 selected)
✅ All approved mappings saved successfully
✅ 10x faster than single entity mapping!
```

---

### **STEP 3.4: View and Manage Saved Mappings**

**Goal**: Access saved mappings for reuse

**Steps:**
1. **Click**: "Saved" tab (third tab)
2. **View all saved mappings**:
   - Mapping name
   - ERP system name
   - ERP entity name  
   - LicenseIQ entity name
   - Created by (username)
   - Creation date
3. **Available actions**:
   - 👁️ **View**: See full mapping details in dialog
   - 🗑️ **Delete**: Remove mapping

**Expected Results:**
```
✅ Saved tab loads
✅ All saved mappings displayed (single + batch)
✅ Shows mappings created from both workflows
✅ View shows complete field mappings
✅ Delete removes mapping from database
✅ Can reuse mappings in ERP Data Import
```

---

## **MODULE 4: ERP MATCHING TOGGLE** 🔄

### **STEP 4.1: Enable ERP Semantic Matching**

**Goal**: Turn on AI-powered semantic matching for contract

**Steps:**
1. **Navigate to**: Dashboard → Click your **contract card**
2. **Scroll to**: "ERP Data Matching Configuration" section
3. **Review toggle**:
   - Currently shows: "Traditional Sales Upload" (OFF)
   - Description: "Sales data will be uploaded directly using CSV/Excel files"
4. **Click toggle** to turn ON (purple)
5. **Verify changes**:
   - Label becomes: "✅ ERP Semantic Matching Enabled"
   - Description updates: "Sales will be matched against imported ERP records using AI-powered semantic search"
   - Toggle is purple/active

**Expected Results:**
```
✅ Toggle switches to ON state
✅ Success toast notification
✅ Label and description update
✅ Contract now ERP-enabled
```

---

### **STEP 4.2: Verify Toggle State**

**Goal**: Confirm toggle saved correctly

**Steps:**
1. **Refresh page**: Press F5 or navigate away and back
2. **Scroll to**: "ERP Data Matching Configuration"
3. **Verify**: Toggle still shows ON/enabled state
4. **Check database**: Toggle state persisted

**Expected Results:**
```
✅ Toggle state persists after refresh
✅ Still shows enabled
✅ Data saved to database
```

---

## **MODULE 5: ERP DATA IMPORT** 📤

### **STEP 5.1: Import ERP Master Data**

**Goal**: Import product catalog with AI embeddings

**CSV File Structure (`erp_master_data_sample.csv`):**
```csv
productCode,productName,category,territory,description,unitCost,standardPrice
MAPLE-001,Aurora Flame Maple,Ornamental Trees,Primary,Premium ornamental maple tree...,12.50,25.00
MAPLE-002,Crimson King Maple,Ornamental Trees,Primary,Classic shade tree...,10.00,22.00
```

**Steps:**
1. **Navigate to**: Sidebar → "ERP Data Import"
2. **Select contract**: Choose your ERP-enabled contract
3. **Select mapping**: "Oracle Items to Products" (the mapping you created in Step 3.2)
   - This tells the system how to map CSV columns to LicenseIQ schema
   - Maps: productCode → productCode, productName → productName, standardPrice → listPrice, etc.
4. **Upload file**: Click "Choose File" → Select `erp_master_data_sample.csv`
5. **Preview data**: System shows first 3-5 rows with column headers
6. **Verify columns match**:
   - ✅ productCode
   - ✅ productName
   - ✅ category
   - ✅ territory
   - ✅ description
   - ✅ unitCost
   - ✅ standardPrice
7. **Click**: "Start Import" button
8. **Watch progress**:
   - Status: Processing...
   - Progress bar: "Processing 5 of 15 records" (updates every 2 seconds)
   - Shows embedding generation in progress
9. **Wait**: ~30-60 seconds for 15 records

**Expected Results:**
```
✅ Import job created
✅ Status: Completed
✅ Records processed: 15
✅ Progress: 100%
✅ Import appears in history table

Data Imported (15 products):
- MAPLE-001: Aurora Flame Maple (Ornamental Trees, Primary)
- MAPLE-002: Crimson King Maple (Ornamental Trees, Primary)
- JUNIPER-001: Golden Spire Juniper (Ornamental Shrubs, Secondary)
- JUNIPER-002: Blue Arrow Juniper (Ornamental Shrubs, Primary)
- ROSE-001: Pacific Sunset Rose (Flowering Shrubs, Primary)
- ROSE-002: Crimson Glory Rose (Flowering Shrubs, Primary)
- HOSTA-001: Emerald Crown Hosta (Perennials, Primary)
- HOSTA-002: Royal Standard Hosta (Perennials, Secondary)
- HYDRANGEA-001: Cascade Blue Hydrangea (Flowering Shrubs, Primary)
- HYDRANGEA-002: Pink Diamond Hydrangea (Flowering Shrubs, Secondary)
- AZALEA-001: Flame Azalea (Flowering Shrubs, Primary)
- LILAC-001: Purple Majesty Lilac (Flowering Shrubs, Primary)
- BOXWOOD-001: Green Mountain Boxwood (Evergreen Shrubs, Primary)
- SPRUCE-001: Colorado Blue Spruce (Evergreen Trees, Secondary)
- DOGWOOD-001: Pink Flowering Dogwood (Ornamental Trees, Primary)

✅ Each product has 384-dimensional embedding generated by HuggingFace
✅ Stored in imported_erp_records table with vector search enabled
✅ Linked to contract for semantic matching
```

---

### **STEP 5.2: View Import History**

**Goal**: Review all import jobs

**Steps:**
1. **Stay on**: ERP Data Import page
2. **View "Import History" table**:
   - Import ID
   - Contract name
   - Records processed
   - Status (Completed/Processing/Failed)
   - Progress percentage
   - Created date
3. **Check details**: Click on completed import to view details

**Expected Results:**
```
✅ Import history table populated
✅ Latest import shown at top
✅ Status badges color-coded
✅ All 15 records processed successfully
```

---

## **MODULE 6: RULES MANAGEMENT** ⚖️

### **STEP 6.1: View Existing Rules**

**Goal**: See AI-extracted payment rules

**Steps:**
1. **Navigate to**: Contract analysis page for your contract
2. **Scroll to**: "Payment Rules" section
3. **Click**: "Manage Rules" button (purple button)
4. **View existing rules** (if AI extracted any):
   - Rule name
   - Rule type (tiered_pricing, percentage, etc.)
   - Product categories
   - Territories
   - Active/Inactive status
   - Priority

**Expected Results:**
```
✅ Rules Management page loads
✅ Shows all rules for contract
✅ AI-extracted rules visible (if any)
✅ Manual rules visible (if any)
```

---

### **STEP 6.2: Add New Royalty Rule**

**Goal**: Create custom payment calculation rule

**Steps:**
1. **Click**: "Add New Rule" button (➕ plus icon, top right)
2. **Fill form**:
   
   **Basic Info:**
   - Rule Name: "Spring Ornamental Trees Premium"
   - Description: "Premium rate for ornamental trees sold in spring season"
   - Rule Type: Select "tiered_pricing" from dropdown
   - Priority: 1 (higher priority = evaluated first)
   
   **Product Categories:**
   - Type "Ornamental Trees" → Press Enter
   - Type "Flowering Shrubs" → Press Enter
   - Tags appear as removable chips
   
   **Territories:**
   - Type "Primary" → Press Enter
   - Type "Secondary" → Press Enter
   
   **Container Sizes:**
   - Type "1-gallon" → Press Enter
   - Type "5-gallon" → Press Enter
   - Type "3-gallon" → Press Enter
   
   **Volume Tiers** (click "Add Tier" button):
   - Tier 1: Min: 0, Max: 5000, Rate: 12
   - Tier 2: Min: 5001, Max: 10000, Rate: 15
   - Tier 3: Min: 10001, Max: (leave empty = unlimited), Rate: 18
   
   **Seasonal Adjustments:**
   - Spring: 1.2 (20% premium)
   - Summer: 1.0 (no adjustment)
   - Fall: 1.1 (10% premium)
   - Winter: 1.0
   - Holiday: 1.3 (30% premium)
   
   **Territory Premiums:**
   - Primary: 1.2 (20% premium)
   - Secondary: 1.0 (no adjustment)
   
   **Other Fields:**
   - Base Rate: 10 (percentage)
   - Minimum Guarantee: (leave empty)

3. **Click**: "Save Rule"

**Expected Results:**
```
✅ Rule created successfully
✅ Toast notification shown
✅ Rule appears in rules list
✅ All fields saved correctly
✅ Rule is active by default
```

---

### **STEP 6.3: Add Second Rule (Minimum Guarantee)**

**Goal**: Create minimum guarantee rule

**Steps:**
1. **Click**: "Add New Rule" again
2. **Fill form**:
   - Rule Name: "Annual Minimum Guarantee"
   - Description: "Minimum royalty payment of $25,000 per year"
   - Rule Type: "minimum_guarantee"
   - Priority: 10 (lower priority)
   - Minimum Guarantee: 25000
3. **Click**: "Save Rule"

**Expected Results:**
```
✅ Second rule created
✅ Both rules visible in list
✅ Rules sorted by priority (1, then 10)
```

---

### **STEP 6.4: Edit Existing Rule**

**Goal**: Modify rule parameters

**Steps:**
1. **Find rule**: "Spring Ornamental Trees Premium"
2. **Click**: Edit icon (✏️ pencil)
3. **Form appears inline** with current values
4. **Modify**: Change Spring seasonal adjustment from 1.2 to 1.25
5. **Click**: "Save Changes"
6. **Verify**: Rule updated with new value

**Expected Results:**
```
✅ Edit form displays with current values
✅ Changes save successfully
✅ Updated value reflected immediately
✅ Toast notification shown
```

---

### **STEP 6.5: Delete Rule (Optional)**

**Goal**: Remove unused rule

**Steps:**
1. **Find rule**: Click delete icon (🗑️ trash) on any rule
2. **Confirmation appears**
3. **Click**: "Confirm Delete"
4. **Verify**: Rule removed from list

**Expected Results:**
```
✅ Confirmation dialog appears
✅ Rule deleted from database
✅ List updates immediately
✅ Toast notification shown
```

---

## **MODULE 7: SALES DATA UPLOAD** 📊

### **STEP 7.1: Upload Sales with ERP Semantic Matching**

**Goal**: Upload sales transactions with AI product matching

**CSV File Structure (`sales_data_sample.csv`):**
```csv
transactionDate,transactionId,productCode,productName,category,territory,containerSize,season,currency,grossAmount,netAmount,quantity,unitPrice
2024-03-15,TXN-2024-001,MAPLE-001,Aurora Flame Maple,Ornamental Trees,Primary,1-gallon,Spring,USD,30000,27000,6200,4.84
2024-03-20,TXN-2024-002,MAPLE-001,Aurora Flame Maple,Ornamental Trees,Primary,5-gallon,Off-Season,USD,25000,22500,1100,22.73
```

**How Semantic Matching Works:**
1. System generates embedding for each sales row (combines productCode, productName, category)
2. Performs vector similarity search against 15 imported ERP products (from Step 5.1)
3. Finds best match based on semantic similarity (not just exact productCode match)
4. Assigns confidence score (0-100%)
5. Links sale to matched ERP product if confidence ≥ 70%

**Steps:**
1. **Navigate to**: Sidebar → "Sales Data"
2. **Notice purple banner**: "🔮 ERP Semantic Matching Active" (confirms ERP enabled)
3. **Select contract**: Choose your ERP-enabled contract
4. **Upload file**: Select `sales_data_sample.csv`
5. **Verify columns** (system shows preview):
   - transactionDate ✅
   - transactionId ✅
   - productCode ✅
   - productName ✅
   - category ✅
   - territory ✅
   - grossAmount ✅
   - quantity ✅
   - (plus containerSize, season, currency, netAmount, unitPrice)
6. **Click**: "Upload & Process"
7. **Wait**: Processing takes 30-60 seconds
   - AI generates embeddings for each sale
   - Performs semantic similarity search against 15 imported products
   - Matches based on productCode, productName, and category
   - Calculates confidence scores

**Expected Results:**
```
✅ Upload successful
✅ Results card displays:

📊 Upload Results
━━━━━━━━━━━━━━━━━━━━━━━━
Total Rows: 15
Valid: 15
Errors: 0

🔮 ERP Semantic Matching Results
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Matched: 15          (≥70% confidence)
⚠️ Unmatched: 0         (<70% confidence)
📊 Avg Confidence: 95%+ (AI-powered)

💡 Sales records were matched against 15 imported 
   ERP products using semantic similarity search

Sales Data Imported (15 transactions):
- TXN-2024-001: MAPLE-001 → Aurora Flame Maple (6,200 units, $30,000) [98% match]
- TXN-2024-002: MAPLE-001 → Aurora Flame Maple (1,100 units, $25,000) [98% match]
- TXN-2024-003: JUNIPER-001 → Golden Spire Juniper (1,800 units, $28,000) [97% match]
- TXN-2024-004: ROSE-001 → Pacific Sunset Rose (3,000 units, $12,000) [98% match]
- TXN-2024-005: HOSTA-001 → Emerald Crown Hosta (900 units, $18,000) [96% match]
- TXN-2024-006: HYDRANGEA-001 → Cascade Blue Hydrangea (20,000 units, $120,000) [97% match]
- TXN-2024-007: ROSE-001 → Pacific Sunset Rose (250 units, $5,000) [98% match]
- TXN-2024-008: MAPLE-002 → Crimson King Maple (3,000 units, $15,000) [97% match]
- TXN-2024-009: JUNIPER-002 → Blue Arrow Juniper (800 units, $22,000) [96% match]
- TXN-2024-010: HYDRANGEA-002 → Pink Diamond Hydrangea (1,200 units, $18,000) [95% match]
- TXN-2024-011: HOSTA-002 → Royal Standard Hosta (1,500 units, $12,000) [95% match]
- TXN-2024-012: ROSE-002 → Crimson Glory Rose (1,000 units, $16,000) [96% match]
- TXN-2024-013: AZALEA-001 → Flame Azalea (1,300 units, $22,000) [96% match]
- TXN-2024-014: LILAC-001 → Purple Majesty Lilac (1,600 units, $8,000) [95% match]
- TXN-2024-015: BOXWOOD-001 → Green Mountain Boxwood (1,250 units, $20,000) [96% match]

✅ Perfect 15/15 match because productCodes align exactly
✅ All confidence scores ≥95% due to exact productCode + semantic name matching
✅ Sales now linked to ERP products for royalty calculations
```

---

### **STEP 7.2: Test Traditional Upload (Compare)**

**Goal**: See difference without ERP matching

**Steps:**
1. **Go back to**: Contract analysis page
2. **Toggle OFF**: ERP semantic matching
3. **Return to**: Sales Data page
4. **Notice**: Purple banner gone (ERP matching inactive)
5. **Upload**: Same `sales_data_sample.csv` file
6. **Compare results**:
   - No semantic matching results shown
   - No confidence scores
   - Direct import without AI analysis

**Expected Results:**
```
✅ Upload works without ERP matching
✅ No semantic matching statistics
✅ Data imported directly
✅ Demonstrates hybrid approach
```

---

## **MODULE 8: ROYALTY CALCULATION** 💰

### **STEP 8.1: Navigate to Royalty Calculator**

**Goal**: Access payment calculation dashboard

**Steps:**
1. **Navigate to**: Sidebar → "Royalty Calculator"
2. **View all contracts** with calculation history
3. **Find your contract**: Should show sales data imported
4. **Click**: "Calculate" button or "View Details"

**Expected Results:**
```
✅ Royalty Calculator page loads
✅ All contracts listed
✅ Contract shows sales data available
✅ Quick action buttons visible
```

---

### **STEP 8.2: Run Royalty Calculation**

**Goal**: Calculate payments using dynamic rules engine

**Steps:**
1. **Select date range**:
   - Start Date: 2024-01-01
   - End Date: 2024-12-31
2. **Click**: "Calculate Royalties" button
3. **Wait**: Calculation processes (5-10 seconds)
4. **View results**:
   - Total royalty amount
   - Number of sales processed
   - Rules applied
   - Breakdown by product/category
   - Minimum guarantee comparison

**Expected Results:**
```
✅ Calculation completes successfully
✅ Results displayed:

💰 Calculation Results
━━━━━━━━━━━━━━━━━━━━━━━━
Total Sales Amount: $365,000
Total Royalty: $65,400
Sales Transactions: 15
Rules Applied: 2

Breakdown by Category:
- Ornamental Trees: $45,000 sales → $8,100 royalty
- Flowering Shrubs: $155,000 sales → $27,900 royalty
- Perennials: $30,000 sales → $5,400 royalty
- Evergreen Shrubs: $20,000 sales → $3,600 royalty
- Evergreen Trees: $15,000 sales → $2,700 royalty

Minimum Guarantee Check:
- Calculated Royalty: $65,400
- Minimum Guarantee: $25,000
- Final Royalty: $65,400 ✅ (exceeds minimum)

Rule Application Details:
Rule: "Spring Ornamental Trees Premium" (Priority 1)
- 5 transactions matched
- Applied seasonal adjustments (Spring 1.25x, Fall 1.1x)
- Applied territory premiums (Primary 1.2x)
- Volume tiers triggered for large orders

Rule: "Annual Minimum Guarantee" (Priority 10)
- Applied as fallback check
- Not triggered (calculated exceeds minimum)
```

---

### **STEP 8.3: View Calculation Breakdown**

**Goal**: Review detailed transaction-level calculations

**Steps:**
1. **Scroll to**: "Calculation Breakdown" section
2. **View each transaction**:
   - Product name and code
   - Category and territory
   - Quantity and gross amount
   - Rule applied
   - Base rate
   - Tier rate (if applicable)
   - Seasonal multiplier
   - Territory multiplier
   - Calculated royalty
   - Explanation text
3. **Verify calculations**: Check math for accuracy

**Expected Example Transaction:**
```
Transaction: TXN-2024-001
Product: Aurora Flame Maple (MAPLE-001)
Category: Ornamental Trees
Territory: Primary
Quantity: 6,200 units
Gross Amount: $30,000
Container Size: 1-gallon
Season: Spring (March 2024)

Rule Applied: "Spring Ornamental Trees Premium"
- Volume Tier: Tier 2 (5,001-10,000 units) → 15% rate
- Seasonal Adjustment: Spring → 1.25x multiplier
- Territory Premium: Primary → 1.2x multiplier

Calculation:
$30,000 × 15% × 1.25 × 1.2 = $6,750

Explanation: "15% of gross sales × 1.25 (Spring) × 1.2 (Primary)"
```

---

### **STEP 8.4: Download Calculation Report**

**Goal**: Export calculation as PDF invoice

**Steps:**
1. **Click**: "Download Report" or "Generate PDF" button
2. **Wait**: PDF generation (5 seconds)
3. **Download**: PDF file automatically downloads
4. **Review PDF**:
   - Company header
   - Contract details
   - Calculation period
   - Summary totals
   - Detailed breakdown table
   - Terms and conditions

**Expected Results:**
```
✅ PDF generates successfully
✅ File downloads: calculation_report_YYYY-MM-DD.pdf
✅ PDF contains all calculation details
✅ Professional invoice format
✅ Ready for payment processing
```

---

## **MODULE 9: CONTRACT Q&A (RAG)** 💬

### **STEP 9.1: Access Contract Q&A**

**Goal**: Use AI to query contract content

**Steps:**
1. **Navigate to**: Sidebar → "Contract Q&A"
2. **View page**:
   - Contract selector dropdown
   - Question input box
   - Chat history (if any previous queries)
3. **Select contract**: Choose your uploaded contract
4. **Verify**: Contract has been processed for Q&A (embeddings created)

**Expected Results:**
```
✅ Contract Q&A page loads
✅ Contract selector populated
✅ Ready to ask questions
```

---

### **STEP 9.2: Ask Basic Questions**

**Goal**: Test RAG retrieval with simple queries

**Test Questions:**

**Question 1:** "What is the royalty rate?"
```
Expected Answer:
"Based on the contract, the royalty rate is tiered:
- 0-5,000 units: 12% of gross sales
- 5,001-10,000 units: 15% of gross sales  
- 10,001+ units: 18% of gross sales

Source: Section 3.2 - Royalty Rates (Page 5)
Confidence: 95%"
```

**Question 2:** "Who are the parties to this agreement?"
```
Expected Answer:
"The parties to this agreement are:
- Licensor: GreenLeaf Nurseries, Inc.
- Licensee: Mountain Vista Garden Centers, LLC

Source: Preamble (Page 1)
Confidence: 98%"
```

**Question 3:** "What is the payment schedule?"
```
Expected Answer:
"Royalty payments are due quarterly:
- Q1: April 15
- Q2: July 15
- Q3: October 15
- Q4: January 15

Payments must be accompanied by a detailed sales report.

Source: Section 4.1 - Payment Terms (Page 6)
Confidence: 92%"
```

**Steps for each question:**
1. **Type question** in input box
2. **Click**: "Ask" button or press Enter
3. **Wait**: AI processes query (3-5 seconds)
4. **Review answer**:
   - AI-generated response
   - Source citation (section, page)
   - Confidence score
   - Relevant contract excerpts

**Expected Results:**
```
✅ AI responds with accurate answers
✅ Citations include section and page numbers
✅ Confidence scores displayed (90-100%)
✅ Relevant excerpts highlighted
```

---

### **STEP 9.3: Ask Complex Questions**

**Goal**: Test advanced RAG capabilities

**Test Questions:**

**Question 4:** "What happens if minimum guarantee is not met?"
```
Expected Answer:
"If the calculated royalties do not meet the annual minimum guarantee of $25,000, the Licensee must pay the difference. This ensures the Licensor receives at least the minimum amount regardless of actual sales performance. The minimum guarantee is reconciled annually."
```

**Question 5:** "Are there any seasonal adjustments to royalty rates?"
```
Expected Answer:
"Yes, the contract includes seasonal rate adjustments:
- Spring (March-May): 1.2x multiplier (20% premium)
- Fall (September-November): 1.1x multiplier (10% premium)  
- Holiday (December): 1.3x multiplier (30% premium)
- Other periods: 1.0x (standard rate)

These multipliers apply to the base royalty rate before final calculation."
```

**Question 6:** "What are the termination conditions?"
```
Expected Answer based on contract content
```

**Expected Results:**
```
✅ Complex questions answered accurately
✅ AI synthesizes information from multiple sections
✅ Answers include context and implications
✅ Citations remain accurate
```

---

### **STEP 9.4: View Chat History**

**Goal**: Review previous Q&A interactions

**Steps:**
1. **Scroll through chat history**
2. **View all previous questions and answers**
3. **Check chronological order**
4. **Verify**: All conversations saved per contract

**Expected Results:**
```
✅ All Q&A history displayed
✅ Organized by timestamp
✅ Searchable and scrollable
✅ Persistent across sessions
```

---

### **STEP 9.5: Test Omnipresent AI Agent**

**Goal**: Access global AI assistant from any page

**Steps:**
1. **Navigate to**: Any authenticated page (Dashboard, Contracts, etc.)
2. **Look for**: Floating purple AI button (bottom right corner)
3. **Click**: AI assistant button
4. **Side panel opens**: Global AI Q&A interface
5. **Ask question**: "How do I calculate royalties?"
6. **Get response**: Context-aware assistance
7. **Close panel**: Click X or click outside
8. **Navigate to another page**: AI button still visible

**Expected Results:**
```
✅ AI button visible on all pages
✅ Side panel slides in from right
✅ Can ask general platform questions
✅ Context-aware responses
✅ Available everywhere (omnipresent)
```

---

## **MODULE 10: ADDITIONAL FEATURES** 🎁

### **STEP 10.1: View Analytics Dashboard**

**Goal**: Review contract analytics and insights

**Steps:**
1. **Navigate to**: Sidebar → "Analytics"
2. **View charts**:
   - Total contracts by status
   - Revenue trends over time
   - Top contracts by value
   - Category distribution
   - Risk assessment summary
3. **Interactive charts**: Click to drill down
4. **Date range selector**: Filter by time period

**Expected Results:**
```
✅ Analytics page loads with charts
✅ Data reflects imported contracts/sales
✅ Charts interactive and responsive
✅ Insights actionable
```

---

### **STEP 10.2: Generate Reports**

**Goal**: Export comprehensive contract reports

**Steps:**
1. **Navigate to**: Sidebar → "Reports"
2. **Select report type**:
   - Contract Summary Report
   - Royalty Payment Report
   - Sales Analysis Report
   - Compliance Report
3. **Select date range**
4. **Select contracts** (one or multiple)
5. **Choose format**: PDF or Excel
6. **Click**: "Generate Report"
7. **Download**: Report file

**Expected Results:**
```
✅ Report generates successfully
✅ Contains selected data
✅ Professional formatting
✅ Downloadable file
```

---

### **STEP 10.3: User Management (Admin Only)**

**Goal**: Manage users and permissions (if admin)

**Steps:**
1. **Navigate to**: Sidebar → "User Management" (admin only)
2. **View all users**:
   - Username
   - Email
   - Role (Admin/Manager/Analyst/Viewer)
   - Status (Active/Inactive)
3. **Add new user** (optional):
   - Click "Add User"
   - Fill form (username, email, role)
   - Send invitation
4. **Edit user role** (optional):
   - Click edit on any user
   - Change role
   - Save changes

**Expected Results (Admin Only):**
```
✅ User management page accessible
✅ All users listed
✅ Can add/edit/deactivate users
✅ Role-based access control working
```

---

## 🎯 **COMPLETE SUCCESS CHECKLIST**

### **Contract Management** ✅
- [ ] Contract uploaded successfully
- [ ] AI analysis completed (parties, dates, terms extracted)
- [ ] Contract metadata reviewed and updated
- [ ] Version control tested (if applicable)
- [ ] Contract visible in dashboard

### **ERP Catalog** ✅
- [ ] Viewed pre-configured ERP systems (5 systems)
- [ ] Reviewed ERP entities (14+ Oracle entities)
- [ ] Checked ERP fields (10+ customer fields)
- [ ] (Optional) Added custom entity

### **Master Data Mapping** ✅
- [ ] Generated AI field mapping
- [ ] Reviewed confidence scores (all ≥90%)
- [ ] Saved mapping with notes
- [ ] Viewed saved mappings
- [ ] Can reuse mappings

### **ERP Configuration** ✅
- [ ] Enabled ERP semantic matching toggle
- [ ] Toggle state persists
- [ ] Contract now ERP-enabled

### **ERP Data Import** ✅
- [ ] Imported 15 products from erp_master_data_sample.csv
- [ ] Status: Completed
- [ ] All records processed with embeddings
- [ ] Import history visible

### **Rules Management** ✅
- [ ] Viewed existing rules (AI-extracted)
- [ ] Added new tiered pricing rule
- [ ] Added minimum guarantee rule
- [ ] Edited existing rule
- [ ] (Optional) Deleted rule
- [ ] All rules saved and active

### **Sales Upload** ✅
- [ ] Uploaded 15 transactions from sales_data_sample.csv
- [ ] ERP semantic matching active (purple banner)
- [ ] All 15 sales matched (≥70% confidence)
- [ ] Average confidence: 95%+
- [ ] Tested traditional upload (comparison)

### **Royalty Calculation** ✅
- [ ] Navigated to Royalty Calculator
- [ ] Selected date range
- [ ] Ran calculation
- [ ] Reviewed total royalty amount
- [ ] Viewed transaction breakdown
- [ ] Downloaded PDF report
- [ ] All calculations accurate

### **Contract Q&A** ✅
- [ ] Asked basic questions (3+ questions)
- [ ] Asked complex questions (2+ questions)
- [ ] Received accurate answers with citations
- [ ] Confidence scores displayed
- [ ] Chat history saved
- [ ] Tested omnipresent AI agent

### **Additional Features** ✅
- [ ] Viewed Analytics dashboard
- [ ] Generated at least one report
- [ ] (Admin) Accessed user management

---

## 📊 **EXPECTED DATA SUMMARY**

After completing all steps, your system should contain:

**Contracts:** 1 contract
- Status: Analyzed
- Parties: 2 parties identified
- Payment Rules: 2-5 rules (AI + manual)
- ERP Matching: Enabled
- Metadata: Completed with version history

**ERP Catalog:**
- Systems: 5 pre-configured
- Entities: 14+ (Oracle)
- Fields: 10+ (Customer fields)

**Master Data Mappings:** 1+ saved mapping
- Confidence: All ≥90%
- Status: Active
- Reusable: Yes

**ERP Master Data:** 15 products
- Products: Maples, Junipers, Roses, Hostas, Hydrangeas, etc.
- Embeddings: 384-dimensional vectors
- Contract: Linked to your contract

**Sales Data:** 15 transactions
- Date Range: 2024-03-15 to 2024-12-15
- Total Sales: $365,000
- Match Confidence: 90-100%
- Matched Records: 15/15

**Royalty Rules:** 2+ rules
- Tiered pricing rule (3 tiers)
- Minimum guarantee rule ($25,000)
- All active and prioritized

**Calculations:** 1+ calculation
- Total Royalty: ~$65,400 (example)
- Transactions: 15
- PDF Report: Generated

**Q&A History:** 5+ questions
- Basic questions answered
- Complex questions answered
- Citations accurate
- Confidence: 90-100%

---

## 🔍 **TROUBLESHOOTING GUIDE**

### **Contract Upload Issues**
**Problem:** Upload fails or stalls
**Solution:**
- Check file size (max 10MB)
- Verify PDF format (not scanned image)
- Check server logs for errors
- Try different contract PDF

### **AI Analysis Not Starting**
**Problem:** Contract stuck in "Uploading" status
**Solution:**
- Verify GROQ_API_KEY is set
- Check server logs for API errors
- Refresh page and check status
- Wait 60 seconds, analysis may be queued

### **ERP Catalog Empty**
**Problem:** No ERP systems visible
**Solution:**
- Database may need seeding
- Check if migration ran
- Re-run `npm run db:push`
- Contact administrator

### **Mapping Generation Fails**
**Problem:** AI mapping returns errors
**Solution:**
- Verify JSON schema is valid
- Check GROQ_API_KEY exists
- Ensure ERP system and entity selected
- Simplify schema (fewer fields)

### **ERP Import Processing Forever**
**Problem:** Import stuck at "Processing"
**Solution:**
- Wait at least 2 minutes (embeddings take time)
- Check HUGGINGFACE_API_KEY is set
- Verify CSV file format
- Check server logs for errors
- Refresh page to see updated status

### **Sales Upload Shows 0% Confidence**
**Problem:** All sales show low confidence
**Solution:**
- Verify ERP master data imported first
- Check product codes/names match between files
- Ensure toggle is ON for contract
- Re-import ERP master data

### **Calculation Errors**
**Problem:** Royalty calculation fails
**Solution:**
- Verify sales data exists
- Check rules are active
- Ensure date range includes sales dates
- Review rule conditions (categories, territories)

### **Q&A Not Responding**
**Problem:** Questions return no answers
**Solution:**
- Verify contract has embeddings
- Check GROQ_API_KEY and HUGGINGFACE_API_KEY
- Simplify question
- Check server logs for API errors

---

## 📈 **NEXT STEPS AFTER TESTING**

### **1. Production Readiness**
- [ ] Test with real contract PDFs
- [ ] Upload actual ERP master data
- [ ] Import real sales transactions
- [ ] Verify calculation accuracy against manual calculations
- [ ] Configure production API keys

### **2. Data Migration**
- [ ] Export test data (if needed)
- [ ] Clean test database
- [ ] Import production contracts
- [ ] Bulk import historical sales
- [ ] Verify data integrity

### **3. User Training**
- [ ] Create user documentation
- [ ] Train contract managers on upload process
- [ ] Train finance team on calculations
- [ ] Train analysts on reporting features
- [ ] Document workflows and SOPs

### **4. Integration**
- [ ] Connect to actual ERP system (Oracle/SAP/NetSuite)
- [ ] Set up automated data feeds
- [ ] Configure scheduled imports
- [ ] Implement approval workflows
- [ ] Set up email notifications

### **5. Optimization**
- [ ] Review AI confidence thresholds
- [ ] Fine-tune field mappings
- [ ] Optimize royalty rules
- [ ] Customize reports
- [ ] Configure dashboards

---

## 💡 **TESTING TIPS**

1. **Take Screenshots**: Document each step for training materials
2. **Note Timestamps**: Track how long each process takes
3. **Test Edge Cases**: Try invalid data, missing fields, etc.
4. **Compare Results**: Verify calculations manually
5. **Break Things**: Test error handling and recovery
6. **Ask Questions**: Use Q&A to verify contract understanding
7. **Test Permissions**: Try features with different user roles
8. **Mobile Testing**: Test responsiveness on mobile devices
9. **Browser Testing**: Test on Chrome, Firefox, Safari
10. **Performance**: Note any slow pages or processes

---

## 📝 **FEEDBACK & NOTES**

Use this space to document:
- Issues encountered
- Feature requests
- Usability concerns
- Performance observations
- Training needs
- Integration requirements

---

## 🆘 **SUPPORT**

If you encounter issues during testing:

1. **Check Server Logs**: Workflow output panel
2. **Check Browser Console**: F12 → Console tab
3. **Review API Keys**: Ensure all secrets are set
4. **Database Status**: Verify PostgreSQL is running
5. **Restart Server**: Sometimes fixes transient issues

**Common API Keys Required:**
- `GROQ_API_KEY` - For AI contract analysis and Q&A
- `HUGGINGFACE_API_KEY` - For embeddings and semantic search
- `OPENAI_API_KEY` - (Optional) Alternative AI provider
- `DATABASE_URL` - PostgreSQL connection (auto-configured)

---

## ✨ **CONCLUSION**

Congratulations! If you've completed all modules, you've successfully tested:
- ✅ Complete contract lifecycle (upload → analysis → calculation → payment)
- ✅ AI-powered features (extraction, mapping, matching, Q&A)
- ✅ ERP integration (catalog, mapping, import, semantic matching)
- ✅ Rules engine (add, edit, delete, calculate)
- ✅ End-to-end workflow from contract to royalty payment

**Total Features Tested:** 10+ major modules
**Total Sample Data Used:** 30+ records (1 contract, 15 products, 15 sales)
**Total Time:** 30-45 minutes

The LicenseIQ platform is now fully tested and ready for production use! 🚀

---

**Version:** 1.0.0
**Last Updated:** 2025-01-05
**Test Data Files:** erp_master_data_sample.csv, sales_data_sample.csv
