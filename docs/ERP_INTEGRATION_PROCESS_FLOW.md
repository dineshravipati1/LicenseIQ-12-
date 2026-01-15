# ERP Integration Hub - Process Flow Documentation

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Database schema enhancements (company hierarchy, versioning, AI confidence) | Completed |
| Phase 2 | Mapping management APIs (CRUD, versions, approve/deprecate) | Completed |
| Phase 3 | AI mapping proposal integration | Existing (via Master Data Mapping page) |
| Phase 4 | Data ingestion APIs with dry-run | Completed |
| Phase 5 | UI - ERP Integration Hub page | Completed |
| Phase 6 | Security - Role-based access control & org-context filtering | Completed |
| Phase 7 | Pre-Import Data Filtering | Completed |

---

## Company Hierarchy Integration (3-Level)

The ERP Integration system enforces the **mandatory 3-level company hierarchy** throughout:

```
COMPANY (Top Level)
    └── BUSINESS UNIT (Middle Level)
            └── LOCATION (Bottom Level)
```

### Where Hierarchy is Applied:

| Component | Hierarchy Scope |
|-----------|----------------|
| **Field Mappings** | Can be scoped to Company, Business Unit, or Location |
| **Data Imports** | Tagged with Company + Business Unit + Location |
| **Import Sources** | Configured per hierarchy scope |
| **Import Jobs** | Filtered by hierarchy scope |
| **Imported Records** | Stored with hierarchy context |
| **Version History** | Tracked per hierarchy level |
| **Approval Workflows** | Role-based within hierarchy |

### UI Filters (Cascading):

1. **Company** - Select company first (required to unlock BU/Location)
2. **Business Unit** - Filtered by selected company
3. **Location** - Filtered by selected company/BU
4. **ERP System** - Filter by ERP system
5. **Status** - Filter by mapping status (draft/approved/deprecated)

---

## End-to-End Process Flow

### Step 1: Configure ERP Systems (ERP Catalog)
**Location:** `/erp-catalog`

1. Navigate to **Data → ERP Catalog**
2. Add your ERP system (e.g., SAP, Oracle, NetSuite)
3. Define ERP entities (e.g., Customer, Vendor, Product)
4. Define ERP fields for each entity with data types

### Step 2: Configure LicenseIQ Schema (Target Schema)
**Location:** `/licenseiq-schema`

1. Navigate to **Data → LicenseIQ Schema**
2. Review existing standard entities (Contract, Licensee, Product, etc.)
3. Add custom fields if needed

### Step 3: Create Field Mappings (Master Data Mapping)
**Location:** `/master-data-mapping`

1. Navigate to **Data → Master Data Mapping**
2. Select your ERP System and Entity Type
3. Click **"Generate AI Mapping"** to get AI-powered field suggestions
4. Review and adjust the AI-generated mappings
5. Set confidence thresholds and transformation rules
6. Save the mapping (creates in "draft" status)

### Step 4: Manage Mappings (ERP Integration Hub)
**Location:** `/erp-integration`

**Mappings Tab:**
1. Navigate to **Data → ERP Integration Hub**
2. View all mappings with filters (Company, ERP System, Status)
3. **Approve** mappings to make them active for imports
4. **View Version History** to see all changes
5. **Revert** to a previous version if needed
6. **Deprecate** mappings no longer in use

### Step 5: Configure Import Sources (ERP Integration Hub)
**Location:** `/erp-integration` → Import Sources Tab

1. Go to **Import Sources** tab
2. Click **"Add Source"** button
3. Choose source type: **File Upload** or **API Connection**
4. Configure source details (name, description, file type/API endpoint)
5. **Configure Pre-Import Filters** (optional):
   - Expand "Pre-Import Data Filters" section
   - Add filter conditions (field, operator, value)
   - Choose AND/OR logic for combining conditions
   - Preview filter results before saving
6. Save the import source

### Step 6: Import Data (ERP Integration Hub)
**Location:** `/erp-integration` → Imports Tab

1. Go to **Data Imports** tab
2. Click **"New Import"** button
3. Select an **approved mapping** to use
4. Upload your CSV/Excel file (filters are automatically applied)
5. System performs **dry-run** (preview without committing)
6. Review the transformed data preview
7. **Commit** to save records or **Discard** to cancel

---

## Pre-Import Data Filtering

### Overview

Pre-import filtering allows you to define criteria that records must meet before being imported. Filters are applied **before** schema validation for efficiency.

### Supported Data Types

| Type | Description | Example Values |
|------|-------------|----------------|
| **text** | String values | "Active", "North Region" |
| **number** | Numeric values | 100, 49.99, 1000 |
| **date** | Date values | "2024-01-15", "2024-12-31" |
| **boolean** | True/false values | true, false, "Yes", "No", "1", "0" |

### Supported Operators

| Operator | Text | Number | Date | Boolean | Description |
|----------|------|--------|------|---------|-------------|
| `equals` | ✓ | ✓ | ✓ | ✓ | Exact match |
| `not_equals` | ✓ | ✓ | ✓ | ✓ | Not equal to value |
| `contains` | ✓ | - | - | - | Contains substring |
| `not_contains` | ✓ | - | - | - | Does not contain substring |
| `starts_with` | ✓ | - | - | - | Starts with string |
| `ends_with` | ✓ | - | - | - | Ends with string |
| `greater_than` | - | ✓ | ✓ | - | Greater than value |
| `less_than` | - | ✓ | ✓ | - | Less than value |
| `greater_than_or_equal` | - | ✓ | ✓ | - | Greater than or equal |
| `less_than_or_equal` | - | ✓ | ✓ | - | Less than or equal |
| `between` | - | ✓ | ✓ | - | Between two values (inclusive) |
| `in` | ✓ | ✓ | - | - | In list of values |
| `not_in` | ✓ | ✓ | - | - | Not in list of values |
| `is_empty` | ✓ | ✓ | ✓ | ✓ | Field is empty/null |
| `is_not_empty` | ✓ | ✓ | ✓ | ✓ | Field has a value |

### Filter Logic

- **AND Logic**: All conditions must be true (record passes only if ALL filters match)
- **OR Logic**: Any condition can be true (record passes if ANY filter matches)

### Filter Configuration UI

1. **Add Condition**: Click to add a new filter condition
2. **Field Selection**: Dropdown of detected fields from sample data
3. **Operator Selection**: Context-aware operators based on data type
4. **Value Input**: Single value, range (for "between"), or comma-separated list (for "in")
5. **Remove Condition**: Delete individual filter conditions
6. **Logic Toggle**: Switch between AND/OR logic

---

## API Endpoints Reference

### Mapping Management APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/erp/mappings` | GET | List mappings with filters |
| `/api/erp/mappings/:id/versions` | GET | Get version history |
| `/api/erp/mappings/:id/propose` | POST | Create new version |
| `/api/erp/mappings/:id/approve` | PUT | Approve a mapping |
| `/api/erp/mappings/:id/revert` | POST | Revert to version |
| `/api/erp/mappings/:id/deprecate` | PUT | Deprecate mapping |

### Import Source APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/erp/import/sources` | GET | List import sources |
| `/api/erp/import/sources` | POST | Create import source |
| `/api/erp/import/sources/:id` | PUT | Update import source |
| `/api/erp/import/sources/:id` | DELETE | Delete import source |

### Data Import APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/erp/import/jobs` | GET | List import jobs |
| `/api/erp/import/jobs/:id` | GET | Get job details with records |
| `/api/erp/import/dry-run` | POST | Preview import (multipart/form-data) |
| `/api/erp/import/jobs/:id/commit` | POST | Commit staged records |
| `/api/erp/import/jobs/:id/discard` | POST | Discard staged records |

### Filter APIs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/erp/import/filter-preview` | POST | Preview filter results on sample data |
| `/api/erp/import/detect-fields` | POST | Detect field names and types from file |

---

## Test Scenarios

### Sample Test Data

A sample items file is available at: `sample-data/items-import-sample.csv`

**Fields:**
- `item_id` (text) - Unique item identifier
- `item_name` (text) - Item name/description
- `category` (text) - Product category
- `unit_price` (number) - Price per unit
- `quantity` (number) - Stock quantity
- `status` (text) - Availability status
- `is_active` (boolean) - Active flag (Yes/No)
- `created_date` (date) - Creation date
- `supplier` (text) - Supplier name
- `region` (text) - Geographic region

### Test Scenario 1: Text Filtering - Equals

**Objective:** Import only "Available" items

| Setting | Value |
|---------|-------|
| Field | status |
| Operator | equals |
| Value | Available |
| Data Type | text |

**Expected Result:** 17 records imported (excludes Low Stock, Out of Stock, Discontinued)

---

### Test Scenario 2: Number Filtering - Greater Than

**Objective:** Import only high-value items (price > $50)

| Setting | Value |
|---------|-------|
| Field | unit_price |
| Operator | greater_than |
| Value | 50 |
| Data Type | number |

**Expected Result:** 8 records imported (items priced above $50)

---

### Test Scenario 3: Boolean Filtering - Equals

**Objective:** Import only active items

| Setting | Value |
|---------|-------|
| Field | is_active |
| Operator | equals |
| Value | Yes |
| Data Type | boolean |

**Expected Result:** 22 records imported (excludes 3 inactive/discontinued items)

---

### Test Scenario 4: Date Filtering - Between

**Objective:** Import items created in Q3-Q4 2024

| Setting | Value |
|---------|-------|
| Field | created_date |
| Operator | between |
| Value | 2024-07-01 |
| Value End | 2024-12-31 |
| Data Type | date |

**Expected Result:** 12 records imported (July - December 2024)

---

### Test Scenario 5: Text Filtering - Contains

**Objective:** Import items with "Cable" in the name

| Setting | Value |
|---------|-------|
| Field | item_name |
| Operator | contains |
| Value | Cable |
| Data Type | text |

**Expected Result:** 4 records imported (Basic Cable, Deluxe Cable, Cable Organizer, Old Model Cable)

---

### Test Scenario 6: Number Filtering - Between

**Objective:** Import items with quantity between 100 and 300

| Setting | Value |
|---------|-------|
| Field | quantity |
| Operator | between |
| Value | 100 |
| Value End | 300 |
| Data Type | number |

**Expected Result:** 8 records imported

---

### Test Scenario 7: Text Filtering - In List

**Objective:** Import items from specific categories

| Setting | Value |
|---------|-------|
| Field | category |
| Operator | in |
| Value | Electronics,Peripherals |
| Data Type | text |

**Expected Result:** 10 records imported (6 Electronics + 4 Peripherals)

---

### Test Scenario 8: Combined Filters - AND Logic

**Objective:** Import only active Electronics items priced over $30

| Filter 1 | Value |
|----------|-------|
| Field | category |
| Operator | equals |
| Value | Electronics |

| Filter 2 | Value |
|----------|-------|
| Field | unit_price |
| Operator | greater_than |
| Value | 30 |

| Filter 3 | Value |
|----------|-------|
| Field | is_active |
| Operator | equals |
| Value | Yes |

| Logic | AND |

**Expected Result:** 4 records imported (Widget Pro, Premium Gadget, Wireless Charger, Discontinued Widget excluded)

---

### Test Scenario 9: Combined Filters - OR Logic

**Objective:** Import items that are either Discontinued OR Out of Stock

| Filter 1 | Value |
|----------|-------|
| Field | status |
| Operator | equals |
| Value | Discontinued |

| Filter 2 | Value |
|----------|-------|
| Field | status |
| Operator | equals |
| Value | Out of Stock |

| Logic | OR |

**Expected Result:** 3 records imported (2 Discontinued + 1 Out of Stock)

---

### Test Scenario 10: Not Equals Boolean

**Objective:** Import all items except inactive ones

| Setting | Value |
|---------|-------|
| Field | is_active |
| Operator | not_equals |
| Value | No |
| Data Type | boolean |

**Expected Result:** 22 records imported (same as Scenario 3)

---

### Test Scenario 11: Empty Field Detection

**Objective:** Find records with missing supplier

| Setting | Value |
|---------|-------|
| Field | supplier |
| Operator | is_empty |
| Data Type | text |

**Expected Result:** 0 records (all items have suppliers in sample data)

---

### Test Scenario 12: Region Filtering - Not In

**Objective:** Import items NOT from North or South regions

| Setting | Value |
|---------|-------|
| Field | region |
| Operator | not_in |
| Value | North,South |
| Data Type | text |

**Expected Result:** 12 records imported (East + West regions only)

---

## Frontend API Configuration

The frontend uses **TanStack Query** for data fetching. API calls are configured in:

**File:** `client/src/pages/erp-integration.tsx`

```typescript
// Fetch mappings with filters
const { data: mappingsData } = useQuery({
  queryKey: ['/api/erp/mappings', companyFilter, erpSystemFilter, statusFilter],
  queryFn: async () => {
    const params = new URLSearchParams();
    if (companyFilter) params.append('companyId', companyFilter);
    if (erpSystemFilter) params.append('erpSystemId', erpSystemFilter);
    if (statusFilter) params.append('status', statusFilter);
    params.append('latestVersionOnly', 'true');
    const response = await fetch(`/api/erp/mappings?${params.toString()}`);
    return response.json();
  },
});

// Mutations use apiRequest from queryClient
const approveMutation = useMutation({
  mutationFn: async (mappingId: string) => {
    const response = await apiRequest('PUT', `/api/erp/mappings/${mappingId}/approve`);
    return response.json();
  },
});
```

**API Client Configuration:** `client/src/lib/queryClient.ts`

---

## Security Model

### Role-Based Access Control
- Only **admin** and **owner** roles can access ERP Integration Hub
- System Admins bypass all filters and can see all data

### Org-Context Filtering (3-Level Hierarchy)
- **Company Level**: Users see only their company's data
- **Business Unit Level**: BU-scoped users see only their BU's data
- **Location Level**: Location-scoped users see only their location's data

---

## Database Schema

### Enhanced Tables

1. **master_data_mappings**
   - `company_id`, `business_unit_id`, `location_id` - Org hierarchy
   - `version`, `parent_mapping_id` - Version control
   - `ai_confidence` - AI mapping confidence score
   - `status` - draft/approved/deprecated
   - `approved_by`, `approved_at` - Approval workflow

2. **data_import_sources**
   - `company_id`, `business_unit_id`, `location_id` - Org hierarchy
   - `source_type` - file/api
   - `config` - Source configuration (JSONB)
   - `filters` - Pre-import filter configuration (JSONB)
   - `is_active` - Active status

3. **data_import_jobs**
   - `company_id`, `business_unit_id`, `location_id` - Org hierarchy
   - `mapping_version` - Track which mapping version was used
   - `source_id` - Link to import source
   - `job_type` - import/dry_run
   - `metadata` - Job metadata including filter statistics (JSONB)
   - Status tracking and error logging

4. **imported_erp_records**
   - `record_status` - staged/committed/failed/discarded
   - Source and target record storage
   - Validation error tracking

---

## Workflow Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   ERP Catalog   │────▶│ LicenseIQ Schema│────▶│ Master Data     │
│   (Define ERP)  │     │ (Define Target) │     │ Mapping (AI)    │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ERP Integration Hub                               │
│  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │
│  │   Mappings Tab    │  │ Import Sources Tab│  │  Imports Tab    │  │
│  │   • View mappings │  │ • Configure files │  │  • Upload data  │  │
│  │   • Approve/Dep   │  │ • Configure APIs  │  │  • Dry-run      │  │
│  │   • Version hist  │  │ • Set filters     │  │  • Commit       │  │
│  │   • Revert        │  │ • Preview filters │  │  • Job history  │  │
│  └───────────────────┘  └───────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Filter Processing Flow

```
┌─────────────────┐
│  Upload File    │
│  (CSV/Excel)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│  Parse Records  │────▶│  Apply Filters  │
│  (All rows)     │     │  (AND/OR logic) │
└─────────────────┘     └────────┬────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         │                                               │
         ▼                                               ▼
┌─────────────────┐                             ┌─────────────────┐
│ Matching Records│                             │Filtered Records │
│ (Pass filters)  │                             │ (Excluded)      │
└────────┬────────┘                             └─────────────────┘
         │
         ▼
┌─────────────────┐
│ Schema Validate │
│ & Transform     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Stage Records  │
│  (Dry-run)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Commit/Discard  │
└─────────────────┘
```

---

## How to Access

1. **Login** as admin user (admin / Admin@123!)
2. Navigate to **Data** category in the left sidebar
3. Click **ERP Integration Hub**
4. You should see the filters and three tabs (Mappings / Import Sources / Data Imports)

If the page is not visible:
- Ensure you have admin or owner role
- Check that your navigation permissions include the ERP Integration Hub
- Clear browser cache and refresh

---

## Troubleshooting

### Filter Not Working

1. **Check data type**: Ensure the filter data type matches the actual field data
2. **Check operator**: Some operators only work with specific data types
3. **Check value format**: 
   - Dates should be in ISO format (YYYY-MM-DD)
   - Boolean values accept: true, false, yes, no, 1, 0
   - Numbers should not include currency symbols (cleaned automatically)

### No Records Passing Filter

1. Use **Filter Preview** to test your filter configuration
2. Check if filter logic (AND/OR) is correct
3. Verify field names match exactly (case-sensitive)

### Import Job Shows Filtered Statistics

Import job metadata includes:
- `totalRecords`: Total records in source file
- `matchedRecords`: Records passing all filters
- `filteredOutRecords`: Records excluded by filters
- `filtersApplied`: Number of filter conditions applied
