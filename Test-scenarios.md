# LicenseIQ Research Platform - Test Scenarios

This document outlines testing procedures for the LicenseIQ Research Platform's key features.

---

## Table of Contents

1. [Test User Credentials](#test-user-credentials)
2. [Two-Tier Admin System](#two-tier-admin-system)
3. [Multi-Location Context Filtering](#multi-location-context-filtering)
4. [Master Data Management](#master-data-management)
5. [User-Organization Role Management](#user-organization-role-management)
6. [Contract Management](#contract-management)
7. [Navigation System](#navigation-system)

---

## Test User Credentials

### System Administrator
| Field | Value |
|-------|-------|
| Username | admin |
| Password | Admin@123! |
| Email | admin@licenseiq.com |
| Role | System Admin (`isSystemAdmin = true`) |

### Test Users with Multi-Location Access

| Username | Password | Organization Access |
|----------|----------|---------------------|
| alice.test | Test@123! | NY Office [editor], Frisco [viewer] |
| bob.test | Test@123! | LA Office [editor], NY Office [manager] |
| charlie.test | Test@123! | Sales Division [manager], Frisco [auditor] |
| diana.test | Test@123! | Acme Corporation [owner], Rao Group [viewer] |

---

## Two-Tier Admin System

### Overview
The system distinguishes between:
- **System Admin**: Super user who can manage ALL companies
- **Company Admin**: Can only manage their own company's data

### Test Scenarios

#### Scenario 1: System Admin - Full Access
1. Login as `admin` / `Admin@123!`
2. Navigate to **Settings > Master Data**
3. **Expected Results:**
   - Can see ALL companies in the system
   - Can create new companies
   - Can edit any company
   - Can delete any company
   - Can create business units under any company
   - Can create locations under any business unit
   - Can view all users in the system

#### Scenario 2: Company Admin - Restricted Access
1. Login as `diana.test` / `Test@123!` (has owner role at Acme Corporation)
2. Switch context to Acme Corporation
3. Navigate to **Settings > Master Data**
4. **Expected Results:**
   - Can only see Acme Corporation (NOT Rao Group or other companies)
   - CANNOT create new companies (option should be hidden or return 403)
   - CAN edit Acme Corporation details
   - CANNOT delete Acme Corporation
   - CAN create business units under Acme Corporation
   - CAN create locations under Acme Corporation's business units
   - Can only see users with roles in Acme Corporation

#### Scenario 3: Cross-Company Creation Prevention
1. Login as company admin (e.g., diana.test)
2. Try to create a business unit via API with a different company's ID
3. **Expected Result:** Server should override the companyId with the user's company

#### Scenario 4: Cross-Company Update Prevention
1. Login as company admin
2. Try to update a business unit from another company via API
3. **Expected Result:** 403 Forbidden - "Cannot modify entity from another company"

#### Scenario 5: Location orgId Validation
1. Login as company admin
2. Try to create a location with an orgId (business unit) from another company
3. **Expected Result:** 403 Forbidden - "Invalid business unit for your company"

---

## Multi-Location Context Filtering

### Overview
Users only see data relevant to their active organizational context (location, business unit, or company level).

### Test Scenarios

#### Scenario 1: Location-Level Access
1. Login as `alice.test` / `Test@123!`
2. Switch context to "NY Office" (location-level role)
3. Navigate to Contracts list
4. **Expected Results:**
   - Only see contracts assigned to NY Office
   - Sales data filtered to NY Office contracts only
   - Calculations filtered to NY Office contracts only

#### Scenario 2: Business Unit-Level Access
1. Login as `charlie.test` / `Test@123!`
2. Switch context to "Sales Division" (BU-level role)
3. Navigate to Contracts list
4. **Expected Results:**
   - See contracts from ALL locations within Sales Division
   - Sales data from all Sales Division locations
   - Calculations from all Sales Division locations

#### Scenario 3: Company-Level Access
1. Login as `diana.test` / `Test@123!`
2. Switch context to "Acme Corporation" (company-level role)
3. Navigate to Contracts list
4. **Expected Results:**
   - See ALL contracts across all business units and locations in Acme Corporation
   - Sales data from entire company
   - Calculations from entire company

#### Scenario 4: Admin Bypass
1. Login as `admin` / `Admin@123!`
2. View Contracts list (without selecting a specific context)
3. **Expected Results:**
   - See ALL contracts in the system regardless of organization
   - No context filtering applied

#### Scenario 5: Context Switching
1. Login as `alice.test` / `Test@123!`
2. Initially view contracts with "NY Office" context
3. Switch context to "Frisco" using the context switcher
4. **Expected Results:**
   - Contract list refreshes immediately
   - Now only shows contracts assigned to Frisco location
   - Navigation may update based on role (viewer vs editor)

---

## Master Data Management

### Companies

#### Scenario 1: Create Company (System Admin Only)
1. Login as system admin
2. Navigate to Settings > Master Data > Companies
3. Click "Add Company"
4. Fill in company details
5. **Expected Result:** Company created successfully

#### Scenario 2: Create Company Denied (Company Admin)
1. Login as company admin (diana.test)
2. Try to access company creation
3. **Expected Result:** Option hidden or 403 Forbidden

### Business Units

#### Scenario 1: Create Business Unit
1. Login as company admin or system admin
2. Navigate to Settings > Master Data > Business Units
3. Select parent company (auto-selected for company admins)
4. Fill in business unit details
5. **Expected Result:** Business unit created under correct company

#### Scenario 2: Edit Business Unit
1. Select an existing business unit
2. Update details
3. **Expected Result:** Changes saved, audit trail updated

### Locations

#### Scenario 1: Create Location
1. Login as company admin or system admin
2. Navigate to Settings > Master Data > Locations
3. Select parent business unit (filtered to user's company)
4. Fill in location details
5. **Expected Result:** Location created under correct business unit

---

## User-Organization Role Management

### Test Scenarios

#### Scenario 1: Assign User to Organization
1. Login as admin or company admin
2. Navigate to Settings > Users
3. Select a user
4. Click "Manage Roles" or "Assign to Organization"
5. Select organization level (Company/BU/Location)
6. Select specific organization
7. Assign role (owner/admin/manager/editor/viewer/auditor)
8. **Expected Result:** User can now access data at that organization level

#### Scenario 2: Multiple Role Assignments
1. Assign same user to multiple organizations
2. User logs in and uses context switcher
3. **Expected Result:** User sees all their assigned organizations in context switcher

#### Scenario 3: Role Hierarchy Test
1. Assign user as "editor" at location level
2. Assign same user as "manager" at BU level
3. User switches between contexts
4. **Expected Result:** 
   - At location context: editor permissions
   - At BU context: manager permissions

#### Scenario 4: Remove Organization Assignment
1. Remove a user's organization assignment
2. User refreshes or re-logs
3. **Expected Result:** User no longer sees that organization in context switcher

---

## Contract Management

### Test Scenarios

#### Scenario 1: Upload Contract
1. Login as user with editor+ role
2. Navigate to Contracts
3. Click "Upload Contract"
4. Select PDF file
5. **Expected Result:** 
   - Contract uploaded
   - AI analysis triggered
   - Contract assigned to user's active context organization

#### Scenario 2: View Contract (Context Filtered)
1. Login as location-level user
2. Navigate to Contracts list
3. **Expected Result:** Only contracts for that location visible

#### Scenario 3: Contract Search
1. Use the contract search feature
2. Search by various criteria
3. **Expected Result:** Results filtered by user's organizational context

#### Scenario 4: AI Analysis
1. Upload a contract or select existing
2. Trigger AI analysis
3. **Expected Result:**
   - Key terms extracted
   - Risk assessment generated
   - Payment rules identified

---

## Navigation System

### Test Scenarios

#### Scenario 1: Role-Based Menu Visibility
1. Login as user with "viewer" role
2. Check navigation menu
3. **Expected Result:** Limited menu items (read-only features)

4. Login as user with "admin" role
5. Check navigation menu
6. **Expected Result:** Full menu with settings and admin options

#### Scenario 2: Context-Based Navigation
1. Login as user with multiple contexts at different roles
2. Switch between contexts
3. **Expected Result:** Menu items update based on current context's role

#### Scenario 3: Category Collapse State
1. Collapse a navigation category
2. Navigate to a different page
3. Return to dashboard
4. **Expected Result:** Category remains collapsed (state persisted)

#### Scenario 4: Navigation Item Reordering
1. Use drag-and-drop to reorder menu items (if enabled)
2. Refresh page
3. **Expected Result:** Custom order persisted

---

## API Testing (Advanced)

### Two-Tier Admin Authorization Tests

```bash
# Test 1: System admin can get all companies
curl -X GET http://localhost:5000/api/master-data/companies \
  -H "Cookie: <system-admin-session>"
# Expected: Returns all companies

# Test 2: Company admin only gets their company
curl -X GET http://localhost:5000/api/master-data/companies \
  -H "Cookie: <company-admin-session>"
# Expected: Returns only user's company

# Test 3: Company admin cannot create company
curl -X POST http://localhost:5000/api/master-data/companies \
  -H "Content-Type: application/json" \
  -H "Cookie: <company-admin-session>" \
  -d '{"companyName": "Test Corp"}'
# Expected: 403 Forbidden

# Test 4: Company admin cannot update other company
curl -X PATCH http://localhost:5000/api/master-data/companies/<other-company-id> \
  -H "Content-Type: application/json" \
  -H "Cookie: <company-admin-session>" \
  -d '{"companyName": "Hacked Name"}'
# Expected: 403 Forbidden

# Test 5: CompanyId override for BU creation
curl -X POST http://localhost:5000/api/master-data/business-units \
  -H "Content-Type: application/json" \
  -H "Cookie: <company-admin-session>" \
  -d '{"orgName": "Test BU", "companyId": "<other-company-id>"}'
# Expected: BU created under user's company, not the spoofed companyId

# Test 6: OrgId validation for location creation
curl -X POST http://localhost:5000/api/master-data/locations \
  -H "Content-Type: application/json" \
  -H "Cookie: <company-admin-session>" \
  -d '{"locName": "Test Loc", "orgId": "<bu-from-other-company>"}'
# Expected: 403 "Invalid business unit for your company"
```

### Multi-Location Context Tests

```bash
# Test 1: Contracts filtered by context
curl -X GET http://localhost:5000/api/contracts \
  -H "Cookie: <location-user-session>"
# Expected: Only contracts for user's active location

# Test 2: Admin sees all contracts
curl -X GET http://localhost:5000/api/contracts \
  -H "Cookie: <admin-session>"
# Expected: All contracts in system
```

---

## Troubleshooting

### Common Issues

1. **Can't see expected data**
   - Check user's active context in context switcher
   - Verify user has organization assignment at correct level
   - Confirm data has org context fields populated

2. **Permission denied errors**
   - Verify user's role at current context level
   - Check if trying to access cross-company data
   - Ensure user is not trying admin-only operations as company admin

3. **Context switcher empty**
   - User has no organization assignments
   - Run organization assignment for user in Settings > Users

4. **Navigation items missing**
   - Check user's role permissions
   - Verify navigation items are configured for user's role level

---

## Test Data Setup

To create test data for all scenarios, run:

```bash
npx tsx server/scripts/create-test-data.ts
```

This creates:
- 2 companies (Acme Corporation, Rao Group)
- 3 business units across companies
- 4 locations across business units
- 4 test users with various role assignments
- Sample contracts, sales data, and calculations

---

## Production Setup

### Automatic Data Seeding

All required data is **automatically seeded on server startup**. No manual scripts are needed.

When the server starts (both development and production), you should see these log messages:
```
🌱 Seeding/Updating Navigation System...
✅ Navigation seeding complete: 6 categories, 21 items, 21 mappings
🌱 Seeding Master Data...
✅ Master Data seeding complete
```

#### Navigation Data (Auto-seeded)
The navigation system is automatically seeded and updated on every server startup:
- **Navigation Categories**: Main, Contracts, Finance, Data Management, AI & Analytics, Administration
- **Navigation Items**: All 21 menu items with correct icons and routes
- **Item-to-Category Mappings**: Assigns each item to its category
- **Role Permissions**: Sets up which roles can see which menu items

**Important**: Navigation paths are kept in sync between development and production through the upsert mechanism. Any path corrections in the codebase will be applied on the next server restart/deployment.

#### 2. LicenseIQ Schema Catalog (Auto-seeded)
This is automatically seeded on server startup. Look for:
```
🌱 Seeding LicenseIQ Schema Catalog...
✓ All 28 LicenseIQ schema entities already exist
```

#### 3. System Knowledge Base (Auto-seeded)
This is automatically seeded on server startup. Look for:
```
🌱 Seeding System Knowledge Base for LIQ AI...
✅ System Knowledge Base seeding complete
```

#### 4. Admin User
Ensure the system admin user exists:
- Username: `admin`
- Password: `Admin@123!`
- Email: `admin@licenseiq.com`
- `isSystemAdmin = true`

### Production Deployment Checklist

- [ ] Database migrations applied (`npm run db:push`)
- [ ] Deploy/Publish the latest code to production (triggers auto-seeding)
- [ ] Verify server logs show seeding messages on startup
- [ ] Admin user exists with `isSystemAdmin = true` (auto-created by seeding)
- [ ] Environment variables configured (DATABASE_URL, GROQ_API_KEY, etc.)
- [ ] Left navigation menu visible after login
- [ ] Context switcher working for users with org assignments
- [ ] All navigation links point to correct pages (License Fee Rules → /contracts, Royalty Calculator → /calculations, liQ AI → /contract-qna, Sales Data → /sales-upload)

### Troubleshooting Production Issues

#### Navigation Menu Not Showing
1. Check if navigation data exists:
   ```sql
   SELECT COUNT(*) FROM navigation_permissions;
   SELECT COUNT(*) FROM navigation_categories;
   SELECT COUNT(*) FROM navigation_item_categories;
   ```
2. If counts are 0, run the seeding script:
   ```bash
   npx tsx server/scripts/seed-navigation.ts
   ```

#### User Can't See Any Menu Items
1. Check user's role:
   ```sql
   SELECT role FROM users WHERE username = 'your-user';
   ```
2. Check role permissions:
   ```sql
   SELECT * FROM role_navigation_permissions WHERE role = 'your-role';
   ```
3. If no permissions, re-run the navigation seeding script

#### Context Switcher Empty
1. Check user organization assignments:
   ```sql
   SELECT * FROM user_organization_roles WHERE user_id = 'user-id';
   ```
2. Assign user to organizations via User Management page

---

*Last Updated: December 2, 2024*
