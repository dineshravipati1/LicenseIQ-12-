# LicenseIQ Company Hierarchy & Role Testing Guide

## Table of Contents
1. [Understanding the Company Hierarchy](#1-understanding-the-company-hierarchy)
2. [How Users Are Assigned to Hierarchy Levels](#2-how-users-are-assigned-to-hierarchy-levels)
3. [What Each User Level Can See](#3-what-each-user-level-can-see)
4. [Test User Setup Instructions](#4-test-user-setup-instructions)
5. [Testing Scenarios by Navigation Item](#5-testing-scenarios-by-navigation-item)
6. [Quick Reference Tables](#6-quick-reference-tables)

---

## 1. Understanding the Company Hierarchy

### 1.1 The 3-Level Structure

LicenseIQ uses a mandatory 3-level hierarchy. Every piece of data belongs to a specific place in this structure:

```
LEVEL 1: COMPANY (Top Level)
    │
    ├── LEVEL 2: BUSINESS UNIT (Division)
    │       │
    │       └── LEVEL 3: LOCATION (Physical Site)
```

### 1.2 Our Test Data (Pre-Seeded)

The system comes with this sample organization:

```
MONROVIA NURSERY COMPANY (Company ID: cmp-001)
│
├── MONROVIA BRANDED DIVISION (BU ID: bu-001)
│   ├── Dayton, OR - Headquarters (Location ID: loc-001)
│   ├── Visalia, CA (Location ID: loc-002)
│   └── Cairo, GA (Location ID: loc-003)
│
└── WIGHT/BERRYHILL NON-BRANDED (BU ID: bu-002)
    ├── North Carolina (Location ID: loc-004)
    └── Ohio (Location ID: loc-005)
```

### 1.3 Data Ownership

When data is created (contracts, sales, calculations), it gets tagged with:
- `companyId` - Which company owns this data
- `businessUnitId` - Which division/BU owns this data
- `locationId` - Which specific location owns this data

**Example:** A contract uploaded by a user at "Visalia, CA" gets:
- companyId = cmp-001 (Monrovia)
- businessUnitId = bu-001 (Branded Division)
- locationId = loc-002 (Visalia)

---

## 2. How Users Are Assigned to Hierarchy Levels

### 2.1 User Assignment Options

A user can be assigned to ONE of these levels:

| Assignment Level | What It Means |
|------------------|---------------|
| **Location Level** | User works at one specific location only |
| **Business Unit Level** | User oversees all locations in one BU |
| **Company Level** | User oversees the entire company |
| **System Admin** | Super user - sees ALL companies |

### 2.2 How to Assign Users (Step-by-Step)

**To assign a user to a specific level:**

1. Login as Admin
2. Go to **User Management** → Click on a user
3. In the user's **Organization Assignments** section:
   - Select **Company**: Always required
   - Select **Business Unit**: Optional (leave blank for company-level access)
   - Select **Location**: Optional (leave blank for BU-level access)
   - Select **Role**: The role they have at this level

**Examples:**

| User Type | Company | Business Unit | Location | Result |
|-----------|---------|---------------|----------|--------|
| Location User | Monrovia | Branded Division | Visalia, CA | Only sees Visalia data |
| BU User | Monrovia | Branded Division | (blank) | Sees all Branded Division data |
| Company User | Monrovia | (blank) | (blank) | Sees all Monrovia data |

### 2.3 Active Context Switching

Users with multiple assignments can switch between them:
- Click the **Organization Switcher** in the top navigation
- Select which context to work in
- All data shown will filter to that context

---

## 3. What Each User Level Can See

### 3.1 Data Visibility Rules

This is the CORE concept. Based on where a user is assigned, they see different data:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA VISIBILITY RULES                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  SYSTEM ADMIN (isSystemAdmin = true)                                     │
│  └── Sees: EVERYTHING across ALL companies                               │
│                                                                          │
│  COMPANY LEVEL USER (admin/owner role)                                   │
│  └── Sees: All data in their company + Legacy data (NULL context)        │
│                                                                          │
│  BUSINESS UNIT LEVEL USER                                                │
│  └── Sees: All data from all locations within their BU                   │
│                                                                          │
│  LOCATION LEVEL USER                                                     │
│  └── Sees: Only data from their specific location                        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Practical Example: Who Sees Which Contracts?

Imagine we have these contracts in the system:

| Contract | Company | Business Unit | Location |
|----------|---------|---------------|----------|
| Contract A | Monrovia | Branded | Dayton, OR |
| Contract B | Monrovia | Branded | Visalia, CA |
| Contract C | Monrovia | Branded | Cairo, GA |
| Contract D | Monrovia | Non-Branded | North Carolina |
| Contract E | Monrovia | Non-Branded | Ohio |
| Contract F | (NULL) | (NULL) | (NULL) | ← Legacy contract |

**Who sees what?**

| User | Assigned To | Contracts They See |
|------|-------------|-------------------|
| **System Admin** | ALL | A, B, C, D, E, F (all 6) |
| **Company Admin** | Monrovia (company level) | A, B, C, D, E, F (all 6, including legacy) |
| **BU Manager** | Branded Division | A, B, C (3 contracts in Branded BU) |
| **Location Editor** | Visalia, CA | B only (1 contract at Visalia) |
| **Location Viewer** | Ohio | E only (1 contract at Ohio) |

### 3.3 Legacy Data (NULL Context)

Old data created before the hierarchy system has NULL values for company/BU/location.

**Who can see legacy data?**
- System Admin: YES
- Company Admin (admin/owner role): YES
- BU-level users: NO
- Location-level users: NO

---

## 4. Test User Setup Instructions

### 4.1 Pre-Existing Test Users

| Username | Password | Type | Access |
|----------|----------|------|--------|
| `admin` | `Admin@123!` | System Admin | Everything |
| `charlie.test` | `123456` | Company Admin | Monrovia company |

### 4.2 Create These Test Users for Complete Testing

Login as System Admin and create these users:

#### Location-Level Users (Most Restricted)

| Username | Role | Company | Business Unit | Location |
|----------|------|---------|---------------|----------|
| viewer.dayton | viewer | Monrovia | Branded | Dayton, OR |
| editor.visalia | editor | Monrovia | Branded | Visalia, CA |
| analyst.cairo | analyst | Monrovia | Branded | Cairo, GA |
| viewer.nc | viewer | Monrovia | Non-Branded | North Carolina |
| editor.ohio | editor | Monrovia | Non-Branded | Ohio |

#### Business Unit-Level Users (BU Oversight)

| Username | Role | Company | Business Unit | Location |
|----------|------|---------|---------------|----------|
| manager.branded | manager | Monrovia | Branded | (blank) |
| analyst.branded | analyst | Monrovia | Branded | (blank) |
| manager.nonbranded | manager | Monrovia | Non-Branded | (blank) |

#### Company-Level Users (Full Company Access)

| Username | Role | Company | Business Unit | Location |
|----------|------|---------|---------------|----------|
| admin.monrovia | admin | Monrovia | (blank) | (blank) |
| owner.monrovia | owner | Monrovia | (blank) | (blank) |
| auditor.company | auditor | Monrovia | (blank) | (blank) |

### 4.3 Step-by-Step: Creating a Test User

1. Login as `admin` / `Admin@123!`
2. Go to **Administration** → **User Management**
3. Click **"Add User"**
4. Fill in:
   - Username: `viewer.dayton`
   - Email: `viewer.dayton@test.com`
   - Password: `Test@123!`
   - Role: `viewer`
5. In **Organization Assignment**:
   - Company: Select "Monrovia Nursery Company"
   - Business Unit: Select "Monrovia Branded Division"
   - Location: Select "Dayton, OR - Headquarters"
   - Role at this level: `viewer`
6. Save the user

---

## 5. Testing Scenarios by Navigation Item

### 5.1 CONTRACTS Page Testing

**Test Setup:** Create contracts at different locations first.

#### Scenario 1: Location User Sees Only Their Location

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `editor.visalia` (Location: Visalia) | Success |
| 2 | Go to Contracts page | Page loads |
| 3 | Check visible contracts | ONLY Visalia contracts visible |
| 4 | Contracts from Dayton, Cairo, NC, Ohio | NOT visible |

#### Scenario 2: BU User Sees All BU Locations

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `manager.branded` (BU: Branded Division) | Success |
| 2 | Go to Contracts page | Page loads |
| 3 | Check visible contracts | Dayton + Visalia + Cairo contracts visible |
| 4 | Contracts from NC, Ohio (different BU) | NOT visible |

#### Scenario 3: Company Admin Sees Everything + Legacy

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `admin.monrovia` (Company level) | Success |
| 2 | Go to Contracts page | Page loads |
| 3 | Check visible contracts | ALL Monrovia contracts (all 5 locations) |
| 4 | Legacy contracts (NULL context) | ALSO visible |

---

### 5.2 SALES DATA Page Testing

**How Sales Data Works:** Sales data inherits context from the contract it's matched to.

#### Scenario 1: Upload Sales at Location Level

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `analyst.cairo` (Location: Cairo, GA) | Success |
| 2 | Go to Sales Data page | Page loads |
| 3 | Upload a sales CSV file | File uploads |
| 4 | Check the uploaded data | Data tagged with Cairo location |
| 5 | Login as `editor.visalia` (different location) | Success |
| 6 | Go to Sales Data page | Cairo data NOT visible |

#### Scenario 2: BU User Sees All BU Sales

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Create sales data at Dayton, Visalia, Cairo | 3 sets of sales |
| 2 | Login as `analyst.branded` (BU: Branded) | Success |
| 3 | Go to Sales Data page | ALL 3 locations' sales visible |
| 4 | Sales from NC, Ohio | NOT visible |

---

### 5.3 CALCULATIONS Page Testing

**How Calculations Work:** Calculations inherit context from the parent contract.

#### Scenario 1: Location User Runs Calculation

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `analyst.cairo` (Location: Cairo) | Success |
| 2 | Go to Calculations page | Page loads |
| 3 | Only Cairo contracts available in dropdown | Cannot select other locations |
| 4 | Run a calculation | Calculation saved with Cairo context |
| 5 | Login as `viewer.dayton` (different location) | Success |
| 6 | Go to Calculations page | Cairo calculation NOT visible |

#### Scenario 2: BU Manager Reviews All BU Calculations

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `manager.branded` (BU: Branded) | Success |
| 2 | Go to Calculations page | Page loads |
| 3 | Check visible calculations | Dayton + Visalia + Cairo calculations |
| 4 | Calculations from Non-Branded BU | NOT visible |

---

### 5.4 CONTRACT UPLOAD Testing

**Who Can Upload:** editor, manager, admin, owner roles only

#### Scenario 1: Location Editor Uploads Contract

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `editor.visalia` | Success |
| 2 | Go to Upload Contract | Page loads |
| 3 | Upload a PDF contract | Contract saved |
| 4 | Check contract's context fields | Automatically set to Visalia location |
| 5 | Login as `editor.ohio` (different location) | Success |
| 6 | Go to Contracts page | New contract NOT visible |

#### Scenario 2: Viewer Cannot Upload

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `viewer.dayton` | Success |
| 2 | Check sidebar | "Upload Contract" NOT visible |
| 3 | Try direct URL `/upload` | Access denied or redirected |

---

### 5.5 ADMINISTRATION Pages Testing

**Admin-Only Pages:** User Management, Configuration, Master Data, Lead Management

#### Scenario 1: Company Admin Access

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `admin.monrovia` (Company admin) | Success |
| 2 | Check sidebar | User Management, Configuration visible |
| 3 | Go to User Management | Only Monrovia users visible |
| 4 | Try to create user in another company | Cannot - only Monrovia available |

#### Scenario 2: Location Manager Cannot Access Admin Pages

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `manager.branded` (BU level, manager role) | Success |
| 2 | Check sidebar | Admin pages NOT visible |
| 3 | Try direct URL `/users` | Access denied |

#### Scenario 3: System Admin Sees All Companies

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `admin` (System Admin) | Success |
| 2 | Go to Master Data | ALL companies visible |
| 3 | Go to User Management | ALL users from ALL companies visible |
| 4 | Can create users in any company | Yes |

---

### 5.6 liQ AI (Contract Q&A) Testing

**AI answers based on contracts user can see**

#### Scenario 1: Location User Gets Location-Specific Answers

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `viewer.dayton` (Location: Dayton) | Success |
| 2 | Go to liQ AI page | Chat interface loads |
| 3 | Ask: "What contracts do we have?" | Only Dayton contracts mentioned |
| 4 | AI references contracts from other locations | Should NOT happen |

#### Scenario 2: Company Admin Gets Complete Picture

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | Login as `admin.monrovia` | Success |
| 2 | Go to liQ AI page | Chat interface loads |
| 3 | Ask: "What contracts do we have?" | ALL company contracts mentioned |

---

## 6. Quick Reference Tables

### 6.1 User Level vs Data Visibility Matrix

| User Level | Own Location | Other Locations in BU | Other BUs | Legacy Data |
|------------|--------------|----------------------|-----------|-------------|
| **Location User** | YES | NO | NO | NO |
| **BU User** | YES | YES | NO | NO |
| **Company Admin** | YES | YES | YES | YES |
| **System Admin** | YES | YES | YES | YES |

### 6.2 Role vs Navigation Access (Default)

| Navigation Item | viewer | editor | analyst | auditor | manager | admin | owner |
|-----------------|--------|--------|---------|---------|---------|-------|-------|
| Dashboard | YES | YES | YES | YES | YES | YES | YES |
| Contracts | YES | YES | YES | YES | YES | YES | YES |
| Upload Contract | NO | YES | NO | NO | YES | YES | YES |
| License Fee Rules | YES | YES | YES | YES | YES | YES | YES |
| Review Queue | NO | NO | NO | NO | NO | YES | YES |
| Royalty Calculator | NO | NO | YES | NO | YES | YES | YES |
| Calculations | NO | NO | YES | YES | YES | YES | YES |
| Sales Data | NO | NO | YES | NO | YES | YES | YES |
| ERP Mapping | NO | NO | NO | NO | NO | YES | YES |
| ERP Catalog | NO | NO | NO | NO | NO | YES | YES |
| LicenseIQ Schema | NO | NO | NO | NO | NO | YES | YES |
| Master Data | NO | NO | NO | NO | NO | YES | YES |
| liQ AI | YES | YES | YES | YES | YES | YES | YES |
| Knowledge Base | NO | NO | NO | NO | NO | YES | YES |
| RAG Dashboard | NO | NO | NO | NO | NO | YES | YES |
| Analytics | NO | NO | YES | NO | NO | YES | YES |
| Reports | NO | NO | YES | NO | NO | YES | YES |
| User Management | NO | NO | NO | NO | NO | YES | YES |
| Lead Management | NO | NO | NO | NO | NO | YES | YES |
| Audit Trail | NO | NO | NO | YES | NO | YES | YES |
| Configuration | NO | NO | NO | NO | NO | YES | YES |
| Navigation Manager | NO | NO | NO | NO | NO | NO | YES |

### 6.3 Combined: Role + Level = What You See

**Example: Analyst Role at Different Levels**

| Analyst at... | Contracts Visible | Sales Visible | Admin Pages |
|---------------|-------------------|---------------|-------------|
| Dayton Location | Dayton only | Dayton only | NO |
| Branded BU | Dayton + Visalia + Cairo | Same | NO |
| Monrovia Company | All 5 locations | All 5 locations | NO |

**Example: Admin Role at Different Levels**

| Admin at... | Contracts Visible | Users Can Manage | Config Access |
|-------------|-------------------|------------------|---------------|
| Dayton Location | Dayton only | Dayton users | YES |
| Branded BU | Dayton + Visalia + Cairo | BU users | YES |
| Monrovia Company | All + Legacy | All company users | YES |
| System Admin | ALL companies | ALL users | YES |

---

## 7. Troubleshooting

### Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| User sees no data | No org assignment | Assign user to company/BU/location |
| User sees everything | System Admin flag set | Check `isSystemAdmin` in database |
| Menu item missing | Role doesn't have permission | Check Configuration page |
| Can't see legacy contracts | Not admin/owner role | Assign admin role at company level |
| Context switcher empty | No assignments | Create org assignments for user |

---

**Document Version:** 2.0  
**Last Updated:** December 2024  
**Focus:** Company Hierarchy + Role Testing
