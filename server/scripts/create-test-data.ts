/**
 * Test Data Script: Multi-Location Context Testing with Multiple Roles
 * 
 * Creates comprehensive test data using EXISTING organizational hierarchy to verify:
 * - Multiple organization assignments per user with DIFFERENT roles
 * - Dynamic navigation changes when switching contexts
 * - Role-based navigation permissions (fully database-driven)
 * 
 * Each test user has 2 organization assignments with different roles:
 * - alice.test: editor (NY) + viewer (Frisco)
 * - bob.test: editor (LA) + manager (NY)
 * - charlie.test: manager (Sales Division) + auditor (Frisco)
 * - diana.test: owner (Acme Company) + viewer (Rao Group)
 * 
 * Usage:
 *   tsx server/scripts/create-test-data.ts
 */

import { db } from '../db';
import { 
  users, userOrganizationRoles,
  contracts, salesData, contractRoyaltyCalculations
} from '../../shared/schema';
import { eq } from 'drizzle-orm';
import crypto from 'crypto';

function hashPassword(password: string): string {
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(password, salt, 64);
  return `${hash.toString('hex')}.${salt.toString('hex')}`;
}

async function createTestData() {
  console.log('🧪 Creating Multi-Location Test Data with Multiple Roles...\n');

  try {
    // Get admin user ID for audit columns
    const [adminUser] = await db.select().from(users).where(eq(users.username, 'admin')).limit(1);
    const ADMIN_USER_ID = adminUser?.id;

    if (!ADMIN_USER_ID) {
      throw new Error('Admin user not found. Please ensure admin user exists before running this script.');
    }

    // ==========================================
    // STEP 1: Define Organizational Hierarchy IDs
    // ==========================================
    console.log('🏢 STEP 1: Using Existing Organizational Hierarchy...');
    
    const ACME_COMPANY_ID = 'cmp-001';
    const SALES_DIVISION_ID = 'org-001';
    const OPERATIONS_DIVISION_ID = 'org-002';
    const NY_OFFICE_ID = 'loc-001';
    const LA_OFFICE_ID = 'loc-002';
    
    const RAO_GROUP_ID = 'eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc';
    const DALLAS_UNIT_ID = '7e06ef6e-0dfb-4068-8e93-17c770b7d053';
    const FRISCO_LOCATION_ID = '391f5e20-4161-4480-abca-a5b2a8f959f8';

    console.log(`   ✓ Acme Corporation (cmp-001)`);
    console.log(`     - Sales Division (org-001): NY Office, LA Office`);
    console.log(`     - Operations Division (org-002)`);
    console.log(`   ✓ Rao Group of Companies`);
    console.log(`     - Dallas Unit: Frisco Location\n`);

    // ==========================================
    // STEP 2: Create Test Users with MULTIPLE ROLES
    // ==========================================
    console.log('👥 STEP 2: Creating Test Users with MULTIPLE Organization Roles...');

    const hashedPassword = hashPassword('Test@123!');

    // User 1: Alice - editor at NY, viewer at Frisco
    const [alice] = await db.insert(users).values({
      username: 'alice.test',
      email: 'alice@acmecorp.test',
      password: hashedPassword,
      role: 'editor',
      firstName: 'Alice',
      lastName: 'Johnson',
    }).returning();

    await db.insert(userOrganizationRoles).values([
      {
        id: 'uor-alice-ny',
        userId: alice.id,
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: NY_OFFICE_ID,
        role: 'editor',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
      {
        id: 'uor-alice-frisco',
        userId: alice.id,
        companyId: RAO_GROUP_ID,
        businessUnitId: DALLAS_UNIT_ID,
        locationId: FRISCO_LOCATION_ID,
        role: 'viewer',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
    ]);

    console.log(`   ✓ Alice: editor (NY Office) + viewer (Frisco)`);

    // User 2: Bob - editor at LA, manager at NY
    const [bob] = await db.insert(users).values({
      username: 'bob.test',
      email: 'bob@acmecorp.test',
      password: hashedPassword,
      role: 'editor',
      firstName: 'Bob',
      lastName: 'Smith',
    }).returning();

    await db.insert(userOrganizationRoles).values([
      {
        id: 'uor-bob-la',
        userId: bob.id,
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: LA_OFFICE_ID,
        role: 'editor',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
      {
        id: 'uor-bob-ny',
        userId: bob.id,
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: NY_OFFICE_ID,
        role: 'manager',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
    ]);

    console.log(`   ✓ Bob: editor (LA Office) + manager (NY Office)`);

    // User 3: Charlie - manager at Sales Division, auditor at Frisco
    const [charlie] = await db.insert(users).values({
      username: 'charlie.test',
      email: 'charlie@acmecorp.test',
      password: hashedPassword,
      role: 'editor',
      firstName: 'Charlie',
      lastName: 'Brown',
    }).returning();

    await db.insert(userOrganizationRoles).values([
      {
        id: 'uor-charlie-sales',
        userId: charlie.id,
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: null,
        role: 'manager',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
      {
        id: 'uor-charlie-frisco',
        userId: charlie.id,
        companyId: RAO_GROUP_ID,
        businessUnitId: DALLAS_UNIT_ID,
        locationId: FRISCO_LOCATION_ID,
        role: 'auditor',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
    ]);

    console.log(`   ✓ Charlie: manager (Sales Division) + auditor (Frisco)`);

    // User 4: Diana - owner at Acme, viewer at Rao Group
    const [diana] = await db.insert(users).values({
      username: 'diana.test',
      email: 'diana@acmecorp.test',
      password: hashedPassword,
      role: 'owner',
      firstName: 'Diana',
      lastName: 'Prince',
    }).returning();

    await db.insert(userOrganizationRoles).values([
      {
        id: 'uor-diana-acme',
        userId: diana.id,
        companyId: ACME_COMPANY_ID,
        businessUnitId: null,
        locationId: null,
        role: 'owner',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
      {
        id: 'uor-diana-rao',
        userId: diana.id,
        companyId: RAO_GROUP_ID,
        businessUnitId: null,
        locationId: null,
        role: 'viewer',
        status: 'A',
        createdBy: ADMIN_USER_ID,
        lastUpdatedBy: ADMIN_USER_ID,
      },
    ]);

    console.log(`   ✓ Diana: owner (Acme Company) + viewer (Rao Group)\n`);

    // ==========================================
    // STEP 3: Create Contracts
    // ==========================================
    console.log('📄 STEP 3: Creating Contracts...');

    const [contractNY] = await db.insert(contracts).values({
      originalName: 'NY-Sales-License-Agreement.pdf',
      fileName: 'ny-sales-license.pdf',
      filePath: '/test-data/contracts/ny-sales-license.pdf',
      fileSize: 150000,
      fileType: 'application/pdf',
      uploadedBy: alice.id,
      status: 'active',
      companyId: ACME_COMPANY_ID,
      businessUnitId: SALES_DIVISION_ID,
      locationId: NY_OFFICE_ID,
      displayName: 'NY Office Software License',
      contractType: 'license',
    }).returning();

    const [contractLA] = await db.insert(contracts).values({
      originalName: 'LA-Distribution-Agreement.pdf',
      fileName: 'la-distribution.pdf',
      filePath: '/test-data/contracts/la-distribution.pdf',
      fileSize: 180000,
      fileType: 'application/pdf',
      uploadedBy: bob.id,
      status: 'active',
      companyId: ACME_COMPANY_ID,
      businessUnitId: SALES_DIVISION_ID,
      locationId: LA_OFFICE_ID,
      displayName: 'LA Office Distribution Agreement',
      contractType: 'license',
    }).returning();

    const [contractFrisco] = await db.insert(contracts).values({
      originalName: 'Frisco-Partnership-Agreement.pdf',
      fileName: 'frisco-partnership.pdf',
      filePath: '/test-data/contracts/frisco-partnership.pdf',
      fileSize: 200000,
      fileType: 'application/pdf',
      uploadedBy: diana.id,
      status: 'active',
      companyId: RAO_GROUP_ID,
      businessUnitId: DALLAS_UNIT_ID,
      locationId: FRISCO_LOCATION_ID,
      displayName: 'Frisco Partnership Agreement',
      contractType: 'partnership',
    }).returning();

    console.log(`   ✓ Created 3 contracts (NY, LA, Frisco)\n`);

    // ==========================================
    // STEP 4: Create Sales Data
    // ==========================================
    console.log('💰 STEP 4: Creating Sales Data...');

    await db.insert(salesData).values([
      {
        matchedContractId: contractNY.id,
        matchConfidence: '95.5',
        transactionDate: new Date('2024-01-15'),
        transactionId: 'NY-TXN-001',
        productName: 'Enterprise Software Suite',
        category: 'Software',
        territory: 'East Coast',
        grossAmount: '50000.00',
        netAmount: '45000.00',
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: NY_OFFICE_ID,
      },
      {
        matchedContractId: contractNY.id,
        matchConfidence: '92.0',
        transactionDate: new Date('2024-02-20'),
        transactionId: 'NY-TXN-002',
        productName: 'Professional Services',
        category: 'Services',
        territory: 'East Coast',
        grossAmount: '30000.00',
        netAmount: '28000.00',
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: NY_OFFICE_ID,
      },
    ]);

    await db.insert(salesData).values({
      matchedContractId: contractLA.id,
      matchConfidence: '88.0',
      transactionDate: new Date('2024-03-10'),
      transactionId: 'LA-TXN-001',
      productName: 'Cloud Platform Subscription',
      category: 'Software',
      territory: 'West Coast',
      grossAmount: '75000.00',
      netAmount: '72000.00',
      companyId: ACME_COMPANY_ID,
      businessUnitId: SALES_DIVISION_ID,
      locationId: LA_OFFICE_ID,
    });

    console.log(`   ✓ Created 3 sales records\n`);

    // ==========================================
    // STEP 5: Create License Fee Calculations
    // ==========================================
    console.log('📊 STEP 5: Creating License Fee Calculations...');

    await db.insert(contractRoyaltyCalculations).values([
      {
        contractId: contractNY.id,
        name: 'Q1 2024 License Fees - NY Office',
        periodStart: new Date('2024-01-01'),
        periodEnd: new Date('2024-03-31'),
        totalSalesAmount: '80000.00',
        totalRoyalty: '8000.00',
        salesCount: 2,
        calculatedBy: alice.id,
        status: 'approved',
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: NY_OFFICE_ID,
      },
      {
        contractId: contractLA.id,
        name: 'Q1 2024 License Fees - LA Office',
        periodStart: new Date('2024-01-01'),
        periodEnd: new Date('2024-03-31'),
        totalSalesAmount: '75000.00',
        totalRoyalty: '7500.00',
        salesCount: 1,
        calculatedBy: bob.id,
        status: 'pending_approval',
        companyId: ACME_COMPANY_ID,
        businessUnitId: SALES_DIVISION_ID,
        locationId: LA_OFFICE_ID,
      },
    ]);

    console.log(`   ✓ Created 2 calculations\n`);

    // ==========================================
    // SUMMARY
    // ==========================================
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('📝 MULTI-LOCATION TEST DATA SUMMARY');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('\n🔐 ROLE PERMISSIONS (Database-Driven):');
    console.log('   admin:   Full access (39 menu items)');
    console.log('   owner:   Full access (22 menu items)');
    console.log('   manager: Standard access (21 menu items)');
    console.log('   editor:  Standard access (21 menu items)');
    console.log('   auditor: Limited access (7 menu items)');
    console.log('   viewer:  Minimal access (5 menu items)');
    console.log('\n👥 TEST USERS (Password: Test@123!)');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('\n1. alice.test - TWO CONTEXTS:');
    console.log('   📍 Context 1: Acme → Sales → NY Office [editor]');
    console.log('      → Full navigation (21 menu items)');
    console.log('   📍 Context 2: Rao Group → Dallas → Frisco [viewer]');
    console.log('      → Limited navigation (5 menu items)');
    console.log('\n2. bob.test - TWO CONTEXTS:');
    console.log('   📍 Context 1: Acme → Sales → LA Office [editor]');
    console.log('      → Full navigation (21 menu items)');
    console.log('   📍 Context 2: Acme → Sales → NY Office [manager]');
    console.log('      → Full navigation (21 menu items)');
    console.log('\n3. charlie.test - TWO CONTEXTS:');
    console.log('   📍 Context 1: Acme → Sales Division [manager]');
    console.log('      → Full navigation (21 menu items)');
    console.log('   📍 Context 2: Rao Group → Dallas → Frisco [auditor]');
    console.log('      → Audit-focused navigation (7 menu items)');
    console.log('\n4. diana.test - TWO CONTEXTS:');
    console.log('   📍 Context 1: Acme Corporation [owner]');
    console.log('      → Full navigation (22 menu items)');
    console.log('   📍 Context 2: Rao Group of Companies [viewer]');
    console.log('      → Limited navigation (5 menu items)');
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 TESTING SCENARIOS:');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('1. Login as alice.test → See context switcher in header');
    console.log('2. Switch from NY Office [editor] → Frisco [viewer]');
    console.log('3. Navigation menu should CHANGE based on role');
    console.log('4. Editor sees: Dashboard, Contracts, Analytics, etc.');
    console.log('5. Viewer sees: Dashboard, Contracts, Analytics, Reports only');
    console.log('═══════════════════════════════════════════════════════════════\n');
    console.log('✅ Test data created successfully!');

  } catch (error) {
    console.error('❌ Test data creation failed:', error);
    process.exit(1);
  }
}

createTestData()
  .then(() => {
    console.log('🎉 All done! Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  });
