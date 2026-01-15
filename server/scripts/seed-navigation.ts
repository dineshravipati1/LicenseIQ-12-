/**
 * Navigation Seeding Script for Production
 * 
 * Seeds all navigation-related data:
 * - Navigation items (navigationPermissions)
 * - Navigation categories (navigationCategories)
 * - Item-to-category mappings (navigationItemCategories)
 * - Role permissions (roleNavigationPermissions)
 * 
 * Usage:
 *   npx tsx server/scripts/seed-navigation.ts
 * 
 * This script is idempotent - safe to run multiple times.
 */

import { db } from '../db';
import { 
  navigationPermissions, 
  navigationCategories, 
  navigationItemCategories, 
  roleNavigationPermissions 
} from '../../shared/schema';
import { eq } from 'drizzle-orm';

async function seedNavigation() {
  console.log('🌱 Seeding Navigation Data for Production...\n');

  try {
    // ==========================================
    // STEP 1: Seed Navigation Categories
    // ==========================================
    console.log('📁 STEP 1: Seeding Navigation Categories...');

    const categories = [
      { categoryKey: 'main', categoryName: 'Main', iconName: 'Home', isCollapsible: false, defaultExpanded: true, defaultSortOrder: 1 },
      { categoryKey: 'contracts', categoryName: 'Contracts', iconName: 'FileText', isCollapsible: true, defaultExpanded: true, defaultSortOrder: 2 },
      { categoryKey: 'finance', categoryName: 'Finance', iconName: 'DollarSign', isCollapsible: true, defaultExpanded: true, defaultSortOrder: 3 },
      { categoryKey: 'data', categoryName: 'Data Management', iconName: 'Database', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 4 },
      { categoryKey: 'ai', categoryName: 'AI & Analytics', iconName: 'Sparkles', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 5 },
      { categoryKey: 'admin', categoryName: 'Administration', iconName: 'Settings', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 6 },
    ];

    let categoriesCreated = 0;
    for (const cat of categories) {
      try {
        await db.insert(navigationCategories).values({
          categoryKey: cat.categoryKey,
          categoryName: cat.categoryName,
          iconName: cat.iconName,
          isCollapsible: cat.isCollapsible,
          defaultExpanded: cat.defaultExpanded,
          defaultSortOrder: cat.defaultSortOrder,
          isActive: true,
        }).onConflictDoUpdate({
          target: navigationCategories.categoryKey,
          set: {
            categoryName: cat.categoryName,
            iconName: cat.iconName,
            isCollapsible: cat.isCollapsible,
            defaultExpanded: cat.defaultExpanded,
            defaultSortOrder: cat.defaultSortOrder,
            isActive: true,
            updatedAt: new Date(),
          }
        });
        categoriesCreated++;
        console.log(`   ✓ Category: ${cat.categoryName}`);
      } catch (err) {
        console.error(`   ✗ Failed to seed category ${cat.categoryKey}:`, err);
      }
    }
    console.log(`   → ${categoriesCreated} categories seeded\n`);

    // ==========================================
    // STEP 2: Seed Navigation Items
    // ==========================================
    console.log('🔗 STEP 2: Seeding Navigation Items...');

    const navItems = [
      { itemKey: 'dashboard', itemName: 'Dashboard', href: '/', iconName: 'LayoutDashboard', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 1 },
      { itemKey: 'contracts', itemName: 'Contracts', href: '/contracts', iconName: 'FileText', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 2 },
      { itemKey: 'contract-upload', itemName: 'Upload Contract', href: '/contracts/upload', iconName: 'Upload', defaultRoles: ['editor', 'manager', 'admin', 'owner'], sortOrder: 3 },
      { itemKey: 'royalty-rules', itemName: 'License Fee Rules', href: '/royalty-rules', iconName: 'Scale', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 4 },
      { itemKey: 'calculations', itemName: 'License Fee Calculator', href: '/calculations', iconName: 'Calculator', defaultRoles: ['analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 5 },
      { itemKey: 'sales-data', itemName: 'Sales Data', href: '/sales', iconName: 'TrendingUp', defaultRoles: ['analyst', 'manager', 'admin', 'owner'], sortOrder: 7 },
      { itemKey: 'erp-mapping', itemName: 'ERP Mapping', href: '/erp-mapping', iconName: 'ArrowLeftRight', defaultRoles: ['admin', 'owner'], sortOrder: 8 },
      { itemKey: 'erp-catalog', itemName: 'ERP Catalog', href: '/erp-catalog', iconName: 'Layers', defaultRoles: ['admin', 'owner'], sortOrder: 9 },
      { itemKey: 'licenseiq-catalog', itemName: 'LicenseIQ Catalog', href: '/licenseiq-catalog', iconName: 'BookOpen', defaultRoles: ['admin', 'owner'], sortOrder: 10 },
      { itemKey: 'liq-ai', itemName: 'liQ AI', href: '/liq-ai', iconName: 'Bot', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 11 },
      { itemKey: 'knowledge-base', itemName: 'Knowledge Base', href: '/knowledge-base', iconName: 'BookOpen', defaultRoles: ['admin', 'owner'], sortOrder: 12 },
      { itemKey: 'rag-dashboard', itemName: 'RAG Dashboard', href: '/rag-dashboard', iconName: 'Sparkles', defaultRoles: ['admin', 'owner'], sortOrder: 13 },
      { itemKey: 'analytics', itemName: 'Analytics', href: '/analytics', iconName: 'TrendingUp', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 14 },
      { itemKey: 'reports', itemName: 'Reports', href: '/reports', iconName: 'FileText', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 15 },
      { itemKey: 'lead-management', itemName: 'Lead Management', href: '/admin/leads', iconName: 'Mail', defaultRoles: ['admin', 'owner'], sortOrder: 16 },
      { itemKey: 'review-queue', itemName: 'Review Queue', href: '/review-queue', iconName: 'ClipboardCheck', defaultRoles: ['admin', 'owner'], sortOrder: 17 },
      { itemKey: 'user-management', itemName: 'User Management', href: '/users', iconName: 'Users', defaultRoles: ['admin', 'owner'], sortOrder: 18 },
      { itemKey: 'audit-trail', itemName: 'Audit Trail', href: '/audit', iconName: 'History', defaultRoles: ['auditor', 'admin', 'owner'], sortOrder: 19 },
      { itemKey: 'configuration', itemName: 'Configuration', href: '/configuration', iconName: 'Sparkles', defaultRoles: ['admin', 'owner'], sortOrder: 20 },
      { itemKey: 'master-data', itemName: 'Master Data', href: '/master-data', iconName: 'Database', defaultRoles: ['admin', 'owner'], sortOrder: 21 },
      { itemKey: 'erp-mapping-rules', itemName: 'ERP Mapping Rules', href: '/erp-mapping-rules', iconName: 'Scale', defaultRoles: ['admin', 'owner'], sortOrder: 22 },
    ];

    let itemsCreated = 0;
    for (const item of navItems) {
      try {
        await db.insert(navigationPermissions).values({
          itemKey: item.itemKey,
          itemName: item.itemName,
          href: item.href,
          iconName: item.iconName,
          defaultRoles: item.defaultRoles,
          sortOrder: item.sortOrder,
          isActive: true,
        }).onConflictDoUpdate({
          target: navigationPermissions.itemKey,
          set: {
            itemName: item.itemName,
            href: item.href,
            iconName: item.iconName,
            defaultRoles: item.defaultRoles,
            sortOrder: item.sortOrder,
            isActive: true,
            updatedAt: new Date(),
          }
        });
        itemsCreated++;
        console.log(`   ✓ Item: ${item.itemName}`);
      } catch (err) {
        console.error(`   ✗ Failed to seed item ${item.itemKey}:`, err);
      }
    }
    console.log(`   → ${itemsCreated} navigation items seeded\n`);

    // ==========================================
    // STEP 3: Seed Item-to-Category Mappings
    // ==========================================
    console.log('🔗 STEP 3: Seeding Item-to-Category Mappings...');

    const itemCategoryMappings = [
      // Main category
      { navItemKey: 'dashboard', categoryKey: 'main', sortOrder: 1 },
      
      // Contracts category
      { navItemKey: 'contracts', categoryKey: 'contracts', sortOrder: 1 },
      { navItemKey: 'contract-upload', categoryKey: 'contracts', sortOrder: 2 },
      { navItemKey: 'royalty-rules', categoryKey: 'contracts', sortOrder: 3 },
      { navItemKey: 'review-queue', categoryKey: 'contracts', sortOrder: 4 },
      
      // Finance category
      { navItemKey: 'calculations', categoryKey: 'finance', sortOrder: 1 },
      { navItemKey: 'sales-data', categoryKey: 'finance', sortOrder: 2 },
      
      // Data Management category
      { navItemKey: 'erp-mapping', categoryKey: 'data', sortOrder: 1 },
      { navItemKey: 'erp-catalog', categoryKey: 'data', sortOrder: 2 },
      { navItemKey: 'licenseiq-catalog', categoryKey: 'data', sortOrder: 3 },
      { navItemKey: 'master-data', categoryKey: 'data', sortOrder: 4 },
      { navItemKey: 'erp-mapping-rules', categoryKey: 'data', sortOrder: 5 },
      
      // AI & Analytics category
      { navItemKey: 'liq-ai', categoryKey: 'ai', sortOrder: 1 },
      { navItemKey: 'knowledge-base', categoryKey: 'ai', sortOrder: 2 },
      { navItemKey: 'rag-dashboard', categoryKey: 'ai', sortOrder: 3 },
      { navItemKey: 'analytics', categoryKey: 'ai', sortOrder: 4 },
      { navItemKey: 'reports', categoryKey: 'ai', sortOrder: 5 },
      
      // Administration category
      { navItemKey: 'user-management', categoryKey: 'admin', sortOrder: 1 },
      { navItemKey: 'lead-management', categoryKey: 'admin', sortOrder: 2 },
      { navItemKey: 'audit-trail', categoryKey: 'admin', sortOrder: 3 },
      { navItemKey: 'configuration', categoryKey: 'admin', sortOrder: 4 },
    ];

    let mappingsCreated = 0;
    for (const mapping of itemCategoryMappings) {
      try {
        // Check if mapping already exists
        const existing = await db.select()
          .from(navigationItemCategories)
          .where(eq(navigationItemCategories.navItemKey, mapping.navItemKey))
          .limit(1);
        
        if (existing.length > 0) {
          // Update existing mapping
          await db.update(navigationItemCategories)
            .set({
              categoryKey: mapping.categoryKey,
              sortOrder: mapping.sortOrder,
              updatedAt: new Date(),
            })
            .where(eq(navigationItemCategories.navItemKey, mapping.navItemKey));
        } else {
          // Insert new mapping
          await db.insert(navigationItemCategories).values({
            navItemKey: mapping.navItemKey,
            categoryKey: mapping.categoryKey,
            sortOrder: mapping.sortOrder,
          });
        }
        mappingsCreated++;
      } catch (err) {
        console.error(`   ✗ Failed to map ${mapping.navItemKey} → ${mapping.categoryKey}:`, err);
      }
    }
    console.log(`   → ${mappingsCreated} item-category mappings seeded\n`);

    // ==========================================
    // STEP 4: Seed Role Permissions
    // ==========================================
    console.log('🔐 STEP 4: Seeding Role Navigation Permissions...');

    const allRoles = ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'];
    let permissionsCreated = 0;

    for (const item of navItems) {
      for (const role of allRoles) {
        const isEnabled = item.defaultRoles.includes(role);
        try {
          await db.insert(roleNavigationPermissions).values({
            role,
            navItemKey: item.itemKey,
            isEnabled,
          }).onConflictDoNothing();
          permissionsCreated++;
        } catch (err) {
          // Ignore duplicates
        }
      }
    }
    console.log(`   → ${permissionsCreated} role permissions seeded\n`);

    // ==========================================
    // Summary
    // ==========================================
    console.log('✅ Navigation Seeding Complete!');
    console.log('=====================================');
    console.log(`   Categories:     ${categoriesCreated}`);
    console.log(`   Nav Items:      ${itemsCreated}`);
    console.log(`   Mappings:       ${mappingsCreated}`);
    console.log(`   Permissions:    ${permissionsCreated}`);
    console.log('=====================================\n');
    console.log('🎉 Navigation should now be visible in the application.');

  } catch (error) {
    console.error('❌ Navigation seeding failed:', error);
    process.exit(1);
  }

  process.exit(0);
}

seedNavigation();
