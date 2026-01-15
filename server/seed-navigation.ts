/**
 * Navigation Seeding Module
 * 
 * Seeds all navigation-related data on server startup:
 * - Navigation categories
 * - Navigation items  
 * - Item-to-category mappings
 * - Role permissions
 * 
 * This runs automatically on every server start and is idempotent.
 */

import { db } from './db';
import { 
  navigationPermissions, 
  navigationCategories, 
  navigationItemCategories, 
  roleNavigationPermissions 
} from '../shared/schema';
import { eq } from 'drizzle-orm';

export async function seedNavigation() {
  console.log('🌱 Seeding/Updating Navigation System...');

  try {
    // Always run upserts to ensure navigation data is up-to-date
    // The onConflictDoUpdate ensures existing records get updated with latest paths

    // ==========================================
    // STEP 1: Seed Navigation Categories
    // ==========================================
    const categories = [
      { categoryKey: 'main', categoryName: 'Main', iconName: 'Home', isCollapsible: false, defaultExpanded: true, defaultSortOrder: 1 },
      { categoryKey: 'contracts', categoryName: 'Contracts', iconName: 'FileText', isCollapsible: true, defaultExpanded: true, defaultSortOrder: 2 },
      { categoryKey: 'finance', categoryName: 'Finance', iconName: 'DollarSign', isCollapsible: true, defaultExpanded: true, defaultSortOrder: 3 },
      { categoryKey: 'data', categoryName: 'Data Management', iconName: 'Database', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 4 },
      { categoryKey: 'ai', categoryName: 'AI & Analytics', iconName: 'Sparkles', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 5 },
      { categoryKey: 'admin', categoryName: 'Administration', iconName: 'Settings', isCollapsible: true, defaultExpanded: false, defaultSortOrder: 6 },
    ];

    for (const cat of categories) {
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
    }

    // ==========================================
    // STEP 2: Seed Navigation Items
    // ==========================================
    const navItems = [
      { itemKey: 'dashboard', itemName: 'Dashboard', href: '/', iconName: 'LayoutDashboard', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 1 },
      { itemKey: 'contracts', itemName: 'Contracts', href: '/contracts', iconName: 'FileText', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 2 },
      { itemKey: 'contract-upload', itemName: 'Upload Contract', href: '/upload', iconName: 'Upload', defaultRoles: ['editor', 'manager', 'admin', 'owner'], sortOrder: 3 },
      { itemKey: 'royalty-rules', itemName: 'License Fee Rules', href: '/contracts', iconName: 'Scale', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 4 },
      { itemKey: 'calculations', itemName: 'License Fee Calculator', href: '/calculations', iconName: 'Calculator', defaultRoles: ['analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 5 },
      { itemKey: 'sales-data', itemName: 'Sales Data', href: '/sales-upload', iconName: 'TrendingUp', defaultRoles: ['analyst', 'manager', 'admin', 'owner'], sortOrder: 7 },
      { itemKey: 'erp-hub', itemName: 'Integration Overview', href: '/erp-hub', iconName: 'Plug', defaultRoles: ['admin', 'owner'], sortOrder: 8 },
      { itemKey: 'erp-mapping', itemName: 'ERP Mapping', href: '/master-data-mapping', iconName: 'ArrowLeftRight', defaultRoles: ['admin', 'owner'], sortOrder: 9 },
      { itemKey: 'erp-integration', itemName: 'ERP Integration Hub', href: '/erp-integration', iconName: 'Database', defaultRoles: ['admin', 'owner'], sortOrder: 10 },
      { itemKey: 'erp-catalog', itemName: 'ERP Catalog', href: '/erp-catalog', iconName: 'Layers', defaultRoles: ['admin', 'owner'], sortOrder: 11 },
      { itemKey: 'licenseiq-catalog', itemName: 'LicenseIQ Schema', href: '/licenseiq-schema', iconName: 'BookOpen', defaultRoles: ['admin', 'owner'], sortOrder: 10 },
      { itemKey: 'liq-ai', itemName: 'liQ AI', href: '/contract-qna', iconName: 'Bot', defaultRoles: ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'], sortOrder: 11 },
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
      { itemKey: 'navigation-manager', itemName: 'Navigation Manager', href: '/navigation-manager', iconName: 'Settings', defaultRoles: ['owner'], sortOrder: 22 },
    ];

    for (const item of navItems) {
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
    }

    // ==========================================
    // STEP 3: Seed Item-to-Category Mappings
    // ==========================================
    const itemCategoryMappings = [
      { navItemKey: 'dashboard', categoryKey: 'main', sortOrder: 1 },
      { navItemKey: 'contracts', categoryKey: 'contracts', sortOrder: 1 },
      { navItemKey: 'contract-upload', categoryKey: 'contracts', sortOrder: 2 },
      { navItemKey: 'royalty-rules', categoryKey: 'contracts', sortOrder: 3 },
      { navItemKey: 'review-queue', categoryKey: 'contracts', sortOrder: 4 },
      { navItemKey: 'calculations', categoryKey: 'finance', sortOrder: 1 },
      { navItemKey: 'sales-data', categoryKey: 'finance', sortOrder: 3 },
      { navItemKey: 'erp-hub', categoryKey: 'data', sortOrder: 1 },
      { navItemKey: 'erp-mapping', categoryKey: 'data', sortOrder: 2 },
      { navItemKey: 'erp-integration', categoryKey: 'data', sortOrder: 3 },
      { navItemKey: 'erp-catalog', categoryKey: 'data', sortOrder: 4 },
      { navItemKey: 'licenseiq-catalog', categoryKey: 'data', sortOrder: 5 },
      { navItemKey: 'master-data', categoryKey: 'data', sortOrder: 6 },
      { navItemKey: 'liq-ai', categoryKey: 'ai', sortOrder: 1 },
      { navItemKey: 'knowledge-base', categoryKey: 'ai', sortOrder: 2 },
      { navItemKey: 'rag-dashboard', categoryKey: 'ai', sortOrder: 3 },
      { navItemKey: 'analytics', categoryKey: 'ai', sortOrder: 4 },
      { navItemKey: 'reports', categoryKey: 'ai', sortOrder: 5 },
      { navItemKey: 'user-management', categoryKey: 'admin', sortOrder: 1 },
      { navItemKey: 'lead-management', categoryKey: 'admin', sortOrder: 2 },
      { navItemKey: 'audit-trail', categoryKey: 'admin', sortOrder: 3 },
      { navItemKey: 'configuration', categoryKey: 'admin', sortOrder: 4 },
      { navItemKey: 'navigation-manager', categoryKey: 'admin', sortOrder: 5 },
    ];

    for (const mapping of itemCategoryMappings) {
      const existing = await db.select()
        .from(navigationItemCategories)
        .where(eq(navigationItemCategories.navItemKey, mapping.navItemKey))
        .limit(1);
      
      if (existing.length > 0) {
        await db.update(navigationItemCategories)
          .set({
            categoryKey: mapping.categoryKey,
            sortOrder: mapping.sortOrder,
            updatedAt: new Date(),
          })
          .where(eq(navigationItemCategories.navItemKey, mapping.navItemKey));
      } else {
        await db.insert(navigationItemCategories).values({
          navItemKey: mapping.navItemKey,
          categoryKey: mapping.categoryKey,
          sortOrder: mapping.sortOrder,
        });
      }
    }

    // ==========================================
    // STEP 4: Seed Role Permissions
    // ==========================================
    const allRoles = ['viewer', 'editor', 'analyst', 'auditor', 'manager', 'admin', 'owner'];

    for (const item of navItems) {
      for (const role of allRoles) {
        const isEnabled = item.defaultRoles.includes(role);
        await db.insert(roleNavigationPermissions).values({
          role,
          navItemKey: item.itemKey,
          isEnabled,
        }).onConflictDoNothing();
      }
    }

    console.log(`✅ Navigation seeding complete: ${categories.length} categories, ${navItems.length} items, ${itemCategoryMappings.length} mappings`);

  } catch (error: any) {
    console.error('⚠ Navigation seeding warning:', error.message);
  }
}
