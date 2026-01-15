import { db } from './db';
import { licenseiqEntities, licenseiqFields, type InsertLicenseiqEntity, type InsertLicenseiqField } from '@shared/schema';

/**
 * Seed LicenseIQ Schema Catalog with standard entities and fields
 * Now incrementally adds missing entities instead of all-or-nothing
 */
export async function seedLicenseIQSchema() {
  console.log('🌱 Seeding LicenseIQ Schema Catalog...');

  try {
    // Check which entities already exist
    const existingEntities = await db.select().from(licenseiqEntities);
    const existingTechnicalNames = new Set(existingEntities.map(e => e.technicalName));
    
    // Define core entities only - Items and Vendors (as requested by user)
    // Additional entities can be added through the UI
    const standardEntities: InsertLicenseiqEntity[] = [
      { name: 'Items', technicalName: 'items', category: 'Master Data', description: 'Item master data' },
      { name: 'Vendors', technicalName: 'vendors', category: 'Master Data', description: 'Vendor master data' },
    ];

    // Only seed entities that don't exist yet
    let newEntitiesCount = 0;
    for (const entityData of standardEntities) {
      if (!existingTechnicalNames.has(entityData.technicalName)) {
        await db.insert(licenseiqEntities).values(entityData);
        newEntitiesCount++;
        console.log(`  ✓ Added: ${entityData.name}`);
      }
    }
    
    if (newEntitiesCount === 0) {
      console.log(`✓ Core LicenseIQ schema entities already exist (${existingEntities.length} entities total)`);
      return;
    }
    
    console.log(`✅ Added ${newEntitiesCount} new entities to LicenseIQ schema (${existingEntities.length} already existed)`);
  } catch (error) {
    console.error('❌ Error seeding LicenseIQ schema:', error);
    throw error;
  }
}

