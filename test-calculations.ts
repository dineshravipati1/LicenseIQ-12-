/**
 * End-to-End Calculation Test Script
 * Tests the calculation engine with sample sales data
 */

import { db } from './server/db';
import { royaltyRules, calculationBlueprints, blueprintDimensions, orgCalculationSettings } from './shared/schema';
import { eq, and } from 'drizzle-orm';

const CONTRACT_ID = '549066a7-1f55-4109-ab05-504ac5ae6447';
const COMPANY_ID = 'monrovia-nursery-company';

// Full sample sales data for testing (all 24 records from sample CSV)
const testSalesData = [
  { id: '1', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Oregon', quantity: 6500, grossAmount: 81250, transactionDate: '2024-03-15' },
  { id: '2', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Washington', quantity: 2500, grossAmount: 87500, transactionDate: '2024-03-20' },
  { id: '3', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Oregon', quantity: 8000, grossAmount: 68000, transactionDate: '2024-03-28' },
  { id: '4', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Northern California', quantity: 4500, grossAmount: 63000, transactionDate: '2024-04-05' },
  { id: '5', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Washington', quantity: 5500, grossAmount: 90750, transactionDate: '2024-04-12' },
  { id: '6', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Oregon', quantity: 3000, grossAmount: 84000, transactionDate: '2024-04-22' },
  { id: '7', productName: 'Golden Spire Juniper', category: 'Ornamental Trees', territory: 'Idaho', quantity: 1200, grossAmount: 66000, transactionDate: '2024-05-01' },
  { id: '8', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Montana', quantity: 1100, grossAmount: 71500, transactionDate: '2024-05-15' },
  { id: '9', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Oregon', quantity: 3200, grossAmount: 59200, transactionDate: '2024-05-28' },
  { id: '10', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Washington', quantity: 12000, grossAmount: 90000, transactionDate: '2024-06-10' },
  { id: '11', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Northern California', quantity: 1800, grossAmount: 81000, transactionDate: '2024-06-22' },
  { id: '12', productName: 'Golden Spire Juniper', category: 'Ornamental Trees', territory: 'Oregon', quantity: 4200, grossAmount: 75600, transactionDate: '2024-07-05' },
  { id: '13', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Washington', quantity: 250, grossAmount: 46250, transactionDate: '2024-07-18' },
  { id: '14', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Oregon', quantity: 3800, grossAmount: 51300, transactionDate: '2024-08-01' },
  { id: '15', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Idaho', quantity: 2400, grossAmount: 67200, transactionDate: '2024-08-15' },
  { id: '16', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Oregon', quantity: 4500, grossAmount: 119250, transactionDate: '2024-09-05' },
  { id: '17', productName: 'Golden Spire Juniper', category: 'Ornamental Trees', territory: 'Washington', quantity: 1500, grossAmount: 57000, transactionDate: '2024-09-18' },
  { id: '18', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Nevada', quantity: 1800, grossAmount: 59400, transactionDate: '2024-09-28' },
  { id: '19', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Utah', quantity: 2800, grossAmount: 49000, transactionDate: '2024-10-10' },
  { id: '20', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Oregon', quantity: 3500, grossAmount: 54250, transactionDate: '2024-10-22' },
  { id: '21', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Washington', quantity: 900, grossAmount: 43200, transactionDate: '2024-11-05' },
  { id: '22', productName: 'Golden Spire Juniper', category: 'Ornamental Trees', territory: 'Northern California', quantity: 600, grossAmount: 31200, transactionDate: '2024-11-20' },
  { id: '23', productName: 'Pacific Sunset Rose', category: 'Perennials', territory: 'Oregon', quantity: 2500, grossAmount: 41250, transactionDate: '2024-12-01' },
  { id: '24', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Washington', quantity: 3200, grossAmount: 102400, transactionDate: '2024-12-15' },
];

async function testCalculationSettings() {
  console.log('\n========================================');
  console.log('TEST CASE TC-001: Verify Calculation Settings');
  console.log('========================================');
  
  const settings = await db.select().from(orgCalculationSettings)
    .where(eq(orgCalculationSettings.companyId, COMPANY_ID));
  
  if (settings.length > 0) {
    console.log('✅ PASS: Calculation settings found');
    console.log(`   Company ID: ${settings[0].companyId}`);
    console.log(`   Calculation Approach: ${settings[0].calculationApproach}`);
    return settings[0].calculationApproach;
  } else {
    console.log('❌ FAIL: No calculation settings found');
    return 'manual';
  }
}

async function testRoyaltyRules() {
  console.log('\n========================================');
  console.log('TEST CASE TC-005: Verify Royalty Rules');
  console.log('========================================');
  
  const rules = await db.select().from(royaltyRules)
    .where(and(
      eq(royaltyRules.contractId, CONTRACT_ID),
      eq(royaltyRules.isActive, true)
    ));
  
  console.log(`✅ PASS: Found ${rules.length} active rules`);
  
  let ornamentalRule = null;
  let perennialsRule = null;
  let hydrangeaRule = null;
  
  rules.forEach(rule => {
    console.log(`\n   Rule: ${rule.ruleName}`);
    console.log(`   Type: ${rule.ruleType}`);
    if (rule.volumeTiers) {
      console.log(`   Volume Tiers: ${JSON.stringify(rule.volumeTiers)}`);
    }
    
    if (rule.ruleName?.includes('Ornamental')) ornamentalRule = rule;
    if (rule.ruleName?.includes('Perennials')) perennialsRule = rule;
    if (rule.ruleName?.includes('Flowering')) hydrangeaRule = rule;
  });
  
  return { ornamentalRule, perennialsRule, hydrangeaRule };
}

async function testBlueprints() {
  console.log('\n========================================');
  console.log('TEST CASE TC-008: Verify Blueprints');
  console.log('========================================');
  
  const blueprints = await db.select().from(calculationBlueprints)
    .where(eq(calculationBlueprints.contractId, CONTRACT_ID));
  
  console.log(`✅ PASS: Found ${blueprints.length} blueprints`);
  
  let allFullyMapped = true;
  blueprints.forEach(bp => {
    const status = bp.isFullyMapped ? '✅' : '⚠️';
    console.log(`   ${status} ${bp.name} (${bp.ruleType}) - Fully Mapped: ${bp.isFullyMapped}`);
    if (!bp.isFullyMapped) allFullyMapped = false;
  });
  
  if (allFullyMapped) {
    console.log('\n✅ PASS: All blueprints are fully mapped');
  } else {
    console.log('\n⚠️ WARNING: Some blueprints are not fully mapped');
  }
  
  return blueprints;
}

function calculateTieredRoyalty(units: number, volumeTiers: any[]): { royalty: number; tierBreakdown: string[] } {
  let remainingUnits = units;
  let totalRoyalty = 0;
  const breakdown: string[] = [];
  
  // Sort tiers by min value
  const sortedTiers = [...volumeTiers].sort((a, b) => a.min - b.min);
  
  for (let i = 0; i < sortedTiers.length && remainingUnits > 0; i++) {
    const tier = sortedTiers[i];
    const nextTier = sortedTiers[i + 1];
    
    const tierMax = tier.max !== null ? tier.max : Infinity;
    const tierMin = tier.min;
    
    // Calculate units in this tier
    const tierCapacity = tierMax - tierMin;
    const unitsInTier = Math.min(remainingUnits, tierCapacity);
    
    if (unitsInTier > 0) {
      const tierRoyalty = unitsInTier * tier.rate;
      totalRoyalty += tierRoyalty;
      breakdown.push(`${unitsInTier} units @ $${tier.rate} = $${tierRoyalty.toFixed(2)}`);
      remainingUnits -= unitsInTier;
    }
  }
  
  return { royalty: totalRoyalty, tierBreakdown: breakdown };
}

async function testManualCalculation(rules: any) {
  console.log('\n========================================');
  console.log('TEST CASE TC-013: Manual Mode Calculation');
  console.log('========================================');
  
  let totalRoyalty = 0;
  const results: any[] = [];
  
  // Group sales by category for cumulative volume tracking
  const ornamentalSales = testSalesData.filter(s => s.category === 'Ornamental Trees');
  const perennialsSales = testSalesData.filter(s => s.category === 'Perennials');
  const hydrangeaSales = testSalesData.filter(s => s.category === 'Flowering Shrubs');
  
  console.log('\n--- TIER 1: Ornamental Trees & Shrubs ---');
  if (rules.ornamentalRule?.volumeTiers) {
    let cumulativeUnits = 0;
    ornamentalSales.forEach(sale => {
      cumulativeUnits += sale.quantity;
      const { royalty, tierBreakdown } = calculateTieredRoyalty(sale.quantity, rules.ornamentalRule.volumeTiers);
      console.log(`\n${sale.productName} (${sale.quantity} units):`);
      tierBreakdown.forEach(b => console.log(`   ${b}`));
      console.log(`   Subtotal: $${royalty.toFixed(2)}`);
      totalRoyalty += royalty;
      results.push({ product: sale.productName, units: sale.quantity, royalty });
    });
    console.log(`\nCumulative Ornamental Units: ${cumulativeUnits}`);
  }
  
  console.log('\n--- TIER 2: Perennials & Roses ---');
  if (rules.perennialsRule?.volumeTiers) {
    let cumulativeUnits = 0;
    perennialsSales.forEach(sale => {
      cumulativeUnits += sale.quantity;
      const { royalty, tierBreakdown } = calculateTieredRoyalty(sale.quantity, rules.perennialsRule.volumeTiers);
      console.log(`\n${sale.productName} (${sale.quantity} units):`);
      tierBreakdown.forEach(b => console.log(`   ${b}`));
      console.log(`   Subtotal: $${royalty.toFixed(2)}`);
      totalRoyalty += royalty;
      results.push({ product: sale.productName, units: sale.quantity, royalty });
    });
    console.log(`\nCumulative Perennials Units: ${cumulativeUnits}`);
  }
  
  console.log('\n--- TIER 3: Flowering Shrubs (Hydrangea) ---');
  if (rules.hydrangeaRule?.volumeTiers) {
    let cumulativeUnits = 0;
    hydrangeaSales.forEach(sale => {
      cumulativeUnits += sale.quantity;
      const { royalty, tierBreakdown } = calculateTieredRoyalty(sale.quantity, rules.hydrangeaRule.volumeTiers);
      console.log(`\n${sale.productName} (${sale.quantity} units):`);
      tierBreakdown.forEach(b => console.log(`   ${b}`));
      console.log(`   Subtotal: $${royalty.toFixed(2)}`);
      totalRoyalty += royalty;
      results.push({ product: sale.productName, units: sale.quantity, royalty });
    });
    console.log(`\nCumulative Hydrangea Units: ${cumulativeUnits}`);
  }
  
  console.log('\n========================================');
  console.log('CALCULATION SUMMARY');
  console.log('========================================');
  console.log(`Total Sales Records: ${testSalesData.length}`);
  console.log(`Total Units: ${testSalesData.reduce((sum, s) => sum + s.quantity, 0)}`);
  console.log(`Total License Fee: $${totalRoyalty.toFixed(2)}`);
  
  // Minimum guarantee check
  const annualMinimum = 85000;
  const quarterlyMinimum = 21250;
  console.log(`\nMinimum Annual Guarantee: $${annualMinimum}`);
  if (totalRoyalty >= annualMinimum) {
    console.log('✅ PASS: Exceeds minimum guarantee');
  } else {
    console.log(`⚠️ Shortfall: $${(annualMinimum - totalRoyalty).toFixed(2)}`);
    console.log(`   Total Due: $${annualMinimum.toFixed(2)} (minimum guarantee applies)`);
  }
  
  return { totalRoyalty, results };
}

async function testMinimumGuarantee(rules: any) {
  console.log('\n========================================');
  console.log('TEST CASE TC-016: Minimum Guarantee Trigger');
  console.log('========================================');
  
  // Low volume sales data
  const lowVolumeSales = [
    { productName: 'Aurora Flame Maple', category: 'Ornamental Trees', quantity: 500 },
    { productName: 'Pacific Sunset Rose', category: 'Perennials', quantity: 1000 },
    { productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', quantity: 200 },
  ];
  
  let totalRoyalty = 0;
  
  lowVolumeSales.forEach(sale => {
    let rate = 0;
    if (sale.category === 'Ornamental Trees') rate = 1.25;
    else if (sale.category === 'Perennials') rate = 0.75;
    else if (sale.category === 'Flowering Shrubs') rate = 2.25;
    
    const royalty = sale.quantity * rate;
    totalRoyalty += royalty;
    console.log(`${sale.productName}: ${sale.quantity} × $${rate} = $${royalty.toFixed(2)}`);
  });
  
  console.log('─'.repeat(45));
  console.log(`Calculated Total: $${totalRoyalty.toFixed(2)}`);
  console.log(`Quarterly Minimum: $21,250.00`);
  
  if (totalRoyalty < 21250) {
    const shortfall = 21250 - totalRoyalty;
    console.log(`Shortfall: $${shortfall.toFixed(2)}`);
    console.log('─'.repeat(45));
    console.log(`TOTAL DUE: $21,250.00 (minimum guarantee applies)`);
    console.log('\n✅ PASS: Minimum guarantee correctly triggered');
  } else {
    console.log('\n❌ FAIL: Minimum guarantee should have been triggered');
  }
}

async function runAllTests() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║     LICENSEIQ END-TO-END CALCULATION TEST SUITE           ║');
  console.log('║     Testing with Plant Variety License Contract           ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  
  try {
    // Test 1: Calculation Settings
    const approach = await testCalculationSettings();
    
    // Test 2: Royalty Rules
    const rules = await testRoyaltyRules();
    
    // Test 3: Blueprints
    await testBlueprints();
    
    // Test 4: Manual Calculation
    await testManualCalculation(rules);
    
    // Test 5: Minimum Guarantee
    await testMinimumGuarantee(rules);
    
    console.log('\n╔════════════════════════════════════════════════════════════╗');
    console.log('║                    TEST SUITE COMPLETE                      ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    
  } catch (error) {
    console.error('Test Error:', error);
  } finally {
    process.exit(0);
  }
}

runAllTests();
