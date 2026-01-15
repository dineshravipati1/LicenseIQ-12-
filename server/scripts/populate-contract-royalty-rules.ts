import { storage } from '../storage';

// Populate royalty rules for the Plant Variety License & Royalty Agreement
async function populateContractRoyaltyRules(contractId: string) {
  try {
    console.log(`Populating royalty rules for contract ${contractId}...`);

    // Define the royalty rules based on the contract analysis
    const rules = [
      // Tier 1 - Ornamental Trees & Shrubs (1-gallon containers)
      {
        contractId,
        ruleType: 'tiered_pricing',
        ruleName: 'Tier 1 - Trees & Shrubs (1-gallon)',
        description: 'Ornamental Trees & Shrubs in 1-gallon containers with volume discounts',
        productCategories: ['Ornamental Trees', 'Ornamental Shrubs'],
        territories: ['Primary', 'Secondary'],
        containerSizes: ['1-gallon'],
        seasonalAdjustments: {
          Spring: 1.10,
          Summer: 1.0,
          Fall: 0.95,
          'Off-Season': 0.85,
          Holiday: 1.20
        },
        territoryPremiums: {
          Primary: 1.0,
          Secondary: 1.10
        },
        volumeTiers: [
          { minQuantity: 0, maxQuantity: 4999, rate: 1.25 },
          { minQuantity: 5000, maxQuantity: null, rate: 1.10 }
        ],
        baseRate: '1.25',
        calculationFormula: 'quantity * tier_rate * seasonal_adjustment * territory_premium',
        priority: 1,
        isActive: true,
        confidence: '0.95',
        sourceSection: 'Section 3.1 - Royalty Structure',
        sourceText: 'Tier 1 - Ornamental Trees & Shrubs: $1.25/unit for 1-gallon containers, volume discount $1.10 for 5,000+ units'
      },

      // Tier 1 - Flowering Shrubs with special rates
      {
        contractId,
        ruleType: 'tiered_pricing',
        ruleName: 'Tier 1 - Flowering Shrubs',
        description: 'Flowering Shrubs with size-based multipliers and seasonal adjustments',
        productCategories: ['Flowering Shrubs'],
        territories: ['Primary', 'Secondary'],
        containerSizes: ['6-inch', '1-gallon', '2-gallon', '3-gallon', 'Mixed'],
        seasonalAdjustments: {
          Spring: 1.10,
          Summer: 1.0,
          Fall: 0.95,
          'Off-Season': 0.85,
          Holiday: 1.20
        },
        territoryPremiums: {
          Primary: 1.0,
          Secondary: 1.10
        },
        volumeTiers: [
          { minQuantity: 0, maxQuantity: 9999, rate: 1.15 },
          { minQuantity: 10000, maxQuantity: null, rate: 1.45 }
        ],
        baseRate: '1.15',
        calculationFormula: 'quantity * tier_rate * size_multiplier * seasonal_adjustment * territory_premium',
        priority: 2,
        isActive: true,
        confidence: '0.90',
        sourceSection: 'Section 3.1 - Royalty Structure',
        sourceText: 'Flowering Shrubs: Base $1.15/unit with size multipliers (6-inch: 1.2x, 1-gallon: 1.3x, 2-gallon+: 1.4x)'
      },

      // Tier 2 - Perennials
      {
        contractId,
        ruleType: 'tiered_pricing',
        ruleName: 'Tier 2 - Perennials',
        description: 'Perennial plants with organic premium and size-based rates',
        productCategories: ['Perennials'],
        territories: ['Primary', 'Secondary'],
        containerSizes: ['1-gallon', '2-gallon', '3-gallon'],
        seasonalAdjustments: {
          Spring: 1.10,
          Summer: 1.0,
          Fall: 0.95,
          'Off-Season': 0.85,
          Holiday: 1.20
        },
        territoryPremiums: {
          Primary: 1.0,
          Secondary: 1.10
        },
        volumeTiers: [
          { minQuantity: 0, maxQuantity: null, rate: 3.25 }
        ],
        baseRate: '3.25',
        calculationFormula: 'quantity * base_rate * size_multiplier * seasonal_adjustment * territory_premium * organic_premium',
        priority: 3,
        isActive: true,
        confidence: '0.88',
        sourceSection: 'Section 3.1 - Royalty Structure',
        sourceText: 'Tier 2 - Perennials: Base $3.25/unit with size multipliers (1-gallon: 1.2x, 2-gallon+: 1.5x), 25% organic premium'
      },

      // Tier 3 - Ornamental Shrubs (large containers)
      {
        contractId,
        ruleType: 'tiered_pricing',
        ruleName: 'Tier 3 - Shrubs (5-gallon)',
        description: 'Large container ornamental shrubs with volume discounts',
        productCategories: ['Ornamental Shrubs'],
        territories: ['Primary', 'Secondary'],
        containerSizes: ['5-gallon'],
        seasonalAdjustments: {
          Spring: 1.10,
          Summer: 1.0,
          Fall: 0.95,
          'Off-Season': 0.85,
          Holiday: 1.20
        },
        territoryPremiums: {
          Primary: 1.0,
          Secondary: 1.10
        },
        volumeTiers: [
          { minQuantity: 0, maxQuantity: 999, rate: 4.20 },
          { minQuantity: 1000, maxQuantity: null, rate: 3.95 }
        ],
        baseRate: '4.20',
        calculationFormula: 'quantity * tier_rate * seasonal_adjustment * territory_premium',
        priority: 4,
        isActive: true,
        confidence: '0.92',
        sourceSection: 'Section 3.1 - Royalty Structure',
        sourceText: 'Tier 3 - 5-gallon containers: $4.20/unit, volume discount $3.95 for 1,000+ units'
      },

      // Minimum Annual Guarantee
      {
        contractId,
        ruleType: 'minimum_guarantee',
        ruleName: 'Minimum Annual Guarantee',
        description: 'Minimum annual royalty payment of $85,000',
        productCategories: null,
        territories: null,
        containerSizes: null,
        seasonalAdjustments: null,
        territoryPremiums: null,
        volumeTiers: null,
        baseRate: null,
        minimumGuarantee: '85000.00',
        calculationFormula: 'MAX(calculated_royalty, 85000)',
        priority: 100,
        isActive: true,
        confidence: '0.95',
        sourceSection: 'Section 3.2 - Minimum Guarantee',
        sourceText: 'Minimum annual royalty: $85,000 payable in quarterly installments of $21,250'
      }
    ];

    // Insert all rules
    for (const rule of rules) {
      await storage.createRoyaltyRule(rule);
      console.log(`✓ Created rule: ${rule.ruleName}`);
    }

    console.log(`\n✅ Successfully populated ${rules.length} royalty rules for contract ${contractId}`);
    return rules.length;

  } catch (error) {
    console.error('Error populating royalty rules:', error);
    throw error;
  }
}

// Get contract ID from command line or use latest
async function main() {
  const contractId = process.argv[2];
  
  if (!contractId) {
    console.error('Usage: tsx server/scripts/populate-contract-royalty-rules.ts <contractId>');
    process.exit(1);
  }

  await populateContractRoyaltyRules(contractId);
  process.exit(0);
}

main();
