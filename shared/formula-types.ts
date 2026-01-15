/**
 * JSON-based Formula Expression Tree
 * 
 * This schema supports dynamic royalty calculation formulas with varying structures.
 * Each formula is represented as an expression tree of nodes that can be evaluated.
 */

// Base formula node interface
export interface FormulaNode {
  type: string;
  description?: string;
}

// Literal value node (e.g., 1.25, 100, "Primary")
export interface LiteralNode extends FormulaNode {
  type: 'literal';
  value: number | string;
  unit?: string; // 'dollars', 'percent', 'multiplier'
}

// Reference to sales data field (e.g., units, season, territory)
export interface ReferenceNode extends FormulaNode {
  type: 'reference';
  field: string; // 'units', 'season', 'territory', 'containerSize', 'productName', etc.
}

// Volume tier lookup
export interface TierNode extends FormulaNode {
  type: 'tier';
  reference: ReferenceNode; // What to compare (usually units)
  tiers: Array<{
    min: number;
    max: number | null; // null = infinity
    rate: number;
    label?: string;
  }>;
}

// Forward declaration for recursive types
export type AnyFormulaNode = 
  | LiteralNode 
  | ReferenceNode 
  | TierNode 
  | MultiplyNode 
  | AddNode 
  | SubtractNode
  | MaxNode 
  | MinNode 
  | IfNode 
  | LookupNode
  | PremiumNode
  | RoundNode;

// Multiplication operation
export interface MultiplyNode extends FormulaNode {
  type: 'multiply';
  operands: AnyFormulaNode[];
}

// Addition operation
export interface AddNode extends FormulaNode {
  type: 'add';
  operands: AnyFormulaNode[];
}

// Subtraction operation
export interface SubtractNode extends FormulaNode {
  type: 'subtract';
  operands: [AnyFormulaNode, AnyFormulaNode]; // left - right
}

// Maximum value
export interface MaxNode extends FormulaNode {
  type: 'max';
  operands: AnyFormulaNode[];
}

// Minimum value
export interface MinNode extends FormulaNode {
  type: 'min';
  operands: AnyFormulaNode[];
}

// Conditional operation
export interface IfNode extends FormulaNode {
  type: 'if';
  condition: {
    field: string;
    operator: 'equals' | 'contains' | 'greaterThan' | 'lessThan' | 'in';
    value: any;
  };
  then: AnyFormulaNode;
  else?: AnyFormulaNode;
}

// Lookup table (for seasonal adjustments, territory premiums, etc.)
export interface LookupNode extends FormulaNode {
  type: 'lookup';
  reference: AnyFormulaNode; // Field to lookup (e.g., season, territory)
  table: Record<string, number>; // { "Spring": 1.10, "Summer": 1.05, ... }
  default?: number; // Default if not found
}

/**
 * Apply premium/discount percentage to a base value
 * 
 * IMPORTANT: Choose the correct mode:
 * - 'additive': Applies percentage as adjustment to get final total
 *   Formula: base × (1 + percentage)
 *   Example: $100 base with +25% premium → $100 × 1.25 = $125
 *   Use when: Adding a premium/discount to get the final amount
 * 
 * - 'multiplicative': Multiplies base by percentage directly
 *   Formula: base × percentage
 *   Example: $100 base with 0.25 → $100 × 0.25 = $25 (just the premium portion)
 *   Use when: Calculating only the premium/discount portion (rare)
 */
export interface PremiumNode extends FormulaNode {
  type: 'premium';
  base: AnyFormulaNode; // Base value to apply premium to
  percentage: number; // e.g., 0.25 for +25%, -0.05 for -5%
  mode: 'additive' | 'multiplicative';
}

/**
 * Round a value to specified precision
 * Used when contracts specify intermediate rounding steps
 */
export interface RoundNode extends FormulaNode {
  type: 'round';
  value: AnyFormulaNode; // Value to round
  precision: number; // Decimal places (e.g., 2 for cents, 3 for thousandths)
  mode?: 'round' | 'floor' | 'ceil'; // Rounding mode, defaults to 'round' (standard rounding)
}

// Complete formula definition with metadata
export interface FormulaDefinition {
  version: string; // "1.0", "2.0" for versioning
  name: string; // Human-readable name
  description?: string;
  
  // Applicability filters
  filters?: {
    products?: string[]; // Apply only to these products
    territories?: string[]; // Apply only to these territories
    containerSizes?: string[]; // Apply only to these sizes
    dateRange?: {
      start?: string;
      end?: string;
    };
  };
  
  // The actual formula as an expression tree
  formula: AnyFormulaNode;
  
  // Display template for UI (e.g., "units × tier_rate")
  displayTemplate?: string;
  
  // Metadata
  createdAt?: string;
  createdBy?: string;
  confidence?: number; // AI extraction confidence (0-1)
}

// Example formulas for each pattern

/**
 * Example 1: Simple Volume Tier
 * Aurora Flame Maple: 6,200 × $1.10 = $6,820
 */
export const exampleVolumeTier: FormulaDefinition = {
  version: "1.0",
  name: "Volume Tier - Aurora Flame Maple 1-gallon",
  description: "Tier 1 with discount after 5,000 units",
  filters: {
    products: ["Aurora Flame Maple"],
    containerSizes: ["1-gallon"]
  },
  formula: {
    type: 'multiply',
    operands: [
      { type: 'reference', field: 'units' } as ReferenceNode,
      {
        type: 'tier',
        reference: { type: 'reference', field: 'units' } as ReferenceNode,
        tiers: [
          { min: 0, max: 5000, rate: 1.25, label: "Base rate" },
          { min: 5001, max: null, rate: 1.10, label: "Volume discount" }
        ]
      } as TierNode
    ]
  } as MultiplyNode,
  displayTemplate: "units × tier_rate"
};

/**
 * Example 2: Base Rate + Territory Premium
 * Golden Spire Juniper: 1,800 × $3.135 = $5,643
 * (base $2.85 + 10% secondary territory premium)
 */
export const exampleTerritoryPremium: FormulaDefinition = {
  version: "1.0",
  name: "Base + Territory Premium - Golden Spire Juniper",
  filters: {
    products: ["Golden Spire Juniper"],
    containerSizes: ["3-gallon"]
  },
  formula: {
    type: 'multiply',
    operands: [
      { type: 'reference', field: 'units' } as ReferenceNode,
      {
        type: 'add',
        operands: [
          { type: 'literal', value: 2.85, unit: 'dollars' } as LiteralNode,
          {
            type: 'multiply',
            operands: [
              { type: 'literal', value: 2.85, unit: 'dollars' } as LiteralNode,
              {
                type: 'lookup',
                reference: { type: 'reference', field: 'territory' } as ReferenceNode,
                table: {
                  "Primary": 0,
                  "Secondary": 0.10,
                  "International": 0.20
                },
                default: 0
              } as LookupNode
            ]
          } as MultiplyNode
        ]
      } as AddNode
    ]
  } as MultiplyNode,
  displayTemplate: "units × (base_rate + territory_premium)"
};

/**
 * Example 3: Multiplier Chain with Seasonal
 * Pacific Sunset Rose: 3,000 × (1.15 × 1.2 × 1.10) = $4,554
 */
export const exampleMultiplierChain: FormulaDefinition = {
  version: "1.0",
  name: "Multiplier Chain - Pacific Sunset Rose",
  filters: {
    products: ["Pacific Sunset Rose"],
    containerSizes: ["6-inch"]
  },
  formula: {
    type: 'multiply',
    operands: [
      { type: 'reference', field: 'units' } as ReferenceNode,
      {
        type: 'multiply',
        operands: [
          { type: 'literal', value: 1.15, unit: 'dollars', description: 'Tier 2 base' } as LiteralNode,
          { type: 'literal', value: 1.2, unit: 'multiplier', description: 'Tier 2 multiplier' } as LiteralNode,
          {
            type: 'lookup',
            reference: { type: 'reference', field: 'season' } as ReferenceNode,
            table: {
              "Spring": 1.10,
              "Summer": 1.05,
              "Fall": 0.95,
              "Winter": 1.00,
              "Holiday": 1.20,
              "Off-Season": 0.90
            },
            default: 1.0,
            description: 'Seasonal adjustment'
          } as LookupNode
        ]
      } as MultiplyNode
    ]
  } as MultiplyNode,
  displayTemplate: "units × (base × multiplier × seasonal_adj)"
};

/**
 * Example 4: Complex with Organic Premium
 * Emerald Crown Hosta, Fall season (900 units)
 * 
 * Formula Pattern: Multiply base rate by tier multiplier, apply seasonal adjustment, then organic premium
 * Structure: units × round(((base × multiplier × seasonal) × (1 + organic_premium)), 2)
 * 
 * Demonstrates: Chained multipliers + premium with intermediate rounding
 * Contract specifies ~$5,207 total (exact amount varies with rounding rules)
 */
export const exampleComplexPremium: FormulaDefinition = {
  version: "1.0",
  name: "Complex with Organic Premium - Emerald Crown Hosta",
  filters: {
    products: ["Emerald Crown Hosta"],
    containerSizes: ["2-gallon"]
  },
  formula: {
    type: 'multiply',
    operands: [
      { type: 'reference', field: 'units' } as ReferenceNode,
      {
        type: 'round',
        precision: 2,
        value: {
          type: 'premium',
          mode: 'additive',  // additive mode: base × (1 + percentage)
          percentage: 0.25, // +25% for organic → multiply by 1.25
          base: {
            type: 'round',
            precision: 2,
            value: {
              type: 'multiply',
              operands: [
                { type: 'literal', value: 3.25, unit: 'dollars', description: 'Tier 2 base' } as LiteralNode,
                { type: 'literal', value: 1.5, unit: 'multiplier', description: 'Tier 2 multiplier' } as LiteralNode,
                {
                  type: 'lookup',
                  reference: { type: 'reference', field: 'season' } as ReferenceNode,
                  table: {
                    "Fall": 0.95,
                    "Spring": 1.10
                  },
                  default: 1.0,
                  description: 'Seasonal adjustment'
                } as LookupNode
              ]
            } as MultiplyNode
          } as RoundNode
        } as PremiumNode
      } as RoundNode
    ]
  } as MultiplyNode,
  displayTemplate: "units × round(((base × multiplier × seasonal) × (1 + organic_premium)), 2)"
};

/**
 * Example 5: Sliding Scale with Minimum Guarantee
 * Cascade Blue Hydrangea: max(20,000 × $1.45, $25,000) = $29,000
 */
export const exampleMinimumGuarantee: FormulaDefinition = {
  version: "1.0",
  name: "Sliding Scale with Minimum - Cascade Blue Hydrangea",
  filters: {
    products: ["Cascade Blue Hydrangea"]
  },
  formula: {
    type: 'max',
    operands: [
      {
        type: 'multiply',
        operands: [
          { type: 'reference', field: 'units' } as ReferenceNode,
          {
            type: 'tier',
            reference: { type: 'reference', field: 'units' } as ReferenceNode,
            tiers: [
              { min: 0, max: 5000, rate: 1.85, label: "0-5,000 units" },
              { min: 5001, max: 15000, rate: 1.65, label: "5,001-15,000 units" },
              { min: 15001, max: null, rate: 1.45, label: "15,001+ units" }
            ]
          } as TierNode
        ]
      } as MultiplyNode,
      { type: 'literal', value: 25000, unit: 'dollars', description: 'Annual minimum guarantee' } as LiteralNode
    ]
  } as MaxNode,
  displayTemplate: "max(units × tier_rate, minimum_guarantee)"
};
