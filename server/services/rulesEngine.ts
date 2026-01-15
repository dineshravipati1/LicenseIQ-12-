/**
 * LicenseIQ Rules Engine
 * Handles dynamic royalty calculation rules and formula evaluation
 */

interface RoyaltyCalculationInput {
  grossRevenue?: number;
  netRevenue?: number;
  units?: number;
  territory?: string;
  productCategory?: string;
  timeframe?: 'monthly' | 'quarterly' | 'annually';
  contractDate?: Date;
  customFields?: Record<string, number | string>;
}

interface RoyaltyCalculationResult {
  totalRoyalty: number;
  breakdown: RoyaltyRuleResult[];
  currency: string;
  calculatedAt: Date;
  metadata: {
    rulesApplied: number;
    highestPriorityRule?: string;
    warnings: string[];
  };
}

interface RoyaltyRuleResult {
  ruleName: string;
  ruleType: string;
  amount: number;
  rate?: number;
  baseAmount?: number;
  applied: boolean;
  reason?: string;
}

interface RoyaltyRule {
  id: string;
  ruleName: string;
  ruleType: 'percentage' | 'tiered' | 'minimum_guarantee' | 'cap' | 'deduction' | 'fixed_fee';
  description: string;
  conditions: {
    productCategories?: string[];
    territories?: string[];
    salesVolumeMin?: number;
    salesVolumeMax?: number;
    timeperiod?: 'monthly' | 'quarterly' | 'annually' | 'one-time';
    currency?: string;
    customConditions?: Record<string, any>;
  };
  calculation: {
    rate?: number; // for percentage rules
    tiers?: Array<{ min: number; max?: number; rate: number }>; // for tiered rules
    amount?: number; // for fixed amounts
    formula?: string; // for complex calculations
    baseField?: 'grossRevenue' | 'netRevenue' | 'units' | 'custom';
  };
  priority: number;
  isActive: boolean;
  confidence: number;
}

export class RulesEngine {
  /**
   * Apply all active rules to calculate total royalty
   */
  public static async calculateRoyalties(
    rules: RoyaltyRule[],
    input: RoyaltyCalculationInput
  ): Promise<RoyaltyCalculationResult> {
    const result: RoyaltyCalculationResult = {
      totalRoyalty: 0,
      breakdown: [],
      currency: 'USD',
      calculatedAt: new Date(),
      metadata: {
        rulesApplied: 0,
        warnings: []
      }
    };

    // Validate input
    const validation = this.validateInput(input);
    if (validation.errors.length > 0) {
      result.metadata.warnings.push(...validation.errors.map(e => `Input validation: ${e}`));
    }

    // Filter and sort rules by priority
    const activeRules = rules
      .filter(rule => rule.isActive)
      .sort((a, b) => a.priority - b.priority);

    // Apply each rule
    for (const rule of activeRules) {
      const ruleResult = await this.applyRule(rule, input);
      result.breakdown.push(ruleResult);
      
      if (ruleResult.applied) {
        result.totalRoyalty += ruleResult.amount;
        result.metadata.rulesApplied++;
        
        if (!result.metadata.highestPriorityRule) {
          result.metadata.highestPriorityRule = rule.ruleName;
        }
      }
    }

    // Apply post-processing rules (caps, minimums)
    result.totalRoyalty = this.applyPostProcessingRules(result.totalRoyalty, activeRules, input);

    return result;
  }

  /**
   * Apply a single rule to the input data
   */
  private static async applyRule(
    rule: RoyaltyRule,
    input: RoyaltyCalculationInput
  ): Promise<RoyaltyRuleResult> {
    const ruleResult: RoyaltyRuleResult = {
      ruleName: rule.ruleName,
      ruleType: rule.ruleType,
      amount: 0,
      applied: false
    };

    // Check if rule conditions are met
    if (!this.evaluateConditions(rule.conditions, input)) {
      ruleResult.reason = 'Conditions not met';
      return ruleResult;
    }

    // Calculate amount based on rule type
    try {
      switch (rule.ruleType) {
        case 'percentage':
          ruleResult.amount = this.calculatePercentageRule(rule, input);
          break;
        case 'tiered':
          ruleResult.amount = this.calculateTieredRule(rule, input);
          break;
        case 'minimum_guarantee':
          ruleResult.amount = this.calculateMinimumGuarantee(rule, input);
          break;
        case 'cap':
          ruleResult.amount = this.calculateCap(rule, input);
          break;
        case 'deduction':
          ruleResult.amount = -this.calculateDeduction(rule, input); // Negative for deductions
          break;
        case 'fixed_fee':
          ruleResult.amount = rule.calculation.amount || 0;
          break;
        default:
          if (rule.calculation.formula) {
            ruleResult.amount = this.evaluateFormula(rule.calculation.formula, input);
          }
      }

      ruleResult.applied = ruleResult.amount !== 0;
    } catch (error) {
      ruleResult.reason = `Calculation error: ${error.message}`;
    }

    return ruleResult;
  }

  /**
   * Calculate percentage-based royalty
   */
  private static calculatePercentageRule(rule: RoyaltyRule, input: RoyaltyCalculationInput): number {
    const rate = rule.calculation.rate || 0;
    const baseField = rule.calculation.baseField || 'netRevenue';
    
    let baseAmount = 0;
    switch (baseField) {
      case 'grossRevenue':
        baseAmount = input.grossRevenue || 0;
        break;
      case 'netRevenue':
        baseAmount = input.netRevenue || 0;
        break;
      case 'units':
        baseAmount = input.units || 0;
        break;
      default:
        baseAmount = input.netRevenue || input.grossRevenue || 0;
    }

    return baseAmount * (rate / 100);
  }

  /**
   * Calculate tiered royalty based on volume thresholds
   */
  private static calculateTieredRule(rule: RoyaltyRule, input: RoyaltyCalculationInput): number {
    const tiers = rule.calculation.tiers || [];
    const baseField = rule.calculation.baseField || 'netRevenue';
    
    let baseAmount = 0;
    switch (baseField) {
      case 'grossRevenue':
        baseAmount = input.grossRevenue || 0;
        break;
      case 'netRevenue':
        baseAmount = input.netRevenue || 0;
        break;
      case 'units':
        baseAmount = input.units || 0;
        break;
      default:
        baseAmount = input.netRevenue || input.grossRevenue || 0;
    }

    // Find applicable tier
    for (const tier of tiers.sort((a, b) => a.min - b.min)) {
      if (baseAmount >= tier.min && (!tier.max || baseAmount <= tier.max)) {
        return baseAmount * (tier.rate / 100);
      }
    }

    return 0;
  }

  /**
   * Calculate minimum guarantee
   */
  private static calculateMinimumGuarantee(rule: RoyaltyRule, input: RoyaltyCalculationInput): number {
    return rule.calculation.amount || 0;
  }

  /**
   * Calculate cap (maximum limit)
   */
  private static calculateCap(rule: RoyaltyRule, input: RoyaltyCalculationInput): number {
    return rule.calculation.amount || 0;
  }

  /**
   * Calculate deduction amount
   */
  private static calculateDeduction(rule: RoyaltyRule, input: RoyaltyCalculationInput): number {
    const rate = rule.calculation.rate || 0;
    const baseField = rule.calculation.baseField || 'netRevenue';
    
    let baseAmount = 0;
    switch (baseField) {
      case 'grossRevenue':
        baseAmount = input.grossRevenue || 0;
        break;
      case 'netRevenue':
        baseAmount = input.netRevenue || 0;
        break;
      case 'units':
        baseAmount = input.units || 0;
        break;
      default:
        baseAmount = input.netRevenue || input.grossRevenue || 0;
    }

    if (rule.calculation.amount) {
      return rule.calculation.amount;
    }
    
    return baseAmount * (rate / 100);
  }

  /**
   * Evaluate custom formula
   */
  private static evaluateFormula(formula: string, input: RoyaltyCalculationInput): number {
    try {
      // Safe formula evaluation - replace variables with input values
      let processedFormula = formula
        .replace(/grossRevenue/g, String(input.grossRevenue || 0))
        .replace(/netRevenue/g, String(input.netRevenue || 0))
        .replace(/units/g, String(input.units || 0));

      // Add custom fields
      if (input.customFields) {
        for (const [key, value] of Object.entries(input.customFields)) {
          processedFormula = processedFormula.replace(
            new RegExp(key, 'g'), 
            String(typeof value === 'number' ? value : 0)
          );
        }
      }

      // Basic math operations only - security check
      if (!/^[0-9+\-*/(). ]+$/.test(processedFormula)) {
        throw new Error('Formula contains invalid characters');
      }

      // Evaluate using Function constructor (safer than eval)
      const result = new Function('return ' + processedFormula)();
      return typeof result === 'number' ? result : 0;
    } catch (error) {
      console.error('Formula evaluation error:', error, 'Formula:', formula);
      return 0;
    }
  }

  /**
   * Check if rule conditions are satisfied
   */
  private static evaluateConditions(
    conditions: RoyaltyRule['conditions'], 
    input: RoyaltyCalculationInput
  ): boolean {
    // 🔒 CONTROLLED PRODUCT MATCHING with wildcard support
    // Check product matching but continue to validate other conditions (territory, volume, etc.)
    
    if (input.productCategory) {
      // Normalize wildcards (case-insensitive, trim whitespace)
      const normalizeCategories = (cats: string[] | undefined) => 
        cats?.map(c => c.trim().toLowerCase()) || [];
      
      const normalizedRuleCategories = normalizeCategories(conditions.productCategories);
      const wildcardMarkers = ['*', 'general', 'all', ''];
      
      // Check if rule is a general-purpose rule (applies to ALL products)
      const isGeneralRule = !conditions.productCategories || 
                           conditions.productCategories.length === 0 ||
                           normalizedRuleCategories.some(cat => wildcardMarkers.includes(cat));
      
      if (!isGeneralRule) {
        // Specific product rule - check if input product matches
        if (!conditions.productCategories.includes(input.productCategory)) {
          console.log(`⚠️ [RULES] Product mismatch: '${input.productCategory}' not in [${conditions.productCategories.join(', ')}]`);
          return false; // Product doesn't match - fail immediately
        }
        console.log(`✅ [RULES] Product match: '${input.productCategory}' in [${conditions.productCategories.join(', ')}]`);
      } else {
        console.log(`✅ [RULES] General rule applies to all products (including '${input.productCategory}')`);
      }
      // Continue to check other conditions (territory, volume, etc.)
    } else {
      // If no product specified in input, only general rules apply
      const normalizedRuleCategories = conditions.productCategories?.map(c => c.trim().toLowerCase()) || [];
      const wildcardMarkers = ['*', 'general', 'all', ''];
      
      const hasSpecificProducts = conditions.productCategories && 
                                  conditions.productCategories.length > 0 &&
                                  !normalizedRuleCategories.some(cat => wildcardMarkers.includes(cat));
      
      if (hasSpecificProducts) {
        console.log(`⚠️ [RULES] No product specified in input, but rule requires: [${conditions.productCategories.join(', ')}]`);
        return false; // Product-specific rule but no product in input - fail
      }
      // General rule - continue to check other conditions
    }

    // Check territories
    if (conditions.territories && conditions.territories.length > 0) {
      if (!input.territory || !conditions.territories.includes(input.territory)) {
        return false;
      }
    }

    // Check sales volume thresholds
    const checkVolume = input.netRevenue || input.grossRevenue || input.units || 0;
    if (conditions.salesVolumeMin && checkVolume < conditions.salesVolumeMin) {
      return false;
    }
    if (conditions.salesVolumeMax && checkVolume > conditions.salesVolumeMax) {
      return false;
    }

    // Check timeframe
    if (conditions.timeperiod && input.timeframe && conditions.timeperiod !== input.timeframe) {
      return false;
    }

    // Check custom conditions
    if (conditions.customConditions && input.customFields) {
      for (const [key, expectedValue] of Object.entries(conditions.customConditions)) {
        if (input.customFields[key] !== expectedValue) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Apply post-processing rules like caps and minimums
   */
  private static applyPostProcessingRules(
    totalRoyalty: number,
    rules: RoyaltyRule[],
    input: RoyaltyCalculationInput
  ): number {
    let adjustedTotal = totalRoyalty;

    // Apply caps (maximum limits)
    const capRules = rules.filter(r => r.ruleType === 'cap' && r.isActive);
    for (const capRule of capRules) {
      if (this.evaluateConditions(capRule.conditions, input)) {
        const capAmount = capRule.calculation.amount || 0;
        if (adjustedTotal > capAmount) {
          adjustedTotal = capAmount;
        }
      }
    }

    // Apply minimum guarantees
    const minRules = rules.filter(r => r.ruleType === 'minimum_guarantee' && r.isActive);
    for (const minRule of minRules) {
      if (this.evaluateConditions(minRule.conditions, input)) {
        const minAmount = minRule.calculation.amount || 0;
        if (adjustedTotal < minAmount) {
          adjustedTotal = minAmount;
        }
      }
    }

    return Math.max(0, adjustedTotal); // Never negative
  }

  /**
   * Validate input data
   */
  private static validateInput(input: RoyaltyCalculationInput): { 
    isValid: boolean; 
    errors: string[]; 
    warnings: string[];
  } {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check if at least one revenue source is provided
    if (!input.grossRevenue && !input.netRevenue && !input.units) {
      errors.push('At least one of grossRevenue, netRevenue, or units must be provided');
    }

    // Check for negative values
    if ((input.grossRevenue ?? 0) < 0) {
      errors.push('Gross revenue cannot be negative');
    }
    if ((input.netRevenue ?? 0) < 0) {
      errors.push('Net revenue cannot be negative');
    }
    if ((input.units ?? 0) < 0) {
      errors.push('Units cannot be negative');
    }

    // Warnings for potential data issues
    if (input.grossRevenue && input.netRevenue && input.netRevenue > input.grossRevenue) {
      warnings.push('Net revenue is higher than gross revenue - please verify');
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }

  /**
   * Create a new royalty calculation rule
   */
  public static createRule(ruleData: Partial<RoyaltyRule>): RoyaltyRule {
    return {
      id: ruleData.id || crypto.randomUUID(),
      ruleName: ruleData.ruleName || 'Untitled Rule',
      ruleType: ruleData.ruleType || 'percentage',
      description: ruleData.description || '',
      conditions: ruleData.conditions || {},
      calculation: ruleData.calculation || {},
      priority: ruleData.priority || 10,
      isActive: ruleData.isActive ?? true,
      confidence: ruleData.confidence || 1.0
    };
  }

  /**
   * Validate a rule configuration
   */
  public static validateRule(rule: RoyaltyRule): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!rule.ruleName?.trim()) {
      errors.push('Rule name is required');
    }

    if (!rule.ruleType) {
      errors.push('Rule type is required');
    }

    // Validate calculation based on rule type
    switch (rule.ruleType) {
      case 'percentage':
        if (!rule.calculation.rate || rule.calculation.rate <= 0) {
          errors.push('Percentage rules require a valid rate > 0');
        }
        break;
      case 'tiered':
        if (!rule.calculation.tiers || rule.calculation.tiers.length === 0) {
          errors.push('Tiered rules require at least one tier');
        }
        break;
      case 'fixed_fee':
      case 'minimum_guarantee':
      case 'cap':
        if (!rule.calculation.amount || rule.calculation.amount <= 0) {
          errors.push('Fixed amount rules require a valid amount > 0');
        }
        break;
    }

    if (rule.priority < 1 || rule.priority > 100) {
      errors.push('Priority must be between 1 and 100');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }
}

export type {
  RoyaltyCalculationInput,
  RoyaltyCalculationResult,
  RoyaltyRuleResult,
  RoyaltyRule
};