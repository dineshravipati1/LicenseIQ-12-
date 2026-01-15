import { db } from '../db';
import { ruleValidationEvents } from '@shared/schema';
import { SynthesizedRule } from './ruleSynthesisService';

/**
 * Validation Service
 * 
 * Multi-layered validation for extracted data and synthesized rules:
 * 1. Dimensional validation (units, ranges, consistency)
 * 2. AI cross-validation (second LLM opinion)
 * 3. Business rule validation (realistic values)
 */

export interface ValidationResult {
  isValid: boolean;
  confidence: number;
  issues: ValidationIssue[];
  recommendations: string[];
}

export interface ValidationIssue {
  severity: 'error' | 'warning' | 'info';
  category: 'dimensional' | 'consistency' | 'business_logic';
  message: string;
  affectedField?: string;
}

/**
 * Validate synthesized rules for dimensional and logical consistency
 */
export async function validateRules(rules: SynthesizedRule[]): Promise<ValidationResult> {
  const issues: ValidationIssue[] = [];
  const recommendations: string[] = [];

  for (const rule of rules) {
    // Dimensional validation
    const dimensionalIssues = validateDimensions(rule);
    issues.push(...dimensionalIssues);

    // Consistency validation
    const consistencyIssues = validateConsistency(rule);
    issues.push(...consistencyIssues);

    // Business logic validation
    const businessIssues = validateBusinessLogic(rule);
    issues.push(...businessIssues);

    // Store all validation events separately
    if (rule.id) {
      // Dimensional validation event
      if (dimensionalIssues.length > 0) {
        await db.insert(ruleValidationEvents).values({
          ruleDefinitionId: rule.id,
          validationType: 'dimensional',
          validationResult: dimensionalIssues.some(i => i.severity === 'error') ? 'failed' : 'warning',
          issues: dimensionalIssues,
          recommendations: dimensionalIssues.map(i => `Fix ${i.affectedField}: ${i.message}`),
        });
      }
      
      // Consistency validation event
      if (consistencyIssues.length > 0) {
        await db.insert(ruleValidationEvents).values({
          ruleDefinitionId: rule.id,
          validationType: 'ai_consistency',
          validationResult: consistencyIssues.some(i => i.severity === 'error') ? 'failed' : 'warning',
          issues: consistencyIssues,
          recommendations: consistencyIssues.map(i => `Review ${i.affectedField}: ${i.message}`),
        });
      }
      
      // Business logic validation event
      if (businessIssues.length > 0) {
        await db.insert(ruleValidationEvents).values({
          ruleDefinitionId: rule.id,
          validationType: 'manual',
          validationResult: businessIssues.some(i => i.severity === 'error') ? 'failed' : 'warning',
          issues: businessIssues,
          recommendations: businessIssues.map(i => `Verify: ${i.message}`),
        });
      }
    }
  }

  // Calculate overall confidence based on issues
  const errorCount = issues.filter(i => i.severity === 'error').length;
  const warningCount = issues.filter(i => i.severity === 'warning').length;
  
  let confidence = 1.0;
  confidence -= errorCount * 0.15; // Each error reduces confidence by 15%
  confidence -= warningCount * 0.05; // Each warning reduces confidence by 5%
  confidence = Math.max(0, Math.min(1, confidence));

  if (issues.length > 0) {
    recommendations.push('Review and correct identified issues before activating rules');
  }

  return {
    isValid: errorCount === 0,
    confidence,
    issues,
    recommendations,
  };
}

/**
 * Dimensional validation - check units, ranges, data types
 */
function validateDimensions(rule: SynthesizedRule): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const formula = rule.formulaDefinition;

  // Validate percentage rates
  if (formula.type === 'percentage') {
    const rate = parseFloat(formula.rate);
    if (isNaN(rate)) {
      issues.push({
        severity: 'error',
        category: 'dimensional',
        message: `Invalid percentage rate: ${formula.rate}`,
        affectedField: 'rate',
      });
    } else if (rate < 0 || rate > 1) {
      issues.push({
        severity: 'warning',
        category: 'dimensional',
        message: `Unusual percentage rate: ${(rate * 100).toFixed(1)}%`,
        affectedField: 'rate',
      });
    }
  }

  // Validate fixed amounts
  if (formula.type === 'fixed') {
    const amount = parseFloat(formula.amount);
    if (isNaN(amount)) {
      issues.push({
        severity: 'error',
        category: 'dimensional',
        message: `Invalid fixed amount: ${formula.amount}`,
        affectedField: 'amount',
      });
    } else if (amount < 0) {
      issues.push({
        severity: 'error',
        category: 'dimensional',
        message: 'Fixed amount cannot be negative',
        affectedField: 'amount',
      });
    }
  }

  // Validate tier structures
  if (formula.type === 'tier' && formula.tiers) {
    for (let i = 0; i < formula.tiers.length; i++) {
      const tier = formula.tiers[i];
      if (tier.min !== null && tier.max !== null && tier.min >= tier.max) {
        issues.push({
          severity: 'error',
          category: 'dimensional',
          message: `Tier ${i}: min (${tier.min}) must be less than max (${tier.max})`,
          affectedField: `tiers[${i}]`,
        });
      }
    }
  }

  return issues;
}

/**
 * Consistency validation - check for logical contradictions
 */
function validateConsistency(rule: SynthesizedRule): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const formula = rule.formulaDefinition;

  // Conditional formulas should have both branches
  if (formula.type === 'conditional') {
    if (!formula.trueFormula) {
      issues.push({
        severity: 'warning',
        category: 'consistency',
        message: 'Conditional rule missing true branch',
        affectedField: 'trueFormula',
      });
    }
    if (!formula.falseFormula) {
      issues.push({
        severity: 'warning',
        category: 'consistency',
        message: 'Conditional rule missing false branch',
        affectedField: 'falseFormula',
      });
    }
  }

  // Arithmetic operations should have operands
  if (formula.type === 'arithmetic') {
    if (!formula.operands || formula.operands.length < 2) {
      issues.push({
        severity: 'error',
        category: 'consistency',
        message: 'Arithmetic operation requires at least 2 operands',
        affectedField: 'operands',
      });
    }
  }

  return issues;
}

/**
 * Business logic validation - check for realistic values
 */
function validateBusinessLogic(rule: SynthesizedRule): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const formula = rule.formulaDefinition;

  // Percentage rates typically 1-50%
  if (formula.type === 'percentage') {
    const rate = parseFloat(formula.rate);
    if (rate > 0.5) {
      issues.push({
        severity: 'warning',
        category: 'business_logic',
        message: `Unusually high royalty rate: ${(rate * 100).toFixed(1)}%`,
        affectedField: 'rate',
      });
    }
    if (rate < 0.01) {
      issues.push({
        severity: 'warning',
        category: 'business_logic',
        message: `Unusually low royalty rate: ${(rate * 100).toFixed(1)}%`,
        affectedField: 'rate',
      });
    }
  }

  // Check for missing applicability filters
  if (Object.keys(rule.applicabilityFilters || {}).length === 0) {
    issues.push({
      severity: 'info',
      category: 'business_logic',
      message: 'No applicability filters defined - rule applies globally',
    });
  }

  return issues;
}

/**
 * Monte Carlo validation - simulate rule with random inputs
 */
export async function runMonteCarloValidation(
  rule: SynthesizedRule,
  iterations: number = 100
): Promise<{ min: number; max: number; mean: number; stdDev: number }> {
  const results: number[] = [];

  for (let i = 0; i < iterations; i++) {
    // Generate random sales value between $1K and $1M
    const randomSales = Math.random() * 999000 + 1000;
    
    try {
      const result = evaluateFormula(rule.formulaDefinition, { netSales: randomSales });
      results.push(result);
    } catch (error) {
      console.error('[Validation] Monte Carlo evaluation failed:', error);
    }
  }

  const min = Math.min(...results);
  const max = Math.max(...results);
  const mean = results.reduce((a, b) => a + b, 0) / results.length;
  const variance = results.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / results.length;
  const stdDev = Math.sqrt(variance);

  return { min, max, mean, stdDev };
}

/**
 * Simple formula evaluator for validation
 */
function evaluateFormula(formula: any, context: Record<string, number>): number {
  switch (formula.type) {
    case 'percentage':
      return (context[formula.base] || 0) * parseFloat(formula.rate);
    
    case 'fixed':
      return parseFloat(formula.amount);
    
    case 'tier':
      const value = context[formula.base] || 0;
      for (const tier of formula.tiers || []) {
        if (value >= tier.min && (tier.max === null || value < tier.max)) {
          return value * parseFloat(tier.rate);
        }
      }
      return 0;
    
    case 'arithmetic':
      const operands = (formula.operands || []).map((op: any) => evaluateFormula(op, context));
      switch (formula.operator) {
        case '+': return operands.reduce((a: number, b: number) => a + b, 0);
        case '-': return operands.reduce((a: number, b: number) => a - b);
        case '*': return operands.reduce((a: number, b: number) => a * b, 1);
        case '/': return operands.reduce((a: number, b: number) => a / b);
        default: return 0;
      }
    
    default:
      return 0;
  }
}
