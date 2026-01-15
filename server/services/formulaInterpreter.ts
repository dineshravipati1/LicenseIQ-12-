/**
 * Formula Interpreter - Evaluates FormulaNode expression trees
 * 
 * This interpreter walks JSON-based formula trees and calculates royalty amounts
 * based on sales data context. It supports all formula node types defined in
 * shared/formula-types.ts
 */

import {
  AnyFormulaNode,
  LiteralNode,
  ReferenceNode,
  TierNode,
  MultiplyNode,
  AddNode,
  SubtractNode,
  MaxNode,
  MinNode,
  IfNode,
  LookupNode,
  PremiumNode,
  RoundNode,
  FormulaDefinition
} from '../../shared/formula-types';

/**
 * Sales data context for formula evaluation
 * Contains all fields that formulas can reference
 */
export interface SalesContext {
  units: number;
  season?: string;
  territory?: string;
  containerSize?: string;
  productName?: string;
  vendorName?: string;
  customerName?: string;
  saleDate?: string;
  saleAmount?: number;
  [key: string]: any; // Allow custom fields
}

/**
 * Evaluation options
 */
export interface EvaluationOptions {
  debug?: boolean; // Enable debug logging
  precision?: number; // Default rounding precision (decimal places)
}

/**
 * Evaluation result with debug info
 */
export interface EvaluationResult {
  value: number;
  debugLog?: string[];
}

/**
 * Main Formula Interpreter
 */
export class FormulaInterpreter {
  private debugLog: string[] = [];
  private options: EvaluationOptions;

  constructor(options: EvaluationOptions = {}) {
    this.options = {
      debug: false,
      precision: 2,
      ...options
    };
  }

  /**
   * Evaluate a complete formula definition
   */
  evaluateFormula(
    formula: FormulaDefinition,
    context: SalesContext
  ): EvaluationResult {
    this.debugLog = [];
    this.log(`Evaluating formula: ${formula.name}`);
    
    // Check if formula applies to this sales context (filters)
    if (formula.filters) {
      if (!this.matchesFilters(formula.filters, context)) {
        this.log(`Formula does not match sales context - skipping`);
        return { value: 0, debugLog: this.debugLog };
      }
    }

    const rawValue = this.evaluateNode(formula.formula, context);
    const value = this.toNumber(rawValue); // Final result must be numeric
    this.log(`Final result: $${value.toFixed(2)}`);
    
    return {
      value,
      debugLog: this.options.debug ? this.debugLog : undefined
    };
  }

  /**
   * Evaluate a single formula node
   */
  private evaluateNode(node: AnyFormulaNode, context: SalesContext): number | string {
    switch (node.type) {
      case 'literal':
        return this.evaluateLiteral(node as LiteralNode);
      
      case 'reference':
        return this.evaluateReference(node as ReferenceNode, context);
      
      case 'tier':
        return this.evaluateTier(node as TierNode, context);
      
      case 'multiply':
        return this.evaluateMultiply(node as MultiplyNode, context);
      
      case 'add':
        return this.evaluateAdd(node as AddNode, context);
      
      case 'subtract':
        return this.evaluateSubtract(node as SubtractNode, context);
      
      case 'max':
        return this.evaluateMax(node as MaxNode, context);
      
      case 'min':
        return this.evaluateMin(node as MinNode, context);
      
      case 'if':
        return this.evaluateIf(node as IfNode, context);
      
      case 'lookup':
        return this.evaluateLookup(node as LookupNode, context);
      
      case 'premium':
        return this.evaluatePremium(node as PremiumNode, context);
      
      case 'round':
        return this.evaluateRound(node as RoundNode, context);
      
      default:
        throw new Error(`Unknown node type: ${(node as any).type}`);
    }
  }

  /**
   * Helper to coerce a value to a number
   */
  private toNumber(value: number | string): number {
    if (typeof value === 'number') return value;
    const num = parseFloat(value);
    return isNaN(num) ? 0 : num;
  }

  private evaluateLiteral(node: LiteralNode): number | string {
    const value = node.value;
    this.log(`Literal: ${value}${node.unit ? ` ${node.unit}` : ''}`);
    return value;
  }

  private evaluateReference(node: ReferenceNode, context: SalesContext): number | string {
    const value = context[node.field];
    if (value === undefined || value === null) {
      this.log(`Reference '${node.field}': undefined (defaulting to 0)`);
      return 0;
    }
    this.log(`Reference '${node.field}': ${value}`);
    return value;
  }

  private evaluateTier(node: TierNode, context: SalesContext): number {
    const referenceValue = this.toNumber(this.evaluateNode(node.reference, context));
    
    for (const tier of node.tiers) {
      const inRange = 
        referenceValue >= tier.min && 
        (tier.max === null || referenceValue <= tier.max);
      
      if (inRange) {
        // ⚠️ CRITICAL ASSUMPTION: ALL tier rates MUST be stored as percentages (11.25 = 11.25%, NOT 0.1125)
        // This is enforced by: (1) Groq service prompt, (2) database normalization, (3) safety guard
        // Interpreter always converts percentage → decimal for calculation
        const rateAsDecimal = tier.rate / 100; // Convert percentage to decimal (e.g., 11.25% → 0.1125)
        this.log(`Tier match: ${referenceValue} in [${tier.min}, ${tier.max ?? '∞'}] → rate ${tier.rate}% (${rateAsDecimal})${tier.label ? ` (${tier.label})` : ''}`);
        return rateAsDecimal;
      }
    }
    
    this.log(`Tier: No tier matched for value ${referenceValue}`);
    return 0;
  }

  private evaluateMultiply(node: MultiplyNode, context: SalesContext): number {
    const values = node.operands.map(op => this.toNumber(this.evaluateNode(op, context)));
    const result = values.reduce((acc, val) => acc * val, 1);
    this.log(`Multiply: ${values.join(' × ')} = ${result}`);
    return result;
  }

  private evaluateAdd(node: AddNode, context: SalesContext): number {
    const values = node.operands.map(op => this.toNumber(this.evaluateNode(op, context)));
    const result = values.reduce((acc, val) => acc + val, 0);
    this.log(`Add: ${values.join(' + ')} = ${result}`);
    return result;
  }

  private evaluateSubtract(node: SubtractNode, context: SalesContext): number {
    const [left, right] = node.operands.map(op => this.toNumber(this.evaluateNode(op, context)));
    const result = left - right;
    this.log(`Subtract: ${left} - ${right} = ${result}`);
    return result;
  }

  private evaluateMax(node: MaxNode, context: SalesContext): number {
    const values = node.operands.map(op => this.toNumber(this.evaluateNode(op, context)));
    const result = Math.max(...values);
    this.log(`Max: max(${values.join(', ')}) = ${result}`);
    return result;
  }

  private evaluateMin(node: MinNode, context: SalesContext): number {
    const values = node.operands.map(op => this.toNumber(this.evaluateNode(op, context)));
    const result = Math.min(...values);
    this.log(`Min: min(${values.join(', ')}) = ${result}`);
    return result;
  }

  private evaluateIf(node: IfNode, context: SalesContext): number | string {
    const conditionMet = this.evaluateCondition(node.condition, context);
    this.log(`If: condition ${conditionMet ? 'TRUE' : 'FALSE'}`);
    
    if (conditionMet) {
      return this.evaluateNode(node.then, context);
    } else if (node.else) {
      return this.evaluateNode(node.else, context);
    }
    return 0;
  }

  private evaluateLookup(node: LookupNode, context: SalesContext): number {
    const key = this.evaluateNode(node.reference, context);
    const keyStr = String(key);
    const value = node.table[keyStr];
    
    if (value !== undefined) {
      this.log(`Lookup: '${keyStr}' → ${value}${node.description ? ` (${node.description})` : ''}`);
      return value;
    }
    
    const defaultValue = node.default ?? 0;
    this.log(`Lookup: '${keyStr}' not found, using default ${defaultValue}`);
    return defaultValue;
  }

  private evaluatePremium(node: PremiumNode, context: SalesContext): number {
    const baseValue = this.toNumber(this.evaluateNode(node.base, context));
    
    if (node.mode === 'additive') {
      // base × (1 + percentage)
      const result = baseValue * (1 + node.percentage);
      this.log(`Premium (additive): ${baseValue} × (1 + ${node.percentage}) = ${result}`);
      return result;
    } else {
      // base × percentage
      const result = baseValue * node.percentage;
      this.log(`Premium (multiplicative): ${baseValue} × ${node.percentage} = ${result}`);
      return result;
    }
  }

  private evaluateRound(node: RoundNode, context: SalesContext): number {
    const value = this.toNumber(this.evaluateNode(node.value, context));
    const mode = node.mode ?? 'round';
    let result: number;
    
    const multiplier = Math.pow(10, node.precision);
    
    switch (mode) {
      case 'floor':
        result = Math.floor(value * multiplier) / multiplier;
        break;
      case 'ceil':
        result = Math.ceil(value * multiplier) / multiplier;
        break;
      case 'round':
      default:
        result = Math.round(value * multiplier) / multiplier;
        break;
    }
    
    this.log(`Round (${mode}, ${node.precision} decimals): ${value} → ${result}`);
    return result;
  }

  /**
   * Check if sales context matches formula filters
   */
  private matchesFilters(
    filters: FormulaDefinition['filters'],
    context: SalesContext
  ): boolean {
    if (!filters) return true;

    // Check product filters
    if (filters.products && filters.products.length > 0) {
      if (!context.productName) return false;
      const productMatch = filters.products.some(p => 
        context.productName?.toLowerCase().includes(p.toLowerCase()) ||
        p.toLowerCase().includes(context.productName?.toLowerCase() ?? '')
      );
      if (!productMatch) {
        this.log(`Product filter mismatch: '${context.productName}' not in ${filters.products.join(', ')}`);
        return false;
      }
    }

    // Check territory filters
    if (filters.territories && filters.territories.length > 0) {
      if (!context.territory || !filters.territories.includes(context.territory)) {
        this.log(`Territory filter mismatch: '${context.territory}' not in ${filters.territories.join(', ')}`);
        return false;
      }
    }

    // Check container size filters
    if (filters.containerSizes && filters.containerSizes.length > 0) {
      if (!context.containerSize || !filters.containerSizes.includes(context.containerSize)) {
        this.log(`Container size filter mismatch: '${context.containerSize}' not in ${filters.containerSizes.join(', ')}`);
        return false;
      }
    }

    // Check date range filters
    if (filters.dateRange) {
      if (!context.saleDate) return false;
      const saleDate = new Date(context.saleDate);
      
      if (filters.dateRange.start) {
        const startDate = new Date(filters.dateRange.start);
        if (saleDate < startDate) {
          this.log(`Date filter mismatch: ${context.saleDate} before ${filters.dateRange.start}`);
          return false;
        }
      }
      
      if (filters.dateRange.end) {
        const endDate = new Date(filters.dateRange.end);
        if (saleDate > endDate) {
          this.log(`Date filter mismatch: ${context.saleDate} after ${filters.dateRange.end}`);
          return false;
        }
      }
    }

    this.log(`Filters matched`);
    return true;
  }

  /**
   * Evaluate a conditional expression
   */
  private evaluateCondition(
    condition: IfNode['condition'],
    context: SalesContext
  ): boolean {
    const fieldValue = context[condition.field];
    
    switch (condition.operator) {
      case 'equals':
        return fieldValue === condition.value;
      
      case 'contains':
        return String(fieldValue).toLowerCase().includes(String(condition.value).toLowerCase());
      
      case 'greaterThan':
        return Number(fieldValue) > Number(condition.value);
      
      case 'lessThan':
        return Number(fieldValue) < Number(condition.value);
      
      case 'in':
        return Array.isArray(condition.value) && condition.value.includes(fieldValue);
      
      default:
        return false;
    }
  }

  /**
   * Debug logging
   */
  private log(message: string): void {
    this.debugLog.push(message);
    if (this.options.debug) {
      console.log(`[FormulaInterpreter] ${message}`);
    }
  }
}

/**
 * Convenience function to evaluate a formula
 */
export function evaluateFormula(
  formula: FormulaDefinition,
  context: SalesContext,
  options?: EvaluationOptions
): EvaluationResult {
  const interpreter = new FormulaInterpreter(options);
  return interpreter.evaluateFormula(formula, context);
}
