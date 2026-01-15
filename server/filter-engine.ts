/**
 * Server-side Filter Engine for Data Import
 * Applies user-defined filter conditions to imported data before processing
 */

export type FilterOperator =
  | 'equals'
  | 'not_equals'
  | 'contains'
  | 'not_contains'
  | 'starts_with'
  | 'ends_with'
  | 'greater_than'
  | 'less_than'
  | 'greater_than_or_equal'
  | 'less_than_or_equal'
  | 'between'
  | 'is_empty'
  | 'is_not_empty'
  | 'in'
  | 'not_in';

export interface ImportFilter {
  id: string;
  field: string;
  operator: FilterOperator;
  value: string | number | boolean | null;
  valueEnd?: string | number;
  dataType?: 'text' | 'number' | 'date' | 'boolean';
}

export interface FilterConfig {
  conditions: ImportFilter[];
  logic: 'AND' | 'OR';
  apiQueryParams?: Record<string, string>;
}

/**
 * Normalize a value for comparison (handles case insensitivity and trimming)
 */
function normalizeValue(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).trim().toLowerCase();
}

/**
 * Parse a date value for comparison
 */
function parseDate(value: any): Date | null {
  if (!value) return null;
  const date = new Date(value);
  return isNaN(date.getTime()) ? null : date;
}

/**
 * Parse a number value for comparison
 */
function parseNumber(value: any): number {
  if (value === null || value === undefined || value === '') {
    return NaN;
  }
  // Handle currency strings like "$1,234.56"
  const cleaned = String(value).replace(/[,$\s]/g, '');
  return parseFloat(cleaned);
}

/**
 * Evaluate a single filter condition against a record
 */
function evaluateCondition(
  record: Record<string, any>,
  condition: ImportFilter
): boolean {
  const fieldValue = record[condition.field];
  const compareValue = condition.value;
  const compareValueEnd = condition.valueEnd;
  const dataType = condition.dataType || 'text';

  switch (condition.operator) {
    case 'equals':
      if (dataType === 'number') {
        return parseNumber(fieldValue) === parseNumber(compareValue);
      }
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return false;
        return d1.toISOString().split('T')[0] === d2.toISOString().split('T')[0];
      }
      if (dataType === 'boolean') {
        const boolField = String(fieldValue).toLowerCase();
        const boolCompare = String(compareValue).toLowerCase();
        const trueValues = ['true', '1', 'yes', 'y', 'on'];
        const falseValues = ['false', '0', 'no', 'n', 'off'];
        const isFieldTrue = trueValues.includes(boolField);
        const isCompareTrue = trueValues.includes(boolCompare);
        return isFieldTrue === isCompareTrue;
      }
      return normalizeValue(fieldValue) === normalizeValue(compareValue);

    case 'not_equals':
      if (dataType === 'number') {
        return parseNumber(fieldValue) !== parseNumber(compareValue);
      }
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return true;
        return d1.toISOString().split('T')[0] !== d2.toISOString().split('T')[0];
      }
      if (dataType === 'boolean') {
        const boolField = String(fieldValue).toLowerCase();
        const boolCompare = String(compareValue).toLowerCase();
        const trueValues = ['true', '1', 'yes', 'y', 'on'];
        const isFieldTrue = trueValues.includes(boolField);
        const isCompareTrue = trueValues.includes(boolCompare);
        return isFieldTrue !== isCompareTrue;
      }
      return normalizeValue(fieldValue) !== normalizeValue(compareValue);

    case 'contains':
      return normalizeValue(fieldValue).includes(normalizeValue(compareValue));

    case 'not_contains':
      return !normalizeValue(fieldValue).includes(normalizeValue(compareValue));

    case 'starts_with':
      return normalizeValue(fieldValue).startsWith(normalizeValue(compareValue));

    case 'ends_with':
      return normalizeValue(fieldValue).endsWith(normalizeValue(compareValue));

    case 'greater_than':
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return false;
        return d1 > d2;
      }
      const numGt = parseNumber(fieldValue);
      const compareGt = parseNumber(compareValue);
      if (isNaN(numGt) || isNaN(compareGt)) return false;
      return numGt > compareGt;

    case 'less_than':
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return false;
        return d1 < d2;
      }
      const numLt = parseNumber(fieldValue);
      const compareLt = parseNumber(compareValue);
      if (isNaN(numLt) || isNaN(compareLt)) return false;
      return numLt < compareLt;

    case 'greater_than_or_equal':
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return false;
        return d1 >= d2;
      }
      const numGte = parseNumber(fieldValue);
      const compareGte = parseNumber(compareValue);
      if (isNaN(numGte) || isNaN(compareGte)) return false;
      return numGte >= compareGte;

    case 'less_than_or_equal':
      if (dataType === 'date') {
        const d1 = parseDate(fieldValue);
        const d2 = parseDate(compareValue);
        if (!d1 || !d2) return false;
        return d1 <= d2;
      }
      const numLte = parseNumber(fieldValue);
      const compareLte = parseNumber(compareValue);
      if (isNaN(numLte) || isNaN(compareLte)) return false;
      return numLte <= compareLte;

    case 'between':
      if (dataType === 'date') {
        const d = parseDate(fieldValue);
        const dStart = parseDate(compareValue);
        const dEnd = parseDate(compareValueEnd);
        if (!d || !dStart || !dEnd) return false;
        return d >= dStart && d <= dEnd;
      }
      const numBetween = parseNumber(fieldValue);
      const startVal = parseNumber(compareValue);
      const endVal = parseNumber(compareValueEnd);
      if (isNaN(numBetween) || isNaN(startVal) || isNaN(endVal)) return false;
      return numBetween >= startVal && numBetween <= endVal;

    case 'is_empty':
      return (
        fieldValue === null ||
        fieldValue === undefined ||
        String(fieldValue).trim() === ''
      );

    case 'is_not_empty':
      return (
        fieldValue !== null &&
        fieldValue !== undefined &&
        String(fieldValue).trim() !== ''
      );

    case 'in':
      const inList = String(compareValue)
        .split(',')
        .map((s) => s.trim().toLowerCase());
      return inList.includes(normalizeValue(fieldValue));

    case 'not_in':
      const notInList = String(compareValue)
        .split(',')
        .map((s) => s.trim().toLowerCase());
      return !notInList.includes(normalizeValue(fieldValue));

    default:
      console.warn(`Unknown filter operator: ${condition.operator}`);
      return true;
  }
}

/**
 * Apply filter configuration to an array of records
 * Returns filtered records and statistics
 */
export function applyFilters<T extends Record<string, any>>(
  data: T[],
  filterConfig: FilterConfig | null | undefined
): { filtered: T[]; stats: FilterStats } {
  const stats: FilterStats = {
    totalRecords: data.length,
    matchedRecords: 0,
    filteredOutRecords: 0,
    conditionsApplied: 0,
  };

  // If no filter config or no conditions, return all data
  if (!filterConfig || !filterConfig.conditions || filterConfig.conditions.length === 0) {
    stats.matchedRecords = data.length;
    return { filtered: data, stats };
  }

  stats.conditionsApplied = filterConfig.conditions.length;

  const filtered = data.filter((record) => {
    const results = filterConfig.conditions.map((condition) =>
      evaluateCondition(record, condition)
    );

    const passes =
      filterConfig.logic === 'OR'
        ? results.some((r) => r)
        : results.every((r) => r);

    return passes;
  });

  stats.matchedRecords = filtered.length;
  stats.filteredOutRecords = data.length - filtered.length;

  return { filtered, stats };
}

export interface FilterStats {
  totalRecords: number;
  matchedRecords: number;
  filteredOutRecords: number;
  conditionsApplied: number;
}

/**
 * Validate filter configuration
 */
export function validateFilterConfig(config: any): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!config) {
    return { valid: true, errors: [] }; // Empty config is valid (means no filtering)
  }

  if (config.conditions && !Array.isArray(config.conditions)) {
    errors.push('conditions must be an array');
  }

  if (config.logic && !['AND', 'OR'].includes(config.logic)) {
    errors.push('logic must be either "AND" or "OR"');
  }

  if (config.conditions && Array.isArray(config.conditions)) {
    config.conditions.forEach((condition: any, index: number) => {
      if (!condition.field) {
        errors.push(`Condition ${index + 1}: field is required`);
      }
      if (!condition.operator) {
        errors.push(`Condition ${index + 1}: operator is required`);
      }
      const validOperators: FilterOperator[] = [
        'equals', 'not_equals', 'contains', 'not_contains',
        'starts_with', 'ends_with', 'greater_than', 'less_than',
        'greater_than_or_equal', 'less_than_or_equal', 'between',
        'is_empty', 'is_not_empty', 'in', 'not_in'
      ];
      if (condition.operator && !validOperators.includes(condition.operator)) {
        errors.push(`Condition ${index + 1}: invalid operator "${condition.operator}"`);
      }
      if (condition.operator === 'between' && condition.valueEnd === undefined) {
        errors.push(`Condition ${index + 1}: "between" operator requires valueEnd`);
      }
    });
  }

  return { valid: errors.length === 0, errors };
}

/**
 * Build API query parameters from filter config
 * For server-side filtering when calling external APIs
 */
export function buildApiQueryParams(
  filterConfig: FilterConfig | null | undefined,
  baseParams: Record<string, string> = {}
): Record<string, string> {
  const params = { ...baseParams };

  if (!filterConfig) {
    return params;
  }

  // Add explicit API query params
  if (filterConfig.apiQueryParams) {
    Object.assign(params, filterConfig.apiQueryParams);
  }

  // Convert conditions to common API parameter patterns
  filterConfig.conditions?.forEach((condition) => {
    if (condition.operator === 'equals' && condition.value !== null) {
      params[condition.field] = String(condition.value);
    } else if (condition.operator === 'greater_than' && condition.dataType === 'date') {
      params[`${condition.field}_from`] = String(condition.value);
      params[`${condition.field}_after`] = String(condition.value);
    } else if (condition.operator === 'less_than' && condition.dataType === 'date') {
      params[`${condition.field}_to`] = String(condition.value);
      params[`${condition.field}_before`] = String(condition.value);
    } else if (condition.operator === 'between') {
      params[`${condition.field}_from`] = String(condition.value);
      params[`${condition.field}_to`] = String(condition.valueEnd);
    } else if (condition.operator === 'in' && condition.value) {
      params[condition.field] = String(condition.value);
    }
  });

  return params;
}

/**
 * Get column names from first few records (for auto-detecting available fields)
 */
export function extractFieldsFromData(
  data: Record<string, any>[],
  sampleSize: number = 10
): { name: string; detectedType: 'text' | 'number' | 'date' | 'boolean' }[] {
  if (!data || data.length === 0) {
    return [];
  }

  const sample = data.slice(0, sampleSize);
  const allFields = new Set<string>();
  
  sample.forEach((record) => {
    Object.keys(record).forEach((key) => allFields.add(key));
  });

  return Array.from(allFields).map((fieldName) => {
    // Detect type from sample values
    const values = sample
      .map((r) => r[fieldName])
      .filter((v) => v !== null && v !== undefined && v !== '');

    if (values.length === 0) {
      return { name: fieldName, detectedType: 'text' as const };
    }

    // Check for boolean
    const boolStrings = ['true', 'false', 'yes', 'no', '1', '0'];
    if (values.every((v) => boolStrings.includes(String(v).toLowerCase()))) {
      return { name: fieldName, detectedType: 'boolean' as const };
    }

    // Check for date
    const datePatterns = values.filter((v) => {
      const date = new Date(v);
      return !isNaN(date.getTime()) && String(v).match(/\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}/);
    });
    if (datePatterns.length === values.length) {
      return { name: fieldName, detectedType: 'date' as const };
    }

    // Check for number
    const numericValues = values.filter((v) => {
      const cleaned = String(v).replace(/[,$\s]/g, '');
      return !isNaN(parseFloat(cleaned));
    });
    if (numericValues.length === values.length) {
      return { name: fieldName, detectedType: 'number' as const };
    }

    return { name: fieldName, detectedType: 'text' as const };
  });
}

export default {
  applyFilters,
  validateFilterConfig,
  buildApiQueryParams,
  extractFieldsFromData,
};
