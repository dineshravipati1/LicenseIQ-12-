import { useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Plus, Trash2, Filter, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

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

interface FieldOption {
  name: string;
  label: string;
  dataType: 'text' | 'number' | 'date' | 'boolean';
}

interface FilterBuilderProps {
  value: FilterConfig;
  onChange: (config: FilterConfig) => void;
  availableFields?: FieldOption[];
  showApiParams?: boolean;
  className?: string;
  previewCount?: number;
  onPreviewRequest?: () => void;
  isPreviewLoading?: boolean;
}

const OPERATORS: Record<string, { label: string; types: ('text' | 'number' | 'date' | 'boolean')[] }> = {
  equals: { label: 'Equals', types: ['text', 'number', 'date', 'boolean'] },
  not_equals: { label: 'Not Equals', types: ['text', 'number', 'date', 'boolean'] },
  contains: { label: 'Contains', types: ['text'] },
  not_contains: { label: 'Does Not Contain', types: ['text'] },
  starts_with: { label: 'Starts With', types: ['text'] },
  ends_with: { label: 'Ends With', types: ['text'] },
  greater_than: { label: 'Greater Than', types: ['number', 'date'] },
  less_than: { label: 'Less Than', types: ['number', 'date'] },
  greater_than_or_equal: { label: 'Greater Than or Equal', types: ['number', 'date'] },
  less_than_or_equal: { label: 'Less Than or Equal', types: ['number', 'date'] },
  between: { label: 'Between', types: ['number', 'date'] },
  is_empty: { label: 'Is Empty', types: ['text', 'number', 'date'] },
  is_not_empty: { label: 'Is Not Empty', types: ['text', 'number', 'date'] },
  in: { label: 'In List', types: ['text', 'number'] },
  not_in: { label: 'Not In List', types: ['text', 'number'] },
};

const DEFAULT_FIELDS: FieldOption[] = [
  { name: 'status', label: 'Status', dataType: 'text' },
  { name: 'date', label: 'Date', dataType: 'date' },
  { name: 'amount', label: 'Amount', dataType: 'number' },
  { name: 'name', label: 'Name', dataType: 'text' },
  { name: 'id', label: 'ID', dataType: 'text' },
  { name: 'type', label: 'Type', dataType: 'text' },
  { name: 'active', label: 'Active', dataType: 'boolean' },
];

function generateId(): string {
  return Math.random().toString(36).substring(2, 11);
}

export function FilterBuilder({
  value,
  onChange,
  availableFields = DEFAULT_FIELDS,
  showApiParams = false,
  className,
  previewCount,
  onPreviewRequest,
  isPreviewLoading = false,
}: FilterBuilderProps) {
  const [customFieldName, setCustomFieldName] = useState('');

  const addCondition = useCallback(() => {
    const newCondition: ImportFilter = {
      id: generateId(),
      field: availableFields[0]?.name || '',
      operator: 'equals',
      value: '',
      dataType: availableFields[0]?.dataType || 'text',
    };
    onChange({
      ...value,
      conditions: [...value.conditions, newCondition],
    });
  }, [value, onChange, availableFields]);

  const removeCondition = useCallback((id: string) => {
    onChange({
      ...value,
      conditions: value.conditions.filter((c) => c.id !== id),
    });
  }, [value, onChange]);

  const updateCondition = useCallback((id: string, updates: Partial<ImportFilter>) => {
    onChange({
      ...value,
      conditions: value.conditions.map((c) =>
        c.id === id ? { ...c, ...updates } : c
      ),
    });
  }, [value, onChange]);

  const setLogic = useCallback((logic: 'AND' | 'OR') => {
    onChange({ ...value, logic });
  }, [value, onChange]);

  const addApiParam = useCallback(() => {
    if (!customFieldName.trim()) return;
    onChange({
      ...value,
      apiQueryParams: {
        ...value.apiQueryParams,
        [customFieldName.trim()]: '',
      },
    });
    setCustomFieldName('');
  }, [value, onChange, customFieldName]);

  const updateApiParam = useCallback((key: string, newValue: string) => {
    onChange({
      ...value,
      apiQueryParams: {
        ...value.apiQueryParams,
        [key]: newValue,
      },
    });
  }, [value, onChange]);

  const removeApiParam = useCallback((key: string) => {
    const params = { ...value.apiQueryParams };
    delete params[key];
    onChange({
      ...value,
      apiQueryParams: params,
    });
  }, [value, onChange]);

  const getOperatorsForType = (dataType: string) => {
    return Object.entries(OPERATORS).filter(([_, config]) =>
      config.types.includes(dataType as any)
    );
  };

  const getFieldDataType = (fieldName: string): 'text' | 'number' | 'date' | 'boolean' => {
    const field = availableFields.find((f) => f.name === fieldName);
    return field?.dataType || 'text';
  };

  const needsNoValue = (operator: FilterOperator): boolean => {
    return operator === 'is_empty' || operator === 'is_not_empty';
  };

  const needsTwoValues = (operator: FilterOperator): boolean => {
    return operator === 'between';
  };

  return (
    <Card className={cn('border-dashed', className)} data-testid="filter-builder">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Filter className="h-4 w-4" />
            Import Filters
          </CardTitle>
          {previewCount !== undefined && (
            <Badge variant="secondary" data-testid="filter-preview-count">
              {previewCount.toLocaleString()} records match
            </Badge>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {value.conditions.length === 0 ? (
          <div className="text-center py-6 text-muted-foreground border border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p className="text-sm">No filters configured</p>
            <p className="text-xs">All records will be imported</p>
          </div>
        ) : (
          <>
            {value.conditions.map((condition, index) => (
              <div
                key={condition.id}
                className="flex flex-wrap items-end gap-2 p-3 bg-muted/50 rounded-lg"
                data-testid={`filter-condition-${index}`}
              >
                {index > 0 && (
                  <Badge variant="outline" className="mb-2 w-full justify-center">
                    {value.logic}
                  </Badge>
                )}

                <div className="flex-1 min-w-[140px] space-y-1">
                  <Label className="text-xs">Field</Label>
                  <Select
                    value={condition.field}
                    onValueChange={(v) => {
                      const dataType = getFieldDataType(v);
                      updateCondition(condition.id, {
                        field: v,
                        dataType,
                        operator: 'equals',
                        value: '',
                        valueEnd: undefined,
                      });
                    }}
                  >
                    <SelectTrigger data-testid={`filter-field-${index}`}>
                      <SelectValue placeholder="Select field" />
                    </SelectTrigger>
                    <SelectContent>
                      {availableFields.map((f) => (
                        <SelectItem key={f.name} value={f.name}>
                          {f.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex-1 min-w-[160px] space-y-1">
                  <Label className="text-xs">Operator</Label>
                  <Select
                    value={condition.operator}
                    onValueChange={(v) =>
                      updateCondition(condition.id, {
                        operator: v as FilterOperator,
                        valueEnd: undefined,
                      })
                    }
                  >
                    <SelectTrigger data-testid={`filter-operator-${index}`}>
                      <SelectValue placeholder="Select operator" />
                    </SelectTrigger>
                    <SelectContent>
                      {getOperatorsForType(condition.dataType || 'text').map(
                        ([key, config]) => (
                          <SelectItem key={key} value={key}>
                            {config.label}
                          </SelectItem>
                        )
                      )}
                    </SelectContent>
                  </Select>
                </div>

                {!needsNoValue(condition.operator) && (
                  <div className={cn('flex-1 min-w-[140px] space-y-1', needsTwoValues(condition.operator) && 'flex gap-2 items-end')}>
                    <Label className="text-xs">
                      {needsTwoValues(condition.operator) ? 'From' : 'Value'}
                    </Label>
                    <Input
                      type={
                        condition.dataType === 'number'
                          ? 'number'
                          : condition.dataType === 'date'
                          ? 'date'
                          : 'text'
                      }
                      value={condition.value?.toString() || ''}
                      onChange={(e) =>
                        updateCondition(condition.id, {
                          value:
                            condition.dataType === 'number'
                              ? parseFloat(e.target.value) || 0
                              : e.target.value,
                        })
                      }
                      placeholder={
                        condition.operator === 'in' || condition.operator === 'not_in'
                          ? 'Comma-separated values'
                          : 'Enter value'
                      }
                      data-testid={`filter-value-${index}`}
                    />
                    {needsTwoValues(condition.operator) && (
                      <div className="space-y-1">
                        <Label className="text-xs">To</Label>
                        <Input
                          type={
                            condition.dataType === 'number'
                              ? 'number'
                              : condition.dataType === 'date'
                              ? 'date'
                              : 'text'
                          }
                          value={condition.valueEnd?.toString() || ''}
                          onChange={(e) =>
                            updateCondition(condition.id, {
                              valueEnd:
                                condition.dataType === 'number'
                                  ? parseFloat(e.target.value) || 0
                                  : e.target.value,
                            })
                          }
                          placeholder="End value"
                          data-testid={`filter-value-end-${index}`}
                        />
                      </div>
                    )}
                  </div>
                )}

                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => removeCondition(condition.id)}
                  className="text-destructive hover:text-destructive"
                  data-testid={`filter-remove-${index}`}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            ))}

            {value.conditions.length > 1 && (
              <div className="flex items-center gap-4 p-3 bg-muted/30 rounded-lg">
                <Label className="text-sm font-medium">Match:</Label>
                <RadioGroup
                  value={value.logic}
                  onValueChange={(v) => setLogic(v as 'AND' | 'OR')}
                  className="flex gap-4"
                >
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="AND" id="logic-and" data-testid="filter-logic-and" />
                    <Label htmlFor="logic-and" className="text-sm cursor-pointer">
                      ALL conditions (AND)
                    </Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="OR" id="logic-or" data-testid="filter-logic-or" />
                    <Label htmlFor="logic-or" className="text-sm cursor-pointer">
                      ANY condition (OR)
                    </Label>
                  </div>
                </RadioGroup>
              </div>
            )}
          </>
        )}

        <Button
          variant="outline"
          size="sm"
          onClick={addCondition}
          className="w-full"
          data-testid="filter-add-condition"
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Filter Condition
        </Button>

        {showApiParams && (
          <div className="pt-4 border-t space-y-3">
            <Label className="text-sm font-medium">API Query Parameters</Label>
            <p className="text-xs text-muted-foreground">
              These parameters are sent directly to the ERP API endpoint
            </p>
            
            {Object.entries(value.apiQueryParams || {}).map(([key, paramValue]) => (
              <div key={key} className="flex items-center gap-2">
                <Input
                  value={key}
                  disabled
                  className="flex-1 bg-muted"
                />
                <span className="text-muted-foreground">=</span>
                <Input
                  value={paramValue}
                  onChange={(e) => updateApiParam(key, e.target.value)}
                  placeholder="Value"
                  className="flex-1"
                  data-testid={`api-param-value-${key}`}
                />
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => removeApiParam(key)}
                  className="text-destructive hover:text-destructive"
                  data-testid={`api-param-remove-${key}`}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            ))}

            <div className="flex items-center gap-2">
              <Input
                value={customFieldName}
                onChange={(e) => setCustomFieldName(e.target.value)}
                placeholder="Parameter name"
                className="flex-1"
                data-testid="api-param-name-input"
              />
              <Button
                variant="outline"
                size="sm"
                onClick={addApiParam}
                disabled={!customFieldName.trim()}
                data-testid="api-param-add"
              >
                <Plus className="h-4 w-4 mr-1" />
                Add
              </Button>
            </div>
          </div>
        )}

        {onPreviewRequest && (
          <Button
            variant="secondary"
            size="sm"
            onClick={onPreviewRequest}
            disabled={isPreviewLoading}
            className="w-full"
            data-testid="filter-preview-button"
          >
            {isPreviewLoading ? 'Checking...' : 'Preview Matching Records'}
          </Button>
        )}
      </CardContent>
    </Card>
  );
}

export function applyFiltersToData<T extends Record<string, any>>(
  data: T[],
  config: FilterConfig
): T[] {
  if (!config.conditions || config.conditions.length === 0) {
    return data;
  }

  return data.filter((record) => {
    const results = config.conditions.map((condition) => {
      const fieldValue = record[condition.field];
      const compareValue = condition.value;
      const compareValueEnd = condition.valueEnd;

      switch (condition.operator) {
        case 'equals':
          return String(fieldValue).toLowerCase() === String(compareValue).toLowerCase();
        
        case 'not_equals':
          return String(fieldValue).toLowerCase() !== String(compareValue).toLowerCase();
        
        case 'contains':
          return String(fieldValue).toLowerCase().includes(String(compareValue).toLowerCase());
        
        case 'not_contains':
          return !String(fieldValue).toLowerCase().includes(String(compareValue).toLowerCase());
        
        case 'starts_with':
          return String(fieldValue).toLowerCase().startsWith(String(compareValue).toLowerCase());
        
        case 'ends_with':
          return String(fieldValue).toLowerCase().endsWith(String(compareValue).toLowerCase());
        
        case 'greater_than':
          return Number(fieldValue) > Number(compareValue);
        
        case 'less_than':
          return Number(fieldValue) < Number(compareValue);
        
        case 'greater_than_or_equal':
          return Number(fieldValue) >= Number(compareValue);
        
        case 'less_than_or_equal':
          return Number(fieldValue) <= Number(compareValue);
        
        case 'between':
          const numValue = Number(fieldValue);
          return numValue >= Number(compareValue) && numValue <= Number(compareValueEnd);
        
        case 'is_empty':
          return fieldValue === null || fieldValue === undefined || fieldValue === '';
        
        case 'is_not_empty':
          return fieldValue !== null && fieldValue !== undefined && fieldValue !== '';
        
        case 'in':
          const inList = String(compareValue).split(',').map(s => s.trim().toLowerCase());
          return inList.includes(String(fieldValue).toLowerCase());
        
        case 'not_in':
          const notInList = String(compareValue).split(',').map(s => s.trim().toLowerCase());
          return !notInList.includes(String(fieldValue).toLowerCase());
        
        default:
          return true;
      }
    });

    if (config.logic === 'OR') {
      return results.some((r) => r);
    }
    return results.every((r) => r);
  });
}

export function serializeFiltersForApi(config: FilterConfig): Record<string, any> {
  const result: Record<string, any> = {
    ...config.apiQueryParams,
  };

  config.conditions.forEach((condition) => {
    if (condition.operator === 'equals') {
      result[condition.field] = condition.value;
    } else if (condition.operator === 'greater_than' && condition.dataType === 'date') {
      result[`${condition.field}_from`] = condition.value;
    } else if (condition.operator === 'less_than' && condition.dataType === 'date') {
      result[`${condition.field}_to`] = condition.value;
    } else if (condition.operator === 'between') {
      result[`${condition.field}_from`] = condition.value;
      result[`${condition.field}_to`] = condition.valueEnd;
    }
  });

  return result;
}

export const emptyFilterConfig: FilterConfig = {
  conditions: [],
  logic: 'AND',
  apiQueryParams: {},
};
