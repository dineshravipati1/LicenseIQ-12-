import { useQuery } from "@tanstack/react-query";
import { AlertCircle, CheckCircle, Info, XCircle } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";

interface FormulaPreviewProps {
  contractId: string;
  periodStart?: Date;
  periodEnd?: Date;
}

export function FormulaPreview({ contractId, periodStart, periodEnd }: FormulaPreviewProps) {
  // Build query URL with date parameters
  const queryUrl = periodStart && periodEnd
    ? `/api/contracts/${contractId}/formula-preview?periodStart=${periodStart.toISOString()}&periodEnd=${periodEnd.toISOString()}`
    : `/api/contracts/${contractId}/formula-preview`;

  const { data: preview, isLoading, error } = useQuery({
    queryKey: [queryUrl],
    enabled: !!contractId
  });

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Info className="h-5 w-5" />
            Formula Preview
          </CardTitle>
          <CardDescription>Loading formula preview...</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>
          Failed to load formula preview. Please refresh the page.
        </AlertDescription>
      </Alert>
    );
  }

  if (!preview || preview.totalProducts === 0) {
    return null;
  }

  const matchedSamples = preview.samples.filter((s: any) => s.matched);
  const unmatchedSamples = preview.samples.filter((s: any) => !s.matched);

  return (
    <Card className="bg-gradient-to-br from-purple-50 to-blue-50 dark:from-purple-950/20 dark:to-blue-950/20">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          🌱 Formula Preview ({preview.totalProducts} Products)
        </CardTitle>
        <CardDescription>
          Formulas that will be applied to your sales data
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Summary Stats */}
        <div className="grid grid-cols-3 gap-3">
          <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center">
            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
              {preview.totalRules}
            </div>
            <div className="text-sm text-gray-600 dark:text-gray-400">Active Rules</div>
          </div>
          <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center">
            <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
              {matchedSamples.length}
            </div>
            <div className="text-sm text-gray-600 dark:text-gray-400">Matched Products</div>
          </div>
          <div className="bg-white dark:bg-gray-900 rounded-lg p-3 text-center">
            <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
              {preview.unmatchedSales}
            </div>
            <div className="text-sm text-gray-600 dark:text-gray-400">Unmatched</div>
          </div>
        </div>

        {/* Matched Products */}
        {matchedSamples.length > 0 && (
          <div className="space-y-2">
            <h4 className="font-semibold text-sm flex items-center gap-2 text-green-700 dark:text-green-400">
              <CheckCircle className="h-5 w-5" />
              ✅ Matched Products (Will Generate License Fees)
            </h4>
            <div className="space-y-2">
              {matchedSamples.map((sample: any, idx: number) => (
                <div 
                  key={idx} 
                  className="bg-green-50 dark:bg-green-950/30 border border-green-200 dark:border-green-800 rounded-lg p-3 flex items-start gap-3"
                  data-testid={`formula-preview-matched-${idx}`}
                >
                  <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400 flex-shrink-0 mt-0.5" />
                  <div className="flex-1">
                    <div className="font-medium text-sm text-green-900 dark:text-green-100">{sample.productName}</div>
                    <div className="text-xs text-green-700 dark:text-green-300">
                      {sample.category} • {sample.sampleUnits} units
                    </div>
                    <div className="text-xs text-purple-600 dark:text-purple-400 mt-1 font-medium">
                      📊 {sample.formulaType}
                    </div>
                    
                    {/* Calculation Formula */}
                    {sample.formulaDetails?.calculationFormula && (
                      <div className="mt-2 p-2 bg-blue-100 dark:bg-blue-950 rounded border border-blue-300 dark:border-blue-700">
                        <div className="text-xs font-semibold text-blue-900 dark:text-blue-100 mb-1">
                          💡 Calculation Formula:
                        </div>
                        <pre className="text-xs text-blue-800 dark:text-blue-200 whitespace-pre-wrap font-mono">
                          {sample.formulaDetails.calculationFormula}
                        </pre>
                      </div>
                    )}
                    
                    {/* Detailed Formula Information */}
                    {sample.formulaDetails && (
                      <div className="mt-2 space-y-1 text-xs">
                        {/* Base Rate */}
                        {sample.formulaDetails.baseRate && (
                          <div className="text-green-800 dark:text-green-200">
                            <span className="font-semibold">Base Rate:</span> {
                              sample.formulaDetails.baseRate > 1 
                                ? `$${sample.formulaDetails.baseRate.toFixed(2)} per unit`
                                : `${(sample.formulaDetails.baseRate * 100).toFixed(1)}%`
                            }
                          </div>
                        )}
                        
                        {/* Volume Tiers */}
                        {sample.formulaDetails.volumeTiers && sample.formulaDetails.volumeTiers.length > 0 && (
                          <div className="text-green-800 dark:text-green-200">
                            <div className="font-semibold">Volume Tiers:</div>
                            <ul className="ml-3 mt-1 space-y-0.5">
                              {sample.formulaDetails.volumeTiers.map((tier: any, i: number) => (
                                <li key={i}>
                                  • {tier.min.toLocaleString()}{tier.max ? ` - ${tier.max.toLocaleString()}` : '+'} units → {
                                    tier.rate > 1 
                                      ? `$${tier.rate.toFixed(2)}/unit`
                                      : `${(tier.rate * 100).toFixed(1)}%`
                                  }
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                        
                        {/* Seasonal Adjustments */}
                        {sample.formulaDetails.seasonalAdjustments && Object.keys(sample.formulaDetails.seasonalAdjustments).length > 0 && (
                          <div className="text-green-800 dark:text-green-200">
                            <div className="font-semibold">Seasonal Adjustments:</div>
                            <ul className="ml-3 mt-1 space-y-0.5">
                              {Object.entries(sample.formulaDetails.seasonalAdjustments).map(([season, multiplier]: [string, any]) => (
                                <li key={season}>
                                  • {season}: {multiplier === 1 ? 'Standard' : `${((multiplier - 1) * 100).toFixed(0)}% ${multiplier > 1 ? 'bonus' : 'reduction'}`}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                        
                        {/* Territory Premiums */}
                        {sample.formulaDetails.territoryPremiums && Object.keys(sample.formulaDetails.territoryPremiums).length > 0 && (
                          <div className="text-green-800 dark:text-green-200">
                            <div className="font-semibold">Territory Premiums:</div>
                            <ul className="ml-3 mt-1 space-y-0.5">
                              {Object.entries(sample.formulaDetails.territoryPremiums).map(([territory, multiplier]: [string, any]) => (
                                <li key={territory}>
                                  • {territory}: {multiplier === 1 ? 'Standard' : `${((multiplier - 1) * 100).toFixed(0)}% ${multiplier > 1 ? 'premium' : 'discount'}`}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                  <div className="text-right flex-shrink-0">
                    <div className="text-xs font-medium text-green-800 dark:text-green-200">
                      {sample.ruleName}
                    </div>
                    {sample.confidence && (
                      <div className="text-xs text-green-600 dark:text-green-400">
                        {Math.round(sample.confidence * 100)}% confident
                      </div>
                    )}
                    {sample.sourceSection && (
                      <div className="text-xs text-purple-700 dark:text-purple-400 mt-2 max-w-[200px]" title={sample.sourceText || sample.sourceSection}>
                        📄 {sample.sourceSection}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Unmatched Products */}
        {unmatchedSamples.length > 0 && (
          <div className="space-y-2">
            <h4 className="font-semibold text-sm flex items-center gap-2 text-red-700 dark:text-red-400">
              <XCircle className="h-5 w-5" />
              ❌ Unmatched Products (No License Fees)
            </h4>
            <div className="bg-red-50/50 dark:bg-red-950/20 border border-red-200 dark:border-red-800 rounded-lg p-3 mb-2">
              <p className="text-xs text-red-800 dark:text-red-300">
                Tried to match against <strong>{preview.totalRules} active rule{preview.totalRules !== 1 ? 's' : ''}</strong> but found no matches. 
                {preview.totalRules === 0 ? ' Create rules to generate license fees.' : ' Update rule categories or create new rules.'}
              </p>
            </div>
            <div className="space-y-2">
              {unmatchedSamples.map((sample: any, idx: number) => (
                <div 
                  key={idx} 
                  className="bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 rounded-lg p-3 flex items-start gap-3"
                  data-testid={`formula-preview-unmatched-${idx}`}
                >
                  <XCircle className="h-5 w-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
                  <div className="flex-1">
                    <div className="font-medium text-sm text-red-900 dark:text-red-100">{sample.productName}</div>
                    <div className="text-xs text-red-700 dark:text-red-300">
                      {sample.category} • {sample.sampleUnits} units
                    </div>
                    <div className="text-xs text-red-600 dark:text-red-400 mt-1">
                      No matching rule found - will not generate license fees
                    </div>
                  </div>
                  <div className="px-3 py-1 bg-red-100 dark:bg-red-900 rounded text-xs font-medium text-red-800 dark:text-red-200">
                    Needs Rule
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Summary Alert */}
        {preview.unmatchedSales > 0 && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              <strong>{preview.unmatchedSales} product{preview.unmatchedSales > 1 ? 's' : ''}</strong> will not generate license fees. Please create matching rules or update existing rule categories to include these products.
            </AlertDescription>
          </Alert>
        )}
      </CardContent>
    </Card>
  );
}
