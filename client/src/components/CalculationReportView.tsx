import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { 
  BarChart3, 
  FileText, 
  Download, 
  Layers, 
  Package, 
  MapPin, 
  Users, 
  Calendar,
  TrendingUp,
  DollarSign,
  Hash
} from 'lucide-react';
import { formatDateUSA } from '@/lib/dateFormat';

interface DimensionConfig {
  dimensionKey: string;
  displayName: string;
  dimensionType: string;
  erpFieldName?: string;
  isGroupable: boolean;
  sortOrder: number;
}

interface AggregatedResult {
  dimensionValue: string;
  totalSalesAmount: number;
  totalQuantity: number;
  totalFee: number;
  transactionCount: number;
  avgRate?: number;
}

interface LineItem {
  id: string;
  transactionDate: string;
  transactionId?: string;
  salesAmount: number;
  quantity: number;
  unitPrice: number;
  calculatedFee: number;
  appliedRate: number;
  ruleName?: string;
  ruleType?: string;
  tierApplied?: string;
  dimensions: Record<string, string>;
  vendorName?: string;
  itemName?: string;
  itemClass?: string;
  territory?: string;
  period?: string;
}

interface CalculationReportProps {
  calculationId: string;
  contractId?: string;
}

const getDimensionIcon = (dimensionType: string) => {
  switch (dimensionType) {
    case 'vendor':
      return <Users className="h-4 w-4" />;
    case 'product':
      return <Package className="h-4 w-4" />;
    case 'category':
      return <Layers className="h-4 w-4" />;
    case 'territory':
      return <MapPin className="h-4 w-4" />;
    case 'period':
      return <Calendar className="h-4 w-4" />;
    case 'summary':
      return <BarChart3 className="h-4 w-4" />;
    case 'detail':
      return <FileText className="h-4 w-4" />;
    default:
      return <Layers className="h-4 w-4" />;
  }
};

const formatCurrency = (value: number) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
};

const formatNumber = (value: number, decimals = 0) => {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  }).format(value);
};

export default function CalculationReportView({ calculationId, contractId }: CalculationReportProps) {
  const [activeTab, setActiveTab] = useState('summary');

  const { data: reportData, isLoading: reportLoading } = useQuery({
    queryKey: ['/api/calculations', calculationId, 'report'],
    enabled: !!calculationId
  });

  const { data: dimensionsData, isLoading: dimensionsLoading } = useQuery<{ dimensions: DimensionConfig[] }>({
    queryKey: ['/api/contracts', contractId, 'dimensions'],
    enabled: !!contractId
  });

  const { data: aggregatedData, isLoading: aggregatedLoading } = useQuery<{ dimensionKey: string; data: AggregatedResult[] }>({
    queryKey: ['/api/calculations', calculationId, 'aggregate', activeTab],
    enabled: !!calculationId && activeTab !== 'summary' && activeTab !== 'detail'
  });

  const { data: lineItemsData, isLoading: lineItemsLoading } = useQuery<{ lineItems: LineItem[] }>({
    queryKey: ['/api/calculations', calculationId, 'line-items'],
    enabled: !!calculationId && activeTab === 'detail'
  });

  const { data: summaryData, isLoading: summaryLoading } = useQuery({
    queryKey: ['/api/calculations', calculationId, 'summary-report'],
    enabled: !!calculationId && activeTab === 'summary'
  });

  // Always include Summary and Detail tabs, then add dynamic dimensions
  const baseTabs: DimensionConfig[] = [
    { dimensionKey: 'summary', displayName: 'Summary', dimensionType: 'summary', isGroupable: false, sortOrder: 0 },
    { dimensionKey: 'detail', displayName: 'Detail', dimensionType: 'detail', isGroupable: false, sortOrder: 1 },
  ];
  
  // Filter out summary/detail from API response to avoid duplicates
  const dynamicDimensions = (dimensionsData?.dimensions || [])
    .filter(d => d.dimensionKey !== 'summary' && d.dimensionKey !== 'detail');
  
  // Combine base tabs with dynamic dimensions
  const dimensions = [...baseTabs, ...dynamicDimensions];
  const report = reportData as any;

  if (reportLoading) {
    return (
      <Card>
        <CardHeader>
          <Skeleton className="h-8 w-64" />
          <Skeleton className="h-4 w-96" />
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-64 w-full" />
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card data-testid="calculation-report-view">
      <CardHeader>
        <div className="flex justify-between items-start">
          <div>
            <CardTitle className="flex items-center gap-2" data-testid="text-report-title">
              <BarChart3 className="h-5 w-5" />
              Calculation Report
            </CardTitle>
            <CardDescription>
              {report?.calculationName || 'License Fee Calculation'} 
              {report?.periodStart && report?.periodEnd && (
                <span className="ml-2">
                  ({formatDateUSA(report.periodStart)} - {formatDateUSA(report.periodEnd)})
                </span>
              )}
            </CardDescription>
          </div>
          <Button 
            variant="outline" 
            size="sm" 
            data-testid="button-export-report"
            onClick={() => {
              const link = document.createElement('a');
              link.href = `/api/calculations/${calculationId}/report/${activeTab}/pdf`;
              link.download = `report-${activeTab}.pdf`;
              document.body.appendChild(link);
              link.click();
              document.body.removeChild(link);
            }}
            disabled={activeTab === 'summary' || activeTab === 'detail'}
          >
            <Download className="h-4 w-4 mr-1" />
            Export PDF
          </Button>
        </div>

        <div className="flex gap-4 mt-4 flex-wrap">
          <div className="flex items-center gap-2 px-3 py-2 bg-blue-50 dark:bg-blue-950/20 rounded-lg">
            <DollarSign className="h-4 w-4 text-blue-600" />
            <div>
              <div className="text-xs text-muted-foreground">Total Sales</div>
              <div className="font-semibold text-blue-600" data-testid="text-total-sales">
                {formatCurrency(report?.totalSalesAmount || 0)}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2 px-3 py-2 bg-green-50 dark:bg-green-950/20 rounded-lg">
            <TrendingUp className="h-4 w-4 text-green-600" />
            <div>
              <div className="text-xs text-muted-foreground">Total License Fee</div>
              <div className="font-semibold text-green-600" data-testid="text-total-fee">
                {formatCurrency(report?.totalFee || 0)}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2 px-3 py-2 bg-purple-50 dark:bg-purple-950/20 rounded-lg">
            <Hash className="h-4 w-4 text-purple-600" />
            <div>
              <div className="text-xs text-muted-foreground">Transactions</div>
              <div className="font-semibold text-purple-600" data-testid="text-transaction-count">
                {formatNumber(report?.transactionCount || 0)}
              </div>
            </div>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="flex flex-wrap h-auto gap-1 mb-4" data-testid="tabs-dimensions">
            {dimensions.map((dim) => (
              <TabsTrigger 
                key={dim.dimensionKey} 
                value={dim.dimensionKey}
                className="flex items-center gap-1"
                data-testid={`tab-${dim.dimensionKey}`}
              >
                {getDimensionIcon(dim.dimensionType)}
                {dim.displayName}
              </TabsTrigger>
            ))}
          </TabsList>

          <TabsContent value="summary">
            {summaryLoading ? (
              <Skeleton className="h-64 w-full" />
            ) : (
              <SummaryView data={summaryData} />
            )}
          </TabsContent>

          <TabsContent value="detail">
            {lineItemsLoading ? (
              <Skeleton className="h-64 w-full" />
            ) : (
              <DetailView lineItems={lineItemsData?.lineItems || []} />
            )}
          </TabsContent>

          {dimensions
            .filter(dim => dim.dimensionKey !== 'summary' && dim.dimensionKey !== 'detail')
            .map((dim) => (
              <TabsContent key={dim.dimensionKey} value={dim.dimensionKey}>
                {aggregatedLoading && activeTab === dim.dimensionKey ? (
                  <Skeleton className="h-64 w-full" />
                ) : (
                  <AggregatedView 
                    dimensionName={dim.displayName} 
                    data={activeTab === dim.dimensionKey ? aggregatedData?.data || [] : []} 
                  />
                )}
              </TabsContent>
            ))}
        </Tabs>
      </CardContent>
    </Card>
  );
}

function SummaryView({ data }: { data: any }) {
  if (!data) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        No summary data available. Run a calculation to see results.
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">By Rule</CardTitle>
          </CardHeader>
          <CardContent>
            {data.byRule?.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Rule Name</TableHead>
                    <TableHead className="text-right">Quantity</TableHead>
                    <TableHead className="text-right">License Fee</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.byRule.map((row: any, idx: number) => (
                    <TableRow key={idx}>
                      <TableCell>
                        <div className="font-medium">{row.ruleName || 'Unknown'}</div>
                        <Badge variant="outline" className="text-xs">{row.ruleType}</Badge>
                      </TableCell>
                      <TableCell className="text-right">{formatNumber(row.totalQuantity)}</TableCell>
                      <TableCell className="text-right font-medium">{formatCurrency(row.totalFee)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="text-sm text-muted-foreground text-center py-4">No rule breakdown available</div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">By Item Class</CardTitle>
          </CardHeader>
          <CardContent>
            {data.byItemClass?.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Item Class</TableHead>
                    <TableHead className="text-right">Quantity</TableHead>
                    <TableHead className="text-right">License Fee</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.byItemClass.map((row: any, idx: number) => (
                    <TableRow key={idx}>
                      <TableCell className="font-medium">{row.itemClass}</TableCell>
                      <TableCell className="text-right">{formatNumber(row.totalQuantity)}</TableCell>
                      <TableCell className="text-right font-medium">{formatCurrency(row.totalFee)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="text-sm text-muted-foreground text-center py-4">No item class breakdown available</div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function DetailView({ lineItems }: { lineItems: LineItem[] }) {
  if (lineItems.length === 0) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        No line item details available. Line items are populated when calculations are run.
      </div>
    );
  }

  return (
    <div className="rounded-md border overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Date</TableHead>
            <TableHead>Item</TableHead>
            <TableHead>Category</TableHead>
            <TableHead className="text-right">Quantity</TableHead>
            <TableHead className="text-right">Sales Amount</TableHead>
            <TableHead>Rule Applied</TableHead>
            <TableHead className="text-right">Rate</TableHead>
            <TableHead className="text-right">License Fee</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {lineItems.map((item) => (
            <TableRow key={item.id} data-testid={`row-line-item-${item.id}`}>
              <TableCell className="whitespace-nowrap">
                {item.transactionDate ? formatDateUSA(item.transactionDate) : '-'}
              </TableCell>
              <TableCell>
                <div className="font-medium">{item.itemName || '-'}</div>
                {item.vendorName && (
                  <div className="text-xs text-muted-foreground">{item.vendorName}</div>
                )}
              </TableCell>
              <TableCell>
                {item.itemClass && <Badge variant="outline">{item.itemClass}</Badge>}
              </TableCell>
              <TableCell className="text-right">{formatNumber(item.quantity)}</TableCell>
              <TableCell className="text-right">{formatCurrency(item.salesAmount)}</TableCell>
              <TableCell>
                <div className="text-sm">{item.ruleName || '-'}</div>
                {item.tierApplied && (
                  <div className="text-xs text-muted-foreground">{item.tierApplied}</div>
                )}
              </TableCell>
              <TableCell className="text-right">
                {item.appliedRate ? `${(item.appliedRate * 100).toFixed(2)}%` : '-'}
              </TableCell>
              <TableCell className="text-right font-medium text-green-600">
                {formatCurrency(item.calculatedFee)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function AggregatedView({ dimensionName, data }: { dimensionName: string; data: AggregatedResult[] }) {
  if (data.length === 0) {
    return (
      <div className="text-center py-8 text-muted-foreground">
        No data available for {dimensionName}. Run a calculation to populate this view.
      </div>
    );
  }

  // Check if all values are "Unknown" - this means the dimension data wasn't captured in the source
  const allUnknown = data.every(row => row.dimensionValue === 'Unknown' || !row.dimensionValue);
  
  if (allUnknown && (dimensionName.includes('Supplier') || dimensionName.includes('Vendor'))) {
    return (
      <div className="text-center py-12 text-muted-foreground space-y-3">
        <Users className="h-12 w-12 mx-auto text-muted-foreground/50" />
        <div className="font-medium">Supplier data not available</div>
        <p className="text-sm max-w-md mx-auto">
          The uploaded sales data does not include supplier information. To see this breakdown, 
          ensure your sales data includes a supplier or vendor column when importing.
        </p>
      </div>
    );
  }

  const totalFee = data.reduce((sum, row) => sum + row.totalFee, 0);

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{dimensionName}</TableHead>
            <TableHead className="text-right">Transactions</TableHead>
            <TableHead className="text-right">Quantity</TableHead>
            <TableHead className="text-right">Sales Amount</TableHead>
            <TableHead className="text-right">License Fee</TableHead>
            <TableHead className="text-right">% of Total</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row, idx) => (
            <TableRow key={idx} data-testid={`row-aggregated-${idx}`}>
              <TableCell className="font-medium">{row.dimensionValue}</TableCell>
              <TableCell className="text-right">{formatNumber(row.transactionCount)}</TableCell>
              <TableCell className="text-right">{formatNumber(row.totalQuantity)}</TableCell>
              <TableCell className="text-right">{formatCurrency(row.totalSalesAmount)}</TableCell>
              <TableCell className="text-right font-medium text-green-600">
                {formatCurrency(row.totalFee)}
              </TableCell>
              <TableCell className="text-right">
                <Badge variant="secondary">
                  {totalFee > 0 ? ((row.totalFee / totalFee) * 100).toFixed(1) : 0}%
                </Badge>
              </TableCell>
            </TableRow>
          ))}
          <TableRow className="bg-muted/50 font-semibold">
            <TableCell>Total</TableCell>
            <TableCell className="text-right">{formatNumber(data.reduce((s, r) => s + r.transactionCount, 0))}</TableCell>
            <TableCell className="text-right">{formatNumber(data.reduce((s, r) => s + r.totalQuantity, 0))}</TableCell>
            <TableCell className="text-right">{formatCurrency(data.reduce((s, r) => s + r.totalSalesAmount, 0))}</TableCell>
            <TableCell className="text-right text-green-600">{formatCurrency(totalFee)}</TableCell>
            <TableCell className="text-right">100%</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </div>
  );
}
