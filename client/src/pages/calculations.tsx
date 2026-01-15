import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Calculator, FileText, Download, TrendingUp, Calendar, DollarSign, ArrowLeft, ChevronDown, ChevronUp, BarChart3 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useLocation } from "wouter";
import { format } from "date-fns";
import { useState, useEffect } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import MainLayout from "@/components/layout/main-layout";
import { Separator } from "@/components/ui/separator";

interface CalculationsResponse {
  calculations: any[];
}

export default function CalculationsPage() {
  const [_, setLocation] = useLocation();
  const [expandedCalcId, setExpandedCalcId] = useState<string | null>(null);
  const [calcDetails, setCalcDetails] = useState<Record<string, any>>({});

  // Fetch all calculations across all contracts
  const { data: calculations, isLoading } = useQuery<CalculationsResponse>({
    queryKey: ["/api/calculations/all"],
  });

  const handleToggleDetails = async (calculation: any) => {
    const calcId = calculation.id;
    
    // If clicking the same card, collapse it
    if (expandedCalcId === calcId) {
      setExpandedCalcId(null);
      return;
    }

    // If we don't have details cached, fetch them
    if (!calcDetails[calcId]) {
      const response = await fetch(`/api/calculations/${calcId}/details`);
      if (response.ok) {
        const details = await response.json();
        setCalcDetails(prev => ({ ...prev, [calcId]: details }));
      }
    }
    
    // Expand this card
    setExpandedCalcId(calcId);
  };

  const handleDownloadReport = async (calcId: string) => {
    window.open(`/api/royalty-calculations/${calcId}/invoice/detailed`, '_blank');
  };

  if (isLoading) {
    return (
      <MainLayout title="License Fee Calculator" description="Loading...">
        <div className="space-y-4">
          <div className="flex items-center justify-between space-y-2">
            <Skeleton className="h-10 w-64" />
          </div>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-48" />
            ))}
          </div>
        </div>
      </MainLayout>
    );
  }

  const allCalculations = calculations?.calculations || [];

  return (
    <MainLayout 
      title="License Fee Calculator" 
      description="View and manage all license fee calculations across your contracts"
    >
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <Button 
            variant="outline" 
            onClick={() => setLocation("/")}
            data-testid="button-back-dashboard"
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Dashboard
          </Button>
          <Button 
            onClick={() => setLocation("/contracts")}
            data-testid="button-view-contracts"
          >
            <FileText className="mr-2 h-4 w-4" />
            View Contracts
          </Button>
        </div>

        {/* Summary Stats */}
        <div className="grid gap-4 md:grid-cols-3">
          <Card className="bg-gradient-to-br from-blue-50 to-indigo-50 dark:from-blue-950/20 dark:to-indigo-950/20 border-blue-200 dark:border-blue-800">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-blue-900 dark:text-blue-100">Total Calculations</CardTitle>
              <Calculator className="h-5 w-5 text-blue-600 dark:text-blue-400" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-blue-900 dark:text-blue-100">{allCalculations.length}</div>
              <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">Completed license fee runs</p>
            </CardContent>
          </Card>
          <Card className="bg-gradient-to-br from-green-50 to-emerald-50 dark:from-green-950/20 dark:to-emerald-950/20 border-green-200 dark:border-green-800">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-green-900 dark:text-green-100">Total Amount</CardTitle>
              <DollarSign className="h-5 w-5 text-green-600 dark:text-green-400" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-green-900 dark:text-green-100">
                ${allCalculations.reduce((sum: number, calc: any) => sum + (parseFloat(calc.totalRoyalty) || 0), 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
              </div>
              <p className="text-xs text-green-600 dark:text-green-400 mt-1">Total license fees calculated</p>
            </CardContent>
          </Card>
          <Card className="bg-gradient-to-br from-purple-50 to-pink-50 dark:from-purple-950/20 dark:to-pink-950/20 border-purple-200 dark:border-purple-800">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-purple-900 dark:text-purple-100">Avg Calculation</CardTitle>
              <TrendingUp className="h-5 w-5 text-purple-600 dark:text-purple-400" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-purple-900 dark:text-purple-100">
                ${allCalculations.length > 0 ? (allCalculations.reduce((sum: number, calc: any) => sum + (parseFloat(calc.totalRoyalty) || 0), 0) / allCalculations.length).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '0.00'}
              </div>
              <p className="text-xs text-purple-600 dark:text-purple-400 mt-1">Per calculation average</p>
            </CardContent>
          </Card>
        </div>

        {/* Calculations List */}
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {allCalculations.length === 0 ? (
          <Card className="col-span-full">
            <CardContent className="flex flex-col items-center justify-center py-12">
              <Calculator className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground text-center">
                No calculations found. Upload sales data and calculate royalties from your contracts.
              </p>
              <Button className="mt-4" onClick={() => setLocation("/contracts")}>
                Go to Contracts
              </Button>
            </CardContent>
          </Card>
        ) : (
          allCalculations.map((calc: any) => {
            const isExpanded = expandedCalcId === calc.id;
            const details = calcDetails[calc.id];

            return (
              <Card key={calc.id} className={`hover:shadow-lg transition-all ${isExpanded ? 'col-span-full' : ''}`}>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <CardTitle className="text-lg">{calc.name}</CardTitle>
                      <CardDescription className="mt-1">{calc.contractName}</CardDescription>
                    </div>
                    <Badge variant="outline" className="ml-2">
                      {calc.itemsProcessed || 0} items
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center text-sm text-muted-foreground">
                      <Calendar className="mr-2 h-4 w-4" />
                      {calc.createdAt ? format(new Date(calc.createdAt), 'MMM dd, yyyy') : 'N/A'}
                    </div>
                  </div>

                  <div className="pt-2 border-t">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium">Total Royalty:</span>
                      <span className="text-xl font-bold text-green-600">
                        ${parseFloat(calc.totalRoyalty || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-2 pt-2">
                    <div className="flex gap-2">
                      <Button
                        variant={isExpanded ? "default" : "outline"}
                        size="sm"
                        className="flex-1"
                        onClick={() => handleToggleDetails(calc)}
                        data-testid={`button-view-details-${calc.id}`}
                      >
                        {isExpanded ? <ChevronUp className="mr-2 h-4 w-4" /> : <ChevronDown className="mr-2 h-4 w-4" />}
                        {isExpanded ? 'Hide Details' : 'View Details'}
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="flex-1"
                        onClick={() => handleDownloadReport(calc.id)}
                        data-testid={`button-download-${calc.id}`}
                      >
                        <Download className="mr-2 h-4 w-4" />
                        Download
                      </Button>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      className="w-full"
                      onClick={() => setLocation(`/contracts/${calc.contractId}/rules`)}
                      data-testid={`button-view-rules-${calc.id}`}
                    >
                      <FileText className="mr-2 h-4 w-4" />
                      View Rules
                    </Button>
                    <Button
                      variant="default"
                      size="sm"
                      className="w-full"
                      onClick={() => setLocation(`/calculations/${calc.id}/report`)}
                      data-testid={`button-view-report-${calc.id}`}
                    >
                      <BarChart3 className="mr-2 h-4 w-4" />
                      View Report
                    </Button>
                  </div>

                  {/* Inline Details Section */}
                  {isExpanded && details && (
                    <div className="pt-4 space-y-6 animate-in fade-in slide-in-from-top-4 duration-300">
                      <Separator />
                      
                      {/* Calculation Summary */}
                      <div className="bg-slate-50 dark:bg-slate-900 p-4 rounded-lg space-y-3">
                        <h3 className="font-semibold text-lg">Calculation Summary</h3>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                          <div>
                            <p className="text-sm text-muted-foreground">Total Items</p>
                            <p className="text-lg font-semibold">{details.itemsProcessed || 0}</p>
                          </div>
                          <div>
                            <p className="text-sm text-muted-foreground">Total Royalty</p>
                            <p className="text-lg font-semibold text-green-600">
                              ${parseFloat(details.totalRoyalty || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                            </p>
                          </div>
                          <div>
                            <p className="text-sm text-muted-foreground">Calculation Date</p>
                            <p className="text-lg font-semibold">
                              {details.createdAt ? format(new Date(details.createdAt), 'MMM dd, yyyy') : 'N/A'}
                            </p>
                          </div>
                        </div>
                      </div>

                      {/* Applied Rules */}
                      {details.appliedRules && details.appliedRules.length > 0 && (
                        <div>
                          <h3 className="font-semibold text-lg mb-3">Applied Royalty Rules</h3>
                          <div className="space-y-2">
                            {details.appliedRules.map((rule: any, idx: number) => (
                              <div key={idx} className="p-3 border rounded-lg bg-white dark:bg-slate-950">
                                <div className="font-medium">{rule.ruleName}</div>
                                <div className="text-sm text-muted-foreground mt-1">{rule.description}</div>
                                <div className="flex gap-2 mt-2">
                                  <Badge variant="secondary">{rule.ruleType}</Badge>
                                  {rule.rate && <Badge variant="outline">{rule.rate}% rate</Badge>}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Transaction Breakdown */}
                      {details.lineItems && details.lineItems.length > 0 && (
                        <div>
                          <h3 className="font-semibold text-lg mb-3">Transaction Processing Details</h3>
                          <div className="border rounded-lg overflow-hidden">
                            <Table>
                              <TableHeader>
                                <TableRow className="bg-slate-50 dark:bg-slate-900">
                                  <TableHead className="font-semibold">Product</TableHead>
                                  <TableHead className="text-right font-semibold">Quantity</TableHead>
                                  <TableHead className="text-right font-semibold">Sales Amount</TableHead>
                                  <TableHead className="font-semibold">Rule Applied</TableHead>
                                  <TableHead className="text-right font-semibold">Royalty</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {details.lineItems.map((item: any, idx: number) => (
                                  <TableRow key={idx}>
                                    <TableCell className="font-medium">{item.productName}</TableCell>
                                    <TableCell className="text-right">{item.quantity}</TableCell>
                                    <TableCell className="text-right">
                                      ${parseFloat(item.salesAmount || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                                    </TableCell>
                                    <TableCell>
                                      <Badge variant="outline">{item.ruleName || 'Default'}</Badge>
                                    </TableCell>
                                    <TableCell className="text-right font-medium text-green-600">
                                      ${parseFloat(item.royaltyAmount || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })
        )}
        </div>
      </div>
    </MainLayout>
  );
}
