import { useState } from "react";
import { useParams, useLocation } from "wouter";
import { useQuery, useMutation } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Calculator,
  DollarSign,
  TrendingUp,
  FileText,
  ArrowLeft,
  CheckCircle,
  Clock,
  XCircle,
  Receipt,
  BarChart3,
  PieChart as PieChartIcon,
  Calendar,
  Trash2,
  Settings,
  Download,
  FileDown,
} from "lucide-react";
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { format } from "date-fns";
import { FormulaPreview } from "@/components/formula-preview";

export default function RoyaltyDashboard() {
  const { id } = useParams();
  const [, setLocation] = useLocation();
  const { toast } = useToast();
  const [isCalculating, setIsCalculating] = useState(false);
  const [periodStart, setPeriodStart] = useState("");
  const [periodEnd, setPeriodEnd] = useState("");
  const [calculationName, setCalculationName] = useState("");
  const [isFormOpen, setIsFormOpen] = useState(false);

  // Fetch contract details
  const { data: contract, isLoading: contractLoading } = useQuery({
    queryKey: ["/api/contracts", id],
    enabled: !!id,
  });

  // Fetch sales data for this contract
  const { data: salesResponse, isLoading: salesLoading } = useQuery({
    queryKey: [`/api/contracts/${id}/sales`],
    enabled: !!id,
  });

  // Fetch royalty calculations
  const { data: calculationsResponse, isLoading: calculationsLoading } = useQuery({
    queryKey: [`/api/contracts/${id}/royalty-calculations`],
    enabled: !!id,
  });

  const salesData = (salesResponse as any)?.salesData || [];
  const calculations = (calculationsResponse as any)?.calculations || [];
  const latestCalculation = calculations[0];

  const calculateMutation = useMutation({
    mutationFn: async () => {
      setIsCalculating(true);
      const response = await apiRequest("POST", `/api/contracts/${id}/calculate-royalties`, {
        periodStart: periodStart || undefined,
        periodEnd: periodEnd || undefined,
        name: calculationName || undefined,
      });
      return response.json();
    },
    onSuccess: (data) => {
      setIsCalculating(false);
      setIsFormOpen(false);
      toast({
        title: "Calculation Complete",
        description: data.message || "License fees calculated successfully",
      });
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
    },
    onError: (error: Error) => {
      setIsCalculating(false);
      toast({
        title: "Calculation Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const deleteSalesMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("DELETE", `/api/contracts/${id}/sales`);
      try {
        return await response.json();
      } catch {
        return { message: "Sales data deleted successfully" };
      }
    },
    onSuccess: async (data) => {
      toast({
        title: "Sales Data Deleted",
        description: data.message || "All sales data has been deleted",
      });
      await queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/sales`] });
      await queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
      await queryClient.refetchQueries({ queryKey: [`/api/contracts/${id}/sales`] });
      await queryClient.refetchQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
    },
    onError: (error: Error) => {
      toast({
        title: "Delete Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const deleteCalculationsMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("DELETE", `/api/contracts/${id}/royalty-calculations`);
      try {
        return await response.json();
      } catch {
        return { message: "All calculations deleted successfully" };
      }
    },
    onSuccess: async (data) => {
      toast({
        title: "Calculations Deleted",
        description: data.message || "All license fee calculations have been deleted",
      });
      await queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
      await queryClient.refetchQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
    },
    onError: (error: Error) => {
      toast({
        title: "Delete Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const deleteSingleCalculationMutation = useMutation({
    mutationFn: async (calculationId: string) => {
      const response = await apiRequest("DELETE", `/api/royalty-calculations/${calculationId}`);
      try {
        return await response.json();
      } catch {
        return { message: "Calculation deleted successfully" };
      }
    },
    onSuccess: async (data) => {
      toast({
        title: "Calculation Deleted",
        description: data.message || "Calculation has been deleted",
      });
      await queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
      await queryClient.refetchQueries({ queryKey: [`/api/contracts/${id}/royalty-calculations`] });
    },
    onError: (error: Error) => {
      toast({
        title: "Delete Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Prepare chart data - aggregate by product name
  const chartData = (() => {
    if (!latestCalculation?.breakdown) return [];
    
    // Group by product name and sum amounts
    const productMap = new Map<string, { sales: number; royalty: number }>();
    
    latestCalculation.breakdown.forEach((item: any) => {
      const productName = item.productName || item.transactionId || 'Unknown';
      const existing = productMap.get(productName) || { sales: 0, royalty: 0 };
      
      productMap.set(productName, {
        sales: existing.sales + parseFloat(item.saleAmount || 0),
        royalty: existing.royalty + parseFloat(item.royaltyAmount || 0),
      });
    });
    
    // Convert to array and sort by sales amount (descending)
    return Array.from(productMap.entries())
      .map(([name, data]) => ({ name, ...data }))
      .sort((a, b) => b.sales - a.sales)
      .slice(0, 10); // Top 10 products
  })();

  const pieData = [
    { name: "Sales Amount", value: parseFloat(latestCalculation?.totalSalesAmount || 0), color: "#8b5cf6" },
    { name: "License Fee", value: parseFloat(latestCalculation?.totalRoyalty || 0), color: "#ec4899" },
  ];

  const COLORS = ["#8b5cf6", "#ec4899"];

  if (contractLoading || salesLoading || calculationsLoading) {
    return (
      <MainLayout
        title="License Fee Calculator"
        description="Calculate and manage license fee payments"
      >
        <div className="space-y-6">
          <Skeleton className="h-40 w-full" />
          <Skeleton className="h-96 w-full" />
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout
      title="License Fee Calculator"
      description={
        `Calculate license fees for ${(contract as any)?.contractNumber ? `${(contract as any).contractNumber} - ` : ''}${(contract as any)?.originalName || 'contract'}`
      }
    >
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <Button variant="ghost" onClick={() => setLocation(`/contracts/${id}`)}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Contract
          </Button>
          <Button 
            variant="outline" 
            onClick={() => setLocation(`/contracts/${id}/rules`)}
            className="bg-gradient-to-r from-violet-600 to-purple-600 text-white hover:from-violet-700 hover:to-purple-700"
            data-testid="button-manage-rules"
          >
            <Settings className="h-4 w-4 mr-2" />
            Manage License Fee Rules
          </Button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="bg-gradient-to-br from-purple-500 to-purple-600 text-white">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <Receipt className="h-4 w-4" />
                Sales Transactions
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{salesData.length}</div>
              <p className="text-xs text-purple-100 mt-1">Total matched sales</p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-pink-500 to-pink-600 text-white">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <DollarSign className="h-4 w-4" />
                Total Sales
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">
                ${parseFloat(latestCalculation?.totalSalesAmount || "0").toLocaleString()}
              </div>
              <p className="text-xs text-pink-100 mt-1">Gross revenue</p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-blue-500 to-blue-600 text-white">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Final License Fee
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">
                ${parseFloat(latestCalculation?.totalRoyalty || "0").toLocaleString()}
              </div>
              <p className="text-xs text-blue-100 mt-1">
                {latestCalculation?.minimumGuarantee && parseFloat(latestCalculation.totalRoyalty) === latestCalculation.minimumGuarantee
                  ? "Minimum guarantee applied"
                  : "Calculated amount"}
              </p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-green-500 to-green-600 text-white">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <Calculator className="h-4 w-4" />
                Calculations
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">{calculations.length}</div>
              <p className="text-xs text-green-100 mt-1">Total runs</p>
            </CardContent>
          </Card>
        </div>

        {latestCalculation?.minimumGuarantee && latestCalculation?.calculatedRoyalty && (
          <Card className="border-amber-200 bg-amber-50 dark:bg-amber-950 dark:border-amber-800">
            <CardContent className="pt-6">
              <div className="flex items-start gap-3">
                <div className="p-2 bg-amber-100 dark:bg-amber-900 rounded-lg">
                  <TrendingUp className="h-5 w-5 text-amber-600 dark:text-amber-400" />
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold text-amber-900 dark:text-amber-100">
                    Minimum Guarantee Status
                  </h3>
                  <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
                    Calculated royalty: <strong>${parseFloat(latestCalculation.calculatedRoyalty).toLocaleString()}</strong>
                    {" • "}
                    Minimum guarantee: <strong>${parseFloat(latestCalculation.minimumGuarantee).toLocaleString()}</strong>
                    {" • "}
                    Final amount: <strong>${parseFloat(latestCalculation.totalRoyalty).toLocaleString()}</strong>
                  </p>
                  {parseFloat(latestCalculation.totalRoyalty) === latestCalculation.minimumGuarantee && (
                    <p className="text-sm text-amber-600 dark:text-amber-400 mt-2 font-medium">
                      ⚠️ Minimum guarantee has been applied. The calculated amount was below the contractual minimum.
                    </p>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {salesData.length > 0 && (
          <FormulaPreview 
            contractId={id!} 
            periodStart={periodStart ? new Date(periodStart) : undefined}
            periodEnd={periodEnd ? new Date(periodEnd) : undefined}
          />
        )}

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator className="h-5 w-5" />
              Calculate License Fee
            </CardTitle>
            <CardDescription>
              Calculate license fees based on matched sales data. 
              {salesData.length === 0 && " Please upload sales data first."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Collapsible open={isFormOpen} onOpenChange={setIsFormOpen}>
              <CollapsibleTrigger asChild>
                <Button 
                  className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600"
                  disabled={salesData.length === 0}
                  data-testid="button-trigger-calculation"
                >
                  <Calculator className="h-4 w-4 mr-2" />
                  {isFormOpen ? "Hide Form" : "Run Calculation"}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent className="mt-4">
                <div className="border rounded-lg p-6 space-y-4 bg-gray-50 dark:bg-gray-900">
                  <div className="space-y-2">
                    <h3 className="text-lg font-semibold">Calculate License Fee</h3>
                    <p className="text-sm text-muted-foreground">
                      Configure the calculation parameters for this contract.
                    </p>
                  </div>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="calc-name">Calculation Name</Label>
                      <Input
                        id="calc-name"
                        placeholder="Q1 2024 Royalties"
                        value={calculationName}
                        onChange={(e) => setCalculationName(e.target.value)}
                        data-testid="input-calculation-name"
                      />
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="period-start">Period Start (Optional)</Label>
                        <Input
                          id="period-start"
                          type="date"
                          value={periodStart}
                          onChange={(e) => setPeriodStart(e.target.value)}
                          data-testid="input-period-start"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="period-end">Period End (Optional)</Label>
                        <Input
                          id="period-end"
                          type="date"
                          value={periodEnd}
                          onChange={(e) => setPeriodEnd(e.target.value)}
                          data-testid="input-period-end"
                        />
                      </div>
                    </div>
                    <div className="bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-lg p-3">
                      <p className="text-sm text-blue-800 dark:text-blue-200">
                        <strong>Total Sales Data:</strong> {salesData.length} transactions
                      </p>
                    </div>
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button
                      variant="outline"
                      onClick={() => setIsFormOpen(false)}
                      data-testid="button-cancel-calculation"
                    >
                      Cancel
                    </Button>
                    <Button
                      onClick={() => calculateMutation.mutate()}
                      disabled={isCalculating}
                      className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600"
                      data-testid="button-confirm-calculation"
                    >
                      {isCalculating ? "Calculating..." : "Calculate"}
                    </Button>
                  </div>
                </div>
              </CollapsibleContent>
            </Collapsible>
          </CardContent>
        </Card>

        <Card className="border-red-200 dark:border-red-900">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-red-700 dark:text-red-400">
              <Trash2 className="h-5 w-5" />
              Data Management
            </CardTitle>
            <CardDescription>
              Delete data to start fresh testing with contract PDF rules
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <h4 className="text-sm font-semibold">Sales Data</h4>
                <p className="text-xs text-muted-foreground mb-2">
                  Delete all {salesData.length} sales transactions for this contract
                </p>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button 
                      variant="destructive" 
                      className="w-full"
                      disabled={salesData.length === 0 || deleteSalesMutation.isPending}
                      data-testid="button-delete-sales"
                    >
                      <Trash2 className="h-4 w-4 mr-2" />
                      {deleteSalesMutation.isPending ? "Deleting..." : "Delete All Sales Data"}
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
                      <AlertDialogDescription>
                        This will permanently delete all {salesData.length} sales transactions for this contract. 
                        This action cannot be undone.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel data-testid="button-cancel-delete-sales">Cancel</AlertDialogCancel>
                      <AlertDialogAction
                        onClick={() => deleteSalesMutation.mutate()}
                        className="bg-red-600 hover:bg-red-700"
                        data-testid="button-confirm-delete-sales"
                      >
                        Delete All Sales
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              </div>

              <div className="space-y-2">
                <h4 className="text-sm font-semibold">Calculation History</h4>
                <p className="text-xs text-muted-foreground mb-2">
                  Delete all {calculations.length} license fee calculation runs
                </p>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button 
                      variant="destructive" 
                      className="w-full"
                      disabled={calculations.length === 0 || deleteCalculationsMutation.isPending}
                      data-testid="button-delete-calculations"
                    >
                      <Trash2 className="h-4 w-4 mr-2" />
                      {deleteCalculationsMutation.isPending ? "Deleting..." : "Delete All Calculations"}
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
                      <AlertDialogDescription>
                        This will permanently delete all {calculations.length} license fee calculation runs for this contract. 
                        This action cannot be undone.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel data-testid="button-cancel-delete-calculations">Cancel</AlertDialogCancel>
                      <AlertDialogAction
                        onClick={() => deleteCalculationsMutation.mutate()}
                        className="bg-red-600 hover:bg-red-700"
                        data-testid="button-confirm-delete-calculations"
                      >
                        Delete All Calculations
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              </div>
            </div>
            
            <div className="bg-amber-50 dark:bg-amber-950 border border-amber-200 dark:border-amber-800 rounded-lg p-3 mt-4">
              <p className="text-sm text-amber-800 dark:text-amber-200">
                💡 <strong>Tip:</strong> Use this to start fresh testing. After deleting data, upload new sales files and ensure calculations are based on your contract PDF's extracted rules.
              </p>
            </div>
          </CardContent>
        </Card>

        {latestCalculation && chartData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5" />
                Sales & License Fee Breakdown
              </CardTitle>
              <CardDescription>
                Top {chartData.length} products by sales amount
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="sales" fill="#8b5cf6" name="Sales Amount" />
                  <Bar dataKey="royalty" fill="#ec4899" name="License Fee Amount" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {latestCalculation && pieData.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <PieChartIcon className="h-5 w-5" />
                Revenue Distribution
              </CardTitle>
              <CardDescription>
                Sales amount vs License Fee amount
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, value }) => `${name}: $${value.toLocaleString()}`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {pieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              Calculation History
            </CardTitle>
            <CardDescription>
              Previous license fee calculations for this contract
            </CardDescription>
          </CardHeader>
          <CardContent>
            {calculations.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Calculator className="h-12 w-12 mx-auto mb-3 opacity-50" />
                <p>No calculations yet. Run your first calculation above.</p>
              </div>
            ) : (
              <div className="space-y-4">
                {calculations.map((calc: any) => (
                  <div
                    key={calc.id}
                    className="flex items-center justify-between p-4 border rounded-lg hover:bg-accent transition-colors"
                    data-testid={`calculation-${calc.id}`}
                  >
                    <div className="flex-1">
                      <h4 className="font-semibold">{calc.name}</h4>
                      <div className="flex items-center gap-4 mt-1 text-sm text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {calc.createdAt ? format(new Date(calc.createdAt), "PPP") : "N/A"}
                        </span>
                        <span>{calc.salesCount} transactions</span>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <p className="text-sm text-muted-foreground">Sales</p>
                        <p className="font-semibold">${parseFloat(calc.totalSalesAmount || "0").toLocaleString()}</p>
                      </div>
                      <div className="text-right">
                        <p className="text-sm text-muted-foreground">License Fee</p>
                        <p className="font-semibold text-purple-600">${parseFloat(calc.totalRoyalty || "0").toLocaleString()}</p>
                      </div>
                      <Badge variant={calc.status === "approved" ? "default" : calc.status === "rejected" ? "destructive" : "secondary"}>
                        {calc.status === "approved" && <CheckCircle className="h-3 w-3 mr-1" />}
                        {calc.status === "rejected" && <XCircle className="h-3 w-3 mr-1" />}
                        {calc.status === "pending" && <Clock className="h-3 w-3 mr-1" />}
                        {calc.status || "Pending"}
                      </Badge>
                      <div className="flex items-center gap-1 border-l pl-2 ml-2">
                        <Button
                          variant="ghost"
                          size="icon"
                          title="Download Detailed Invoice"
                          className="text-purple-600 hover:text-purple-700 hover:bg-purple-50 dark:hover:bg-purple-950"
                          onClick={() => {
                            const link = document.createElement('a');
                            link.href = `/api/royalty-calculations/${calc.id}/invoice/detailed`;
                            link.download = `invoice-detailed-${calc.name}.pdf`;
                            link.click();
                          }}
                          data-testid={`button-download-detailed-${calc.id}`}
                        >
                          <FileDown className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          title="Download Summary Invoice"
                          className="text-blue-600 hover:text-blue-700 hover:bg-blue-50 dark:hover:bg-blue-950"
                          onClick={() => {
                            const link = document.createElement('a');
                            link.href = `/api/royalty-calculations/${calc.id}/invoice/summary`;
                            link.download = `invoice-summary-${calc.name}.pdf`;
                            link.click();
                          }}
                          data-testid={`button-download-summary-${calc.id}`}
                        >
                          <Download className="h-4 w-4" />
                        </Button>
                      </div>
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="text-red-600 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950"
                            disabled={deleteSingleCalculationMutation.isPending}
                            data-testid={`button-delete-calculation-${calc.id}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </AlertDialogTrigger>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>Delete this calculation?</AlertDialogTitle>
                            <AlertDialogDescription>
                              This will permanently delete "{calc.name}". This action cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel data-testid={`button-cancel-delete-calculation-${calc.id}`}>
                              Cancel
                            </AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => deleteSingleCalculationMutation.mutate(calc.id)}
                              className="bg-red-600 hover:bg-red-700"
                              data-testid={`button-confirm-delete-calculation-${calc.id}`}
                            >
                              Delete
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {salesData.length === 0 && (
          <Card className="border-blue-200 bg-blue-50 dark:bg-blue-950 dark:border-blue-800">
            <CardContent className="pt-6">
              <div className="flex items-start gap-3">
                <Receipt className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5" />
                <div>
                  <h4 className="font-semibold text-blue-900 dark:text-blue-100 mb-1">
                    No Sales Data Yet
                  </h4>
                  <p className="text-sm text-blue-800 dark:text-blue-200">
                    To calculate royalties, you need to upload sales data first. Go to the{" "}
                    <Button
                      variant="link"
                      className="h-auto p-0 text-blue-600 dark:text-blue-400"
                      onClick={() => setLocation("/sales-upload")}
                    >
                      Sales Data
                    </Button>{" "}
                    page to upload your sales transactions.
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </MainLayout>
  );
}
