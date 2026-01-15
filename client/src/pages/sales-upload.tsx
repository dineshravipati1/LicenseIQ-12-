import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/hooks/use-toast";
import { Upload, FileSpreadsheet, AlertCircle, CheckCircle, Download, Calculator, Network, FileUp } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { apiRequest, queryClient } from "@/lib/queryClient";

export default function SalesUpload() {
  const { toast } = useToast();
  const [, setLocation] = useLocation();
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [selectedContractId, setSelectedContractId] = useState<string>("");
  const [uploadResult, setUploadResult] = useState<any>(null);

  // Fetch contracts
  const { data: contractsData } = useQuery<{ contracts: any[] }>({
    queryKey: ['/api/contracts'],
  });

  const contracts = contractsData?.contracts || [];

  // Get selected contract details
  const selectedContract = contracts.find((c: any) => c.id === selectedContractId);
  const isErpMatchingEnabled = selectedContract?.useErpMatching || false;

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedFile) {
        throw new Error("No file selected");
      }
      if (!selectedContractId) {
        throw new Error("Please select a contract");
      }

      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("contractId", selectedContractId);

      const response = await apiRequest("POST", "/api/sales/upload", formData);
      return response.json();
    },
    onSuccess: (data) => {
      setUploadResult(data);
      toast({
        title: "Upload Successful",
        description: `Imported ${data.validRows || 0} sales transactions successfully!`,
      });
      setSelectedFile(null);
      
      // Invalidate all relevant queries for the dashboard to refresh immediately
      if (selectedContractId) {
        queryClient.invalidateQueries({ queryKey: [`/api/contracts/${selectedContractId}/sales`] });
        queryClient.invalidateQueries({ queryKey: [`/api/contracts/${selectedContractId}/formula-preview`] });
        queryClient.invalidateQueries({ queryKey: [`/api/contracts/${selectedContractId}/royalty-calculations`] });
      }
    },
    onError: (error: Error) => {
      toast({
        title: "Upload Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      // Validate file type
      const validTypes = ['.csv', '.xlsx', '.xls'];
      const fileExt = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));
      
      if (!validTypes.includes(fileExt)) {
        toast({
          title: "Invalid File Type",
          description: "Please upload a CSV or Excel file (.csv, .xlsx, .xls)",
          variant: "destructive",
        });
        return;
      }
      
      setSelectedFile(file);
      setUploadResult(null);
    }
  };

  const handleUpload = () => {
    if (!selectedFile) {
      toast({
        title: "No File Selected",
        description: "Please select a CSV or Excel file to upload.",
        variant: "destructive",
      });
      return;
    }

    uploadMutation.mutate();
  };

  const handleDownloadSample = async () => {
    try {
      const response = await fetch('/api/sales/sample-data', {
        credentials: 'include',
      });
      
      if (!response.ok) {
        throw new Error('Failed to download sample data');
      }
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'sample_sales_data.csv';
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      toast({
        title: "Sample Data Downloaded",
        description: "You can now upload this file to test the system.",
      });
    } catch (error) {
      toast({
        title: "Download Failed",
        description: "Failed to download sample data",
        variant: "destructive",
      });
    }
  };

  return (
    <MainLayout
      title="Import Sales Data"
      description="Upload sales transactions for AI-driven royalty calculations"
    >
      <div className="max-w-4xl mx-auto space-y-6">
        {/* Upload Instructions */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileSpreadsheet className="h-5 w-5" />
              Upload Sales Data
              {selectedContract && (
                <Badge 
                  variant={isErpMatchingEnabled ? "default" : "outline"}
                  className={isErpMatchingEnabled ? "bg-purple-600" : ""}
                >
                  {isErpMatchingEnabled ? (
                    <>
                      <Network className="h-3 w-3 mr-1" />
                      ERP Matching
                    </>
                  ) : (
                    <>
                      <FileUp className="h-3 w-3 mr-1" />
                      Traditional
                    </>
                  )}
                </Badge>
              )}
            </CardTitle>
            <CardDescription>
              {isErpMatchingEnabled 
                ? "Upload sales data to match against imported ERP records using AI-powered semantic search. Sales will be automatically linked to contract terms."
                : "Upload a CSV or Excel file containing sales transactions. The system will automatically match each sale to contracts using AI."
              }
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* ERP Matching Info Banner */}
            {isErpMatchingEnabled && (
              <div className="bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-950/50 dark:to-pink-950/50 border-2 border-purple-300 dark:border-purple-700 rounded-lg p-4">
                <div className="flex items-start gap-3">
                  <Network className="h-5 w-5 text-purple-600 dark:text-purple-400 mt-0.5 flex-shrink-0" />
                  <div className="flex-1">
                    <h4 className="font-semibold text-purple-900 dark:text-purple-100 mb-1">
                      🔮 ERP Semantic Matching Active
                    </h4>
                    <p className="text-sm text-purple-800 dark:text-purple-200 mb-2">
                      This contract is configured for advanced ERP integration. Sales data will be matched against imported ERP records using AI-powered semantic search for enhanced accuracy.
                    </p>
                    <p className="text-xs text-purple-700 dark:text-purple-300 italic">
                      💡 Tip: Import your ERP master data first for best results, or toggle off ERP matching in the contract settings to use traditional upload mode.
                    </p>
                  </div>
                </div>
              </div>
            )}
            
            {/* Required Columns Info */}
            <div className="bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
              <h4 className="font-semibold text-blue-900 dark:text-blue-100 mb-2">Required Columns:</h4>
              <ul className="text-sm text-blue-800 dark:text-blue-200 space-y-1">
                <li>• <strong>transactionDate</strong> - Date of sale (YYYY-MM-DD)</li>
                <li>• <strong>grossAmount</strong> - Sale amount (number)</li>
              </ul>
            </div>

            <div className="bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-4">
              <h4 className="font-semibold text-gray-900 dark:text-gray-100 mb-2">Optional Columns:</h4>
              <ul className="text-sm text-gray-700 dark:text-gray-300 space-y-1">
                <li>• <strong>transactionId</strong> - Unique transaction ID</li>
                <li>• <strong>productName</strong> - Product or service name</li>
                <li>• <strong>productCode</strong> - SKU or product code</li>
                <li>• <strong>category</strong> - Product category</li>
                <li>• <strong>territory</strong> - Sales region/territory</li>
                <li>• <strong>currency</strong> - Currency code (default: USD)</li>
                <li>• <strong>netAmount</strong> - Net sale amount after deductions</li>
                <li>• <strong>quantity</strong> - Units sold</li>
                <li>• <strong>unitPrice</strong> - Price per unit</li>
              </ul>
            </div>

            {/* Sample Data Download */}
            <div className="bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-950 dark:to-pink-950 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <h4 className="font-semibold text-purple-900 dark:text-purple-100 mb-1">
                    Need Sample Data?
                  </h4>
                  <p className="text-sm text-purple-800 dark:text-purple-200">
                    Download our sample CSV file with 15 plant variety sales transactions. Perfect for testing!
                  </p>
                </div>
                <Button
                  onClick={handleDownloadSample}
                  variant="outline"
                  className="ml-4 border-purple-300 dark:border-purple-700 hover:bg-purple-100 dark:hover:bg-purple-900"
                  data-testid="button-download-sample"
                >
                  <Download className="h-4 w-4 mr-2" />
                  Download Sample
                </Button>
              </div>
            </div>

            {/* Contract Selection */}
            <div className="space-y-2">
              <Label htmlFor="contract-select">Select Contract</Label>
              <Select value={selectedContractId} onValueChange={setSelectedContractId}>
                <SelectTrigger id="contract-select" data-testid="select-contract">
                  <SelectValue placeholder="Choose a contract..." />
                </SelectTrigger>
                <SelectContent>
                  {contracts.length === 0 ? (
                    <SelectItem value="no-contracts" disabled>
                      No contracts found - upload a contract first
                    </SelectItem>
                  ) : (
                    contracts.map((contract: any) => (
                      <SelectItem key={contract.id} value={contract.id}>
                        {contract.displayName || contract.originalName || contract.name}
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
              {selectedContract && (
                <p className="text-xs text-muted-foreground">
                  Selected Contract ID: <code className="bg-muted px-1 rounded">{selectedContract.id}</code>
                </p>
              )}
            </div>

            {/* File Upload */}
            <div className="space-y-2">
              <Label htmlFor="sales-file">Select File</Label>
              <Input
                id="sales-file"
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={handleFileChange}
                disabled={uploadMutation.isPending}
                data-testid="input-sales-file"
              />
              {selectedFile && (
                <p className="text-sm text-muted-foreground">
                  Selected: {selectedFile.name} ({(selectedFile.size / 1024).toFixed(2)} KB)
                </p>
              )}
            </div>

            {/* Upload Button */}
            <Button
              onClick={handleUpload}
              disabled={!selectedFile || uploadMutation.isPending}
              className="w-full"
              data-testid="button-upload-sales"
            >
              {uploadMutation.isPending ? (
                <>Processing...</>
              ) : (
                <>
                  <Upload className="h-4 w-4 mr-2" />
                  Upload & Process
                </>
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Upload Results */}
        {uploadResult && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                {uploadResult.success ? (
                  <CheckCircle className="h-5 w-5 text-green-600" />
                ) : (
                  <AlertCircle className="h-5 w-5 text-orange-600" />
                )}
                Import Results
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-blue-50 dark:bg-blue-950 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Total Rows</p>
                  <p className="text-2xl font-bold">{uploadResult.totalRows || 0}</p>
                </div>
                <div className="bg-green-50 dark:bg-green-950 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Valid</p>
                  <p className="text-2xl font-bold text-green-600">{uploadResult.validRows || 0}</p>
                </div>
                <div className="bg-orange-50 dark:bg-orange-950 p-4 rounded-lg">
                  <p className="text-sm text-muted-foreground">Errors</p>
                  <p className="text-2xl font-bold text-orange-600">{uploadResult.errors || 0}</p>
                </div>
              </div>

              {/* Semantic Matching Statistics */}
              {uploadResult.erpMatchingEnabled && (
                <div className="bg-purple-50 dark:bg-purple-950 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
                  <h4 className="font-semibold text-purple-900 dark:text-purple-100 mb-3 flex items-center gap-2">
                    <Network className="h-4 w-4" />
                    ERP Semantic Matching Results
                  </h4>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center">
                      <p className="text-sm text-purple-800 dark:text-purple-200 mb-1">Matched</p>
                      <p className="text-2xl font-bold text-green-600">{uploadResult.matchedRecords || 0}</p>
                      <Badge className="mt-1 bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-100">
                        ≥70% confidence
                      </Badge>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-purple-800 dark:text-purple-200 mb-1">Unmatched</p>
                      <p className="text-2xl font-bold text-orange-600">{uploadResult.unmatchedRecords || 0}</p>
                      <Badge className="mt-1 bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-100">
                        &lt;70% confidence
                      </Badge>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-purple-800 dark:text-purple-200 mb-1">Avg Confidence</p>
                      <p className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                        {uploadResult.avgConfidence ? `${(parseFloat(uploadResult.avgConfidence) * 100).toFixed(0)}%` : '0%'}
                      </p>
                      <Badge className="mt-1 bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-100">
                        AI-powered
                      </Badge>
                    </div>
                  </div>
                  <p className="text-xs text-purple-700 dark:text-purple-300 mt-3 italic">
                    💡 Sales records were matched against imported ERP master data using semantic similarity search
                  </p>
                </div>
              )}

              {uploadResult.errors > 0 && (
                <div className="bg-orange-50 dark:bg-orange-950 border border-orange-200 dark:border-orange-800 rounded-lg p-4">
                  <h4 className="font-semibold text-orange-900 dark:text-orange-100 mb-2">⚠️ Validation Errors</h4>
                  <p className="text-sm text-orange-800 dark:text-orange-200">
                    {uploadResult.errors} row(s) failed validation and were skipped during import. 
                    Common issues include missing required fields (transactionDate, grossAmount) or invalid data types.
                  </p>
                </div>
              )}

              <div className="bg-gradient-to-r from-green-50 to-emerald-50 dark:from-green-950 dark:to-emerald-950 border border-green-200 dark:border-green-800 rounded-lg p-4">
                <p className="text-sm text-green-800 dark:text-green-200 font-semibold mb-3">✅ Sales data uploaded successfully!</p>
                <p className="text-sm text-green-800 dark:text-green-200 mb-4">
                  What would you like to do next?
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <Button
                    onClick={() => setLocation(`/royalty-dashboard/${selectedContractId}`)}
                    className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 text-white"
                    data-testid="button-view-dashboard"
                  >
                    <Calculator className="h-4 w-4 mr-2" />
                    View License Fee Dashboard
                  </Button>
                  <Button
                    onClick={() => setLocation(`/contracts/${selectedContractId}/rules`)}
                    variant="outline"
                    className="border-purple-300 dark:border-purple-700 hover:bg-purple-50 dark:hover:bg-purple-950"
                    data-testid="button-manage-rules"
                  >
                    <FileSpreadsheet className="h-4 w-4 mr-2" />
                    Manage License Fee Rules
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </MainLayout>
  );
}
