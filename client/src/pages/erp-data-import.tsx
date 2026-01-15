import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { useToast } from "@/hooks/use-toast";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { Upload, Database, CheckCircle2, XCircle, Clock, AlertCircle, RefreshCw, ArrowLeft } from "lucide-react";
import { useLocation } from "wouter";
import { format } from "date-fns";

interface Contract {
  id: string;
  displayName: string | null;
  contractNumber: string | null;
  originalName: string;
}

interface MasterDataMapping {
  id: string;
  mappingName: string;
  erpSystem: string;
  entityType: string;
}

interface DataImportJob {
  id: string;
  jobName: string;
  status: string;
  recordsTotal: number;
  recordsProcessed: number;
  recordsFailed: number;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  uploadMeta: any;
}

export default function ErpDataImport() {
  const [, navigate] = useLocation();
  const { toast } = useToast();
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [selectedContract, setSelectedContract] = useState<string>("");
  const [selectedMapping, setSelectedMapping] = useState<string>("");
  const [isUploading, setIsUploading] = useState(false);

  // Fetch contracts with ERP matching enabled
  const { data: contracts = [] } = useQuery<Contract[]>({
    queryKey: ["/api/contracts"],
    select: (allContracts: any[]) => 
      allContracts.filter((c: any) => c.useErpMatching === true),
  });

  // Fetch saved mappings
  const { data: mappings = [] } = useQuery<MasterDataMapping[]>({
    queryKey: ["/api/master-data-mappings"],
  });

  // Fetch import jobs with real-time polling
  const { data: jobs = [], refetch: refetchJobs } = useQuery<DataImportJob[]>({
    queryKey: ["/api/erp-import-jobs"],
    // Poll every 2 seconds if there are any processing jobs
    refetchInterval: (query) => {
      const hasProcessingJobs = query.state.data?.some((job: DataImportJob) => 
        job.status === 'processing' || job.status === 'pending'
      );
      return hasProcessingJobs ? 2000 : false;
    },
  });

  // File upload mutation
  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedFile || !selectedContract || !selectedMapping) {
        throw new Error("Please select a file, contract, and mapping");
      }

      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("contractId", selectedContract);
      formData.append("mappingId", selectedMapping);

      const response = await fetch("/api/erp-import", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Upload failed");
      }

      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Import Started",
        description: "Your ERP data is being processed. This may take a few minutes.",
      });
      setSelectedFile(null);
      setSelectedContract("");
      setSelectedMapping("");
      queryClient.invalidateQueries({ queryKey: ["/api/erp-import-jobs"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Import Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const handleImport = () => {
    uploadMutation.mutate();
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "completed":
        return <Badge className="bg-green-500"><CheckCircle2 className="w-3 h-3 mr-1" />Completed</Badge>;
      case "failed":
        return <Badge variant="destructive"><XCircle className="w-3 h-3 mr-1" />Failed</Badge>;
      case "processing":
        return <Badge className="bg-blue-500"><RefreshCw className="w-3 h-3 mr-1 animate-spin" />Processing</Badge>;
      case "pending":
        return <Badge variant="secondary"><Clock className="w-3 h-3 mr-1" />Pending</Badge>;
      default:
        return <Badge variant="outline">{status}</Badge>;
    }
  };

  const selectedContractObj = contracts.find((c) => c.id === selectedContract);
  const selectedMappingObj = mappings.find((m) => m.id === selectedMapping);

  return (
    <MainLayout title="ERP Data Import" description="Import master data from your ERP system for semantic sales matching">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">ERP Data Import</h1>
            <p className="text-muted-foreground mt-2">
              Import master data from your ERP system for semantic sales matching
            </p>
          </div>
          <Button
            variant="outline"
            onClick={() => navigate("/")}
            data-testid="button-back"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back
          </Button>
        </div>

        {/* Upload Form */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Upload className="w-5 h-5" />
              Import ERP Master Data
            </CardTitle>
            <CardDescription>
              Upload CSV or Excel files containing customer, product, or other master data from your ERP system
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Contract Selection */}
            <div className="space-y-2">
              <Label htmlFor="contract-select">Select Contract</Label>
              <Select value={selectedContract} onValueChange={setSelectedContract}>
                <SelectTrigger id="contract-select" data-testid="select-contract">
                  <SelectValue placeholder="Choose a contract with ERP matching enabled" />
                </SelectTrigger>
                <SelectContent>
                  {contracts.length === 0 ? (
                    <div className="p-4 text-sm text-muted-foreground text-center">
                      No contracts with ERP matching enabled.
                      <br />
                      Enable it on the contract analysis page.
                    </div>
                  ) : (
                    contracts.map((contract) => (
                      <SelectItem key={contract.id} value={contract.id}>
                        {contract.displayName || contract.contractNumber || contract.originalName}
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
            </div>

            {/* Mapping Selection */}
            <div className="space-y-2">
              <Label htmlFor="mapping-select">Select Field Mapping</Label>
              <Select value={selectedMapping} onValueChange={setSelectedMapping}>
                <SelectTrigger id="mapping-select" data-testid="select-mapping">
                  <SelectValue placeholder="Choose a saved mapping configuration" />
                </SelectTrigger>
                <SelectContent>
                  {mappings.length === 0 ? (
                    <div className="p-4 text-sm text-muted-foreground text-center">
                      No mappings configured.
                      <br />
                      Create one in Master Data Mapping page.
                    </div>
                  ) : (
                    mappings.map((mapping) => (
                      <SelectItem key={mapping.id} value={mapping.id}>
                        {mapping.mappingName} ({mapping.erpSystem} - {mapping.entityType})
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
            </div>

            {/* File Upload */}
            <div className="space-y-2">
              <Label htmlFor="file-upload">Upload File</Label>
              <div className="flex items-center gap-4">
                <input
                  id="file-upload"
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={handleFileChange}
                  className="block w-full text-sm text-muted-foreground file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-semibold file:bg-primary file:text-primary-foreground hover:file:bg-primary/90"
                  data-testid="input-file"
                />
              </div>
              {selectedFile && (
                <p className="text-sm text-muted-foreground">
                  Selected: {selectedFile.name} ({(selectedFile.size / 1024).toFixed(2)} KB)
                </p>
              )}
            </div>

            {/* Import Summary */}
            {selectedFile && selectedContractObj && selectedMappingObj && (
              <div className="p-4 bg-muted rounded-lg space-y-2">
                <div className="flex items-center gap-2 text-sm">
                  <Database className="w-4 h-4" />
                  <span className="font-semibold">Import Summary</span>
                </div>
                <div className="text-sm space-y-1">
                  <p><span className="font-medium">Contract:</span> {selectedContractObj.displayName || selectedContractObj.contractNumber || selectedContractObj.originalName}</p>
                  <p><span className="font-medium">Mapping:</span> {selectedMappingObj.mappingName}</p>
                  <p><span className="font-medium">ERP System:</span> {selectedMappingObj.erpSystem} / {selectedMappingObj.entityType}</p>
                  <p><span className="font-medium">File:</span> {selectedFile.name}</p>
                </div>
              </div>
            )}

            {/* Import Button */}
            <Button
              onClick={handleImport}
              disabled={!selectedFile || !selectedContract || !selectedMapping || uploadMutation.isPending}
              className="w-full"
              data-testid="button-import"
            >
              {uploadMutation.isPending ? (
                <>
                  <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                  Processing...
                </>
              ) : (
                <>
                  <Upload className="w-4 h-4 mr-2" />
                  Start Import
                </>
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Import History */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="w-5 h-5" />
              Import History
            </CardTitle>
            <CardDescription>
              Track the status of your ERP data import jobs
            </CardDescription>
          </CardHeader>
          <CardContent>
            {jobs.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Database className="w-12 h-12 mx-auto mb-4 opacity-50" />
                <p>No import jobs yet. Upload your first ERP data file above.</p>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Job Name</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Progress</TableHead>
                    <TableHead>Records</TableHead>
                    <TableHead>Started</TableHead>
                    <TableHead>Completed</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => {
                    const progress = job.recordsTotal > 0
                      ? (job.recordsProcessed / job.recordsTotal) * 100
                      : 0;

                    return (
                      <TableRow key={job.id} data-testid={`row-job-${job.id}`}>
                        <TableCell className="font-medium">{job.jobName}</TableCell>
                        <TableCell>{getStatusBadge(job.status)}</TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <Progress value={progress} className="w-32" />
                            <p className="text-xs text-muted-foreground">
                              {job.recordsProcessed} / {job.recordsTotal}
                            </p>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="text-sm">
                            <p className="text-green-600 dark:text-green-400">✓ {job.recordsProcessed}</p>
                            {job.recordsFailed > 0 && (
                              <p className="text-red-600 dark:text-red-400">✗ {job.recordsFailed}</p>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          {job.startedAt ? format(new Date(job.startedAt), "MMM d, h:mm a") : "-"}
                        </TableCell>
                        <TableCell>
                          {job.completedAt ? format(new Date(job.completedAt), "MMM d, h:mm a") : "-"}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </MainLayout>
  );
}
