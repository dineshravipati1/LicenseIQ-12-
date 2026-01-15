import { useRef, useEffect } from "react";
import { useParams, useLocation } from "wouter";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
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
import { useToast } from "@/hooks/use-toast";
import { isUnauthorizedError } from "@/lib/authUtils";
import { apiRequest } from "@/lib/queryClient";
import { RoyaltyRulesEditor } from "@/components/RoyaltyRulesEditor";
import PendingTermMappings from "@/components/pending-term-mappings";
import CombinedRulesView from "@/components/CombinedRulesView";
import { formatDateUSA } from "@/lib/dateFormat";
import { 
  FileText, 
  Download, 
  Edit, 
  Eye, 
  Share, 
  Flag,
  CheckCircle,
  AlertTriangle,
  Lightbulb,
  Clock,
  User,
  Calendar,
  Trash2,
  Calculator,
  Sparkles,
  Network,
  ListChecks
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";

export default function ContractAnalysis() {
  const { id } = useParams();
  const [, setLocation] = useLocation();
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Move mutation hooks to top level to avoid conditional hook calls
  const reprocessMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", `/api/contracts/${id}/reprocess`);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Reprocessing Started",
        description: "The document is being reanalyzed with improved AI detection.",
      });
      // Invalidate and refetch contract data
      queryClient.invalidateQueries({ queryKey: ["/api/contracts", id] });
    },
    onError: (error: Error) => {
      toast({
        title: "Reprocessing Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("DELETE", `/api/contracts/${id}`);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Contract Deleted",
        description: "The contract has been permanently deleted.",
      });
      
      // Invalidate all related caches
      queryClient.invalidateQueries({ queryKey: ["/api/contracts"] }); // Contracts list
      queryClient.invalidateQueries({ queryKey: ["/api/calculations/all"] }); // Global calculations page
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}`] }); // This contract's detail
      
      // Redirect to contracts list
      setLocation("/contracts");
    },
    onError: (error: Error) => {
      toast({
        title: "Delete Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const flagMutation = useMutation({
    mutationFn: async (flagged: boolean) => {
      const response = await apiRequest("PATCH", `/api/contracts/${id}/flag`, { flagged });
      return response.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}`] });
      toast({
        title: data.flagged ? "Flagged for Review" : "Flag Removed",
        description: data.flagged 
          ? "Contract has been flagged for review by administrators."
          : "Review flag has been removed from this contract.",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Action Failed",
        description: error.message || "Failed to update flag status.",
        variant: "destructive",
      });
    },
  });

  const updateErpMatchingMutation = useMutation({
    mutationFn: async (enabled: boolean) => {
      const response = await apiRequest("PATCH", `/api/contracts/${id}/erp-matching`, { enabled });
      return response.json();
    },
    onSuccess: (data) => {
      // Invalidate both detail and list queries so sales upload page updates
      queryClient.invalidateQueries({ queryKey: ["/api/contracts", id] });
      queryClient.invalidateQueries({ queryKey: ["/api/contracts"] });
      
      toast({
        title: data.enabled ? "ERP Matching Enabled" : "ERP Matching Disabled",
        description: data.enabled 
          ? "Sales data will now use semantic matching with imported ERP records."
          : "Sales data will use the traditional direct upload approach.",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Update Failed",
        description: error.message || "Failed to update ERP matching setting.",
        variant: "destructive",
      });
    },
  });

  const triggerExtractionMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", `/api/contracts/${id}/extract-dynamic`);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Dynamic Extraction Started",
        description: "AI is analyzing the contract structure. Results will appear shortly.",
      });
      // Invalidate both extraction runs and dynamic rules to show fresh data
      queryClient.invalidateQueries({ queryKey: ["/api/contracts", id, "extraction-runs"] });
      queryClient.invalidateQueries({ queryKey: ["/api/contracts", id, "dynamic-rules"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Extraction Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const { data: extractionRuns = [] } = useQuery({
    queryKey: ["/api/contracts", id, "extraction-runs"],
    enabled: !!id,
  }) as { data: any[] };

  const { data: dynamicRules = [] } = useQuery({
    queryKey: ["/api/contracts", id, "dynamic-rules"],
    enabled: !!id,
  }) as { data: any[] };

  const { data: contract, isLoading, error } = useQuery({
    queryKey: ["/api/contracts", id],
    enabled: !!id,
    retry: false,
    refetchInterval: 2000, // Refresh every 2s
    refetchIntervalInBackground: false,
  }) as { data: any; isLoading: boolean; error: any };

  // Fetch royalty rules for this contract (NEW)
  const { data: royaltyRules, isLoading: rulesLoading } = useQuery({
    queryKey: ["/api/contracts", id, "rules"],
    enabled: !!id && !!contract,
    retry: false,
    refetchInterval: contract?.status === 'processing' ? 3000 : false, // Auto-refresh during processing
    refetchIntervalInBackground: false,
  }) as { data: any; isLoading: boolean };

  // Auto-invalidate rules cache when contract status changes to analyzed
  const prevStatusRef = useRef(contract?.status);
  useEffect(() => {
    if (prevStatusRef.current === 'processing' && contract?.status === 'analyzed') {
      // Contract just finished processing, refresh rules immediately
      queryClient.invalidateQueries({ queryKey: ["/api/contracts", id, "rules"] });
    }
    prevStatusRef.current = contract?.status;
  }, [contract?.status, id, queryClient]);

  // Handle unauthorized errors
  if (error && isUnauthorizedError(error as Error)) {
    toast({
      title: "Unauthorized",
      description: "You are logged out. Logging in again...",
      variant: "destructive",
    });
    setTimeout(() => {
      window.location.href = "/api/login";
    }, 500);
    return null;
  }

  if (isLoading) {
    return (
      <MainLayout title="Loading..." description="Loading contract analysis">
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <div className="space-y-2">
              <Skeleton className="h-8 w-96" />
              <Skeleton className="h-4 w-64" />
            </div>
            <div className="flex space-x-3">
              <Skeleton className="h-10 w-24" />
              <Skeleton className="h-10 w-32" />
            </div>
          </div>
          
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-2 space-y-6">
              <Skeleton className="h-64 w-full" />
              <Skeleton className="h-96 w-full" />
            </div>
            <div className="space-y-6">
              <Skeleton className="h-64 w-full" />
              <Skeleton className="h-48 w-full" />
            </div>
          </div>
        </div>
      </MainLayout>
    );
  }

  if (!contract || !contract.id) {
    return (
      <MainLayout title="Contract Not Found" description="The requested contract could not be found">
        <Card>
          <CardContent className="text-center py-12">
            <FileText className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-medium mb-2">Contract Not Found</h3>
            <p className="text-muted-foreground mb-6">
              The contract you're looking for doesn't exist or you don't have permission to view it.
            </p>
            <Button onClick={() => setLocation("/contracts")}>
              Back to Contracts
            </Button>
          </CardContent>
        </Card>
      </MainLayout>
    );
  }

  const analysis = contract?.analysis;
  const hasAnalysis = analysis && contract?.status === 'analyzed';

  // Extract key contract details from analysis data
  const extractContractDetails = () => {
    if (!analysis) return {};

    // Extract parties from summary with improved pattern matching
    const summary = analysis.summary || '';
    
    // ✅ NEW: Get parties directly from keyTerms if available (more reliable than parsing)
    const keyTermsObj = analysis.keyTerms as any;
    let party1: string | null = keyTermsObj?.licensor || null;
    let party2: string | null = keyTermsObj?.licensee || null;
    
    // Fallback: Try parsing from summary if not found in keyTerms or marked as "Not specified"
    if (!party1 || party1 === 'Not specified' || !party2 || party2 === 'Not specified') {
      // Pattern 1: Licensing-style (Licensor/Licensee)
      const licensorMatch = summary.match(/([^(,]+)\s*\(Licensor\)/i);
      const licenseeMatch = summary.match(/([^(,]+)\s*\(Licensee\)/i);
      
      if (licensorMatch && licenseeMatch) {
        party1 = licensorMatch[1]?.trim();
        party2 = licenseeMatch[1]?.trim();
      }
      
      // Pattern 2: Generic "between X and Y" pattern (works for all contract types)
      if (!party1 || !party2) {
        const betweenPattern = summary.match(/between\s+([^,]+(?:\s+(?:Inc|LLC|Ltd|Corp|Corporation|Company))?)\s+and\s+([^,]+(?:\s+(?:Inc|LLC|Ltd|Corp|Corporation|Company))?)/i);
        if (betweenPattern) {
          party1 = betweenPattern[1]?.trim();
          party2 = betweenPattern[2]?.trim();
        }
      }
      
      // Pattern 3: Subcontractor/Service Agreement style
      if (!party1 || !party2) {
        const subcontractorPattern = summary.match(/(?:company|client|contractor)?\s*([^,]+(?:\s+(?:Inc|LLC|Ltd|Corp))?)\s+(?:hires|engages|contracts with)\s+([^,]+(?:\s+(?:Inc|LLC|Ltd|Corp))?)/i);
        if (subcontractorPattern) {
          party1 = subcontractorPattern[1]?.trim();
          party2 = subcontractorPattern[2]?.trim();
        }
      }
    }
    
    // Extract information from keyTerms with safe type checking
    // keyTerms can be either an array (old format) or an object with terms array (new format)
    const keyTermsArray = Array.isArray(analysis.keyTerms) ? analysis.keyTerms : (analysis.keyTerms as any)?.terms || [];
    const paymentTerms = keyTermsArray.find((term: any) => term?.type && term.type.toLowerCase().includes('payment'))?.description;
    const financialObligation = keyTermsArray.find((term: any) => term?.type && term.type.toLowerCase().includes('financial'))?.description;
    const territory = keyTermsArray.find((term: any) => term?.type && term.type.toLowerCase().includes('territory'))?.description;
    
    // Extract dates and amounts using regex patterns
    const datePattern = /(\w+\s+\d{1,2},\s+\d{4})/g;
    const amountPattern = /\$[\d,]+(?:\.\d{2})?/g;
    
    const dates = summary.match(datePattern) || [];
    const amounts = (paymentTerms || financialObligation || '').match(amountPattern) || [];

    return {
      licensor: party1 || 'Not specified',
      licensee: party2 || 'Not specified',
      paymentTerms: paymentTerms || null,
      contractValue: amounts?.[0] || null,
      territory: territory || null,
      startDate: dates?.[0] || null,
      endDate: dates?.[1] || (analysis as any)?.expirationDate || null,
    };
  };

  const contractDetails = extractContractDetails();

  const handleViewOriginal = () => {
    // Open the original PDF file in a new window
    window.open(`/api/contracts/${id}/file`, '_blank');
  };

  const handleDownloadReport = async () => {
    try {
      const response = await apiRequest("GET", `/api/contracts/${id}/report`);
      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${contract?.originalName || 'contract'}_analysis_report.txt`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        toast({
          title: "Report Downloaded",
          description: "Analysis report has been downloaded successfully.",
        });
      } else {
        throw new Error('Failed to download report');
      }
    } catch (error) {
      toast({
        title: "Download Failed",
        description: "Failed to download analysis report. Please try again.",
        variant: "destructive",
      });
    }
  };

  const handleShareAnalysis = async () => {
    try {
      const shareData = {
        title: `${contract?.originalName || 'Contract'} Analysis`,
        text: analysis?.summary || 'Contract Analysis Report',
        url: window.location.href,
      };

      if (navigator.share) {
        await navigator.share(shareData);
        toast({
          title: "Shared Successfully",
          description: "Analysis has been shared.",
        });
      } else {
        // Fallback: copy link to clipboard
        await navigator.clipboard.writeText(window.location.href);
        toast({
          title: "Link Copied",
          description: "Analysis link has been copied to your clipboard.",
        });
      }
    } catch (error) {
      console.error('Share failed:', error);
      toast({
        title: "Share Failed",
        description: "Unable to share analysis. Please try again.",
        variant: "destructive",
      });
    }
  };

  const handleFlagForReview = () => {
    const isCurrentlyFlagged = contract?.flaggedForReview || false;
    flagMutation.mutate(!isCurrentlyFlagged);
  };

  const handleExport = () => {
    handleDownloadReport();
  };

  const handleEditAnalysis = () => {
    // Navigate to the edit analysis page or show edit modal
    toast({
      title: "Edit Analysis",
      description: "Analysis editing functionality is being developed. For now, you can reprocess the contract to regenerate analysis.",
    });
  };

  const handleReprocess = () => {
    reprocessMutation.mutate();
  };

  const handleDelete = () => {
    deleteMutation.mutate();
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.9) return "text-green-600";
    if (confidence >= 0.7) return "text-yellow-600";
    return "text-red-600";
  };

  const getRiskLevelColor = (level: string) => {
    switch (level) {
      case 'high':
        return 'bg-red-50 dark:bg-red-950/20 border-red-200 dark:border-red-800 text-red-900 dark:text-red-100';
      case 'medium':
        return 'bg-amber-50 dark:bg-amber-950/20 border-amber-200 dark:border-amber-800 text-amber-900 dark:text-amber-100';
      case 'low':
        return 'bg-green-50 dark:bg-green-950/20 border-green-200 dark:border-green-800 text-green-900 dark:text-green-100';
      default:
        return 'bg-muted border-border text-foreground';
    }
  };

  return (
    <MainLayout 
      title={`${contract?.originalName || 'Contract'} Analysis`}
      description={`${hasAnalysis ? 'Processed' : 'Processing'} ${contract?.createdAt ? formatDistanceToNow(new Date(contract.createdAt), { addSuffix: true }) : 'recently'}`}
    >
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Badge 
              variant={contract?.status === 'analyzed' ? 'default' : 
                      contract?.status === 'processing' ? 'secondary' : 'outline'}
            >
              {contract?.status}
            </Badge>
            {hasAnalysis && analysis.confidence && (
              <Badge variant="outline" className={getConfidenceColor(parseFloat(analysis.confidence))}>
                {Math.round(parseFloat(analysis.confidence) * 100)}% confidence
              </Badge>
            )}
          </div>
          <div className="flex space-x-3">
            <Button 
              variant="outline" 
              onClick={handleReprocess} 
              disabled={reprocessMutation.isPending}
              data-testid="button-reprocess"
            >
              <AlertTriangle className="h-4 w-4 mr-2 text-amber-400" />
              {reprocessMutation.isPending ? "Reprocessing..." : "Reprocess"}
            </Button>
            {/* AI Extract button - HIDDEN */}
            {false && (
              <Button 
                variant="outline"
                onClick={() => triggerExtractionMutation.mutate()}
                disabled={triggerExtractionMutation.isPending}
                data-testid="button-trigger-extraction"
                className="border-purple-200 text-purple-600 hover:bg-purple-50 dark:border-purple-800 dark:text-purple-400 dark:hover:bg-purple-950"
              >
                <Sparkles className="h-4 w-4 mr-2" />
                {triggerExtractionMutation.isPending ? "Extracting..." : "AI Extract"}
              </Button>
            )}
            <Button variant="outline" onClick={handleExport} data-testid="button-export">
              <Download className="h-4 w-4 mr-2 text-blue-400" />
              Export
            </Button>
            <Button 
              onClick={() => setLocation(`/royalty-dashboard/${id}`)} 
              data-testid="button-calculate-royalties"
              className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 text-white"
            >
              <Calculator className="h-4 w-4 mr-2" />
              License Fee Dashboard
            </Button>
            <Button 
              onClick={() => setLocation(`/contracts/${id}/rules`)} 
              data-testid="button-manage-rules"
              variant="outline"
              className="border-blue-200 text-blue-600 hover:bg-blue-50 dark:border-blue-800 dark:text-blue-400 dark:hover:bg-blue-950"
            >
              <ListChecks className="h-4 w-4 mr-2" />
              Manage License Fee Rules
            </Button>
            <Button onClick={handleEditAnalysis} data-testid="button-edit-analysis">
              <Edit className="h-4 w-4 mr-2 text-green-400" />
              Edit Analysis
            </Button>
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button 
                  variant="outline" 
                  className="border-red-200 text-red-600 hover:bg-red-50"
                  data-testid="button-delete-contract"
                >
                  <Trash2 className="h-4 w-4 mr-2 text-red-500" />
                  Delete
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle className="text-red-600">⚠️ Delete Contract & All Related Data</AlertDialogTitle>
                  <AlertDialogDescription className="space-y-3">
                    <p className="font-semibold text-foreground">
                      Are you sure you want to delete "{contract?.originalName}"?
                    </p>
                    <p className="text-red-600 font-medium">
                      This action cannot be undone and will permanently delete:
                    </p>
                    <ul className="list-disc list-inside space-y-1 text-sm text-foreground bg-red-50 dark:bg-red-950 p-3 rounded-md">
                      <li>📄 Contract file and AI analysis</li>
                      <li>📊 All license fee rules and formulas</li>
                      <li>💰 All sales data</li>
                      <li>🧮 All license fee calculations and history</li>
                      <li>🤖 Contract embeddings and AI data</li>
                      <li>📋 All audit trail records</li>
                    </ul>
                    <p className="text-sm text-muted-foreground italic">
                      You can always re-upload the contract PDF to extract rules again.
                    </p>
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    className="bg-red-600 hover:bg-red-700"
                    onClick={handleDelete}
                    disabled={deleteMutation.isPending}
                  >
                    {deleteMutation.isPending ? "Deleting..." : "Yes, Delete Everything"}
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </div>

        {!hasAnalysis ? (
          /* Processing State */
          <Card>
            <CardContent className="text-center py-12">
              <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-primary mx-auto mb-4" />
              <h3 className="text-lg font-medium mb-2">
                {contract?.status === 'processing' ? 'Analysis in Progress' : 'Waiting for Analysis'}
              </h3>
              <p className="text-muted-foreground mb-6">
                {contract?.status === 'processing' 
                  ? 'Our AI is analyzing your contract. This usually takes a few minutes.'
                  : contract?.status === 'failed'
                  ? 'Analysis failed. Please try uploading the contract again.'
                  : 'Analysis will begin shortly.'
                }
              </p>
              {contract?.status !== 'failed' && (
                <Button variant="outline" onClick={() => setLocation("/contracts")}>
                  Back to Contracts
                </Button>
              )}
            </CardContent>
          </Card>
        ) : (
          /* Analysis Results */
          <div className="space-y-6">
            {/* Key Contract Details - Top Section */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              {/* Parties Card */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <User className="h-4 w-4" />
                    Contract Parties
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Licensor</p>
                    <p className="text-sm font-medium" data-testid="text-licensor">
                      {contractDetails.licensor || "Not identified"}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Licensee</p>
                    <p className="text-sm font-medium" data-testid="text-licensee">
                      {contractDetails.licensee || "Not identified"}
                    </p>
                  </div>
                </CardContent>
              </Card>

              {/* Key Dates Card */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <Calendar className="h-4 w-4" />
                    Key Dates
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Start Date</p>
                    <p className="text-sm font-medium" data-testid="text-start-date">
                      {contractDetails.startDate || "Not specified"}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">End Date</p>
                    <p className="text-sm font-medium" data-testid="text-end-date">
                      {contractDetails.endDate || "Not specified"}
                    </p>
                  </div>
                </CardContent>
              </Card>

              {/* Financial Terms Card */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <FileText className="h-4 w-4" />
                    Financial Terms
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Payment Terms</p>
                    <p className="text-sm font-medium" data-testid="text-payment-terms">
                      {contractDetails.paymentTerms || "Not specified"}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Amount/Rate</p>
                    <p className="text-sm font-medium" data-testid="text-amount">
                      {contractDetails.contractValue || "Variable license fee rates"}
                    </p>
                  </div>
                </CardContent>
              </Card>

              {/* Agreement Type Card */}
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <CheckCircle className="h-4 w-4" />
                    Agreement Details
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Agreement Type</p>
                    <p className="text-sm font-medium" data-testid="text-agreement-type">
                      {contract?.contractType || "License Agreement"}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">Territory</p>
                    <p className="text-sm font-medium" data-testid="text-jurisdiction">
                      {contractDetails.territory || "Not specified"}
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* ERP Integration Toggle */}
            <Card className="border-purple-200 dark:border-purple-800 bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-950/20 dark:to-pink-950/20">
              <CardHeader>
                <CardTitle className="text-base font-medium flex items-center gap-2">
                  <Network className="h-5 w-5 text-purple-600 dark:text-purple-400" />
                  ERP Integration Mode
                </CardTitle>
              </CardHeader>
              <CardContent className="flex items-center justify-between">
                <div className="space-y-1 flex-1">
                  <Label htmlFor="erp-matching-toggle" className="text-sm font-medium cursor-pointer">
                    {contract?.useErpMatching ? "✅ ERP Semantic Matching Enabled" : "Traditional Sales Upload"}
                  </Label>
                  <p className="text-xs text-muted-foreground">
                    {contract?.useErpMatching 
                      ? "Sales will be matched against imported ERP records using AI-powered semantic search"
                      : "Sales data will be uploaded directly using CSV/Excel files (classic mode)"
                    }
                  </p>
                </div>
                <Switch
                  id="erp-matching-toggle"
                  checked={contract?.useErpMatching || false}
                  onCheckedChange={(checked) => updateErpMatchingMutation.mutate(checked)}
                  disabled={updateErpMatchingMutation.isPending}
                  data-testid="switch-erp-matching"
                  className="data-[state=checked]:bg-purple-600"
                />
              </CardContent>
            </Card>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Main Analysis Panel */}
            <div className="lg:col-span-2 space-y-6">
              {/* AI Summary */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Lightbulb className="h-5 w-5" />
                    AI Summary
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="prose prose-sm max-w-none text-foreground">
                    <p>{analysis.summary}</p>
                  </div>
                </CardContent>
              </Card>

              {/* Extracted Terms */}
              {analysis.keyTerms && analysis.keyTerms.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <FileText className="h-5 w-5" />
                      Extracted Terms
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {analysis.keyTerms.map((term: any, index: number) => (
                        <div key={index} className="border border-border rounded-lg p-4">
                          <div className="flex items-center justify-between mb-2">
                            <span className="font-medium text-foreground">{term.type}</span>
                            <Badge variant="outline" className={getConfidenceColor(term.confidence)}>
                              {Math.round(term.confidence * 100)}% confidence
                            </Badge>
                          </div>
                          <p className="text-sm text-muted-foreground mb-2">{term.description}</p>
                          <div className="text-xs text-muted-foreground">
                            Found in: {term.location}
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Risk Analysis */}
              {analysis.riskAnalysis && analysis.riskAnalysis.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertTriangle className="h-5 w-5" />
                      Risk Analysis
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {analysis.riskAnalysis.map((risk: any, index: number) => (
                        <div key={index} className={`p-4 rounded-lg border ${getRiskLevelColor(risk.level)}`}>
                          <div className="flex items-start space-x-3">
                            <div className="h-6 w-6 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5">
                              <AlertTriangle className="h-4 w-4" />
                            </div>
                            <div>
                              <span className="font-medium capitalize">{risk.level} Risk - {risk.title}</span>
                              <p className="text-sm mt-1">{risk.description}</p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Insights */}
              {analysis.insights && analysis.insights.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <CheckCircle className="h-5 w-5" />
                      AI Insights
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {analysis.insights.map((insight: any, index: number) => (
                        <div key={index} className="p-4 bg-blue-50 dark:bg-blue-950/20 rounded-lg border border-blue-200 dark:border-blue-800">
                          <div className="flex items-start space-x-3">
                            <div className="h-8 w-8 bg-blue-500 rounded-full flex items-center justify-center flex-shrink-0">
                              <Lightbulb className="h-4 w-4 text-white" />
                            </div>
                            <div>
                              <h4 className="font-medium text-blue-900 dark:text-blue-100">{insight.title}</h4>
                              <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">{insight.description}</p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* ERP Field Mappings - Pending Term Mappings */}
              <PendingTermMappings contractId={id || ''} showConfirmed={true} />

              {/* Dynamic Extraction Results - HIDDEN */}
              {false && extractionRuns && extractionRuns.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Network className="h-5 w-5 text-purple-500" />
                      AI Dynamic Extraction
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {extractionRuns.slice(0, 3).map((run: any) => (
                      <div key={run.id} className="p-4 border rounded-lg">
                        <div className="flex items-center justify-between mb-2">
                          <Badge variant={run.status === 'completed' ? 'default' : run.status === 'processing' ? 'secondary' : 'destructive'}>
                            {run.status}
                          </Badge>
                          <span className="text-sm text-muted-foreground">
                            {run.createdAt ? formatDistanceToNow(new Date(run.createdAt), { addSuffix: true }) : 'recently'}
                          </span>
                        </div>
                        <div className="grid grid-cols-3 gap-4 text-sm">
                          <div>
                            <p className="text-muted-foreground">Confidence</p>
                            <p className="font-medium">{run.overallConfidence ? Math.round(Number(run.overallConfidence) * 100) : '0'}%</p>
                          </div>
                          <div>
                            <p className="text-muted-foreground">Entities</p>
                            <p className="font-medium">{run.nodesExtracted || 0}</p>
                          </div>
                          <div>
                            <p className="text-muted-foreground">Rules</p>
                            <p className="font-medium">{run.rulesExtracted || 0}</p>
                          </div>
                        </div>
                        {run.validationResults && run.validationResults.issues && run.validationResults.issues.length > 0 && (
                          <div className="mt-2 p-2 bg-amber-50 dark:bg-amber-950/20 rounded text-sm">
                            <p className="text-amber-800 dark:text-amber-200">
                              {run.validationResults.issues.length} validation {run.validationResults.issues.length === 1 ? 'issue' : 'issues'} found
                            </p>
                          </div>
                        )}
                      </div>
                    ))}
                    {dynamicRules && dynamicRules.length > 0 && (
                      <div className="mt-4 p-4 bg-purple-50 dark:bg-purple-950/20 border border-purple-200 dark:border-purple-800 rounded-lg">
                        <h4 className="font-semibold text-purple-900 dark:text-purple-100 mb-2">
                          Dynamically Extracted Rules ({dynamicRules.length})
                        </h4>
                        <div className="space-y-2">
                          {dynamicRules.slice(0, 3).map((rule: any) => (
                            <div key={rule.id} className="flex items-center justify-between text-sm">
                              <span className="text-purple-800 dark:text-purple-200">{rule.ruleName}</span>
                              <Badge variant={rule.isActive ? 'default' : 'secondary'}>
                                {rule.isActive ? 'Active' : 'Pending Review'}
                              </Badge>
                            </div>
                          ))}
                          {dynamicRules.length > 3 && (
                            <p className="text-sm text-muted-foreground italic">
                              + {dynamicRules.length - 3} more rules
                            </p>
                          )}
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Enhanced Royalty Rules Editor - HIDDEN */}
              {false && (
                <RoyaltyRulesEditor 
                  contractId={id || ''}
                  ruleSets={royaltyRules?.ruleSets || []}
                  onRulesUpdate={() => {
                    // Refetch rules when they're updated
                    queryClient.invalidateQueries({ queryKey: ['/api/contracts', id, 'rules'] });
                  }}
                  onReprocess={handleReprocess}
                />
              )}

              {/* Combined Rules View - Shows both manual and ERP-generated rules */}
              <CombinedRulesView contractId={id || ''} />
            </div>

            {/* Sidebar Info */}
            <div className="space-y-6">
              {/* Contract Info */}
              <Card>
                <CardHeader>
                  <CardTitle>Contract Information</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">File Name:</span>
                      <span className="text-foreground font-medium">{contract?.originalName}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">File Size:</span>
                      <span className="text-foreground">{((contract?.fileSize || 0) / 1024 / 1024).toFixed(1)} MB</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Type:</span>
                      <span className="text-foreground capitalize">{contract?.contractType || 'Unknown'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Priority:</span>
                      <Badge variant="outline" className="capitalize">{contract?.priority}</Badge>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Upload Date:</span>
                      <span className="text-foreground">{formatDateUSA(contract?.createdAt)}</span>
                    </div>
                    {contract?.uploadedByUser && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Uploaded By:</span>
                        <span className="text-foreground">
                          {contract?.uploadedByUser?.firstName && contract?.uploadedByUser?.lastName
                            ? `${contract.uploadedByUser.firstName} ${contract.uploadedByUser.lastName}`
                            : contract?.uploadedByUser?.email
                          }
                        </span>
                      </div>
                    )}
                    {analysis?.processingTime && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Processing Time:</span>
                        <span className="text-foreground">{analysis.processingTime}s</span>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>

              {/* Quick Actions */}
              <Card>
                <CardHeader>
                  <CardTitle>Quick Actions</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    <Button
                      variant="outline"
                      className="w-full justify-start"
                      onClick={() => setLocation(`/contracts/${id}/manage`)}
                      data-testid="button-manage-metadata"
                    >
                      <Edit className="h-4 w-4 mr-2" />
                      Manage Metadata
                    </Button>
                    <Button
                      variant="outline"
                      className="w-full justify-start"
                      onClick={handleViewOriginal}
                      data-testid="button-view-original"
                    >
                      <Eye className="h-4 w-4 mr-2" />
                      View Original Document
                    </Button>
                    <Button
                      variant="outline"
                      className="w-full justify-start"
                      onClick={handleDownloadReport}
                      data-testid="button-download-analysis"
                    >
                      <Download className="h-4 w-4 mr-2" />
                      Download Analysis Report
                    </Button>
                    <Button
                      variant="outline"
                      className="w-full justify-start"
                      onClick={handleShareAnalysis}
                      data-testid="button-share-analysis"
                    >
                      <Share className="h-4 w-4 mr-2" />
                      Share Analysis
                    </Button>
                    {/* Calculate Royalties button - HIDDEN */}
                    {false && (
                      <Button
                        variant="outline"
                        className="w-full justify-start bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 text-white"
                        onClick={() => setLocation(`/royalty-calculations/${id}`)}
                        data-testid="button-calculate-royalties-sidebar"
                      >
                        <Calculator className="h-4 w-4 mr-2" />
                        Calculate Royalties
                      </Button>
                    )}
                    <Button
                      variant={contract?.flaggedForReview ? "default" : "outline"}
                      className="w-full justify-start"
                      onClick={handleFlagForReview}
                      disabled={flagMutation.isPending}
                      data-testid="button-flag-review"
                    >
                      <Flag className={`h-4 w-4 mr-2 ${contract?.flaggedForReview ? 'text-white' : ''}`} />
                      {contract?.flaggedForReview ? 'Remove Flag' : 'Flag for Review'}
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Notes */}
              {contract?.notes && (
                <Card>
                  <CardHeader>
                    <CardTitle>Notes</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-sm text-foreground whitespace-pre-wrap" data-testid="text-notes">
                      {contract.notes}
                    </p>
                  </CardContent>
                </Card>
              )}
            </div>
          </div>
          </div>
        )}
      </div>
    </MainLayout>
  );
}
