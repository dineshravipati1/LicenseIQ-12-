import { useEffect, useState } from "react";
import { useLocation, useRoute } from "wouter";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/hooks/use-toast";
import { ArrowLeft, Save, Send, CheckCircle, XCircle, Clock, FileText, History, ThumbsUp, ThumbsDown } from "lucide-react";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { format } from "date-fns";

export default function ContractManagement() {
  const [, params] = useRoute("/contracts/:id/manage");
  const [, navigate] = useLocation();
  const { toast } = useToast();
  const contractId = params?.id;

  // Form state
  const [displayName, setDisplayName] = useState("");
  const [effectiveStart, setEffectiveStart] = useState("");
  const [effectiveEnd, setEffectiveEnd] = useState("");
  const [renewalTerms, setRenewalTerms] = useState("");
  const [governingLaw, setGoverningLaw] = useState("");
  const [organizationName, setOrganizationName] = useState("");
  const [counterpartyName, setCounterpartyName] = useState("");
  const [contractType, setContractType] = useState("");
  const [priority, setPriority] = useState("normal");
  const [notes, setNotes] = useState("");
  const [changeSummary, setChangeSummary] = useState("");
  
  // Approval inline state
  const [expandedVersionId, setExpandedVersionId] = useState<string | null>(null);
  const [approvalAction, setApprovalAction] = useState<'approve' | 'reject' | null>(null);
  const [approvalNotes, setApprovalNotes] = useState("");
  const [isAdminOverride, setIsAdminOverride] = useState(false);

  // Fetch current user
  const { data: user } = useQuery({
    queryKey: ["/api/user"],
  });

  // Fetch contract data
  const { data: contract, isLoading } = useQuery({
    queryKey: [`/api/contracts/${contractId}`],
    enabled: !!contractId,
  });

  // Fetch version history
  const { data: versionsData } = useQuery({
    queryKey: [`/api/contracts/${contractId}/versions`],
    enabled: !!contractId,
  });

  const versions = versionsData?.versions || [];
  
  // Check if current user can approve (admin or owner)
  const canApprove = user?.role === 'admin' || user?.role === 'owner';

  // Initialize form with contract data
  useEffect(() => {
    if (contract) {
      setDisplayName(contract.displayName || contract.originalName || "");
      setEffectiveStart(contract.effectiveStart ? format(new Date(contract.effectiveStart), "yyyy-MM-dd") : "");
      setEffectiveEnd(contract.effectiveEnd ? format(new Date(contract.effectiveEnd), "yyyy-MM-dd") : "");
      setRenewalTerms(contract.renewalTerms || "");
      setGoverningLaw(contract.governingLaw || "");
      setOrganizationName(contract.organizationName || "");
      setCounterpartyName(contract.counterpartyName || "");
      setContractType(contract.contractType || "");
      setPriority(contract.priority || "normal");
      setNotes(contract.notes || "");
    }
  }, [contract]);

  // Update metadata mutation
  const updateMutation = useMutation({
    mutationFn: async () => {
      if (!changeSummary.trim()) {
        throw new Error("Please describe what changed");
      }

      const metadata = {
        displayName,
        effectiveStart: effectiveStart || undefined,
        effectiveEnd: effectiveEnd || undefined,
        renewalTerms,
        governingLaw,
        organizationName,
        counterpartyName,
        contractType,
        priority,
        notes,
        changeSummary,
      };

      return await apiRequest("PATCH", `/api/contracts/${contractId}/metadata`, metadata);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}`] });
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}/versions`] });
      toast({
        title: "Success",
        description: "Contract metadata updated successfully",
      });
      setChangeSummary(""); // Reset change summary
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update contract metadata",
        variant: "destructive",
      });
    },
  });

  // Submit for approval mutation
  const submitApprovalMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest("POST", `/api/contracts/${contractId}/submit-approval`, {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}`] });
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}/versions`] });
      toast({
        title: "Success",
        description: "Contract submitted for approval",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to submit for approval",
        variant: "destructive",
      });
    },
  });

  // Approve version mutation
  const approveMutation = useMutation({
    mutationFn: async (versionId: string) => {
      return await apiRequest("POST", `/api/contracts/versions/${versionId}/approve`, {
        status: 'approved',
        decisionNotes: approvalNotes.trim() || undefined,
        adminOverride: isAdminOverride,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}`] });
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}/versions`] });
      toast({
        title: "Approved",
        description: "Contract version approved successfully",
      });
      setExpandedVersionId(null);
      setApprovalNotes("");
      setApprovalAction(null);
      setIsAdminOverride(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to approve version",
        variant: "destructive",
      });
    },
  });

  // Reject version mutation
  const rejectMutation = useMutation({
    mutationFn: async (versionId: string) => {
      if (!approvalNotes.trim()) throw new Error("Please provide a reason for rejection");
      return await apiRequest("POST", `/api/contracts/versions/${versionId}/approve`, {
        status: 'rejected',
        decisionNotes: approvalNotes.trim(),
        adminOverride: isAdminOverride,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}`] });
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${contractId}/versions`] });
      toast({
        title: "Rejected",
        description: "Contract version rejected",
      });
      setExpandedVersionId(null);
      setApprovalNotes("");
      setApprovalAction(null);
      setIsAdminOverride(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to reject version",
        variant: "destructive",
      });
    },
  });

  const handleApprovalAction = (action: 'approve' | 'reject', versionId: string, adminOverride = false) => {
    setApprovalAction(action);
    setExpandedVersionId(versionId);
    setApprovalNotes(""); // Reset notes when opening
    setIsAdminOverride(adminOverride); // Set admin override flag
  };

  const handleConfirmApproval = (versionId: string) => {
    if (approvalAction === 'approve') {
      approveMutation.mutate(versionId);
    } else if (approvalAction === 'reject') {
      rejectMutation.mutate(versionId);
    }
  };

  const handleCancelApproval = () => {
    setExpandedVersionId(null);
    setApprovalAction(null);
    setApprovalNotes("");
    setIsAdminOverride(false);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto" data-testid="loading-spinner" />
          <p className="mt-4 text-muted-foreground">Loading contract...</p>
        </div>
      </div>
    );
  }

  if (!contract) {
    return (
      <div className="text-center py-12">
        <FileText className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">Contract not found</h3>
        <Button onClick={() => navigate("/contracts")} data-testid="button-back-contracts">
          Back to Contracts
        </Button>
      </div>
    );
  }

  const getApprovalBadge = (state: string) => {
    switch (state) {
      case "approved":
        return <Badge className="bg-green-500" data-testid="badge-approved"><CheckCircle className="h-3 w-3 mr-1" />Approved</Badge>;
      case "rejected":
        return <Badge variant="destructive" data-testid="badge-rejected"><XCircle className="h-3 w-3 mr-1" />Rejected</Badge>;
      case "pending_approval":
        return <Badge variant="secondary" data-testid="badge-pending"><Clock className="h-3 w-3 mr-1" />Pending Approval</Badge>;
      default:
        return <Badge variant="outline" data-testid="badge-draft">Draft</Badge>;
    }
  };

  return (
    <div className="container mx-auto py-8 px-4 max-w-7xl space-y-6" data-testid="contract-management-page">
      {/* Header with Back Button */}
      <div className="flex items-center space-x-4 mb-6">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => navigate("/contracts")}
          className="gap-2"
          data-testid="button-back"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Contracts
        </Button>
      </div>

      {/* Contract Info Card */}
      <Card className="border-2 shadow-lg bg-gradient-to-r from-background to-muted/20">
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
            <div className="space-y-2">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-primary/10 rounded-lg">
                  <FileText className="h-6 w-6 text-primary" />
                </div>
                <div>
                  <h1 className="text-2xl md:text-3xl font-bold tracking-tight" data-testid="text-page-title">
                    {contract.displayName || contract.originalName || "Contract Management"}
                  </h1>
                  <p className="text-sm text-muted-foreground" data-testid="text-contract-number">
                    Contract ID: {contract.contractNumber || contract.id?.substring(0, 8)}
                  </p>
                </div>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {getApprovalBadge(contract.approvalState)}
              <Badge variant="outline" className="text-sm" data-testid="badge-version">
                Version {contract.currentVersion || 1}
              </Badge>
              {contract.contractType && (
                <Badge variant="secondary" className="text-sm">
                  {contract.contractType}
                </Badge>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      <Tabs defaultValue="metadata" className="w-full">
        <TabsList className="grid w-full grid-cols-2 h-12">
          <TabsTrigger value="metadata" className="text-base" data-testid="tab-metadata">
            <FileText className="h-4 w-4 mr-2" />
            Metadata
          </TabsTrigger>
          <TabsTrigger value="versions" className="text-base" data-testid="tab-versions">
            <History className="h-4 w-4 mr-2" />
            Version History
          </TabsTrigger>
        </TabsList>

        {/* Metadata Tab */}
        <TabsContent value="metadata" className="space-y-6 mt-6">
          <Card className="shadow-md" data-testid="card-metadata">
            <CardHeader className="bg-muted/50 border-b">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-xl">Basic Information</CardTitle>
                  <CardDescription className="mt-1">
                    Core contract details and identifying information
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent className="pt-6 space-y-6">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="displayName" className="text-sm font-semibold">
                    Contract Name <span className="text-destructive">*</span>
                  </Label>
                  <Input
                    id="displayName"
                    value={displayName}
                    onChange={(e) => setDisplayName(e.target.value)}
                    placeholder="Enter contract name"
                    className="h-11"
                    data-testid="input-display-name"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="contractType" className="text-sm font-semibold">
                    Contract Type
                  </Label>
                  <Select value={contractType} onValueChange={setContractType}>
                    <SelectTrigger className="h-11" data-testid="select-contract-type">
                      <SelectValue placeholder="Select contract type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Licensing Agreement">Licensing Agreement</SelectItem>
                      <SelectItem value="Royalty Agreement">Royalty Agreement</SelectItem>
                      <SelectItem value="Service Agreement">Service Agreement</SelectItem>
                      <SelectItem value="Sales Agreement">Sales Agreement</SelectItem>
                      <SelectItem value="SaaS Agreement">SaaS Agreement</SelectItem>
                      <SelectItem value="Partnership Agreement">Partnership Agreement</SelectItem>
                      <SelectItem value="Employment Agreement">Employment Agreement</SelectItem>
                      <SelectItem value="Consulting Agreement">Consulting Agreement</SelectItem>
                      <SelectItem value="NDA">Non-Disclosure Agreement (NDA)</SelectItem>
                      <SelectItem value="Master Service Agreement">Master Service Agreement (MSA)</SelectItem>
                      <SelectItem value="Subscription Agreement">Subscription Agreement</SelectItem>
                      <SelectItem value="Distribution Agreement">Distribution Agreement</SelectItem>
                      <SelectItem value="Reseller Agreement">Reseller Agreement</SelectItem>
                      <SelectItem value="Other">Other</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="organizationName" className="text-sm font-semibold">
                    Your Organization
                  </Label>
                  <Input
                    id="organizationName"
                    value={organizationName}
                    onChange={(e) => setOrganizationName(e.target.value)}
                    placeholder="Your company/organization name"
                    className="h-11"
                    data-testid="input-organization"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="counterpartyName" className="text-sm font-semibold">
                    Counterparty
                  </Label>
                  <Input
                    id="counterpartyName"
                    value={counterpartyName}
                    onChange={(e) => setCounterpartyName(e.target.value)}
                    placeholder="Other party (vendor, customer, partner, etc.)"
                    className="h-11"
                    data-testid="input-counterparty"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="governingLaw" className="text-sm font-semibold">
                    Governing Law
                  </Label>
                  <Input
                    id="governingLaw"
                    value={governingLaw}
                    onChange={(e) => setGoverningLaw(e.target.value)}
                    placeholder="Jurisdiction"
                    className="h-11"
                    data-testid="input-governing-law"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="shadow-md" data-testid="card-dates">
            <CardHeader className="bg-muted/50 border-b">
              <CardTitle className="text-xl">Contract Timeline</CardTitle>
              <CardDescription className="mt-1">
                Effective dates and contract duration
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="effectiveStart" className="text-sm font-semibold">
                    Effective Start Date
                  </Label>
                  <Input
                    id="effectiveStart"
                    type="date"
                    value={effectiveStart}
                    onChange={(e) => setEffectiveStart(e.target.value)}
                    className="h-11"
                    data-testid="input-effective-start"
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="effectiveEnd" className="text-sm font-semibold">
                    Effective End Date
                  </Label>
                  <Input
                    id="effectiveEnd"
                    type="date"
                    value={effectiveEnd}
                    onChange={(e) => setEffectiveEnd(e.target.value)}
                    className="h-11"
                    data-testid="input-effective-end"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="shadow-md" data-testid="card-terms">
            <CardHeader className="bg-muted/50 border-b">
              <CardTitle className="text-xl">Terms & Additional Details</CardTitle>
              <CardDescription className="mt-1">
                Renewal terms, notes, and supplementary information
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6 space-y-6">
              <div className="space-y-2">
                <Label htmlFor="renewalTerms" className="text-sm font-semibold">
                  Renewal Terms
                </Label>
                <Textarea
                  id="renewalTerms"
                  value={renewalTerms}
                  onChange={(e) => setRenewalTerms(e.target.value)}
                  placeholder="Describe renewal terms and conditions"
                  rows={4}
                  className="resize-none"
                  data-testid="textarea-renewal-terms"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="notes" className="text-sm font-semibold">
                  Notes
                </Label>
                <Textarea
                  id="notes"
                  value={notes}
                  onChange={(e) => setNotes(e.target.value)}
                  placeholder="Additional notes or comments"
                  rows={4}
                  className="resize-none"
                  data-testid="textarea-notes"
                />
              </div>
            </CardContent>
          </Card>

          <Card className="shadow-md border-2 border-primary/20" data-testid="card-submit">
            <CardHeader className="bg-primary/5 border-b border-primary/20">
              <CardTitle className="text-xl">Save Changes</CardTitle>
              <CardDescription className="mt-1">
                Document your changes before saving
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6 space-y-6">
              <div className="space-y-2">
                <Label htmlFor="changeSummary" className="text-sm font-semibold">
                  Change Summary <span className="text-destructive">*</span>
                </Label>
                <Input
                  id="changeSummary"
                  value={changeSummary}
                  onChange={(e) => setChangeSummary(e.target.value)}
                  placeholder="Briefly describe what changed"
                  className="h-11"
                  data-testid="input-change-summary"
                />
                <p className="text-sm text-muted-foreground">
                  Required: Explain what changes you made to the contract metadata
                </p>
              </div>

              <div className="flex flex-col sm:flex-row justify-end gap-3 pt-2">
                <Button
                  onClick={() => updateMutation.mutate()}
                  disabled={updateMutation.isPending || !changeSummary.trim()}
                  size="lg"
                  className="gap-2"
                  data-testid="button-save-changes"
                >
                  <Save className="h-4 w-4" />
                  {updateMutation.isPending ? "Saving..." : "Save Changes"}
                </Button>
                {contract.approvalState === "draft" && (
                  <Button
                    variant="secondary"
                    size="lg"
                    onClick={() => submitApprovalMutation.mutate()}
                    disabled={submitApprovalMutation.isPending}
                    className="gap-2"
                    data-testid="button-submit-approval"
                  >
                    <Send className="h-4 w-4" />
                    Submit for Approval
                  </Button>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Version History Tab */}
        <TabsContent value="versions" className="space-y-4 mt-6">
          <Card className="shadow-md" data-testid="card-version-history">
            <CardHeader className="bg-muted/50 border-b">
              <CardTitle className="text-xl">Version History</CardTitle>
              <CardDescription className="mt-1">
                Complete timeline of all contract versions and approval decisions
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
              {versions.length === 0 ? (
                <div className="text-center py-12">
                  <History className="h-16 w-16 mx-auto text-muted-foreground/50 mb-4" />
                  <p className="text-muted-foreground font-medium" data-testid="text-no-versions">
                    No version history available yet
                  </p>
                  <p className="text-sm text-muted-foreground mt-1">
                    Changes to contract metadata will appear here
                  </p>
                </div>
              ) : (
                <div className="space-y-6">
                  {versions.map((version: any, index: number) => (
                    <div
                      key={version.id}
                      className="relative"
                      data-testid={`version-${version.versionNumber}`}
                    >
                      {/* Timeline line - only show if not last item */}
                      {index < versions.length - 1 && (
                        <div className="absolute left-5 top-12 bottom-0 w-0.5 bg-border" />
                      )}
                      
                      <div className="flex gap-4">
                        {/* Timeline dot */}
                        <div className="flex flex-col items-center">
                          <div className={`flex items-center justify-center h-10 w-10 rounded-full border-2 ${
                            version.approvalState === 'approved' 
                              ? 'bg-green-100 border-green-500 dark:bg-green-950' 
                              : version.approvalState === 'rejected'
                              ? 'bg-red-100 border-red-500 dark:bg-red-950'
                              : version.approvalState === 'pending_approval'
                              ? 'bg-yellow-100 border-yellow-500 dark:bg-yellow-950'
                              : 'bg-gray-100 border-gray-400 dark:bg-gray-900'
                          }`}>
                            {version.approvalState === 'approved' ? (
                              <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400" />
                            ) : version.approvalState === 'rejected' ? (
                              <XCircle className="h-5 w-5 text-red-600 dark:text-red-400" />
                            ) : version.approvalState === 'pending_approval' ? (
                              <Clock className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
                            ) : (
                              <FileText className="h-5 w-5 text-gray-600 dark:text-gray-400" />
                            )}
                          </div>
                        </div>

                        {/* Version card */}
                        <div className="flex-1 pb-6">
                          <Card className={`shadow-sm ${
                            version.approvalState === 'pending_approval' 
                              ? 'border-2 border-yellow-500/50 bg-yellow-50/50 dark:bg-yellow-950/20'
                              : ''
                          }`}>
                            <CardContent className="p-5">
                              <div className="space-y-3">
                                {/* Header */}
                                <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
                                  <div className="flex items-center gap-2 flex-wrap">
                                    <Badge variant="outline" className="font-semibold" data-testid={`badge-version-${version.versionNumber}`}>
                                      Version {version.versionNumber}
                                    </Badge>
                                    {getApprovalBadge(version.approvalState)}
                                  </div>
                                  <span className="text-sm text-muted-foreground" data-testid={`text-version-date-${version.versionNumber}`}>
                                    {format(new Date(version.createdAt), "MMM d, yyyy 'at' h:mm a")}
                                  </span>
                                </div>

                                {/* Change summary */}
                                <div className="bg-muted/50 dark:bg-muted/20 rounded-md p-3">
                                  <p className="text-sm font-medium text-foreground" data-testid={`text-change-summary-${version.versionNumber}`}>
                                    {version.changeSummary || "No summary provided"}
                                  </p>
                                </div>

                                {/* Metadata */}
                                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                                  <span data-testid={`text-editor-${version.versionNumber}`}>
                                    Edited by: {version.editorUsername || 'Unknown'} (ID: {version.editorId?.substring(0, 8)})
                                  </span>
                                </div>
                                
                                {/* Approval Actions - Only show for admins when version is pending */}
                                {canApprove && version.approvalState === 'pending_approval' && user?.id !== version.editorId && (
                                  <div className="pt-3 border-t space-y-3">
                                    {expandedVersionId !== version.id ? (
                                      <div className="flex items-center gap-2">
                                        <Button
                                          size="sm"
                                          onClick={() => handleApprovalAction('approve', version.id)}
                                          className="bg-green-600 hover:bg-green-700 gap-2"
                                          data-testid={`button-approve-${version.versionNumber}`}
                                        >
                                          <ThumbsUp className="h-4 w-4" />
                                          Approve
                                        </Button>
                                        <Button
                                          size="sm"
                                          variant="destructive"
                                          onClick={() => handleApprovalAction('reject', version.id)}
                                          className="gap-2"
                                          data-testid={`button-reject-${version.versionNumber}`}
                                        >
                                          <ThumbsDown className="h-4 w-4" />
                                          Reject
                                        </Button>
                                      </div>
                                    ) : (
                                      <div className="bg-muted/50 dark:bg-muted/20 rounded-md p-4 space-y-3">
                                        <div className="flex items-center gap-2 text-sm font-semibold">
                                          {approvalAction === 'approve' ? (
                                            <>
                                              <ThumbsUp className="h-4 w-4 text-green-600" />
                                              <span>Approve Version {version.versionNumber}</span>
                                            </>
                                          ) : (
                                            <>
                                              <ThumbsDown className="h-4 w-4 text-red-600" />
                                              <span>Reject Version {version.versionNumber}</span>
                                            </>
                                          )}
                                        </div>
                                        <div className="space-y-2">
                                          <Label htmlFor={`notes-${version.id}`} className="text-sm">
                                            {approvalAction === 'approve' ? 'Notes (Optional)' : 'Rejection Reason (Required)'}
                                          </Label>
                                          <Textarea
                                            id={`notes-${version.id}`}
                                            value={approvalNotes}
                                            onChange={(e) => setApprovalNotes(e.target.value)}
                                            placeholder={approvalAction === 'approve' 
                                              ? 'Add any notes about this approval...'
                                              : 'Explain what needs to be changed...'}
                                            rows={3}
                                            className="resize-none"
                                            data-testid={`textarea-approval-notes-${version.versionNumber}`}
                                          />
                                        </div>
                                        <div className="flex items-center gap-2">
                                          <Button
                                            size="sm"
                                            onClick={() => handleConfirmApproval(version.id)}
                                            disabled={
                                              (approvalAction === 'reject' && !approvalNotes.trim()) ||
                                              approveMutation.isPending ||
                                              rejectMutation.isPending
                                            }
                                            className={approvalAction === 'approve' ? 'bg-green-600 hover:bg-green-700' : ''}
                                            variant={approvalAction === 'approve' ? 'default' : 'destructive'}
                                            data-testid={`button-confirm-approval-${version.versionNumber}`}
                                          >
                                            {approveMutation.isPending || rejectMutation.isPending ? (
                                              'Processing...'
                                            ) : (
                                              approvalAction === 'approve' ? 'Confirm Approval' : 'Confirm Rejection'
                                            )}
                                          </Button>
                                          <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={handleCancelApproval}
                                            disabled={approveMutation.isPending || rejectMutation.isPending}
                                            data-testid={`button-cancel-approval-${version.versionNumber}`}
                                          >
                                            Cancel
                                          </Button>
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                )}
                                
                                {/* Show if user can't approve their own version - with admin override */}
                                {canApprove && version.approvalState === 'pending_approval' && user?.id === version.editorId && (
                                  <div className="pt-3 border-t space-y-3">
                                    {expandedVersionId !== version.id ? (
                                      <>
                                        <div className="flex items-center gap-2 text-sm text-amber-600 dark:text-amber-400">
                                          <Clock className="h-4 w-4" />
                                          <span className="italic">
                                            You cannot approve your own changes
                                          </span>
                                        </div>
                                        {user?.role === 'admin' && (
                                          <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-md p-3">
                                            <p className="text-xs text-amber-700 dark:text-amber-400 mb-2">
                                              <strong>Admin Override:</strong> As an admin, you can force-approve this version for testing purposes. This bypasses the self-approval restriction.
                                            </p>
                                            <div className="flex items-center gap-2">
                                              <Button
                                                size="sm"
                                                onClick={() => handleApprovalAction('approve', version.id, true)}
                                                variant="outline"
                                                className="border-amber-500 text-amber-700 hover:bg-amber-100 dark:text-amber-400 dark:hover:bg-amber-950 gap-2"
                                                data-testid={`button-force-approve-${version.versionNumber}`}
                                              >
                                                <ThumbsUp className="h-4 w-4" />
                                                Force Approve (Override)
                                              </Button>
                                              <Button
                                                size="sm"
                                                variant="outline"
                                                onClick={() => handleApprovalAction('reject', version.id, true)}
                                                className="border-red-500 text-red-700 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950 gap-2"
                                                data-testid={`button-force-reject-${version.versionNumber}`}
                                              >
                                                <ThumbsDown className="h-4 w-4" />
                                                Force Reject (Override)
                                              </Button>
                                            </div>
                                          </div>
                                        )}
                                      </>
                                    ) : (
                                      <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-md p-4 space-y-3">
                                        <div className="flex items-center gap-2 text-sm font-semibold text-amber-700 dark:text-amber-400">
                                          {approvalAction === 'approve' ? (
                                            <>
                                              <ThumbsUp className="h-4 w-4" />
                                              <span>Force Approve Version {version.versionNumber} (Admin Override)</span>
                                            </>
                                          ) : (
                                            <>
                                              <ThumbsDown className="h-4 w-4" />
                                              <span>Force Reject Version {version.versionNumber} (Admin Override)</span>
                                            </>
                                          )}
                                        </div>
                                        <div className="space-y-2">
                                          <Label htmlFor={`notes-${version.id}`} className="text-sm text-amber-700 dark:text-amber-400">
                                            {approvalAction === 'approve' ? 'Notes (Optional)' : 'Rejection Reason (Required)'}
                                          </Label>
                                          <Textarea
                                            id={`notes-${version.id}`}
                                            value={approvalNotes}
                                            onChange={(e) => setApprovalNotes(e.target.value)}
                                            placeholder={approvalAction === 'approve' 
                                              ? 'Add any notes about this override approval...'
                                              : 'Explain what needs to be changed...'}
                                            rows={3}
                                            className="resize-none border-amber-300 dark:border-amber-700"
                                            data-testid={`textarea-approval-notes-${version.versionNumber}`}
                                          />
                                        </div>
                                        <div className="flex items-center gap-2">
                                          <Button
                                            size="sm"
                                            onClick={() => handleConfirmApproval(version.id)}
                                            disabled={
                                              (approvalAction === 'reject' && !approvalNotes.trim()) ||
                                              approveMutation.isPending ||
                                              rejectMutation.isPending
                                            }
                                            className={approvalAction === 'approve' ? 'bg-amber-600 hover:bg-amber-700' : 'bg-red-600 hover:bg-red-700'}
                                            data-testid={`button-confirm-approval-${version.versionNumber}`}
                                          >
                                            {approveMutation.isPending || rejectMutation.isPending ? (
                                              'Processing...'
                                            ) : (
                                              approvalAction === 'approve' ? 'Confirm Override Approval' : 'Confirm Rejection'
                                            )}
                                          </Button>
                                          <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={handleCancelApproval}
                                            disabled={approveMutation.isPending || rejectMutation.isPending}
                                            className="border-amber-500"
                                            data-testid={`button-cancel-approval-${version.versionNumber}`}
                                          >
                                            Cancel
                                          </Button>
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                )}

                                {/* Submit Draft for Approval - Only show for creator when version is draft */}
                                {version.approvalState === 'draft' && user?.id === version.editorId && (
                                  <div className="pt-3 border-t space-y-3">
                                    <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 rounded-md p-3">
                                      <div className="flex items-center gap-2 text-sm text-blue-700 dark:text-blue-400 mb-2">
                                        <Clock className="h-4 w-4" />
                                        <span className="font-medium">
                                          Draft version - Not yet submitted for approval
                                        </span>
                                      </div>
                                      <p className="text-xs text-blue-600 dark:text-blue-400 mb-3">
                                        Submit this version for approval to make it the active contract metadata.
                                      </p>
                                      <Button
                                        size="sm"
                                        onClick={() => submitApprovalMutation.mutate()}
                                        disabled={submitApprovalMutation.isPending}
                                        className="bg-blue-600 hover:bg-blue-700 gap-2"
                                        data-testid={`button-submit-draft-${version.versionNumber}`}
                                      >
                                        <Send className="h-4 w-4" />
                                        {submitApprovalMutation.isPending ? 'Submitting...' : 'Submit for Approval'}
                                      </Button>
                                    </div>
                                  </div>
                                )}
                              </div>
                            </CardContent>
                          </Card>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
