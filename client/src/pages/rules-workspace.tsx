import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useForm } from "react-hook-form";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/useAuth";
import { Link } from "wouter";
import { 
  Calculator, Database, Layers, Search, Edit, Trash2, 
  Plus, ExternalLink, ArrowRight, FileText, Settings,
  CheckCircle, AlertCircle, Loader2, Filter, Save
} from "lucide-react";

interface Contract {
  id: string;
  contractName: string;
  licensorName?: string;
}

interface RoyaltyRule {
  id: string;
  contractId: string;
  ruleName: string;
  ruleType: string;
  description?: string;
  baseRate?: string;
  isActive: boolean;
  priority: number;
  contractName?: string;
}

interface TermMapping {
  id: string;
  contractId: string;
  contractTerm: string;
  erpFieldName: string;
  confidence: number;
  status: string;
  contractName?: string;
}

interface ErpRuleSet {
  id: string;
  name: string;
  description?: string;
  status: string;
  version: number;
}

export default function RulesWorkspacePage() {
  const { toast } = useToast();
  const { user } = useAuth();
  const companyId = (user as any)?.activeContext?.companyId || '';
  
  const [activeTab, setActiveTab] = useState("calculation-rules");
  const [searchQuery, setSearchQuery] = useState("");
  const [contractFilter, setContractFilter] = useState("all");
  const [editingMapping, setEditingMapping] = useState<TermMapping | null>(null);
  const [editFormData, setEditFormData] = useState({ contractTerm: "", erpFieldName: "" });

  const { data: contractsData } = useQuery<{ contracts: Contract[], total: number }>({
    queryKey: ['/api/contracts'],
  });
  const contracts = contractsData?.contracts || [];

  const { data: rulesData, isLoading: rulesLoading } = useQuery<{ rules: RoyaltyRule[], total: number }>({
    queryKey: ['/api/royalty-rules', contractFilter],
    queryFn: async () => {
      if (contractFilter === 'all') {
        return { rules: [], total: 0 };
      }
      const res = await fetch(`/api/royalty-rules?contractId=${contractFilter}`, { credentials: 'include' });
      if (!res.ok) return { rules: [], total: 0 };
      return res.json();
    },
    enabled: contractFilter !== 'all',
  });

  const { data: mappingsData, isLoading: mappingsLoading } = useQuery<{ mappings: TermMapping[] }>({
    queryKey: ['/api/confirmed-term-mappings', contractFilter],
    queryFn: async () => {
      if (contractFilter === 'all') {
        return { mappings: [] };
      }
      const res = await fetch(`/api/confirmed-term-mappings?contractId=${contractFilter}`, { credentials: 'include' });
      if (!res.ok) return { mappings: [] };
      return res.json();
    },
    enabled: contractFilter !== 'all',
  });

  const { data: erpRuleSetsData, isLoading: erpLoading } = useQuery<{ ruleSets: ErpRuleSet[] }>({
    queryKey: ['/api/erp-mapping-rule-sets', companyId],
    queryFn: async () => {
      const res = await fetch(`/api/erp-mapping-rule-sets?companyId=${companyId}`);
      return res.json();
    },
    enabled: !!companyId,
  });

  const deleteRuleMutation = useMutation({
    mutationFn: async ({ contractId, ruleId }: { contractId: string; ruleId: string }) => {
      await apiRequest('DELETE', `/api/contracts/${contractId}/rules/${ruleId}`);
    },
    onSuccess: () => {
      toast({ title: 'Rule deleted', description: 'The rule has been removed successfully.' });
      queryClient.invalidateQueries({ queryKey: ['/api/royalty-rules'] });
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to delete rule.', variant: 'destructive' });
    },
  });

  const deleteMappingMutation = useMutation({
    mutationFn: async (mappingId: string) => {
      await apiRequest('DELETE', `/api/confirmed-term-mappings/${mappingId}`);
    },
    onSuccess: () => {
      toast({ title: 'Mapping deleted', description: 'The mapping has been removed.' });
      queryClient.invalidateQueries({ queryKey: ['/api/confirmed-term-mappings'] });
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to delete mapping.', variant: 'destructive' });
    },
  });

  const updateMappingMutation = useMutation({
    mutationFn: async ({ id, contractTerm, erpFieldName }: { id: string; contractTerm: string; erpFieldName: string }) => {
      await apiRequest('PATCH', `/api/confirmed-term-mappings/${id}`, { contractTerm, erpFieldName });
    },
    onSuccess: () => {
      toast({ title: 'Mapping updated', description: 'The mapping has been updated successfully.' });
      queryClient.invalidateQueries({ queryKey: ['/api/confirmed-term-mappings'] });
      setEditingMapping(null);
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to update mapping.', variant: 'destructive' });
    },
  });

  const handleEditMapping = (mapping: TermMapping) => {
    setEditFormData({ contractTerm: mapping.contractTerm, erpFieldName: mapping.erpFieldName });
    setEditingMapping(mapping);
  };

  const handleSaveMapping = () => {
    if (!editingMapping) return;
    updateMappingMutation.mutate({
      id: editingMapping.id,
      contractTerm: editFormData.contractTerm,
      erpFieldName: editFormData.erpFieldName
    });
  };

  const filteredRules = rulesData?.rules?.filter(rule => 
    !searchQuery || 
    rule.ruleName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    rule.ruleType?.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  const filteredMappings = mappingsData?.mappings?.filter(mapping =>
    !searchQuery ||
    mapping.contractTerm?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    mapping.erpFieldName?.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  const filteredErpRuleSets = erpRuleSetsData?.ruleSets?.filter(rs =>
    !searchQuery ||
    rs.name?.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  return (
    <MainLayout title="Rules Workspace">
      <div className="container mx-auto py-6 space-y-6">
        <div className="flex justify-between items-start">
          <div>
            <h1 className="text-3xl font-bold tracking-tight" data-testid="text-page-title">Rules Workspace</h1>
            <p className="text-muted-foreground mt-1">
              Centralized management for all calculation rules, field mappings, and ERP rule sets
            </p>
          </div>
        </div>

        <div className="flex gap-4 items-center">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search rules and mappings..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
              data-testid="input-search"
            />
          </div>
          <Select value={contractFilter} onValueChange={setContractFilter}>
            <SelectTrigger className="w-[250px]" data-testid="select-contract-filter">
              <Filter className="h-4 w-4 mr-2" />
              <SelectValue placeholder="Filter by contract" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Select a Contract...</SelectItem>
              {contracts?.map((contract) => (
                <SelectItem key={contract.id} value={contract.id}>
                  {(contract as any).displayName || (contract as any).counterpartyName || `Contract ${contract.id.slice(0, 8)}`}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="calculation-rules" className="flex items-center gap-2" data-testid="tab-calculation-rules">
              <Calculator className="h-4 w-4" />
              Calculation Rules
              <Badge variant="secondary" className="ml-1">{filteredRules.length}</Badge>
            </TabsTrigger>
            <TabsTrigger value="confirmed-mappings" className="flex items-center gap-2" data-testid="tab-confirmed-mappings">
              <Database className="h-4 w-4" />
              Confirmed Mappings
              <Badge variant="secondary" className="ml-1">{filteredMappings.length}</Badge>
            </TabsTrigger>
            <TabsTrigger value="erp-rule-sets" className="flex items-center gap-2" data-testid="tab-erp-rule-sets">
              <Layers className="h-4 w-4" />
              ERP Rule Sets
              <Badge variant="secondary" className="ml-1">{filteredErpRuleSets.length}</Badge>
            </TabsTrigger>
          </TabsList>

          <TabsContent value="calculation-rules">
            <Card>
              <CardHeader>
                <div className="flex justify-between items-center">
                  <div>
                    <CardTitle>Contract Calculation Rules</CardTitle>
                    <CardDescription>
                      Rules that define how to calculate license fees - rates, percentages, formulas, and tiers
                    </CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {contractFilter === 'all' ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Calculator className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>Select a contract to view its calculation rules.</p>
                    <p className="text-sm mt-2">Use the dropdown above to choose a specific contract.</p>
                  </div>
                ) : rulesLoading ? (
                  <div className="flex items-center justify-center py-8 gap-2">
                    <Loader2 className="h-5 w-5 animate-spin" />
                    <span>Loading rules...</span>
                  </div>
                ) : filteredRules.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Calculator className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No calculation rules found for this contract.</p>
                    <p className="text-sm mt-2">Rules are created when contracts are analyzed by AI.</p>
                  </div>
                ) : (
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Rule Name</TableHead>
                          <TableHead>Contract</TableHead>
                          <TableHead>Type</TableHead>
                          <TableHead>Rate</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead className="w-[100px]">Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredRules.map((rule) => (
                          <TableRow key={rule.id} data-testid={`row-rule-${rule.id}`}>
                            <TableCell>
                              <div className="font-medium">{rule.ruleName}</div>
                              {rule.description && (
                                <div className="text-sm text-muted-foreground truncate max-w-[200px]">
                                  {rule.description}
                                </div>
                              )}
                            </TableCell>
                            <TableCell>
                              <Link href={`/contracts/${rule.contractId}`}>
                                <Badge variant="outline" className="cursor-pointer hover:bg-accent">
                                  <FileText className="h-3 w-3 mr-1" />
                                  {(rule as any).contractName || `Contract`}
                                </Badge>
                              </Link>
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline">{rule.ruleType}</Badge>
                            </TableCell>
                            <TableCell>{rule.baseRate || '-'}</TableCell>
                            <TableCell>
                              {rule.isActive ? (
                                <Badge variant="default" className="bg-green-500">Active</Badge>
                              ) : (
                                <Badge variant="secondary">Inactive</Badge>
                              )}
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center gap-1">
                                <Link href={`/rules/${rule.contractId}?edit=${rule.id}`}>
                                  <Button variant="ghost" size="icon" className="h-8 w-8" data-testid={`button-edit-rule-${rule.id}`}>
                                    <Edit className="h-4 w-4" />
                                  </Button>
                                </Link>
                                <AlertDialog>
                                  <AlertDialogTrigger asChild>
                                    <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive" data-testid={`button-delete-rule-${rule.id}`}>
                                      <Trash2 className="h-4 w-4" />
                                    </Button>
                                  </AlertDialogTrigger>
                                  <AlertDialogContent>
                                    <AlertDialogHeader>
                                      <AlertDialogTitle>Delete Rule</AlertDialogTitle>
                                      <AlertDialogDescription>
                                        Are you sure you want to delete "{rule.ruleName}"? This action cannot be undone.
                                      </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                                      <AlertDialogAction 
                                        onClick={() => deleteRuleMutation.mutate({ contractId: rule.contractId, ruleId: rule.id })}
                                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                      >
                                        Delete
                                      </AlertDialogAction>
                                    </AlertDialogFooter>
                                  </AlertDialogContent>
                                </AlertDialog>
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="confirmed-mappings">
            <Card>
              <CardHeader>
                <div className="flex justify-between items-center">
                  <div>
                    <CardTitle>Confirmed Field Mappings</CardTitle>
                    <CardDescription>
                      Mappings that link contract terms to ERP fields for dual-terminology display and automated calculations
                    </CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {contractFilter === 'all' ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>Select a contract to view its confirmed mappings.</p>
                    <p className="text-sm mt-2">Use the dropdown above to choose a specific contract.</p>
                  </div>
                ) : mappingsLoading ? (
                  <div className="flex items-center justify-center py-8 gap-2">
                    <Loader2 className="h-5 w-5 animate-spin" />
                    <span>Loading mappings...</span>
                  </div>
                ) : filteredMappings.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No confirmed mappings found for this contract.</p>
                    <p className="text-sm mt-2">Confirm AI mapping suggestions from contract analysis to populate this list.</p>
                  </div>
                ) : (
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Contract Term</TableHead>
                          <TableHead className="w-8"></TableHead>
                          <TableHead>ERP Field</TableHead>
                          <TableHead>Confidence</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead className="w-[100px]">Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredMappings.map((mapping) => (
                          <TableRow key={mapping.id} data-testid={`row-mapping-${mapping.id}`}>
                            <TableCell>
                              <div className="font-medium">{mapping.contractTerm}</div>
                            </TableCell>
                            <TableCell>
                              <ArrowRight className="h-4 w-4 text-muted-foreground" />
                            </TableCell>
                            <TableCell>
                              <div className="font-medium text-primary">{mapping.erpFieldName}</div>
                            </TableCell>
                            <TableCell>
                              <Badge variant={mapping.confidence >= 0.8 ? "default" : "secondary"}>
                                {Math.round(mapping.confidence * 100)}%
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <Badge variant="default" className="bg-green-500">
                                <CheckCircle className="h-3 w-3 mr-1" />
                                Confirmed
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center gap-1">
                                <Button 
                                  variant="ghost" 
                                  size="icon" 
                                  className="h-8 w-8" 
                                  onClick={() => handleEditMapping(mapping)}
                                  data-testid={`button-edit-mapping-${mapping.id}`}
                                >
                                  <Edit className="h-4 w-4" />
                                </Button>
                                <AlertDialog>
                                  <AlertDialogTrigger asChild>
                                    <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive" data-testid={`button-delete-mapping-${mapping.id}`}>
                                      <Trash2 className="h-4 w-4" />
                                    </Button>
                                  </AlertDialogTrigger>
                                  <AlertDialogContent>
                                    <AlertDialogHeader>
                                      <AlertDialogTitle>Delete Mapping</AlertDialogTitle>
                                      <AlertDialogDescription>
                                        Are you sure you want to delete the mapping "{mapping.contractTerm} → {mapping.erpFieldName}"? This may affect calculation blueprints.
                                      </AlertDialogDescription>
                                    </AlertDialogHeader>
                                    <AlertDialogFooter>
                                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                                      <AlertDialogAction 
                                        onClick={() => deleteMappingMutation.mutate(mapping.id)}
                                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                      >
                                        Delete
                                      </AlertDialogAction>
                                    </AlertDialogFooter>
                                  </AlertDialogContent>
                                </AlertDialog>
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="erp-rule-sets">
            <Card>
              <CardHeader>
                <div className="flex justify-between items-center">
                  <div>
                    <CardTitle>ERP Mapping Rule Sets</CardTitle>
                    <CardDescription>
                      Organized collections of field mappings between your ERP system and LicenseIQ schema
                    </CardDescription>
                  </div>
                  <Link href="/erp-mapping-rules">
                    <Button data-testid="button-manage-erp-rules">
                      <Settings className="h-4 w-4 mr-2" />
                      Full Management
                      <ExternalLink className="h-4 w-4 ml-2" />
                    </Button>
                  </Link>
                </div>
              </CardHeader>
              <CardContent>
                {erpLoading ? (
                  <div className="flex items-center justify-center py-8 gap-2">
                    <Loader2 className="h-5 w-5 animate-spin" />
                    <span>Loading ERP rule sets...</span>
                  </div>
                ) : filteredErpRuleSets.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Layers className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No ERP rule sets found.</p>
                    <p className="text-sm mt-2">Create rule sets to map ERP fields to LicenseIQ schema.</p>
                    <Link href="/erp-mapping-rules">
                      <Button variant="outline" size="sm" className="mt-4" data-testid="button-create-first-ruleset">
                        <Plus className="h-4 w-4 mr-1" />
                        Create Rule Set
                      </Button>
                    </Link>
                  </div>
                ) : (
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Rule Set Name</TableHead>
                          <TableHead>Description</TableHead>
                          <TableHead>Version</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead className="w-[100px]">Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredErpRuleSets.map((ruleSet) => (
                          <TableRow key={ruleSet.id} data-testid={`row-ruleset-${ruleSet.id}`}>
                            <TableCell>
                              <div className="font-medium">{ruleSet.name}</div>
                            </TableCell>
                            <TableCell>
                              <div className="text-sm text-muted-foreground truncate max-w-[200px]">
                                {ruleSet.description || '-'}
                              </div>
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline">v{ruleSet.version}</Badge>
                            </TableCell>
                            <TableCell>
                              <Badge 
                                variant={ruleSet.status === 'active' ? 'default' : 'secondary'}
                                className={ruleSet.status === 'active' ? 'bg-green-500' : ''}
                              >
                                {ruleSet.status}
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <Link href="/erp-mapping-rules">
                                <Button variant="ghost" size="icon" className="h-8 w-8" data-testid={`button-view-ruleset-${ruleSet.id}`}>
                                  <ExternalLink className="h-4 w-4" />
                                </Button>
                              </Link>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>

      <Dialog open={!!editingMapping} onOpenChange={(open) => !open && setEditingMapping(null)}>
        <DialogContent className="sm:max-w-[425px]" data-testid="dialog-edit-mapping">
          <DialogHeader>
            <DialogTitle>Edit Mapping</DialogTitle>
            <DialogDescription>
              Modify the contract term or ERP field name for this mapping.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="contractTerm">Contract Term</Label>
              <Input
                id="contractTerm"
                value={editFormData.contractTerm}
                onChange={(e) => setEditFormData(prev => ({ ...prev, contractTerm: e.target.value }))}
                placeholder="Enter contract term"
                data-testid="input-edit-contract-term"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="erpFieldName">ERP Field Name</Label>
              <Input
                id="erpFieldName"
                value={editFormData.erpFieldName}
                onChange={(e) => setEditFormData(prev => ({ ...prev, erpFieldName: e.target.value }))}
                placeholder="Enter ERP field name"
                data-testid="input-edit-erp-field"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditingMapping(null)} data-testid="button-cancel-edit">
              Cancel
            </Button>
            <Button 
              onClick={handleSaveMapping} 
              disabled={updateMappingMutation.isPending || !editFormData.contractTerm || !editFormData.erpFieldName}
              data-testid="button-save-mapping"
            >
              {updateMappingMutation.isPending ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save className="h-4 w-4 mr-2" />
                  Save Changes
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </MainLayout>
  );
}
