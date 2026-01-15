import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/useAuth";
import { 
  Settings, Plus, Play, Pause, Trash2, Edit, Eye, 
  ArrowRight, Calculator, Database, FileCode, Layers,
  AlertTriangle, CheckCircle, Clock, RefreshCw, Info,
  ChevronRight, Link2, Filter, Search, Download
} from "lucide-react";
import type { ErpSystem, LicenseiqEntity } from "@shared/schema";

type RuleSet = {
  id: string;
  name: string;
  description: string | null;
  companyId: string;
  businessUnitId: string | null;
  locationId: string | null;
  sourceSystemId: string;
  sourceEntityId: string | null;
  targetEntityId: string | null;
  mappingId: string | null;
  status: string;
  version: number;
  effectiveDate: string | null;
  expiryDate: string | null;
  createdBy: string | null;
  createdAt: string;
  updatedAt: string;
};

type MappingRule = {
  id: string;
  ruleSetId: string;
  name: string;
  description: string | null;
  priority: number;
  sourceField: string;
  sourceFieldId: string | null;
  targetField: string;
  targetFieldId: string | null;
  transformationType: string;
  transformationConfig: any;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
  conditions?: any[];
  outputs?: any[];
};

export default function ErpMappingRulesPage() {
  const { toast } = useToast();
  const { user } = useAuth();
  const companyId = (user as any)?.activeContext?.companyId || '';
  
  const [activeTab, setActiveTab] = useState("rule-sets");
  const [searchQuery, setSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [selectedRuleSet, setSelectedRuleSet] = useState<RuleSet | null>(null);
  const [editingRuleSet, setEditingRuleSet] = useState<RuleSet | null>(null);
  
  const [newRuleSet, setNewRuleSet] = useState({
    name: "",
    description: "",
    sourceSystemId: "",
    sourceEntityId: "",
    targetEntityId: "",
  });

  // Fetch organization calculation settings
  const { data: orgSettings } = useQuery({
    queryKey: ['/api/org-calculation-settings', companyId],
    enabled: !!companyId,
  });

  // Fetch ERP Mapping Rule Sets
  const { data: ruleSetsData, isLoading: ruleSetsLoading } = useQuery<{ ruleSets: RuleSet[] }>({
    queryKey: ['/api/erp-mapping-rule-sets', companyId],
    queryFn: async () => {
      const res = await fetch(`/api/erp-mapping-rule-sets?companyId=${companyId}`);
      return res.json();
    },
    enabled: !!companyId,
  });

  // Fetch ERP Systems for dropdown
  const { data: systemsData } = useQuery<{ systems: ErpSystem[] }>({
    queryKey: ['/api/erp-systems'],
  });

  // Fetch LicenseIQ Entities for dropdown
  const { data: entitiesData } = useQuery<{ entities: LicenseiqEntity[] }>({
    queryKey: ['/api/licenseiq-entities'],
  });

  // Fetch ERP Entities for dropdown
  const { data: erpEntitiesData } = useQuery({
    queryKey: ['/api/erp-entities'],
  });

  const ruleSets = ruleSetsData?.ruleSets || [];
  const systems = systemsData?.systems || [];
  const licenseiqEntities = entitiesData?.entities || [];
  const erpEntities = (erpEntitiesData as any)?.entities || [];
  const calculationApproach = (orgSettings as any)?.calculationApproach || 'manual';

  // Filter rule sets
  const filteredRuleSets = ruleSets.filter(rs => 
    (statusFilter === 'all' || rs.status === statusFilter) &&
    (!searchQuery || rs.name.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  // Update calculation approach
  const updateApproachMutation = useMutation({
    mutationFn: async (approach: string) => {
      return apiRequest('PUT', `/api/org-calculation-settings/${companyId}`, { calculationApproach: approach });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/org-calculation-settings', companyId] });
      toast({ title: "Settings updated", description: "Calculation approach has been updated." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to update settings.", variant: "destructive" });
    },
  });

  // Create rule set
  const createRuleSetMutation = useMutation({
    mutationFn: async (data: any) => {
      return apiRequest('POST', '/api/erp-mapping-rule-sets', data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets', companyId] });
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets/stats', companyId] });
      setCreateDialogOpen(false);
      setNewRuleSet({ name: "", description: "", sourceSystemId: "", sourceEntityId: "", targetEntityId: "" });
      toast({ title: "Success", description: "Rule set created successfully." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to create rule set.", variant: "destructive" });
    },
  });

  // Delete rule set
  const deleteRuleSetMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiRequest('DELETE', `/api/erp-mapping-rule-sets/${id}`, {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets', companyId] });
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets/stats', companyId] });
      setSelectedRuleSet(null);
      toast({ title: "Success", description: "Rule set deleted." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to delete rule set.", variant: "destructive" });
    },
  });

  // Activate/Deactivate rule set
  const toggleRuleSetMutation = useMutation({
    mutationFn: async ({ id, action }: { id: string; action: 'activate' | 'deactivate' }) => {
      return apiRequest('POST', `/api/erp-mapping-rule-sets/${id}/${action}`, {});
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets', companyId] });
      queryClient.invalidateQueries({ queryKey: ['/api/erp-mapping-rule-sets/stats', companyId] });
      toast({ 
        title: "Success", 
        description: `Rule set ${variables.action === 'activate' ? 'activated' : 'deactivated'}.` 
      });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to update rule set status.", variant: "destructive" });
    },
  });

  const handleCreateRuleSet = () => {
    if (!newRuleSet.name || !newRuleSet.sourceSystemId) {
      toast({ title: "Validation Error", description: "Name and Source System are required.", variant: "destructive" });
      return;
    }
    createRuleSetMutation.mutate({
      ...newRuleSet,
      companyId,
    });
  };

  const getSystemName = (id: string) => systems.find(s => s.id === id)?.name || 'Unknown';
  const getEntityName = (id: string) => {
    const liq = licenseiqEntities.find(e => e.id === id);
    if (liq) return (liq as any).displayName || liq.name;
    const erp = erpEntities.find((e: any) => e.id === id);
    return erp?.name || 'Unknown';
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'active':
        return <Badge className="bg-green-600"><CheckCircle className="h-3 w-3 mr-1" /> Active</Badge>;
      case 'inactive':
        return <Badge variant="secondary"><Pause className="h-3 w-3 mr-1" /> Inactive</Badge>;
      case 'draft':
        return <Badge variant="outline"><Edit className="h-3 w-3 mr-1" /> Draft</Badge>;
      default:
        return <Badge variant="outline">{status}</Badge>;
    }
  };

  return (
    <MainLayout title="ERP Mapping Rules">
      <div className="space-y-6">
        {/* Header */}
        <div className="flex justify-between items-start">
          <div>
            <h1 className="text-3xl font-bold" data-testid="page-title">ERP Mapping Rules</h1>
            <p className="text-muted-foreground mt-1">
              Configure automated license fee calculations based on ERP field mappings
            </p>
          </div>
          <Button 
            onClick={() => setCreateDialogOpen(true)} 
            className="gap-2"
            data-testid="button-create-ruleset"
          >
            <Plus className="h-4 w-4" /> Create Rule Set
          </Button>
        </div>

        {/* Configuration Settings Card */}
        <Card className="border-2 border-amber-200 bg-amber-50/50 dark:bg-amber-900/10 dark:border-amber-700">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Settings className="h-5 w-5 text-amber-600" />
                <CardTitle className="text-lg">Calculation Approach</CardTitle>
              </div>
              <Badge variant="outline" className="text-amber-700 border-amber-300">
                Organization Setting
              </Badge>
            </div>
            <CardDescription>
              Choose how license fees should be calculated for this organization
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-6">
              <div className="flex-1 grid grid-cols-3 gap-4">
                <div 
                  className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                    calculationApproach === 'manual' 
                      ? 'border-amber-500 bg-amber-100 dark:bg-amber-900/30' 
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                  onClick={() => updateApproachMutation.mutate('manual')}
                  data-testid="option-manual"
                >
                  <div className="flex items-center gap-2 mb-2">
                    <FileCode className="h-5 w-5 text-amber-600" />
                    <span className="font-semibold">Manual Rules</span>
                    {calculationApproach === 'manual' && <CheckCircle className="h-4 w-4 text-amber-600 ml-auto" />}
                  </div>
                  <p className="text-sm text-muted-foreground">
                    Use manually created license fee rules per contract
                  </p>
                </div>

                <div 
                  className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                    calculationApproach === 'erp_mapping_rules' 
                      ? 'border-green-500 bg-green-100 dark:bg-green-900/30' 
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                  onClick={() => updateApproachMutation.mutate('erp_mapping_rules')}
                  data-testid="option-erp-rules"
                >
                  <div className="flex items-center gap-2 mb-2">
                    <Database className="h-5 w-5 text-green-600" />
                    <span className="font-semibold">ERP Mapping Rules</span>
                    {calculationApproach === 'erp_mapping_rules' && <CheckCircle className="h-4 w-4 text-green-600 ml-auto" />}
                  </div>
                  <p className="text-sm text-muted-foreground">
                    Automated calculations based on ERP field mappings
                  </p>
                </div>

                <div 
                  className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                    calculationApproach === 'hybrid' 
                      ? 'border-blue-500 bg-blue-100 dark:bg-blue-900/30' 
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                  onClick={() => updateApproachMutation.mutate('hybrid')}
                  data-testid="option-hybrid"
                >
                  <div className="flex items-center gap-2 mb-2">
                    <Layers className="h-5 w-5 text-blue-600" />
                    <span className="font-semibold">Hybrid</span>
                    {calculationApproach === 'hybrid' && <CheckCircle className="h-4 w-4 text-blue-600 ml-auto" />}
                  </div>
                  <p className="text-sm text-muted-foreground">
                    Per-contract override with ERP rules as fallback
                  </p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* KPI Cards */}
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Total Rule Sets</p>
                  <p className="text-2xl font-bold" data-testid="text-total-rulesets">{ruleSets.length}</p>
                </div>
                <Layers className="h-8 w-8 text-muted-foreground/50" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Active</p>
                  <p className="text-2xl font-bold text-green-600" data-testid="text-active-rulesets">
                    {ruleSets.filter(rs => rs.status === 'active').length}
                  </p>
                </div>
                <CheckCircle className="h-8 w-8 text-green-600/50" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Draft</p>
                  <p className="text-2xl font-bold text-amber-600" data-testid="text-draft-rulesets">
                    {ruleSets.filter(rs => rs.status === 'draft').length}
                  </p>
                </div>
                <Edit className="h-8 w-8 text-amber-600/50" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Connected Systems</p>
                  <p className="text-2xl font-bold" data-testid="text-connected-systems">
                    {new Set(ruleSets.map(rs => rs.sourceSystemId)).size}
                  </p>
                </div>
                <Link2 className="h-8 w-8 text-muted-foreground/50" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Rule Sets List */}
        <div className="grid grid-cols-3 gap-6">
          {/* Left: Rule Sets List */}
          <div className="col-span-2">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle>Rule Sets</CardTitle>
                  <div className="flex items-center gap-2">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                      <Input 
                        placeholder="Search rule sets..." 
                        className="pl-9 w-64"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        data-testid="input-search-rulesets"
                      />
                    </div>
                    <Select value={statusFilter} onValueChange={setStatusFilter}>
                      <SelectTrigger className="w-32" data-testid="select-status-filter">
                        <SelectValue placeholder="Status" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All Status</SelectItem>
                        <SelectItem value="active">Active</SelectItem>
                        <SelectItem value="draft">Draft</SelectItem>
                        <SelectItem value="inactive">Inactive</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {ruleSetsLoading ? (
                  <div className="flex items-center justify-center py-12">
                    <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
                  </div>
                ) : filteredRuleSets.length === 0 ? (
                  <div className="text-center py-12">
                    <Layers className="h-12 w-12 mx-auto text-muted-foreground/50 mb-4" />
                    <h3 className="text-lg font-semibold mb-2">No Rule Sets Found</h3>
                    <p className="text-muted-foreground mb-4">
                      {searchQuery || statusFilter !== 'all' 
                        ? "No rule sets match your filters." 
                        : "Create your first ERP mapping rule set to automate license fee calculations."}
                    </p>
                    {!searchQuery && statusFilter === 'all' && (
                      <Button onClick={() => setCreateDialogOpen(true)} data-testid="button-create-first">
                        <Plus className="h-4 w-4 mr-2" /> Create Rule Set
                      </Button>
                    )}
                  </div>
                ) : (
                  <div className="space-y-3">
                    {filteredRuleSets.map((ruleSet) => (
                      <div 
                        key={ruleSet.id}
                        className={`p-4 border rounded-lg cursor-pointer transition-all hover:shadow-md ${
                          selectedRuleSet?.id === ruleSet.id 
                            ? 'border-primary bg-primary/5' 
                            : 'border-border hover:border-primary/50'
                        }`}
                        onClick={() => setSelectedRuleSet(ruleSet)}
                        data-testid={`card-ruleset-${ruleSet.id}`}
                      >
                        <div className="flex items-start justify-between">
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-1">
                              <h4 className="font-semibold">{ruleSet.name}</h4>
                              {getStatusBadge(ruleSet.status)}
                            </div>
                            {ruleSet.description && (
                              <p className="text-sm text-muted-foreground mb-2">{ruleSet.description}</p>
                            )}
                            <div className="flex items-center gap-4 text-sm text-muted-foreground">
                              <span className="flex items-center gap-1">
                                <Database className="h-3 w-3" />
                                {getSystemName(ruleSet.sourceSystemId)}
                              </span>
                              <span>v{ruleSet.version}</span>
                              <span>Created: {new Date(ruleSet.createdAt).toLocaleDateString()}</span>
                            </div>
                          </div>
                          <ChevronRight className="h-5 w-5 text-muted-foreground" />
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Right: Selected Rule Set Details */}
          <div>
            {selectedRuleSet ? (
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">Rule Set Details</CardTitle>
                    {getStatusBadge(selectedRuleSet.status)}
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <Label className="text-xs text-muted-foreground">Name</Label>
                    <p className="font-medium" data-testid="text-selected-name">{selectedRuleSet.name}</p>
                  </div>
                  {selectedRuleSet.description && (
                    <div>
                      <Label className="text-xs text-muted-foreground">Description</Label>
                      <p className="text-sm">{selectedRuleSet.description}</p>
                    </div>
                  )}
                  <Separator />
                  <div>
                    <Label className="text-xs text-muted-foreground">Source System</Label>
                    <p className="font-medium">{getSystemName(selectedRuleSet.sourceSystemId)}</p>
                  </div>
                  {selectedRuleSet.sourceEntityId && (
                    <div>
                      <Label className="text-xs text-muted-foreground">Source Entity</Label>
                      <p className="text-sm">{getEntityName(selectedRuleSet.sourceEntityId)}</p>
                    </div>
                  )}
                  {selectedRuleSet.targetEntityId && (
                    <div>
                      <Label className="text-xs text-muted-foreground">Target Entity</Label>
                      <p className="text-sm">{getEntityName(selectedRuleSet.targetEntityId)}</p>
                    </div>
                  )}
                  <Separator />
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label className="text-xs text-muted-foreground">Version</Label>
                      <p className="font-medium">v{selectedRuleSet.version}</p>
                    </div>
                    <div>
                      <Label className="text-xs text-muted-foreground">Created</Label>
                      <p className="text-sm">{new Date(selectedRuleSet.createdAt).toLocaleDateString()}</p>
                    </div>
                  </div>
                  {selectedRuleSet.effectiveDate && (
                    <div>
                      <Label className="text-xs text-muted-foreground">Effective Date</Label>
                      <p className="text-sm">{new Date(selectedRuleSet.effectiveDate).toLocaleDateString()}</p>
                    </div>
                  )}
                </CardContent>
                <CardFooter className="flex flex-col gap-2">
                  <div className="flex gap-2 w-full">
                    {selectedRuleSet.status === 'active' ? (
                      <Button 
                        variant="outline" 
                        className="flex-1"
                        onClick={() => toggleRuleSetMutation.mutate({ id: selectedRuleSet.id, action: 'deactivate' })}
                        data-testid="button-deactivate"
                      >
                        <Pause className="h-4 w-4 mr-2" /> Deactivate
                      </Button>
                    ) : (
                      <Button 
                        className="flex-1 bg-green-600 hover:bg-green-700"
                        onClick={() => toggleRuleSetMutation.mutate({ id: selectedRuleSet.id, action: 'activate' })}
                        data-testid="button-activate"
                      >
                        <Play className="h-4 w-4 mr-2" /> Activate
                      </Button>
                    )}
                    <Button 
                      variant="destructive"
                      onClick={() => {
                        if (confirm('Are you sure you want to delete this rule set?')) {
                          deleteRuleSetMutation.mutate(selectedRuleSet.id);
                        }
                      }}
                      data-testid="button-delete-ruleset"
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                  <Button 
                    variant="secondary" 
                    className="w-full"
                    onClick={() => setEditingRuleSet(selectedRuleSet)}
                    data-testid="button-edit-rules"
                  >
                    <Edit className="h-4 w-4 mr-2" /> Edit Rules
                  </Button>
                </CardFooter>
              </Card>
            ) : (
              <Card>
                <CardContent className="py-12">
                  <div className="text-center text-muted-foreground">
                    <Eye className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>Select a rule set to view details</p>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>

        {/* Create Rule Set Dialog */}
        <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
          <DialogContent className="max-w-lg">
            <DialogHeader>
              <DialogTitle>Create New Rule Set</DialogTitle>
              <DialogDescription>
                Define a new set of ERP mapping rules for automated license fee calculations.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label htmlFor="name">Rule Set Name *</Label>
                <Input 
                  id="name"
                  placeholder="e.g., Oracle Fusion Sales Mapping"
                  value={newRuleSet.name}
                  onChange={(e) => setNewRuleSet({ ...newRuleSet, name: e.target.value })}
                  data-testid="input-ruleset-name"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="description">Description</Label>
                <Textarea 
                  id="description"
                  placeholder="Describe the purpose of this rule set..."
                  value={newRuleSet.description}
                  onChange={(e) => setNewRuleSet({ ...newRuleSet, description: e.target.value })}
                  data-testid="input-ruleset-description"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="sourceSystem">Source ERP System *</Label>
                <Select 
                  value={newRuleSet.sourceSystemId} 
                  onValueChange={(v) => setNewRuleSet({ ...newRuleSet, sourceSystemId: v })}
                >
                  <SelectTrigger data-testid="select-source-system">
                    <SelectValue placeholder="Select ERP system..." />
                  </SelectTrigger>
                  <SelectContent>
                    {systems.map((system) => (
                      <SelectItem key={system.id} value={system.id}>
                        {system.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="sourceEntity">Source Entity</Label>
                  <Select 
                    value={newRuleSet.sourceEntityId} 
                    onValueChange={(v) => setNewRuleSet({ ...newRuleSet, sourceEntityId: v })}
                  >
                    <SelectTrigger data-testid="select-source-entity">
                      <SelectValue placeholder="Select entity..." />
                    </SelectTrigger>
                    <SelectContent>
                      {erpEntities
                        .filter((e: any) => !newRuleSet.sourceSystemId || e.erpSystemId === newRuleSet.sourceSystemId)
                        .map((entity: any) => (
                          <SelectItem key={entity.id} value={entity.id}>
                            {entity.name}
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="targetEntity">Target LicenseIQ Entity</Label>
                  <Select 
                    value={newRuleSet.targetEntityId} 
                    onValueChange={(v) => setNewRuleSet({ ...newRuleSet, targetEntityId: v })}
                  >
                    <SelectTrigger data-testid="select-target-entity">
                      <SelectValue placeholder="Select entity..." />
                    </SelectTrigger>
                    <SelectContent>
                      {licenseiqEntities.map((entity) => (
                        <SelectItem key={entity.id} value={entity.id}>
                          {(entity as any).displayName || entity.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                Cancel
              </Button>
              <Button 
                onClick={handleCreateRuleSet}
                disabled={createRuleSetMutation.isPending}
                data-testid="button-save-ruleset"
              >
                {createRuleSetMutation.isPending ? (
                  <><RefreshCw className="h-4 w-4 mr-2 animate-spin" /> Creating...</>
                ) : (
                  <><Plus className="h-4 w-4 mr-2" /> Create Rule Set</>
                )}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Edit Rules Dialog (placeholder for future implementation) */}
        <Dialog open={!!editingRuleSet} onOpenChange={(open) => !open && setEditingRuleSet(null)}>
          <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Edit Rules: {editingRuleSet?.name}</DialogTitle>
              <DialogDescription>
                Configure field mappings and transformation rules
              </DialogDescription>
            </DialogHeader>
            <div className="py-4">
              <div className="text-center py-12 border-2 border-dashed rounded-lg">
                <Calculator className="h-12 w-12 mx-auto text-muted-foreground/50 mb-4" />
                <h3 className="text-lg font-semibold mb-2">Rule Editor Coming Soon</h3>
                <p className="text-muted-foreground max-w-md mx-auto">
                  The rule editor will allow you to configure field mappings, 
                  conditions, transformations, and calculation outputs for each rule set.
                </p>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setEditingRuleSet(null)}>
                Close
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </MainLayout>
  );
}
