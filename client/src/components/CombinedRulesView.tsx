import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { 
  Calculator, 
  Settings, 
  ArrowRight, 
  CheckCircle, 
  Database,
  FileCode,
  ExternalLink,
  Loader2,
  Info,
  Layers,
  AlertCircle,
  CheckCircle2,
  Edit,
  Trash2,
  Plus,
  Save,
  Sparkles,
  User
} from 'lucide-react';
import { Link } from 'wouter';
import { apiRequest, queryClient } from '@/lib/queryClient';
import { useToast } from '@/hooks/use-toast';

interface CombinedRulesViewProps {
  contractId: string;
}

interface CombinedRulesData {
  contractId: string;
  calculationApproach: string;
  manualRules: any[];
  erpGeneratedRules: any[];
  combinedCount: number;
  summary: {
    manualCount: number;
    erpCount: number;
    calculationMode: string;
  };
}

interface BlueprintData {
  blueprints: Array<{
    id: string;
    name: string;
    ruleType: string;
    isFullyMapped: boolean;
    unmappedFields: string[];
    dimensions: Array<{
      dimensionType: string;
      contractTerm: string;
      erpFieldName: string | null;
      isMapped: boolean;
    }>;
  }>;
  totalCount: number;
  fullyMappedCount: number;
}

const CALCULATION_APPROACH_LABELS: Record<string, string> = {
  manual: 'Manual Rules Only',
  erp_rules: 'ERP Mapping Rules Only',
  erp_mapping_rules: 'ERP Mapping Rules',
  hybrid: 'Hybrid (Manual + ERP)',
};

interface EditingMapping {
  id: string;
  sourceField: string;
  targetField: string;
}

export function CombinedRulesView({ contractId }: CombinedRulesViewProps) {
  const { toast } = useToast();
  const [editingMapping, setEditingMapping] = useState<EditingMapping | null>(null);
  const [editFormData, setEditFormData] = useState({ contractTerm: '', erpFieldName: '' });
  
  const { data, isLoading, error } = useQuery<CombinedRulesData>({
    queryKey: ['/api/contracts', contractId, 'combined-rules'],
    queryFn: async () => {
      const res = await fetch(`/api/contracts/${contractId}/combined-rules`);
      if (!res.ok) throw new Error('Failed to fetch combined rules');
      return res.json();
    },
    enabled: !!contractId,
  });

  const { data: blueprintData, isLoading: blueprintLoading } = useQuery<BlueprintData>({
    queryKey: ['/api/contracts', contractId, 'blueprints'],
    queryFn: async () => {
      const res = await fetch(`/api/contracts/${contractId}/blueprints`);
      if (!res.ok) throw new Error('Failed to fetch blueprints');
      return res.json();
    },
    enabled: !!contractId,
  });

  const deleteRuleMutation = useMutation({
    mutationFn: async (ruleId: string) => {
      await apiRequest('DELETE', `/api/contracts/${contractId}/rules/${ruleId}`);
    },
    onSuccess: () => {
      toast({ title: 'Rule deleted', description: 'The rule has been removed successfully.' });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'combined-rules'] });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'blueprints'] });
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
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'combined-rules'] });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'blueprints'] });
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
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'combined-rules'] });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'blueprints'] });
      queryClient.invalidateQueries({ queryKey: ['/api/confirmed-term-mappings'] });
      setEditingMapping(null);
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to update mapping.', variant: 'destructive' });
    },
  });

  const handleEditMapping = (rule: any) => {
    setEditFormData({ contractTerm: rule.sourceField, erpFieldName: rule.targetField });
    setEditingMapping({ id: rule.id, sourceField: rule.sourceField, targetField: rule.targetField });
  };

  const handleSaveMapping = () => {
    if (!editingMapping) return;
    updateMappingMutation.mutate({
      id: editingMapping.id,
      contractTerm: editFormData.contractTerm,
      erpFieldName: editFormData.erpFieldName
    });
  };

  const deleteBlueprintMutation = useMutation({
    mutationFn: async (blueprintId: string) => {
      await apiRequest('DELETE', `/api/blueprints/${blueprintId}`);
    },
    onSuccess: () => {
      toast({ title: 'Blueprint deleted', description: 'The blueprint has been removed.' });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'blueprints'] });
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to delete blueprint.', variant: 'destructive' });
    },
  });

  const regenerateBlueprintsMutation = useMutation({
    mutationFn: async () => {
      await apiRequest('POST', `/api/contracts/${contractId}/materialize-blueprints`);
    },
    onSuccess: () => {
      toast({ title: 'Blueprints regenerated', description: 'All blueprints have been re-materialized.' });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'blueprints'] });
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'combined-rules'] });
    },
    onError: () => {
      toast({ title: 'Error', description: 'Failed to regenerate blueprints.', variant: 'destructive' });
    },
  });

  if (isLoading) {
    return (
      <Card>
        <CardContent className="py-8">
          <div className="flex items-center justify-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin" />
            <span>Loading rules...</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    return (
      <Card>
        <CardContent className="py-8">
          <div className="text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>Unable to load combined rules.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const { manualRules, erpGeneratedRules, calculationApproach, summary } = data;

  return (
    <div className="space-y-6">
      {/* Section: Confirmed Mapping Library (Step 2) */}
      <Card>
        <CardHeader>
          <div className="flex justify-between items-start">
            <div>
              <CardTitle className="flex items-center gap-2" data-testid="text-erp-mappings-title">
                <Database className="h-5 w-5" />
                <Badge variant="outline" className="mr-1 text-xs">Step 2</Badge>
                Confirmed Mapping Library
              </CardTitle>
              <CardDescription>
                Confirmed mappings that link contract terms to your ERP system for data sync and dual-terminology display
              </CardDescription>
            </div>
            <Link href="/erp-mapping-rules">
              <Button variant="outline" size="sm" data-testid="link-manage-erp-rules">
                <Settings className="h-4 w-4 mr-1" />
                Manage Mappings
                <ExternalLink className="h-3 w-3 ml-1" />
              </Button>
            </Link>
          </div>

          <div className="flex gap-4 mt-4 flex-wrap">
            <Badge variant="default" className="bg-emerald-500">
              <Database className="h-3 w-3 mr-1" />
              {summary.erpCount} Field Mappings
            </Badge>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <Badge variant="outline" className="text-blue-600 border-blue-300 cursor-help">
                    <Info className="h-3 w-3 mr-1" />
                    What are these?
                  </Badge>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="text-sm">
                    Confirmed mappings (from Step 1) connect contract terms to your ERP fields for 
                    dual-terminology display. They flow into Blueprints (Step 4) for automated calculations.
                  </p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        </CardHeader>

        <CardContent>
          {erpGeneratedRules.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>No confirmed mappings found.</p>
              <p className="text-sm mt-2">
                Confirm AI mapping suggestions (Step 1 above) to populate this library.
              </p>
            </div>
          ) : (
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Contract Term</TableHead>
                    <TableHead className="w-8 text-center"></TableHead>
                    <TableHead>ERP Field</TableHead>
                    <TableHead>Dual Terminology</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead className="w-[100px]">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {erpGeneratedRules.map((rule, index) => (
                    <TableRow key={rule.id || index} data-testid={`row-erp-mapping-${rule.id || index}`}>
                      <TableCell>
                        <div className="font-medium">{rule.sourceField}</div>
                      </TableCell>
                      <TableCell className="text-center">
                        <ArrowRight className="h-4 w-4 text-muted-foreground" />
                      </TableCell>
                      <TableCell>
                        <div className="font-medium text-primary">{rule.targetField}</div>
                      </TableCell>
                      <TableCell>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger>
                              <Badge variant="outline" className="text-emerald-600 border-emerald-300">
                                <CheckCircle className="h-3 w-3 mr-1" />
                                {rule.dualTerminology?.length > 40 
                                  ? rule.dualTerminology.substring(0, 40) + '...' 
                                  : rule.dualTerminology || `${rule.sourceField} (ERP: ${rule.targetField})`}
                              </Badge>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>{rule.dualTerminology || `${rule.sourceField} (ERP: ${rule.targetField})`}</p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </TableCell>
                      <TableCell>
                        {rule.isActive ? (
                          <Badge variant="default" className="bg-green-500">Active</Badge>
                        ) : (
                          <Badge variant="secondary">Inactive</Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Button 
                            variant="ghost" 
                            size="icon" 
                            className="h-8 w-8" 
                            onClick={() => handleEditMapping(rule)}
                            data-testid={`button-edit-erp-mapping-${rule.id || index}`}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive" data-testid={`button-delete-erp-mapping-${rule.id || index}`}>
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>Delete Mapping</AlertDialogTitle>
                                <AlertDialogDescription>
                                  Are you sure you want to delete the mapping "{rule.sourceField} → {rule.targetField}"? This may affect calculation blueprints.
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Cancel</AlertDialogCancel>
                                <AlertDialogAction 
                                  onClick={() => deleteMappingMutation.mutate(rule.id)}
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

      {/* Section: Contract Calculation Rules (Step 3) */}
      <Card>
        <CardHeader>
          <div className="flex justify-between items-start">
            <div>
              <CardTitle className="flex items-center gap-2" data-testid="text-calculation-rules-title">
                <Calculator className="h-5 w-5" />
                <Badge variant="outline" className="mr-1 text-xs">Step 3</Badge>
                Contract Calculation Rules
              </CardTitle>
              <CardDescription>
                Rules that define how to calculate license fees (rates, percentages, formulas) - extracted from contract
              </CardDescription>
            </div>
            <div className="flex gap-2">
              <Link href={`/rules/${contractId}`}>
                <Button variant="outline" size="sm" data-testid="link-manage-rules">
                  <Settings className="h-4 w-4 mr-1" />
                  Manage Rules
                  <ExternalLink className="h-3 w-3 ml-1" />
                </Button>
              </Link>
              <Link href="/rules-workspace">
                <Button variant="ghost" size="sm" data-testid="link-rules-workspace">
                  <Layers className="h-4 w-4 mr-1" />
                  All Rules
                </Button>
              </Link>
            </div>
          </div>

          <div className="flex gap-4 mt-4 flex-wrap">
            <Badge variant="outline" className="flex items-center gap-1">
              <Calculator className="h-3 w-3" />
              Mode: {CALCULATION_APPROACH_LABELS[calculationApproach] || calculationApproach}
            </Badge>
            <Badge variant="secondary">
              <FileCode className="h-3 w-3 mr-1" />
              {summary.manualCount} Calculation Rules
            </Badge>
          </div>
        </CardHeader>

        <CardContent>
          {manualRules.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <Calculator className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>No calculation rules found for this contract.</p>
              <p className="text-sm mt-2">
                Calculation rules are extracted automatically from contract analysis.
              </p>
              <Link href={`/rules/${contractId}`}>
                <Button variant="outline" size="sm" className="mt-4" data-testid="button-add-first-rule">
                  <Plus className="h-4 w-4 mr-1" />
                  Add First Rule
                </Button>
              </Link>
            </div>
          ) : (
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Rule Name</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead>Rate/Value</TableHead>
                    <TableHead>Priority</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead className="w-[100px]">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {manualRules.map((rule, index) => {
                    const hasSource = rule.sourceText || rule.sourceSection || rule.confidence;
                    const isAiExtracted = hasSource;
                    return (
                    <TableRow key={rule.id || index} data-testid={`row-rule-${rule.id || index}`}>
                      <TableCell>
                        <div className="font-medium">{rule.ruleName}</div>
                        {rule.description && (
                          <div className="text-sm text-muted-foreground truncate max-w-[300px]">
                            {rule.description}
                          </div>
                        )}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{rule.ruleType}</Badge>
                      </TableCell>
                      <TableCell>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Badge 
                                variant="outline" 
                                className={`flex items-center gap-1 w-fit ${
                                  isAiExtracted 
                                    ? 'bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-950 dark:text-purple-300 dark:border-purple-700' 
                                    : 'bg-blue-100 text-blue-700 border-blue-300 dark:bg-blue-950 dark:text-blue-300 dark:border-blue-700'
                                }`}
                                data-testid={`badge-source-${rule.id || index}`}
                              >
                                {isAiExtracted ? <Sparkles className="h-3 w-3" /> : <User className="h-3 w-3" />}
                                {isAiExtracted ? 'AI Extracted' : 'Manual'}
                              </Badge>
                            </TooltipTrigger>
                            <TooltipContent side="bottom" className="max-w-xs">
                              <p className="text-sm">
                                {isAiExtracted 
                                  ? `Automatically extracted from contract by AI${rule.sourceSection ? ` (Section: ${rule.sourceSection})` : ''}${rule.confidence ? ` with ${parseFloat(rule.confidence).toFixed(0)}% confidence` : ''}`
                                  : 'Manually created by a user'
                                }
                              </p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </TableCell>
                      <TableCell>
                        {rule.percentage ? `${rule.percentage}%` : 
                         rule.fixedAmount ? `$${rule.fixedAmount}` : 
                         rule.formulaDetails ? 'Formula' : '-'}
                      </TableCell>
                      <TableCell>{rule.priority || '-'}</TableCell>
                      <TableCell>
                        {rule.isActive ? (
                          <Badge variant="default" className="bg-green-500">Active</Badge>
                        ) : (
                          <Badge variant="secondary">Inactive</Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Link href={`/rules/${contractId}?edit=${rule.id}`}>
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
                                  onClick={() => deleteRuleMutation.mutate(rule.id)}
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
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Section: Blueprint Execution Status (Step 4) */}
      {(calculationApproach === 'erp_rules' || calculationApproach === 'erp_mapping_rules' || calculationApproach === 'hybrid') && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-start">
              <div>
                <CardTitle className="flex items-center gap-2" data-testid="text-blueprints-title">
                  <Layers className="h-5 w-5" />
                  <Badge variant="outline" className="mr-1 text-xs">Step 4</Badge>
                  Blueprint Execution Status
                </CardTitle>
                <CardDescription>
                  Blueprints merge calculation rules (Step 3) with confirmed mappings (Step 2) for automated sales matching
                </CardDescription>
              </div>
              <Button 
                variant="outline" 
                size="sm" 
                onClick={() => regenerateBlueprintsMutation.mutate()}
                disabled={regenerateBlueprintsMutation.isPending}
                data-testid="button-regenerate-blueprints"
              >
                {regenerateBlueprintsMutation.isPending ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                    Regenerating...
                  </>
                ) : (
                  <>
                    <Settings className="h-4 w-4 mr-1" />
                    Regenerate All
                  </>
                )}
              </Button>
            </div>

            <div className="flex gap-4 mt-4 flex-wrap">
              {blueprintLoading ? (
                <Badge variant="outline" className="animate-pulse">
                  <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                  Loading...
                </Badge>
              ) : blueprintData ? (
                <>
                  <Badge variant="outline" className="flex items-center gap-1">
                    <Layers className="h-3 w-3" />
                    {blueprintData.totalCount} Blueprints
                  </Badge>
                  <Badge 
                    variant={blueprintData.fullyMappedCount === blueprintData.totalCount ? "default" : "secondary"}
                    className={blueprintData.fullyMappedCount === blueprintData.totalCount ? "bg-green-500" : ""}
                  >
                    <CheckCircle2 className="h-3 w-3 mr-1" />
                    {blueprintData.fullyMappedCount} Fully Mapped
                  </Badge>
                </>
              ) : (
                <Badge variant="secondary">No blueprints</Badge>
              )}
            </div>
          </CardHeader>

          <CardContent>
            {blueprintLoading ? (
              <div className="flex items-center justify-center py-8 gap-2">
                <Loader2 className="h-5 w-5 animate-spin" />
                <span>Loading blueprints...</span>
              </div>
            ) : !blueprintData || blueprintData.blueprints.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Layers className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No calculation blueprints found.</p>
                <p className="text-sm mt-2">
                  Blueprints are auto-generated when you confirm AI mapping suggestions (Step 1) for calculation rules (Step 3).
                </p>
              </div>
            ) : (
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Blueprint Name</TableHead>
                      <TableHead>Rule Type</TableHead>
                      <TableHead>Dimensions</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="w-[80px]">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {blueprintData.blueprints.map((bp, index) => {
                      const mappedDims = bp.dimensions?.filter(d => d.isMapped).length || 0;
                      const totalDims = bp.dimensions?.length || 0;
                      
                      return (
                        <TableRow key={bp.id || index} data-testid={`row-blueprint-${bp.id || index}`}>
                          <TableCell>
                            <div className="font-medium">{bp.name}</div>
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline">{bp.ruleType}</Badge>
                          </TableCell>
                          <TableCell>
                            <TooltipProvider>
                              <Tooltip>
                                <TooltipTrigger>
                                  <Badge 
                                    variant={mappedDims === totalDims ? "default" : "secondary"}
                                    className={mappedDims === totalDims ? "bg-green-500" : ""}
                                  >
                                    {mappedDims}/{totalDims} mapped
                                  </Badge>
                                </TooltipTrigger>
                                <TooltipContent className="max-w-xs">
                                  <div className="text-sm">
                                    {bp.dimensions?.map((dim, i) => (
                                      <div key={i} className="flex items-center gap-1">
                                        {dim.isMapped ? (
                                          <CheckCircle className="h-3 w-3 text-green-500" />
                                        ) : (
                                          <AlertCircle className="h-3 w-3 text-amber-500" />
                                        )}
                                        {dim.dimensionType}: {dim.contractTerm}
                                        {dim.erpFieldName && ` → ${dim.erpFieldName}`}
                                      </div>
                                    ))}
                                  </div>
                                </TooltipContent>
                              </Tooltip>
                            </TooltipProvider>
                          </TableCell>
                          <TableCell>
                            {bp.isFullyMapped ? (
                              <Badge variant="default" className="bg-green-500">
                                <CheckCircle2 className="h-3 w-3 mr-1" />
                                Ready
                              </Badge>
                            ) : (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger>
                                    <Badge variant="secondary" className="text-amber-600">
                                      <AlertCircle className="h-3 w-3 mr-1" />
                                      Partial
                                    </Badge>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    <p>Unmapped: {bp.unmappedFields?.join(', ') || 'Unknown fields'}</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </TableCell>
                          <TableCell>
                            <AlertDialog>
                              <AlertDialogTrigger asChild>
                                <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive" data-testid={`button-delete-blueprint-${bp.id || index}`}>
                                  <Trash2 className="h-4 w-4" />
                                </Button>
                              </AlertDialogTrigger>
                              <AlertDialogContent>
                                <AlertDialogHeader>
                                  <AlertDialogTitle>Delete Blueprint</AlertDialogTitle>
                                  <AlertDialogDescription>
                                    Are you sure you want to delete the blueprint "{bp.name}"? It will be regenerated if you run "Regenerate All".
                                  </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                                  <AlertDialogAction 
                                    onClick={() => deleteBlueprintMutation.mutate(bp.id)}
                                    className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                  >
                                    Delete
                                  </AlertDialogAction>
                                </AlertDialogFooter>
                              </AlertDialogContent>
                            </AlertDialog>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      <Dialog open={!!editingMapping} onOpenChange={(open) => !open && setEditingMapping(null)}>
        <DialogContent className="sm:max-w-[425px]" data-testid="dialog-edit-erp-mapping">
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
    </div>
  );
}

export default CombinedRulesView;
