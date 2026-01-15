import { useQuery, useMutation } from '@tanstack/react-query';
import { apiRequest, queryClient } from '@/lib/queryClient';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/checkbox';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Progress } from '@/components/ui/progress';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { 
  CheckCircle, 
  XCircle, 
  Clock, 
  ArrowRight, 
  Sparkles,
  AlertTriangle,
  ThumbsUp,
  ThumbsDown,
  Edit2,
  ChevronDown,
  ChevronUp,
  Loader2,
  RefreshCw,
  Info,
  ExternalLink,
  Calculator
} from 'lucide-react';
import { Link } from 'wouter';
import { useState } from 'react';
import { useToast } from '@/hooks/use-toast';

interface PendingTermMapping {
  id: string;
  contractId: string;
  extractionRunId: string | null;
  originalTerm: string;
  originalValue: string | null;
  sourceText: string | null;
  erpSystemId: string;
  erpEntityId: string | null;
  erpFieldId: string | null;
  erpFieldName: string;
  erpEntityName: string | null;
  confidence: string;
  mappingMethod: string;
  alternativeMappings: AlternativeMapping[] | null;
  status: 'pending' | 'confirmed' | 'rejected' | 'modified';
  confirmedBy: string | null;
  confirmedAt: string | null;
  modifiedValue: string | null;
  modifiedFieldId: string | null;
  createdAt: string;
  // LicenseIQ dual terminology fields
  licenseiqEntityId?: string | null;
  licenseiqEntityName?: string | null;
  licenseiqFieldId?: string | null;
  licenseiqFieldName?: string | null;
  masterDataValue?: string | null;
  masterDataLookupValue?: string | null;
}

interface AlternativeMapping {
  erpFieldId: string;
  erpFieldName: string;
  erpEntityName?: string;
  confidence: number;
}

interface ErpField {
  id: string;
  fieldName: string;
  displayName: string;
  entityId: string;
}

interface ErpEntity {
  id: string;
  name: string;
  displayName: string | null;
  entityType: string | null;
}

interface PendingTermMappingsProps {
  contractId: string;
  showConfirmed?: boolean;
}

const ORACLE_FUSION_SYSTEM_ID = '03c4a661-b078-4e6c-bde2-3d32dda784c2';

export default function PendingTermMappings({ contractId, showConfirmed = false }: PendingTermMappingsProps) {
  const { toast } = useToast();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editEntityId, setEditEntityId] = useState<string>('');
  const [editFieldId, setEditFieldId] = useState<string>('');

  const statusFilter = showConfirmed ? undefined : 'pending';

  const { data: response, isLoading, refetch } = useQuery<{ mappings: PendingTermMapping[] }>({
    queryKey: ['/api/contracts', contractId, 'pending-mappings', statusFilter],
  });

  const { data: entitiesResponse } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', ORACLE_FUSION_SYSTEM_ID],
    queryFn: async () => {
      const res = await fetch(`/api/erp-entities?systemId=${ORACLE_FUSION_SYSTEM_ID}`);
      if (!res.ok) throw new Error('Failed to fetch entities');
      return res.json();
    },
  });

  const { data: erpFieldsResponse } = useQuery<{ fields: ErpField[] }>({
    queryKey: ['/api/erp-fields', editEntityId],
    queryFn: async () => {
      if (!editEntityId) return { fields: [] };
      const res = await fetch(`/api/erp-fields?entityId=${editEntityId}`);
      if (!res.ok) throw new Error('Failed to fetch fields');
      return res.json();
    },
    enabled: !!editEntityId,
  });

  const mappings = response?.mappings || [];
  const erpEntities = entitiesResponse?.entities || [];
  const erpFields = erpFieldsResponse?.fields || [];

  const confirmMutation = useMutation({
    mutationFn: async ({ id, modifiedFieldId }: { id: string; modifiedFieldId?: string }) => {
      return apiRequest('POST', `/api/pending-mappings/${id}/confirm`, { modifiedFieldId });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'pending-mappings'] });
      toast({
        title: 'Mapping Confirmed',
        description: 'The term mapping has been confirmed.',
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Confirmation Failed',
        description: error.message || 'Failed to confirm mapping',
        variant: 'destructive',
      });
    },
  });

  const bulkConfirmMutation = useMutation({
    mutationFn: async (mappingIds: string[]) => {
      return apiRequest('POST', `/api/pending-mappings/bulk-confirm`, { mappingIds });
    },
    onSuccess: (data: any) => {
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'pending-mappings'] });
      setSelectedIds(new Set());
      toast({
        title: 'Mappings Confirmed',
        description: `${data.count} mappings have been confirmed.`,
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Bulk Confirmation Failed',
        description: error.message || 'Failed to confirm mappings',
        variant: 'destructive',
      });
    },
  });

  const rejectMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiRequest('POST', `/api/pending-mappings/${id}/reject`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'pending-mappings'] });
      toast({
        title: 'Mapping Rejected',
        description: 'The term mapping has been rejected.',
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Rejection Failed',
        description: error.message || 'Failed to reject mapping',
        variant: 'destructive',
      });
    },
  });

  const regenerateRulesMutation = useMutation({
    mutationFn: async () => {
      return apiRequest('POST', `/api/contracts/${contractId}/regenerate-erp-rules`);
    },
    onSuccess: (data: any) => {
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'combined-rules'] });
      toast({
        title: 'ERP Rules Regenerated',
        description: data.message || `Generated ${data.rulesGenerated} rules`,
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Regeneration Failed',
        description: error.message || 'Failed to regenerate ERP rules',
        variant: 'destructive',
      });
    },
  });

  const updateMappingMutation = useMutation({
    mutationFn: async ({ id, updates }: { id: string; updates: Record<string, unknown> }) => {
      return apiRequest('PATCH', `/api/pending-mappings/${id}`, updates);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/contracts', contractId, 'pending-mappings'] });
      toast({
        title: 'Mapping Updated',
        description: 'The field mapping has been changed.',
      });
      setEditingId(null);
      setEditEntityId('');
      setEditFieldId('');
    },
    onError: (error: any) => {
      toast({
        title: 'Update Failed',
        description: error.message || 'Failed to update mapping',
        variant: 'destructive',
      });
    },
  });

  const handleConfirm = (id: string, modifiedFieldId?: string) => {
    confirmMutation.mutate({ id, modifiedFieldId });
    setEditingId(null);
    setEditEntityId('');
    setEditFieldId('');
  };

  const handleUpdateMapping = (mappingId: string) => {
    const selectedField = erpFields.find(f => f.id === editFieldId);
    const selectedEntity = erpEntities.find(e => e.id === editEntityId);
    
    if (!selectedField || !selectedEntity) {
      toast({
        title: 'Missing Selection',
        description: 'Please select both an entity and a field.',
        variant: 'destructive',
      });
      return;
    }
    
    updateMappingMutation.mutate({
      id: mappingId,
      updates: {
        erpFieldId: selectedField.id,
        erpFieldName: selectedField.fieldName || selectedField.displayName,
        erpEntityId: selectedEntity.id,
        erpEntityName: selectedEntity.name,
        modifiedFieldId: selectedField.id,
        modifiedValue: selectedField.fieldName || selectedField.displayName,
        status: 'modified',
      }
    });
  };

  const startEditing = (mapping: PendingTermMapping) => {
    setEditingId(mapping.id);
    setEditEntityId(mapping.erpEntityId || '');
    setEditFieldId(mapping.erpFieldId || '');
  };

  const cancelEditing = () => {
    setEditingId(null);
    setEditEntityId('');
    setEditFieldId('');
  };

  const selectAlternativeMapping = (mapping: PendingTermMapping, alt: AlternativeMapping) => {
    const altEntity = erpEntities.find(e => e.name === alt.erpEntityName);
    setEditingId(mapping.id);
    setEditEntityId(altEntity?.id || mapping.erpEntityId || '');
    setEditFieldId(alt.erpFieldId);
  };

  const handleReject = (id: string) => {
    rejectMutation.mutate(id);
  };

  const handleBulkConfirm = () => {
    if (selectedIds.size === 0) {
      toast({
        title: 'No Mappings Selected',
        description: 'Please select at least one mapping to confirm.',
        variant: 'destructive',
      });
      return;
    }
    bulkConfirmMutation.mutate(Array.from(selectedIds));
  };

  const toggleSelection = (id: string) => {
    const newSelection = new Set(selectedIds);
    if (newSelection.has(id)) {
      newSelection.delete(id);
    } else {
      newSelection.add(id);
    }
    setSelectedIds(newSelection);
  };

  const selectAll = () => {
    const pendingIds = mappings.filter(m => m.status === 'pending').map(m => m.id);
    setSelectedIds(new Set(pendingIds));
  };

  const deselectAll = () => {
    setSelectedIds(new Set());
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return 'text-green-600 dark:text-green-400';
    if (confidence >= 0.6) return 'text-yellow-600 dark:text-yellow-400';
    return 'text-red-600 dark:text-red-400';
  };

  const getConfidenceBadge = (confidence: number) => {
    if (confidence >= 0.8) return 'default';
    if (confidence >= 0.6) return 'secondary';
    return 'destructive';
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'confirmed':
        return <Badge variant="default" className="bg-green-500"><CheckCircle className="w-3 h-3 mr-1" /> Confirmed</Badge>;
      case 'rejected':
        return <Badge variant="destructive"><XCircle className="w-3 h-3 mr-1" /> Rejected</Badge>;
      case 'modified':
        return <Badge variant="secondary"><Edit2 className="w-3 h-3 mr-1" /> Modified</Badge>;
      default:
        return <Badge variant="outline"><Clock className="w-3 h-3 mr-1" /> Pending</Badge>;
    }
  };

  // Build dual terminology display: "Contract Term (LicenseIQ Entity: Master Data Value)"
  const buildDualTerminologyDisplay = (mapping: PendingTermMapping): { primary: string; secondary: string | null } => {
    const contractTerm = mapping.originalTerm;
    
    // If we have LicenseIQ entity and master data value, show dual terminology
    if (mapping.licenseiqEntityName && mapping.masterDataLookupValue) {
      return {
        primary: contractTerm,
        secondary: `LicenseIQ ${mapping.licenseiqEntityName}: ${mapping.masterDataLookupValue}`
      };
    }
    
    // If we have LicenseIQ entity but no master data value, show just the entity
    if (mapping.licenseiqEntityName && mapping.licenseiqFieldName) {
      return {
        primary: contractTerm,
        secondary: `LicenseIQ ${mapping.licenseiqEntityName}: ${mapping.licenseiqFieldName}`
      };
    }
    
    // Fallback to ERP field
    return {
      primary: contractTerm,
      secondary: mapping.erpEntityName ? `ERP: ${mapping.erpFieldName}` : null
    };
  };

  const pendingCount = mappings.filter(m => m.status === 'pending').length;
  const confirmedCount = mappings.filter(m => m.status === 'confirmed').length;
  const rejectedCount = mappings.filter(m => m.status === 'rejected').length;

  if (isLoading) {
    return (
      <Card>
        <CardContent className="py-8">
          <div className="flex items-center justify-center gap-2">
            <Loader2 className="h-5 w-5 animate-spin" />
            <span>Loading mappings...</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (mappings.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5" />
            <Badge variant="outline" className="mr-1 text-xs">Step 1</Badge>
            AI Mapping Suggestions
          </CardTitle>
          <CardDescription>
            Review AI-suggested mappings between contract terms and your ERP/LicenseIQ fields
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="text-center py-8 text-muted-foreground">
            <Info className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No pending AI mapping suggestions found for this contract.</p>
            <p className="text-sm mt-2">AI mapping suggestions will appear here after contract extraction with an ERP system configured.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex justify-between items-start">
          <div>
            <CardTitle className="flex items-center gap-2" data-testid="text-mapping-title">
              <Sparkles className="h-5 w-5" />
              <Badge variant="outline" className="mr-1 text-xs">Step 1</Badge>
              AI Mapping Suggestions
            </CardTitle>
            <CardDescription>
              Review and confirm AI-suggested mappings between contract terms and ERP/LicenseIQ fields
            </CardDescription>
          </div>
          <Button variant="outline" size="sm" onClick={() => refetch()} data-testid="button-refresh-mappings">
            <RefreshCw className="h-4 w-4 mr-1" />
            Refresh
          </Button>
        </div>
        
        <div className="flex gap-4 mt-4">
          <div className="flex items-center gap-2">
            <Badge variant="outline">{pendingCount} Pending</Badge>
            <Badge variant="default" className="bg-green-500">{confirmedCount} Confirmed</Badge>
            <Badge variant="destructive">{rejectedCount} Rejected</Badge>
            
            {confirmedCount > 0 && (
              <>
                <Button 
                  variant="outline" 
                  size="sm" 
                  className="ml-2"
                  onClick={() => regenerateRulesMutation.mutate()}
                  disabled={regenerateRulesMutation.isPending}
                  data-testid="button-regenerate-erp-rules"
                >
                  {regenerateRulesMutation.isPending ? (
                    <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                  ) : (
                    <RefreshCw className="h-4 w-4 mr-1" />
                  )}
                  Regenerate Rules
                </Button>
                <Link href="/erp-mapping-rules">
                  <Button variant="ghost" size="sm" className="ml-2 text-primary" data-testid="link-view-generated-rules">
                    <Calculator className="h-4 w-4 mr-1" />
                    View Generated Rules
                    <ExternalLink className="h-3 w-3 ml-1" />
                  </Button>
                </Link>
              </>
            )}
          </div>
          
          {pendingCount > 0 && (
            <div className="flex gap-2 ml-auto">
              <Button 
                variant="outline" 
                size="sm" 
                onClick={selectedIds.size === pendingCount ? deselectAll : selectAll}
                data-testid="button-select-all"
              >
                {selectedIds.size === pendingCount ? 'Deselect All' : 'Select All'}
              </Button>
              <Button 
                size="sm" 
                onClick={handleBulkConfirm}
                disabled={selectedIds.size === 0 || bulkConfirmMutation.isPending}
                data-testid="button-bulk-confirm"
              >
                {bulkConfirmMutation.isPending ? (
                  <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                ) : (
                  <ThumbsUp className="h-4 w-4 mr-1" />
                )}
                Confirm Selected ({selectedIds.size})
              </Button>
            </div>
          )}
        </div>
      </CardHeader>
      
      <CardContent>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12"></TableHead>
                <TableHead>Contract Term</TableHead>
                <TableHead className="w-8 text-center"></TableHead>
                <TableHead>Mapped Field</TableHead>
                <TableHead className="w-24">Confidence</TableHead>
                <TableHead className="w-24">Status</TableHead>
                <TableHead className="w-32">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {mappings.map((mapping) => {
                const confidence = parseFloat(mapping.confidence);
                const isExpanded = expandedId === mapping.id;
                const isEditing = editingId === mapping.id;
                const isPending = mapping.status === 'pending';
                
                return (
                  <>
                    <TableRow 
                      key={mapping.id}
                      className={isPending ? 'bg-amber-50/50 dark:bg-amber-950/20' : ''}
                      data-testid={`row-mapping-${mapping.id}`}
                    >
                      <TableCell>
                        {isPending && (
                          <Checkbox
                            checked={selectedIds.has(mapping.id)}
                            onCheckedChange={() => toggleSelection(mapping.id)}
                            data-testid={`checkbox-select-${mapping.id}`}
                          />
                        )}
                      </TableCell>
                      
                      <TableCell>
                        {(() => {
                          const dualDisplay = buildDualTerminologyDisplay(mapping);
                          return (
                            <>
                              <div className="font-medium">{dualDisplay.primary}</div>
                              {dualDisplay.secondary && (
                                <div className="text-sm text-emerald-600 dark:text-emerald-400 font-medium">
                                  ({dualDisplay.secondary})
                                </div>
                              )}
                              {mapping.originalValue && !dualDisplay.secondary && (
                                <div className="text-sm text-muted-foreground truncate max-w-[200px]">
                                  Value: {mapping.originalValue}
                                </div>
                              )}
                            </>
                          );
                        })()}
                      </TableCell>
                      
                      <TableCell className="text-center">
                        <ArrowRight className="h-4 w-4 text-muted-foreground" />
                      </TableCell>
                      
                      <TableCell>
                        <div className="font-medium text-primary">
                          {mapping.modifiedFieldId ? 
                            erpFields.find(f => f.id === mapping.modifiedFieldId)?.displayName || mapping.erpFieldName
                            : mapping.erpFieldName
                          }
                        </div>
                        {mapping.erpEntityName && (
                          <div className="text-sm text-muted-foreground">
                            {mapping.erpEntityName}
                          </div>
                        )}
                      </TableCell>
                      
                      <TableCell>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <div className="flex items-center gap-2">
                                <Progress 
                                  value={confidence * 100} 
                                  className="h-2 w-16" 
                                />
                                <span className={`text-sm font-medium ${getConfidenceColor(confidence)}`}>
                                  {Math.round(confidence * 100)}%
                                </span>
                              </div>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>AI confidence: {(confidence * 100).toFixed(1)}%</p>
                              <p className="text-xs text-muted-foreground">Method: {mapping.mappingMethod}</p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </TableCell>
                      
                      <TableCell>
                        {getStatusBadge(mapping.status)}
                      </TableCell>
                      
                      <TableCell>
                        <div className="flex items-center gap-1">
                          {isPending && (
                            <>
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => handleConfirm(mapping.id)}
                                disabled={confirmMutation.isPending}
                                title="Confirm mapping"
                                data-testid={`button-confirm-${mapping.id}`}
                              >
                                <ThumbsUp className="h-4 w-4 text-green-600" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => handleReject(mapping.id)}
                                disabled={rejectMutation.isPending}
                                title="Reject mapping"
                                data-testid={`button-reject-${mapping.id}`}
                              >
                                <ThumbsDown className="h-4 w-4 text-red-600" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => {
                                  if (isEditing) {
                                    cancelEditing();
                                  } else {
                                    startEditing(mapping);
                                  }
                                }}
                                title="Change ERP field"
                                data-testid={`button-edit-${mapping.id}`}
                              >
                                <Edit2 className="h-4 w-4" />
                              </Button>
                            </>
                          )}
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => setExpandedId(isExpanded ? null : mapping.id)}
                            title={isExpanded ? 'Hide details' : 'Show details'}
                            data-testid={`button-expand-${mapping.id}`}
                          >
                            {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                    
                    {(isExpanded || isEditing) && (
                      <TableRow key={`${mapping.id}-details`}>
                        <TableCell colSpan={7} className="bg-muted/30">
                          <div className="p-4 space-y-4">
                            {isEditing && (
                              <div className="space-y-3 bg-blue-50 dark:bg-blue-950/30 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                                <div className="flex items-center gap-2 text-blue-700 dark:text-blue-300">
                                  <Edit2 className="h-4 w-4" />
                                  <span className="text-sm font-medium">Change ERP Field Mapping</span>
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                  <div className="space-y-1">
                                    <label className="text-sm font-medium text-muted-foreground">ERP Entity</label>
                                    <Select 
                                      value={editEntityId} 
                                      onValueChange={(value) => {
                                        setEditEntityId(value);
                                        setEditFieldId('');
                                      }}
                                    >
                                      <SelectTrigger data-testid={`select-erp-entity-${mapping.id}`}>
                                        <SelectValue placeholder="Select entity..." />
                                      </SelectTrigger>
                                      <SelectContent>
                                        {erpEntities.map(entity => (
                                          <SelectItem key={entity.id} value={entity.id}>
                                            {entity.displayName || entity.name}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                  <div className="space-y-1">
                                    <label className="text-sm font-medium text-muted-foreground">ERP Field</label>
                                    <Select 
                                      value={editFieldId} 
                                      onValueChange={setEditFieldId}
                                      disabled={!editEntityId}
                                    >
                                      <SelectTrigger data-testid={`select-erp-field-${mapping.id}`}>
                                        <SelectValue placeholder={editEntityId ? "Select field..." : "Select entity first"} />
                                      </SelectTrigger>
                                      <SelectContent>
                                        {erpFields.map(field => (
                                          <SelectItem key={field.id} value={field.id}>
                                            {field.displayName || field.fieldName}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                </div>
                                <div className="flex gap-2 pt-2">
                                  <Button 
                                    size="sm" 
                                    onClick={() => handleUpdateMapping(mapping.id)}
                                    disabled={!editFieldId || !editEntityId || updateMappingMutation.isPending}
                                    data-testid={`button-save-edit-${mapping.id}`}
                                  >
                                    {updateMappingMutation.isPending ? (
                                      <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                                    ) : (
                                      <CheckCircle className="h-4 w-4 mr-1" />
                                    )}
                                    Save Changes
                                  </Button>
                                  <Button 
                                    variant="outline" 
                                    size="sm" 
                                    onClick={cancelEditing}
                                    data-testid={`button-cancel-edit-${mapping.id}`}
                                  >
                                    Cancel
                                  </Button>
                                </div>
                              </div>
                            )}
                            
                            {isExpanded && (
                              <>
                                {mapping.sourceText && (
                                  <div>
                                    <span className="text-sm font-medium">Source Text:</span>
                                    <p className="text-sm text-muted-foreground mt-1 bg-background p-2 rounded border">
                                      "{mapping.sourceText}"
                                    </p>
                                  </div>
                                )}
                                
                                {mapping.alternativeMappings && mapping.alternativeMappings.length > 0 && (
                                  <div>
                                    <span className="text-sm font-medium">Alternative Mappings:</span>
                                    <div className="flex flex-wrap gap-2 mt-2">
                                      {mapping.alternativeMappings.map((alt, idx) => (
                                        <Badge 
                                          key={idx} 
                                          variant="outline" 
                                          className="cursor-pointer hover:bg-primary hover:text-primary-foreground"
                                          onClick={() => {
                                            if (isPending) {
                                              selectAlternativeMapping(mapping, alt);
                                            }
                                          }}
                                        >
                                          {alt.erpFieldName} ({Math.round(alt.confidence * 100)}%)
                                        </Badge>
                                      ))}
                                    </div>
                                  </div>
                                )}
                                
                                <div className="grid grid-cols-2 gap-4 text-sm">
                                  <div>
                                    <span className="font-medium">Mapping Method:</span>
                                    <span className="ml-2 text-muted-foreground">{mapping.mappingMethod}</span>
                                  </div>
                                  <div>
                                    <span className="font-medium">Created:</span>
                                    <span className="ml-2 text-muted-foreground">
                                      {new Date(mapping.createdAt).toLocaleString()}
                                    </span>
                                  </div>
                                  {mapping.confirmedAt && (
                                    <div>
                                      <span className="font-medium">Confirmed:</span>
                                      <span className="ml-2 text-muted-foreground">
                                        {new Date(mapping.confirmedAt).toLocaleString()}
                                      </span>
                                    </div>
                                  )}
                                </div>
                              </>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    )}
                  </>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
