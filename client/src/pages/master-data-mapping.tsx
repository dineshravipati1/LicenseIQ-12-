import { useState, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { useLocation } from 'wouter';
import { apiRequest, queryClient } from '@/lib/queryClient';
import { useToast } from '@/hooks/use-toast';
import MainLayout from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Sparkles, Upload, Download, Save, Trash2, Eye, FileJson, AlertCircle, CheckCircle2, Loader2, Settings, Layers, Database, ChevronDown, ChevronRight, X, ArrowLeft, Plus, Pencil } from 'lucide-react';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import type { ErpSystem, ErpEntity, LicenseiqEntity, LicenseiqField } from '@shared/schema';
import { formatDateUSA } from '@/lib/dateFormat';

interface FieldMapping {
  source_field: string | null;
  target_field: string;
  transformation_rule: string;
  confidence: number;
}

interface MappingResult {
  mappingResults: FieldMapping[];
  sourceSchema: any;
  targetSchema: any;
  entityType: string;
  erpSystem: string;
}

interface SavedMapping {
  id: string;
  mappingName: string;
  erpSystem: string;
  entityType: string;
  sourceSchema: any;
  targetSchema: any;
  mappingResults: FieldMapping[];
  status: string;
  aiModel: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  notes?: string;
}

interface BatchSuggestion {
  erpEntityId: string;
  erpEntityName: string;
  erpEntityType: string;
  erpSchema: Record<string, string>;
  licenseiqEntityId: string | null;
  licenseiqEntityName: string | null;
  licenseiqSchema: Record<string, string> | null;
  fieldMappings: FieldMapping[];
  confidence: number;
  reasoning: string;
  erpFieldCount: number;
  mappedFieldCount: number;
}

export default function MasterDataMapping() {
  const [, navigate] = useLocation();
  const { toast } = useToast();
  const [sourceSchema, setSourceSchema] = useState('');
  const [targetSchema, setTargetSchema] = useState('');
  const [selectedSystemId, setSelectedSystemId] = useState('');
  const [selectedEntityId, setSelectedEntityId] = useState('');
  const [selectedLicenseiqEntityId, setSelectedLicenseiqEntityId] = useState('');
  const [mappingResult, setMappingResult] = useState<MappingResult | null>(null);
  const [editableMappings, setEditableMappings] = useState<FieldMapping[]>([]);
  const [saveMappingName, setSaveMappingName] = useState('');
  const [saveNotes, setSaveNotes] = useState('');
  const [expandedMappingId, setExpandedMappingId] = useState<string | null>(null);
  const [showSampleData, setShowSampleData] = useState(false);

  // Batch mapping state
  const [batchSystemId, setBatchSystemId] = useState('');
  const [batchEntityIds, setBatchEntityIds] = useState<string[]>([]);
  const [batchSuggestions, setBatchSuggestions] = useState<BatchSuggestion[]>([]);
  const [selectedSuggestions, setSelectedSuggestions] = useState<Set<string>>(new Set());
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  // Fetch ERP systems from catalog
  const { data: erpSystemsData } = useQuery<{ systems: ErpSystem[] }>({
    queryKey: ['/api/erp-systems'],
  });

  // Fetch entities for selected ERP system (single mapping tab)
  const { data: erpEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', selectedSystemId],
    enabled: !!selectedSystemId,
    queryFn: () => fetch(`/api/erp-entities?systemId=${selectedSystemId}`).then(res => res.json()),
  });

  // Fetch entities for batch mapping tab
  const { data: batchErpEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', batchSystemId],
    enabled: !!batchSystemId,
    queryFn: () => fetch(`/api/erp-entities?systemId=${batchSystemId}`).then(res => res.json()),
  });

  // Fetch fields for selected ERP entity
  const { data: erpFieldsData } = useQuery<{ fields: any[] }>({
    queryKey: ['/api/erp-fields', selectedEntityId],
    enabled: !!selectedEntityId,
    queryFn: () => fetch(`/api/erp-fields?entityId=${selectedEntityId}`).then(res => res.json()),
  });

  // Fetch LicenseIQ entities
  const { data: licenseiqEntitiesData } = useQuery<{ entities: LicenseiqEntity[] }>({
    queryKey: ['/api/licenseiq-entities'],
  });

  // Fetch fields for selected LicenseIQ entity
  const { data: licenseiqFieldsData } = useQuery<{ fields: LicenseiqField[] }>({
    queryKey: ['/api/licenseiq-fields', selectedLicenseiqEntityId],
    enabled: !!selectedLicenseiqEntityId,
    queryFn: () => fetch(`/api/licenseiq-fields?entityId=${selectedLicenseiqEntityId}`).then(res => res.json()),
  });

  // Fetch saved mappings
  const { data: savedMappings = { mappings: [] }, refetch: refetchMappings } = useQuery<{ mappings: SavedMapping[] }>({
    queryKey: ['/api/mapping'],
  });

  // Fetch sample data from ERP entity records (for source preview)
  const { data: erpSampleData } = useQuery<{ records: any[] }>({
    queryKey: ['/api/erp-entity-records', selectedEntityId],
    enabled: !!selectedEntityId && showSampleData,
    queryFn: () => fetch(`/api/erp-entity-records?entityId=${selectedEntityId}&limit=3`).then(res => res.json()),
  });

  // Fetch sample data from LicenseIQ tables (for target preview)
  const { data: licenseiqSampleData } = useQuery<{ records: any[] }>({
    queryKey: ['/api/licenseiq-entity-records', selectedLicenseiqEntityId],
    enabled: !!selectedLicenseiqEntityId && showSampleData,
    queryFn: () => fetch(`/api/licenseiq-entity-records?entityId=${selectedLicenseiqEntityId}&limit=3`).then(res => res.json()),
  });

  const selectedSystem = erpSystemsData?.systems?.find(s => s.id === selectedSystemId);
  const selectedEntity = erpEntitiesData?.entities?.find(e => e.id === selectedEntityId);
  const selectedLicenseiqEntity = licenseiqEntitiesData?.entities?.find(e => e.id === selectedLicenseiqEntityId);

  // Auto-populate SOURCE schema when ERP entity is selected
  useEffect(() => {
    if (erpFieldsData?.fields) {
      const schema: Record<string, string> = {};
      erpFieldsData.fields.forEach(field => {
        schema[field.fieldName] = field.dataType;
      });
      setSourceSchema(JSON.stringify(schema, null, 2));
    }
  }, [erpFieldsData]);

  // Auto-populate TARGET schema when LicenseIQ entity is selected
  useEffect(() => {
    if (licenseiqFieldsData?.fields) {
      const schema: Record<string, string> = {};
      licenseiqFieldsData.fields.forEach(field => {
        schema[field.fieldName] = field.dataType;
      });
      setTargetSchema(JSON.stringify(schema, null, 2));
    }
  }, [licenseiqFieldsData]);

  // Clear batch entity selections when batch system changes
  useEffect(() => {
    setBatchEntityIds([]);
  }, [batchSystemId]);

  // Generate mapping mutation
  const generateMutation = useMutation({
    mutationFn: async (data: { sourceSchema: any; targetSchema: any; entityType: string; erpSystem: string }) => {
      const response = await apiRequest('POST', '/api/mapping/generate', data);
      return response.json();
    },
    onSuccess: (data: MappingResult) => {
      setMappingResult(data);
      setEditableMappings([...data.mappingResults]);
      toast({
        title: 'Mapping Generated',
        description: `Successfully generated ${data.mappingResults.length} field mappings using AI. You can edit the mappings below before saving.`,
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Generation Failed',
        description: error.message || 'Failed to generate mapping',
        variant: 'destructive',
      });
    },
  });

  // Save mapping mutation
  const saveMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await apiRequest('POST', '/api/mapping/save', data);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: 'Mapping Saved',
        description: 'Mapping configuration has been saved successfully.',
      });
      setSaveMappingName('');
      setSaveNotes('');
      refetchMappings();
    },
    onError: (error: any) => {
      toast({
        title: 'Save Failed',
        description: error.message || 'Failed to save mapping',
        variant: 'destructive',
      });
    },
  });

  // Batch generate mutation
  const batchGenerateMutation = useMutation({
    mutationFn: async (data: { erpSystemId: string; erpEntityIds: string[] }) => {
      const response = await apiRequest('POST', '/api/mapping/batch-generate', data);
      return response.json();
    },
    onSuccess: (data: { suggestions: BatchSuggestion[]; erpSystemName: string }) => {
      setBatchSuggestions(data.suggestions);
      // Auto-select high confidence mappings (90%+)
      const highConfidence = new Set(
        data.suggestions.filter(s => s.confidence >= 90).map(s => s.erpEntityId)
      );
      setSelectedSuggestions(highConfidence);
      toast({
        title: 'Batch Mapping Complete',
        description: `Generated ${data.suggestions.length} entity mappings. ${highConfidence.size} high-confidence matches auto-selected.`,
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Batch Generation Failed',
        description: error.message || 'Failed to generate batch mappings',
        variant: 'destructive',
      });
    },
  });

  // Delete mapping mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const response = await apiRequest('DELETE', `/api/mapping/${id}`);
      if (response.status === 204) return { success: true };
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: 'Mapping Deleted',
        description: 'Mapping has been removed successfully.',
      });
      refetchMappings();
    },
    onError: (error: any) => {
      toast({
        title: 'Delete Failed',
        description: error.message || 'Failed to delete mapping',
        variant: 'destructive',
      });
    },
  });

  const handleGenerateMapping = () => {
    let parsedSource, parsedTarget;
    
    try {
      parsedSource = JSON.parse(sourceSchema);
      parsedTarget = JSON.parse(targetSchema);
    } catch (error) {
      toast({
        title: 'Invalid JSON',
        description: 'Please provide valid JSON schemas.',
        variant: 'destructive',
      });
      return;
    }

    if (!selectedSystemId) {
      toast({
        title: 'Missing ERP System',
        description: 'Please select an ERP system.',
        variant: 'destructive',
      });
      return;
    }

    if (!selectedEntityId) {
      toast({
        title: 'Missing Entity',
        description: 'Please select an entity type.',
        variant: 'destructive',
      });
      return;
    }

    generateMutation.mutate({
      sourceSchema: parsedSource,
      targetSchema: parsedTarget,
      entityType: selectedEntity?.name || 'unknown',
      erpSystem: selectedSystem?.name || 'unknown',
    });
  };

  const handleSaveMapping = () => {
    if (!saveMappingName || !mappingResult) {
      toast({
        title: 'Invalid Data',
        description: 'Please provide a mapping name.',
        variant: 'destructive',
      });
      return;
    }

    // Use editableMappings (user-modified) instead of original AI mappings
    saveMutation.mutate({
      mappingName: saveMappingName,
      erpSystem: mappingResult.erpSystem,
      entityType: mappingResult.entityType,
      sourceSchema: mappingResult.sourceSchema,
      targetSchema: mappingResult.targetSchema,
      mappingResults: editableMappings,
      notes: saveNotes || null,
    });
  };

  const handleLoadMapping = (mapping: SavedMapping) => {
    setSourceSchema(JSON.stringify(mapping.sourceSchema, null, 2));
    setTargetSchema(JSON.stringify(mapping.targetSchema, null, 2));
    // Note: Can't restore system/entity selection from names alone - would need IDs
    setMappingResult({
      mappingResults: mapping.mappingResults,
      sourceSchema: mapping.sourceSchema,
      targetSchema: mapping.targetSchema,
      entityType: mapping.entityType,
      erpSystem: mapping.erpSystem,
    });
    setEditableMappings([...mapping.mappingResults]);
    toast({
      title: 'Mapping Loaded',
      description: `Loaded mapping: ${mapping.mappingName}. You can edit the mappings below.`,
    });
  };

  const getConfidenceBadge = (confidence: number) => {
    if (confidence >= 90) return <Badge className="bg-green-500">High ({confidence}%)</Badge>;
    if (confidence >= 70) return <Badge className="bg-yellow-500">Medium ({confidence}%)</Badge>;
    return <Badge className="bg-red-500">Low ({confidence}%)</Badge>;
  };

  // Helper functions for inline editing of mappings
  const updateMappingField = (index: number, field: keyof FieldMapping, value: string | number | null) => {
    const updated = [...editableMappings];
    if (field === 'confidence') {
      updated[index] = { ...updated[index], [field]: value as number };
    } else if (field === 'source_field') {
      updated[index] = { ...updated[index], [field]: value === '__null__' ? null : value as string };
    } else {
      updated[index] = { ...updated[index], [field]: value as string };
    }
    setEditableMappings(updated);
  };

  const addNewMapping = () => {
    setEditableMappings([
      ...editableMappings,
      { source_field: null, target_field: '', transformation_rule: 'direct', confidence: 50 }
    ]);
  };

  const removeMapping = (index: number) => {
    setEditableMappings(editableMappings.filter((_, i) => i !== index));
  };

  // Get source field options from the source schema
  const getSourceFieldOptions = (): string[] => {
    try {
      const parsed = JSON.parse(sourceSchema);
      return Object.keys(parsed);
    } catch {
      return [];
    }
  };

  // Get target field options from the target schema
  const getTargetFieldOptions = (): string[] => {
    try {
      const parsed = JSON.parse(targetSchema);
      return Object.keys(parsed);
    } catch {
      return [];
    }
  };

  // Get sample values for a specific source field from ERP data
  const getSourceSampleValues = (fieldName: string): string[] => {
    if (!erpSampleData?.records) return [];
    return erpSampleData.records
      .map(record => {
        const data = record.data || record;
        return data[fieldName];
      })
      .filter(v => v !== undefined && v !== null)
      .map(v => String(v).substring(0, 50))
      .slice(0, 3);
  };

  // Get sample values for a specific target field from LicenseIQ data
  const getTargetSampleValues = (fieldName: string): string[] => {
    if (!licenseiqSampleData?.records) return [];
    return licenseiqSampleData.records
      .map(record => record[fieldName])
      .filter(v => v !== undefined && v !== null)
      .map(v => String(v).substring(0, 50))
      .slice(0, 3);
  };

  return (
    <MainLayout
      title="AI Master Data Mapping"
      description="Map your ERP field names to LicenseIQ standard fields using AI - Create reusable mapping templates"
      actions={
        <Button
          variant="outline"
          onClick={() => navigate('/erp-catalog')}
          data-testid="button-configure-erp"
        >
          <Settings className="h-4 w-4 mr-2" />
          Configure ERP Catalog
        </Button>
      }
    >
      <div className="space-y-6">
        {/* Breadcrumb Navigation */}
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Button 
            variant="ghost" 
            size="sm" 
            onClick={() => navigate('/erp-hub')}
            className="gap-1 px-2 hover:text-primary"
            data-testid="button-back-to-hub"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to ERP Hub
          </Button>
        </div>

      <Tabs defaultValue="generate" className="w-full">
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="generate" data-testid="tab-generate">Single Mapping</TabsTrigger>
          <TabsTrigger value="batch" data-testid="tab-batch">
            <Sparkles className="h-4 w-4 mr-2" />
            Batch Auto-Map
          </TabsTrigger>
          <TabsTrigger value="saved" data-testid="tab-saved">Saved ({savedMappings.mappings.length})</TabsTrigger>
        </TabsList>

        <TabsContent value="generate" className="space-y-6">
          {/* Step 1: Configuration - Clean single card with all selections */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Settings className="h-5 w-5" />
                Step 1: Configure Mapping
              </CardTitle>
              <CardDescription>
                Select the source ERP entity and target LicenseIQ entity to map between
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {!erpSystemsData?.systems?.length ? (
                <Alert>
                  <AlertCircle className="h-4 w-4" />
                  <AlertTitle>No ERP Systems Configured</AlertTitle>
                  <AlertDescription>
                    Please configure ERP systems in the catalog first.{' '}
                    <Button 
                      variant="link" 
                      className="p-0 h-auto" 
                      onClick={() => navigate('/erp-catalog')}
                    >
                      Go to ERP Catalog
                    </Button>
                  </AlertDescription>
                </Alert>
              ) : (
                <>
                  {/* Source Configuration */}
                  <div className="p-4 rounded-lg border bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800">
                    <div className="flex items-center gap-2 mb-4">
                      <Database className="h-5 w-5 text-orange-600" />
                      <h3 className="font-semibold text-orange-800 dark:text-orange-200">Source (ERP System)</h3>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="erp-system">ERP System *</Label>
                        <Select value={selectedSystemId} onValueChange={(value) => {
                          setSelectedSystemId(value);
                          setSelectedEntityId('');
                        }}>
                          <SelectTrigger id="erp-system" data-testid="select-erp-system">
                            <SelectValue placeholder="Select ERP system..." />
                          </SelectTrigger>
                          <SelectContent>
                            {erpSystemsData.systems.map((system) => (
                              <SelectItem key={system.id} value={system.id}>
                                {system.name} ({system.vendor})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="entity-type">Entity Type *</Label>
                        <Select 
                          value={selectedEntityId} 
                          onValueChange={setSelectedEntityId}
                          disabled={!selectedSystemId || !erpEntitiesData?.entities?.length}
                        >
                          <SelectTrigger id="entity-type" data-testid="select-entity-type">
                            <SelectValue placeholder={
                              !selectedSystemId 
                                ? "Select ERP system first..." 
                                : !erpEntitiesData?.entities?.length
                                  ? "No entities configured"
                                  : "Select entity..."
                            } />
                          </SelectTrigger>
                          <SelectContent>
                            {erpEntitiesData?.entities?.map((entity) => (
                              <SelectItem key={entity.id} value={entity.id}>
                                {entity.name} ({entity.technicalName})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    {selectedEntity && erpFieldsData?.fields && (
                      <p className="mt-3 text-sm text-orange-700 dark:text-orange-300">
                        ✓ {erpFieldsData.fields.length} source fields available from {selectedEntity.name}
                      </p>
                    )}
                  </div>

                  {/* Arrow indicator */}
                  <div className="flex justify-center">
                    <div className="flex items-center gap-2 text-muted-foreground">
                      <div className="h-px w-8 bg-border"></div>
                      <span className="text-xs font-medium">MAPS TO</span>
                      <div className="h-px w-8 bg-border"></div>
                    </div>
                  </div>

                  {/* Target Configuration */}
                  <div className="p-4 rounded-lg border bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800">
                    <div className="flex items-center gap-2 mb-4">
                      <Layers className="h-5 w-5 text-green-600" />
                      <h3 className="font-semibold text-green-800 dark:text-green-200">Target (LicenseIQ Schema)</h3>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2">
                      {/* Left: Entity Selector */}
                      <div className="space-y-2">
                        <Label htmlFor="licenseiq-entity">LicenseIQ Entity *</Label>
                        <Select 
                          value={selectedLicenseiqEntityId} 
                          onValueChange={setSelectedLicenseiqEntityId}
                        >
                          <SelectTrigger id="licenseiq-entity" data-testid="select-licenseiq-entity">
                            <SelectValue placeholder="Select LicenseIQ entity..." />
                          </SelectTrigger>
                          <SelectContent>
                            {licenseiqEntitiesData?.entities?.map((entity) => (
                              <SelectItem key={entity.id} value={entity.id}>
                                {entity.name} ({entity.category})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {selectedLicenseiqEntity && licenseiqFieldsData?.fields && (
                          <p className="text-sm text-green-700 dark:text-green-300">
                            ✓ {licenseiqFieldsData.fields.length} target fields available
                          </p>
                        )}
                      </div>

                      {/* Right: Target Fields Preview */}
                      <div className="space-y-2">
                        <Label className="flex items-center gap-2">
                          <Eye className="h-3.5 w-3.5" />
                          Available Target Fields
                        </Label>
                        <div className="rounded-md border border-green-300 dark:border-green-700 bg-white dark:bg-green-900/50 p-3 max-h-32 overflow-y-auto">
                          {licenseiqFieldsData?.fields && licenseiqFieldsData.fields.length > 0 ? (
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                              {licenseiqFieldsData.fields.slice(0, 12).map((field: any) => (
                                <div key={field.id} className="flex items-center gap-1.5 text-xs">
                                  <span className={`w-1.5 h-1.5 rounded-full ${
                                    field.isRequired ? 'bg-red-500' : 'bg-green-500'
                                  }`} title={field.isRequired ? 'Required' : 'Optional'}></span>
                                  <span className="font-mono text-green-800 dark:text-green-200 truncate" title={field.description || field.fieldName}>
                                    {field.fieldName}
                                  </span>
                                  <span className="text-muted-foreground">({field.dataType})</span>
                                </div>
                              ))}
                              {licenseiqFieldsData.fields.length > 12 && (
                                <p className="col-span-2 text-xs text-muted-foreground italic pt-1">
                                  +{licenseiqFieldsData.fields.length - 12} more fields...
                                </p>
                              )}
                            </div>
                          ) : (
                            <p className="text-xs text-muted-foreground italic">
                              {selectedLicenseiqEntityId ? "Loading fields..." : "Select an entity to preview fields"}
                            </p>
                          )}
                        </div>
                        {licenseiqFieldsData?.fields && (
                          <p className="text-xs text-muted-foreground flex items-center gap-1">
                            <span className="w-1.5 h-1.5 rounded-full bg-red-500"></span> Required
                            <span className="mx-1">|</span>
                            <span className="w-1.5 h-1.5 rounded-full bg-green-500"></span> Optional
                          </p>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Generate Button */}
                  <div className="pt-2">
                    <Button
                      onClick={handleGenerateMapping}
                      disabled={generateMutation.isPending || !sourceSchema || !targetSchema || !selectedEntityId || !selectedLicenseiqEntityId}
                      className="w-full"
                      size="lg"
                      data-testid="button-generate-mapping"
                    >
                      {generateMutation.isPending ? (
                        <>
                          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          Generating AI Mapping...
                        </>
                      ) : (
                        <>
                          <Sparkles className="mr-2 h-4 w-4" />
                          Generate AI Field Mapping
                        </>
                      )}
                    </Button>
                    {(!selectedEntityId || !selectedLicenseiqEntityId) && (
                      <p className="mt-2 text-sm text-muted-foreground text-center">
                        Select both source and target entities to generate mapping
                      </p>
                    )}
                  </div>
                </>
              )}
            </CardContent>
          </Card>

          {/* Mapping Results */}
          {mappingResult && (
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <CheckCircle2 className="h-5 w-5 text-green-500" />
                      Mapping Results
                    </CardTitle>
                    <CardDescription>
                      {mappingResult.mappingResults.length} field mappings generated for {mappingResult.entityType}
                    </CardDescription>
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => {
                      const dataStr = JSON.stringify(mappingResult, null, 2);
                      const blob = new Blob([dataStr], { type: 'application/json' });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a');
                      a.href = url;
                      a.download = `mapping-${mappingResult.entityType}-${Date.now()}.json`;
                      a.click();
                    }}
                    data-testid="button-export-json"
                  >
                    <Download className="mr-2 h-4 w-4" />
                    Export JSON
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {/* Editable notice and sample data toggle */}
                <div className="mb-4 flex items-center justify-between gap-4">
                  <div className="p-3 rounded-lg bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 flex-1">
                    <p className="text-sm text-blue-700 dark:text-blue-300 flex items-center gap-2">
                      <Pencil className="h-4 w-4" />
                      <strong>Editable Mappings:</strong> Click on any dropdown to change the mapping before saving.
                    </p>
                  </div>
                  <Button
                    variant={showSampleData ? "default" : "outline"}
                    size="sm"
                    onClick={() => setShowSampleData(!showSampleData)}
                    className="shrink-0"
                    data-testid="button-toggle-sample-data"
                  >
                    <Eye className="h-4 w-4 mr-2" />
                    {showSampleData ? "Hide Sample Data" : "Show Sample Data"}
                  </Button>
                </div>

                {/* Sample Data Preview Panel */}
                {showSampleData && (
                  <div className="mb-4 grid gap-4 md:grid-cols-2">
                    {/* Source (ERP) Sample Data */}
                    <div className="p-4 rounded-lg border bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800">
                      <h4 className="font-semibold text-sm mb-3 flex items-center gap-2 text-orange-700 dark:text-orange-300">
                        <Database className="h-4 w-4" />
                        Source Sample Data (ERP: {selectedEntity?.name || 'Select Entity'})
                      </h4>
                      {erpSampleData?.records && erpSampleData.records.length > 0 ? (
                        <div className="space-y-2 max-h-48 overflow-y-auto">
                          {getSourceFieldOptions().slice(0, 8).map(field => {
                            const samples = getSourceSampleValues(field);
                            return (
                              <div key={field} className="text-xs">
                                <span className="font-mono font-semibold text-orange-800 dark:text-orange-200">{field}:</span>
                                <span className="ml-2 text-muted-foreground">
                                  {samples.length > 0 ? samples.join(' | ') : <em>No data</em>}
                                </span>
                              </div>
                            );
                          })}
                          {getSourceFieldOptions().length > 8 && (
                            <p className="text-xs text-muted-foreground italic">
                              +{getSourceFieldOptions().length - 8} more fields...
                            </p>
                          )}
                        </div>
                      ) : (
                        <p className="text-xs text-muted-foreground italic">
                          {selectedEntityId ? "Loading sample data..." : "Select an ERP entity to see sample data"}
                        </p>
                      )}
                    </div>

                    {/* Target (LicenseIQ) Sample Data */}
                    <div className="p-4 rounded-lg border bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800">
                      <h4 className="font-semibold text-sm mb-3 flex items-center gap-2 text-green-700 dark:text-green-300">
                        <Layers className="h-4 w-4" />
                        Target Sample Data (LicenseIQ: {selectedLicenseiqEntity?.name || 'Select Entity'})
                      </h4>
                      {licenseiqSampleData?.records && licenseiqSampleData.records.length > 0 ? (
                        <div className="space-y-2 max-h-48 overflow-y-auto">
                          {getTargetFieldOptions().slice(0, 8).map(field => {
                            const samples = getTargetSampleValues(field);
                            return (
                              <div key={field} className="text-xs">
                                <span className="font-mono font-semibold text-green-800 dark:text-green-200">{field}:</span>
                                <span className="ml-2 text-muted-foreground">
                                  {samples.length > 0 ? samples.join(' | ') : <em>No data</em>}
                                </span>
                              </div>
                            );
                          })}
                          {getTargetFieldOptions().length > 8 && (
                            <p className="text-xs text-muted-foreground italic">
                              +{getTargetFieldOptions().length - 8} more fields...
                            </p>
                          )}
                        </div>
                      ) : (
                        <p className="text-xs text-muted-foreground italic">
                          {selectedLicenseiqEntityId ? "Loading sample data..." : "Select a LicenseIQ entity to see sample data"}
                        </p>
                      )}
                    </div>
                  </div>
                )}

                <div className="rounded-lg border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[25%]">Source Field (Your ERP)</TableHead>
                        <TableHead className="w-[25%]">Target Field (LicenseIQ)</TableHead>
                        <TableHead className="w-[25%]">Transformation Rule</TableHead>
                        <TableHead className="w-[15%]">Confidence</TableHead>
                        <TableHead className="w-[10%] text-center">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {editableMappings.map((mapping, idx) => (
                        <TableRow key={idx} data-testid={`row-mapping-${idx}`}>
                          <TableCell>
                            <Select
                              value={mapping.source_field || '__null__'}
                              onValueChange={(value) => updateMappingField(idx, 'source_field', value)}
                            >
                              <SelectTrigger className="font-mono text-sm" data-testid={`select-source-${idx}`}>
                                <SelectValue placeholder="Select source field" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="__null__">
                                  <span className="text-muted-foreground italic">No match (null)</span>
                                </SelectItem>
                                {getSourceFieldOptions().map((field) => (
                                  <SelectItem key={field} value={field}>
                                    {field}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Select
                              value={mapping.target_field || ''}
                              onValueChange={(value) => updateMappingField(idx, 'target_field', value)}
                            >
                              <SelectTrigger className="font-mono text-sm font-semibold" data-testid={`select-target-${idx}`}>
                                <SelectValue placeholder="Select target field" />
                              </SelectTrigger>
                              <SelectContent>
                                {getTargetFieldOptions().map((field) => (
                                  <SelectItem key={field} value={field}>
                                    {field}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Input
                              value={mapping.transformation_rule}
                              onChange={(e) => updateMappingField(idx, 'transformation_rule', e.target.value)}
                              className="text-sm"
                              placeholder="e.g., direct, lowercase"
                              data-testid={`input-transform-${idx}`}
                            />
                          </TableCell>
                          <TableCell>
                            {getConfidenceBadge(mapping.confidence)}
                          </TableCell>
                          <TableCell className="text-center">
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeMapping(idx)}
                              className="h-8 w-8 text-red-500 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950"
                              data-testid={`button-remove-mapping-${idx}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                {/* Add New Mapping Button */}
                <div className="mt-4">
                  <Button
                    variant="outline"
                    onClick={addNewMapping}
                    className="w-full border-dashed"
                    data-testid="button-add-mapping"
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    Add New Field Mapping
                  </Button>
                </div>

                {/* Statistics */}
                <div className="mt-4 grid gap-4 md:grid-cols-4">
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertTitle>Total Mappings</AlertTitle>
                    <AlertDescription>
                      {editableMappings.length} field mappings
                    </AlertDescription>
                  </Alert>
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertTitle>High Confidence</AlertTitle>
                    <AlertDescription>
                      {editableMappings.filter(m => m.confidence >= 90).length} mappings (≥90%)
                    </AlertDescription>
                  </Alert>
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertTitle>Medium Confidence</AlertTitle>
                    <AlertDescription>
                      {editableMappings.filter(m => m.confidence >= 70 && m.confidence < 90).length} mappings (70-89%)
                    </AlertDescription>
                  </Alert>
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertTitle>Requires Review</AlertTitle>
                    <AlertDescription>
                      {editableMappings.filter(m => m.confidence < 70).length} mappings (&lt;70%)
                    </AlertDescription>
                  </Alert>
                </div>

                {/* Inline Save Configuration */}
                <div className="mt-6 p-6 rounded-lg border-2 border-primary/20 bg-primary/5">
                  <div className="mb-4">
                    <h3 className="text-lg font-semibold flex items-center gap-2">
                      <Save className="h-5 w-5" />
                      Save Mapping Configuration
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      Provide a name and optional notes to save this mapping for reuse
                    </p>
                  </div>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="mapping-name">Mapping Name *</Label>
                      <Input
                        id="mapping-name"
                        value={saveMappingName}
                        onChange={(e) => setSaveMappingName(e.target.value)}
                        placeholder="e.g., Customer Master Mapping v1"
                        data-testid="input-mapping-name"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="notes">Notes (Optional)</Label>
                      <Textarea
                        id="notes"
                        value={saveNotes}
                        onChange={(e) => setSaveNotes(e.target.value)}
                        placeholder="Add any notes about this mapping configuration..."
                        rows={3}
                        data-testid="input-mapping-notes"
                      />
                    </div>
                    <Button
                      onClick={handleSaveMapping}
                      disabled={saveMutation.isPending || !saveMappingName}
                      className="w-full"
                      size="lg"
                      data-testid="button-save-mapping"
                    >
                      {saveMutation.isPending ? (
                        <>
                          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          Saving Mapping...
                        </>
                      ) : (
                        <>
                          <Save className="mr-2 h-4 w-4" />
                          Save Mapping
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="batch" className="space-y-6">
          {/* Batch Configuration */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Sparkles className="h-5 w-5 text-purple-500" />
                Batch Auto-Mapping
              </CardTitle>
              <CardDescription>
                Automatically map multiple ERP entities at once using AI - Review and approve suggestions before saving
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* System Selection */}
              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="batch-system">ERP System</Label>
                    <Select value={batchSystemId} onValueChange={setBatchSystemId}>
                      <SelectTrigger id="batch-system" data-testid="select-batch-system">
                        <SelectValue placeholder="Select ERP system..." />
                      </SelectTrigger>
                      <SelectContent>
                        {erpSystemsData?.systems?.map((system) => (
                          <SelectItem key={system.id} value={system.id}>
                            {system.name} (v{system.version})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Entity Multi-Select */}
                {batchSystemId && (
                  <div className="space-y-2">
                    <Label>Select Entities to Map ({batchEntityIds.length} selected)</Label>
                    <Card className="p-4 max-h-64 overflow-y-auto">
                      <div className="space-y-2">
                        {batchErpEntitiesData?.entities?.length === 0 ? (
                          <p className="text-sm text-muted-foreground">No entities available for this system</p>
                        ) : (
                          batchErpEntitiesData?.entities?.map((entity) => (
                            <label key={entity.id} className="flex items-start gap-3 p-2 rounded hover:bg-accent cursor-pointer">
                              <input
                                type="checkbox"
                                checked={batchEntityIds.includes(entity.id)}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setBatchEntityIds([...batchEntityIds, entity.id]);
                                  } else {
                                    setBatchEntityIds(batchEntityIds.filter(id => id !== entity.id));
                                  }
                                }}
                                className="mt-1"
                                data-testid={`checkbox-entity-${entity.id}`}
                              />
                              <div className="flex-1">
                                <div className="font-medium">{entity.name}</div>
                                <div className="text-sm text-muted-foreground">{entity.entityType} - {entity.description || 'No description'}</div>
                              </div>
                            </label>
                          ))
                        )}
                      </div>
                    </Card>
                    <div className="flex gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => setBatchEntityIds(batchErpEntitiesData?.entities?.map(e => e.id) || [])}
                        data-testid="button-select-all-entities"
                      >
                        Select All
                      </Button>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => setBatchEntityIds([])}
                        data-testid="button-deselect-all-entities"
                      >
                        Clear All
                      </Button>
                    </div>
                  </div>
                )}

                {/* Generate Button */}
                <Button
                  onClick={() => {
                    if (batchSystemId && batchEntityIds.length > 0) {
                      batchGenerateMutation.mutate({ erpSystemId: batchSystemId, erpEntityIds: batchEntityIds });
                    }
                  }}
                  disabled={!batchSystemId || batchEntityIds.length === 0 || batchGenerateMutation.isPending}
                  className="w-full"
                  data-testid="button-batch-generate"
                >
                  {batchGenerateMutation.isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Generating Mappings ({batchEntityIds.length} entities)...
                    </>
                  ) : (
                    <>
                      <Sparkles className="h-4 w-4 mr-2" />
                      Generate Batch Mappings ({batchEntityIds.length} entities)
                    </>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>

          {/* Results Table */}
          {batchSuggestions.length > 0 && (
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Mapping Suggestions - Review & Approve</CardTitle>
                    <CardDescription>
                      {selectedSuggestions.size} of {batchSuggestions.length} mappings selected for saving
                    </CardDescription>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const highConf = new Set(batchSuggestions.filter(s => s.confidence >= 90).map(s => s.erpEntityId));
                        setSelectedSuggestions(highConf);
                      }}
                      data-testid="button-select-high-confidence"
                    >
                      Select High Confidence (≥90%)
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setSelectedSuggestions(new Set())}
                      data-testid="button-deselect-all-suggestions"
                    >
                      Clear Selection
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-12">
                          <input
                            type="checkbox"
                            checked={selectedSuggestions.size === batchSuggestions.filter(s => s.licenseiqEntityId).length}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setSelectedSuggestions(new Set(batchSuggestions.filter(s => s.licenseiqEntityId).map(s => s.erpEntityId)));
                              } else {
                                setSelectedSuggestions(new Set());
                              }
                            }}
                            data-testid="checkbox-select-all-suggestions"
                          />
                        </TableHead>
                        <TableHead>ERP Entity</TableHead>
                        <TableHead>LicenseIQ Entity</TableHead>
                        <TableHead>Confidence</TableHead>
                        <TableHead>Fields</TableHead>
                        <TableHead className="w-24">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {batchSuggestions.map((suggestion) => (
                        <>
                          <TableRow key={suggestion.erpEntityId} className={expandedRows.has(suggestion.erpEntityId) ? 'bg-muted/50' : ''}>
                            <TableCell>
                              <input
                                type="checkbox"
                                checked={selectedSuggestions.has(suggestion.erpEntityId)}
                                disabled={!suggestion.licenseiqEntityId}
                                onChange={(e) => {
                                  const newSet = new Set(selectedSuggestions);
                                  if (e.target.checked) {
                                    newSet.add(suggestion.erpEntityId);
                                  } else {
                                    newSet.delete(suggestion.erpEntityId);
                                  }
                                  setSelectedSuggestions(newSet);
                                }}
                                data-testid={`checkbox-suggestion-${suggestion.erpEntityId}`}
                              />
                            </TableCell>
                            <TableCell>
                              <div className="font-medium">{suggestion.erpEntityName}</div>
                              <div className="text-xs text-muted-foreground">{suggestion.erpEntityType}</div>
                            </TableCell>
                            <TableCell>
                              {suggestion.licenseiqEntityId ? (
                                <div className="font-medium">{suggestion.licenseiqEntityName}</div>
                              ) : (
                                <span className="text-sm text-muted-foreground italic">No match found</span>
                              )}
                            </TableCell>
                            <TableCell>
                              <Badge 
                                variant={suggestion.confidence >= 90 ? 'default' : suggestion.confidence >= 70 ? 'secondary' : 'outline'}
                                className={
                                  suggestion.confidence >= 90 ? 'bg-green-500' : 
                                  suggestion.confidence >= 70 ? 'bg-yellow-500' : 
                                  'bg-red-500 text-white'
                                }
                              >
                                {suggestion.confidence}%
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <div className="text-sm">
                                {suggestion.mappedFieldCount}/{suggestion.erpFieldCount} mapped
                              </div>
                            </TableCell>
                            <TableCell>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => {
                                  const newExpanded = new Set(expandedRows);
                                  if (expandedRows.has(suggestion.erpEntityId)) {
                                    newExpanded.delete(suggestion.erpEntityId);
                                  } else {
                                    newExpanded.add(suggestion.erpEntityId);
                                  }
                                  setExpandedRows(newExpanded);
                                }}
                                data-testid={`button-expand-${suggestion.erpEntityId}`}
                              >
                                <Eye className="h-4 w-4" />
                              </Button>
                            </TableCell>
                          </TableRow>
                          {expandedRows.has(suggestion.erpEntityId) && (
                            <TableRow>
                              <TableCell colSpan={6} className="bg-muted/30 p-4">
                                <div className="space-y-4">
                                  <div>
                                    <h4 className="font-medium mb-2">AI Reasoning:</h4>
                                    <p className="text-sm text-muted-foreground">{suggestion.reasoning}</p>
                                  </div>
                                  {suggestion.fieldMappings.length > 0 && (
                                    <div>
                                      <h4 className="font-medium mb-2">Field Mappings ({suggestion.fieldMappings.length}):</h4>
                                      <div className="rounded border bg-background">
                                        <Table>
                                          <TableHeader>
                                            <TableRow>
                                              <TableHead>Source Field (ERP)</TableHead>
                                              <TableHead>Target Field (LicenseIQ)</TableHead>
                                              <TableHead>Transformation</TableHead>
                                              <TableHead className="w-24">Confidence</TableHead>
                                            </TableRow>
                                          </TableHeader>
                                          <TableBody>
                                            {suggestion.fieldMappings.map((fm, idx) => (
                                              <TableRow key={idx}>
                                                <TableCell className="font-mono text-xs">
                                                  {fm.source_field || <span className="text-muted-foreground italic">null</span>}
                                                </TableCell>
                                                <TableCell className="font-mono text-xs">{fm.target_field}</TableCell>
                                                <TableCell className="text-xs">{fm.transformation_rule}</TableCell>
                                                <TableCell>
                                                  <Badge variant={fm.confidence >= 80 ? 'default' : 'secondary'} className="text-xs">
                                                    {fm.confidence}%
                                                  </Badge>
                                                </TableCell>
                                              </TableRow>
                                            ))}
                                          </TableBody>
                                        </Table>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              </TableCell>
                            </TableRow>
                          )}
                        </>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                {/* Save Selected Button */}
                {selectedSuggestions.size > 0 && (
                  <div className="mt-4 flex justify-end gap-2">
                    <Alert className="flex-1">
                      <CheckCircle2 className="h-4 w-4" />
                      <AlertTitle>Ready to Save</AlertTitle>
                      <AlertDescription>
                        {selectedSuggestions.size} mapping{selectedSuggestions.size !== 1 ? 's' : ''} selected. Click Save to add them to your catalog.
                      </AlertDescription>
                    </Alert>
                    <Button
                      onClick={async () => {
                        const selectedMappings = batchSuggestions.filter(s => selectedSuggestions.has(s.erpEntityId));
                        let savedCount = 0;
                        
                        for (const suggestion of selectedMappings) {
                          if (!suggestion.licenseiqEntityId) continue;
                          
                          try {
                            await apiRequest('POST', '/api/mapping/save', {
                              mappingName: `${suggestion.erpEntityName} → ${suggestion.licenseiqEntityName}`,
                              erpSystem: erpSystemsData?.systems?.find(s => s.id === batchSystemId)?.name || '',
                              entityType: suggestion.licenseiqEntityName || '',
                              sourceSchema: suggestion.erpSchema,
                              targetSchema: suggestion.licenseiqSchema,
                              mappingResults: suggestion.fieldMappings,
                              notes: `AI-generated batch mapping with ${suggestion.confidence}% confidence. ${suggestion.reasoning}`,
                            });
                            savedCount++;
                          } catch (error) {
                            console.error(`Failed to save mapping for ${suggestion.erpEntityName}:`, error);
                          }
                        }
                        
                        toast({
                          title: 'Batch Save Complete',
                          description: `Successfully saved ${savedCount} of ${selectedMappings.length} mappings.`,
                        });
                        
                        queryClient.invalidateQueries({ queryKey: ['/api/mapping'] });
                        setBatchSuggestions([]);
                        setSelectedSuggestions(new Set());
                        setBatchEntityIds([]);
                      }}
                      className="shrink-0"
                      data-testid="button-save-selected-mappings"
                    >
                      <Save className="h-4 w-4 mr-2" />
                      Save Selected ({selectedSuggestions.size})
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="saved">
          <Card>
            <CardHeader>
              <CardTitle>Saved Mapping Configurations</CardTitle>
              <CardDescription>
                Previously saved mapping configurations for Oracle ERP integration
              </CardDescription>
            </CardHeader>
            <CardContent>
              {savedMappings.mappings.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <FileJson className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No saved mappings yet. Generate and save your first mapping!</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {savedMappings.mappings.map((mapping) => (
                    <Collapsible
                      key={mapping.id}
                      open={expandedMappingId === mapping.id}
                      onOpenChange={() => setExpandedMappingId(prev => prev === mapping.id ? null : mapping.id)}
                    >
                      <div className="border rounded-lg" data-testid={`row-saved-mapping-${mapping.id}`}>
                        {/* Main Row */}
                        <div className="flex items-center p-4 gap-4">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" data-testid={`button-expand-${mapping.id}`}>
                              {expandedMappingId === mapping.id ? (
                                <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ChevronRight className="h-4 w-4" />
                              )}
                            </Button>
                          </CollapsibleTrigger>
                          
                          <div className="flex-1 min-w-0">
                            <p className="font-semibold truncate">{mapping.mappingName}</p>
                            <p className="text-sm text-muted-foreground">
                              {mapping.erpSystem} | {mapping.mappingResults.length} field mappings
                            </p>
                          </div>
                          
                          <Badge variant="outline">{mapping.entityType}</Badge>
                          
                          <span className="text-sm text-muted-foreground hidden md:block">
                            {formatDateUSA(mapping.createdAt)}
                          </span>
                          
                          <div className="flex gap-2">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setExpandedMappingId(prev => prev === mapping.id ? null : mapping.id)}
                              data-testid={`button-view-${mapping.id}`}
                            >
                              <Eye className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleLoadMapping(mapping)}
                              data-testid={`button-load-${mapping.id}`}
                            >
                              <Upload className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => deleteMutation.mutate(mapping.id)}
                              data-testid={`button-delete-${mapping.id}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                        
                        {/* Inline Mapping Details (replaces dialog) */}
                        <CollapsibleContent>
                          <div className="border-t bg-muted/30 p-4" data-testid={`section-mapping-details-${mapping.id}`}>
                            <div className="flex items-center justify-between mb-4">
                              <h3 className="font-semibold flex items-center gap-2">
                                <FileJson className="h-5 w-5" />
                                Field Mappings
                              </h3>
                              <Button 
                                variant="ghost" 
                                size="sm" 
                                onClick={() => setExpandedMappingId(null)}
                                data-testid={`button-close-${mapping.id}`}
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                            
                            {mapping.notes && (
                              <div className="rounded-lg bg-muted p-4 mb-4">
                                <p className="text-sm"><strong>Notes:</strong> {mapping.notes}</p>
                              </div>
                            )}
                            
                            <div className="rounded-lg border bg-background">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Source Field</TableHead>
                                    <TableHead>Target Field</TableHead>
                                    <TableHead>Transformation</TableHead>
                                    <TableHead>Confidence</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {mapping.mappingResults.map((fieldMapping, idx) => (
                                    <TableRow key={idx}>
                                      <TableCell className="font-mono text-sm">
                                        {fieldMapping.source_field || <span className="text-muted-foreground italic">No match</span>}
                                      </TableCell>
                                      <TableCell className="font-mono text-sm font-semibold">
                                        {fieldMapping.target_field}
                                      </TableCell>
                                      <TableCell className="text-sm">{fieldMapping.transformation_rule}</TableCell>
                                      <TableCell>{getConfidenceBadge(fieldMapping.confidence)}</TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          </div>
                        </CollapsibleContent>
                      </div>
                    </Collapsible>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      </div>
    </MainLayout>
  );
}
