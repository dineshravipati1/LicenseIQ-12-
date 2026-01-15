import { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useLocation, useSearch } from "wouter";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Separator } from "@/components/ui/separator";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { Plus, Edit, Trash2, Database, FileText, Table as TableIcon, Save, Plug, ChevronDown, ChevronRight, ChevronUp, Code, Globe, X, Settings, ExternalLink, ArrowLeft, Filter, RefreshCw, ArrowUpDown } from "lucide-react";
import type { ErpSystem, ErpEntity, ErpField, IntegrationEndpointTemplate } from "@shared/schema";

export default function ErpCatalogPage() {
  const [, navigate] = useLocation();
  const searchString = useSearch();
  const { toast } = useToast();
  
  // Parse tab from URL query params
  const getInitialTab = () => {
    const params = new URLSearchParams(searchString);
    const tab = params.get('tab');
    if (tab && ['systems', 'entities', 'fields', 'data', 'templates'].includes(tab)) {
      return tab;
    }
    return 'systems';
  };
  
  const [activeTab, setActiveTab] = useState(getInitialTab);
  
  // Update tab when URL changes
  useEffect(() => {
    const params = new URLSearchParams(searchString);
    const tab = params.get('tab');
    if (tab && ['systems', 'entities', 'fields', 'data', 'templates'].includes(tab)) {
      setActiveTab(tab);
    }
  }, [searchString]);
  
  const [selectedSystemId, setSelectedSystemId] = useState<string | null>(null);
  const [selectedEntityId, setSelectedEntityId] = useState<string | null>(null);
  
  // Inline form states and form data
  const [showSystemForm, setShowSystemForm] = useState(false);
  const [systemForm, setSystemForm] = useState({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });
  
  const [showEntityForm, setShowEntityForm] = useState(false);
  const [entityForm, setEntityForm] = useState({ systemId: '', name: '', technicalName: '', entityType: 'master_data', description: '', status: 'active' });
  
  const [showFieldForm, setShowFieldForm] = useState(false);
  const [fieldForm, setFieldForm] = useState({ entityId: '', fieldName: '', dataType: 'VARCHAR2', description: '', isPrimaryKey: false, isRequired: false, sampleValues: '' });

  // Entity Data state
  const [dataSystemFilter, setDataSystemFilter] = useState<string>('');
  const [dataEntityFilter, setDataEntityFilter] = useState<string>('');
  const [showDataRecordForm, setShowDataRecordForm] = useState(false);
  const [editingDataRecord, setEditingDataRecord] = useState<any>(null);
  const [dataRecordForm, setDataRecordForm] = useState<Record<string, any>>({});

  // Entity Data - Pagination, Sorting, Filtering
  const [dataPage, setDataPage] = useState(1);
  const [dataPageSize, setDataPageSize] = useState(25);
  const [dataSortColumn, setDataSortColumn] = useState<string>('');
  const [dataSortDirection, setDataSortDirection] = useState<'asc' | 'desc'>('asc');
  const [dataFilters, setDataFilters] = useState<Record<string, string>>({});
  const [showDataFilters, setShowDataFilters] = useState(false);
  
  // Advanced filter types
  type FilterOperator = 'contains' | 'equals' | 'starts_with' | 'ends_with' | 'gt' | 'gte' | 'lt' | 'lte' | 'is_null' | 'not_null';
  interface AdvancedFilter {
    field: string;
    operator: FilterOperator;
    value: string;
  }
  const [advancedFilters, setAdvancedFilters] = useState<AdvancedFilter[]>([]);
  const [filterLogic, setFilterLogic] = useState<'and' | 'or'>('and');
  const [useAdvancedFilters, setUseAdvancedFilters] = useState(false);
  
  const textOperators: { value: FilterOperator; label: string }[] = [
    { value: 'contains', label: 'Contains' },
    { value: 'equals', label: 'Equals' },
    { value: 'starts_with', label: 'Starts with' },
    { value: 'ends_with', label: 'Ends with' },
    { value: 'is_null', label: 'Is empty' },
    { value: 'not_null', label: 'Is not empty' },
  ];
  const numericOperators: { value: FilterOperator; label: string }[] = [
    { value: 'equals', label: 'Equals' },
    { value: 'gt', label: 'Greater than' },
    { value: 'gte', label: 'Greater or equal' },
    { value: 'lt', label: 'Less than' },
    { value: 'lte', label: 'Less or equal' },
    { value: 'is_null', label: 'Is empty' },
    { value: 'not_null', label: 'Is not empty' },
  ];

  // API Templates state
  const [showTemplateForm, setShowTemplateForm] = useState(false);
  const [editingTemplate, setEditingTemplate] = useState<IntegrationEndpointTemplate | null>(null);
  const [expandedTemplateId, setExpandedTemplateId] = useState<string | null>(null);
  const [templateSystemFilter, setTemplateSystemFilter] = useState<string>('');
  const [templateEntityFilter, setTemplateEntityFilter] = useState<string>('');
  const [generationResult, setGenerationResult] = useState<{ templateId: string; createdFields: number; skippedFields: string[]; entityName?: string } | null>(null);
  const [templateForm, setTemplateForm] = useState({
    erpSystemId: '',
    erpEntityId: '',
    operationType: 'list',
    name: '',
    httpMethod: 'GET',
    pathTemplate: '',
    paginationType: 'offset',
    responseDataPath: '',
    responseTotalPath: '',
    expectedResponseTimeMs: 5000,
    requiresCompanyScope: true,
    description: '',
    status: 'active',
  });

  // Fetch ERP Systems
  const { data: systemsData, isLoading: systemsLoading } = useQuery<{ systems: ErpSystem[] }>({
    queryKey: ['/api/erp-systems'],
  });

  // Fetch ERP Entities for selected system
  const { data: entitiesData, isLoading: entitiesLoading } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', selectedSystemId],
    enabled: !!selectedSystemId,
    queryFn: () => fetch(`/api/erp-entities?systemId=${selectedSystemId}`).then(res => res.json()),
  });

  // Fetch ERP Fields for selected entity
  const { data: fieldsData, isLoading: fieldsLoading } = useQuery<{ fields: ErpField[] }>({
    queryKey: ['/api/erp-fields', selectedEntityId],
    enabled: !!selectedEntityId,
    queryFn: () => fetch(`/api/erp-fields?entityId=${selectedEntityId}`).then(res => res.json()),
  });

  // System mutations
  const createSystemMutation = useMutation({
    mutationFn: async (data: typeof systemForm) => apiRequest('POST', '/api/erp-systems', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-systems'] });
      toast({ title: "Success", description: "ERP system created successfully" });
      setShowSystemForm(false);
      setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });
    },
  });

  const deleteSystemMutation = useMutation({
    mutationFn: async (id: string) => apiRequest('DELETE', `/api/erp-systems/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-systems'] });
      toast({ title: "Success", description: "ERP system deleted successfully" });
    },
  });

  const createEntityMutation = useMutation({
    mutationFn: async (data: typeof entityForm) => apiRequest('POST', '/api/erp-entities', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === '/api/erp-entities' });
      toast({ title: "Success", description: "Entity created successfully" });
      setShowEntityForm(false);
      setEntityForm({ systemId: selectedSystemId!, name: '', technicalName: '', entityType: 'master_data', description: '', status: 'active' });
    },
  });

  const deleteEntityMutation = useMutation({
    mutationFn: async (id: string) => apiRequest('DELETE', `/api/erp-entities/${id}`),
    onSuccess: () => {
      // Invalidate all entity queries for all systems
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === '/api/erp-entities' });
      toast({ title: "Success", description: "Entity deleted successfully" });
    },
  });

  const createFieldMutation = useMutation({
    mutationFn: async (data: typeof fieldForm) => apiRequest('POST', '/api/erp-fields', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === '/api/erp-fields' });
      toast({ title: "Success", description: "Field created successfully" });
      setShowFieldForm(false);
      setFieldForm({ entityId: selectedEntityId!, fieldName: '', dataType: 'VARCHAR2', description: '', isPrimaryKey: false, isRequired: false, sampleValues: '' });
    },
  });

  const deleteFieldMutation = useMutation({
    mutationFn: async (id: string) => apiRequest('DELETE', `/api/erp-fields/${id}`),
    onSuccess: () => {
      // Invalidate all field queries for all entities
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === '/api/erp-fields' });
      toast({ title: "Success", description: "Field deleted successfully" });
    },
  });

  // Entity Data queries - entities for the data tab filter
  const { data: dataEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', 'data', dataSystemFilter],
    enabled: !!dataSystemFilter,
    queryFn: () => fetch(`/api/erp-entities?systemId=${dataSystemFilter}`).then(res => res.json()),
  });

  // Entity Data - fields for selected entity
  const { data: dataFieldsData, isLoading: dataFieldsLoading } = useQuery<{ fields: ErpField[] }>({
    queryKey: ['/api/erp-fields', 'data', dataEntityFilter],
    enabled: !!dataEntityFilter,
    queryFn: () => fetch(`/api/erp-fields?entityId=${dataEntityFilter}`).then(res => res.json()),
  });

  // Entity Data - records for selected entity
  const { data: dataRecordsData, isLoading: dataRecordsLoading } = useQuery<{ records: any[] }>({
    queryKey: ['/api/erp-entity-records', dataEntityFilter],
    enabled: !!dataEntityFilter,
    queryFn: () => fetch(`/api/erp-entity-records?entityId=${dataEntityFilter}`).then(res => res.json()),
  });

  // Entity Data mutations
  const createDataRecordMutation = useMutation({
    mutationFn: async (data: { entityId: string; data: Record<string, any> }) => 
      apiRequest('POST', '/api/erp-entity-records', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityFilter] });
      toast({ title: "Success", description: "Record created successfully" });
      setShowDataRecordForm(false);
      setDataRecordForm({});
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to create record", variant: "destructive" });
    },
  });

  const updateDataRecordMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Record<string, any> }) => 
      apiRequest('PATCH', `/api/erp-entity-records/${id}`, { data }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityFilter] });
      toast({ title: "Success", description: "Record updated successfully" });
      setShowDataRecordForm(false);
      setEditingDataRecord(null);
      setDataRecordForm({});
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to update record", variant: "destructive" });
    },
  });

  const deleteDataRecordMutation = useMutation({
    mutationFn: async (id: string) => apiRequest('DELETE', `/api/erp-entity-records/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityFilter] });
      toast({ title: "Success", description: "Record deleted successfully" });
    },
  });

  // API Templates queries
  const { data: templatesData, isLoading: templatesLoading } = useQuery<IntegrationEndpointTemplate[]>({
    queryKey: ['/api/integration-endpoint-templates', templateSystemFilter, templateEntityFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (templateSystemFilter) params.append('erpSystemId', templateSystemFilter);
      if (templateEntityFilter) params.append('entityId', templateEntityFilter);
      const response = await fetch(`/api/integration-endpoint-templates?${params.toString()}`);
      return response.json();
    },
  });

  // Entities for template filter (based on selected system)
  const { data: templateEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', 'templates', templateSystemFilter],
    enabled: !!templateSystemFilter,
    queryFn: () => fetch(`/api/erp-entities?systemId=${templateSystemFilter}`).then(res => res.json()),
  });

  // All entities for name lookup (always fetch)
  const { data: allEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', 'all'],
    queryFn: () => fetch('/api/erp-entities').then(res => res.json()),
  });

  // Entities for template form (based on form's selected system)
  const { data: formEntitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities', 'form', templateForm.erpSystemId],
    enabled: !!templateForm.erpSystemId && showTemplateForm,
    queryFn: () => fetch(`/api/erp-entities?systemId=${templateForm.erpSystemId}`).then(res => res.json()),
  });

  // API Templates mutations
  const createTemplateMutation = useMutation({
    mutationFn: async (data: typeof templateForm) => apiRequest('POST', '/api/integration-endpoint-templates', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => String(query.queryKey[0]).includes('endpoint-templates') });
      toast({ title: "Success", description: "API template created successfully" });
      setShowTemplateForm(false);
      resetTemplateForm();
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to create template", variant: "destructive" });
    },
  });

  const updateTemplateMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => apiRequest('PATCH', `/api/integration-endpoint-templates/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => String(query.queryKey[0]).includes('endpoint-templates') });
      toast({ title: "Success", description: "API template updated successfully" });
      setShowTemplateForm(false);
      setEditingTemplate(null);
      resetTemplateForm();
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to update template", variant: "destructive" });
    },
  });

  const deleteTemplateMutation = useMutation({
    mutationFn: async (id: string) => apiRequest('DELETE', `/api/integration-endpoint-templates/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => String(query.queryKey[0]).includes('endpoint-templates') });
      toast({ title: "Success", description: "API template deleted successfully" });
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to delete template", variant: "destructive" });
    },
  });

  const generateFieldsMutation = useMutation({
    mutationFn: async ({ templateId, entityName }: { templateId: string; entityName: string }) => {
      const result = await apiRequest('POST', `/api/integration-endpoint-templates/${templateId}/generate-entity-fields`, {});
      return { ...result, entityName };
    },
    onSuccess: (data: any) => {
      queryClient.invalidateQueries({ predicate: (query) => String(query.queryKey[0]).includes('erp-fields') });
      setGenerationResult({
        templateId: data.templateId,
        createdFields: data.createdFields || 0,
        skippedFields: data.skippedFields || [],
        entityName: data.entityName
      });
      setExpandedTemplateId(data.templateId);
      toast({ 
        title: "Fields Generated", 
        description: `Created ${data.createdFields || 0} new fields for ${data.entityName}${data.skippedFields?.length > 0 ? ` (${data.skippedFields.length} skipped)` : ''}` 
      });
    },
    onError: (error: any) => {
      toast({ title: "Error", description: error.message || "Failed to generate fields", variant: "destructive" });
    },
  });

  // Helper functions
  const resetTemplateForm = () => {
    setTemplateForm({
      erpSystemId: '',
      erpEntityId: '',
      operationType: 'list',
      name: '',
      httpMethod: 'GET',
      pathTemplate: '',
      paginationType: 'offset',
      responseDataPath: '',
      responseTotalPath: '',
      expectedResponseTimeMs: 5000,
      requiresCompanyScope: true,
      description: '',
      status: 'active',
    });
  };

  const openEditTemplate = (template: IntegrationEndpointTemplate) => {
    setEditingTemplate(template);
    setTemplateForm({
      erpSystemId: template.erpSystemId,
      erpEntityId: template.erpEntityId || '',
      operationType: template.operationType,
      name: template.name,
      httpMethod: template.httpMethod,
      pathTemplate: template.pathTemplate,
      paginationType: template.paginationType || 'offset',
      responseDataPath: template.responseDataPath || '',
      responseTotalPath: template.responseTotalPath || '',
      expectedResponseTimeMs: template.expectedResponseTimeMs || 5000,
      requiresCompanyScope: template.requiresCompanyScope ?? true,
      description: template.description || '',
      status: template.status,
    });
    setShowTemplateForm(true);
  };

  const handleTemplateSubmit = () => {
    const data = {
      ...templateForm,
      erpEntityId: templateForm.erpEntityId || undefined,
      expectedResponseTimeMs: Number(templateForm.expectedResponseTimeMs),
    };
    
    if (editingTemplate) {
      updateTemplateMutation.mutate({ id: editingTemplate.id, data });
    } else {
      createTemplateMutation.mutate(data);
    }
  };

  const selectedSystem = systemsData?.systems?.find(s => s.id === selectedSystemId);
  const selectedEntity = entitiesData?.entities?.find(e => e.id === selectedEntityId);
  const templateEntities = templateEntitiesData?.entities || [];
  const allEntities = allEntitiesData?.entities || [];
  const formEntities = formEntitiesData?.entities || [];
  const templates = templatesData || [];
  
  // Helper to find entity by ID from all entities
  const findEntityById = (entityId: string | null | undefined) => {
    if (!entityId) return undefined;
    return allEntities.find(e => e.id === entityId) || templateEntities.find(e => e.id === entityId);
  };

  return (
    <MainLayout
      title="ERP Catalog Management"
      description="Configure ERP systems, entities, and fields for universal data mapping"
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

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="grid w-full max-w-2xl grid-cols-5">
          <TabsTrigger value="systems" data-testid="tab-systems">
            <Database className="h-4 w-4 mr-2" />
            ERP Systems
          </TabsTrigger>
          <TabsTrigger value="entities" data-testid="tab-entities">
            <TableIcon className="h-4 w-4 mr-2" />
            Entities
          </TabsTrigger>
          <TabsTrigger value="fields" data-testid="tab-fields">
            <FileText className="h-4 w-4 mr-2" />
            Fields
          </TabsTrigger>
          <TabsTrigger value="data" data-testid="tab-entity-data">
            <TableIcon className="h-4 w-4 mr-2" />
            Entity Data
          </TabsTrigger>
          <TabsTrigger value="templates" data-testid="tab-templates">
            <Plug className="h-4 w-4 mr-2" />
            API Templates
          </TabsTrigger>
        </TabsList>

        {/* Systems Tab */}
        <TabsContent value="systems" className="mt-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>ERP Systems</CardTitle>
                  <CardDescription>
                    Manage ERP platforms (Oracle, SAP, NetSuite, custom systems)
                  </CardDescription>
                </div>
                <Button onClick={() => setShowSystemForm(true)} data-testid="button-add-system">
                  <Plus className="h-4 w-4 mr-2" />
                  Add System
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {/* Inline System Form */}
              {showSystemForm && (
                <Card className="border-2 border-primary/20 bg-primary/5 mb-4">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      <span>Add ERP System</span>
                      <Button variant="ghost" size="sm" onClick={() => {setShowSystemForm(false); setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });}}>✕</Button>
                    </CardTitle>
                    <CardDescription>Configure a new ERP platform for data mapping</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div>
                      <Label htmlFor="name">System Name</Label>
                      <Input id="name" placeholder="e.g., Oracle EBS, SAP S/4HANA" value={systemForm.name} onChange={(e) => setSystemForm({...systemForm, name: e.target.value})} data-testid="input-system-name" />
                    </div>
                    <div>
                      <Label htmlFor="vendor">Vendor</Label>
                      <Input id="vendor" placeholder="e.g., Oracle, SAP, Microsoft" value={systemForm.vendor} onChange={(e) => setSystemForm({...systemForm, vendor: e.target.value})} data-testid="input-vendor" />
                    </div>
                    <div>
                      <Label htmlFor="version">Version (Optional)</Label>
                      <Input id="version" placeholder="e.g., R12.2, 2023" value={systemForm.version} onChange={(e) => setSystemForm({...systemForm, version: e.target.value})} data-testid="input-version" />
                    </div>
                    <div>
                      <Label htmlFor="category">Category</Label>
                      <Select value={systemForm.category} onValueChange={(value) => setSystemForm({...systemForm, category: value})}>
                        <SelectTrigger data-testid="select-category"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="enterprise">Enterprise</SelectItem>
                          <SelectItem value="cloud">Cloud</SelectItem>
                          <SelectItem value="custom">Custom</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="description">Description</Label>
                      <Textarea id="description" placeholder="Brief description of the ERP system" value={systemForm.description} onChange={(e) => setSystemForm({...systemForm, description: e.target.value})} rows={2} data-testid="input-description" />
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" onClick={() => {setShowSystemForm(false); setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });}} className="flex-1">Cancel</Button>
                      <Button onClick={() => createSystemMutation.mutate(systemForm)} disabled={!systemForm.name || !systemForm.vendor || createSystemMutation.isPending} className="flex-1" data-testid="button-save-system">
                        <Save className="h-4 w-4 mr-2" />
                        {createSystemMutation.isPending ? 'Saving...' : 'Save System'}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}

              {systemsLoading ? (
                <div className="text-center py-8 text-muted-foreground">Loading systems...</div>
              ) : !systemsData?.systems?.length ? (
                <div className="text-center py-8 text-muted-foreground">No ERP systems configured yet</div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Vendor</TableHead>
                      <TableHead>Version</TableHead>
                      <TableHead>Category</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {systemsData.systems.map((system) => (
                      <TableRow key={system.id} data-testid={`row-system-${system.id}`}>
                        <TableCell className="font-medium">{system.name}</TableCell>
                        <TableCell>{system.vendor}</TableCell>
                        <TableCell>{system.version || '-'}</TableCell>
                        <TableCell>
                          <Badge variant={system.category === 'cloud' ? 'default' : 'secondary'}>
                            {system.category}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={system.status === 'active' ? 'default' : 'secondary'}>
                            {system.status}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => {
                              setSelectedSystemId(system.id);
                              setActiveTab("entities");
                            }}
                            data-testid={`button-view-entities-${system.id}`}
                          >
                            View Entities
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteSystemMutation.mutate(system.id)}
                            data-testid={`button-delete-system-${system.id}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Entities Tab */}
        <TabsContent value="entities" className="mt-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>ERP Entities</CardTitle>
                  <CardDescription>
                    {selectedSystemId 
                      ? `Entities for ${selectedSystem?.name}` 
                      : 'Select an ERP system to view entities'}
                  </CardDescription>
                </div>
                {selectedSystemId && (
                  <Button onClick={() => {setEntityForm({...entityForm, systemId: selectedSystemId}); setShowEntityForm(true);}} data-testid="button-add-entity">
                    <Plus className="h-4 w-4 mr-2" />
                    Add Entity
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent>
              {/* Inline Entity Form */}
              {showEntityForm && selectedSystemId && (
                <Card className="border-2 border-primary/20 bg-primary/5 mb-4">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      <span>Add Entity</span>
                      <Button variant="ghost" size="sm" onClick={() => {setShowEntityForm(false); setEntityForm({ systemId: selectedSystemId, name: '', technicalName: '', entityType: 'master_data', description: '', status: 'active' });}}>✕</Button>
                    </CardTitle>
                    <CardDescription>Define a table or entity for this ERP system</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div>
                      <Label>Entity Name</Label>
                      <Input placeholder="e.g., Customers, Items, Invoices" value={entityForm.name} onChange={(e) => setEntityForm({...entityForm, name: e.target.value})} data-testid="input-entity-name" />
                    </div>
                    <div>
                      <Label>Technical Name</Label>
                      <Input placeholder="e.g., CUSTOMERS, AR_CUSTOMERS" value={entityForm.technicalName} onChange={(e) => setEntityForm({...entityForm, technicalName: e.target.value})} data-testid="input-technical-name" />
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" onClick={() => {setShowEntityForm(false); setEntityForm({ systemId: selectedSystemId, name: '', technicalName: '', entityType: 'master_data', description: '', status: 'active' });}} className="flex-1">Cancel</Button>
                      <Button onClick={() => createEntityMutation.mutate(entityForm)} disabled={!entityForm.name || !entityForm.technicalName || createEntityMutation.isPending} className="flex-1" data-testid="button-save-entity">
                        <Save className="h-4 w-4 mr-2" />
                        {createEntityMutation.isPending ? 'Saving...' : 'Save Entity'}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}
              {!selectedSystemId ? (
                <div className="text-center py-8">
                  <p className="text-muted-foreground mb-4">Please select an ERP system first</p>
                  <Button onClick={() => setActiveTab("systems")} data-testid="button-go-to-systems">
                    Go to Systems Tab
                  </Button>
                </div>
              ) : entitiesLoading ? (
                <div className="text-center py-8 text-muted-foreground">Loading entities...</div>
              ) : !entitiesData?.entities?.length ? (
                <div className="text-center py-8 text-muted-foreground">
                  No entities configured for this system yet
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Technical Name</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {entitiesData.entities.map((entity) => (
                      <TableRow key={entity.id} data-testid={`row-entity-${entity.id}`}>
                        <TableCell className="font-medium">{entity.name}</TableCell>
                        <TableCell className="font-mono text-sm">{entity.technicalName}</TableCell>
                        <TableCell>
                          <Badge>{entity.entityType}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={entity.status === 'active' ? 'default' : 'secondary'}>
                            {entity.status}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => {
                              setSelectedEntityId(entity.id);
                              setActiveTab("fields");
                            }}
                            data-testid={`button-view-fields-${entity.id}`}
                          >
                            View Fields
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteEntityMutation.mutate(entity.id)}
                            data-testid={`button-delete-entity-${entity.id}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Fields Tab */}
        <TabsContent value="fields" className="mt-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Entity Fields</CardTitle>
                  <CardDescription>
                    {selectedEntityId 
                      ? `Fields for ${selectedEntity?.name}` 
                      : 'Select an entity to view fields'}
                  </CardDescription>
                </div>
                {selectedEntityId && (
                  <Button onClick={() => {setFieldForm({...fieldForm, entityId: selectedEntityId}); setShowFieldForm(true);}} data-testid="button-add-field">
                    <Plus className="h-4 w-4 mr-2" />
                    Add Field
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent>
              {/* Inline Field Form */}
              {showFieldForm && selectedEntityId && (
                <Card className="border-2 border-primary/20 bg-primary/5 mb-4">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      <span>Add Field</span>
                      <Button variant="ghost" size="sm" onClick={() => {setShowFieldForm(false); setFieldForm({ entityId: selectedEntityId, fieldName: '', dataType: 'VARCHAR2', description: '', isPrimaryKey: false, isRequired: false, sampleValues: '' });}}>✕</Button>
                    </CardTitle>
                    <CardDescription>Define a field/column for this entity</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div>
                      <Label>Field Name</Label>
                      <Input placeholder="e.g., CUSTOMER_ID, PARTY_NAME" value={fieldForm.fieldName} onChange={(e) => setFieldForm({...fieldForm, fieldName: e.target.value})} data-testid="input-field-name" />
                    </div>
                    <div>
                      <Label>Data Type</Label>
                      <Select value={fieldForm.dataType} onValueChange={(value) => setFieldForm({...fieldForm, dataType: value})}>
                        <SelectTrigger data-testid="select-data-type"><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="VARCHAR2">VARCHAR2</SelectItem>
                          <SelectItem value="NUMBER">NUMBER</SelectItem>
                          <SelectItem value="DATE">DATE</SelectItem>
                          <SelectItem value="TIMESTAMP">TIMESTAMP</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="flex gap-2">
                      <Button variant="outline" onClick={() => {setShowFieldForm(false); setFieldForm({ entityId: selectedEntityId, fieldName: '', dataType: 'VARCHAR2', description: '', isPrimaryKey: false, isRequired: false, sampleValues: '' });}} className="flex-1">Cancel</Button>
                      <Button onClick={() => createFieldMutation.mutate(fieldForm)} disabled={!fieldForm.fieldName || createFieldMutation.isPending} className="flex-1" data-testid="button-save-field">
                        <Save className="h-4 w-4 mr-2" />
                        {createFieldMutation.isPending ? 'Saving...' : 'Save Field'}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}
              {!selectedEntityId ? (
                <div className="text-center py-8">
                  <p className="text-muted-foreground mb-4">Please select an entity first</p>
                  <Button onClick={() => setActiveTab("entities")} data-testid="button-go-to-entities">
                    Go to Entities Tab
                  </Button>
                </div>
              ) : fieldsLoading ? (
                <div className="text-center py-8 text-muted-foreground">Loading fields...</div>
              ) : !fieldsData?.fields?.length ? (
                <div className="text-center py-8 text-muted-foreground">
                  No fields configured for this entity yet
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Field Name</TableHead>
                      <TableHead>Data Type</TableHead>
                      <TableHead>Primary Key</TableHead>
                      <TableHead>Required</TableHead>
                      <TableHead>Sample Values</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {fieldsData.fields.map((field) => (
                      <TableRow key={field.id} data-testid={`row-field-${field.id}`}>
                        <TableCell className="font-mono font-medium">{field.fieldName}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{field.dataType}</Badge>
                        </TableCell>
                        <TableCell>
                          {field.isPrimaryKey ? <Badge>PK</Badge> : '-'}
                        </TableCell>
                        <TableCell>
                          {field.isRequired ? <Badge variant="secondary">Required</Badge> : '-'}
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {field.sampleValues || '-'}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => deleteFieldMutation.mutate(field.id)}
                            data-testid={`button-delete-field-${field.id}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Entity Data Tab */}
        <TabsContent value="data" className="mt-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <TableIcon className="h-5 w-5" />
                    Entity Data Records
                  </CardTitle>
                  <CardDescription>
                    View and manage actual data records for ERP entities
                  </CardDescription>
                </div>
                {dataEntityFilter && (
                  <Button 
                    onClick={() => {
                      setEditingDataRecord(null);
                      setDataRecordForm({});
                      setShowDataRecordForm(true);
                    }} 
                    data-testid="button-add-record"
                  >
                    <Plus className="h-4 w-4 mr-2" />
                    Add Record
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Filters */}
              <div className="flex gap-4 items-end">
                <div className="flex-1 max-w-xs">
                  <Label>ERP System</Label>
                  <Select value={dataSystemFilter || 'all'} onValueChange={(v) => { setDataSystemFilter(v === 'all' ? '' : v); setDataEntityFilter(''); }}>
                    <SelectTrigger data-testid="select-data-system">
                      <SelectValue placeholder="Select System" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Select System...</SelectItem>
                      {systemsData?.systems?.map(sys => (
                        <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex-1 max-w-xs">
                  <Label>Entity</Label>
                  <Select 
                    value={dataEntityFilter || 'all'} 
                    onValueChange={(v) => { 
                      setDataEntityFilter(v === 'all' ? '' : v); 
                      setShowDataRecordForm(false);
                      // Reset filtering, sorting, pagination when switching entities
                      setDataPage(1);
                      setDataFilters({});
                      setAdvancedFilters([]);
                      setDataSortColumn('');
                      setDataSortDirection('asc');
                    }}
                    disabled={!dataSystemFilter}
                  >
                    <SelectTrigger data-testid="select-data-entity">
                      <SelectValue placeholder="Select Entity" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Select Entity...</SelectItem>
                      {dataEntitiesData?.entities?.map(ent => (
                        <SelectItem key={ent.id} value={ent.id}>{ent.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Loading State */}
              {(dataFieldsLoading || dataRecordsLoading) && (
                <div className="text-center py-8 text-muted-foreground">
                  <div className="animate-spin h-6 w-6 border-2 border-primary border-t-transparent rounded-full mx-auto mb-2"></div>
                  <p>Loading...</p>
                </div>
              )}

              {/* Empty State - No Entity Selected */}
              {!dataEntityFilter && !dataFieldsLoading && (
                <div className="text-center py-8 text-muted-foreground border rounded-lg">
                  <TableIcon className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>Select an ERP system and entity to view its data records</p>
                </div>
              )}

              {/* Record Form */}
              {showDataRecordForm && dataEntityFilter && dataFieldsData?.fields && !dataFieldsLoading && (
                <Card className="border-2 border-primary/20 bg-primary/5">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between">
                      <span>{editingDataRecord ? 'Edit Record' : 'Add New Record'}</span>
                      <Button variant="ghost" size="sm" onClick={() => { setShowDataRecordForm(false); setEditingDataRecord(null); setDataRecordForm({}); }}>
                        <X className="h-4 w-4" />
                      </Button>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      {dataFieldsData.fields.map((field) => (
                        <div key={field.id} className="space-y-1">
                          <Label>
                            {field.fieldName}
                            {field.isRequired && <span className="text-destructive ml-1">*</span>}
                          </Label>
                          <Input
                            placeholder={field.description || field.fieldName}
                            value={dataRecordForm[field.fieldName] || ''}
                            onChange={(e) => setDataRecordForm(prev => ({ ...prev, [field.fieldName]: e.target.value }))}
                            data-testid={`input-record-${field.fieldName}`}
                          />
                          {field.dataType && (
                            <p className="text-xs text-muted-foreground">{field.dataType}</p>
                          )}
                        </div>
                      ))}
                    </div>
                    <div className="flex justify-end gap-2 pt-4">
                      <Button 
                        variant="outline" 
                        onClick={() => { setShowDataRecordForm(false); setEditingDataRecord(null); setDataRecordForm({}); }}
                        data-testid="button-cancel-record"
                      >
                        Cancel
                      </Button>
                      <Button 
                        onClick={() => {
                          if (editingDataRecord) {
                            updateDataRecordMutation.mutate({ id: editingDataRecord.id, data: dataRecordForm });
                          } else {
                            createDataRecordMutation.mutate({ entityId: dataEntityFilter, data: dataRecordForm });
                          }
                        }}
                        disabled={createDataRecordMutation.isPending || updateDataRecordMutation.isPending}
                        data-testid="button-save-record"
                      >
                        {(createDataRecordMutation.isPending || updateDataRecordMutation.isPending) ? 'Saving...' : 'Save Record'}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Records Table with Filtering, Sorting, Pagination */}
              {dataEntityFilter && dataFieldsData?.fields && dataRecordsData?.records && !dataFieldsLoading && !dataRecordsLoading && (() => {
                // Client-side filtering
                const applyFilter = (value: string, filter: AdvancedFilter): boolean => {
                  const v = (value || '').toString().toLowerCase();
                  const f = filter.value.toLowerCase();
                  switch (filter.operator) {
                    case 'contains': return v.includes(f);
                    case 'equals': return v === f;
                    case 'starts_with': return v.startsWith(f);
                    case 'ends_with': return v.endsWith(f);
                    case 'gt': return parseFloat(value) > parseFloat(filter.value);
                    case 'gte': return parseFloat(value) >= parseFloat(filter.value);
                    case 'lt': return parseFloat(value) < parseFloat(filter.value);
                    case 'lte': return parseFloat(value) <= parseFloat(filter.value);
                    case 'is_null': return !value || value === '';
                    case 'not_null': return !!value && value !== '';
                    default: return true;
                  }
                };

                let filteredRecords = [...dataRecordsData.records];
                
                // Apply simple filters
                if (!useAdvancedFilters) {
                  Object.entries(dataFilters).forEach(([field, filterValue]) => {
                    if (filterValue) {
                      filteredRecords = filteredRecords.filter(r => 
                        (r.data?.[field] || '').toString().toLowerCase().includes(filterValue.toLowerCase())
                      );
                    }
                  });
                } else {
                  // Apply advanced filters
                  if (advancedFilters.length > 0) {
                    filteredRecords = filteredRecords.filter(record => {
                      const results = advancedFilters.map(f => applyFilter(record.data?.[f.field], f));
                      return filterLogic === 'and' ? results.every(Boolean) : results.some(Boolean);
                    });
                  }
                }

                // Apply sorting
                if (dataSortColumn) {
                  filteredRecords.sort((a, b) => {
                    const aVal = a.data?.[dataSortColumn] || '';
                    const bVal = b.data?.[dataSortColumn] || '';
                    const compare = aVal.toString().localeCompare(bVal.toString(), undefined, { numeric: true });
                    return dataSortDirection === 'asc' ? compare : -compare;
                  });
                }

                // Calculate pagination
                const totalRecords = filteredRecords.length;
                const totalPages = Math.ceil(totalRecords / dataPageSize);
                const startIndex = (dataPage - 1) * dataPageSize;
                const paginatedRecords = filteredRecords.slice(startIndex, startIndex + dataPageSize);
                const activeFilterCount = useAdvancedFilters 
                  ? advancedFilters.length 
                  : Object.values(dataFilters).filter(v => v).length;

                return (
                  <div className="space-y-4">
                    {/* Toolbar */}
                    <div className="flex flex-wrap items-center justify-between gap-4">
                      <div className="flex items-center gap-3">
                        <p className="text-sm text-muted-foreground" data-testid="text-record-count">
                          {totalRecords > 0 
                            ? `Showing ${startIndex + 1}-${Math.min(startIndex + dataPageSize, totalRecords)} of ${totalRecords} records`
                            : 'No records found'}
                        </p>
                        <Button 
                          variant={showDataFilters ? 'secondary' : 'outline'} 
                          size="sm" 
                          onClick={() => setShowDataFilters(!showDataFilters)}
                          data-testid="button-toggle-filters"
                        >
                          <Filter className="h-4 w-4 mr-1" />
                          Filters {activeFilterCount > 0 && `(${activeFilterCount})`}
                        </Button>
                        {activeFilterCount > 0 && (
                          <Button 
                            variant="ghost" 
                            size="sm" 
                            onClick={() => { 
                              setDataFilters({}); 
                              setAdvancedFilters([]);
                              setDataPage(1); 
                            }}
                            data-testid="button-clear-filters"
                          >
                            <X className="h-4 w-4 mr-1" />
                            Clear
                          </Button>
                        )}
                      </div>
                      <div className="flex items-center gap-2">
                        <Label className="text-sm text-muted-foreground">Per page:</Label>
                        <Select value={dataPageSize.toString()} onValueChange={(v) => { setDataPageSize(parseInt(v)); setDataPage(1); }}>
                          <SelectTrigger className="w-[80px]" data-testid="select-page-size">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="10">10</SelectItem>
                            <SelectItem value="25">25</SelectItem>
                            <SelectItem value="50">50</SelectItem>
                            <SelectItem value="100">100</SelectItem>
                          </SelectContent>
                        </Select>
                        <Button 
                          variant="outline" 
                          size="sm" 
                          onClick={() => queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityFilter] })}
                          data-testid="button-refresh-data"
                        >
                          <RefreshCw className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>

                    {/* Filter Panel */}
                    {showDataFilters && (
                      <div className="border rounded-lg p-4 bg-muted/30">
                        <div className="flex items-center justify-between mb-3">
                          <div className="flex items-center gap-2">
                            <Filter className="h-4 w-4 text-muted-foreground" />
                            <span className="font-medium text-sm">Filters</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Label className="text-xs text-muted-foreground">Mode:</Label>
                            <Button
                              variant={useAdvancedFilters ? 'outline' : 'default'}
                              size="sm"
                              onClick={() => { setUseAdvancedFilters(false); setAdvancedFilters([]); }}
                              className="h-7 text-xs"
                              data-testid="button-simple-filters"
                            >
                              Simple
                            </Button>
                            <Button
                              variant={useAdvancedFilters ? 'default' : 'outline'}
                              size="sm"
                              onClick={() => { setUseAdvancedFilters(true); setDataFilters({}); }}
                              className="h-7 text-xs"
                              data-testid="button-advanced-filters"
                            >
                              Advanced
                            </Button>
                          </div>
                        </div>
                        
                        {/* Simple Filters */}
                        {!useAdvancedFilters && (
                          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                            {dataFieldsData.fields.slice(0, 8).map((field) => (
                              <div key={field.id} className="space-y-1">
                                <Label className="text-xs text-muted-foreground">{field.fieldName}</Label>
                                <Input
                                  placeholder="Contains..."
                                  value={dataFilters[field.fieldName] || ''}
                                  onChange={(e) => {
                                    setDataFilters(prev => ({ ...prev, [field.fieldName]: e.target.value }));
                                    setDataPage(1);
                                  }}
                                  className="h-8 text-sm"
                                  data-testid={`input-filter-${field.fieldName}`}
                                />
                              </div>
                            ))}
                          </div>
                        )}
                        
                        {/* Advanced Filters */}
                        {useAdvancedFilters && (
                          <div className="space-y-3">
                            <div className="flex items-center gap-3 pb-2 border-b">
                              <Label className="text-xs text-muted-foreground">Combine filters with:</Label>
                              <div className="flex items-center gap-1">
                                <Button
                                  variant={filterLogic === 'and' ? 'default' : 'outline'}
                                  size="sm"
                                  onClick={() => setFilterLogic('and')}
                                  className="h-6 text-xs px-3"
                                  data-testid="button-logic-and"
                                >
                                  AND
                                </Button>
                                <Button
                                  variant={filterLogic === 'or' ? 'default' : 'outline'}
                                  size="sm"
                                  onClick={() => setFilterLogic('or')}
                                  className="h-6 text-xs px-3"
                                  data-testid="button-logic-or"
                                >
                                  OR
                                </Button>
                              </div>
                            </div>
                            
                            {advancedFilters.map((filter, idx) => (
                              <div key={idx} className="flex items-center gap-2">
                                <Select value={filter.field} onValueChange={(v) => {
                                  const newFilters = [...advancedFilters];
                                  newFilters[idx].field = v;
                                  setAdvancedFilters(newFilters);
                                }}>
                                  <SelectTrigger className="w-[180px]" data-testid={`select-filter-field-${idx}`}>
                                    <SelectValue placeholder="Select field" />
                                  </SelectTrigger>
                                  <SelectContent>
                                    {dataFieldsData.fields.map((f) => (
                                      <SelectItem key={f.id} value={f.fieldName}>{f.fieldName}</SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                                <Select value={filter.operator} onValueChange={(v: FilterOperator) => {
                                  const newFilters = [...advancedFilters];
                                  newFilters[idx].operator = v;
                                  setAdvancedFilters(newFilters);
                                }}>
                                  <SelectTrigger className="w-[150px]" data-testid={`select-filter-operator-${idx}`}>
                                    <SelectValue />
                                  </SelectTrigger>
                                  <SelectContent>
                                    {textOperators.map((op) => (
                                      <SelectItem key={op.value} value={op.value}>{op.label}</SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                                {!['is_null', 'not_null'].includes(filter.operator) && (
                                  <Input
                                    value={filter.value}
                                    onChange={(e) => {
                                      const newFilters = [...advancedFilters];
                                      newFilters[idx].value = e.target.value;
                                      setAdvancedFilters(newFilters);
                                      setDataPage(1);
                                    }}
                                    placeholder="Value"
                                    className="w-[200px]"
                                    data-testid={`input-filter-value-${idx}`}
                                  />
                                )}
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => {
                                    setAdvancedFilters(advancedFilters.filter((_, i) => i !== idx));
                                    setDataPage(1);
                                  }}
                                  data-testid={`button-remove-filter-${idx}`}
                                >
                                  <X className="h-4 w-4" />
                                </Button>
                              </div>
                            ))}
                            
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setAdvancedFilters([...advancedFilters, { field: dataFieldsData.fields[0]?.fieldName || '', operator: 'contains', value: '' }])}
                              data-testid="button-add-filter"
                            >
                              <Plus className="h-4 w-4 mr-1" />
                              Add Filter
                            </Button>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Data Table */}
                    <div className="border rounded-lg overflow-x-auto max-w-full">
                      <Table className="min-w-max">
                        <TableHeader>
                          <TableRow>
                            {dataFieldsData.fields.map((field) => (
                              <TableHead 
                                key={field.id} 
                                className="whitespace-nowrap px-3 cursor-pointer hover:bg-muted/50"
                                onClick={() => {
                                  if (dataSortColumn === field.fieldName) {
                                    setDataSortDirection(dataSortDirection === 'asc' ? 'desc' : 'asc');
                                  } else {
                                    setDataSortColumn(field.fieldName);
                                    setDataSortDirection('asc');
                                  }
                                }}
                                data-testid={`header-${field.fieldName}`}
                              >
                                <div className="flex items-center gap-1">
                                  {field.fieldName}
                                  {field.isPrimaryKey && <span className="text-xs text-primary">(PK)</span>}
                                  {dataSortColumn === field.fieldName && (
                                    dataSortDirection === 'asc' 
                                      ? <ChevronUp className="h-4 w-4" />
                                      : <ChevronDown className="h-4 w-4" />
                                  )}
                                </div>
                              </TableHead>
                            ))}
                            <TableHead className="text-right sticky right-0 bg-background">Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {paginatedRecords.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={dataFieldsData.fields.length + 1} className="text-center text-muted-foreground py-8">
                                {activeFilterCount > 0 ? 'No records match your filters.' : 'No records found. Click "Add Record" to create one.'}
                              </TableCell>
                            </TableRow>
                          ) : (
                            paginatedRecords.map((record: any) => (
                              <TableRow key={record.id} data-testid={`row-record-${record.id}`}>
                                {dataFieldsData.fields.map((field) => (
                                  <TableCell key={field.id} className="whitespace-nowrap px-3 max-w-xs truncate" title={record.data?.[field.fieldName] || ''}>
                                    {record.data?.[field.fieldName] || '-'}
                                  </TableCell>
                                ))}
                                <TableCell className="text-right sticky right-0 bg-background">
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => {
                                      setEditingDataRecord(record);
                                      setDataRecordForm(record.data || {});
                                      setShowDataRecordForm(true);
                                    }}
                                    data-testid={`button-edit-record-${record.id}`}
                                  >
                                    <Edit className="h-4 w-4" />
                                  </Button>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => deleteDataRecordMutation.mutate(record.id)}
                                    data-testid={`button-delete-record-${record.id}`}
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </TableCell>
                              </TableRow>
                            ))
                          )}
                        </TableBody>
                      </Table>
                    </div>

                    {/* Pagination */}
                    {totalPages > 1 && (
                      <div className="flex items-center justify-between">
                        <p className="text-sm text-muted-foreground">
                          Page {dataPage} of {totalPages}
                        </p>
                        <div className="flex items-center gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(1)}
                            disabled={dataPage === 1}
                            data-testid="button-first-page"
                          >
                            First
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(p => Math.max(1, p - 1))}
                            disabled={dataPage === 1}
                            data-testid="button-prev-page"
                          >
                            Previous
                          </Button>
                          <div className="flex items-center gap-1">
                            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                              let pageNum: number;
                              if (totalPages <= 5) {
                                pageNum = i + 1;
                              } else if (dataPage <= 3) {
                                pageNum = i + 1;
                              } else if (dataPage >= totalPages - 2) {
                                pageNum = totalPages - 4 + i;
                              } else {
                                pageNum = dataPage - 2 + i;
                              }
                              return (
                                <Button
                                  key={pageNum}
                                  variant={dataPage === pageNum ? 'default' : 'outline'}
                                  size="sm"
                                  onClick={() => setDataPage(pageNum)}
                                  className="w-8 h-8 p-0"
                                  data-testid={`button-page-${pageNum}`}
                                >
                                  {pageNum}
                                </Button>
                              );
                            })}
                          </div>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(p => Math.min(totalPages, p + 1))}
                            disabled={dataPage === totalPages}
                            data-testid="button-next-page"
                          >
                            Next
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(totalPages)}
                            disabled={dataPage === totalPages}
                            data-testid="button-last-page"
                          >
                            Last
                          </Button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })()}

              {/* No Fields Message */}
              {dataEntityFilter && dataFieldsData?.fields?.length === 0 && !dataFieldsLoading && (
                <div className="text-center py-8 text-muted-foreground border rounded-lg">
                  <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
                  <p>No fields defined for this entity.</p>
                  <p className="text-sm">Add fields in the Fields tab first.</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* API Templates Tab */}
        <TabsContent value="templates" className="mt-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <Plug className="h-5 w-5" />
                    API Endpoint Templates
                  </CardTitle>
                  <CardDescription>
                    Configure API endpoints for each ERP entity operation (list, get, create, update, delete)
                  </CardDescription>
                </div>
                <Button onClick={() => { 
                  setEditingTemplate(null); 
                  resetTemplateForm(); 
                  if (templateSystemFilter) {
                    setTemplateForm(prev => ({ ...prev, erpSystemId: templateSystemFilter }));
                  }
                  setShowTemplateForm(true); 
                }} data-testid="button-add-template">
                  <Plus className="h-4 w-4 mr-2" />
                  Add Template
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Filters */}
              <div className="flex gap-4 items-end">
                <div className="flex-1 max-w-xs">
                  <Label>ERP System</Label>
                  <Select value={templateSystemFilter || 'all'} onValueChange={(v) => { setTemplateSystemFilter(v === 'all' ? '' : v); setTemplateEntityFilter(''); }}>
                    <SelectTrigger data-testid="select-template-system">
                      <SelectValue placeholder="All Systems" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Systems</SelectItem>
                      {systemsData?.systems?.map(sys => (
                        <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex-1 max-w-xs">
                  <Label>Entity</Label>
                  <Select 
                    value={templateEntityFilter || 'all'} 
                    onValueChange={(v) => setTemplateEntityFilter(v === 'all' ? '' : v)}
                    disabled={!templateSystemFilter}
                  >
                    <SelectTrigger data-testid="select-template-entity">
                      <SelectValue placeholder="All Entities" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Entities</SelectItem>
                      {templateEntities.map(ent => (
                        <SelectItem key={ent.id} value={ent.id}>{ent.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Inline Template Form */}
              {showTemplateForm && (
                <Card className="border-2 border-primary/20 bg-primary/5">
                  <CardHeader className="pb-3">
                    <CardTitle className="flex items-center justify-between text-lg">
                      <span className="flex items-center gap-2">
                        <Settings className="h-5 w-5" />
                        {editingTemplate ? 'Edit API Template' : 'Create API Template'}
                      </span>
                      <Button variant="ghost" size="sm" onClick={() => { setShowTemplateForm(false); setEditingTemplate(null); resetTemplateForm(); }}>
                        <X className="h-4 w-4" />
                      </Button>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {/* Basic Info */}
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>Template Name *</Label>
                        <Input
                          placeholder="e.g., Get Customers List"
                          value={templateForm.name}
                          onChange={(e) => setTemplateForm(prev => ({ ...prev, name: e.target.value }))}
                          data-testid="input-template-name"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Operation Type *</Label>
                        <Select value={templateForm.operationType} onValueChange={(v) => setTemplateForm(prev => ({ ...prev, operationType: v }))}>
                          <SelectTrigger data-testid="select-operation-type">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="metadata">Metadata (Schema Discovery)</SelectItem>
                            <SelectItem value="list">List (Get All)</SelectItem>
                            <SelectItem value="get">Get (Single Record)</SelectItem>
                            <SelectItem value="upsert">Upsert (Create/Update)</SelectItem>
                            <SelectItem value="delete">Delete</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>ERP System *</Label>
                        <Select value={templateForm.erpSystemId} onValueChange={(v) => setTemplateForm(prev => ({ ...prev, erpSystemId: v, erpEntityId: '' }))}>
                          <SelectTrigger data-testid="select-form-erp-system">
                            <SelectValue placeholder="Select ERP System" />
                          </SelectTrigger>
                          <SelectContent>
                            {systemsData?.systems?.map(sys => (
                              <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-2">
                        <Label>Entity (Optional)</Label>
                        <Select 
                          value={templateForm.erpEntityId || 'none'} 
                          onValueChange={(v) => setTemplateForm(prev => ({ ...prev, erpEntityId: v === 'none' ? '' : v }))}
                          disabled={!templateForm.erpSystemId}
                        >
                          <SelectTrigger data-testid="select-form-entity">
                            <SelectValue placeholder="All Entities" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="none">General (All Entities)</SelectItem>
                            {formEntities.map(ent => (
                              <SelectItem key={ent.id} value={ent.id}>{ent.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    <Separator />

                    {/* HTTP Configuration */}
                    <div className="space-y-4">
                      <h4 className="font-medium flex items-center gap-2">
                        <Globe className="h-4 w-4" />
                        HTTP Configuration
                      </h4>
                      <div className="grid grid-cols-3 gap-4">
                        <div className="space-y-2">
                          <Label>HTTP Method *</Label>
                          <Select value={templateForm.httpMethod} onValueChange={(v) => setTemplateForm(prev => ({ ...prev, httpMethod: v }))}>
                            <SelectTrigger data-testid="select-http-method">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="GET">GET</SelectItem>
                              <SelectItem value="POST">POST</SelectItem>
                              <SelectItem value="PUT">PUT</SelectItem>
                              <SelectItem value="PATCH">PATCH</SelectItem>
                              <SelectItem value="DELETE">DELETE</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="col-span-2 space-y-2">
                          <Label>Path Template *</Label>
                          <Input
                            placeholder="/api/v1/customers or /api/v1/customers/{id}"
                            value={templateForm.pathTemplate}
                            onChange={(e) => setTemplateForm(prev => ({ ...prev, pathTemplate: e.target.value }))}
                            className="font-mono"
                            data-testid="input-path-template"
                          />
                        </div>
                      </div>
                    </div>

                    <Separator />

                    {/* Response & Pagination */}
                    <div className="space-y-4">
                      <h4 className="font-medium flex items-center gap-2">
                        <Code className="h-4 w-4" />
                        Response & Pagination
                      </h4>
                      <div className="grid grid-cols-3 gap-4">
                        <div className="space-y-2">
                          <Label>Pagination Type</Label>
                          <Select value={templateForm.paginationType} onValueChange={(v) => setTemplateForm(prev => ({ ...prev, paginationType: v }))}>
                            <SelectTrigger data-testid="select-pagination">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="offset">Offset/Limit</SelectItem>
                              <SelectItem value="cursor">Cursor</SelectItem>
                              <SelectItem value="page">Page Number</SelectItem>
                              <SelectItem value="none">None</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label>Data Path</Label>
                          <Input
                            placeholder="data.items or results"
                            value={templateForm.responseDataPath}
                            onChange={(e) => setTemplateForm(prev => ({ ...prev, responseDataPath: e.target.value }))}
                            className="font-mono"
                            data-testid="input-data-path"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Total Count Path</Label>
                          <Input
                            placeholder="data.totalCount"
                            value={templateForm.responseTotalPath}
                            onChange={(e) => setTemplateForm(prev => ({ ...prev, responseTotalPath: e.target.value }))}
                            className="font-mono"
                            data-testid="input-total-path"
                          />
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label>Expected Response Time (ms)</Label>
                          <Input
                            type="number"
                            value={templateForm.expectedResponseTimeMs}
                            onChange={(e) => setTemplateForm(prev => ({ ...prev, expectedResponseTimeMs: parseInt(e.target.value) || 5000 }))}
                            data-testid="input-response-time"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Status</Label>
                          <Select value={templateForm.status} onValueChange={(v) => setTemplateForm(prev => ({ ...prev, status: v }))}>
                            <SelectTrigger data-testid="select-status">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="active">Active</SelectItem>
                              <SelectItem value="deprecated">Deprecated</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label>Description</Label>
                      <Textarea
                        placeholder="Describe this API endpoint template..."
                        value={templateForm.description}
                        onChange={(e) => setTemplateForm(prev => ({ ...prev, description: e.target.value }))}
                        rows={2}
                        data-testid="input-template-description"
                      />
                    </div>

                    <div className="flex gap-2 pt-2">
                      <Button variant="outline" className="flex-1" onClick={() => { setShowTemplateForm(false); setEditingTemplate(null); resetTemplateForm(); }}>
                        Cancel
                      </Button>
                      <Button 
                        className="flex-1" 
                        onClick={handleTemplateSubmit}
                        disabled={!templateForm.name || !templateForm.erpSystemId || !templateForm.pathTemplate || createTemplateMutation.isPending || updateTemplateMutation.isPending}
                        data-testid="button-save-template"
                      >
                        <Save className="h-4 w-4 mr-2" />
                        {createTemplateMutation.isPending || updateTemplateMutation.isPending ? 'Saving...' : (editingTemplate ? 'Update Template' : 'Create Template')}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Templates List - Documentation Style */}
              {templatesLoading ? (
                <div className="text-center py-8 text-muted-foreground">Loading templates...</div>
              ) : templates.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  {templateSystemFilter ? 'No API templates configured for this system yet' : 'No API templates configured yet. Add one to define how to connect to ERP APIs.'}
                </div>
              ) : (
                <div className="divide-y divide-border">
                  {templates.map((template) => {
                    const system = systemsData?.systems?.find(s => s.id === template.erpSystemId);
                    const entity = findEntityById(template.erpEntityId);
                    const isExpanded = expandedTemplateId === template.id;
                    const isGenerating = generateFieldsMutation.isPending && generateFieldsMutation.variables?.templateId === template.id;
                    const hasResult = generationResult && generationResult.templateId === template.id;

                    const getMethodColor = (method: string) => {
                      switch (method?.toUpperCase()) {
                        case 'GET': return 'bg-green-600 text-white';
                        case 'POST': return 'bg-blue-600 text-white';
                        case 'PUT': return 'bg-amber-600 text-white';
                        case 'PATCH': return 'bg-orange-600 text-white';
                        case 'DELETE': return 'bg-red-600 text-white';
                        default: return 'bg-gray-600 text-white';
                      }
                    };

                    return (
                      <Collapsible
                        key={template.id}
                        open={isExpanded}
                        onOpenChange={(open) => {
                          setExpandedTemplateId(open ? template.id : null);
                          if (!open) setGenerationResult(null);
                        }}
                      >
                        <div className={`py-5 px-2 transition-all ${isExpanded ? 'bg-muted/30 border-l-4 border-primary' : ''}`}>
                          {/* Template Header - Clickable Name */}
                          <CollapsibleTrigger asChild>
                            <button className="text-left w-full group">
                              <div className="flex items-center gap-2">
                                {isExpanded ? (
                                  <ChevronDown className="h-4 w-4 text-primary" />
                                ) : (
                                  <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors" />
                                )}
                                <span className="text-[#0891b2] hover:text-[#06b6d4] font-semibold text-base cursor-pointer hover:underline">
                                  {template.name}
                                </span>
                                {template.status === 'active' && (
                                  <Badge variant="outline" className="text-xs bg-green-50 text-green-700 border-green-200">
                                    active
                                  </Badge>
                                )}
                                {hasResult && (
                                  <Badge variant="outline" className="text-xs bg-green-100 text-green-800 border-green-300">
                                    {generationResult.createdFields} fields created
                                  </Badge>
                                )}
                                {isExpanded && (
                                  <span className="text-xs text-muted-foreground ml-2">(click to collapse)</span>
                                )}
                              </div>
                            </button>
                          </CollapsibleTrigger>

                          {/* Method Badge */}
                          <div className="mt-3 flex items-center gap-2">
                            <span className="text-sm text-muted-foreground">Method:</span>
                            <span className={`px-2.5 py-0.5 rounded text-xs font-semibold uppercase ${getMethodColor(template.httpMethod)}`}>
                              {template.httpMethod}
                            </span>
                          </div>

                          {/* Path */}
                          <div className="mt-2">
                            <span className="text-sm text-muted-foreground">Path:</span>
                            <div className="mt-1 bg-slate-100 dark:bg-slate-800 border-l-4 border-slate-300 dark:border-slate-600 px-3 py-2 rounded-r">
                              <code className="text-sm font-mono text-slate-700 dark:text-slate-300 break-all">
                                {template.pathTemplate}
                              </code>
                            </div>
                          </div>

                          {/* Entity & System Info */}
                          <div className="mt-3 flex items-center gap-4 text-sm text-muted-foreground">
                            <span>System: <span className="font-medium text-foreground">{system?.name || 'Unknown'}</span></span>
                            {entity && (
                              <span>Entity: <span className="font-medium text-foreground">{entity.name}</span></span>
                            )}
                          </div>

                          {/* Expanded Content - Swagger Style */}
                          <CollapsibleContent>
                            <div className="mt-6 space-y-6">
                              {/* Generation Results */}
                              {generationResult && generationResult.templateId === template.id && (
                                <div className="p-3 bg-green-50 dark:bg-green-950 border border-green-200 dark:border-green-800 rounded-lg">
                                  <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                      <Database className="h-4 w-4 text-green-600" />
                                      <span className="font-medium text-green-800 dark:text-green-200">
                                        Generated {generationResult.createdFields} fields for "{generationResult.entityName}"
                                      </span>
                                    </div>
                                    <Button variant="ghost" size="sm" onClick={() => setGenerationResult(null)} className="h-6 w-6 p-0">
                                      <X className="h-4 w-4" />
                                    </Button>
                                  </div>
                                  {generationResult.skippedFields.length > 0 && (
                                    <div className="mt-2 text-sm text-muted-foreground">
                                      <span className="font-medium">Skipped ({generationResult.skippedFields.length}):</span>{' '}
                                      {generationResult.skippedFields.slice(0, 5).join(', ')}
                                      {generationResult.skippedFields.length > 5 && ` +${generationResult.skippedFields.length - 5} more`}
                                    </div>
                                  )}
                                </div>
                              )}

                              {/* Description */}
                              {template.description && (
                                <div className="text-sm text-muted-foreground italic border-l-2 border-muted pl-3">
                                  {template.description}
                                </div>
                              )}

                              {/* REQUEST SECTION */}
                              <div>
                                <h4 className="text-lg font-semibold mb-4">Request</h4>
                                
                                {/* Path Parameters */}
                                {(() => {
                                  const pathParams = template.pathTemplate?.match(/\{([^}]+)\}/g)?.map(p => p.replace(/[{}]/g, '')) || [];
                                  if (pathParams.length === 0) return null;
                                  return (
                                    <div className="mb-6">
                                      <h5 className="text-sm font-semibold text-muted-foreground mb-2">Path Parameters</h5>
                                      <div className="border rounded-lg divide-y">
                                        {pathParams.map((param, idx) => (
                                          <div key={idx} className="px-4 py-3">
                                            <span className="text-[#0891b2] font-mono">{param}</span>
                                            <span className="text-orange-600 ml-1">(required)</span>
                                            <span className="text-muted-foreground">: string</span>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  );
                                })()}

                                {/* Query Parameters */}
                                <div className="mb-6">
                                  <h5 className="text-sm font-semibold text-muted-foreground mb-2">Query Parameters</h5>
                                  <div className="border rounded-lg divide-y">
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">expand</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Expand related resources inline</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">fields</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Specify fields to return</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">limit</span>
                                      <span className="text-muted-foreground">: integer</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Maximum number of records</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">offset</span>
                                      <span className="text-muted-foreground">: integer</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Starting position for results</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">onlyData</span>
                                      <span className="text-muted-foreground">: boolean</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Return only data without metadata</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">q</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Query filter expression</span>
                                    </div>
                                  </div>
                                </div>

                                {/* Header Parameters */}
                                <div className="mb-6">
                                  <h5 className="text-sm font-semibold text-muted-foreground mb-2">Header Parameters</h5>
                                  <div className="border rounded-lg divide-y">
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">Authorization</span>
                                      <span className="text-orange-600 ml-1">(required)</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- Bearer token</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">Content-Type</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- application/json</span>
                                    </div>
                                    <div className="px-4 py-3">
                                      <span className="text-[#0891b2] font-mono">REST-Framework-Version</span>
                                      <span className="text-muted-foreground">: string</span>
                                      <span className="text-xs text-muted-foreground ml-2">- API version number</span>
                                    </div>
                                  </div>
                                </div>
                              </div>

                              {/* RESPONSE SECTION */}
                              <div>
                                <h4 className="text-lg font-semibold mb-4">Response</h4>
                                
                                {/* Response Info Grid */}
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                                  <div className="border rounded-lg p-3">
                                    <div className="text-xs text-muted-foreground mb-1">Pagination Type</div>
                                    <div className="font-mono text-sm font-medium">{template.paginationType || 'none'}</div>
                                  </div>
                                  <div className="border rounded-lg p-3">
                                    <div className="text-xs text-muted-foreground mb-1">Data Path</div>
                                    <div className="font-mono text-sm font-medium">{template.responseDataPath || 'root'}</div>
                                  </div>
                                  <div className="border rounded-lg p-3">
                                    <div className="text-xs text-muted-foreground mb-1">Total Count Path</div>
                                    <div className="font-mono text-sm font-medium">{template.responseTotalPath || '-'}</div>
                                  </div>
                                  <div className="border rounded-lg p-3">
                                    <div className="text-xs text-muted-foreground mb-1">Expected Response Time</div>
                                    <div className="text-sm font-medium">{template.expectedResponseTimeMs || 5000}ms</div>
                                  </div>
                                </div>

                                {/* Sample Response */}
                                {template.sampleResponse && (
                                  <div>
                                    <h5 className="text-sm font-semibold text-muted-foreground mb-2">Sample Response</h5>
                                    <div className="bg-slate-900 dark:bg-slate-950 rounded-lg p-4 overflow-x-auto">
                                      <pre className="text-sm font-mono text-green-400">
                                        {JSON.stringify(template.sampleResponse, null, 2)}
                                      </pre>
                                    </div>
                                  </div>
                                )}
                              </div>

                              {/* Action Buttons */}
                              <div className="flex items-center gap-2 pt-4 border-t">
                                {template.erpEntityId && template.sampleResponse && (
                                  <Button 
                                    variant={hasResult ? "default" : "outline"}
                                    size="sm" 
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      generateFieldsMutation.mutate({ templateId: template.id, entityName: entity?.name || 'Unknown Entity' });
                                    }}
                                    disabled={isGenerating}
                                    title={`Generate fields for ${entity?.name || 'entity'} from sample response`}
                                    data-testid={`button-generate-fields-${template.id}`}
                                    className={`gap-1.5 ${hasResult ? 'bg-green-600 hover:bg-green-700' : ''}`}
                                  >
                                    {isGenerating ? (
                                      <>
                                        <div className="animate-spin h-4 w-4 border-2 border-current border-t-transparent rounded-full" />
                                        <span>Generating...</span>
                                      </>
                                    ) : (
                                      <>
                                        <Database className="h-4 w-4" />
                                        <span>Generate Fields</span>
                                      </>
                                    )}
                                  </Button>
                                )}
                                <Button variant="outline" size="sm" onClick={() => openEditTemplate(template)} data-testid={`button-edit-template-${template.id}`}>
                                  <Edit className="h-4 w-4 mr-1.5" />
                                  Edit Template
                                </Button>
                                <Button 
                                  variant="outline" 
                                  size="sm" 
                                  onClick={() => {
                                    setActiveTab('entities');
                                    const entitySystem = systemsData?.systems?.find(s => 
                                      allEntities.some(e => e.id === template.erpEntityId && e.systemId === s.id)
                                    );
                                    if (entitySystem) setSelectedSystemId(entitySystem.id);
                                    setSelectedEntityId(template.erpEntityId || null);
                                  }}
                                  disabled={!template.erpEntityId}
                                >
                                  <ExternalLink className="h-4 w-4 mr-1.5" />
                                  View Entity Fields
                                </Button>
                                <Button variant="outline" size="sm" className="text-red-600 hover:text-red-700 hover:bg-red-50 ml-auto" onClick={() => deleteTemplateMutation.mutate(template.id)} data-testid={`button-delete-template-${template.id}`}>
                                  <Trash2 className="h-4 w-4 mr-1.5" />
                                  Delete
                                </Button>
                              </div>
                            </div>
                          </CollapsibleContent>
                        </div>
                      </Collapsible>
                    );
                  })}
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
