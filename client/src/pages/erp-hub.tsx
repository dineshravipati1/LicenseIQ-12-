import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useLocation } from "wouter";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { 
  Plug, Database, GitBranch, ChevronDown, ChevronRight, 
  CheckCircle, XCircle, AlertCircle, RefreshCw, Plus, 
  Settings, ExternalLink, Zap, Activity, FileCode, 
  ArrowRight, Layers, Link2, Search, Filter
} from "lucide-react";
import type { ErpSystem, ErpEntity, IntegrationEndpointTemplate, MasterDataMapping } from "@shared/schema";

export default function ErpHubPage() {
  const [, navigate] = useLocation();
  const { toast } = useToast();
  
  // Filter states
  const [selectedSystemId, setSelectedSystemId] = useState<string>('all');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState('');
  
  // Section expansion states
  const [connectionsExpanded, setConnectionsExpanded] = useState(true);
  const [templatesExpanded, setTemplatesExpanded] = useState(true);
  const [mappingsExpanded, setMappingsExpanded] = useState(true);
  
  // Detail panel state (inline, not popup)
  const [selectedItem, setSelectedItem] = useState<{
    type: 'connection' | 'template' | 'mapping';
    data: any;
  } | null>(null);

  // Fetch ERP Systems
  const { data: systemsData, isLoading: systemsLoading } = useQuery<{ systems: ErpSystem[] }>({
    queryKey: ['/api/erp-systems'],
  });

  // Fetch all entities
  const { data: entitiesData } = useQuery<{ entities: ErpEntity[] }>({
    queryKey: ['/api/erp-entities'],
  });

  // Fetch API Templates
  const { data: templatesData, isLoading: templatesLoading } = useQuery<IntegrationEndpointTemplate[]>({
    queryKey: ['/api/integration-endpoint-templates', selectedSystemId],
    queryFn: () => {
      const url = selectedSystemId && selectedSystemId !== 'all'
        ? `/api/integration-endpoint-templates?systemId=${selectedSystemId}`
        : '/api/integration-endpoint-templates';
      return fetch(url).then(res => res.json());
    },
  });

  // Fetch Master Data Mappings
  const { data: mappingsData, isLoading: mappingsLoading } = useQuery<{ mappings: MasterDataMapping[] }>({
    queryKey: ['/api/master-data-mappings'],
  });

  const systems = systemsData?.systems || [];
  const entities = entitiesData?.entities || [];
  const templates = templatesData || [];
  const mappings = mappingsData?.mappings || [];

  // Helper functions
  const getSystemById = (id: string) => systems.find(s => s.id === id);
  const getEntityById = (id: string) => entities.find(e => e.id === id);
  
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

  const selectItem = (type: 'connection' | 'template' | 'mapping', data: any) => {
    setSelectedItem({ type, data });
  };
  
  const closePanel = () => {
    setSelectedItem(null);
  };

  // Filter systems/connections by selected system and status
  const filteredSystems = systems.filter(s =>
    (selectedSystemId === 'all' || s.id === selectedSystemId) &&
    (statusFilter === 'all' || s.status === statusFilter) &&
    (!searchQuery || s.name.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  // Filter templates based on selected system and search
  const filteredTemplates = templates.filter(t => 
    (selectedSystemId === 'all' || t.erpSystemId === selectedSystemId) &&
    (!searchQuery || 
      t.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.pathTemplate?.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  // Filter mappings by selected system, status, and search
  const filteredMappings = mappings.filter(m =>
    (selectedSystemId === 'all' || m.erpSystem === getSystemById(selectedSystemId)?.name) &&
    (statusFilter === 'all' || m.status === statusFilter) &&
    (!searchQuery || m.mappingName?.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  // Calculate KPIs based on filtered data
  const activeConnections = filteredSystems.filter(s => s.status === 'active').length;
  const totalTemplates = filteredTemplates.length;
  const approvedMappings = filteredMappings.filter(m => m.status === 'approved').length;
  const draftMappings = filteredMappings.filter(m => m.status === 'draft').length;

  return (
    <MainLayout 
      title="Integration Overview"
    >
      <div className="flex h-[calc(100vh-120px)]">
        {/* LEFT RAIL - Filters & KPIs */}
        <div className="w-72 border-r bg-muted/20 p-4 flex flex-col gap-4">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input 
              placeholder="Search templates, fields..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
              data-testid="input-hub-search"
            />
          </div>

          {/* Filters */}
          <div className="space-y-3">
            <Label className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Filters</Label>
            
            <div>
              <Label className="text-xs mb-1 block">ERP System</Label>
              <Select value={selectedSystemId} onValueChange={setSelectedSystemId}>
                <SelectTrigger data-testid="select-system-filter">
                  <SelectValue placeholder="All Systems" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Systems</SelectItem>
                  {systems.map(system => (
                    <SelectItem key={system.id} value={system.id}>{system.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <Label className="text-xs mb-1 block">Status</Label>
              <Select value={statusFilter} onValueChange={setStatusFilter}>
                <SelectTrigger data-testid="select-status-filter">
                  <SelectValue placeholder="All Status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Status</SelectItem>
                  <SelectItem value="active">Active</SelectItem>
                  <SelectItem value="approved">Approved</SelectItem>
                  <SelectItem value="draft">Draft</SelectItem>
                  <SelectItem value="inactive">Inactive</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <Separator />

          {/* KPI Dashboard */}
          <div className="space-y-3">
            <Label className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Dashboard</Label>
            
            <Card className="bg-gradient-to-br from-green-50 to-green-100 dark:from-green-950 dark:to-green-900 border-green-200 dark:border-green-800">
              <CardContent className="p-3">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs text-green-600 dark:text-green-400">Connections</p>
                    <p className="text-2xl font-bold text-green-700 dark:text-green-300">{activeConnections}</p>
                  </div>
                  <Plug className="h-8 w-8 text-green-500/50" />
                </div>
                <p className="text-xs text-green-600/70 mt-1">{systems.length} total systems</p>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-950 dark:to-blue-900 border-blue-200 dark:border-blue-800">
              <CardContent className="p-3">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs text-blue-600 dark:text-blue-400">API Templates</p>
                    <p className="text-2xl font-bold text-blue-700 dark:text-blue-300">{totalTemplates}</p>
                  </div>
                  <FileCode className="h-8 w-8 text-blue-500/50" />
                </div>
                <p className="text-xs text-blue-600/70 mt-1">Endpoint configurations</p>
              </CardContent>
            </Card>

            <Card className="bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-950 dark:to-purple-900 border-purple-200 dark:border-purple-800">
              <CardContent className="p-3">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs text-purple-600 dark:text-purple-400">Field Mappings</p>
                    <p className="text-2xl font-bold text-purple-700 dark:text-purple-300">{mappings.length}</p>
                  </div>
                  <GitBranch className="h-8 w-8 text-purple-500/50" />
                </div>
                <div className="flex gap-2 mt-1">
                  <Badge variant="outline" className="text-xs bg-green-50 text-green-700 border-green-200">{approvedMappings} approved</Badge>
                  <Badge variant="outline" className="text-xs bg-amber-50 text-amber-700 border-amber-200">{draftMappings} draft</Badge>
                </div>
              </CardContent>
            </Card>
          </div>

          <Separator />

          {/* Quick Actions */}
          <div className="space-y-2 mt-auto">
            <Label className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Quick Actions</Label>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2" onClick={() => navigate('/erp-catalog')}>
              <Database className="h-4 w-4" />
              Manage Catalog
            </Button>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2" onClick={() => navigate('/erp-integration')}>
              <Zap className="h-4 w-4" />
              Run Import
            </Button>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2" onClick={() => navigate('/master-data-mapping')}>
              <Link2 className="h-4 w-4" />
              Field Mapper
            </Button>
          </div>
        </div>

        {/* MAIN WORKSPACE */}
        <div className="flex-1 overflow-hidden">
          <ScrollArea className="h-full">
            <div className="p-6 space-y-4">
              
              {/* CONNECTIONS SECTION */}
              <Collapsible open={connectionsExpanded} onOpenChange={setConnectionsExpanded}>
                <Card>
                  <CollapsibleTrigger asChild>
                    <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          {connectionsExpanded ? <ChevronDown className="h-5 w-5" /> : <ChevronRight className="h-5 w-5" />}
                          <Plug className="h-5 w-5 text-green-600" />
                          <div>
                            <CardTitle className="text-lg">ERP Connections</CardTitle>
                            <CardDescription>Connected ERP systems and their health status</CardDescription>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary">{filteredSystems.length} systems</Badge>
                          <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); navigate('/erp-catalog'); }}>
                            <Plus className="h-4 w-4 mr-1" />
                            Add
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <CardContent className="pt-0">
                      {systemsLoading ? (
                        <div className="text-center py-4 text-muted-foreground">Loading...</div>
                      ) : filteredSystems.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                          <Plug className="h-12 w-12 mx-auto mb-2 opacity-20" />
                          <p>No ERP systems match the current filters</p>
                          <Button variant="link" onClick={() => navigate('/erp-catalog')}>Add your first connection</Button>
                        </div>
                      ) : (
                        <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
                          {filteredSystems.map(system => (
                            <div 
                              key={system.id}
                              className="border rounded-lg p-4 hover:border-primary/50 hover:bg-muted/30 cursor-pointer transition-all"
                              onClick={() => selectItem('connection', system)}
                              data-testid={`card-connection-${system.id}`}
                            >
                              <div className="flex items-start justify-between mb-2">
                                <div className="flex items-center gap-2">
                                  {system.status === 'active' ? (
                                    <CheckCircle className="h-4 w-4 text-green-500" />
                                  ) : (
                                    <XCircle className="h-4 w-4 text-red-500" />
                                  )}
                                  <span className="font-medium">{system.name}</span>
                                </div>
                                <Badge variant={system.status === 'active' ? 'default' : 'secondary'} className="text-xs">
                                  {system.status}
                                </Badge>
                              </div>
                              <p className="text-sm text-muted-foreground">{system.vendor}</p>
                              <div className="flex items-center gap-2 mt-2 text-xs text-muted-foreground">
                                <span>v{system.version}</span>
                                <span>•</span>
                                <span>{system.category}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </CardContent>
                  </CollapsibleContent>
                </Card>
              </Collapsible>

              {/* API TEMPLATES SECTION */}
              <Collapsible open={templatesExpanded} onOpenChange={setTemplatesExpanded}>
                <Card>
                  <CollapsibleTrigger asChild>
                    <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          {templatesExpanded ? <ChevronDown className="h-5 w-5" /> : <ChevronRight className="h-5 w-5" />}
                          <FileCode className="h-5 w-5 text-blue-600" />
                          <div>
                            <CardTitle className="text-lg">API Templates</CardTitle>
                            <CardDescription>Endpoint configurations with Swagger-style documentation</CardDescription>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary">{filteredTemplates.length} templates</Badge>
                          <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); navigate('/erp-catalog?tab=templates'); }}>
                            <Plus className="h-4 w-4 mr-1" />
                            Add
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <CardContent className="pt-0">
                      {templatesLoading ? (
                        <div className="text-center py-4 text-muted-foreground">Loading...</div>
                      ) : filteredTemplates.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                          <FileCode className="h-12 w-12 mx-auto mb-2 opacity-20" />
                          <p>No API templates configured</p>
                          <Button variant="link" onClick={() => navigate('/erp-catalog?tab=templates')}>Create your first template</Button>
                        </div>
                      ) : (
                        <div className="space-y-2">
                          {filteredTemplates.slice(0, 10).map(template => {
                            const system = getSystemById(template.erpSystemId);
                            const entity = getEntityById(template.erpEntityId || '');
                            return (
                              <div 
                                key={template.id}
                                className="border rounded-lg p-4 hover:border-primary/50 hover:bg-muted/30 cursor-pointer transition-all"
                                onClick={() => selectItem('template', template)}
                                data-testid={`card-template-${template.id}`}
                              >
                                <div className="flex items-center justify-between">
                                  <div className="flex-1">
                                    <div className="flex items-center gap-2 mb-2">
                                      <span className={`px-2 py-0.5 rounded text-xs font-semibold ${getMethodColor(template.httpMethod)}`}>
                                        {template.httpMethod}
                                      </span>
                                      <span className="font-medium text-[#0891b2]">{template.name}</span>
                                      {template.status === 'active' && (
                                        <Badge variant="outline" className="text-xs bg-green-50 text-green-700">active</Badge>
                                      )}
                                    </div>
                                    <code className="text-xs text-muted-foreground font-mono bg-muted px-2 py-1 rounded">
                                      {template.pathTemplate}
                                    </code>
                                    <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
                                      <span>{system?.name || 'Unknown System'}</span>
                                      {entity && (
                                        <>
                                          <ArrowRight className="h-3 w-3" />
                                          <span>{entity.name}</span>
                                        </>
                                      )}
                                    </div>
                                  </div>
                                  <ExternalLink className="h-4 w-4 text-muted-foreground" />
                                </div>
                              </div>
                            );
                          })}
                          {filteredTemplates.length > 10 && (
                            <Button variant="link" className="w-full" onClick={() => navigate('/erp-catalog?tab=templates')}>
                              View all {filteredTemplates.length} templates
                            </Button>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </CollapsibleContent>
                </Card>
              </Collapsible>

              {/* FIELD MAPPINGS SECTION */}
              <Collapsible open={mappingsExpanded} onOpenChange={setMappingsExpanded}>
                <Card>
                  <CollapsibleTrigger asChild>
                    <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          {mappingsExpanded ? <ChevronDown className="h-5 w-5" /> : <ChevronRight className="h-5 w-5" />}
                          <GitBranch className="h-5 w-5 text-purple-600" />
                          <div>
                            <CardTitle className="text-lg">Field Mappings</CardTitle>
                            <CardDescription>ERP to LicenseIQ schema field transformations</CardDescription>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary">{filteredMappings.length} mappings</Badge>
                          <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); navigate('/master-data-mapping'); }}>
                            <Plus className="h-4 w-4 mr-1" />
                            Add
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <CardContent className="pt-0">
                      {mappingsLoading ? (
                        <div className="text-center py-4 text-muted-foreground">Loading...</div>
                      ) : filteredMappings.length === 0 ? (
                        <div className="text-center py-8 text-muted-foreground">
                          <GitBranch className="h-12 w-12 mx-auto mb-2 opacity-20" />
                          <p>No field mappings configured</p>
                          <Button variant="link" onClick={() => navigate('/master-data-mapping')}>Create your first mapping</Button>
                        </div>
                      ) : (
                        <div className="space-y-2">
                          {filteredMappings.slice(0, 10).map(mapping => (
                            <div 
                              key={mapping.id}
                              className="border rounded-lg p-4 hover:border-primary/50 hover:bg-muted/30 cursor-pointer transition-all"
                              onClick={() => selectItem('mapping', mapping)}
                              data-testid={`card-mapping-${mapping.id}`}
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-4">
                                  <div className="text-right">
                                    <p className="font-mono text-sm font-medium">{mapping.mappingName}</p>
                                    <p className="text-xs text-muted-foreground">{mapping.erpSystem}</p>
                                  </div>
                                  <ArrowRight className="h-4 w-4 text-muted-foreground" />
                                  <div>
                                    <p className="font-mono text-sm font-medium text-primary">{mapping.entityType}</p>
                                    <p className="text-xs text-muted-foreground">v{mapping.version}</p>
                                  </div>
                                </div>
                                <div className="flex items-center gap-2">
                                  <Badge 
                                    variant={mapping.status === 'approved' ? 'default' : 'secondary'}
                                    className={mapping.status === 'approved' ? 'bg-green-600' : ''}
                                  >
                                    {mapping.status}
                                  </Badge>
                                  {mapping.aiModel && (
                                    <Badge variant="outline" className="text-xs">
                                      <Zap className="h-3 w-3 mr-1" />
                                      AI
                                    </Badge>
                                  )}
                                </div>
                              </div>
                            </div>
                          ))}
                          {filteredMappings.length > 10 && (
                            <Button variant="link" className="w-full" onClick={() => navigate('/master-data-mapping')}>
                              View all {filteredMappings.length} mappings
                            </Button>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </CollapsibleContent>
                </Card>
              </Collapsible>

            </div>
          </ScrollArea>
        </div>

        {/* RIGHT PANEL - Inline Context Detail */}
        {selectedItem && (
          <div className="w-80 border-l bg-background flex flex-col">
            <ScrollArea className="flex-1">
              <div className="p-4">
                {selectedItem.type === 'connection' && (
                  <>
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <Plug className="h-5 w-5 text-green-600" />
                        <h3 className="font-semibold">{selectedItem.data.name}</h3>
                      </div>
                      <Button variant="ghost" size="icon" onClick={closePanel} data-testid="button-close-panel">
                        <XCircle className="h-4 w-4" />
                      </Button>
                    </div>
                    <p className="text-sm text-muted-foreground mb-4">ERP System Connection Details</p>
                    <div className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label className="text-xs text-muted-foreground">Vendor</Label>
                          <p className="font-medium">{selectedItem.data.vendor}</p>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Version</Label>
                          <p className="font-medium">{selectedItem.data.version}</p>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Category</Label>
                          <p className="font-medium capitalize">{selectedItem.data.category}</p>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Status</Label>
                          <Badge variant={selectedItem.data.status === 'active' ? 'default' : 'secondary'}>
                            {selectedItem.data.status}
                          </Badge>
                        </div>
                      </div>
                      {selectedItem.data.description && (
                        <div>
                          <Label className="text-xs text-muted-foreground">Description</Label>
                          <p className="text-sm mt-1">{selectedItem.data.description}</p>
                        </div>
                      )}
                      <Separator />
                      <div className="space-y-2">
                        <Button className="w-full" onClick={() => { closePanel(); navigate('/erp-integration?tab=connections'); }}>
                          <Settings className="h-4 w-4 mr-2" />
                          Edit Connection
                        </Button>
                        <Button variant="outline" className="w-full" onClick={() => { closePanel(); navigate('/erp-catalog'); }}>
                          <ExternalLink className="h-4 w-4 mr-2" />
                          Manage in Catalog
                        </Button>
                      </div>
                    </div>
                  </>
                )}

                {selectedItem.type === 'template' && (
                  <>
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded text-xs font-semibold ${getMethodColor(selectedItem.data.httpMethod)}`}>
                          {selectedItem.data.httpMethod}
                        </span>
                        <h3 className="font-semibold text-sm">{selectedItem.data.name}</h3>
                      </div>
                      <Button variant="ghost" size="icon" onClick={closePanel} data-testid="button-close-panel">
                        <XCircle className="h-4 w-4" />
                      </Button>
                    </div>
                    <p className="text-sm text-muted-foreground mb-4">API Template Documentation</p>
                    <div className="space-y-4">
                      <div>
                        <Label className="text-xs text-muted-foreground">Endpoint Path</Label>
                        <div className="mt-1 bg-slate-100 dark:bg-slate-800 border-l-4 border-slate-300 px-3 py-2 rounded-r">
                          <code className="text-xs font-mono break-all">{selectedItem.data.pathTemplate}</code>
                        </div>
                      </div>
                      
                      {/* Path Parameters */}
                      {(() => {
                        const pathParams = selectedItem.data.pathTemplate?.match(/\{([^}]+)\}/g)?.map((p: string) => p.replace(/[{}]/g, '')) || [];
                        if (pathParams.length === 0) return null;
                        return (
                          <div>
                            <Label className="text-xs text-muted-foreground">Path Parameters</Label>
                            <div className="mt-1 border rounded-lg divide-y">
                              {pathParams.map((param: string, idx: number) => (
                                <div key={idx} className="px-3 py-2">
                                  <span className="text-[#0891b2] font-mono text-xs">{param}</span>
                                  <span className="text-orange-600 ml-1 text-xs">(required)</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      })()}

                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label className="text-xs text-muted-foreground">Pagination</Label>
                          <p className="font-mono text-xs">{selectedItem.data.paginationType || 'none'}</p>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Response Time</Label>
                          <p className="text-xs">{selectedItem.data.expectedResponseTimeMs || 5000}ms</p>
                        </div>
                      </div>

                      {selectedItem.data.sampleResponse && (
                        <div>
                          <Label className="text-xs text-muted-foreground">Sample Response</Label>
                          <div className="mt-1 bg-slate-900 rounded-lg p-2 overflow-x-auto max-h-40">
                            <pre className="text-xs font-mono text-green-400">
                              {JSON.stringify(selectedItem.data.sampleResponse, null, 2)}
                            </pre>
                          </div>
                        </div>
                      )}

                      <Separator />
                      <Button className="w-full" onClick={() => { closePanel(); navigate('/erp-catalog?tab=templates'); }}>
                        <Settings className="h-4 w-4 mr-2" />
                        Edit in Catalog
                      </Button>
                    </div>
                  </>
                )}

                {selectedItem.type === 'mapping' && (
                  <>
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <GitBranch className="h-5 w-5 text-purple-600" />
                        <h3 className="font-semibold text-sm">{selectedItem.data.mappingName}</h3>
                      </div>
                      <Button variant="ghost" size="icon" onClick={closePanel} data-testid="button-close-panel">
                        <XCircle className="h-4 w-4" />
                      </Button>
                    </div>
                    <p className="text-sm text-muted-foreground mb-4">ERP to LicenseIQ field mapping</p>
                    <div className="space-y-4">
                      <div className="flex items-center justify-center gap-2 p-3 bg-muted/50 rounded-lg">
                        <div className="text-center">
                          <p className="font-mono font-medium text-xs">{selectedItem.data.erpSystem}</p>
                          <p className="text-xs text-muted-foreground">Source</p>
                        </div>
                        <ArrowRight className="h-4 w-4 text-primary" />
                        <div className="text-center">
                          <p className="font-mono font-medium text-xs text-primary">{selectedItem.data.entityType}</p>
                          <p className="text-xs text-muted-foreground">Target</p>
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label className="text-xs text-muted-foreground">Status</Label>
                          <Badge variant={selectedItem.data.status === 'approved' ? 'default' : 'secondary'}>
                            {selectedItem.data.status}
                          </Badge>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Version</Label>
                          <p className="font-medium">v{selectedItem.data.version}</p>
                        </div>
                        {selectedItem.data.aiModel && (
                          <div>
                            <Label className="text-xs text-muted-foreground">AI Model</Label>
                            <p className="font-mono text-xs">{selectedItem.data.aiModel}</p>
                          </div>
                        )}
                        {selectedItem.data.aiConfidence && (
                          <div>
                            <Label className="text-xs text-muted-foreground">AI Confidence</Label>
                            <p className="text-sm">{Math.round(selectedItem.data.aiConfidence * 100)}%</p>
                          </div>
                        )}
                      </div>

                      {selectedItem.data.notes && (
                        <div>
                          <Label className="text-xs text-muted-foreground">Notes</Label>
                          <p className="text-sm mt-1">{selectedItem.data.notes}</p>
                        </div>
                      )}

                      <Separator />
                      <Button className="w-full" onClick={() => { closePanel(); navigate('/master-data-mapping'); }}>
                        <Settings className="h-4 w-4 mr-2" />
                        Edit in Mapper
                      </Button>
                    </div>
                  </>
                )}
              </div>
            </ScrollArea>
          </div>
        )}
      </div>
    </MainLayout>
  );
}
