import { useState, useCallback, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { useLocation } from 'wouter';
import { apiRequest, queryClient } from '@/lib/queryClient';
import { useToast } from '@/hooks/use-toast';
import MainLayout from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { FilterBuilder, type FilterConfig, type ImportFilter } from '@/components/filter-builder';
import { 
  Database, 
  Upload, 
  History, 
  CheckCircle2, 
  XCircle, 
  Clock, 
  FileJson, 
  AlertCircle, 
  Loader2, 
  RefreshCw,
  Eye,
  Trash2,
  ArrowLeftRight,
  ArrowLeft,
  Building2,
  GitBranch,
  Sparkles,
  ChevronDown,
  ChevronRight,
  X,
  Plug,
  Link2,
  Shield,
  Key,
  TestTube2,
  Activity,
  Plus,
  Pencil,
  Heart,
  HeartPulse,
  Search,
  Table2
} from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { formatDateUSA } from '@/lib/dateFormat';
import { useDropzone } from 'react-dropzone';
import type { Company, BusinessUnit, Location, ErpSystem } from '@shared/schema';

interface MappingWithVersion {
  id: string;
  mappingName: string;
  erpSystem: string;
  erpSystemId?: string;
  entityType: string;
  licenseiqEntityId?: string;
  companyId?: string;
  businessUnitId?: string;
  locationId?: string;
  version: number;
  parentMappingId?: string;
  sourceSchema: any;
  targetSchema: any;
  mappingResults: any[];
  status: string;
  aiModel?: string;
  aiConfidence?: number;
  createdBy: string;
  approvedBy?: string;
  approvedAt?: string;
  createdAt: string;
  updatedAt?: string;
  notes?: string;
}

interface ImportJob {
  id: string;
  mappingId: string;
  mappingVersion?: number;
  companyId?: string;
  businessUnitId?: string;
  locationId?: string;
  erpSystemId?: string;
  entityType?: string;
  jobName: string;
  jobType: string;
  status: string;
  recordsTotal: number;
  recordsProcessed: number;
  recordsFailed: number;
  recordsSkipped?: number;
  uploadMeta?: any;
  errorLog?: any;
  processingLog?: any;
  createdBy: string;
  startedAt?: string;
  completedAt?: string;
  createdAt: string;
}

interface ImportRecord {
  id: string;
  jobId: string;
  mappingId: string;
  sourceRecord: any;
  targetRecord: any;
  recordStatus: string;
  validationErrors?: any;
  metadata?: any;
  createdAt: string;
}

interface IntegrationConnection {
  id: string;
  name: string;
  erpSystemId: string;
  companyId?: string;
  businessUnitId?: string;
  locationId?: string;
  baseUrl: string;
  authType: string;
  clientId?: string;
  tokenUrl?: string;
  authUrl?: string;
  scopes?: string;
  apiKeyHeader?: string;
  apiKeyLocation?: string;
  basicUsername?: string;
  rateLimitRpm?: number;
  rateLimitConcurrent?: number;
  retryMaxAttempts?: number;
  retryBackoffMs?: number;
  healthCheckEndpoint?: string;
  lastHealthCheckAt?: string;
  lastHealthCheckStatus?: string;
  lastHealthCheckMessage?: string;
  status: string;
  lastConnectedAt?: string;
  description?: string;
  createdBy: string;
  createdAt: string;
  updatedAt?: string;
}

interface HealthEvent {
  id: string;
  connectionId: string;
  status: string;
  statusCode?: number;
  message?: string;
  latencyMs?: number;
  eventType: string;
  checkedAt: string;
}

interface DataImportSource {
  id: string;
  name: string;
  description?: string;
  sourceType: 'file' | 'api';
  connectionId?: string;
  endpointTemplateId?: string;
  mappingId?: string;
  erpSystemId?: string;
  entityType?: string;
  licenseiqEntityId?: string;
  companyId?: string;
  businessUnitId?: string;
  locationId?: string;
  filters?: FilterConfig;
  scheduleEnabled: boolean;
  scheduleType?: 'manual' | 'hourly' | 'daily' | 'weekly' | 'monthly';
  scheduleCron?: string;
  lastRunAt?: string;
  nextRunAt?: string;
  importOptions?: {
    dryRunByDefault?: boolean;
    autoCommit?: boolean;
    skipDuplicates?: boolean;
    validateSchema?: boolean;
  };
  status: 'active' | 'inactive' | 'error';
  lastError?: string;
  successCount: number;
  failureCount: number;
  createdBy: string;
  createdAt: string;
  updatedAt?: string;
}

interface IntegrationEndpointTemplate {
  id: string;
  erpSystemId: string;
  erpEntityId?: string;
  operationType: string;
  name: string;
  httpMethod: string;
  pathTemplate: string;
  queryDefaults?: Record<string, any>;
  paginationType?: string;
  paginationConfig?: Record<string, any>;
  requestHeaders?: Record<string, any>;
  requestBodyTemplate?: Record<string, any>;
  responseDataPath?: string;
  responseTotalPath?: string;
  responseSchema?: Record<string, any>;
  expectedResponseTimeMs?: number;
  requiresCompanyScope?: boolean;
  samplePayload?: Record<string, any>;
  sampleResponse?: Record<string, any>;
  status: string;
  description?: string;
  createdAt: string;
  updatedAt?: string;
}

export default function ErpIntegration() {
  const [, navigate] = useLocation();
  const { toast } = useToast();
  const [activeTab, setActiveTab] = useState('connections');
  
  // Filters
  const [companyFilter, setCompanyFilter] = useState<string>('');
  const [businessUnitFilter, setBusinessUnitFilter] = useState<string>('');
  const [locationFilter, setLocationFilter] = useState<string>('');
  const [erpSystemFilter, setErpSystemFilter] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<string>('');
  
  // Inline expanded states (replaces dialogs)
  const [expandedVersionHistoryId, setExpandedVersionHistoryId] = useState<string | null>(null);
  const [expandedJobDetailsId, setExpandedJobDetailsId] = useState<string | null>(null);
  const [jobRecordFilter, setJobRecordFilter] = useState<'all' | 'staged' | 'committed' | 'failed'>('all');
  const [selectedMappingForImport, setSelectedMappingForImport] = useState<MappingWithVersion | null>(null);
  
  // Upload state
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [dryRunResult, setDryRunResult] = useState<any | null>(null);
  
  // Connection states
  const [showConnectionForm, setShowConnectionForm] = useState(false);
  const [editingConnection, setEditingConnection] = useState<IntegrationConnection | null>(null);
  const [testingConnectionId, setTestingConnectionId] = useState<string | null>(null);
  const [expandedHealthId, setExpandedHealthId] = useState<string | null>(null);
  
  // Inline test credentials state (replaces popup dialog)
  const [inlineTestCredentials, setInlineTestCredentials] = useState<{
    connectionId: string | null;
    clientSecret: string;
    apiKey: string;
    basicUsername: string;
    basicPassword: string;
  }>({
    connectionId: null,
    clientSecret: '',
    apiKey: '',
    basicUsername: '',
    basicPassword: '',
  });
  
  // Connection form state
  const [connectionForm, setConnectionForm] = useState({
    name: '',
    erpSystemId: '',
    companyId: '',
    businessUnitId: '',
    locationId: '',
    baseUrl: '',
    authType: 'api_key',
    clientId: '',
    clientSecret: '',
    tokenUrl: '',
    authUrl: '',
    scopes: '',
    apiKeyHeader: 'X-API-Key',
    apiKeyLocation: 'header',
    apiKeyValue: '',
    basicUsername: '',
    basicPassword: '',
    healthCheckEndpoint: '',
    description: '',
  });

  // Data Import Source states
  const [showSourceForm, setShowSourceForm] = useState(false);
  const [editingSource, setEditingSource] = useState<DataImportSource | null>(null);
  const [sourceRunningId, setSourceRunningId] = useState<string | null>(null);
  const [sourceUploadFile, setSourceUploadFile] = useState<File | null>(null);
  
  // Data Import Source form state
  const [sourceForm, setSourceForm] = useState({
    name: '',
    description: '',
    sourceType: 'file' as 'file' | 'api',
    connectionId: '',
    endpointTemplateId: '',
    apiEndpointUrl: '',
    mappingId: '',
    erpSystemId: '',
    entityType: '',
    companyId: '',
    businessUnitId: '',
    locationId: '',
    scheduleEnabled: false,
    scheduleType: 'manual' as 'manual' | 'hourly' | 'daily' | 'weekly' | 'monthly',
    dryRunByDefault: true,
    autoCommit: false,
    skipDuplicates: true,
    filters: null as FilterConfig | null,
  });
  
  // Filter Builder state
  const [showFilters, setShowFilters] = useState(false);
  const [sourceFilterFields, setSourceFilterFields] = useState<{ name: string; label: string; dataType: 'text' | 'number' | 'date' | 'boolean' }[]>([]);

  // ERP Catalog state
  const [catalogTab, setCatalogTab] = useState<'systems' | 'entities' | 'data'>('systems');
  const [showSystemForm, setShowSystemForm] = useState(false);
  const [editingSystem, setEditingSystem] = useState<ErpSystem | null>(null);
  const [showEntityForm, setShowEntityForm] = useState(false);
  const [editingEntity, setEditingEntity] = useState<any>(null);
  const [selectedSystemId, setSelectedSystemId] = useState<string>('');
  
  // ERP Catalog pagination/sorting
  const [systemPage, setSystemPage] = useState(1);
  const [systemPageSize, setSystemPageSize] = useState(10);
  const [systemSortColumn, setSystemSortColumn] = useState('name');
  const [systemSortDirection, setSystemSortDirection] = useState<'asc' | 'desc'>('asc');
  const [systemSearchTerm, setSystemSearchTerm] = useState('');
  
  const [entityPage, setEntityPage] = useState(1);
  const [entityPageSize, setEntityPageSize] = useState(10);
  const [entitySortColumn, setEntitySortColumn] = useState('name');
  const [entitySortDirection, setEntitySortDirection] = useState<'asc' | 'desc'>('asc');
  const [entitySearchTerm, setEntitySearchTerm] = useState('');
  
  // ERP Catalog forms
  const [systemForm, setSystemForm] = useState({
    name: '',
    vendor: '',
    version: '',
    description: '',
    category: 'enterprise',
    status: 'active',
  });
  
  const [entityForm, setEntityForm] = useState({
    systemId: '',
    name: '',
    technicalName: '',
    entityType: '',
    description: '',
    status: 'active',
  });

  // ERP Entity Data state
  const [dataEntityId, setDataEntityId] = useState<string>('');
  const [showDataRecordForm, setShowDataRecordForm] = useState(false);
  const [editingDataRecord, setEditingDataRecord] = useState<any>(null);
  const [dataRecordForm, setDataRecordForm] = useState<Record<string, string>>({});
  const [dataPage, setDataPage] = useState(1);
  const [dataPageSize, setDataPageSize] = useState(10);

  // Fetch companies for context filtering
  const { data: companiesData } = useQuery<Company[]>({
    queryKey: ['/api/master-data/companies'],
  });

  // Fetch business units (filtered by selected company)
  const { data: businessUnitsData } = useQuery<BusinessUnit[]>({
    queryKey: ['/api/master-data/business-units', companyFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (companyFilter) params.append('companyId', companyFilter);
      const response = await fetch(`/api/master-data/business-units?${params.toString()}`);
      return response.json();
    },
    enabled: !!companyFilter,
  });

  // Fetch locations (filtered by selected business unit or company)
  const { data: locationsData } = useQuery<Location[]>({
    queryKey: ['/api/master-data/locations', companyFilter, businessUnitFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (businessUnitFilter) params.append('businessUnitId', businessUnitFilter);
      else if (companyFilter) params.append('companyId', companyFilter);
      const response = await fetch(`/api/master-data/locations?${params.toString()}`);
      return response.json();
    },
    enabled: !!companyFilter,
  });

  // Fetch ERP systems
  const { data: erpSystemsData, refetch: refetchErpSystems } = useQuery<{ systems: ErpSystem[] }>({
    queryKey: ['/api/erp-systems'],
  });

  // Fetch ERP entities (filtered by selected system)
  const { data: erpEntitiesData, refetch: refetchErpEntities } = useQuery<{ entities: any[] }>({
    queryKey: ['/api/erp-entities', selectedSystemId],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedSystemId) params.append('systemId', selectedSystemId);
      const response = await fetch(`/api/erp-entities?${params.toString()}`);
      return response.json();
    },
  });

  // Fetch ERP fields for selected entity (for Data tab)
  const { data: erpFieldsData, isLoading: isLoadingFields } = useQuery<{ fields: any[] }>({
    queryKey: ['/api/erp-fields', dataEntityId],
    queryFn: async () => {
      if (!dataEntityId) return { fields: [] };
      const response = await fetch(`/api/erp-fields?entityId=${dataEntityId}`);
      return response.json();
    },
    enabled: !!dataEntityId,
  });

  // Fetch ERP entity records for selected entity
  const { data: erpRecordsData, refetch: refetchErpRecords, isLoading: isLoadingRecords } = useQuery<{ records: any[] }>({
    queryKey: ['/api/erp-entity-records', dataEntityId],
    queryFn: async () => {
      if (!dataEntityId) return { records: [] };
      const response = await fetch(`/api/erp-entity-records?entityId=${dataEntityId}`);
      return response.json();
    },
    enabled: !!dataEntityId,
  });

  // ERP System mutations
  const createSystemMutation = useMutation({
    mutationFn: (data: typeof systemForm) => apiRequest('POST', '/api/erp-systems', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-systems'] });
      setShowSystemForm(false);
      setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });
      toast({ title: 'Success', description: 'ERP System created successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to create ERP System', variant: 'destructive' });
    },
  });

  const updateSystemMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: typeof systemForm }) => apiRequest('PATCH', `/api/erp-systems/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-systems'] });
      setShowSystemForm(false);
      setEditingSystem(null);
      setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });
      toast({ title: 'Success', description: 'ERP System updated successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to update ERP System', variant: 'destructive' });
    },
  });

  const deleteSystemMutation = useMutation({
    mutationFn: (id: string) => apiRequest('DELETE', `/api/erp-systems/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-systems'] });
      toast({ title: 'Success', description: 'ERP System deleted successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to delete ERP System', variant: 'destructive' });
    },
  });

  // ERP Entity mutations
  const createEntityMutation = useMutation({
    mutationFn: (data: typeof entityForm) => apiRequest('POST', '/api/erp-entities', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entities'] });
      setShowEntityForm(false);
      setEntityForm({ systemId: '', name: '', technicalName: '', entityType: '', description: '', status: 'active' });
      toast({ title: 'Success', description: 'ERP Entity created successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to create ERP Entity', variant: 'destructive' });
    },
  });

  const updateEntityMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: typeof entityForm }) => apiRequest('PATCH', `/api/erp-entities/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entities'] });
      setShowEntityForm(false);
      setEditingEntity(null);
      setEntityForm({ systemId: '', name: '', technicalName: '', entityType: '', description: '', status: 'active' });
      toast({ title: 'Success', description: 'ERP Entity updated successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to update ERP Entity', variant: 'destructive' });
    },
  });

  const deleteEntityMutation = useMutation({
    mutationFn: (id: string) => apiRequest('DELETE', `/api/erp-entities/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entities'] });
      toast({ title: 'Success', description: 'ERP Entity deleted successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to delete ERP Entity', variant: 'destructive' });
    },
  });

  // ERP Entity Record mutations (Data tab)
  const createRecordMutation = useMutation({
    mutationFn: (data: { entityId: string; data: Record<string, any> }) => apiRequest('POST', '/api/erp-entity-records', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityId] });
      setShowDataRecordForm(false);
      setDataRecordForm({});
      toast({ title: 'Success', description: 'Record created successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to create record', variant: 'destructive' });
    },
  });

  const updateRecordMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Record<string, any> }) => apiRequest('PATCH', `/api/erp-entity-records/${id}`, { data }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityId] });
      setShowDataRecordForm(false);
      setEditingDataRecord(null);
      setDataRecordForm({});
      toast({ title: 'Success', description: 'Record updated successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to update record', variant: 'destructive' });
    },
  });

  const deleteRecordMutation = useMutation({
    mutationFn: (id: string) => apiRequest('DELETE', `/api/erp-entity-records/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/erp-entity-records', dataEntityId] });
      toast({ title: 'Success', description: 'Record deleted successfully' });
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to delete record', variant: 'destructive' });
    },
  });

  // Form-specific queries for BU/Location dropdowns
  const { data: formBusinessUnitsData } = useQuery<BusinessUnit[]>({
    queryKey: ['/api/master-data/business-units', 'form', connectionForm.companyId],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (connectionForm.companyId) params.append('companyId', connectionForm.companyId);
      const response = await fetch(`/api/master-data/business-units?${params.toString()}`);
      return response.json();
    },
    enabled: !!connectionForm.companyId && showConnectionForm,
  });

  const { data: formLocationsData } = useQuery<Location[]>({
    queryKey: ['/api/master-data/locations', 'form', connectionForm.companyId, connectionForm.businessUnitId],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (connectionForm.businessUnitId) params.append('businessUnitId', connectionForm.businessUnitId);
      else if (connectionForm.companyId) params.append('companyId', connectionForm.companyId);
      const response = await fetch(`/api/master-data/locations?${params.toString()}`);
      return response.json();
    },
    enabled: !!connectionForm.companyId && showConnectionForm,
  });
  
  // Normalize data for easier access
  const companies = companiesData || [];
  const businessUnits = businessUnitsData || [];
  const locations = locationsData || [];
  const erpSystems = erpSystemsData?.systems || [];
  const erpEntities = erpEntitiesData?.entities || [];
  const erpFields = erpFieldsData?.fields || [];
  const erpRecords = erpRecordsData?.records || [];
  const formBusinessUnits = formBusinessUnitsData || [];
  const formLocations = formLocationsData || [];

  // Computed data for ERP Catalog with sorting/filtering/pagination
  const filteredSystems = erpSystems
    .filter(sys => !systemSearchTerm || 
      sys.name.toLowerCase().includes(systemSearchTerm.toLowerCase()) ||
      sys.vendor.toLowerCase().includes(systemSearchTerm.toLowerCase())
    )
    .sort((a: any, b: any) => {
      const aVal = a[systemSortColumn] || '';
      const bVal = b[systemSortColumn] || '';
      const cmp = String(aVal).localeCompare(String(bVal));
      return systemSortDirection === 'asc' ? cmp : -cmp;
    });
  
  const paginatedSystems = filteredSystems.slice(
    (systemPage - 1) * systemPageSize,
    systemPage * systemPageSize
  );
  const totalSystemPages = Math.ceil(filteredSystems.length / systemPageSize);

  const filteredEntities = erpEntities
    .filter((ent: any) => !entitySearchTerm || 
      ent.name.toLowerCase().includes(entitySearchTerm.toLowerCase()) ||
      ent.technicalName?.toLowerCase().includes(entitySearchTerm.toLowerCase())
    )
    .sort((a: any, b: any) => {
      const aVal = a[entitySortColumn] || '';
      const bVal = b[entitySortColumn] || '';
      const cmp = String(aVal).localeCompare(String(bVal));
      return entitySortDirection === 'asc' ? cmp : -cmp;
    });
  
  const paginatedEntities = filteredEntities.slice(
    (entityPage - 1) * entityPageSize,
    entityPage * entityPageSize
  );
  const totalEntityPages = Math.ceil(filteredEntities.length / entityPageSize);

  // Pagination for Data tab records
  const paginatedRecords = erpRecords.slice(
    (dataPage - 1) * dataPageSize,
    dataPage * dataPageSize
  );
  const totalDataPages = Math.ceil(erpRecords.length / dataPageSize);

  // Get selected entity info for Data tab
  const selectedDataEntity = erpEntities.find((e: any) => e.id === dataEntityId);

  // Cascading filter reset: when company changes, reset BU and location
  const handleCompanyChange = (value: string) => {
    const newCompanyId = value === 'all' ? '' : value;
    setCompanyFilter(newCompanyId);
    setBusinessUnitFilter('');
    setLocationFilter('');
  };

  // When BU changes, reset location
  const handleBusinessUnitChange = (value: string) => {
    const newBuId = value === 'all' ? '' : value;
    setBusinessUnitFilter(newBuId);
    setLocationFilter('');
  };

  // Location change
  const handleLocationChange = (value: string) => {
    setLocationFilter(value === 'all' ? '' : value);
  };

  // Fetch mappings with filters
  const { data: mappingsData, refetch: refetchMappings, isLoading: mappingsLoading } = useQuery<{ mappings: MappingWithVersion[]; total: number }>({
    queryKey: ['/api/erp/mappings', companyFilter, businessUnitFilter, locationFilter, erpSystemFilter, statusFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (companyFilter) params.append('companyId', companyFilter);
      if (businessUnitFilter) params.append('businessUnitId', businessUnitFilter);
      if (locationFilter) params.append('locationId', locationFilter);
      if (erpSystemFilter) params.append('erpSystemId', erpSystemFilter);
      if (statusFilter) params.append('status', statusFilter);
      params.append('latestVersionOnly', 'true');
      const response = await fetch(`/api/erp/mappings?${params.toString()}`);
      return response.json();
    },
  });

  // Fetch version history for expanded mapping
  const { data: versionHistoryData, isLoading: versionsLoading } = useQuery<{ versions: MappingWithVersion[]; total: number }>({
    queryKey: ['/api/erp/mappings', expandedVersionHistoryId, 'versions'],
    queryFn: async () => {
      const response = await fetch(`/api/erp/mappings/${expandedVersionHistoryId}/versions`);
      return response.json();
    },
    enabled: !!expandedVersionHistoryId,
  });

  // Fetch import jobs with filters
  const { data: jobsData, refetch: refetchJobs, isLoading: jobsLoading } = useQuery<{ jobs: ImportJob[]; total: number }>({
    queryKey: ['/api/erp/import/jobs', companyFilter, businessUnitFilter, locationFilter, erpSystemFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (companyFilter) params.append('companyId', companyFilter);
      if (businessUnitFilter) params.append('businessUnitId', businessUnitFilter);
      if (locationFilter) params.append('locationId', locationFilter);
      if (erpSystemFilter) params.append('erpSystemId', erpSystemFilter);
      const response = await fetch(`/api/erp/import/jobs?${params.toString()}`);
      return response.json();
    },
  });

  // Fetch job details with records
  const { data: jobDetailsData, isLoading: jobDetailsLoading } = useQuery<{ job: ImportJob; records: ImportRecord[]; summary: any }>({
    queryKey: ['/api/erp/import/jobs', expandedJobDetailsId, 'details'],
    queryFn: async () => {
      const response = await fetch(`/api/erp/import/jobs/${expandedJobDetailsId}`);
      return response.json();
    },
    enabled: !!expandedJobDetailsId,
  });

  // Fetch user's active context for auto-populating forms
  const { data: activeContextData } = useQuery<{ activeContext: any }>({
    queryKey: ['/api/user/active-context'],
  });
  const activeContext = activeContextData?.activeContext;

  // Fetch integration connections with all filters
  const { data: connectionsData, refetch: refetchConnections, isLoading: connectionsLoading } = useQuery<IntegrationConnection[]>({
    queryKey: ['/api/integration-connections', erpSystemFilter, companyFilter, businessUnitFilter, locationFilter, statusFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (erpSystemFilter) params.append('erpSystemId', erpSystemFilter);
      if (companyFilter) params.append('companyId', companyFilter);
      if (businessUnitFilter) params.append('businessUnitId', businessUnitFilter);
      if (locationFilter) params.append('locationId', locationFilter);
      if (statusFilter) params.append('status', statusFilter);
      const response = await fetch(`/api/integration-connections?${params.toString()}`);
      return response.json();
    },
  });

  // Fetch health events for expanded connection
  const { data: healthEventsData, isLoading: healthLoading } = useQuery<HealthEvent[]>({
    queryKey: ['/api/integration-connections', expandedHealthId, 'health'],
    queryFn: async () => {
      const response = await fetch(`/api/integration-connections/${expandedHealthId}/health?limit=20`);
      return response.json();
    },
    enabled: !!expandedHealthId,
  });

  // Fetch data import sources with filters
  const { data: sourcesData, refetch: refetchSources, isLoading: sourcesLoading } = useQuery<{ sources: DataImportSource[]; total: number }>({
    queryKey: ['/api/erp/import/sources', companyFilter, businessUnitFilter, locationFilter, erpSystemFilter, statusFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (companyFilter) params.append('companyId', companyFilter);
      if (businessUnitFilter) params.append('businessUnitId', businessUnitFilter);
      if (locationFilter) params.append('locationId', locationFilter);
      if (erpSystemFilter) params.append('erpSystemId', erpSystemFilter);
      if (statusFilter) params.append('status', statusFilter);
      const response = await fetch(`/api/erp/import/sources?${params.toString()}`);
      return response.json();
    },
  });

  // Fetch endpoint templates for selected ERP system
  const { data: endpointTemplatesData } = useQuery<{ templates: IntegrationEndpointTemplate[]; total: number }>({
    queryKey: ['/api/erp/endpoint-templates', sourceForm.erpSystemId],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (sourceForm.erpSystemId) params.append('erpSystemId', sourceForm.erpSystemId);
      params.append('operationType', 'list'); // Only fetch list operations for data import
      const response = await fetch(`/api/erp/endpoint-templates?${params.toString()}`);
      return response.json();
    },
    enabled: sourceForm.sourceType === 'api' && !!sourceForm.erpSystemId,
  });

  const endpointTemplates = endpointTemplatesData?.templates || [];

  // Get selected connection details for API configuration display
  const selectedConnection = (connectionsData || []).find(c => c.id === sourceForm.connectionId);
  const selectedEndpointTemplate = endpointTemplates.find(t => t.id === sourceForm.endpointTemplateId);

  // Dynamically populate filter fields when mapping is selected
  useEffect(() => {
    if (!sourceForm.mappingId || !mappingsData?.mappings) {
      setSourceFilterFields([]);
      return;
    }

    const selectedMapping = mappingsData.mappings.find(m => m.id === sourceForm.mappingId);
    if (!selectedMapping || !selectedMapping.sourceSchema) {
      setSourceFilterFields([]);
      return;
    }

    // Extract source fields from the mapping's sourceSchema
    const sourceSchema = selectedMapping.sourceSchema as any;
    const fields: { name: string; label: string; dataType: 'text' | 'number' | 'date' | 'boolean' }[] = [];

    // Handle different sourceSchema formats
    if (Array.isArray(sourceSchema)) {
      // Format: [{ name: "field_name", type: "string", ... }]
      sourceSchema.forEach((field: any) => {
        const fieldName = field.name || field.field || field.column;
        if (fieldName) {
          fields.push({
            name: fieldName,
            label: field.label || field.displayName || fieldName.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase()),
            dataType: inferDataType(field.type || field.dataType || 'string'),
          });
        }
      });
    } else if (typeof sourceSchema === 'object') {
      // Format: { fields: [...] } or { field_name: { type: "string" } }
      const schemaFields = sourceSchema.fields || sourceSchema;
      if (Array.isArray(schemaFields)) {
        schemaFields.forEach((field: any) => {
          const fieldName = field.name || field.field || field.column;
          if (fieldName) {
            fields.push({
              name: fieldName,
              label: field.label || field.displayName || fieldName.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase()),
              dataType: inferDataType(field.type || field.dataType || 'string'),
            });
          }
        });
      } else {
        // Object format: { field_name: { type: "string" } }
        Object.entries(schemaFields).forEach(([fieldName, fieldDef]: [string, any]) => {
          if (fieldName && typeof fieldDef === 'object') {
            fields.push({
              name: fieldName,
              label: fieldDef.label || fieldDef.displayName || fieldName.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase()),
              dataType: inferDataType(fieldDef.type || fieldDef.dataType || 'string'),
            });
          }
        });
      }
    }

    setSourceFilterFields(fields);
  }, [sourceForm.mappingId, mappingsData?.mappings]);

  // Helper function to infer data type from schema type
  function inferDataType(type: string): 'text' | 'number' | 'date' | 'boolean' {
    const typeStr = (type || '').toLowerCase();
    if (typeStr.includes('int') || typeStr.includes('float') || typeStr.includes('double') || typeStr.includes('decimal') || typeStr.includes('number') || typeStr.includes('numeric') || typeStr.includes('price') || typeStr.includes('amount')) {
      return 'number';
    }
    if (typeStr.includes('date') || typeStr.includes('time') || typeStr.includes('timestamp')) {
      return 'date';
    }
    if (typeStr.includes('bool') || typeStr.includes('flag') || typeStr.includes('active') || typeStr.includes('enabled')) {
      return 'boolean';
    }
    return 'text';
  }

  // Create connection mutation
  const createConnectionMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await apiRequest('POST', '/api/integration-connections', data);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Connection Created', description: 'The integration connection has been created.' });
      refetchConnections();
      setShowConnectionForm(false);
      resetConnectionForm();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to create connection', variant: 'destructive' });
    },
  });

  // Update connection mutation
  const updateConnectionMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      const response = await apiRequest('PATCH', `/api/integration-connections/${id}`, data);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Connection Updated', description: 'The integration connection has been updated.' });
      refetchConnections();
      setShowConnectionForm(false);
      setEditingConnection(null);
      resetConnectionForm();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to update connection', variant: 'destructive' });
    },
  });

  // Delete connection mutation
  const deleteConnectionMutation = useMutation({
    mutationFn: async (id: string) => {
      const response = await apiRequest('DELETE', `/api/integration-connections/${id}`);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Connection Deleted', description: 'The integration connection has been deleted.' });
      refetchConnections();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to delete connection', variant: 'destructive' });
    },
  });

  // Test connection mutation - supports optional credentials for full auth testing
  const testConnectionMutation = useMutation({
    mutationFn: async ({ id, credentials }: { id: string; credentials?: { clientSecret?: string; apiKey?: string; basicUsername?: string; basicPassword?: string } }) => {
      setTestingConnectionId(id);
      const response = await apiRequest('POST', `/api/integration-connections/${id}/test`, credentials || {});
      return response.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        toast({ title: 'Connection Healthy', description: data.message });
      } else {
        toast({ title: 'Connection Failed', description: data.message, variant: 'destructive' });
      }
      refetchConnections();
      setTestingConnectionId(null);
      setInlineTestCredentials({ connectionId: null, clientSecret: '', apiKey: '', basicUsername: '', basicPassword: '' });
    },
    onError: (error: any) => {
      toast({ title: 'Test Failed', description: error.message || 'Failed to test connection', variant: 'destructive' });
      setTestingConnectionId(null);
    },
  });
  
  // Open inline test credentials form for connections that need authentication
  const openInlineTestCredentials = (conn: IntegrationConnection) => {
    setExpandedHealthId(conn.id); // Expand the collapsible to show the credentials form
    setInlineTestCredentials({
      connectionId: conn.id,
      clientSecret: '',
      apiKey: '',
      basicUsername: '',
      basicPassword: '',
    });
  };
  
  // Handle test with inline credentials
  const handleTestWithInlineCredentials = (connId: string) => {
    const credentials: { clientSecret?: string; apiKey?: string; basicUsername?: string; basicPassword?: string } = {};
    if (inlineTestCredentials.clientSecret) credentials.clientSecret = inlineTestCredentials.clientSecret;
    if (inlineTestCredentials.apiKey) credentials.apiKey = inlineTestCredentials.apiKey;
    if (inlineTestCredentials.basicUsername) credentials.basicUsername = inlineTestCredentials.basicUsername;
    if (inlineTestCredentials.basicPassword) credentials.basicPassword = inlineTestCredentials.basicPassword;
    
    testConnectionMutation.mutate({ id: connId, credentials });
  };
  
  // Cancel inline test and reset state
  const cancelInlineTest = () => {
    setInlineTestCredentials({ connectionId: null, clientSecret: '', apiKey: '', basicUsername: '', basicPassword: '' });
  };

  // Helper functions for connection form
  const resetConnectionForm = () => {
    setConnectionForm({
      name: '',
      erpSystemId: '',
      companyId: '',
      businessUnitId: '',
      locationId: '',
      baseUrl: '',
      authType: 'api_key',
      clientId: '',
      clientSecret: '',
      tokenUrl: '',
      authUrl: '',
      scopes: '',
      apiKeyHeader: 'X-API-Key',
      apiKeyLocation: 'header',
      apiKeyValue: '',
      basicUsername: '',
      basicPassword: '',
      healthCheckEndpoint: '',
      description: '',
    });
  };

  const openEditConnection = (conn: IntegrationConnection) => {
    setEditingConnection(conn);
    setConnectionForm({
      name: conn.name,
      erpSystemId: conn.erpSystemId,
      companyId: conn.companyId || '',
      businessUnitId: conn.businessUnitId || '',
      locationId: conn.locationId || '',
      baseUrl: conn.baseUrl,
      authType: conn.authType,
      clientId: conn.clientId || '',
      clientSecret: '',
      tokenUrl: conn.tokenUrl || '',
      authUrl: conn.authUrl || '',
      scopes: conn.scopes || '',
      apiKeyHeader: conn.apiKeyHeader || 'X-API-Key',
      apiKeyLocation: conn.apiKeyLocation || 'header',
      apiKeyValue: '',
      basicUsername: conn.basicUsername || '',
      basicPassword: '',
      healthCheckEndpoint: conn.healthCheckEndpoint || '',
      description: conn.description || '',
    });
    setShowConnectionForm(true);
  };

  // Handle creating a new connection with inline form
  // Pre-populate from filters, or fall back to user's active context
  const handleCreateConnection = () => {
    setEditingConnection(null);
    setConnectionForm({
      name: '',
      erpSystemId: erpSystemFilter || '',
      companyId: companyFilter || activeContext?.companyId || '',
      businessUnitId: businessUnitFilter || activeContext?.businessUnitId || '',
      locationId: locationFilter || activeContext?.locationId || '',
      baseUrl: '',
      authType: 'api_key',
      clientId: '',
      clientSecret: '',
      tokenUrl: '',
      authUrl: '',
      scopes: '',
      apiKeyHeader: 'X-API-Key',
      apiKeyLocation: 'header',
      apiKeyValue: '',
      basicUsername: '',
      basicPassword: '',
      healthCheckEndpoint: '',
      description: '',
    });
    setShowConnectionForm(true);
  };

  const handleConnectionSubmit = () => {
    const { clientSecret, apiKeyValue, basicPassword, ...restForm } = connectionForm;
    
    const data: any = {
      ...restForm,
      companyId: connectionForm.companyId || undefined,
      businessUnitId: connectionForm.businessUnitId || undefined,
      locationId: connectionForm.locationId || undefined,
    };
    
    if (editingConnection) {
      if (clientSecret) data.clientSecret = clientSecret;
      if (apiKeyValue) data.apiKeyValue = apiKeyValue;
      if (basicPassword) data.basicPassword = basicPassword;
      updateConnectionMutation.mutate({ id: editingConnection.id, data });
    } else {
      data.clientSecret = clientSecret;
      data.apiKeyValue = apiKeyValue;
      data.basicPassword = basicPassword;
      createConnectionMutation.mutate(data);
    }
  };

  // Approve mapping mutation
  const approveMutation = useMutation({
    mutationFn: async (mappingId: string) => {
      const response = await apiRequest('PUT', `/api/erp/mappings/${mappingId}/approve`);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: 'Mapping Approved',
        description: 'The mapping has been approved and is now active.',
      });
      refetchMappings();
    },
    onError: (error: any) => {
      toast({
        title: 'Approval Failed',
        description: error.message || 'Failed to approve mapping',
        variant: 'destructive',
      });
    },
  });

  // Deprecate mapping mutation
  const deprecateMutation = useMutation({
    mutationFn: async (mappingId: string) => {
      const response = await apiRequest('PUT', `/api/erp/mappings/${mappingId}/deprecate`);
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: 'Mapping Deprecated',
        description: 'The mapping has been marked as deprecated.',
      });
      refetchMappings();
    },
    onError: (error: any) => {
      toast({
        title: 'Deprecation Failed',
        description: error.message || 'Failed to deprecate mapping',
        variant: 'destructive',
      });
    },
  });

  // Revert mapping mutation
  const revertMutation = useMutation({
    mutationFn: async ({ mappingId, targetVersion }: { mappingId: string; targetVersion: number }) => {
      const response = await apiRequest('POST', `/api/erp/mappings/${mappingId}/revert`, { targetVersion });
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: 'Revert Successful',
        description: 'A new version has been created based on the selected version.',
      });
      refetchMappings();
      setExpandedVersionHistoryId(null);
    },
    onError: (error: any) => {
      toast({
        title: 'Revert Failed',
        description: error.message || 'Failed to revert mapping',
        variant: 'destructive',
      });
    },
  });

  // Dry-run import mutation
  const dryRunMutation = useMutation({
    mutationFn: async ({ file, mappingId, companyId, businessUnitId, locationId }: { 
      file: File; 
      mappingId: string; 
      companyId?: string;
      businessUnitId?: string;
      locationId?: string;
    }) => {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('mappingId', mappingId);
      if (companyId) formData.append('companyId', companyId);
      if (businessUnitId) formData.append('businessUnitId', businessUnitId);
      if (locationId) formData.append('locationId', locationId);
      
      const response = await fetch('/api/erp/import/dry-run', {
        method: 'POST',
        body: formData,
        credentials: 'include',
      });
      
      if (!response.ok) {
        throw new Error('Dry-run failed');
      }
      
      return response.json();
    },
    onSuccess: (data) => {
      setDryRunResult(data);
      toast({
        title: 'Dry-Run Complete',
        description: `${data.preview.staged} records staged for review.`,
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Dry-Run Failed',
        description: error.message || 'Failed to process dry-run',
        variant: 'destructive',
      });
    },
  });

  // Commit job mutation
  const commitMutation = useMutation({
    mutationFn: async (jobId: string) => {
      const response = await apiRequest('POST', `/api/erp/import/jobs/${jobId}/commit`);
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: 'Import Committed',
        description: `${data.committed} records committed successfully.`,
      });
      setDryRunResult(null);
      setSelectedMappingForImport(null);
      setUploadFile(null);
      refetchJobs();
    },
    onError: (error: any) => {
      toast({
        title: 'Commit Failed',
        description: error.message || 'Failed to commit import',
        variant: 'destructive',
      });
    },
  });

  // Discard job mutation
  const discardMutation = useMutation({
    mutationFn: async (jobId: string) => {
      const response = await apiRequest('POST', `/api/erp/import/jobs/${jobId}/discard`);
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: 'Import Discarded',
        description: `${data.discarded} staged records discarded.`,
      });
      setDryRunResult(null);
      setSelectedMappingForImport(null);
      setUploadFile(null);
      refetchJobs();
    },
    onError: (error: any) => {
      toast({
        title: 'Discard Failed',
        description: error.message || 'Failed to discard import',
        variant: 'destructive',
      });
    },
  });

  // Retry failed records mutation
  const retryFailedMutation = useMutation({
    mutationFn: async (jobId: string) => {
      const response = await apiRequest('POST', `/api/erp/import/jobs/${jobId}/retry-failed`);
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: 'Failed Records Reset',
        description: data.message || `${data.retriedCount} records reset to staged.`,
      });
      refetchJobs();
      // Refresh job details if viewing this job
      queryClient.invalidateQueries({ queryKey: ['/api/erp/import/jobs'] });
    },
    onError: (error: any) => {
      toast({
        title: 'Retry Failed',
        description: error.message || 'Failed to retry records',
        variant: 'destructive',
      });
    },
  });

  // Create import source mutation
  const createSourceMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await apiRequest('POST', '/api/erp/import/sources', data);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Import Source Created', description: 'The data import source has been created.' });
      refetchSources();
      setShowSourceForm(false);
      resetSourceForm();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to create import source', variant: 'destructive' });
    },
  });

  // Update import source mutation
  const updateSourceMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      const response = await apiRequest('PATCH', `/api/erp/import/sources/${id}`, data);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Import Source Updated', description: 'The data import source has been updated.' });
      refetchSources();
      setShowSourceForm(false);
      setEditingSource(null);
      resetSourceForm();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to update import source', variant: 'destructive' });
    },
  });

  // Delete import source mutation
  const deleteSourceMutation = useMutation({
    mutationFn: async (id: string) => {
      const response = await apiRequest('DELETE', `/api/erp/import/sources/${id}`);
      return response.json();
    },
    onSuccess: () => {
      toast({ title: 'Import Source Deleted', description: 'The data import source has been deleted.' });
      refetchSources();
    },
    onError: (error: any) => {
      toast({ title: 'Error', description: error.message || 'Failed to delete import source', variant: 'destructive' });
    },
  });

  // Run import source mutation
  const runSourceMutation = useMutation({
    mutationFn: async ({ sourceId, file, dryRun }: { sourceId: string; file?: File; dryRun: boolean }) => {
      const formData = new FormData();
      if (file) formData.append('file', file);
      formData.append('dryRun', String(dryRun));
      
      const response = await fetch(`/api/erp/import/sources/${sourceId}/run`, {
        method: 'POST',
        body: formData,
        credentials: 'include',
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Import failed');
      }
      return response.json();
    },
    onSuccess: (data) => {
      toast({ 
        title: data.dryRun ? 'Dry Run Complete' : 'Import Started', 
        description: `Processed ${data.processedCount || 0} records.` 
      });
      setSourceRunningId(null);
      setSourceUploadFile(null);
      refetchJobs();
      refetchSources();
    },
    onError: (error: any) => {
      toast({ title: 'Import Failed', description: error.message || 'Failed to run import', variant: 'destructive' });
      setSourceRunningId(null);
    },
  });

  // Reset source form helper
  const resetSourceForm = () => {
    setSourceForm({
      name: '',
      description: '',
      sourceType: 'file',
      connectionId: '',
      endpointTemplateId: '',
      apiEndpointUrl: '',
      mappingId: '',
      erpSystemId: '',
      entityType: '',
      companyId: '',
      businessUnitId: '',
      locationId: '',
      scheduleEnabled: false,
      scheduleType: 'manual',
      dryRunByDefault: true,
      autoCommit: false,
      skipDuplicates: true,
      filters: null,
    });
    setShowFilters(false);
    setSourceFilterFields([]);
  };

  // Open source form for editing
  const handleEditSource = (source: DataImportSource) => {
    setEditingSource(source);
    const filters = (source as any).filters || null;
    setSourceForm({
      name: source.name,
      description: source.description || '',
      sourceType: source.sourceType,
      connectionId: source.connectionId || '',
      endpointTemplateId: source.endpointTemplateId || '',
      apiEndpointUrl: '',
      mappingId: source.mappingId || '',
      erpSystemId: source.erpSystemId || '',
      entityType: source.entityType || '',
      companyId: source.companyId || '',
      businessUnitId: source.businessUnitId || '',
      locationId: source.locationId || '',
      scheduleEnabled: source.scheduleEnabled,
      scheduleType: source.scheduleType || 'manual',
      dryRunByDefault: source.importOptions?.dryRunByDefault ?? true,
      autoCommit: source.importOptions?.autoCommit ?? false,
      skipDuplicates: source.importOptions?.skipDuplicates ?? true,
      filters: filters,
    });
    setShowFilters(!!filters?.conditions?.length);
    setShowSourceForm(true);
  };

  // Submit source form
  const handleSubmitSource = () => {
    const data = {
      name: sourceForm.name,
      description: sourceForm.description || undefined,
      sourceType: sourceForm.sourceType,
      connectionId: sourceForm.connectionId || undefined,
      endpointTemplateId: sourceForm.endpointTemplateId || undefined,
      mappingId: sourceForm.mappingId || undefined,
      erpSystemId: sourceForm.erpSystemId || undefined,
      entityType: sourceForm.entityType || undefined,
      companyId: sourceForm.companyId || activeContext?.companyId,
      businessUnitId: sourceForm.businessUnitId || activeContext?.businessUnitId,
      locationId: sourceForm.locationId || activeContext?.locationId,
      scheduleEnabled: sourceForm.scheduleEnabled,
      scheduleType: sourceForm.scheduleType,
      importOptions: {
        dryRunByDefault: sourceForm.dryRunByDefault,
        autoCommit: sourceForm.autoCommit,
        skipDuplicates: sourceForm.skipDuplicates,
      },
      filters: sourceForm.filters,
    };

    if (editingSource) {
      updateSourceMutation.mutate({ id: editingSource.id, data });
    } else {
      createSourceMutation.mutate(data);
    }
  };

  // Dropzone for source file upload
  const onSourceDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setSourceUploadFile(acceptedFiles[0]);
    }
  }, []);

  const { getRootProps: getSourceRootProps, getInputProps: getSourceInputProps, isDragActive: isSourceDragActive } = useDropzone({
    onDrop: onSourceDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
    },
    maxFiles: 1,
  });

  // Dropzone
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setUploadFile(acceptedFiles[0]);
      setDryRunResult(null);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
    },
    maxFiles: 1,
  });

  // Status badge helper
  const getStatusBadge = (status: string) => {
    const variants: Record<string, { variant: 'default' | 'secondary' | 'destructive' | 'outline'; icon: any }> = {
      draft: { variant: 'outline', icon: Clock },
      approved: { variant: 'default', icon: CheckCircle2 },
      deprecated: { variant: 'secondary', icon: XCircle },
      pending: { variant: 'outline', icon: Clock },
      processing: { variant: 'secondary', icon: Loader2 },
      pending_commit: { variant: 'outline', icon: AlertCircle },
      completed: { variant: 'default', icon: CheckCircle2 },
      completed_with_errors: { variant: 'destructive', icon: AlertCircle },
      failed: { variant: 'destructive', icon: XCircle },
      cancelled: { variant: 'secondary', icon: XCircle },
    };
    
    const config = variants[status] || { variant: 'outline' as const, icon: Clock };
    const Icon = config.icon;
    
    return (
      <Badge variant={config.variant} className="gap-1" data-testid={`badge-status-${status}`}>
        <Icon className={`h-3 w-3 ${status === 'processing' ? 'animate-spin' : ''}`} />
        {status.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
      </Badge>
    );
  };

  // Confidence indicator
  const getConfidenceBadge = (confidence?: number) => {
    if (confidence === undefined || confidence === null) return null;
    
    const pct = Math.round(confidence * 100);
    let variant: 'default' | 'secondary' | 'destructive' = 'default';
    if (pct < 70) variant = 'destructive';
    else if (pct < 90) variant = 'secondary';
    
    return (
      <Badge variant={variant} className="gap-1" data-testid="badge-confidence">
        <Sparkles className="h-3 w-3" />
        {pct}% AI
      </Badge>
    );
  };

  const handleToggleVersionHistory = (mappingId: string) => {
    setExpandedVersionHistoryId(prev => prev === mappingId ? null : mappingId);
  };

  const handleOpenImport = (mapping: MappingWithVersion) => {
    setSelectedMappingForImport(mapping);
    setUploadFile(null);
    setDryRunResult(null);
  };

  const handleCloseImport = () => {
    setSelectedMappingForImport(null);
    setUploadFile(null);
    setDryRunResult(null);
  };

  const handleDryRun = () => {
    if (!uploadFile || !selectedMappingForImport) return;
    
    dryRunMutation.mutate({
      file: uploadFile,
      mappingId: selectedMappingForImport.id,
      companyId: companyFilter || undefined,
      businessUnitId: businessUnitFilter || undefined,
      locationId: locationFilter || undefined,
    });
  };

  const handleToggleJobDetails = (jobId: string) => {
    setExpandedJobDetailsId(prev => prev === jobId ? null : jobId);
    setJobRecordFilter('all'); // Reset filter when opening different job
  };

  return (
    <MainLayout title="ERP Integration Hub" description="Manage field mappings and data imports with version control">
      <div className="container mx-auto p-6 space-y-6">
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

        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-3" data-testid="text-page-title">
              <Database className="h-8 w-8 text-primary" />
              ERP Integration Hub
            </h1>
            <p className="text-muted-foreground mt-1">
              Manage field mappings with version control and import data with preview
            </p>
          </div>
          <Button onClick={() => refetchMappings()} variant="outline" data-testid="button-refresh">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>

        {/* Filters */}
        <Card>
          <CardHeader className="pb-4">
            <CardTitle className="text-lg flex items-center gap-2">
              <Building2 className="h-5 w-5" />
              Filters
            </CardTitle>
            <CardDescription>
              Filter by company hierarchy (Company → Business Unit → Location) and ERP system
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-4">
            {/* Company Hierarchy Filters */}
            <div className="w-48">
              <Label>Company</Label>
              <Select value={companyFilter || 'all'} onValueChange={handleCompanyChange}>
                <SelectTrigger data-testid="select-company-filter">
                  <SelectValue placeholder="All companies" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Companies</SelectItem>
                  {companies.map(company => (
                    <SelectItem key={company.id} value={company.id}>
                      {company.companyName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-48">
              <Label>Business Unit</Label>
              <Select 
                value={businessUnitFilter || 'all'} 
                onValueChange={handleBusinessUnitChange}
                disabled={!companyFilter}
              >
                <SelectTrigger data-testid="select-bu-filter">
                  <SelectValue placeholder={companyFilter ? "All Business Units" : "Select company first"} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Business Units</SelectItem>
                  {businessUnits.map(bu => (
                    <SelectItem key={bu.id} value={bu.id}>
                      {bu.orgName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-48">
              <Label>Location</Label>
              <Select 
                value={locationFilter || 'all'} 
                onValueChange={handleLocationChange}
                disabled={!companyFilter}
              >
                <SelectTrigger data-testid="select-location-filter">
                  <SelectValue placeholder={companyFilter ? "All Locations" : "Select company first"} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Locations</SelectItem>
                  {locations.map(loc => (
                    <SelectItem key={loc.id} value={loc.id}>
                      {loc.locName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            <Separator orientation="vertical" className="h-auto hidden md:block" />
            
            {/* ERP System & Status Filters */}
            <div className="w-48">
              <Label>ERP System</Label>
              <Select value={erpSystemFilter || 'all'} onValueChange={(v) => setErpSystemFilter(v === 'all' ? '' : v)}>
                <SelectTrigger data-testid="select-erp-filter">
                  <SelectValue placeholder="All systems" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Systems</SelectItem>
                  {erpSystems.map(system => (
                    <SelectItem key={system.id} value={system.id}>
                      {system.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-48">
              <Label>Status</Label>
              <Select value={statusFilter || 'all'} onValueChange={(v) => setStatusFilter(v === 'all' ? '' : v)}>
                <SelectTrigger data-testid="select-status-filter">
                  <SelectValue placeholder="All statuses" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Statuses</SelectItem>
                  <SelectItem value="draft">Draft</SelectItem>
                  <SelectItem value="approved">Approved</SelectItem>
                  <SelectItem value="deprecated">Deprecated</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>

        {/* Inline Import Section (replaces dialog) */}
        {selectedMappingForImport && (
          <Card className="border-primary" data-testid="card-import-inline">
            <CardHeader className="pb-4">
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2">
                  <Upload className="h-5 w-5" />
                  Import Data
                </CardTitle>
                <Button variant="ghost" size="sm" onClick={handleCloseImport} data-testid="button-close-import">
                  <X className="h-4 w-4" />
                </Button>
              </div>
              <CardDescription>
                Upload a file to preview data transformation before committing.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Selected Mapping Info */}
              <div className="flex items-center gap-4 p-3 bg-muted rounded-lg">
                <div>
                  <p className="font-medium">{selectedMappingForImport.mappingName}</p>
                  <p className="text-sm text-muted-foreground">
                    {selectedMappingForImport.erpSystem} → {selectedMappingForImport.entityType}
                  </p>
                </div>
                <Badge variant="secondary" className="ml-auto">
                  v{selectedMappingForImport.version}
                </Badge>
              </div>

              {/* File Upload */}
              <div
                {...getRootProps()}
                className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors ${
                  isDragActive ? 'border-primary bg-primary/5' : 'border-muted-foreground/25 hover:border-primary/50'
                }`}
              >
                <input {...getInputProps()} data-testid="input-file-upload" />
                <Upload className="h-10 w-10 mx-auto mb-4 text-muted-foreground" />
                {uploadFile ? (
                  <div>
                    <p className="font-medium">{uploadFile.name}</p>
                    <p className="text-sm text-muted-foreground">
                      {(uploadFile.size / 1024).toFixed(2)} KB
                    </p>
                  </div>
                ) : (
                  <div>
                    <p className="font-medium">Drop a file here or click to browse</p>
                    <p className="text-sm text-muted-foreground">Supports CSV and Excel files</p>
                  </div>
                )}
              </div>

              {/* Dry Run Button */}
              {uploadFile && !dryRunResult && (
                <Button
                  className="w-full"
                  onClick={handleDryRun}
                  disabled={dryRunMutation.isPending}
                  data-testid="button-dry-run"
                >
                  {dryRunMutation.isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Processing Preview...
                    </>
                  ) : (
                    <>
                      <Eye className="h-4 w-4 mr-2" />
                      Preview Import (Dry Run)
                    </>
                  )}
                </Button>
              )}

              {/* Dry Run Results */}
              {dryRunResult && (
                <div className="space-y-4">
                  <Separator />
                  <div className="flex items-center justify-between">
                    <h3 className="font-semibold">Preview Results</h3>
                    <Badge variant="outline" className="gap-1">
                      {dryRunResult.preview.staged} records staged
                    </Badge>
                  </div>

                  {/* Sample Records */}
                  <ScrollArea className="h-[300px] border rounded-lg">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-[50%]">Source Record</TableHead>
                          <TableHead className="w-[50%]">Transformed Record</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {dryRunResult.preview.sampleRecords.map((record: any, idx: number) => (
                          <TableRow key={idx}>
                            <TableCell className="align-top">
                              <pre className="text-xs whitespace-pre-wrap bg-muted p-2 rounded">
                                {JSON.stringify(record.source, null, 2)}
                              </pre>
                            </TableCell>
                            <TableCell className="align-top">
                              <pre className="text-xs whitespace-pre-wrap bg-green-50 dark:bg-green-950/30 p-2 rounded">
                                {JSON.stringify(record.target, null, 2)}
                              </pre>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </ScrollArea>

                  {/* Action Buttons */}
                  <div className="flex gap-3">
                    <Button
                      className="flex-1"
                      onClick={() => commitMutation.mutate(dryRunResult.job.id)}
                      disabled={commitMutation.isPending}
                      data-testid="button-commit-import"
                    >
                      {commitMutation.isPending ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Committing...
                        </>
                      ) : (
                        <>
                          <CheckCircle2 className="h-4 w-4 mr-2" />
                          Commit All Records
                        </>
                      )}
                    </Button>
                    <Button
                      variant="destructive"
                      className="flex-1"
                      onClick={() => discardMutation.mutate(dryRunResult.job.id)}
                      disabled={discardMutation.isPending}
                      data-testid="button-discard-import"
                    >
                      {discardMutation.isPending ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Discarding...
                        </>
                      ) : (
                        <>
                          <Trash2 className="h-4 w-4 mr-2" />
                          Discard All
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Main Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="grid w-full grid-cols-5" data-testid="tabs-main">
            <TabsTrigger value="catalog" className="gap-2" data-testid="tab-catalog">
              <FileJson className="h-4 w-4" />
              ERP Catalog
            </TabsTrigger>
            <TabsTrigger value="connections" className="gap-2" data-testid="tab-connections">
              <Plug className="h-4 w-4" />
              Connections
            </TabsTrigger>
            <TabsTrigger value="mappings" className="gap-2" data-testid="tab-mappings">
              <ArrowLeftRight className="h-4 w-4" />
              Field Mappings
            </TabsTrigger>
            <TabsTrigger value="sources" className="gap-2" data-testid="tab-sources">
              <Database className="h-4 w-4" />
              Data Sources
            </TabsTrigger>
            <TabsTrigger value="imports" className="gap-2" data-testid="tab-imports">
              <Upload className="h-4 w-4" />
              Import Jobs
            </TabsTrigger>
          </TabsList>

          {/* ERP Catalog Tab */}
          <TabsContent value="catalog" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span className="flex items-center gap-2">
                    <FileJson className="h-5 w-5" />
                    ERP Catalog Management
                  </span>
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary">{erpSystems.length} systems</Badge>
                    <Badge variant="outline">{erpEntities.length} entities</Badge>
                  </div>
                </CardTitle>
                <CardDescription>
                  Define and manage your ERP systems, entities, and field definitions for integration mappings
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Tabs value={catalogTab} onValueChange={(v) => setCatalogTab(v as any)}>
                  <TabsList className="grid w-full grid-cols-3 mb-4">
                    <TabsTrigger value="systems" className="gap-2" data-testid="tab-erp-systems">
                      <Database className="h-4 w-4" />
                      ERP Systems
                    </TabsTrigger>
                    <TabsTrigger value="entities" className="gap-2" data-testid="tab-erp-entities">
                      <FileJson className="h-4 w-4" />
                      ERP Entities
                    </TabsTrigger>
                    <TabsTrigger value="data" className="gap-2" data-testid="tab-erp-data">
                      <Table2 className="h-4 w-4" />
                      Entity Data
                    </TabsTrigger>
                  </TabsList>

                  {/* ERP Systems Sub-Tab */}
                  <TabsContent value="systems">
                    <div className="space-y-4">
                      {/* Header with Add Button */}
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <div className="relative">
                            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                            <Input
                              placeholder="Search systems..."
                              value={systemSearchTerm}
                              onChange={(e) => { setSystemSearchTerm(e.target.value); setSystemPage(1); }}
                              className="pl-8 w-64"
                              data-testid="input-search-systems"
                            />
                          </div>
                          <Select value={String(systemPageSize)} onValueChange={(v) => { setSystemPageSize(Number(v)); setSystemPage(1); }}>
                            <SelectTrigger className="w-20" data-testid="select-system-page-size">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="10">10</SelectItem>
                              <SelectItem value="25">25</SelectItem>
                              <SelectItem value="50">50</SelectItem>
                              <SelectItem value="100">100</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <Button 
                          size="sm" 
                          className="gap-2" 
                          onClick={() => {
                            setEditingSystem(null);
                            setSystemForm({ name: '', vendor: '', version: '', description: '', category: 'enterprise', status: 'active' });
                            setShowSystemForm(true);
                          }}
                          data-testid="button-add-system"
                        >
                          <Plus className="h-4 w-4" />
                          Add ERP System
                        </Button>
                      </div>

                      {/* System Form */}
                      {showSystemForm && (
                        <div className="border rounded-lg p-4 bg-muted/30">
                          <div className="flex items-center justify-between mb-4">
                            <h3 className="font-semibold">{editingSystem ? 'Edit ERP System' : 'New ERP System'}</h3>
                            <Button variant="ghost" size="sm" onClick={() => { setShowSystemForm(false); setEditingSystem(null); }}>
                              <X className="h-4 w-4" />
                            </Button>
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label>Name *</Label>
                              <Input
                                value={systemForm.name}
                                onChange={(e) => setSystemForm(prev => ({ ...prev, name: e.target.value }))}
                                placeholder="e.g., Oracle ERP Cloud"
                                data-testid="input-system-name"
                              />
                            </div>
                            <div className="space-y-2">
                              <Label>Vendor *</Label>
                              <Select value={systemForm.vendor} onValueChange={(v) => setSystemForm(prev => ({ ...prev, vendor: v }))}>
                                <SelectTrigger data-testid="select-system-vendor">
                                  <SelectValue placeholder="Select vendor" />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="oracle">Oracle</SelectItem>
                                  <SelectItem value="sap">SAP</SelectItem>
                                  <SelectItem value="microsoft">Microsoft</SelectItem>
                                  <SelectItem value="netsuite">NetSuite</SelectItem>
                                  <SelectItem value="workday">Workday</SelectItem>
                                  <SelectItem value="custom">Custom</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="space-y-2">
                              <Label>Version</Label>
                              <Input
                                value={systemForm.version}
                                onChange={(e) => setSystemForm(prev => ({ ...prev, version: e.target.value }))}
                                placeholder="e.g., 21D, 2023"
                                data-testid="input-system-version"
                              />
                            </div>
                            <div className="space-y-2">
                              <Label>Category</Label>
                              <Select value={systemForm.category} onValueChange={(v) => setSystemForm(prev => ({ ...prev, category: v }))}>
                                <SelectTrigger data-testid="select-system-category">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="enterprise">Enterprise</SelectItem>
                                  <SelectItem value="sme">SME</SelectItem>
                                  <SelectItem value="custom">Custom</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="col-span-2 space-y-2">
                              <Label>Description</Label>
                              <Textarea
                                value={systemForm.description}
                                onChange={(e) => setSystemForm(prev => ({ ...prev, description: e.target.value }))}
                                placeholder="Description of this ERP system..."
                                rows={2}
                                data-testid="input-system-description"
                              />
                            </div>
                          </div>
                          <div className="flex justify-end gap-2 mt-4">
                            <Button variant="outline" onClick={() => { setShowSystemForm(false); setEditingSystem(null); }} data-testid="button-cancel-system">
                              Cancel
                            </Button>
                            <Button
                              onClick={() => {
                                if (editingSystem) {
                                  updateSystemMutation.mutate({ id: editingSystem.id, data: systemForm });
                                } else {
                                  createSystemMutation.mutate(systemForm);
                                }
                              }}
                              disabled={!systemForm.name || !systemForm.vendor || createSystemMutation.isPending || updateSystemMutation.isPending}
                              data-testid="button-save-system"
                            >
                              {createSystemMutation.isPending || updateSystemMutation.isPending ? (
                                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                              ) : null}
                              {editingSystem ? 'Update' : 'Create'}
                            </Button>
                          </div>
                        </div>
                      )}

                      {/* Systems Table */}
                      <div className="border rounded-lg">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead 
                                className="cursor-pointer hover:bg-muted/50"
                                onClick={() => {
                                  if (systemSortColumn === 'name') {
                                    setSystemSortDirection(systemSortDirection === 'asc' ? 'desc' : 'asc');
                                  } else {
                                    setSystemSortColumn('name');
                                    setSystemSortDirection('asc');
                                  }
                                }}
                              >
                                <div className="flex items-center gap-1">
                                  Name
                                  {systemSortColumn === 'name' && (
                                    systemSortDirection === 'asc' ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4 rotate-90" />
                                  )}
                                </div>
                              </TableHead>
                              <TableHead 
                                className="cursor-pointer hover:bg-muted/50"
                                onClick={() => {
                                  if (systemSortColumn === 'vendor') {
                                    setSystemSortDirection(systemSortDirection === 'asc' ? 'desc' : 'asc');
                                  } else {
                                    setSystemSortColumn('vendor');
                                    setSystemSortDirection('asc');
                                  }
                                }}
                              >
                                <div className="flex items-center gap-1">
                                  Vendor
                                  {systemSortColumn === 'vendor' && (
                                    systemSortDirection === 'asc' ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4 rotate-90" />
                                  )}
                                </div>
                              </TableHead>
                              <TableHead>Version</TableHead>
                              <TableHead>Category</TableHead>
                              <TableHead>Status</TableHead>
                              <TableHead className="w-[100px]">Actions</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {paginatedSystems.length === 0 ? (
                              <TableRow>
                                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                                  No ERP systems found. Click "Add ERP System" to create one.
                                </TableCell>
                              </TableRow>
                            ) : (
                              paginatedSystems.map((system) => (
                                <TableRow key={system.id} data-testid={`row-system-${system.id}`}>
                                  <TableCell className="font-medium">{system.name}</TableCell>
                                  <TableCell>
                                    <Badge variant="outline" className="capitalize">{system.vendor}</Badge>
                                  </TableCell>
                                  <TableCell>{system.version || '-'}</TableCell>
                                  <TableCell>
                                    <Badge variant="secondary" className="capitalize">{system.category}</Badge>
                                  </TableCell>
                                  <TableCell>
                                    <Badge variant={system.status === 'active' ? 'default' : 'secondary'}>
                                      {system.status}
                                    </Badge>
                                  </TableCell>
                                  <TableCell>
                                    <div className="flex items-center gap-1">
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        onClick={() => {
                                          setEditingSystem(system);
                                          setSystemForm({
                                            name: system.name,
                                            vendor: system.vendor,
                                            version: system.version || '',
                                            description: system.description || '',
                                            category: system.category || 'enterprise',
                                            status: system.status,
                                          });
                                          setShowSystemForm(true);
                                        }}
                                        data-testid={`button-edit-system-${system.id}`}
                                      >
                                        <Pencil className="h-4 w-4" />
                                      </Button>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        onClick={() => {
                                          if (confirm('Are you sure you want to delete this ERP system? This will also delete all associated entities.')) {
                                            deleteSystemMutation.mutate(system.id);
                                          }
                                        }}
                                        data-testid={`button-delete-system-${system.id}`}
                                      >
                                        <Trash2 className="h-4 w-4 text-destructive" />
                                      </Button>
                                    </div>
                                  </TableCell>
                                </TableRow>
                              ))
                            )}
                          </TableBody>
                        </Table>
                      </div>

                      {/* Pagination */}
                      {filteredSystems.length > 0 && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-muted-foreground" data-testid="text-system-page-info">
                            Showing {((systemPage - 1) * systemPageSize) + 1}-{Math.min(systemPage * systemPageSize, filteredSystems.length)} of {filteredSystems.length}
                          </span>
                          <div className="flex items-center gap-2">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSystemPage(1)}
                              disabled={systemPage === 1}
                              data-testid="button-system-first"
                            >
                              First
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSystemPage(p => Math.max(1, p - 1))}
                              disabled={systemPage === 1}
                              data-testid="button-system-previous"
                            >
                              Previous
                            </Button>
                            <span className="text-sm" data-testid="text-system-page-number">Page {systemPage} of {totalSystemPages || 1}</span>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSystemPage(p => Math.min(totalSystemPages, p + 1))}
                              disabled={systemPage >= totalSystemPages}
                              data-testid="button-system-next"
                            >
                              Next
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSystemPage(totalSystemPages)}
                              disabled={systemPage >= totalSystemPages}
                              data-testid="button-system-last"
                            >
                              Last
                            </Button>
                          </div>
                        </div>
                      )}
                    </div>
                  </TabsContent>

                  {/* ERP Entities Sub-Tab */}
                  <TabsContent value="entities">
                    <div className="space-y-4">
                      {/* Header */}
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Select 
                            value={selectedSystemId || 'all'} 
                            onValueChange={(v) => { setSelectedSystemId(v === 'all' ? '' : v); setEntityPage(1); }}
                          >
                            <SelectTrigger className="w-48" data-testid="select-entity-system">
                              <SelectValue placeholder="Filter by system" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="all">All Systems</SelectItem>
                              {erpSystems.map(sys => (
                                <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                          <div className="relative">
                            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                            <Input
                              placeholder="Search entities..."
                              value={entitySearchTerm}
                              onChange={(e) => { setEntitySearchTerm(e.target.value); setEntityPage(1); }}
                              className="pl-8 w-64"
                              data-testid="input-search-entities"
                            />
                          </div>
                          <Select value={String(entityPageSize)} onValueChange={(v) => { setEntityPageSize(Number(v)); setEntityPage(1); }}>
                            <SelectTrigger className="w-20" data-testid="select-entity-page-size">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="10">10</SelectItem>
                              <SelectItem value="25">25</SelectItem>
                              <SelectItem value="50">50</SelectItem>
                              <SelectItem value="100">100</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <Button 
                          size="sm" 
                          className="gap-2" 
                          onClick={() => {
                            setEditingEntity(null);
                            setEntityForm({ systemId: selectedSystemId || '', name: '', technicalName: '', entityType: '', description: '', status: 'active' });
                            setShowEntityForm(true);
                          }}
                          disabled={erpSystems.length === 0}
                          data-testid="button-add-entity"
                        >
                          <Plus className="h-4 w-4" />
                          Add Entity
                        </Button>
                      </div>

                      {/* Entity Form */}
                      {showEntityForm && (
                        <div className="border rounded-lg p-4 bg-muted/30">
                          <div className="flex items-center justify-between mb-4">
                            <h3 className="font-semibold">{editingEntity ? 'Edit Entity' : 'New Entity'}</h3>
                            <Button variant="ghost" size="sm" onClick={() => { setShowEntityForm(false); setEditingEntity(null); }} data-testid="button-close-entity-form">
                              <X className="h-4 w-4" />
                            </Button>
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label>ERP System *</Label>
                              <Select 
                                value={entityForm.systemId} 
                                onValueChange={(v) => setEntityForm(prev => ({ ...prev, systemId: v }))}
                              >
                                <SelectTrigger data-testid="select-entity-form-system">
                                  <SelectValue placeholder="Select system" />
                                </SelectTrigger>
                                <SelectContent>
                                  {erpSystems.map(sys => (
                                    <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="space-y-2">
                              <Label>Entity Type *</Label>
                              <Select 
                                value={entityForm.entityType} 
                                onValueChange={(v) => setEntityForm(prev => ({ ...prev, entityType: v }))}
                              >
                                <SelectTrigger data-testid="select-entity-type">
                                  <SelectValue placeholder="Select type" />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="customers">Customers</SelectItem>
                                  <SelectItem value="items">Items</SelectItem>
                                  <SelectItem value="suppliers">Suppliers</SelectItem>
                                  <SelectItem value="invoices">Invoices</SelectItem>
                                  <SelectItem value="orders">Orders</SelectItem>
                                  <SelectItem value="transactions">Transactions</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="space-y-2">
                              <Label>Display Name *</Label>
                              <Input
                                value={entityForm.name}
                                onChange={(e) => setEntityForm(prev => ({ ...prev, name: e.target.value }))}
                                placeholder="e.g., Customer Master"
                                data-testid="input-entity-name"
                              />
                            </div>
                            <div className="space-y-2">
                              <Label>Technical Name *</Label>
                              <Input
                                value={entityForm.technicalName}
                                onChange={(e) => setEntityForm(prev => ({ ...prev, technicalName: e.target.value }))}
                                placeholder="e.g., AR_CUSTOMERS"
                                data-testid="input-entity-technical-name"
                              />
                            </div>
                            <div className="col-span-2 space-y-2">
                              <Label>Description</Label>
                              <Textarea
                                value={entityForm.description}
                                onChange={(e) => setEntityForm(prev => ({ ...prev, description: e.target.value }))}
                                placeholder="Description of this entity..."
                                rows={2}
                                data-testid="input-entity-description"
                              />
                            </div>
                          </div>
                          <div className="flex justify-end gap-2 mt-4">
                            <Button variant="outline" onClick={() => { setShowEntityForm(false); setEditingEntity(null); }} data-testid="button-cancel-entity">
                              Cancel
                            </Button>
                            <Button
                              onClick={() => {
                                if (editingEntity) {
                                  updateEntityMutation.mutate({ id: editingEntity.id, data: entityForm });
                                } else {
                                  createEntityMutation.mutate(entityForm);
                                }
                              }}
                              disabled={!entityForm.systemId || !entityForm.name || !entityForm.technicalName || !entityForm.entityType || createEntityMutation.isPending || updateEntityMutation.isPending}
                              data-testid="button-save-entity"
                            >
                              {createEntityMutation.isPending || updateEntityMutation.isPending ? (
                                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                              ) : null}
                              {editingEntity ? 'Update' : 'Create'}
                            </Button>
                          </div>
                        </div>
                      )}

                      {/* Entities Table */}
                      <div className="border rounded-lg">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead 
                                className="cursor-pointer hover:bg-muted/50"
                                onClick={() => {
                                  if (entitySortColumn === 'name') {
                                    setEntitySortDirection(entitySortDirection === 'asc' ? 'desc' : 'asc');
                                  } else {
                                    setEntitySortColumn('name');
                                    setEntitySortDirection('asc');
                                  }
                                }}
                              >
                                <div className="flex items-center gap-1">
                                  Display Name
                                  {entitySortColumn === 'name' && (
                                    entitySortDirection === 'asc' ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4 rotate-90" />
                                  )}
                                </div>
                              </TableHead>
                              <TableHead 
                                className="cursor-pointer hover:bg-muted/50"
                                onClick={() => {
                                  if (entitySortColumn === 'technicalName') {
                                    setEntitySortDirection(entitySortDirection === 'asc' ? 'desc' : 'asc');
                                  } else {
                                    setEntitySortColumn('technicalName');
                                    setEntitySortDirection('asc');
                                  }
                                }}
                              >
                                <div className="flex items-center gap-1">
                                  Technical Name
                                  {entitySortColumn === 'technicalName' && (
                                    entitySortDirection === 'asc' ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4 rotate-90" />
                                  )}
                                </div>
                              </TableHead>
                              <TableHead>Type</TableHead>
                              <TableHead>ERP System</TableHead>
                              <TableHead>Status</TableHead>
                              <TableHead className="w-[100px]">Actions</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {paginatedEntities.length === 0 ? (
                              <TableRow>
                                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                                  {erpSystems.length === 0 
                                    ? 'Create an ERP System first before adding entities.'
                                    : 'No entities found. Click "Add Entity" to create one.'}
                                </TableCell>
                              </TableRow>
                            ) : (
                              paginatedEntities.map((entity: any) => {
                                const system = erpSystems.find(s => s.id === entity.systemId);
                                return (
                                  <TableRow key={entity.id} data-testid={`row-entity-${entity.id}`}>
                                    <TableCell className="font-medium">{entity.name}</TableCell>
                                    <TableCell>
                                      <code className="bg-muted px-2 py-0.5 rounded text-xs">{entity.technicalName}</code>
                                    </TableCell>
                                    <TableCell>
                                      <Badge variant="outline" className="capitalize">{entity.entityType}</Badge>
                                    </TableCell>
                                    <TableCell>{system?.name || '-'}</TableCell>
                                    <TableCell>
                                      <Badge variant={entity.status === 'active' ? 'default' : 'secondary'}>
                                        {entity.status}
                                      </Badge>
                                    </TableCell>
                                    <TableCell>
                                      <div className="flex items-center gap-1">
                                        <Button
                                          variant="ghost"
                                          size="icon"
                                          onClick={() => {
                                            setEditingEntity(entity);
                                            setEntityForm({
                                              systemId: entity.systemId,
                                              name: entity.name,
                                              technicalName: entity.technicalName,
                                              entityType: entity.entityType,
                                              description: entity.description || '',
                                              status: entity.status,
                                            });
                                            setShowEntityForm(true);
                                          }}
                                          data-testid={`button-edit-entity-${entity.id}`}
                                        >
                                          <Pencil className="h-4 w-4" />
                                        </Button>
                                        <Button
                                          variant="ghost"
                                          size="icon"
                                          onClick={() => {
                                            if (confirm('Are you sure you want to delete this entity?')) {
                                              deleteEntityMutation.mutate(entity.id);
                                            }
                                          }}
                                          data-testid={`button-delete-entity-${entity.id}`}
                                        >
                                          <Trash2 className="h-4 w-4 text-destructive" />
                                        </Button>
                                      </div>
                                    </TableCell>
                                  </TableRow>
                                );
                              })
                            )}
                          </TableBody>
                        </Table>
                      </div>

                      {/* Pagination */}
                      {filteredEntities.length > 0 && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-muted-foreground" data-testid="text-entity-page-info">
                            Showing {((entityPage - 1) * entityPageSize) + 1}-{Math.min(entityPage * entityPageSize, filteredEntities.length)} of {filteredEntities.length}
                          </span>
                          <div className="flex items-center gap-2">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setEntityPage(1)}
                              disabled={entityPage === 1}
                              data-testid="button-entity-first"
                            >
                              First
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setEntityPage(p => Math.max(1, p - 1))}
                              disabled={entityPage === 1}
                              data-testid="button-entity-previous"
                            >
                              Previous
                            </Button>
                            <span className="text-sm" data-testid="text-entity-page-number">Page {entityPage} of {totalEntityPages || 1}</span>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setEntityPage(p => Math.min(totalEntityPages, p + 1))}
                              disabled={entityPage >= totalEntityPages}
                              data-testid="button-entity-next"
                            >
                              Next
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setEntityPage(totalEntityPages)}
                              disabled={entityPage >= totalEntityPages}
                              data-testid="button-entity-last"
                            >
                              Last
                            </Button>
                          </div>
                        </div>
                      )}
                    </div>
                  </TabsContent>

                  {/* Entity Data Sub-Tab */}
                  <TabsContent value="data">
                    <div className="space-y-4">
                      {/* Header */}
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Select 
                            value={dataEntityId || 'none'} 
                            onValueChange={(v) => { setDataEntityId(v === 'none' ? '' : v); setDataPage(1); }}
                          >
                            <SelectTrigger className="w-64" data-testid="select-data-entity">
                              <SelectValue placeholder="Select an entity" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">Select an entity...</SelectItem>
                              {erpEntities.map((ent: any) => (
                                <SelectItem key={ent.id} value={ent.id}>{ent.name} ({ent.technicalName})</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                          {dataEntityId && (
                            <Select value={String(dataPageSize)} onValueChange={(v) => { setDataPageSize(Number(v)); setDataPage(1); }}>
                              <SelectTrigger className="w-20" data-testid="select-data-page-size">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="10">10</SelectItem>
                                <SelectItem value="25">25</SelectItem>
                                <SelectItem value="50">50</SelectItem>
                                <SelectItem value="100">100</SelectItem>
                              </SelectContent>
                            </Select>
                          )}
                        </div>
                        {dataEntityId && (
                          <Button 
                            size="sm" 
                            className="gap-2" 
                            onClick={() => {
                              setEditingDataRecord(null);
                              const initialForm: Record<string, string> = {};
                              erpFields.forEach((f: any) => { initialForm[f.fieldName] = ''; });
                              setDataRecordForm(initialForm);
                              setShowDataRecordForm(true);
                            }}
                            data-testid="button-add-record"
                          >
                            <Plus className="h-4 w-4" />
                            Add Record
                          </Button>
                        )}
                      </div>

                      {/* Empty State */}
                      {!dataEntityId && (
                        <div className="text-center py-12 text-muted-foreground">
                          <Table2 className="h-12 w-12 mx-auto mb-4 opacity-50" />
                          <p>Select an entity above to view and manage its data records</p>
                        </div>
                      )}

                      {/* Loading State */}
                      {dataEntityId && (isLoadingFields || isLoadingRecords) && (
                        <div className="text-center py-12">
                          <Loader2 className="h-8 w-8 mx-auto mb-4 animate-spin text-muted-foreground" />
                          <p className="text-muted-foreground">Loading entity data...</p>
                        </div>
                      )}

                      {/* Record Form */}
                      {showDataRecordForm && dataEntityId && !isLoadingFields && (
                        <div className="border rounded-lg p-4 bg-muted/30">
                          <div className="flex items-center justify-between mb-4">
                            <h3 className="font-semibold">{editingDataRecord ? 'Edit Record' : 'New Record'}</h3>
                            <Button variant="ghost" size="sm" onClick={() => { setShowDataRecordForm(false); setEditingDataRecord(null); }} data-testid="button-close-record-form">
                              <X className="h-4 w-4" />
                            </Button>
                          </div>
                          <div className="grid grid-cols-3 gap-4">
                            {erpFields.map((field: any) => (
                              <div key={field.id} className="space-y-2">
                                <Label>{field.fieldName} {field.isRequired && <span className="text-destructive">*</span>}</Label>
                                <Input
                                  value={dataRecordForm[field.fieldName] || ''}
                                  onChange={(e) => setDataRecordForm(prev => ({ ...prev, [field.fieldName]: e.target.value }))}
                                  placeholder={field.sampleValues ? `e.g., ${field.sampleValues.split(',')[0]?.trim()}` : ''}
                                  data-testid={`input-record-${field.fieldName}`}
                                />
                                <span className="text-xs text-muted-foreground">{field.dataType}</span>
                              </div>
                            ))}
                          </div>
                          <div className="flex justify-end gap-2 mt-4">
                            <Button variant="outline" onClick={() => { setShowDataRecordForm(false); setEditingDataRecord(null); }} data-testid="button-cancel-record">
                              Cancel
                            </Button>
                            <Button
                              onClick={() => {
                                if (editingDataRecord) {
                                  updateRecordMutation.mutate({ id: editingDataRecord.id, data: dataRecordForm });
                                } else {
                                  createRecordMutation.mutate({ entityId: dataEntityId, data: dataRecordForm });
                                }
                              }}
                              disabled={createRecordMutation.isPending || updateRecordMutation.isPending}
                              data-testid="button-save-record"
                            >
                              {createRecordMutation.isPending || updateRecordMutation.isPending ? (
                                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                              ) : null}
                              {editingDataRecord ? 'Update' : 'Create'}
                            </Button>
                          </div>
                        </div>
                      )}

                      {/* Records Table */}
                      {dataEntityId && erpFields.length > 0 && !isLoadingFields && !isLoadingRecords && (
                        <div className="border rounded-lg">
                          <Table>
                            <TableHeader>
                              <TableRow>
                                {erpFields.slice(0, 5).map((field: any) => (
                                  <TableHead key={field.id}>{field.fieldName}</TableHead>
                                ))}
                                <TableHead className="w-[100px]">Actions</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {paginatedRecords.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={Math.min(erpFields.length, 5) + 1} className="text-center py-8 text-muted-foreground">
                                    No records found. Click "Add Record" to create the first one.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                paginatedRecords.map((record: any) => (
                                  <TableRow key={record.id}>
                                    {erpFields.slice(0, 5).map((field: any) => (
                                      <TableCell key={field.id}>
                                        {record.data?.[field.fieldName] || '-'}
                                      </TableCell>
                                    ))}
                                    <TableCell>
                                      <div className="flex items-center gap-1">
                                        <Button
                                          variant="ghost"
                                          size="icon"
                                          onClick={() => {
                                            setEditingDataRecord(record);
                                            setDataRecordForm(record.data || {});
                                            setShowDataRecordForm(true);
                                          }}
                                          data-testid={`button-edit-record-${record.id}`}
                                        >
                                          <Pencil className="h-4 w-4" />
                                        </Button>
                                        <Button
                                          variant="ghost"
                                          size="icon"
                                          onClick={() => {
                                            if (confirm('Are you sure you want to delete this record?')) {
                                              deleteRecordMutation.mutate(record.id);
                                            }
                                          }}
                                          data-testid={`button-delete-record-${record.id}`}
                                        >
                                          <Trash2 className="h-4 w-4 text-destructive" />
                                        </Button>
                                      </div>
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                        </div>
                      )}

                      {/* Pagination */}
                      {dataEntityId && erpRecords.length > 0 && (
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-muted-foreground" data-testid="text-data-page-info">
                            Showing {((dataPage - 1) * dataPageSize) + 1}-{Math.min(dataPage * dataPageSize, erpRecords.length)} of {erpRecords.length}
                          </span>
                          <div className="flex items-center gap-2">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setDataPage(1)}
                              disabled={dataPage === 1}
                              data-testid="button-data-first"
                            >
                              First
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setDataPage(p => Math.max(1, p - 1))}
                              disabled={dataPage === 1}
                              data-testid="button-data-previous"
                            >
                              Previous
                            </Button>
                            <span className="text-sm" data-testid="text-data-page-number">Page {dataPage} of {totalDataPages || 1}</span>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setDataPage(p => Math.min(totalDataPages, p + 1))}
                              disabled={dataPage >= totalDataPages}
                              data-testid="button-data-next"
                            >
                              Next
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setDataPage(totalDataPages)}
                              disabled={dataPage >= totalDataPages}
                              data-testid="button-data-last"
                            >
                              Last
                            </Button>
                          </div>
                        </div>
                      )}

                      {/* No Fields Message */}
                      {dataEntityId && erpFields.length === 0 && !isLoadingFields && (
                        <div className="text-center py-8 text-muted-foreground border rounded-lg">
                          <AlertCircle className="h-8 w-8 mx-auto mb-2 opacity-50" />
                          <p>No fields defined for this entity.</p>
                          <p className="text-sm">Add fields in the Fields tab of the ERP System page first.</p>
                        </div>
                      )}
                    </div>
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Connections Tab */}
          <TabsContent value="connections" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span className="flex items-center gap-2">
                    <Plug className="h-5 w-5" />
                    API Connections
                  </span>
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary">{connectionsData?.length || 0} connections</Badge>
                    <Button size="sm" className="gap-2" onClick={handleCreateConnection} data-testid="button-add-connection">
                      <Plus className="h-4 w-4" />
                      Add Connection
                    </Button>
                  </div>
                </CardTitle>
                <CardDescription>
                  Manage API connections to your ERP systems with authentication and health monitoring
                </CardDescription>
              </CardHeader>
              
              {/* Inline Connection Form */}
              {showConnectionForm && (
                <div className="mx-6 mb-4 p-4 border rounded-lg bg-muted/30">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="font-semibold text-lg">
                      {editingConnection ? 'Edit Connection' : 'New API Connection'}
                    </h3>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      onClick={() => { setShowConnectionForm(false); setEditingConnection(null); resetConnectionForm(); }}
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                  <p className="text-sm text-muted-foreground mb-4">
                    Configure connection settings for your ERP system API
                  </p>
                  
                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label htmlFor="conn-name">Connection Name *</Label>
                        <Input
                          id="conn-name"
                          placeholder="e.g., SAP Production"
                          value={connectionForm.name}
                          onChange={(e) => setConnectionForm(prev => ({ ...prev, name: e.target.value }))}
                          data-testid="input-connection-name"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="conn-erp">ERP System *</Label>
                        <Select
                          value={connectionForm.erpSystemId}
                          onValueChange={(v) => setConnectionForm(prev => ({ ...prev, erpSystemId: v }))}
                        >
                          <SelectTrigger id="conn-erp" data-testid="select-connection-erp">
                            <SelectValue placeholder="Select ERP System" />
                          </SelectTrigger>
                          <SelectContent>
                            {erpSystems.map(sys => (
                              <SelectItem key={sys.id} value={sys.id}>{sys.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    {/* Organization Scope */}
                    <div className="p-3 border rounded-lg bg-muted/20">
                      <h4 className="font-medium text-sm flex items-center gap-2 mb-3">
                        <Building2 className="h-4 w-4" />
                        Organization Scope
                      </h4>
                      <p className="text-xs text-muted-foreground mb-3">
                        Connections are scoped to a specific location. This determines which data this connection can access.
                      </p>
                      <div className="grid grid-cols-3 gap-3">
                        <div className="space-y-1.5">
                          <Label className="text-xs">Company *</Label>
                          <Select
                            value={connectionForm.companyId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ 
                              ...prev, 
                              companyId: v === 'none' ? '' : v,
                              businessUnitId: '',
                              locationId: ''
                            }))}
                          >
                            <SelectTrigger className="h-9" data-testid="select-conn-company">
                              <SelectValue placeholder="Select Company" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">Select Company</SelectItem>
                              {companies.map(c => (
                                <SelectItem key={c.id} value={c.id}>{c.companyName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-1.5">
                          <Label className="text-xs">Business Unit</Label>
                          <Select
                            value={connectionForm.businessUnitId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ 
                              ...prev, 
                              businessUnitId: v === 'none' ? '' : v,
                              locationId: ''
                            }))}
                            disabled={!connectionForm.companyId}
                          >
                            <SelectTrigger className="h-9" data-testid="select-conn-bu">
                              <SelectValue placeholder="All BUs" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">All Business Units</SelectItem>
                              {formBusinessUnits.map(bu => (
                                <SelectItem key={bu.id} value={bu.id}>{bu.orgName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-1.5">
                          <Label className="text-xs">Location *</Label>
                          <Select
                            value={connectionForm.locationId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ 
                              ...prev, 
                              locationId: v === 'none' ? '' : v
                            }))}
                            disabled={!connectionForm.companyId}
                          >
                            <SelectTrigger className="h-9" data-testid="select-conn-location">
                              <SelectValue placeholder="Select Location" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">Select Location</SelectItem>
                              {formLocations.map(loc => (
                                <SelectItem key={loc.id} value={loc.id}>{loc.locName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                    </div>
                    
                    <div className="space-y-2">
                      <Label htmlFor="conn-url">Base URL *</Label>
                      <Input
                        id="conn-url"
                        placeholder="https://api.example.com/v1"
                        value={connectionForm.baseUrl}
                        onChange={(e) => setConnectionForm(prev => ({ ...prev, baseUrl: e.target.value }))}
                        data-testid="input-connection-url"
                      />
                    </div>

                    <Separator />
                    
                    <div className="space-y-4">
                      <h4 className="font-medium flex items-center gap-2">
                        <Shield className="h-4 w-4" />
                        Authentication
                      </h4>
                      
                      <div className="space-y-2">
                        <Label htmlFor="conn-auth-type">Auth Type *</Label>
                        <Select
                          value={connectionForm.authType}
                          onValueChange={(v) => setConnectionForm(prev => ({ ...prev, authType: v }))}
                        >
                          <SelectTrigger id="conn-auth-type" data-testid="select-auth-type">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="api_key">API Key</SelectItem>
                            <SelectItem value="oauth2_client_credentials">OAuth2 (Client Credentials)</SelectItem>
                            <SelectItem value="oauth2_auth_code">OAuth2 (Authorization Code)</SelectItem>
                            <SelectItem value="basic_auth">Basic Auth</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      {connectionForm.authType === 'api_key' && (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <Label htmlFor="conn-api-key">API Key *</Label>
                            <Input
                              id="conn-api-key"
                              type="password"
                              placeholder={editingConnection ? '••••••••' : 'Enter your API key'}
                              value={connectionForm.apiKeyValue}
                              onChange={(e) => setConnectionForm(prev => ({ ...prev, apiKeyValue: e.target.value }))}
                              data-testid="input-api-key"
                            />
                            {editingConnection && (
                              <p className="text-xs text-muted-foreground">Leave blank to keep existing key</p>
                            )}
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label htmlFor="conn-api-header">Header Name</Label>
                              <Input
                                id="conn-api-header"
                                placeholder="X-API-Key"
                                value={connectionForm.apiKeyHeader}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, apiKeyHeader: e.target.value }))}
                              />
                            </div>
                            <div className="space-y-2">
                              <Label htmlFor="conn-api-location">Key Location</Label>
                              <Select
                                value={connectionForm.apiKeyLocation}
                                onValueChange={(v) => setConnectionForm(prev => ({ ...prev, apiKeyLocation: v }))}
                              >
                                <SelectTrigger id="conn-api-location">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="header">Header</SelectItem>
                                  <SelectItem value="query">Query Parameter</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                          </div>
                        </div>
                      )}

                      {(connectionForm.authType === 'oauth2_client_credentials' || connectionForm.authType === 'oauth2_auth_code') && (
                        <div className="space-y-4">
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label htmlFor="conn-client-id">Client ID *</Label>
                              <Input
                                id="conn-client-id"
                                placeholder="your-client-id"
                                value={connectionForm.clientId}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, clientId: e.target.value }))}
                              />
                            </div>
                            <div className="space-y-2">
                              <Label htmlFor="conn-client-secret">Client Secret *</Label>
                              <Input
                                id="conn-client-secret"
                                type="password"
                                placeholder={editingConnection ? '••••••••' : 'Enter client secret'}
                                value={connectionForm.clientSecret}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, clientSecret: e.target.value }))}
                              />
                              {editingConnection && (
                                <p className="text-xs text-muted-foreground">Leave blank to keep existing secret</p>
                              )}
                            </div>
                          </div>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label htmlFor="conn-token-url">Token URL *</Label>
                              <Input
                                id="conn-token-url"
                                placeholder="https://auth.example.com/oauth/token"
                                value={connectionForm.tokenUrl}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, tokenUrl: e.target.value }))}
                              />
                            </div>
                            <div className="space-y-2">
                              <Label htmlFor="conn-scopes">Scopes</Label>
                              <Input
                                id="conn-scopes"
                                placeholder="read write"
                                value={connectionForm.scopes}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, scopes: e.target.value }))}
                              />
                            </div>
                          </div>
                          {connectionForm.authType === 'oauth2_auth_code' && (
                            <div className="space-y-2">
                              <Label htmlFor="conn-auth-url">Authorization URL *</Label>
                              <Input
                                id="conn-auth-url"
                                placeholder="https://auth.example.com/oauth/authorize"
                                value={connectionForm.authUrl}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, authUrl: e.target.value }))}
                              />
                            </div>
                          )}
                          <p className="text-xs text-muted-foreground">Credentials are encrypted at rest</p>
                        </div>
                      )}

                      {connectionForm.authType === 'basic_auth' && (
                        <div className="space-y-4">
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label htmlFor="conn-basic-user">Username *</Label>
                              <Input
                                id="conn-basic-user"
                                placeholder="Enter username"
                                value={connectionForm.basicUsername}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, basicUsername: e.target.value }))}
                              />
                            </div>
                            <div className="space-y-2">
                              <Label htmlFor="conn-basic-pass">Password *</Label>
                              <Input
                                id="conn-basic-pass"
                                type="password"
                                placeholder={editingConnection ? '••••••••' : 'Enter password'}
                                value={connectionForm.basicPassword}
                                onChange={(e) => setConnectionForm(prev => ({ ...prev, basicPassword: e.target.value }))}
                              />
                              {editingConnection && (
                                <p className="text-xs text-muted-foreground">Leave blank to keep existing password</p>
                              )}
                            </div>
                          </div>
                          <p className="text-xs text-muted-foreground">Credentials are encrypted at rest</p>
                        </div>
                      )}
                    </div>

                    <Separator />

                    <div className="space-y-4">
                      <h4 className="font-medium flex items-center gap-2">
                        <Building2 className="h-4 w-4" />
                        Organization Scope (Optional)
                      </h4>
                      <div className="grid grid-cols-3 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="conn-company">Company</Label>
                          <Select
                            value={connectionForm.companyId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ 
                              ...prev, 
                              companyId: v === 'none' ? '' : v,
                              businessUnitId: '',
                              locationId: ''
                            }))}
                          >
                            <SelectTrigger id="conn-company">
                              <SelectValue placeholder="All" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">All Companies</SelectItem>
                              {companies.map(c => (
                                <SelectItem key={c.id} value={c.id}>{c.companyName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="conn-bu">Business Unit</Label>
                          <Select
                            value={connectionForm.businessUnitId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ 
                              ...prev, 
                              businessUnitId: v === 'none' ? '' : v,
                              locationId: ''
                            }))}
                            disabled={!connectionForm.companyId}
                          >
                            <SelectTrigger id="conn-bu">
                              <SelectValue placeholder="All" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">All BUs</SelectItem>
                              {formBusinessUnits.map(bu => (
                                <SelectItem key={bu.id} value={bu.id}>{bu.orgName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="conn-loc">Location</Label>
                          <Select
                            value={connectionForm.locationId || 'none'}
                            onValueChange={(v) => setConnectionForm(prev => ({ ...prev, locationId: v === 'none' ? '' : v }))}
                            disabled={!connectionForm.companyId}
                          >
                            <SelectTrigger id="conn-loc">
                              <SelectValue placeholder="All" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">All Locations</SelectItem>
                              {formLocations.map(loc => (
                                <SelectItem key={loc.id} value={loc.id}>{loc.locName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                    </div>

                    <Separator />

                    <div className="space-y-2">
                      <Label htmlFor="conn-health">Health Check Endpoint (Optional)</Label>
                      <Input
                        id="conn-health"
                        placeholder="/health or /api/status"
                        value={connectionForm.healthCheckEndpoint}
                        onChange={(e) => setConnectionForm(prev => ({ ...prev, healthCheckEndpoint: e.target.value }))}
                      />
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="conn-desc">Description</Label>
                      <Textarea
                        id="conn-desc"
                        placeholder="Notes about this connection..."
                        value={connectionForm.description}
                        onChange={(e) => setConnectionForm(prev => ({ ...prev, description: e.target.value }))}
                        rows={2}
                      />
                    </div>
                  </div>

                  <div className="flex justify-end gap-2 mt-4 pt-4 border-t">
                    <Button variant="outline" onClick={() => { setShowConnectionForm(false); setEditingConnection(null); resetConnectionForm(); }}>
                      Cancel
                    </Button>
                    <Button 
                      onClick={handleConnectionSubmit}
                      disabled={!connectionForm.name || !connectionForm.erpSystemId || !connectionForm.baseUrl || createConnectionMutation.isPending || updateConnectionMutation.isPending}
                      data-testid="button-save-connection"
                    >
                      {(createConnectionMutation.isPending || updateConnectionMutation.isPending) ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Saving...
                        </>
                      ) : (
                        editingConnection ? 'Update Connection' : 'Create Connection'
                      )}
                    </Button>
                  </div>
                </div>
              )}
              
              <CardContent>
                {connectionsLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : !connectionsData?.length ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Plug className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No connections configured yet.</p>
                    <p className="text-sm">Add a connection to start integrating with your ERP system.</p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {connectionsData.map(conn => {
                      const erpSystem = erpSystems.find(e => e.id === conn.erpSystemId);
                      const company = companies.find(c => c.id === conn.companyId);
                      
                      return (
                        <Collapsible 
                          key={conn.id}
                          open={expandedHealthId === conn.id}
                          onOpenChange={() => setExpandedHealthId(expandedHealthId === conn.id ? null : conn.id)}
                        >
                          <div className="border rounded-lg" data-testid={`row-connection-${conn.id}`}>
                            <div className="flex items-center p-4 gap-4">
                              <CollapsibleTrigger asChild>
                                <Button variant="ghost" size="sm">
                                  {expandedHealthId === conn.id ? (
                                    <ChevronDown className="h-4 w-4" />
                                  ) : (
                                    <ChevronRight className="h-4 w-4" />
                                  )}
                                </Button>
                              </CollapsibleTrigger>
                              
                              <div className="flex-1 min-w-0">
                                <p className="font-medium truncate flex items-center gap-2">
                                  {conn.name}
                                  {conn.lastHealthCheckStatus === 'healthy' && (
                                    <Heart className="h-4 w-4 text-green-500 fill-green-500" />
                                  )}
                                  {conn.lastHealthCheckStatus === 'error' && (
                                    <XCircle className="h-4 w-4 text-red-500" />
                                  )}
                                </p>
                                <p className="text-sm text-muted-foreground truncate">
                                  {erpSystem?.name || 'Unknown ERP'} • {conn.baseUrl}
                                </p>
                              </div>
                              
                              <Badge variant="outline" className="gap-1">
                                <Key className="h-3 w-3" />
                                {conn.authType.replace(/_/g, ' ').replace('oauth2', 'OAuth2')}
                              </Badge>
                              
                              {company && (
                                <Badge variant="secondary" className="gap-1">
                                  <Building2 className="h-3 w-3" />
                                  {company.companyName}
                                </Badge>
                              )}
                              
                              <Badge variant={conn.status === 'active' ? 'default' : 'secondary'}>
                                {conn.status}
                              </Badge>
                              
                              <div className="flex items-center gap-2">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    // For auth connections, expand and show inline credentials form
                                    if (conn.authType === 'oauth2_client_credentials' || conn.authType === 'api_key' || conn.authType === 'basic_auth') {
                                      openInlineTestCredentials(conn);
                                    } else {
                                      testConnectionMutation.mutate({ id: conn.id });
                                    }
                                  }}
                                  disabled={testingConnectionId === conn.id}
                                  data-testid={`button-test-${conn.id}`}
                                >
                                  {testingConnectionId === conn.id ? (
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                  ) : (
                                    <TestTube2 className="h-4 w-4" />
                                  )}
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => openEditConnection(conn)}
                                  data-testid={`button-edit-${conn.id}`}
                                >
                                  <Pencil className="h-4 w-4" />
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => {
                                    if (confirm('Delete this connection?')) {
                                      deleteConnectionMutation.mutate(conn.id);
                                    }
                                  }}
                                  data-testid={`button-delete-${conn.id}`}
                                >
                                  <Trash2 className="h-4 w-4 text-red-500" />
                                </Button>
                              </div>
                            </div>
                            
                            <CollapsibleContent>
                              <div className="border-t px-4 py-4 bg-muted/30">
                                {/* Inline Test Credentials Form */}
                                {inlineTestCredentials.connectionId === conn.id && (
                                  <div className="mb-4 p-4 border rounded-lg bg-blue-50/50 dark:bg-blue-950/20 border-blue-200 dark:border-blue-900">
                                    <div className="flex items-center justify-between mb-3">
                                      <h4 className="text-sm font-medium flex items-center gap-2 text-blue-700 dark:text-blue-400">
                                        <TestTube2 className="h-4 w-4" />
                                        Test Connection Authentication
                                      </h4>
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={cancelInlineTest}
                                        className="h-7 w-7 p-0"
                                      >
                                        <X className="h-4 w-4" />
                                      </Button>
                                    </div>
                                    <p className="text-xs text-muted-foreground mb-3">
                                      Enter credentials to test with real authentication. Credentials are not stored.
                                    </p>
                                    
                                    {conn.authType === 'oauth2_client_credentials' && (
                                      <div className="space-y-2 mb-3">
                                        <Label htmlFor={`test-secret-${conn.id}`} className="text-xs">OAuth2 Client Secret</Label>
                                        <Input
                                          id={`test-secret-${conn.id}`}
                                          type="password"
                                          placeholder="Enter your client secret"
                                          value={inlineTestCredentials.clientSecret}
                                          onChange={(e) => setInlineTestCredentials(prev => ({ ...prev, clientSecret: e.target.value }))}
                                          className="h-9"
                                          data-testid={`input-test-secret-${conn.id}`}
                                        />
                                        {conn.tokenUrl && (
                                          <p className="text-xs text-muted-foreground">
                                            Token URL: {conn.tokenUrl}
                                          </p>
                                        )}
                                      </div>
                                    )}
                                    
                                    {conn.authType === 'api_key' && (
                                      <div className="space-y-2 mb-3">
                                        <Label htmlFor={`test-apikey-${conn.id}`} className="text-xs">API Key</Label>
                                        <Input
                                          id={`test-apikey-${conn.id}`}
                                          type="password"
                                          placeholder="Enter your API key"
                                          value={inlineTestCredentials.apiKey}
                                          onChange={(e) => setInlineTestCredentials(prev => ({ ...prev, apiKey: e.target.value }))}
                                          className="h-9"
                                          data-testid={`input-test-apikey-${conn.id}`}
                                        />
                                      </div>
                                    )}
                                    
                                    {conn.authType === 'basic_auth' && (
                                      <div className="grid grid-cols-2 gap-3 mb-3">
                                        <div className="space-y-2">
                                          <Label htmlFor={`test-user-${conn.id}`} className="text-xs">Username</Label>
                                          <Input
                                            id={`test-user-${conn.id}`}
                                            placeholder="Username"
                                            value={inlineTestCredentials.basicUsername}
                                            onChange={(e) => setInlineTestCredentials(prev => ({ ...prev, basicUsername: e.target.value }))}
                                            className="h-9"
                                            data-testid={`input-test-user-${conn.id}`}
                                          />
                                        </div>
                                        <div className="space-y-2">
                                          <Label htmlFor={`test-pass-${conn.id}`} className="text-xs">Password</Label>
                                          <Input
                                            id={`test-pass-${conn.id}`}
                                            type="password"
                                            placeholder="Password"
                                            value={inlineTestCredentials.basicPassword}
                                            onChange={(e) => setInlineTestCredentials(prev => ({ ...prev, basicPassword: e.target.value }))}
                                            className="h-9"
                                            data-testid={`input-test-pass-${conn.id}`}
                                          />
                                        </div>
                                      </div>
                                    )}
                                    
                                    <div className="flex items-center gap-2">
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        onClick={() => testConnectionMutation.mutate({ id: conn.id })}
                                        disabled={testConnectionMutation.isPending}
                                      >
                                        Test Without Auth
                                      </Button>
                                      <Button
                                        size="sm"
                                        onClick={() => handleTestWithInlineCredentials(conn.id)}
                                        disabled={testConnectionMutation.isPending}
                                        data-testid={`button-test-auth-${conn.id}`}
                                      >
                                        {testConnectionMutation.isPending ? (
                                          <>
                                            <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                                            Testing...
                                          </>
                                        ) : (
                                          <>
                                            <Shield className="h-4 w-4 mr-1" />
                                            Test with Auth
                                          </>
                                        )}
                                      </Button>
                                    </div>
                                  </div>
                                )}
                                
                                {/* Health History Section */}
                                <div className="flex items-center justify-between mb-3">
                                  <h4 className="text-sm font-medium flex items-center gap-2">
                                    <Activity className="h-4 w-4" />
                                    Health History
                                  </h4>
                                  {conn.lastHealthCheckAt && (
                                    <span className="text-xs text-muted-foreground">
                                      Last checked: {formatDateUSA(new Date(conn.lastHealthCheckAt))}
                                    </span>
                                  )}
                                </div>
                                
                                {healthLoading ? (
                                  <div className="flex items-center justify-center py-4">
                                    <Loader2 className="h-5 w-5 animate-spin" />
                                  </div>
                                ) : !healthEventsData?.length ? (
                                  <p className="text-sm text-muted-foreground text-center py-4">
                                    No health events recorded. Test the connection to start monitoring.
                                  </p>
                                ) : (
                                  <div className="space-y-2">
                                    {healthEventsData.slice(0, 10).map(event => (
                                      <div 
                                        key={event.id} 
                                        className="flex items-center gap-3 text-sm p-2 rounded bg-background"
                                      >
                                        {event.status === 'healthy' ? (
                                          <CheckCircle2 className="h-4 w-4 text-green-500" />
                                        ) : (
                                          <XCircle className="h-4 w-4 text-red-500" />
                                        )}
                                        <span className="flex-1">{event.message || event.eventType}</span>
                                        {event.latencyMs && (
                                          <span className="text-muted-foreground">{event.latencyMs}ms</span>
                                        )}
                                        <span className="text-muted-foreground">
                                          {formatDateUSA(new Date(event.checkedAt))}
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                )}
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

          {/* Mappings Tab */}
          <TabsContent value="mappings" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Active Mappings</span>
                  <Badge variant="secondary">{mappingsData?.total || 0} mappings</Badge>
                </CardTitle>
                <CardDescription>
                  View and manage ERP-to-LicenseIQ field mappings with version control
                </CardDescription>
              </CardHeader>
              <CardContent>
                {mappingsLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : mappingsData?.mappings?.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No mappings found. Create mappings in the Master Data Mapping page.</p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {mappingsData?.mappings?.map(mapping => (
                      <Collapsible 
                        key={mapping.id} 
                        open={expandedVersionHistoryId === mapping.id}
                        onOpenChange={() => handleToggleVersionHistory(mapping.id)}
                      >
                        <div className="border rounded-lg" data-testid={`row-mapping-${mapping.id}`}>
                          {/* Main Row */}
                          <div className="flex items-center p-4 gap-4">
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm" data-testid={`button-expand-${mapping.id}`}>
                                {expandedVersionHistoryId === mapping.id ? (
                                  <ChevronDown className="h-4 w-4" />
                                ) : (
                                  <ChevronRight className="h-4 w-4" />
                                )}
                              </Button>
                            </CollapsibleTrigger>
                            
                            <div className="flex-1 min-w-0">
                              <p className="font-medium truncate">{mapping.mappingName}</p>
                              <p className="text-sm text-muted-foreground">
                                {mapping.erpSystem} → {mapping.entityType}
                              </p>
                            </div>
                            
                            <Badge variant="secondary" className="gap-1">
                              <GitBranch className="h-3 w-3" />
                              v{mapping.version}
                            </Badge>
                            
                            {getConfidenceBadge(mapping.aiConfidence)}
                            {getStatusBadge(mapping.status)}
                            
                            <span className="text-sm text-muted-foreground hidden md:block">
                              {formatDateUSA(new Date(mapping.createdAt))}
                            </span>
                            
                            <div className="flex items-center gap-2">
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => handleToggleVersionHistory(mapping.id)}
                                data-testid={`button-history-${mapping.id}`}
                              >
                                <History className="h-4 w-4" />
                              </Button>
                              {mapping.status === 'approved' && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => handleOpenImport(mapping)}
                                  data-testid={`button-import-${mapping.id}`}
                                >
                                  <Upload className="h-4 w-4 mr-1" />
                                  Import
                                </Button>
                              )}
                              {mapping.status === 'draft' && (
                                <Button
                                  size="sm"
                                  onClick={() => approveMutation.mutate(mapping.id)}
                                  disabled={approveMutation.isPending}
                                  data-testid={`button-approve-${mapping.id}`}
                                >
                                  <CheckCircle2 className="h-4 w-4 mr-1" />
                                  Approve
                                </Button>
                              )}
                              {mapping.status === 'approved' && (
                                <Button
                                  size="sm"
                                  variant="destructive"
                                  onClick={() => deprecateMutation.mutate(mapping.id)}
                                  disabled={deprecateMutation.isPending}
                                  data-testid={`button-deprecate-${mapping.id}`}
                                >
                                  <XCircle className="h-4 w-4 mr-1" />
                                  Deprecate
                                </Button>
                              )}
                            </div>
                          </div>
                          
                          {/* Inline Version History (replaces dialog) */}
                          <CollapsibleContent>
                            <div className="border-t bg-muted/30 p-4" data-testid={`section-versions-${mapping.id}`}>
                              <div className="flex items-center gap-2 mb-4">
                                <History className="h-5 w-5" />
                                <h3 className="font-semibold">Version History</h3>
                              </div>
                              
                              {versionsLoading ? (
                                <div className="flex items-center justify-center py-4">
                                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                                </div>
                              ) : versionHistoryData?.versions?.length === 0 ? (
                                <p className="text-center text-muted-foreground py-4">No version history available.</p>
                              ) : (
                                <div className="space-y-2">
                                  {versionHistoryData?.versions?.map((version, index) => (
                                    <div 
                                      key={version.id} 
                                      className={`flex items-center justify-between p-3 rounded-lg ${index === 0 ? 'bg-primary/10 border border-primary/20' : 'bg-background border'}`}
                                    >
                                      <div className="flex items-center gap-3">
                                        <Badge variant={index === 0 ? 'default' : 'secondary'} className="gap-1">
                                          <GitBranch className="h-3 w-3" />
                                          v{version.version}
                                        </Badge>
                                        {getStatusBadge(version.status)}
                                        {getConfidenceBadge(version.aiConfidence)}
                                        {version.notes && (
                                          <span className="text-sm text-muted-foreground ml-2">{version.notes}</span>
                                        )}
                                      </div>
                                      <div className="flex items-center gap-2">
                                        <span className="text-sm text-muted-foreground">
                                          {formatDateUSA(new Date(version.createdAt))}
                                        </span>
                                        {index > 0 && version.status !== 'deprecated' && (
                                          <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={() => revertMutation.mutate({
                                              mappingId: mapping.id,
                                              targetVersion: version.version,
                                            })}
                                            disabled={revertMutation.isPending}
                                            data-testid={`button-revert-${version.version}`}
                                          >
                                            <RefreshCw className="h-3 w-3 mr-1" />
                                            Revert
                                          </Button>
                                        )}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              )}
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

          {/* Data Sources Tab */}
          <TabsContent value="sources" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Data Import Sources</span>
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary">{sourcesData?.total || 0} sources</Badge>
                    <Button onClick={() => { setEditingSource(null); resetSourceForm(); setShowSourceForm(true); }} data-testid="button-add-source">
                      <Plus className="h-4 w-4 mr-2" />
                      Add Source
                    </Button>
                  </div>
                </CardTitle>
                <CardDescription>
                  Configure reusable data import sources with scheduling, filtering, and field mapping
                </CardDescription>
              </CardHeader>
              <CardContent>
                {/* Source Form */}
                {showSourceForm && (
                  <Card className="mb-6 border-primary/20">
                    <CardHeader>
                      <CardTitle className="text-lg">{editingSource ? 'Edit Import Source' : 'Create Import Source'}</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="source-name">Source Name *</Label>
                          <Input
                            id="source-name"
                            value={sourceForm.name}
                            onChange={(e) => setSourceForm({ ...sourceForm, name: e.target.value })}
                            placeholder="e.g., Daily Sales Import"
                            data-testid="input-source-name"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="source-type">Source Type</Label>
                          <Select
                            value={sourceForm.sourceType}
                            onValueChange={(value: 'file' | 'api') => setSourceForm({ ...sourceForm, sourceType: value })}
                          >
                            <SelectTrigger data-testid="select-source-type">
                              <SelectValue placeholder="Select type" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="file">File Upload (CSV/Excel)</SelectItem>
                              <SelectItem value="api">API Connection</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="source-description">Description</Label>
                        <Textarea
                          id="source-description"
                          value={sourceForm.description}
                          onChange={(e) => setSourceForm({ ...sourceForm, description: e.target.value })}
                          placeholder="Describe this import source..."
                          data-testid="input-source-description"
                        />
                      </div>

                      <div className="grid grid-cols-3 gap-4">
                        <div className="space-y-2">
                          <Label>ERP System</Label>
                          <Select
                            value={sourceForm.erpSystemId}
                            onValueChange={(value) => setSourceForm({ ...sourceForm, erpSystemId: value })}
                          >
                            <SelectTrigger data-testid="select-source-erp">
                              <SelectValue placeholder="Select ERP" />
                            </SelectTrigger>
                            <SelectContent>
                              {erpSystems.map((erp) => (
                                <SelectItem key={erp.id} value={erp.id}>{erp.name}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label>Field Mapping</Label>
                          <Select
                            value={sourceForm.mappingId}
                            onValueChange={(value) => setSourceForm({ ...sourceForm, mappingId: value })}
                          >
                            <SelectTrigger data-testid="select-source-mapping">
                              <SelectValue placeholder="Select mapping" />
                            </SelectTrigger>
                            <SelectContent>
                              {mappingsData?.mappings?.filter(m => m.status === 'approved').map((mapping) => (
                                <SelectItem key={mapping.id} value={mapping.id}>{mapping.mappingName}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label>Schedule</Label>
                          <Select
                            value={sourceForm.scheduleType}
                            onValueChange={(value: 'manual' | 'hourly' | 'daily' | 'weekly' | 'monthly') => setSourceForm({ ...sourceForm, scheduleType: value })}
                          >
                            <SelectTrigger data-testid="select-source-schedule">
                              <SelectValue placeholder="Select schedule" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="manual">Manual Only</SelectItem>
                              <SelectItem value="hourly">Hourly</SelectItem>
                              <SelectItem value="daily">Daily</SelectItem>
                              <SelectItem value="weekly">Weekly</SelectItem>
                              <SelectItem value="monthly">Monthly</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      {/* Pre-Import Data Filtering */}
                      <Collapsible open={showFilters} onOpenChange={setShowFilters}>
                        <CollapsibleTrigger asChild>
                          <div className="flex items-center justify-between p-4 bg-muted/50 rounded-lg border cursor-pointer hover:bg-muted/70 transition-colors">
                            <div className="flex items-center gap-2 text-sm font-medium">
                              <Activity className="h-4 w-4" />
                              Pre-Import Data Filters
                              {sourceForm.filters?.conditions?.length ? (
                                <Badge variant="secondary" className="ml-2">
                                  {sourceForm.filters.conditions.length} filter{sourceForm.filters.conditions.length > 1 ? 's' : ''}
                                </Badge>
                              ) : null}
                            </div>
                            {showFilters ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                          </div>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="mt-2">
                          <div className="p-4 border rounded-lg space-y-4">
                            <p className="text-sm text-muted-foreground">
                              Define filters to apply before importing data. Only records matching your criteria will be imported.
                            </p>
                            <FilterBuilder
                              availableFields={sourceFilterFields.length > 0 ? sourceFilterFields : [
                                { name: 'status', label: 'Status', dataType: 'text' },
                                { name: 'amount', label: 'Amount', dataType: 'number' },
                                { name: 'date', label: 'Date', dataType: 'date' },
                                { name: 'active', label: 'Active', dataType: 'boolean' },
                              ]}
                              value={sourceForm.filters || { conditions: [], logic: 'AND' }}
                              onChange={(config) => setSourceForm({ ...sourceForm, filters: config })}
                            />
                            {sourceFilterFields.length === 0 && (
                              <p className="text-xs text-amber-600 dark:text-amber-400">
                                {sourceForm.mappingId 
                                  ? 'No source fields found in mapping. Select a different field mapping with source schema defined.'
                                  : 'Select a Field Mapping above to populate filter fields from the mapping configuration.'}
                              </p>
                            )}
                            {sourceFilterFields.length > 0 && (
                              <p className="text-xs text-green-600 dark:text-green-400">
                                {sourceFilterFields.length} fields available from mapping: {sourceFilterFields.slice(0, 5).map(f => f.label).join(', ')}{sourceFilterFields.length > 5 ? '...' : ''}
                              </p>
                            )}
                          </div>
                        </CollapsibleContent>
                      </Collapsible>

                      {sourceForm.sourceType === 'api' && (
                        <div className="space-y-4 p-4 bg-muted/50 rounded-lg border">
                          <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
                            <Plug className="h-4 w-4" />
                            API Configuration
                          </div>
                          
                          <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <Label>API Connection *</Label>
                              <Select
                                value={sourceForm.connectionId}
                                onValueChange={(value) => setSourceForm({ ...sourceForm, connectionId: value })}
                              >
                                <SelectTrigger data-testid="select-source-connection">
                                  <SelectValue placeholder="Select API connection" />
                                </SelectTrigger>
                                <SelectContent>
                                  {(connectionsData || []).filter(c => c.status === 'connected' || c.status === 'active').map((conn) => (
                                    <SelectItem key={conn.id} value={conn.id}>{conn.name}</SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                              {selectedConnection && (
                                <p className="text-xs text-muted-foreground">
                                  Base URL: {selectedConnection.baseUrl}
                                </p>
                              )}
                            </div>

                            <div className="space-y-2">
                              <Label>API Endpoint Template</Label>
                              <Select
                                value={sourceForm.endpointTemplateId}
                                onValueChange={(value) => setSourceForm({ ...sourceForm, endpointTemplateId: value })}
                                disabled={!sourceForm.erpSystemId}
                              >
                                <SelectTrigger data-testid="select-source-endpoint">
                                  <SelectValue placeholder={sourceForm.erpSystemId ? "Select endpoint" : "Select ERP first"} />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="custom">Custom URL</SelectItem>
                                  {endpointTemplates.map((template) => (
                                    <SelectItem key={template.id} value={template.id}>
                                      {template.name} ({template.httpMethod} {template.pathTemplate})
                                    </SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                            </div>
                          </div>

                          {sourceForm.endpointTemplateId === 'custom' && (
                            <div className="space-y-2">
                              <Label>Custom API Endpoint URL</Label>
                              <Input
                                value={sourceForm.apiEndpointUrl}
                                onChange={(e) => setSourceForm({ ...sourceForm, apiEndpointUrl: e.target.value })}
                                placeholder="/api/v1/customers or full URL"
                                data-testid="input-source-api-url"
                              />
                              <p className="text-xs text-muted-foreground">
                                Enter the path (relative to base URL) or full URL to fetch data from
                              </p>
                            </div>
                          )}

                          {selectedEndpointTemplate && sourceForm.endpointTemplateId !== 'custom' && (
                            <div className="p-3 bg-background rounded border space-y-2">
                              <div className="flex items-center gap-2">
                                <Badge variant="outline">{selectedEndpointTemplate.httpMethod}</Badge>
                                <code className="text-xs bg-muted px-2 py-1 rounded">{selectedEndpointTemplate.pathTemplate}</code>
                              </div>
                              {selectedEndpointTemplate.description && (
                                <p className="text-xs text-muted-foreground">{selectedEndpointTemplate.description}</p>
                              )}
                              {selectedEndpointTemplate.responseDataPath && (
                                <p className="text-xs text-muted-foreground">
                                  Response data path: <code className="bg-muted px-1 rounded">{selectedEndpointTemplate.responseDataPath}</code>
                                </p>
                              )}
                            </div>
                          )}

                          {!sourceForm.connectionId && (
                            <p className="text-xs text-amber-600 dark:text-amber-400">
                              Please select or create an API connection in the Connections tab first.
                            </p>
                          )}
                        </div>
                      )}

                      <Separator />

                      <div className="flex items-center gap-6">
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input
                            type="checkbox"
                            checked={sourceForm.dryRunByDefault}
                            onChange={(e) => setSourceForm({ ...sourceForm, dryRunByDefault: e.target.checked })}
                            className="rounded"
                            data-testid="checkbox-dryrun"
                          />
                          <span className="text-sm">Dry-run by default</span>
                        </label>
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input
                            type="checkbox"
                            checked={sourceForm.skipDuplicates}
                            onChange={(e) => setSourceForm({ ...sourceForm, skipDuplicates: e.target.checked })}
                            className="rounded"
                            data-testid="checkbox-skip-duplicates"
                          />
                          <span className="text-sm">Skip duplicates</span>
                        </label>
                        <label className="flex items-center gap-2 cursor-pointer">
                          <input
                            type="checkbox"
                            checked={sourceForm.autoCommit}
                            onChange={(e) => setSourceForm({ ...sourceForm, autoCommit: e.target.checked })}
                            className="rounded"
                            data-testid="checkbox-autocommit"
                          />
                          <span className="text-sm">Auto-commit on success</span>
                        </label>
                      </div>

                      <div className="flex justify-end gap-2 pt-4">
                        <Button variant="outline" onClick={() => { setShowSourceForm(false); setEditingSource(null); }} data-testid="button-cancel-source">
                          Cancel
                        </Button>
                        <Button 
                          onClick={handleSubmitSource} 
                          disabled={!sourceForm.name || createSourceMutation.isPending || updateSourceMutation.isPending}
                          data-testid="button-save-source"
                        >
                          {(createSourceMutation.isPending || updateSourceMutation.isPending) && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                          {editingSource ? 'Update Source' : 'Create Source'}
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Sources List */}
                {sourcesLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : (sourcesData?.sources?.length || 0) === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No data import sources configured yet.</p>
                    <p className="text-sm mt-2">Create a source to streamline your data imports.</p>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {sourcesData?.sources?.map((source: DataImportSource) => (
                      <div key={source.id} className="border rounded-lg p-4" data-testid={`row-source-${source.id}`}>
                        <div className="flex items-center justify-between">
                          <div className="flex-1">
                            <div className="flex items-center gap-3">
                              <h4 className="font-medium">{source.name}</h4>
                              <Badge variant={source.sourceType === 'file' ? 'outline' : 'default'}>
                                {source.sourceType === 'file' ? 'File Upload' : 'API'}
                              </Badge>
                              <Badge variant={source.status === 'active' ? 'default' : source.status === 'error' ? 'destructive' : 'secondary'}>
                                {source.status}
                              </Badge>
                              {source.scheduleType && source.scheduleType !== 'manual' && (
                                <Badge variant="outline" className="gap-1">
                                  <Clock className="h-3 w-3" />
                                  {source.scheduleType}
                                </Badge>
                              )}
                            </div>
                            {source.description && (
                              <p className="text-sm text-muted-foreground mt-1">{source.description}</p>
                            )}
                            <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
                              <span>Runs: {source.successCount + source.failureCount}</span>
                              <span className="text-green-600">{source.successCount} success</span>
                              {source.failureCount > 0 && <span className="text-red-600">{source.failureCount} failed</span>}
                              {source.lastRunAt && <span>Last run: {formatDateUSA(new Date(source.lastRunAt))}</span>}
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            {source.sourceType === 'file' && (
                              <div className="flex items-center gap-2">
                                {sourceRunningId === source.id ? (
                                  <div className="flex items-center gap-2">
                                    <div 
                                      {...getSourceRootProps()} 
                                      className={`border-2 border-dashed rounded-lg p-2 cursor-pointer transition-colors ${
                                        isSourceDragActive ? 'border-primary bg-primary/10' : 'border-muted-foreground/20'
                                      }`}
                                    >
                                      <input {...getSourceInputProps()} />
                                      <span className="text-xs">
                                        {sourceUploadFile ? sourceUploadFile.name : 'Drop file here'}
                                      </span>
                                    </div>
                                    <Button
                                      size="sm"
                                      onClick={() => runSourceMutation.mutate({ sourceId: source.id, file: sourceUploadFile!, dryRun: true })}
                                      disabled={!sourceUploadFile || runSourceMutation.isPending}
                                      data-testid={`button-run-dryrun-${source.id}`}
                                    >
                                      {runSourceMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Eye className="h-4 w-4" />}
                                    </Button>
                                    <Button
                                      size="sm"
                                      variant="default"
                                      onClick={() => runSourceMutation.mutate({ sourceId: source.id, file: sourceUploadFile!, dryRun: false })}
                                      disabled={!sourceUploadFile || runSourceMutation.isPending}
                                      data-testid={`button-run-import-${source.id}`}
                                    >
                                      {runSourceMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
                                    </Button>
                                    <Button size="sm" variant="ghost" onClick={() => { setSourceRunningId(null); setSourceUploadFile(null); }}>
                                      <X className="h-4 w-4" />
                                    </Button>
                                  </div>
                                ) : (
                                  <Button
                                    size="sm"
                                    variant="outline"
                                    onClick={() => setSourceRunningId(source.id)}
                                    data-testid={`button-start-import-${source.id}`}
                                  >
                                    <Upload className="h-4 w-4 mr-1" />
                                    Import
                                  </Button>
                                )}
                              </div>
                            )}
                            {source.sourceType === 'api' && (
                              <div className="flex items-center gap-2">
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => runSourceMutation.mutate({ sourceId: source.id, dryRun: true })}
                                  disabled={runSourceMutation.isPending}
                                  data-testid={`button-api-dryrun-${source.id}`}
                                >
                                  {runSourceMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Eye className="h-4 w-4 mr-1" />}
                                  Preview
                                </Button>
                                <Button
                                  size="sm"
                                  variant="default"
                                  onClick={() => runSourceMutation.mutate({ sourceId: source.id, dryRun: false })}
                                  disabled={runSourceMutation.isPending}
                                  data-testid={`button-api-import-${source.id}`}
                                >
                                  {runSourceMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-1" />}
                                  Pull from API
                                </Button>
                              </div>
                            )}
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => handleEditSource(source)}
                              data-testid={`button-edit-source-${source.id}`}
                            >
                              <Pencil className="h-4 w-4" />
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => {
                                if (confirm('Are you sure you want to delete this import source?')) {
                                  deleteSourceMutation.mutate(source.id);
                                }
                              }}
                              disabled={deleteSourceMutation.isPending}
                              data-testid={`button-delete-source-${source.id}`}
                            >
                              <Trash2 className="h-4 w-4 text-destructive" />
                            </Button>
                          </div>
                        </div>
                        {source.lastError && (
                          <div className="mt-3 p-2 bg-destructive/10 rounded text-sm text-destructive">
                            <AlertCircle className="h-4 w-4 inline mr-2" />
                            {source.lastError}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Imports Tab */}
          <TabsContent value="imports" className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Import Jobs</span>
                  <Badge variant="secondary">{jobsData?.total || 0} jobs</Badge>
                </CardTitle>
                <CardDescription>
                  Track and manage data import operations with dry-run preview
                </CardDescription>
              </CardHeader>
              <CardContent>
                {jobsLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : jobsData?.jobs?.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    <Upload className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No import jobs yet. Start by importing data using an approved mapping.</p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {jobsData?.jobs?.map(job => (
                      <Collapsible 
                        key={job.id}
                        open={expandedJobDetailsId === job.id}
                        onOpenChange={() => handleToggleJobDetails(job.id)}
                      >
                        <div className="border rounded-lg" data-testid={`row-job-${job.id}`}>
                          {/* Main Row */}
                          <div className="flex items-center p-4 gap-4">
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm" data-testid={`button-expand-job-${job.id}`}>
                                {expandedJobDetailsId === job.id ? (
                                  <ChevronDown className="h-4 w-4" />
                                ) : (
                                  <ChevronRight className="h-4 w-4" />
                                )}
                              </Button>
                            </CollapsibleTrigger>
                            
                            <div className="flex-1 min-w-0">
                              <p className="font-medium truncate">{job.jobName}</p>
                              <p className="text-sm text-muted-foreground">
                                {job.recordsTotal} records
                              </p>
                            </div>
                            
                            <Badge variant={job.jobType === 'dry_run' ? 'outline' : 'default'}>
                              {job.jobType === 'dry_run' ? 'Preview' : 'Import'}
                            </Badge>
                            
                            <div className="flex flex-col gap-1 text-sm">
                              {job.recordsProcessed > 0 && (
                                <span className="text-green-600">{job.recordsProcessed} processed</span>
                              )}
                              {job.recordsFailed > 0 && (
                                <span className="text-red-600">{job.recordsFailed} failed</span>
                              )}
                            </div>
                            
                            {getStatusBadge(job.status)}
                            
                            <span className="text-sm text-muted-foreground hidden md:block">
                              {job.startedAt ? formatDateUSA(new Date(job.startedAt)) : '-'}
                            </span>
                            
                            <div className="flex items-center gap-2">
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => handleToggleJobDetails(job.id)}
                                data-testid={`button-view-job-${job.id}`}
                              >
                                <Eye className="h-4 w-4" />
                              </Button>
                              {job.status === 'pending_commit' && (
                                <>
                                  <Button
                                    size="sm"
                                    onClick={() => commitMutation.mutate(job.id)}
                                    disabled={commitMutation.isPending}
                                    data-testid={`button-commit-${job.id}`}
                                  >
                                    <CheckCircle2 className="h-4 w-4 mr-1" />
                                    Commit
                                  </Button>
                                  <Button
                                    size="sm"
                                    variant="destructive"
                                    onClick={() => discardMutation.mutate(job.id)}
                                    disabled={discardMutation.isPending}
                                    data-testid={`button-discard-${job.id}`}
                                  >
                                    <Trash2 className="h-4 w-4 mr-1" />
                                    Discard
                                  </Button>
                                </>
                              )}
                            </div>
                          </div>
                          
                          {/* Inline Job Details (replaces dialog) */}
                          <CollapsibleContent>
                            <div className="border-t bg-muted/30 p-4" data-testid={`section-job-details-${job.id}`}>
                              <div className="flex items-center gap-2 mb-4">
                                <FileJson className="h-5 w-5" />
                                <h3 className="font-semibold">Job Details</h3>
                              </div>
                              
                              {jobDetailsLoading ? (
                                <div className="flex items-center justify-center py-4">
                                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                                </div>
                              ) : jobDetailsData ? (
                                <div className="space-y-4">
                                  {/* Job Summary Cards */}
                                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                    <Card className="cursor-pointer hover:ring-2 ring-primary" onClick={() => setJobRecordFilter('all')}>
                                      <CardContent className="pt-4">
                                        <div className="text-2xl font-bold">{jobDetailsData.summary.total}</div>
                                        <div className="text-sm text-muted-foreground">Total Records</div>
                                      </CardContent>
                                    </Card>
                                    <Card className="cursor-pointer hover:ring-2 ring-yellow-500" onClick={() => setJobRecordFilter('staged')}>
                                      <CardContent className="pt-4">
                                        <div className="text-2xl font-bold text-yellow-600">{jobDetailsData.summary.staged}</div>
                                        <div className="text-sm text-muted-foreground">Staged (Pending)</div>
                                      </CardContent>
                                    </Card>
                                    <Card className="cursor-pointer hover:ring-2 ring-green-500" onClick={() => setJobRecordFilter('committed')}>
                                      <CardContent className="pt-4">
                                        <div className="text-2xl font-bold text-green-600">{jobDetailsData.summary.committed}</div>
                                        <div className="text-sm text-muted-foreground">Committed (Success)</div>
                                      </CardContent>
                                    </Card>
                                    <Card className="cursor-pointer hover:ring-2 ring-red-500" onClick={() => setJobRecordFilter('failed')}>
                                      <CardContent className="pt-4">
                                        <div className="text-2xl font-bold text-red-600">{jobDetailsData.summary.failed}</div>
                                        <div className="text-sm text-muted-foreground">Failed (Errors)</div>
                                      </CardContent>
                                    </Card>
                                  </div>

                                  {/* Processing Information */}
                                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div className="bg-muted/50 p-3 rounded-lg">
                                      <div className="text-sm font-medium mb-2">Job Information</div>
                                      <div className="space-y-1 text-xs">
                                        <div className="flex justify-between">
                                          <span className="text-muted-foreground">Job Type:</span>
                                          <Badge variant="outline">{jobDetailsData.job.jobType}</Badge>
                                        </div>
                                        <div className="flex justify-between">
                                          <span className="text-muted-foreground">Started:</span>
                                          <span>{jobDetailsData.job.startedAt ? formatDateUSA(jobDetailsData.job.startedAt) : 'N/A'}</span>
                                        </div>
                                        <div className="flex justify-between">
                                          <span className="text-muted-foreground">Completed:</span>
                                          <span>{jobDetailsData.job.completedAt ? formatDateUSA(jobDetailsData.job.completedAt) : 'In Progress'}</span>
                                        </div>
                                        {jobDetailsData.job.uploadMeta && (
                                          <div className="flex justify-between">
                                            <span className="text-muted-foreground">Source File:</span>
                                            <span>{(jobDetailsData.job.uploadMeta as any).fileName || 'N/A'}</span>
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                    
                                    {/* Processing Log */}
                                    {jobDetailsData.job.processingLog && (
                                      <div className="bg-muted/50 p-3 rounded-lg">
                                        <div className="text-sm font-medium mb-2">Processing Log</div>
                                        <ScrollArea className="h-[100px]">
                                          <div className="space-y-1 text-xs font-mono">
                                            {Array.isArray(jobDetailsData.job.processingLog) ? (
                                              (jobDetailsData.job.processingLog as any[]).map((log, idx) => (
                                                <div key={idx} className="flex gap-2">
                                                  <span className="text-muted-foreground">{log.timestamp || idx + 1}:</span>
                                                  <span className={log.level === 'error' ? 'text-red-600' : log.level === 'warn' ? 'text-yellow-600' : ''}>{log.message || JSON.stringify(log)}</span>
                                                </div>
                                              ))
                                            ) : (
                                              <pre className="whitespace-pre-wrap">{JSON.stringify(jobDetailsData.job.processingLog, null, 2)}</pre>
                                            )}
                                          </div>
                                        </ScrollArea>
                                      </div>
                                    )}
                                  </div>

                                  {/* Records Table with Filtering */}
                                  {jobDetailsData.records.length > 0 && (
                                    <div>
                                      <div className="flex items-center justify-between mb-2">
                                        <h4 className="font-medium">Import Records</h4>
                                        <div className="flex gap-2">
                                          <Badge 
                                            variant={jobRecordFilter === 'all' ? 'default' : 'outline'} 
                                            className="cursor-pointer"
                                            onClick={() => setJobRecordFilter('all')}
                                          >
                                            All ({jobDetailsData.summary.total})
                                          </Badge>
                                          <Badge 
                                            variant={jobRecordFilter === 'staged' ? 'default' : 'outline'} 
                                            className="cursor-pointer bg-yellow-100 text-yellow-800 hover:bg-yellow-200"
                                            onClick={() => setJobRecordFilter('staged')}
                                          >
                                            Staged ({jobDetailsData.summary.staged})
                                          </Badge>
                                          <Badge 
                                            variant={jobRecordFilter === 'committed' ? 'default' : 'outline'} 
                                            className="cursor-pointer bg-green-100 text-green-800 hover:bg-green-200"
                                            onClick={() => setJobRecordFilter('committed')}
                                          >
                                            Success ({jobDetailsData.summary.committed})
                                          </Badge>
                                          <Badge 
                                            variant={jobRecordFilter === 'failed' ? 'default' : 'outline'} 
                                            className="cursor-pointer bg-red-100 text-red-800 hover:bg-red-200"
                                            onClick={() => setJobRecordFilter('failed')}
                                          >
                                            Failed ({jobDetailsData.summary.failed})
                                          </Badge>
                                        </div>
                                      </div>
                                      <ScrollArea className="h-[300px] border rounded-lg">
                                        <Table>
                                          <TableHeader>
                                            <TableRow>
                                              <TableHead className="w-[80px]">Row #</TableHead>
                                              <TableHead className="w-[100px]">Status</TableHead>
                                              <TableHead>Source Data</TableHead>
                                              <TableHead>Transformed Data</TableHead>
                                              <TableHead className="w-[200px]">Error Details</TableHead>
                                            </TableRow>
                                          </TableHeader>
                                          <TableBody>
                                            {jobDetailsData.records
                                              .filter((record: ImportRecord) => jobRecordFilter === 'all' || record.recordStatus === jobRecordFilter)
                                              .map((record: ImportRecord, idx: number) => (
                                              <TableRow key={record.id} className={record.recordStatus === 'failed' ? 'bg-red-50 dark:bg-red-950/20' : ''}>
                                                <TableCell className="font-mono text-xs">{idx + 1}</TableCell>
                                                <TableCell>{getStatusBadge(record.recordStatus)}</TableCell>
                                                <TableCell>
                                                  <pre className="text-xs whitespace-pre-wrap max-w-xs bg-muted/50 p-2 rounded">
                                                    {JSON.stringify(record.sourceRecord, null, 2)}
                                                  </pre>
                                                </TableCell>
                                                <TableCell>
                                                  <pre className="text-xs whitespace-pre-wrap max-w-xs bg-muted/50 p-2 rounded">
                                                    {JSON.stringify(record.targetRecord, null, 2)}
                                                  </pre>
                                                </TableCell>
                                                <TableCell>
                                                  {record.recordStatus === 'failed' && record.validationErrors ? (
                                                    <div className="text-xs text-red-600">
                                                      {Array.isArray(record.validationErrors) ? (
                                                        <ul className="list-disc list-inside">
                                                          {(record.validationErrors as any[]).map((err, i) => (
                                                            <li key={i}>{typeof err === 'string' ? err : err.message || JSON.stringify(err)}</li>
                                                          ))}
                                                        </ul>
                                                      ) : (
                                                        <span>{JSON.stringify(record.validationErrors)}</span>
                                                      )}
                                                    </div>
                                                  ) : record.recordStatus === 'committed' ? (
                                                    <span className="text-xs text-green-600">Successfully imported</span>
                                                  ) : record.recordStatus === 'staged' ? (
                                                    <span className="text-xs text-yellow-600">Awaiting commit</span>
                                                  ) : null}
                                                </TableCell>
                                              </TableRow>
                                            ))}
                                          </TableBody>
                                        </Table>
                                      </ScrollArea>
                                    </div>
                                  )}

                                  {/* Error Summary - Shows only when failed records exist */}
                                  {jobDetailsData.summary.failed > 0 && (
                                    <div className="bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900 rounded-lg p-4">
                                      <div className="flex items-center justify-between mb-2">
                                        <h4 className="font-medium text-red-700 dark:text-red-400 flex items-center gap-2">
                                          <XCircle className="h-4 w-4" />
                                          Failed Records Summary
                                        </h4>
                                        <Button
                                          size="sm"
                                          variant="outline"
                                          onClick={() => retryFailedMutation.mutate(job.id)}
                                          disabled={retryFailedMutation.isPending}
                                          className="border-red-300 text-red-700 hover:bg-red-100"
                                          data-testid={`button-retry-failed-${job.id}`}
                                        >
                                          {retryFailedMutation.isPending ? (
                                            <>
                                              <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                                              Retrying...
                                            </>
                                          ) : (
                                            <>
                                              <RefreshCw className="h-4 w-4 mr-1" />
                                              Retry Failed ({jobDetailsData.summary.failed})
                                            </>
                                          )}
                                        </Button>
                                      </div>
                                      <p className="text-sm text-red-600 dark:text-red-400 mb-3">
                                        {jobDetailsData.summary.failed} records failed to import. Click on "Failed" above to view details.
                                      </p>
                                      <div className="text-xs text-muted-foreground">
                                        <strong>Common causes:</strong> Missing required fields, invalid data types, duplicate records, validation errors.
                                        <br />
                                        <strong>To fix:</strong> Review the error details, correct your source data, then click "Retry Failed" to re-process.
                                      </div>
                                    </div>
                                  )}

                                  {/* Job-Level Error Log */}
                                  {jobDetailsData.job.errorLog && (
                                    <div>
                                      <h4 className="font-medium mb-2 text-red-600 flex items-center gap-2">
                                        <AlertCircle className="h-4 w-4" />
                                        System Error Log
                                      </h4>
                                      <ScrollArea className="h-[150px]">
                                        <pre className="text-xs bg-red-50 dark:bg-red-950/30 p-3 rounded-lg overflow-auto font-mono">
                                          {Array.isArray(jobDetailsData.job.errorLog) ? (
                                            (jobDetailsData.job.errorLog as any[]).map((err, idx) => (
                                              <div key={idx} className="mb-2 border-b border-red-200 pb-2">
                                                {typeof err === 'string' ? err : JSON.stringify(err, null, 2)}
                                              </div>
                                            ))
                                          ) : (
                                            JSON.stringify(jobDetailsData.job.errorLog, null, 2)
                                          )}
                                        </pre>
                                      </ScrollArea>
                                    </div>
                                  )}
                                </div>
                              ) : (
                                <p className="text-center text-muted-foreground py-4">No details available.</p>
                              )}
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
