import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import MainLayout from "@/components/layout/main-layout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import {
  Plus,
  Edit2,
  Trash2,
  Database,
  FileText,
  DollarSign,
  ShoppingCart,
  Package,
  Search,
  Layers,
  CheckCircle2,
  Globe,
  Code,
  ChevronDown,
  ChevronRight,
  ChevronLeft,
  ChevronUp,
  ChevronsLeft,
  ChevronsRight,
  Building,
  Filter,
  ArrowUpDown,
  Download,
  X,
  AlertCircle,
} from "lucide-react";
import { Separator } from "@/components/ui/separator";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useLocation } from "wouter";

interface LicenseiqEntity {
  id: string;
  name: string;
  technicalName: string;
  description?: string;
  category?: string;
  createdAt: string;
  updatedAt: string;
}

interface LicenseiqField {
  id: string;
  entityId: string;
  fieldName: string;
  dataType: string;
  description?: string;
  isRequired: boolean;
  defaultValue?: string;
  validationRules?: string;
  createdAt: string;
  updatedAt: string;
}

interface LicenseiqApiEndpoint {
  id: string;
  entityId: string;
  operationType: string;
  name: string;
  httpMethod: string;
  pathTemplate: string;
  requestBodyPath?: string;
  responseDataPath?: string;
  paginationType?: string;
  pageParamName?: string;
  limitParamName?: string;
  cursorParamName?: string;
  offsetParamName?: string;
  totalPath?: string;
  hasMorePath?: string;
  nextCursorPath?: string;
  defaultPageSize: number;
  requiredScopes?: string[];
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

const CATEGORY_COLORS: Record<string, string> = {
  "Organization Hierarchy": "bg-orange-100 dark:bg-orange-900 text-orange-800 dark:text-orange-100",
  "Master Data": "bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-100",
  "Transactions": "bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-100",
  "Transactional": "bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-100",
  "Rules": "bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-100",
};

const ENTITY_ICONS: Record<string, typeof Database> = {
  sales_data: ShoppingCart,
  contract_terms: FileText,
  royalty_rules: DollarSign,
  payments: DollarSign,
  products: Package,
};

const DATA_TYPES = [
  "string",
  "number",
  "date",
  "boolean",
  "object",
  "array",
];

export default function LicenseIQSchema() {
  const [, navigate] = useLocation();
  const { toast } = useToast();
  const [activeTab, setActiveTab] = useState("entities");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedCategory, setSelectedCategory] = useState<string>("all");
  const [selectedEntity, setSelectedEntity] = useState<LicenseiqEntity | null>(null);
  
  // Entity inline form states
  const [showEntityForm, setShowEntityForm] = useState(false);
  const [entityDialogMode, setEntityDialogMode] = useState<"create" | "edit">("create");
  const [entityForm, setEntityForm] = useState({
    name: "",
    technicalName: "",
    description: "",
    category: "",
  });

  // Field inline form states
  const [showFieldForm, setShowFieldForm] = useState(false);
  const [fieldDialogMode, setFieldDialogMode] = useState<"create" | "edit">("create");
  const [fieldForm, setFieldForm] = useState({
    id: "",
    fieldName: "",
    dataType: "",
    description: "",
    isRequired: false,
    defaultValue: "",
    validationRules: "",
  });

  // API Endpoint states
  const [showEndpointForm, setShowEndpointForm] = useState(false);
  const [editingEndpoint, setEditingEndpoint] = useState<LicenseiqApiEndpoint | null>(null);
  const [expandedEndpointId, setExpandedEndpointId] = useState<string | null>(null);
  const [endpointEntityFilter, setEndpointEntityFilter] = useState<string>("");
  const [endpointForm, setEndpointForm] = useState({
    entityId: "",
    operationType: "list",
    name: "",
    httpMethod: "GET",
    pathTemplate: "",
    requestBodyPath: "",
    responseDataPath: "",
    paginationType: "none",
    pageParamName: "",
    limitParamName: "",
    cursorParamName: "",
    offsetParamName: "",
    totalPath: "",
    hasMorePath: "",
    nextCursorPath: "",
    defaultPageSize: 100,
    requiredScopes: [] as string[],
    isActive: true,
  });

  // Data tab states
  const [selectedDataEntity, setSelectedDataEntity] = useState<string>("");
  const [showDataForm, setShowDataForm] = useState(false);
  const [dataFormMode, setDataFormMode] = useState<"create" | "edit">("create");
  const [editingRecord, setEditingRecord] = useState<any>(null);
  const [dataFormValues, setDataFormValues] = useState<Record<string, any>>({});
  
  // Multi-tenant context states for Data tab
  const [dataContextCompanyId, setDataContextCompanyId] = useState<string>("");
  const [dataContextBuId, setDataContextBuId] = useState<string>("");
  const [dataContextLocationId, setDataContextLocationId] = useState<string>("");

  // Enterprise data management states - Pagination, Sorting, Filtering
  const [dataPage, setDataPage] = useState(1);
  const [dataPageSize, setDataPageSize] = useState(25);
  const [dataSortColumn, setDataSortColumn] = useState<string>("created_at");
  const [dataSortDirection, setDataSortDirection] = useState<"asc" | "desc">("desc");
  const [dataFilters, setDataFilters] = useState<Record<string, string>>({});
  const [showFilters, setShowFilters] = useState(false);
  
  // Advanced filter states
  type FilterOperator = "contains" | "equals" | "starts_with" | "ends_with" | "gt" | "gte" | "lt" | "lte" | "between" | "is_null" | "not_null";
  interface AdvancedFilter {
    column: string;
    operator: FilterOperator;
    value: string;
    value2?: string; // For "between" operator
  }
  const [advancedFilters, setAdvancedFilters] = useState<AdvancedFilter[]>([]);
  const [filterLogic, setFilterLogic] = useState<"and" | "or">("and");
  const [useAdvancedFilters, setUseAdvancedFilters] = useState(false);
  
  // Operator options based on data type
  const textOperators: { value: FilterOperator; label: string }[] = [
    { value: "contains", label: "Contains" },
    { value: "equals", label: "Equals" },
    { value: "starts_with", label: "Starts with" },
    { value: "ends_with", label: "Ends with" },
    { value: "is_null", label: "Is empty" },
    { value: "not_null", label: "Is not empty" },
  ];
  const numericOperators: { value: FilterOperator; label: string }[] = [
    { value: "equals", label: "Equals" },
    { value: "gt", label: "Greater than" },
    { value: "gte", label: "Greater or equal" },
    { value: "lt", label: "Less than" },
    { value: "lte", label: "Less or equal" },
    { value: "between", label: "Between" },
    { value: "is_null", label: "Is empty" },
    { value: "not_null", label: "Is not empty" },
  ];
  
  const getOperatorsForColumn = (dataType: string) => {
    const numericTypes = ["integer", "bigint", "numeric", "decimal", "real", "double precision", "smallint"];
    if (numericTypes.includes(dataType?.toLowerCase())) return numericOperators;
    return textOperators;
  };

  // Fetch entities
  const { data: entitiesData, isLoading: entitiesLoading } = useQuery<{ entities: LicenseiqEntity[] }>({
    queryKey: ["/api/licenseiq-entities", selectedCategory],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (selectedCategory && selectedCategory !== "all") params.append("category", selectedCategory);
      const response = await fetch(`/api/licenseiq-entities?${params}`);
      if (!response.ok) throw new Error("Failed to fetch entities");
      return response.json();
    },
  });

  // Fetch fields for selected entity
  const { data: fieldsData, isLoading: fieldsLoading } = useQuery<{ fields: LicenseiqField[] }>({
    queryKey: ["/api/licenseiq-fields", selectedEntity?.id],
    enabled: !!selectedEntity,
    queryFn: async () => {
      const response = await fetch(`/api/licenseiq-fields?entityId=${selectedEntity?.id}`);
      if (!response.ok) throw new Error("Failed to fetch fields");
      return response.json();
    },
  });

  // Entity mutations
  const createEntityMutation = useMutation({
    mutationFn: async (data: any) => apiRequest("POST", "/api/licenseiq-entities", data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-entities" });
      toast({ title: "Entity created successfully!" });
      setShowEntityForm(false);
      resetEntityForm();
    },
    onError: () => {
      toast({ title: "Failed to create entity", variant: "destructive" });
    },
  });

  const updateEntityMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) =>
      apiRequest("PATCH", `/api/licenseiq-entities/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-entities" });
      toast({ title: "Entity updated successfully!" });
      setShowEntityForm(false);
      resetEntityForm();
    },
    onError: () => {
      toast({ title: "Failed to update entity", variant: "destructive" });
    },
  });

  const deleteEntityMutation = useMutation({
    mutationFn: async (id: string) => apiRequest("DELETE", `/api/licenseiq-entities/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-entities" });
      toast({ title: "Entity deleted successfully!" });
    },
    onError: () => {
      toast({ title: "Failed to delete entity", variant: "destructive" });
    },
  });

  // Field mutations
  const createFieldMutation = useMutation({
    mutationFn: async (data: any) => apiRequest("POST", "/api/licenseiq-fields", data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-fields" });
      toast({ title: "Field created successfully!" });
      setShowFieldForm(false);
      resetFieldForm();
    },
    onError: () => {
      toast({ title: "Failed to create field", variant: "destructive" });
    },
  });

  const updateFieldMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) =>
      apiRequest("PATCH", `/api/licenseiq-fields/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-fields" });
      toast({ title: "Field updated successfully!" });
      setShowFieldForm(false);
      resetFieldForm();
    },
    onError: () => {
      toast({ title: "Failed to update field", variant: "destructive" });
    },
  });

  const deleteFieldMutation = useMutation({
    mutationFn: async (id: string) => apiRequest("DELETE", `/api/licenseiq-fields/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-fields" });
      toast({ title: "Field deleted successfully!" });
    },
    onError: () => {
      toast({ title: "Failed to delete field", variant: "destructive" });
    },
  });

  // API Endpoints query
  const { data: endpointsData, isLoading: endpointsLoading } = useQuery<LicenseiqApiEndpoint[]>({
    queryKey: ["/api/licenseiq-api-endpoints", endpointEntityFilter],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (endpointEntityFilter) params.append("entityId", endpointEntityFilter);
      const response = await fetch(`/api/licenseiq-api-endpoints?${params}`);
      if (!response.ok) throw new Error("Failed to fetch endpoints");
      return response.json();
    },
  });

  // API Endpoint mutations
  const createEndpointMutation = useMutation({
    mutationFn: async (data: typeof endpointForm) => apiRequest("POST", "/api/licenseiq-api-endpoints", data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-api-endpoints" });
      toast({ title: "API Endpoint created successfully!" });
      setShowEndpointForm(false);
      setEditingEndpoint(null);
      resetEndpointForm();
    },
    onError: () => {
      toast({ title: "Failed to create endpoint", variant: "destructive" });
    },
  });

  const updateEndpointMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Partial<typeof endpointForm> }) =>
      apiRequest("PATCH", `/api/licenseiq-api-endpoints/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-api-endpoints" });
      toast({ title: "API Endpoint updated successfully!" });
      setShowEndpointForm(false);
      setEditingEndpoint(null);
      resetEndpointForm();
    },
    onError: () => {
      toast({ title: "Failed to update endpoint", variant: "destructive" });
    },
  });

  const deleteEndpointMutation = useMutation({
    mutationFn: async (id: string) => apiRequest("DELETE", `/api/licenseiq-api-endpoints/${id}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ predicate: (query) => query.queryKey[0] === "/api/licenseiq-api-endpoints" });
      toast({ title: "API Endpoint deleted successfully!" });
    },
    onError: () => {
      toast({ title: "Failed to delete endpoint", variant: "destructive" });
    },
  });

  // Data tab queries and mutations
  const { data: availableTablesData } = useQuery<{ tables: { id: string; name: string; technicalName: string; category: string }[] }>({
    queryKey: ["/api/entity-data/available-tables"],
    queryFn: async () => {
      const response = await fetch("/api/entity-data/available-tables");
      if (!response.ok) throw new Error("Failed to fetch available tables");
      return response.json();
    },
  });

  // Fetch current user to check if system admin
  const { data: currentUser } = useQuery<{ id: string; isSystemAdmin?: boolean; primaryCompany?: string }>({
    queryKey: ["/api/user"],
  });
  const isSystemAdmin = currentUser?.isSystemAdmin || !currentUser?.primaryCompany;

  // Fetch user's active login context (shown in top right corner)
  const { data: activeContextData } = useQuery<{ activeContext: { companyId?: string; businessUnitId?: string; locationId?: string } | null }>({
    queryKey: ["/api/user/active-context"],
  });
  const loginContext = activeContextData?.activeContext;

  // Fetch companies for context selector
  const { data: companiesData } = useQuery<{ id: string; companyName: string }[]>({
    queryKey: ["/api/master-data/companies"],
  });

  // Fetch business units for selected company
  const { data: businessUnitsData } = useQuery<{ id: string; orgId: string; orgName: string }[]>({
    queryKey: ["/api/master-data/business-units", dataContextCompanyId],
    enabled: !!dataContextCompanyId,
    queryFn: async () => {
      const response = await fetch(`/api/master-data/business-units?companyId=${dataContextCompanyId}`);
      if (!response.ok) throw new Error("Failed to fetch business units");
      return response.json();
    },
  });

  // Fetch locations for selected business unit
  const { data: locationsData } = useQuery<{ id: string; locationName: string }[]>({
    queryKey: ["/api/master-data/locations", dataContextCompanyId, dataContextBuId],
    enabled: !!dataContextCompanyId,
    queryFn: async () => {
      const params = new URLSearchParams();
      if (dataContextBuId) params.append("orgId", dataContextBuId);
      else if (dataContextCompanyId) params.append("companyId", dataContextCompanyId);
      const response = await fetch(`/api/master-data/locations?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to fetch locations");
      return response.json();
    },
  });

  const { data: entityDataResult, isLoading: entityDataLoading, refetch: refetchEntityData } = useQuery<{ records: any[]; total: number; page: number; pageSize: number; totalPages: number; tenantColumns?: { hasCompany: boolean; hasBU: boolean; hasLocation: boolean } }>({
    queryKey: ["/api/entity-data", selectedDataEntity, dataContextCompanyId, dataContextBuId, dataContextLocationId, dataPage, dataPageSize, dataSortColumn, dataSortDirection, dataFilters, useAdvancedFilters, advancedFilters, filterLogic],
    enabled: !!selectedDataEntity,
    queryFn: async () => {
      const params = new URLSearchParams();
      if (dataContextCompanyId) params.append("company_id", dataContextCompanyId);
      if (dataContextBuId) params.append("business_unit_id", dataContextBuId);
      if (dataContextLocationId) params.append("location_id", dataContextLocationId);
      // Pagination
      params.append("page", dataPage.toString());
      params.append("pageSize", dataPageSize.toString());
      // Sorting
      params.append("sortColumn", dataSortColumn);
      params.append("sortDirection", dataSortDirection);
      // Filters - simple or advanced
      if (useAdvancedFilters && advancedFilters.length > 0) {
        params.append("advancedFilters", JSON.stringify(advancedFilters));
        params.append("filterLogic", filterLogic);
      } else {
        Object.entries(dataFilters).forEach(([col, val]) => {
          if (val) params.append(`filter_${col}`, val);
        });
      }
      const response = await fetch(`/api/entity-data/${selectedDataEntity}?${params}`);
      if (!response.ok) throw new Error("Failed to fetch entity data");
      return response.json();
    },
  });

  // Check if context is required but not selected (for system admins adding to tenant-aware tables)
  const needsContextSelection = isSystemAdmin && 
    entityDataResult?.tenantColumns?.hasCompany && 
    !dataContextCompanyId && 
    !loginContext?.companyId;

  const { data: entityColumnsData } = useQuery<{ columns: { column_name: string; data_type: string; is_nullable: string }[] }>({
    queryKey: ["/api/entity-data", selectedDataEntity, "columns"],
    enabled: !!selectedDataEntity,
    queryFn: async () => {
      const response = await fetch(`/api/entity-data/${selectedDataEntity}/columns`);
      if (!response.ok) throw new Error("Failed to fetch columns");
      return response.json();
    },
  });

  const createDataRecordMutation = useMutation({
    mutationFn: async (data: Record<string, any>) => {
      if (!selectedDataEntity) {
        throw new Error("No entity selected");
      }
      // Auto-inject organizational context - use selector value or fall back to login context
      const tenantCols = entityDataResult?.tenantColumns;
      const effectiveCompanyId = dataContextCompanyId || loginContext?.companyId;
      const effectiveBuId = dataContextBuId || loginContext?.businessUnitId;
      const effectiveLocationId = dataContextLocationId || loginContext?.locationId;
      
      const dataWithContext = {
        ...data,
        ...(effectiveCompanyId && tenantCols?.hasCompany && { company_id: effectiveCompanyId }),
        ...(effectiveBuId && tenantCols?.hasBU && { business_unit_id: effectiveBuId }),
        ...(effectiveLocationId && tenantCols?.hasLocation && { location_id: effectiveLocationId }),
      };
      return apiRequest("POST", `/api/entity-data/${selectedDataEntity}`, dataWithContext);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/entity-data", selectedDataEntity, dataContextCompanyId, dataContextBuId, dataContextLocationId] });
      toast({ title: "Record created successfully!" });
      setShowDataForm(false);
      setDataFormValues({});
    },
    onError: (error: any) => {
      toast({ title: "Failed to create record", description: error.message, variant: "destructive" });
    },
  });

  const updateDataRecordMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Record<string, any> }) => {
      if (!selectedDataEntity) {
        throw new Error("No entity selected");
      }
      // Preserve organizational context on update - use existing values from record, or context selector, or login context
      const tenantCols = entityDataResult?.tenantColumns;
      const effectiveCompanyId = data.company_id || dataContextCompanyId || loginContext?.companyId;
      const effectiveBuId = data.business_unit_id || dataContextBuId || loginContext?.businessUnitId;
      const effectiveLocationId = data.location_id || dataContextLocationId || loginContext?.locationId;
      
      const dataWithContext = {
        ...data,
        ...(tenantCols?.hasCompany && effectiveCompanyId && { company_id: effectiveCompanyId }),
        ...(tenantCols?.hasBU && effectiveBuId && { business_unit_id: effectiveBuId }),
        ...(tenantCols?.hasLocation && effectiveLocationId && { location_id: effectiveLocationId }),
      };
      return apiRequest("PATCH", `/api/entity-data/${selectedDataEntity}/${id}`, dataWithContext);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/entity-data", selectedDataEntity, dataContextCompanyId, dataContextBuId, dataContextLocationId] });
      toast({ title: "Record updated successfully!" });
      setShowDataForm(false);
      setEditingRecord(null);
      setDataFormValues({});
    },
    onError: (error: any) => {
      toast({ title: "Failed to update record", description: error.message, variant: "destructive" });
    },
  });

  const deleteDataRecordMutation = useMutation({
    mutationFn: async (id: string) => {
      if (!selectedDataEntity) {
        throw new Error("No entity selected");
      }
      return apiRequest("DELETE", `/api/entity-data/${selectedDataEntity}/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/entity-data", selectedDataEntity, dataContextCompanyId, dataContextBuId, dataContextLocationId] });
      toast({ title: "Record deleted successfully!" });
    },
    onError: () => {
      toast({ title: "Failed to delete record", variant: "destructive" });
    },
  });

  const resetEntityForm = () => {
    setEntityForm({ name: "", technicalName: "", description: "", category: "" });
  };

  const resetFieldForm = () => {
    setFieldForm({
      id: "",
      fieldName: "",
      dataType: "",
      description: "",
      isRequired: false,
      defaultValue: "",
      validationRules: "",
    });
  };

  const resetEndpointForm = () => {
    setEndpointForm({
      entityId: "",
      operationType: "list",
      name: "",
      httpMethod: "GET",
      pathTemplate: "",
      requestBodyPath: "",
      responseDataPath: "",
      paginationType: "none",
      pageParamName: "",
      limitParamName: "",
      cursorParamName: "",
      offsetParamName: "",
      totalPath: "",
      hasMorePath: "",
      nextCursorPath: "",
      defaultPageSize: 100,
      requiredScopes: [],
      isActive: true,
    });
  };

  const openEditEndpoint = (endpoint: LicenseiqApiEndpoint) => {
    setEditingEndpoint(endpoint);
    setEndpointForm({
      entityId: endpoint.entityId,
      operationType: endpoint.operationType,
      name: endpoint.name,
      httpMethod: endpoint.httpMethod,
      pathTemplate: endpoint.pathTemplate,
      requestBodyPath: endpoint.requestBodyPath || "",
      responseDataPath: endpoint.responseDataPath || "",
      paginationType: endpoint.paginationType || "none",
      pageParamName: endpoint.pageParamName || "",
      limitParamName: endpoint.limitParamName || "",
      cursorParamName: endpoint.cursorParamName || "",
      offsetParamName: endpoint.offsetParamName || "",
      totalPath: endpoint.totalPath || "",
      hasMorePath: endpoint.hasMorePath || "",
      nextCursorPath: endpoint.nextCursorPath || "",
      defaultPageSize: endpoint.defaultPageSize,
      requiredScopes: endpoint.requiredScopes || [],
      isActive: endpoint.isActive,
    });
    setShowEndpointForm(true);
  };

  const handleEndpointSubmit = () => {
    if (editingEndpoint) {
      updateEndpointMutation.mutate({ id: editingEndpoint.id, data: endpointForm });
    } else {
      createEndpointMutation.mutate(endpointForm);
    }
  };

  const handleCreateEndpoint = () => {
    setEditingEndpoint(null);
    setEndpointForm({
      entityId: endpointEntityFilter || "",
      operationType: "list",
      name: "",
      httpMethod: "GET",
      pathTemplate: "",
      requestBodyPath: "",
      responseDataPath: "",
      paginationType: "none",
      pageParamName: "",
      limitParamName: "",
      cursorParamName: "",
      offsetParamName: "",
      totalPath: "",
      hasMorePath: "",
      nextCursorPath: "",
      defaultPageSize: 100,
      requiredScopes: [],
      isActive: true,
    });
    setShowEndpointForm(true);
  };

  const handleCreateEntity = () => {
    setEntityDialogMode("create");
    resetEntityForm();
    setShowEntityForm(true);
  };

  const handleEditEntity = (entity: LicenseiqEntity) => {
    setEntityDialogMode("edit");
    setEntityForm({
      name: entity.name,
      technicalName: entity.technicalName,
      description: entity.description || "",
      category: entity.category || "",
    });
    setSelectedEntity(entity);
    setShowEntityForm(true);
  };

  const handleSaveEntity = () => {
    if (entityDialogMode === "create") {
      createEntityMutation.mutate(entityForm);
    } else if (selectedEntity) {
      updateEntityMutation.mutate({ id: selectedEntity.id, data: entityForm });
    }
  };

  const handleCreateField = () => {
    if (!selectedEntity) {
      toast({ title: "Please select an entity first", variant: "destructive" });
      return;
    }
    setFieldDialogMode("create");
    resetFieldForm();
    setShowFieldForm(true);
  };

  const handleEditField = (field: LicenseiqField) => {
    setFieldDialogMode("edit");
    setFieldForm({
      id: field.id,
      fieldName: field.fieldName,
      dataType: field.dataType,
      description: field.description || "",
      isRequired: field.isRequired,
      defaultValue: field.defaultValue || "",
      validationRules: field.validationRules || "",
    });
    setShowFieldForm(true);
  };

  const handleSaveField = () => {
    const fieldData = {
      entityId: selectedEntity?.id,
      ...fieldForm,
    };

    if (fieldDialogMode === "create") {
      createFieldMutation.mutate(fieldData);
    } else {
      updateFieldMutation.mutate({ id: fieldForm.id, data: fieldData });
    }
  };

  const filteredEntities = entitiesData?.entities.filter((entity) =>
    entity.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entity.technicalName.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entity.description?.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  const filteredFields = fieldsData?.fields.filter((field) =>
    field.fieldName.toLowerCase().includes(searchQuery.toLowerCase()) ||
    field.description?.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  return (
    <MainLayout
      title="LicenseIQ Schema Catalog"
      description="Define and manage your platform's standard data entities and fields"
    >
      <div className="space-y-6">
        {/* Category filter and search */}
        <div className="flex flex-col sm:flex-row gap-3">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
              <Input
                placeholder="Search entities or fields..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 bg-white dark:bg-slate-800"
                data-testid="input-search-schema"
              />
            </div>
            {activeTab === "entities" && (
              <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                <SelectTrigger className="w-full sm:w-48 bg-white dark:bg-slate-800" data-testid="select-category-filter">
                  <SelectValue placeholder="All Categories" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Categories</SelectItem>
                  <SelectItem value="Master Data">Master Data</SelectItem>
                  <SelectItem value="Transactional">Transactional</SelectItem>
                  <SelectItem value="Rules">Rules</SelectItem>
                </SelectContent>
              </Select>
            )}
          </div>

        {/* Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
          <TabsList className="bg-white dark:bg-slate-800 p-1 shadow-sm">
            <TabsTrigger value="entities" className="data-[state=active]:bg-blue-500 data-[state=active]:text-white" data-testid="tab-entities">
              <Database className="h-4 w-4 mr-2" />
              Entities
            </TabsTrigger>
            <TabsTrigger value="fields" className="data-[state=active]:bg-blue-500 data-[state=active]:text-white" data-testid="tab-fields">
              <FileText className="h-4 w-4 mr-2" />
              Fields
            </TabsTrigger>
            <TabsTrigger value="api-endpoints" className="data-[state=active]:bg-blue-500 data-[state=active]:text-white" data-testid="tab-api-endpoints">
              <Globe className="h-4 w-4 mr-2" />
              API Endpoints
            </TabsTrigger>
            <TabsTrigger value="data" className="data-[state=active]:bg-green-500 data-[state=active]:text-white" data-testid="tab-data">
              <Layers className="h-4 w-4 mr-2" />
              Data
            </TabsTrigger>
          </TabsList>

          {/* Entities Tab */}
          <TabsContent value="entities" className="space-y-6">
            {/* Inline Entity Form */}
            {showEntityForm && (
              <Card className="border-2 border-primary/20 bg-primary/5">
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <span>{entityDialogMode === "create" ? "Create New Entity" : "Edit Entity"}</span>
                    <Button variant="ghost" size="sm" onClick={() => {setShowEntityForm(false); resetEntityForm();}} data-testid="button-close-entity-form">
                      ✕
                    </Button>
                  </CardTitle>
                  <CardDescription>
                    Define a new data entity in your LicenseIQ platform schema
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <Label htmlFor="entity-name">Entity Name*</Label>
                    <Input
                      id="entity-name"
                      placeholder="e.g., Sales Data"
                      value={entityForm.name}
                      onChange={(e) => setEntityForm({ ...entityForm, name: e.target.value })}
                      data-testid="input-entity-name"
                    />
                  </div>
                  <div>
                    <Label htmlFor="entity-technical-name">Technical Name*</Label>
                    <Input
                      id="entity-technical-name"
                      placeholder="e.g., sales_data"
                      value={entityForm.technicalName}
                      onChange={(e) => setEntityForm({ ...entityForm, technicalName: e.target.value })}
                      data-testid="input-entity-technical-name"
                    />
                  </div>
                  <div>
                    <Label htmlFor="entity-category">Category</Label>
                    <Select value={entityForm.category} onValueChange={(value) => setEntityForm({ ...entityForm, category: value })}>
                      <SelectTrigger data-testid="select-entity-category">
                        <SelectValue placeholder="Select category" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="Master Data">Master Data</SelectItem>
                        <SelectItem value="Transactional">Transactional</SelectItem>
                        <SelectItem value="Rules">Rules</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="entity-description">Description</Label>
                    <Textarea
                      id="entity-description"
                      placeholder="Describe this entity's purpose..."
                      value={entityForm.description}
                      onChange={(e) => setEntityForm({ ...entityForm, description: e.target.value })}
                      rows={3}
                      data-testid="textarea-entity-description"
                    />
                  </div>
                  <div className="flex gap-2">
                    <Button variant="outline" onClick={() => {setShowEntityForm(false); resetEntityForm();}} data-testid="button-cancel-entity" className="flex-1">
                      Cancel
                    </Button>
                    <Button onClick={handleSaveEntity} data-testid="button-save-entity" className="flex-1">
                      {entityDialogMode === "create" ? "Create Entity" : "Save Changes"}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            )}

            <div className="flex justify-between items-center">
              <p className="text-sm text-gray-600 dark:text-gray-400">
                {filteredEntities.length} {filteredEntities.length === 1 ? "entity" : "entities"} found
              </p>
              <Button onClick={handleCreateEntity} className="bg-gradient-to-r from-blue-500 to-indigo-600 hover:from-blue-600 hover:to-indigo-700" data-testid="button-create-entity">
                <Plus className="h-4 w-4 mr-2" />
                Create Entity
              </Button>
            </div>

            {entitiesLoading ? (
              <div className="text-center py-12">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
                <p className="mt-4 text-gray-600 dark:text-gray-400">Loading entities...</p>
              </div>
            ) : filteredEntities.length === 0 ? (
              <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <Database className="h-16 w-16 text-gray-400 mb-4" />
                  <p className="text-gray-600 dark:text-gray-400 text-center">
                    {searchQuery || (selectedCategory && selectedCategory !== "all") ? "No entities match your filters" : "No entities defined yet"}
                  </p>
                  {!searchQuery && (!selectedCategory || selectedCategory === "all") && (
                    <Button onClick={handleCreateEntity} className="mt-4" variant="outline" data-testid="button-create-first-entity">
                      <Plus className="h-4 w-4 mr-2" />
                      Create Your First Entity
                    </Button>
                  )}
                </CardContent>
              </Card>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredEntities.map((entity) => {
                  const Icon = ENTITY_ICONS[entity.technicalName] || Database;
                  return (
                    <Card
                      key={entity.id}
                      className="hover:shadow-lg transition-all duration-200 cursor-pointer group"
                      onClick={() => {
                        setSelectedEntity(entity);
                        setSearchQuery(""); // Clear search when selecting entity to show all fields
                        setActiveTab("fields");
                      }}
                      data-testid={`card-entity-${entity.id}`}
                    >
                      <CardHeader>
                        <div className="flex items-start justify-between">
                          <div className="flex items-center gap-3">
                            <div className="p-2 bg-gradient-to-br from-blue-100 to-indigo-100 dark:from-blue-900 dark:to-indigo-900 rounded-lg group-hover:scale-110 transition-transform">
                              <Icon className="h-5 w-5 text-blue-600 dark:text-blue-400" />
                            </div>
                            <div>
                              <CardTitle className="text-lg">{entity.name}</CardTitle>
                              <code className="text-xs text-gray-500 dark:text-gray-400">
                                {entity.technicalName}
                              </code>
                            </div>
                          </div>
                          <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => handleEditEntity(entity)}
                              data-testid={`button-edit-entity-${entity.id}`}
                            >
                              <Edit2 className="h-4 w-4" />
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => deleteEntityMutation.mutate(entity.id)}
                              data-testid={`button-delete-entity-${entity.id}`}
                            >
                              <Trash2 className="h-4 w-4 text-red-500" />
                            </Button>
                          </div>
                        </div>
                        {entity.category && (
                          <Badge className={`w-fit mt-2 ${CATEGORY_COLORS[entity.category] || ""}`}>
                            {entity.category}
                          </Badge>
                        )}
                      </CardHeader>
                      {entity.description && (
                        <CardContent>
                          <CardDescription className="text-sm">
                            {entity.description}
                          </CardDescription>
                        </CardContent>
                      )}
                    </Card>
                  );
                })}
              </div>
            )}
          </TabsContent>

          {/* Fields Tab */}
          <TabsContent value="fields" className="space-y-6">
            {!selectedEntity ? (
              <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <FileText className="h-16 w-16 text-gray-400 mb-4" />
                  <p className="text-gray-600 dark:text-gray-400 text-center">
                    Select an entity to view and manage its fields
                  </p>
                  <Button onClick={() => setActiveTab("entities")} className="mt-4" variant="outline" data-testid="button-select-entity">
                    <Database className="h-4 w-4 mr-2" />
                    Browse Entities
                  </Button>
                </CardContent>
              </Card>
            ) : (
              <>
                <div className="flex items-center justify-between bg-white dark:bg-slate-800 rounded-lg p-4 shadow-sm">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-gradient-to-br from-blue-100 to-indigo-100 dark:from-blue-900 dark:to-indigo-900 rounded-lg">
                      {(() => {
                        const Icon = ENTITY_ICONS[selectedEntity.technicalName] || Database;
                        return <Icon className="h-5 w-5 text-blue-600 dark:text-blue-400" />;
                      })()}
                    </div>
                    <div>
                      <h3 className="font-semibold text-lg">{selectedEntity.name}</h3>
                      <code className="text-xs text-gray-500 dark:text-gray-400">
                        {selectedEntity.technicalName}
                      </code>
                    </div>
                  </div>
                  <Button onClick={handleCreateField} className="bg-gradient-to-r from-blue-500 to-indigo-600 hover:from-blue-600 hover:to-indigo-700" data-testid="button-create-field">
                    <Plus className="h-4 w-4 mr-2" />
                    Add Field
                  </Button>
                </div>

                {/* Inline Field Form */}
                {showFieldForm && (
                  <Card className="border-2 border-primary/20 bg-primary/5">
                    <CardHeader>
                      <CardTitle className="flex items-center justify-between">
                        <span>{fieldDialogMode === "create" ? "Add New Field" : "Edit Field"}</span>
                        <Button variant="ghost" size="sm" onClick={() => {setShowFieldForm(false); resetFieldForm();}} data-testid="button-close-field-form">
                          ✕
                        </Button>
                      </CardTitle>
                      <CardDescription>
                        Define a field for {selectedEntity.name}
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div>
                        <Label htmlFor="field-name">Field Name*</Label>
                        <Input
                          id="field-name"
                          placeholder="e.g., transactionId"
                          value={fieldForm.fieldName}
                          onChange={(e) => setFieldForm({ ...fieldForm, fieldName: e.target.value })}
                          data-testid="input-field-name"
                        />
                      </div>
                      <div>
                        <Label htmlFor="field-data-type">Data Type*</Label>
                        <Select value={fieldForm.dataType} onValueChange={(value) => setFieldForm({ ...fieldForm, dataType: value })}>
                          <SelectTrigger data-testid="select-field-data-type">
                            <SelectValue placeholder="Select data type" />
                          </SelectTrigger>
                          <SelectContent>
                            {DATA_TYPES.map((type) => (
                              <SelectItem key={type} value={type}>
                                {type}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="field-description">Description</Label>
                        <Textarea
                          id="field-description"
                          placeholder="Describe this field's purpose..."
                          value={fieldForm.description}
                          onChange={(e) => setFieldForm({ ...fieldForm, description: e.target.value })}
                          rows={2}
                          data-testid="textarea-field-description"
                        />
                      </div>
                      <div className="flex items-center space-x-2">
                        <input
                          type="checkbox"
                          id="field-required"
                          checked={fieldForm.isRequired}
                          onChange={(e) => setFieldForm({ ...fieldForm, isRequired: e.target.checked })}
                          className="rounded border-gray-300"
                          data-testid="checkbox-field-required"
                        />
                        <Label htmlFor="field-required" className="cursor-pointer">
                          Required field
                        </Label>
                      </div>
                      <div className="flex gap-2">
                        <Button variant="outline" onClick={() => {setShowFieldForm(false); resetFieldForm();}} data-testid="button-cancel-field" className="flex-1">
                          Cancel
                        </Button>
                        <Button onClick={handleSaveField} data-testid="button-save-field" className="flex-1">
                          {fieldDialogMode === "create" ? "Add Field" : "Save Changes"}
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {fieldsLoading ? (
                  <div className="text-center py-12">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
                    <p className="mt-4 text-gray-600 dark:text-gray-400">Loading fields...</p>
                  </div>
                ) : filteredFields.length === 0 ? (
                  <Card className="border-dashed">
                    <CardContent className="flex flex-col items-center justify-center py-12">
                      <FileText className="h-16 w-16 text-gray-400 mb-4" />
                      <p className="text-gray-600 dark:text-gray-400 text-center">
                        {searchQuery ? "No fields match your search" : "No fields defined for this entity yet"}
                      </p>
                      {!searchQuery && (
                        <Button onClick={handleCreateField} className="mt-4" variant="outline" data-testid="button-create-first-field">
                          <Plus className="h-4 w-4 mr-2" />
                          Add Your First Field
                        </Button>
                      )}
                    </CardContent>
                  </Card>
                ) : (
                  <Card>
                    <CardContent className="p-0">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Field Name</TableHead>
                            <TableHead>Data Type</TableHead>
                            <TableHead>Required</TableHead>
                            <TableHead>Description</TableHead>
                            <TableHead className="text-right">Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {filteredFields.map((field) => (
                            <TableRow key={field.id} data-testid={`row-field-${field.id}`}>
                              <TableCell className="font-mono font-medium">{field.fieldName}</TableCell>
                              <TableCell>
                                <Badge variant="outline">{field.dataType}</Badge>
                              </TableCell>
                              <TableCell>
                                {field.isRequired ? (
                                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                                ) : (
                                  <span className="text-gray-400">Optional</span>
                                )}
                              </TableCell>
                              <TableCell className="max-w-xs truncate text-sm text-gray-600 dark:text-gray-400">
                                {field.description || "—"}
                              </TableCell>
                              <TableCell className="text-right">
                                <div className="flex justify-end gap-1">
                                  <Button
                                    size="sm"
                                    variant="ghost"
                                    onClick={() => handleEditField(field)}
                                    data-testid={`button-edit-field-${field.id}`}
                                  >
                                    <Edit2 className="h-4 w-4" />
                                  </Button>
                                  <Button
                                    size="sm"
                                    variant="ghost"
                                    onClick={() => deleteFieldMutation.mutate(field.id)}
                                    data-testid={`button-delete-field-${field.id}`}
                                  >
                                    <Trash2 className="h-4 w-4 text-red-500" />
                                  </Button>
                                </div>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </CardContent>
                  </Card>
                )}
              </>
            )}
          </TabsContent>

          {/* API Endpoints Tab */}
          <TabsContent value="api-endpoints" className="space-y-6">
            <Card>
              <CardHeader className="border-b">
                <div className="flex justify-between items-start">
                  <div>
                    <CardTitle className="text-lg flex items-center gap-2">
                      <Globe className="h-5 w-5" />
                      LicenseIQ API Endpoints
                    </CardTitle>
                    <CardDescription>
                      Configure outbound API endpoints to push data from LicenseIQ to external systems
                    </CardDescription>
                  </div>
                  <Button onClick={handleCreateEndpoint} data-testid="button-add-endpoint">
                    <Plus className="h-4 w-4 mr-2" />
                    Add Endpoint
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-6 pt-6">
                {/* Entity Filter */}
                <div className="flex gap-4">
                  <div className="flex-1 max-w-xs">
                    <Label>Filter by Entity</Label>
                    <Select value={endpointEntityFilter || "all"} onValueChange={(v) => setEndpointEntityFilter(v === "all" ? "" : v)}>
                      <SelectTrigger data-testid="select-endpoint-entity-filter">
                        <SelectValue placeholder="All Entities" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All Entities</SelectItem>
                        {entitiesData?.entities?.map(ent => (
                          <SelectItem key={ent.id} value={ent.id}>{ent.name}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Inline Endpoint Form */}
                {showEndpointForm && (
                  <Card className="border-2 border-blue-200 dark:border-blue-800 bg-blue-50/50 dark:bg-blue-900/10">
                    <CardHeader className="pb-4">
                      <CardTitle className="text-lg flex items-center justify-between">
                        <span>{editingEndpoint ? "Edit Endpoint" : "New Endpoint"}</span>
                        <Button variant="ghost" size="sm" onClick={() => { setShowEndpointForm(false); setEditingEndpoint(null); resetEndpointForm(); }} data-testid="button-close-endpoint-form">
                          ✕
                        </Button>
                      </CardTitle>
                      <CardDescription>
                        Define how LicenseIQ data is sent to external APIs
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                      {/* Basic Info */}
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label>Endpoint Name *</Label>
                          <Input
                            placeholder="e.g., Get All Sales"
                            value={endpointForm.name}
                            onChange={(e) => setEndpointForm(prev => ({ ...prev, name: e.target.value }))}
                            data-testid="input-endpoint-name"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Operation Type *</Label>
                          <Select value={endpointForm.operationType} onValueChange={(v) => setEndpointForm(prev => ({ ...prev, operationType: v }))}>
                            <SelectTrigger data-testid="select-endpoint-operation">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="list">List (Get All)</SelectItem>
                              <SelectItem value="get">Get (Single Record)</SelectItem>
                              <SelectItem value="create">Create</SelectItem>
                              <SelectItem value="update">Update</SelectItem>
                              <SelectItem value="delete">Delete</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label>LicenseIQ Entity *</Label>
                          <Select value={endpointForm.entityId} onValueChange={(v) => setEndpointForm(prev => ({ ...prev, entityId: v }))}>
                            <SelectTrigger data-testid="select-endpoint-entity">
                              <SelectValue placeholder="Select Entity" />
                            </SelectTrigger>
                            <SelectContent>
                              {entitiesData?.entities?.map(ent => (
                                <SelectItem key={ent.id} value={ent.id}>{ent.name}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="flex items-center gap-3 pt-6">
                          <input
                            type="checkbox"
                            checked={endpointForm.isActive}
                            onChange={(e) => setEndpointForm(prev => ({ ...prev, isActive: e.target.checked }))}
                            className="h-4 w-4"
                            id="endpoint-active"
                            data-testid="checkbox-endpoint-active"
                          />
                          <Label htmlFor="endpoint-active">Active</Label>
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
                            <Select value={endpointForm.httpMethod} onValueChange={(v) => setEndpointForm(prev => ({ ...prev, httpMethod: v }))}>
                              <SelectTrigger data-testid="select-endpoint-method">
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
                              placeholder="/api/v1/sales or /api/v1/sales/{id}"
                              value={endpointForm.pathTemplate}
                              onChange={(e) => setEndpointForm(prev => ({ ...prev, pathTemplate: e.target.value }))}
                              className="font-mono"
                              data-testid="input-endpoint-path"
                            />
                          </div>
                        </div>
                      </div>

                      <Separator />

                      {/* Response Configuration */}
                      <div className="space-y-4">
                        <h4 className="font-medium flex items-center gap-2">
                          <Code className="h-4 w-4" />
                          Response Configuration
                        </h4>
                        <div className="grid grid-cols-2 gap-4">
                          <div className="space-y-2">
                            <Label>Response Data Path</Label>
                            <Input
                              placeholder="e.g., data.items or results"
                              value={endpointForm.responseDataPath}
                              onChange={(e) => setEndpointForm(prev => ({ ...prev, responseDataPath: e.target.value }))}
                              className="font-mono"
                              data-testid="input-endpoint-response-path"
                            />
                          </div>
                          <div className="space-y-2">
                            <Label>Request Body Path</Label>
                            <Input
                              placeholder="For POST/PUT - e.g., data"
                              value={endpointForm.requestBodyPath}
                              onChange={(e) => setEndpointForm(prev => ({ ...prev, requestBodyPath: e.target.value }))}
                              className="font-mono"
                              data-testid="input-endpoint-request-path"
                            />
                          </div>
                        </div>
                      </div>

                      <Separator />

                      {/* Pagination (collapsible) */}
                      <Collapsible>
                        <CollapsibleTrigger className="flex items-center gap-2 font-medium w-full justify-between" data-testid="trigger-pagination-section">
                          <div className="flex items-center gap-2">
                            <Layers className="h-4 w-4" />
                            Pagination Settings
                          </div>
                          <Badge variant="outline">{endpointForm.paginationType}</Badge>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="pt-4 space-y-4">
                          <div className="grid grid-cols-3 gap-4">
                            <div className="space-y-2">
                              <Label>Pagination Type</Label>
                              <Select value={endpointForm.paginationType} onValueChange={(v) => setEndpointForm(prev => ({ ...prev, paginationType: v }))}>
                                <SelectTrigger data-testid="select-endpoint-pagination">
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="none">None</SelectItem>
                                  <SelectItem value="offset">Offset-based</SelectItem>
                                  <SelectItem value="cursor">Cursor-based</SelectItem>
                                  <SelectItem value="page">Page-based</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <div className="space-y-2">
                              <Label>Default Page Size</Label>
                              <Input
                                type="number"
                                value={endpointForm.defaultPageSize}
                                onChange={(e) => setEndpointForm(prev => ({ ...prev, defaultPageSize: parseInt(e.target.value) || 100 }))}
                                data-testid="input-endpoint-page-size"
                              />
                            </div>
                            <div className="space-y-2">
                              <Label>Total Path</Label>
                              <Input
                                placeholder="e.g., meta.total"
                                value={endpointForm.totalPath}
                                onChange={(e) => setEndpointForm(prev => ({ ...prev, totalPath: e.target.value }))}
                                className="font-mono"
                                data-testid="input-endpoint-total-path"
                              />
                            </div>
                          </div>
                          
                          {endpointForm.paginationType === "offset" && (
                            <div className="grid grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label>Offset Param</Label>
                                <Input
                                  placeholder="e.g., offset"
                                  value={endpointForm.offsetParamName}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, offsetParamName: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-offset-param"
                                />
                              </div>
                              <div className="space-y-2">
                                <Label>Limit Param</Label>
                                <Input
                                  placeholder="e.g., limit"
                                  value={endpointForm.limitParamName}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, limitParamName: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-limit-param"
                                />
                              </div>
                            </div>
                          )}
                          
                          {endpointForm.paginationType === "cursor" && (
                            <div className="grid grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label>Cursor Param</Label>
                                <Input
                                  placeholder="e.g., cursor"
                                  value={endpointForm.cursorParamName}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, cursorParamName: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-cursor-param"
                                />
                              </div>
                              <div className="space-y-2">
                                <Label>Next Cursor Path</Label>
                                <Input
                                  placeholder="e.g., meta.next_cursor"
                                  value={endpointForm.nextCursorPath}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, nextCursorPath: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-next-cursor-path"
                                />
                              </div>
                            </div>
                          )}
                          
                          {endpointForm.paginationType === "page" && (
                            <div className="grid grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label>Page Param</Label>
                                <Input
                                  placeholder="e.g., page"
                                  value={endpointForm.pageParamName}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, pageParamName: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-page-param"
                                />
                              </div>
                              <div className="space-y-2">
                                <Label>Has More Path</Label>
                                <Input
                                  placeholder="e.g., meta.has_more"
                                  value={endpointForm.hasMorePath}
                                  onChange={(e) => setEndpointForm(prev => ({ ...prev, hasMorePath: e.target.value }))}
                                  className="font-mono"
                                  data-testid="input-endpoint-has-more-path"
                                />
                              </div>
                            </div>
                          )}
                        </CollapsibleContent>
                      </Collapsible>

                      <div className="flex gap-3 pt-4">
                        <Button 
                          variant="outline" 
                          onClick={() => { setShowEndpointForm(false); setEditingEndpoint(null); resetEndpointForm(); }} 
                          data-testid="button-cancel-endpoint" 
                          className="flex-1"
                        >
                          Cancel
                        </Button>
                        <Button 
                          onClick={handleEndpointSubmit} 
                          disabled={!endpointForm.name || !endpointForm.entityId || !endpointForm.pathTemplate || createEndpointMutation.isPending || updateEndpointMutation.isPending}
                          data-testid="button-save-endpoint" 
                          className="flex-1"
                        >
                          {editingEndpoint ? "Save Changes" : "Create Endpoint"}
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Endpoints List */}
                {endpointsLoading ? (
                  <div className="text-center py-12">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
                    <p className="mt-4 text-gray-600 dark:text-gray-400">Loading endpoints...</p>
                  </div>
                ) : !endpointsData || endpointsData.length === 0 ? (
                  <Card className="border-dashed">
                    <CardContent className="flex flex-col items-center justify-center py-12">
                      <Globe className="h-16 w-16 text-gray-400 mb-4" />
                      <p className="text-gray-600 dark:text-gray-400 text-center">
                        No API endpoints configured yet
                      </p>
                      <Button 
                        onClick={handleCreateEndpoint} 
                        className="mt-4" 
                        variant="outline" 
                        data-testid="button-create-first-endpoint"
                      >
                        <Plus className="h-4 w-4 mr-2" />
                        Create Your First Endpoint
                      </Button>
                    </CardContent>
                  </Card>
                ) : (
                  <div className="space-y-2">
                    {endpointsData.map((endpoint) => {
                      const entity = entitiesData?.entities?.find(e => e.id === endpoint.entityId);
                      const isExpanded = expandedEndpointId === endpoint.id;
                      
                      return (
                        <Collapsible key={endpoint.id} open={isExpanded} onOpenChange={(open) => setExpandedEndpointId(open ? endpoint.id : null)}>
                          <Card>
                            <CollapsibleTrigger asChild>
                              <CardHeader className="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 py-4" data-testid={`trigger-endpoint-${endpoint.id}`}>
                                <div className="flex items-center justify-between">
                                  <div className="flex items-center gap-3">
                                    {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                                    <Badge variant={endpoint.httpMethod === 'GET' ? 'secondary' : endpoint.httpMethod === 'POST' ? 'default' : 'outline'}>
                                      {endpoint.httpMethod}
                                    </Badge>
                                    <span className="font-medium">{endpoint.name}</span>
                                    <span className="font-mono text-sm text-gray-500">{endpoint.pathTemplate}</span>
                                  </div>
                                  <div className="flex items-center gap-2">
                                    <Badge variant="outline">{entity?.name || "Unknown Entity"}</Badge>
                                    <Badge variant={endpoint.isActive ? "default" : "secondary"}>
                                      {endpoint.isActive ? "Active" : "Inactive"}
                                    </Badge>
                                    <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
                                      <Button size="sm" variant="ghost" onClick={() => openEditEndpoint(endpoint)} data-testid={`button-edit-endpoint-${endpoint.id}`}>
                                        <Edit2 className="h-4 w-4" />
                                      </Button>
                                      <Button size="sm" variant="ghost" onClick={() => deleteEndpointMutation.mutate(endpoint.id)} data-testid={`button-delete-endpoint-${endpoint.id}`}>
                                        <Trash2 className="h-4 w-4 text-red-500" />
                                      </Button>
                                    </div>
                                  </div>
                                </div>
                              </CardHeader>
                            </CollapsibleTrigger>
                            <CollapsibleContent>
                              <CardContent className="pt-0 border-t">
                                <div className="grid grid-cols-3 gap-4 text-sm py-4">
                                  <div>
                                    <p className="text-gray-500">Operation</p>
                                    <p className="font-medium capitalize">{endpoint.operationType}</p>
                                  </div>
                                  <div>
                                    <p className="text-gray-500">Pagination</p>
                                    <p className="font-medium capitalize">{endpoint.paginationType || "None"}</p>
                                  </div>
                                  <div>
                                    <p className="text-gray-500">Page Size</p>
                                    <p className="font-medium">{endpoint.defaultPageSize}</p>
                                  </div>
                                  {endpoint.responseDataPath && (
                                    <div>
                                      <p className="text-gray-500">Response Path</p>
                                      <p className="font-mono text-xs">{endpoint.responseDataPath}</p>
                                    </div>
                                  )}
                                  {endpoint.totalPath && (
                                    <div>
                                      <p className="text-gray-500">Total Path</p>
                                      <p className="font-mono text-xs">{endpoint.totalPath}</p>
                                    </div>
                                  )}
                                </div>
                              </CardContent>
                            </CollapsibleContent>
                          </Card>
                        </Collapsible>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Data Tab */}
          <TabsContent value="data" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Layers className="h-5 w-5 text-green-600" />
                    <span>Entity Data Browser</span>
                  </div>
                </CardTitle>
                <CardDescription>
                  View, add, edit, and delete data records for entities that have database tables
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Organizational Context Selector - only show if entity supports any tenant columns */}
                {(!selectedDataEntity || (entityDataResult?.tenantColumns && (
                  entityDataResult.tenantColumns.hasCompany || 
                  entityDataResult.tenantColumns.hasBU || 
                  entityDataResult.tenantColumns.hasLocation
                ))) && (
                <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Building className="h-4 w-4 text-blue-600" />
                    <span className="font-medium text-blue-800 dark:text-blue-200">Organizational Context</span>
                    {selectedDataEntity && entityDataResult?.tenantColumns && (
                      <Badge variant="outline" className="text-xs">
                        Supports: 
                        {entityDataResult.tenantColumns.hasCompany && " Company"}
                        {entityDataResult.tenantColumns.hasBU && " / BU"}
                        {entityDataResult.tenantColumns.hasLocation && " / Location"}
                      </Badge>
                    )}
                  </div>
                  <p className="text-sm text-blue-600 dark:text-blue-300 mb-3">
                    Select the organizational scope to filter data and auto-assign context to new records
                  </p>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <Label htmlFor="data-context-company">Company</Label>
                      <Select 
                        value={dataContextCompanyId || "_all"} 
                        onValueChange={(value) => {
                          setDataContextCompanyId(value === "_all" ? "" : value);
                          setDataContextBuId("");
                          setDataContextLocationId("");
                        }}
                      >
                        <SelectTrigger data-testid="select-data-context-company">
                          <SelectValue placeholder="All Companies" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="_all">All Companies</SelectItem>
                          {companiesData?.map((company: any) => (
                            <SelectItem key={company.id} value={company.id}>
                              {company.companyName}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="data-context-bu">Business Unit</Label>
                      <Select 
                        value={dataContextBuId || "_all"} 
                        onValueChange={(value) => {
                          setDataContextBuId(value === "_all" ? "" : value);
                          setDataContextLocationId("");
                        }}
                        disabled={!dataContextCompanyId}
                      >
                        <SelectTrigger data-testid="select-data-context-bu">
                          <SelectValue placeholder={dataContextCompanyId ? "All Business Units" : "Select company first"} />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="_all">All Business Units</SelectItem>
                          {businessUnitsData?.map((bu: any) => (
                            <SelectItem key={bu.id} value={bu.id}>
                              {bu.orgName}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="data-context-location">Location</Label>
                      <Select 
                        value={dataContextLocationId || "_all"} 
                        onValueChange={(value) => setDataContextLocationId(value === "_all" ? "" : value)}
                        disabled={!dataContextBuId}
                      >
                        <SelectTrigger data-testid="select-data-context-location">
                          <SelectValue placeholder={dataContextBuId ? "All Locations" : "Select business unit first"} />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="_all">All Locations</SelectItem>
                          {locationsData?.map((loc: any) => (
                            <SelectItem key={loc.id} value={loc.id}>
                              {loc.locationName}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>
                )}

                {/* Entity Selector */}
                <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center">
                  <div className="flex-1 max-w-sm">
                    <Label htmlFor="select-data-entity">Select Entity</Label>
                    <Select 
                      value={selectedDataEntity} 
                      onValueChange={(value) => {
                        setSelectedDataEntity(value);
                        setShowDataForm(false);
                        setEditingRecord(null);
                        // Reset context filters when switching entities to prevent filter leakage
                        setDataContextCompanyId("");
                        setDataContextBuId("");
                        setDataContextLocationId("");
                        // Reset pagination, sorting, and filters
                        setDataPage(1);
                        setDataFilters({});
                        setAdvancedFilters([]);
                        setDataSortColumn("created_at");
                        setDataSortDirection("desc");
                      }}
                    >
                      <SelectTrigger data-testid="select-data-entity">
                        <SelectValue placeholder="Choose an entity with data..." />
                      </SelectTrigger>
                      <SelectContent>
                        {availableTablesData?.tables?.map((table) => (
                          <SelectItem key={table.id} value={table.technicalName}>
                            {table.name} ({table.technicalName})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  {selectedDataEntity && (
                    <Button
                      onClick={() => {
                        setDataFormMode("create");
                        setEditingRecord(null);
                        setDataFormValues({});
                        setShowDataForm(true);
                      }}
                      className="bg-green-600 hover:bg-green-700"
                      data-testid="button-add-data-record"
                    >
                      <Plus className="h-4 w-4 mr-2" />
                      Add Record
                    </Button>
                  )}
                </div>

                {/* Info about available tables */}
                {!selectedDataEntity && availableTablesData?.tables && availableTablesData.tables.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4">
                    <p className="text-green-800 dark:text-green-200 text-sm">
                      <CheckCircle2 className="h-4 w-4 inline mr-2" />
                      <strong>{availableTablesData.tables.length}</strong> entities have database tables with data. 
                      Select one above to view and manage records.
                    </p>
                  </div>
                )}

                {!selectedDataEntity && (!availableTablesData?.tables || availableTablesData.tables.length === 0) && (
                  <div className="text-center py-12 text-gray-500">
                    <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No entities with database tables found.</p>
                    <p className="text-sm">Create entities in the Entities tab and their corresponding database tables.</p>
                  </div>
                )}

                {/* Data Form */}
                {showDataForm && selectedDataEntity && entityColumnsData?.columns && (
                  <Card className="border-2 border-green-200 bg-green-50/50 dark:bg-green-900/10">
                    <CardHeader>
                      <CardTitle className="flex items-center justify-between">
                        <span>{dataFormMode === "create" ? "Add New Record" : "Edit Record"}</span>
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          onClick={() => {
                            setShowDataForm(false);
                            setEditingRecord(null);
                            setDataFormValues({});
                          }}
                          data-testid="button-close-data-form"
                        >
                          ✕
                        </Button>
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {entityColumnsData.columns
                          .filter(col => !['id', 'created_at', 'updated_at', 'created_by', 'updated_by'].includes(col.column_name))
                          .map((column) => (
                            <div key={column.column_name}>
                              <Label htmlFor={`data-${column.column_name}`} className="capitalize">
                                {column.column_name.replace(/_/g, ' ')}
                                {column.is_nullable === 'NO' && <span className="text-red-500 ml-1">*</span>}
                              </Label>
                              {column.data_type === 'boolean' ? (
                                <Select
                                  value={dataFormValues[column.column_name]?.toString() || ''}
                                  onValueChange={(value) => setDataFormValues({
                                    ...dataFormValues,
                                    [column.column_name]: value === 'true'
                                  })}
                                >
                                  <SelectTrigger data-testid={`input-data-${column.column_name}`}>
                                    <SelectValue placeholder="Select..." />
                                  </SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="true">Yes</SelectItem>
                                    <SelectItem value="false">No</SelectItem>
                                  </SelectContent>
                                </Select>
                              ) : (
                                <Input
                                  id={`data-${column.column_name}`}
                                  placeholder={column.column_name}
                                  value={dataFormValues[column.column_name] || ''}
                                  onChange={(e) => setDataFormValues({
                                    ...dataFormValues,
                                    [column.column_name]: e.target.value
                                  })}
                                  data-testid={`input-data-${column.column_name}`}
                                />
                              )}
                            </div>
                          ))}
                      </div>
                      {/* Warning for System Admins without context */}
                      {dataFormMode === "create" && needsContextSelection && (
                        <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg p-4 mb-4">
                          <div className="flex items-center gap-2">
                            <AlertCircle className="h-5 w-5 text-amber-600" />
                            <span className="font-medium text-amber-800 dark:text-amber-200">
                              Please select an Organizational Context
                            </span>
                          </div>
                          <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
                            As a System Admin, you need to select a Company in the Organizational Context panel above before adding records to this entity.
                          </p>
                        </div>
                      )}
                      <div className="flex gap-2 mt-6">
                        <Button
                          onClick={() => {
                            if (dataFormMode === "create") {
                              createDataRecordMutation.mutate(dataFormValues);
                            } else if (editingRecord) {
                              updateDataRecordMutation.mutate({ id: editingRecord.id, data: dataFormValues });
                            }
                          }}
                          disabled={createDataRecordMutation.isPending || updateDataRecordMutation.isPending || (dataFormMode === "create" && needsContextSelection)}
                          className="bg-green-600 hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
                          data-testid="button-save-data-record"
                        >
                          {(createDataRecordMutation.isPending || updateDataRecordMutation.isPending) ? "Saving..." : "Save Record"}
                        </Button>
                        <Button
                          variant="outline"
                          onClick={() => {
                            setShowDataForm(false);
                            setEditingRecord(null);
                            setDataFormValues({});
                          }}
                          data-testid="button-cancel-data-form"
                        >
                          Cancel
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Data Table */}
                {selectedDataEntity && entityDataLoading && (
                  <div className="text-center py-8">
                    <p className="text-gray-500">Loading data...</p>
                  </div>
                )}

                {selectedDataEntity && !entityDataLoading && entityDataResult?.records && (
                  <div className="space-y-4">
                    {/* Toolbar: Info, Filters Toggle, Page Size, Refresh */}
                    <div className="flex flex-wrap items-center justify-between gap-4">
                      <div className="flex items-center gap-4">
                        <p className="text-sm text-gray-500" data-testid="text-record-count">
                          {entityDataResult.total > 0 
                            ? `Showing ${((entityDataResult.page - 1) * entityDataResult.pageSize) + 1}-${Math.min(entityDataResult.page * entityDataResult.pageSize, entityDataResult.total)} of ${entityDataResult.total} records`
                            : "No records found"
                          }
                        </p>
                        <Button 
                          variant={showFilters ? "secondary" : "outline"} 
                          size="sm" 
                          onClick={() => setShowFilters(!showFilters)}
                          data-testid="button-toggle-filters"
                        >
                          <Filter className="h-4 w-4 mr-1" />
                          Filters {(useAdvancedFilters ? advancedFilters.length : Object.keys(dataFilters).filter(k => dataFilters[k]).length) > 0 && 
                            `(${useAdvancedFilters ? advancedFilters.length : Object.keys(dataFilters).filter(k => dataFilters[k]).length})`}
                        </Button>
                        {(useAdvancedFilters ? advancedFilters.length > 0 : Object.keys(dataFilters).filter(k => dataFilters[k]).length > 0) && (
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
                            Clear Filters
                          </Button>
                        )}
                      </div>
                      <div className="flex items-center gap-2">
                        <Label htmlFor="page-size" className="text-sm text-gray-500">Per page:</Label>
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
                        <Button variant="outline" size="sm" onClick={() => refetchEntityData()} data-testid="button-refresh-data">
                          Refresh
                        </Button>
                      </div>
                    </div>

                    {/* Column Filters Panel */}
                    {showFilters && entityColumnsData?.columns && (
                      <div className="border rounded-lg p-4 bg-gray-50 dark:bg-slate-800">
                        <div className="flex items-center justify-between mb-3">
                          <div className="flex items-center gap-2">
                            <Filter className="h-4 w-4 text-gray-500" />
                            <span className="font-medium text-sm">Filters</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Label className="text-xs text-gray-500">Mode:</Label>
                            <Button
                              variant={useAdvancedFilters ? "outline" : "default"}
                              size="sm"
                              onClick={() => { setUseAdvancedFilters(false); setAdvancedFilters([]); }}
                              className="h-7 text-xs"
                              data-testid="button-simple-filters"
                            >
                              Simple
                            </Button>
                            <Button
                              variant={useAdvancedFilters ? "default" : "outline"}
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
                            {entityColumnsData.columns
                              .filter(col => !['created_by', 'updated_by', 'company_id', 'business_unit_id', 'location_id'].includes(col.column_name))
                              .slice(0, 8)
                              .map((col) => (
                                <div key={col.column_name} className="space-y-1">
                                  <Label className="text-xs text-gray-500 capitalize">{col.column_name.replace(/_/g, ' ')}</Label>
                                  <Input
                                    placeholder="Contains..."
                                    value={dataFilters[col.column_name] || ""}
                                    onChange={(e) => {
                                      setDataFilters(prev => ({ ...prev, [col.column_name]: e.target.value }));
                                      setDataPage(1);
                                    }}
                                    className="h-8 text-sm"
                                    data-testid={`input-filter-${col.column_name}`}
                                  />
                                </div>
                              ))}
                          </div>
                        )}
                        
                        {/* Advanced Filters */}
                        {useAdvancedFilters && (
                          <div className="space-y-3">
                            {/* AND/OR Toggle */}
                            <div className="flex items-center gap-3 pb-2 border-b">
                              <Label className="text-xs text-gray-500">Combine filters with:</Label>
                              <div className="flex items-center gap-1">
                                <Button
                                  variant={filterLogic === "and" ? "default" : "outline"}
                                  size="sm"
                                  onClick={() => setFilterLogic("and")}
                                  className="h-6 text-xs px-3"
                                  data-testid="button-logic-and"
                                >
                                  AND
                                </Button>
                                <Button
                                  variant={filterLogic === "or" ? "default" : "outline"}
                                  size="sm"
                                  onClick={() => setFilterLogic("or")}
                                  className="h-6 text-xs px-3"
                                  data-testid="button-logic-or"
                                >
                                  OR
                                </Button>
                              </div>
                              <span className="text-xs text-gray-400 ml-2">
                                {filterLogic === "and" ? "All conditions must match" : "Any condition can match"}
                              </span>
                            </div>
                            
                            {/* Filter Rows */}
                            {advancedFilters.map((filter, idx) => {
                              const colDef = entityColumnsData.columns.find(c => c.column_name === filter.column);
                              const operators = getOperatorsForColumn(colDef?.data_type || "text");
                              return (
                                <div key={idx} className="flex items-center gap-2 flex-wrap">
                                  {/* Column Select */}
                                  <Select
                                    value={filter.column}
                                    onValueChange={(v) => {
                                      const newColDef = entityColumnsData.columns.find(c => c.column_name === v);
                                      const newOperators = getOperatorsForColumn(newColDef?.data_type || "text");
                                      const newFilters = [...advancedFilters];
                                      newFilters[idx] = { ...filter, column: v, operator: newOperators[0].value, value: "", value2: "" };
                                      setAdvancedFilters(newFilters);
                                    }}
                                  >
                                    <SelectTrigger className="w-[150px] h-8" data-testid={`select-filter-column-${idx}`}>
                                      <SelectValue placeholder="Column" />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {entityColumnsData.columns
                                        .filter(col => !['created_by', 'updated_by', 'company_id', 'business_unit_id', 'location_id'].includes(col.column_name))
                                        .map((col) => (
                                          <SelectItem key={col.column_name} value={col.column_name}>
                                            {col.column_name.replace(/_/g, ' ')}
                                          </SelectItem>
                                        ))}
                                    </SelectContent>
                                  </Select>
                                  
                                  {/* Operator Select */}
                                  <Select
                                    value={filter.operator}
                                    onValueChange={(v) => {
                                      const newFilters = [...advancedFilters];
                                      newFilters[idx] = { ...filter, operator: v as FilterOperator };
                                      setAdvancedFilters(newFilters);
                                    }}
                                  >
                                    <SelectTrigger className="w-[140px] h-8" data-testid={`select-filter-operator-${idx}`}>
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {operators.map((op) => (
                                        <SelectItem key={op.value} value={op.value}>{op.label}</SelectItem>
                                      ))}
                                    </SelectContent>
                                  </Select>
                                  
                                  {/* Value Input(s) */}
                                  {!["is_null", "not_null"].includes(filter.operator) && (
                                    <>
                                      <Input
                                        placeholder="Value"
                                        value={filter.value}
                                        onChange={(e) => {
                                          const newFilters = [...advancedFilters];
                                          newFilters[idx] = { ...filter, value: e.target.value };
                                          setAdvancedFilters(newFilters);
                                        }}
                                        className="w-[150px] h-8"
                                        data-testid={`input-filter-value-${idx}`}
                                      />
                                      {filter.operator === "between" && (
                                        <>
                                          <span className="text-xs text-gray-500">and</span>
                                          <Input
                                            placeholder="Value 2"
                                            value={filter.value2 || ""}
                                            onChange={(e) => {
                                              const newFilters = [...advancedFilters];
                                              newFilters[idx] = { ...filter, value2: e.target.value };
                                              setAdvancedFilters(newFilters);
                                            }}
                                            className="w-[150px] h-8"
                                            data-testid={`input-filter-value2-${idx}`}
                                          />
                                        </>
                                      )}
                                    </>
                                  )}
                                  
                                  {/* Remove Button */}
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => {
                                      setAdvancedFilters(advancedFilters.filter((_, i) => i !== idx));
                                      setDataPage(1);
                                    }}
                                    className="h-8 px-2 text-red-600 hover:text-red-700"
                                    data-testid={`button-remove-filter-${idx}`}
                                  >
                                    <X className="h-4 w-4" />
                                  </Button>
                                </div>
                              );
                            })}
                            
                            {/* Add Filter Button */}
                            <div className="flex items-center gap-2 pt-2">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => {
                                  const firstCol = entityColumnsData.columns.find(
                                    col => !['created_by', 'updated_by', 'company_id', 'business_unit_id', 'location_id'].includes(col.column_name)
                                  );
                                  if (firstCol) {
                                    const operators = getOperatorsForColumn(firstCol.data_type);
                                    setAdvancedFilters([...advancedFilters, {
                                      column: firstCol.column_name,
                                      operator: operators[0].value,
                                      value: ""
                                    }]);
                                  }
                                }}
                                className="h-8"
                                data-testid="button-add-filter"
                              >
                                <Plus className="h-4 w-4 mr-1" />
                                Add Filter
                              </Button>
                              {advancedFilters.length > 0 && (
                                <>
                                  <Button
                                    variant="default"
                                    size="sm"
                                    onClick={() => { setDataPage(1); refetchEntityData(); }}
                                    className="h-8"
                                    disabled={advancedFilters.some(f => {
                                      if (['is_null', 'not_null'].includes(f.operator)) return false;
                                      if (!f.value.trim()) return true;
                                      if (f.operator === 'between' && !f.value2?.trim()) return true;
                                      if (['gt', 'gte', 'lt', 'lte', 'between'].includes(f.operator) && isNaN(parseFloat(f.value))) return true;
                                      if (f.operator === 'between' && isNaN(parseFloat(f.value2 || ''))) return true;
                                      return false;
                                    })}
                                    data-testid="button-apply-filters"
                                  >
                                    Apply Filters
                                  </Button>
                                  {advancedFilters.some(f => {
                                    if (['is_null', 'not_null'].includes(f.operator)) return false;
                                    if (!f.value.trim()) return true;
                                    if (f.operator === 'between' && !f.value2?.trim()) return true;
                                    if (['gt', 'gte', 'lt', 'lte', 'between'].includes(f.operator) && isNaN(parseFloat(f.value))) return true;
                                    return false;
                                  }) && (
                                    <span className="text-xs text-amber-600">Fill in all required values</span>
                                  )}
                                </>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    <div className="border rounded-lg overflow-auto max-h-[500px]">
                      <Table>
                        <TableHeader className="sticky top-0 bg-white dark:bg-slate-900 z-10">
                          <TableRow>
                            <TableHead className="w-[100px]">Actions</TableHead>
                            {entityColumnsData?.columns
                              ?.filter(col => !['created_by', 'updated_by', 'company_id', 'business_unit_id', 'location_id'].includes(col.column_name))
                              .slice(0, 8)
                              .map((col) => (
                                <TableHead 
                                  key={col.column_name} 
                                  className="capitalize whitespace-nowrap cursor-pointer hover:bg-gray-100 dark:hover:bg-slate-800 select-none"
                                  onClick={() => {
                                    if (dataSortColumn === col.column_name) {
                                      setDataSortDirection(dataSortDirection === 'asc' ? 'desc' : 'asc');
                                    } else {
                                      setDataSortColumn(col.column_name);
                                      setDataSortDirection('asc');
                                    }
                                    setDataPage(1);
                                  }}
                                  data-testid={`header-sort-${col.column_name}`}
                                >
                                  <div className="flex items-center gap-1">
                                    {col.column_name.replace(/_/g, ' ')}
                                    {dataSortColumn === col.column_name ? (
                                      dataSortDirection === 'asc' ? (
                                        <ChevronUp className="h-4 w-4 text-blue-600" />
                                      ) : (
                                        <ChevronDown className="h-4 w-4 text-blue-600" />
                                      )
                                    ) : (
                                      <ArrowUpDown className="h-3 w-3 text-gray-400" />
                                    )}
                                  </div>
                                </TableHead>
                              ))}
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {entityDataResult.records.map((record, idx) => (
                            <TableRow key={record.id || idx}>
                              <TableCell>
                                <div className="flex gap-1">
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => {
                                      setDataFormMode("edit");
                                      setEditingRecord(record);
                                      setDataFormValues(record);
                                      setShowDataForm(true);
                                    }}
                                    data-testid={`button-edit-record-${record.id}`}
                                  >
                                    <Edit2 className="h-4 w-4" />
                                  </Button>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    className="text-red-600 hover:text-red-700"
                                    onClick={() => {
                                      if (confirm("Are you sure you want to delete this record?")) {
                                        deleteDataRecordMutation.mutate(record.id);
                                      }
                                    }}
                                    data-testid={`button-delete-record-${record.id}`}
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </div>
                              </TableCell>
                              {entityColumnsData?.columns
                                ?.filter(col => !['created_by', 'updated_by', 'company_id', 'business_unit_id', 'location_id'].includes(col.column_name))
                                .slice(0, 8)
                                .map((col) => (
                                  <TableCell key={col.column_name} className="max-w-[200px] truncate">
                                    {record[col.column_name] === null ? (
                                      <span className="text-gray-400 italic">null</span>
                                    ) : record[col.column_name] === true ? (
                                      <Badge variant="outline" className="bg-green-100">Yes</Badge>
                                    ) : record[col.column_name] === false ? (
                                      <Badge variant="outline" className="bg-gray-100">No</Badge>
                                    ) : typeof record[col.column_name] === 'object' ? (
                                      <span className="text-xs font-mono">{JSON.stringify(record[col.column_name])}</span>
                                    ) : (
                                      String(record[col.column_name])
                                    )}
                                  </TableCell>
                                ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>

                    {/* Pagination Controls */}
                    {entityDataResult.totalPages > 1 && (
                      <div className="flex items-center justify-between">
                        <p className="text-sm text-gray-500">
                          Page {entityDataResult.page} of {entityDataResult.totalPages}
                        </p>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(1)}
                            disabled={entityDataResult.page === 1}
                            data-testid="button-first-page"
                          >
                            <ChevronsLeft className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(p => Math.max(1, p - 1))}
                            disabled={entityDataResult.page === 1}
                            data-testid="button-prev-page"
                          >
                            <ChevronLeft className="h-4 w-4" />
                          </Button>
                          {/* Page numbers */}
                          {Array.from({ length: Math.min(5, entityDataResult.totalPages) }, (_, i) => {
                            let pageNum: number;
                            if (entityDataResult.totalPages <= 5) {
                              pageNum = i + 1;
                            } else if (entityDataResult.page <= 3) {
                              pageNum = i + 1;
                            } else if (entityDataResult.page >= entityDataResult.totalPages - 2) {
                              pageNum = entityDataResult.totalPages - 4 + i;
                            } else {
                              pageNum = entityDataResult.page - 2 + i;
                            }
                            return (
                              <Button
                                key={pageNum}
                                variant={entityDataResult.page === pageNum ? "default" : "outline"}
                                size="sm"
                                onClick={() => setDataPage(pageNum)}
                                className="w-8"
                                data-testid={`button-page-${pageNum}`}
                              >
                                {pageNum}
                              </Button>
                            );
                          })}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(p => Math.min(entityDataResult.totalPages, p + 1))}
                            disabled={entityDataResult.page === entityDataResult.totalPages}
                            data-testid="button-next-page"
                          >
                            <ChevronRight className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDataPage(entityDataResult.totalPages)}
                            disabled={entityDataResult.page === entityDataResult.totalPages}
                            data-testid="button-last-page"
                          >
                            <ChevronsRight className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    )}

                    {entityDataResult.records.length === 0 && (
                      <div className="text-center py-8 text-gray-500">
                        <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
                        <p>No records found in this entity.</p>
                        <p className="text-sm">Click "Add Record" to create your first record.</p>
                      </div>
                    )}
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
