import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Plus, Edit2, Trash2, Database, FolderOpen, Receipt, Save, X, RefreshCw } from "lucide-react";

type LicenseiqEntity = {
  id: string;
  name: string;
  technicalName: string;
  description?: string;
  category?: string;
  createdAt?: string;
  updatedAt?: string;
};

type LicenseiqField = {
  id: string;
  entityId: string;
  fieldName: string;
  dataType: string;
  description?: string;
  isRequired: boolean;
  defaultValue?: string;
};

type EntityRecord = {
  id: string;
  entityId: string;
  recordData: Record<string, any>;
  createdAt?: string;
  updatedAt?: string;
};

export default function DataManagement() {
  const { toast } = useToast();
  const [, navigate] = useLocation();
  const [selectedCategory, setSelectedCategory] = useState<string>("Master Data");
  const [selectedEntityId, setSelectedEntityId] = useState<string | null>(null);
  const [showRecordForm, setShowRecordForm] = useState(false);
  const [editingRecord, setEditingRecord] = useState<EntityRecord | null>(null);
  const [recordFormData, setRecordFormData] = useState<Record<string, any>>({});

  // Fetch all entities
  const { data: entitiesData, isLoading: entitiesLoading } = useQuery<{ entities: LicenseiqEntity[] }>({
    queryKey: ['/api/licenseiq-entities'],
  });

  // Fetch fields for selected entity
  const { data: fieldsData } = useQuery<{ fields: LicenseiqField[] }>({
    queryKey: ['/api/licenseiq-fields', selectedEntityId],
    queryFn: async () => {
      if (!selectedEntityId) throw new Error('Entity ID required');
      const response = await fetch(`/api/licenseiq-fields?entityId=${selectedEntityId}`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch fields');
      return response.json();
    },
    enabled: !!selectedEntityId,
  });

  // Fetch records for selected entity
  const { data: recordsData, isLoading: recordsLoading } = useQuery<{ records: EntityRecord[] }>({
    queryKey: ['/api/licenseiq-records', selectedEntityId],
    queryFn: async () => {
      if (!selectedEntityId) throw new Error('Entity ID required');
      const response = await fetch(`/api/licenseiq-records?entityId=${selectedEntityId}`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch records');
      return response.json();
    },
    enabled: !!selectedEntityId,
  });

  // Seed entities mutation
  const seedMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/licenseiq-entities/seed', {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/licenseiq-entities'] });
      toast({ title: "Success", description: "25 standard entities have been added" });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to seed entities", variant: "destructive" });
    },
  });

  // Seed fields mutation
  const seedFieldsMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/licenseiq-fields/seed', {});
    },
    onSuccess: (data: any) => {
      queryClient.invalidateQueries({ queryKey: ['/api/licenseiq-fields'] });
      toast({ 
        title: "Success", 
        description: data.message || "Standard fields have been added to all entities" 
      });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to seed fields", variant: "destructive" });
    },
  });

  // Create record mutation
  const createRecordMutation = useMutation({
    mutationFn: async (data: { entityId: string; recordData: Record<string, any> }) => {
      return await apiRequest('POST', '/api/licenseiq-records', data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/licenseiq-records', selectedEntityId] });
      setShowRecordForm(false);
      setRecordFormData({});
      toast({ title: "Success", description: "Record created successfully" });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to create record", variant: "destructive" });
    },
  });

  // Update record mutation
  const updateRecordMutation = useMutation({
    mutationFn: async ({ id, recordData }: { id: string; recordData: Record<string, any> }) => {
      return await apiRequest('PATCH', `/api/licenseiq-records/${id}`, { recordData });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/licenseiq-records', selectedEntityId] });
      setEditingRecord(null);
      setRecordFormData({});
      toast({ title: "Success", description: "Record updated successfully" });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to update record", variant: "destructive" });
    },
  });

  // Delete record mutation
  const deleteRecordMutation = useMutation({
    mutationFn: async (id: string) => {
      return await apiRequest('DELETE', `/api/licenseiq-records/${id}`, {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/licenseiq-records', selectedEntityId] });
      toast({ title: "Success", description: "Record deleted successfully" });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to delete record", variant: "destructive" });
    },
  });

  const entities = entitiesData?.entities || [];
  const fields = fieldsData?.fields || [];
  const records = recordsData?.records || [];

  const filteredEntities = entities.filter(e => e.category === selectedCategory);
  const selectedEntity = entities.find(e => e.id === selectedEntityId);

  const handleStartAdd = () => {
    setEditingRecord(null);
    setRecordFormData({});
    setShowRecordForm(true);
  };

  const handleStartEdit = (record: EntityRecord) => {
    setEditingRecord(record);
    setRecordFormData(record.recordData);
    setShowRecordForm(true);
  };

  const handleSaveRecord = () => {
    if (editingRecord) {
      updateRecordMutation.mutate({ id: editingRecord.id, recordData: recordFormData });
    } else if (selectedEntityId) {
      createRecordMutation.mutate({ entityId: selectedEntityId, recordData: recordFormData });
    }
  };

  const handleCancelForm = () => {
    setShowRecordForm(false);
    setEditingRecord(null);
    setRecordFormData({});
  };

  const renderFieldInput = (field: LicenseiqField) => {
    const value = recordFormData[field.fieldName] || '';
    
    return (
      <div key={field.id} className="space-y-2">
        <Label className="flex items-center gap-2">
          {field.fieldName}
          {field.isRequired && <Badge variant="destructive" className="text-xs">Required</Badge>}
        </Label>
        {field.dataType === 'boolean' ? (
          <Select 
            value={String(value)} 
            onValueChange={(val) => setRecordFormData({...recordFormData, [field.fieldName]: val === 'true'})}
          >
            <SelectTrigger data-testid={`select-${field.fieldName}`}>
              <SelectValue placeholder="Select..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="true">True</SelectItem>
              <SelectItem value="false">False</SelectItem>
            </SelectContent>
          </Select>
        ) : (
          <Input
            type={field.dataType === 'number' ? 'number' : field.dataType === 'date' ? 'date' : 'text'}
            value={value}
            onChange={(e) => setRecordFormData({...recordFormData, [field.fieldName]: e.target.value})}
            placeholder={field.description || `Enter ${field.fieldName}`}
            data-testid={`input-${field.fieldName}`}
          />
        )}
        {field.description && (
          <p className="text-xs text-muted-foreground">{field.description}</p>
        )}
      </div>
    );
  };

  return (
    <MainLayout
      title="Data Management"
      description="Manage data across all 25 standard LicenseIQ entities"
      actions={
        <>
          <Button
            onClick={() => seedMutation.mutate()}
            disabled={seedMutation.isPending}
            className="bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700"
            data-testid="button-seed-entities"
          >
            <Database className="h-4 w-4 mr-2" />
            {seedMutation.isPending ? 'Seeding...' : entities.length < 25 ? 'Seed All 25 Standard Entities' : 'Reseed Entities'}
          </Button>
          <Button
            onClick={() => seedFieldsMutation.mutate()}
            disabled={seedFieldsMutation.isPending}
            className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
            data-testid="button-seed-fields"
          >
            <FolderOpen className="h-4 w-4 mr-2" />
            {seedFieldsMutation.isPending ? 'Seeding Fields...' : 'Seed Standard Fields'}
          </Button>
        </>
      }
    >
      <div className="space-y-6">

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Sidebar - Entity Selection */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FolderOpen className="h-5 w-5" />
              Entities
            </CardTitle>
            <CardDescription>Select a table to manage</CardDescription>
          </CardHeader>
          <CardContent>
            <Tabs value={selectedCategory} onValueChange={setSelectedCategory} className="mb-4">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="Master Data" data-testid="tab-master-data">Master</TabsTrigger>
                <TabsTrigger value="Transactions" data-testid="tab-transactions">Trans</TabsTrigger>
              </TabsList>
            </Tabs>

            <ScrollArea className="h-[500px]">
              {entitiesLoading ? (
                <div className="flex items-center justify-center py-8">
                  <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
                </div>
              ) : filteredEntities.length === 0 ? (
                <div className="text-center py-8">
                  <p className="text-sm text-muted-foreground mb-4">No entities found</p>
                  <Button
                    size="sm"
                    onClick={() => seedMutation.mutate()}
                    disabled={seedMutation.isPending}
                    data-testid="button-seed-sidebar"
                  >
                    <Database className="h-4 w-4 mr-2" />
                    Seed Entities
                  </Button>
                </div>
              ) : (
                <div className="space-y-1">
                  {filteredEntities.map((entity) => (
                    <Button
                      key={entity.id}
                      variant={selectedEntityId === entity.id ? "default" : "ghost"}
                      className="w-full justify-start"
                      onClick={() => {
                        setSelectedEntityId(entity.id);
                        setShowRecordForm(false);
                      }}
                      data-testid={`button-select-entity-${entity.technicalName}`}
                    >
                      <Receipt className="h-4 w-4 mr-2" />
                      <span className="truncate">{entity.name}</span>
                    </Button>
                  ))}
                </div>
              )}
            </ScrollArea>
          </CardContent>
        </Card>

        {/* Main Content - Data Table */}
        <Card className="lg:col-span-3">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2">
                  {selectedEntity ? (
                    <>
                      <Database className="h-5 w-5" />
                      {selectedEntity.name}
                    </>
                  ) : (
                    'Select an Entity'
                  )}
                </CardTitle>
                <CardDescription>
                  {selectedEntity?.description || 'Choose an entity from the sidebar to view and manage its data'}
                </CardDescription>
              </div>
              {selectedEntityId && (
                <Button onClick={handleStartAdd} data-testid="button-add-record">
                  <Plus className="h-4 w-4 mr-2" />
                  Add Record
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {/* Inline Record Form */}
            {showRecordForm && selectedEntityId && (
              <Card className="border-2 border-primary/20 bg-primary/5 mb-6">
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">
                      {editingRecord ? 'Edit Record' : 'New Record'}
                    </CardTitle>
                    <Button variant="ghost" size="sm" onClick={handleCancelForm}>
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                  <CardDescription>
                    {fields.length === 0 
                      ? 'Add fields to this entity in the LicenseIQ Schema page first' 
                      : 'Fill in the form below to save a record'}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {fields.length === 0 ? (
                    <div className="text-center py-8">
                      <p className="text-sm text-muted-foreground">
                        This entity has no fields defined yet
                      </p>
                    </div>
                  ) : (
                    <>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                        {fields.map(renderFieldInput)}
                      </div>
                      <Separator className="my-4" />
                      <div className="flex gap-2">
                        <Button 
                          variant="outline" 
                          onClick={handleCancelForm}
                          className="flex-1"
                        >
                          Cancel
                        </Button>
                        <Button 
                          onClick={handleSaveRecord}
                          disabled={createRecordMutation.isPending || updateRecordMutation.isPending}
                          className="flex-1"
                          data-testid="button-save-record"
                        >
                          <Save className="h-4 w-4 mr-2" />
                          {createRecordMutation.isPending || updateRecordMutation.isPending 
                            ? 'Saving...' 
                            : editingRecord ? 'Update' : 'Save'}
                        </Button>
                      </div>
                    </>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Records Table */}
            {!selectedEntityId ? (
              <div className="text-center py-16">
                <Database className="h-16 w-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                <h3 className="text-lg font-semibold mb-2">No Entity Selected</h3>
                <p className="text-muted-foreground">
                  Select an entity from the sidebar to view and manage its data
                </p>
              </div>
            ) : recordsLoading ? (
              <div className="flex items-center justify-center py-16">
                <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : fields.length === 0 ? (
              <div className="text-center py-16">
                <Database className="h-16 w-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                <h3 className="text-lg font-semibold mb-2">No Fields Defined</h3>
                <p className="text-muted-foreground mb-4">
                  This entity has no fields yet. Define fields in the LicenseIQ Schema page before adding data.
                </p>
                <Button onClick={() => navigate('/licenseiq-schema')} data-testid="button-define-fields">
                  <Database className="h-4 w-4 mr-2" />
                  Go to Schema Manager
                </Button>
              </div>
            ) : records.length === 0 && !showRecordForm ? (
              <div className="text-center py-16">
                <Receipt className="h-16 w-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                <h3 className="text-lg font-semibold mb-2">No Records Found</h3>
                <p className="text-muted-foreground mb-4">
                  This table is empty. Add your first record to get started.
                </p>
                <Button onClick={handleStartAdd} data-testid="button-add-first-record">
                  <Plus className="h-4 w-4 mr-2" />
                  Add First Record
                </Button>
              </div>
            ) : records.length > 0 ? (
              <ScrollArea className="h-[600px]">
                <Table>
                  <TableHeader>
                    <TableRow>
                      {fields.map((field) => (
                        <TableHead key={field.id}>{field.fieldName}</TableHead>
                      ))}
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {records.map((record) => (
                      <TableRow key={record.id} data-testid={`row-record-${record.id}`}>
                        {fields.map((field) => (
                          <TableCell key={field.id}>
                            {record.recordData[field.fieldName] !== undefined && record.recordData[field.fieldName] !== null
                              ? String(record.recordData[field.fieldName])
                              : '-'}
                          </TableCell>
                        ))}
                        <TableCell className="text-right">
                          <div className="flex justify-end gap-2">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleStartEdit(record)}
                              data-testid={`button-edit-record-${record.id}`}
                            >
                              <Edit2 className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => deleteRecordMutation.mutate(record.id)}
                              data-testid={`button-delete-record-${record.id}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </ScrollArea>
            ) : null}
          </CardContent>
        </Card>
      </div>
      </div>
    </MainLayout>
  );
}
