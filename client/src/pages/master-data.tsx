import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { queryClient, apiRequest } from '@/lib/queryClient';
import MainLayout from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { Textarea } from '@/components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { useToast } from '@/hooks/use-toast';
import { Building2, Layers, MapPin, Plus, ChevronRight, ChevronDown, Check, X, Trash2, Edit2 } from 'lucide-react';
import { useLocation } from 'wouter';
import type { Company, BusinessUnit, Location } from '@shared/schema';

type Status = 'A' | 'I' | 'D';

interface HierarchyNode {
  companies: (Company & { 
    businessUnits: (BusinessUnit & { 
      locations: Location[] 
    })[] 
  })[];
}

export default function MasterDataPage() {
  const [filterStatus, setFilterStatus] = useState<string>('A');
  const { toast } = useToast();
  const [, setLocation] = useLocation();

  const { data: hierarchy, isLoading } = useQuery<HierarchyNode>({
    queryKey: ['/api/master-data/hierarchy', filterStatus],
    queryFn: async () => {
      const params = filterStatus !== 'all' ? `?status=${filterStatus}` : '';
      const response = await fetch(`/api/master-data/hierarchy${params}`);
      if (!response.ok) throw new Error('Failed to fetch hierarchy');
      return response.json();
    },
  });

  if (isLoading) {
    return (
      <MainLayout
        title="Client Master Hierarchy"
        description="Manage your organizational hierarchy"
      >
        <div className="flex items-center justify-center h-96">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
            <p className="text-muted-foreground">Loading hierarchy...</p>
          </div>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout
      title="Client Master Hierarchy"
      description="Manage your organizational hierarchy"
      actions={
        <>
          <Select value={filterStatus} onValueChange={setFilterStatus}>
            <SelectTrigger className="w-40" data-testid="select-status-filter">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="A">Active Only</SelectItem>
              <SelectItem value="I">Inactive Only</SelectItem>
              <SelectItem value="all">All Records</SelectItem>
            </SelectContent>
          </Select>
          <AddCompanyButton />
        </>
      }
    >
      <div className="space-y-6">
          <Card className="shadow-lg border-2 backdrop-blur-sm bg-card/95">
            {hierarchy?.companies && hierarchy.companies.length > 0 ? (
              <div className="p-6 space-y-3">
                {hierarchy.companies.map((company) => (
                  <CompanyNode key={company.id} company={company} />
                ))}
              </div>
            ) : (
              <div className="text-center py-16">
                <div className="inline-flex items-center justify-center w-20 h-20 rounded-full bg-primary/10 mb-4">
                  <Building2 className="h-10 w-10 text-primary" />
                </div>
                <h3 className="text-xl font-semibold mb-2">No companies found</h3>
                <p className="text-muted-foreground mb-6 max-w-sm mx-auto">
                  Get started by creating your first company to build your organizational hierarchy
                </p>
                <AddCompanyButton />
              </div>
            )}
          </Card>
      </div>
    </MainLayout>
  );
}

function CompanyNode({ company }: { company: Company & { businessUnits: (BusinessUnit & { locations: Location[] })[] } }) {
  const [isExpanded, setIsExpanded] = useState(true);
  const [isEditing, setIsEditing] = useState(false);
  const [showAddBU, setShowAddBU] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const { toast } = useToast();

  const totalBUs = company.businessUnits?.length || 0;
  const totalLocs = company.businessUnits?.reduce((sum, bu) => sum + (bu.locations?.length || 0), 0) || 0;

  const deleteMutation = useMutation({
    mutationFn: () => apiRequest('DELETE', `/api/master-data/companies/${company.id}`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Company deleted successfully' });
      setShowDeleteDialog(false);
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to delete company', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  return (
    <div className="group" data-testid={`company-node-${company.id}`}>
      <div className="flex items-center gap-3 p-4 rounded-xl hover:bg-gradient-to-r hover:from-primary/5 hover:to-blue-500/5 transition-all duration-200 border border-border/50 hover:border-primary/30 shadow-sm hover:shadow-md bg-card/50">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="shrink-0 hover:bg-primary/10 p-1.5 rounded-md transition-colors"
          data-testid={`button-toggle-company-${company.id}`}
          aria-label={isExpanded ? 'Collapse company' : 'Expand company'}
        >
          {isExpanded ? (
            <ChevronDown className="h-5 w-5 text-primary" />
          ) : (
            <ChevronRight className="h-5 w-5 text-muted-foreground" />
          )}
        </button>
        <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-gradient-to-br from-primary to-blue-600 shadow-md">
          <Building2 className="h-5 w-5 text-white shrink-0" />
        </div>
        <div className="flex-1 min-w-0">
          {isEditing ? (
            <CompanyEditForm 
              company={company} 
              onCancel={() => setIsEditing(false)} 
              onSaved={() => setIsEditing(false)} 
            />
          ) : (
            <div className="flex items-center gap-3">
              <span 
                className="font-semibold cursor-pointer hover:text-primary transition-colors" 
                onClick={() => setIsEditing(true)}
                data-testid={`text-company-name-${company.id}`}
              >
                {company.companyName}
              </span>
              <StatusBadge 
                status={company.status as Status} 
                entityType="company" 
                entityId={company.id} 
              />
            </div>
          )}
        </div>
        <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          {!isEditing && (
            <>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setIsEditing(true)}
                data-testid={`button-edit-company-${company.id}`}
              >
                <Edit2 className="h-4 w-4 mr-1" />
                Edit Details
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowAddBU(!showAddBU)}
                data-testid={`button-add-bu-${company.id}`}
              >
                <Plus className="h-4 w-4 mr-1" />
                Add Unit
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowDeleteDialog(true)}
                className="text-destructive hover:text-destructive hover:bg-destructive/10"
                data-testid={`button-delete-company-${company.id}`}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </>
          )}
        </div>
      </div>

      {isExpanded && (
        <div className="ml-6 mt-1 border-l-2 border-muted pl-4 space-y-1">
          {showAddBU && (
            <BusinessUnitForm 
              companyId={company.id} 
              onCancel={() => setShowAddBU(false)}
              onSaved={() => setShowAddBU(false)}
            />
          )}
          {company.businessUnits?.map((bu) => (
            <BusinessUnitNode key={bu.id} businessUnit={bu} companyId={company.id} />
          ))}
        </div>
      )}

      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Company?</AlertDialogTitle>
            <AlertDialogDescription>
              {totalBUs > 0 || totalLocs > 0 ? (
                <>
                  This will permanently delete <strong>{company.companyName}</strong> and all its children:
                  <ul className="list-disc list-inside mt-2 space-y-1">
                    {totalBUs > 0 && <li>{totalBUs} Business Unit{totalBUs > 1 ? 's' : ''}</li>}
                    {totalLocs > 0 && <li>{totalLocs} Location{totalLocs > 1 ? 's' : ''}</li>}
                  </ul>
                  <p className="mt-2 text-destructive font-semibold">This action cannot be undone.</p>
                </>
              ) : (
                <>
                  Are you sure you want to delete <strong>{company.companyName}</strong>? This action cannot be undone.
                </>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function BusinessUnitNode({ 
  businessUnit, 
  companyId 
}: { 
  businessUnit: BusinessUnit & { locations: Location[] }; 
  companyId: string;
}) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const [showAddLoc, setShowAddLoc] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const { toast } = useToast();

  const totalLocs = businessUnit.locations?.length || 0;

  const deleteMutation = useMutation({
    mutationFn: () => apiRequest('DELETE', `/api/master-data/business-units/${businessUnit.id}`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Business unit deleted successfully' });
      setShowDeleteDialog(false);
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to delete business unit', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  return (
    <div className="group" data-testid={`bu-node-${businessUnit.id}`}>
      <div className="flex items-center gap-2.5 p-3 rounded-lg hover:bg-gradient-to-r hover:from-blue-500/5 hover:to-blue-600/5 transition-all duration-200 border border-transparent hover:border-blue-500/20">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="shrink-0 hover:bg-blue-500/10 p-1 rounded transition-colors"
          data-testid={`button-toggle-bu-${businessUnit.id}`}
          aria-label={isExpanded ? 'Collapse business unit' : 'Expand business unit'}
        >
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-blue-500" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )}
        </button>
        <div className="flex items-center justify-center w-8 h-8 rounded-md bg-gradient-to-br from-blue-500 to-blue-600 shadow">
          <Layers className="h-4 w-4 text-white shrink-0" />
        </div>
        <div className="flex-1 min-w-0">
          {isEditing ? (
            <BusinessUnitEditForm 
              businessUnit={businessUnit} 
              onCancel={() => setIsEditing(false)} 
              onSaved={() => setIsEditing(false)} 
            />
          ) : (
            <div className="flex items-center gap-2">
              <span 
                className="text-sm font-medium cursor-pointer hover:text-primary transition-colors" 
                onClick={() => setIsEditing(true)}
                data-testid={`text-bu-name-${businessUnit.id}`}
              >
                {businessUnit.orgName}
              </span>
              <StatusBadge 
                status={businessUnit.status as Status} 
                entityType="business_unit" 
                entityId={businessUnit.id} 
              />
            </div>
          )}
        </div>
        <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          {!isEditing && (
            <>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setIsEditing(true)}
                data-testid={`button-edit-bu-${businessUnit.id}`}
              >
                <Edit2 className="h-3 w-3 mr-1" />
                Edit
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowAddLoc(!showAddLoc)}
                data-testid={`button-add-location-${businessUnit.id}`}
              >
                <Plus className="h-3 w-3 mr-1" />
                Add Location
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowDeleteDialog(true)}
                className="text-destructive hover:text-destructive hover:bg-destructive/10"
                data-testid={`button-delete-bu-${businessUnit.id}`}
              >
                <Trash2 className="h-3 w-3" />
              </Button>
            </>
          )}
        </div>
      </div>

      {isExpanded && (
        <div className="ml-6 mt-1 border-l-2 border-muted/50 pl-4 space-y-1">
          {showAddLoc && (
            <LocationForm 
              companyId={companyId}
              orgId={businessUnit.id} 
              onCancel={() => setShowAddLoc(false)}
              onSaved={() => setShowAddLoc(false)}
            />
          )}
          {businessUnit.locations?.map((loc) => (
            <LocationNode key={loc.id} location={loc} />
          ))}
        </div>
      )}

      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Business Unit?</AlertDialogTitle>
            <AlertDialogDescription>
              {totalLocs > 0 ? (
                <>
                  This will permanently delete <strong>{businessUnit.orgName}</strong> and all its children:
                  <ul className="list-disc list-inside mt-2">
                    <li>{totalLocs} Location{totalLocs > 1 ? 's' : ''}</li>
                  </ul>
                  <p className="mt-2 text-destructive font-semibold">This action cannot be undone.</p>
                </>
              ) : (
                <>
                  Are you sure you want to delete <strong>{businessUnit.orgName}</strong>? This action cannot be undone.
                </>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function LocationNode({ location }: { location: Location }) {
  const [isEditing, setIsEditing] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const { toast } = useToast();

  const deleteMutation = useMutation({
    mutationFn: () => apiRequest('DELETE', `/api/master-data/locations/${location.id}`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Location deleted successfully' });
      setShowDeleteDialog(false);
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to delete location', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  return (
    <div className="group flex items-center gap-2.5 p-3 rounded-lg hover:bg-gradient-to-r hover:from-green-500/5 hover:to-emerald-500/5 transition-all duration-200 border border-transparent hover:border-green-500/20" data-testid={`location-node-${location.id}`}>
      <div className="flex items-center justify-center w-7 h-7 rounded-md bg-gradient-to-br from-green-500 to-emerald-600 shadow ml-6">
        <MapPin className="h-3.5 w-3.5 text-white shrink-0" />
      </div>
      <div className="flex-1 min-w-0">
        {isEditing ? (
          <LocationEditForm 
            location={location} 
            onCancel={() => setIsEditing(false)} 
            onSaved={() => setIsEditing(false)} 
          />
        ) : (
          <div className="flex items-center gap-2">
            <span 
              className="text-sm cursor-pointer hover:text-primary transition-colors" 
              onClick={() => setIsEditing(true)}
              data-testid={`text-location-name-${location.id}`}
            >
              {location.locName}
            </span>
            <StatusBadge 
              status={location.status as Status} 
              entityType="location" 
              entityId={location.id} 
            />
          </div>
        )}
      </div>
      <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        {!isEditing && (
          <>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsEditing(true)}
              data-testid={`button-edit-location-${location.id}`}
            >
              <Edit2 className="h-3 w-3 mr-1" />
              Edit
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setShowDeleteDialog(true)}
              className="text-destructive hover:text-destructive hover:bg-destructive/10"
              data-testid={`button-delete-location-${location.id}`}
            >
              <Trash2 className="h-3 w-3" />
            </Button>
          </>
        )}
      </div>

      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Location?</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete <strong>{location.locName}</strong>? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function StatusBadge({ 
  status, 
  entityType, 
  entityId 
}: { 
  status: Status; 
  entityType: 'company' | 'business_unit' | 'location'; 
  entityId: string;
}) {
  const [isEditing, setIsEditing] = useState(false);
  const { toast } = useToast();

  const updateMutation = useMutation({
    mutationFn: async (newStatus: Status) => {
      const endpoints = {
        company: '/api/master-data/companies',
        business_unit: '/api/master-data/business-units',
        location: '/api/master-data/locations',
      };
      return apiRequest('PATCH', `${endpoints[entityType]}/${entityId}`, { status: newStatus });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Status updated successfully' });
      setIsEditing(false);
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to update status', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const statusConfig = {
    A: { label: 'Active', className: 'bg-green-500/10 text-green-700 dark:text-green-400 hover:bg-green-500/20' },
    I: { label: 'Inactive', className: 'bg-yellow-500/10 text-yellow-700 dark:text-yellow-400 hover:bg-yellow-500/20' },
    D: { label: 'Deleted', className: 'bg-red-500/10 text-red-700 dark:text-red-400 hover:bg-red-500/20' },
  };

  const config = statusConfig[status];

  if (isEditing) {
    return (
      <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
        <Select 
          value={status} 
          onValueChange={(val) => updateMutation.mutate(val as Status)}
          data-testid={`select-status-${entityType}-${entityId}`}
        >
          <SelectTrigger className="h-6 w-24 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="A">Active</SelectItem>
            <SelectItem value="I">Inactive</SelectItem>
            <SelectItem value="D">Deleted</SelectItem>
          </SelectContent>
        </Select>
        <Button 
          variant="ghost" 
          size="sm" 
          onClick={() => setIsEditing(false)}
          className="h-6 w-6 p-0"
        >
          <X className="h-3 w-3" />
        </Button>
      </div>
    );
  }

  return (
    <Badge 
      className={`text-xs cursor-pointer ${config.className}`}
      onClick={(e) => {
        e.stopPropagation();
        setIsEditing(true);
      }}
      data-testid={`badge-status-${entityType}-${entityId}`}
    >
      {config.label}
    </Badge>
  );
}

function AddCompanyButton() {
  const [isAdding, setIsAdding] = useState(false);

  if (isAdding) {
    return (
      <CompanyForm 
        onCancel={() => setIsAdding(false)} 
        onSaved={() => setIsAdding(false)} 
      />
    );
  }

  return (
    <Button onClick={() => setIsAdding(true)} data-testid="button-add-company">
      <Plus className="h-4 w-4 mr-2" />
      Add Company
    </Button>
  );
}

function CompanyForm({ onCancel, onSaved }: { onCancel: () => void; onSaved: () => void }) {
  const [name, setName] = useState('');
  const { toast } = useToast();

  const createMutation = useMutation({
    mutationFn: (data: { companyName: string }) => 
      apiRequest('POST', '/api/master-data/companies', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Company created successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to create company', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    createMutation.mutate({ companyName: name });
  };

  return (
    <form onSubmit={handleSubmit} className="flex items-center gap-2 p-3 bg-accent/30 rounded-lg">
      <Building2 className="h-5 w-5 text-primary" />
      <Input
        placeholder="Company name *"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="flex-1"
        autoFocus
        data-testid="input-company-name"
      />
      <Button type="submit" size="sm" disabled={!name.trim() || createMutation.isPending} data-testid="button-save-company">
        <Check className="h-4 w-4" />
      </Button>
      <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-company">
        <X className="h-4 w-4" />
      </Button>
    </form>
  );
}

function CompanyEditForm({ company, onCancel, onSaved }: { company: Company; onCancel: () => void; onSaved: () => void }) {
  const [formData, setFormData] = useState({
    companyName: company.companyName,
    companyDescr: company.companyDescr || '',
    address1: company.address1 || '',
    address2: company.address2 || '',
    address3: company.address3 || '',
    city: company.city || '',
    stateProvince: company.stateProvince || '',
    county: company.county || '',
    country: company.country || '',
    contactPerson: company.contactPerson || '',
    contactEmail: company.contactEmail || '',
    contactPhone: company.contactPhone || '',
    contactPreference: company.contactPreference || '',
  });
  const { toast } = useToast();

  const updateMutation = useMutation({
    mutationFn: (data: any) => 
      apiRequest('PATCH', `/api/master-data/companies/${company.id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Company updated successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to update company', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.companyName.trim()) return;
    updateMutation.mutate(formData);
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-3 p-4 bg-accent/30 rounded-lg" onClick={(e) => e.stopPropagation()}>
      <div className="grid grid-cols-2 gap-3">
        <div className="col-span-2">
          <Input
            placeholder="Company name *"
            value={formData.companyName}
            onChange={(e) => setFormData({...formData, companyName: e.target.value})}
            autoFocus
            data-testid="input-edit-company-name"
          />
        </div>
        <div className="col-span-2">
          <Textarea
            placeholder="Description"
            value={formData.companyDescr}
            onChange={(e) => setFormData({...formData, companyDescr: e.target.value})}
            rows={2}
            data-testid="input-edit-company-descr"
          />
        </div>
        <Input
          placeholder="Address 1"
          value={formData.address1}
          onChange={(e) => setFormData({...formData, address1: e.target.value})}
          data-testid="input-edit-company-address1"
        />
        <Input
          placeholder="Address 2"
          value={formData.address2}
          onChange={(e) => setFormData({...formData, address2: e.target.value})}
          data-testid="input-edit-company-address2"
        />
        <Input
          placeholder="Address 3"
          value={formData.address3}
          onChange={(e) => setFormData({...formData, address3: e.target.value})}
          data-testid="input-edit-company-address3"
        />
        <Input
          placeholder="City"
          value={formData.city}
          onChange={(e) => setFormData({...formData, city: e.target.value})}
          data-testid="input-edit-company-city"
        />
        <Input
          placeholder="State/Province"
          value={formData.stateProvince}
          onChange={(e) => setFormData({...formData, stateProvince: e.target.value})}
          data-testid="input-edit-company-state"
        />
        <Input
          placeholder="County"
          value={formData.county}
          onChange={(e) => setFormData({...formData, county: e.target.value})}
          data-testid="input-edit-company-county"
        />
        <Input
          placeholder="Country"
          value={formData.country}
          onChange={(e) => setFormData({...formData, country: e.target.value})}
          data-testid="input-edit-company-country"
        />
        <Input
          placeholder="Contact Person"
          value={formData.contactPerson}
          onChange={(e) => setFormData({...formData, contactPerson: e.target.value})}
          data-testid="input-edit-company-contact-person"
        />
        <Input
          placeholder="Contact Email"
          value={formData.contactEmail}
          onChange={(e) => setFormData({...formData, contactEmail: e.target.value})}
          data-testid="input-edit-company-contact-email"
        />
        <Input
          placeholder="Contact Phone"
          value={formData.contactPhone}
          onChange={(e) => setFormData({...formData, contactPhone: e.target.value})}
          data-testid="input-edit-company-contact-phone"
        />
        <Select 
          value={formData.contactPreference} 
          onValueChange={(val) => setFormData({...formData, contactPreference: val})}
        >
          <SelectTrigger data-testid="select-edit-company-contact-preference">
            <SelectValue placeholder="Contact Preference" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="email">Email</SelectItem>
            <SelectItem value="phone">Phone</SelectItem>
            <SelectItem value="both">Both</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex gap-2">
        <Button type="submit" size="sm" disabled={!formData.companyName.trim() || updateMutation.isPending} data-testid="button-update-company">
          <Check className="h-4 w-4 mr-1" />
          Save Changes
        </Button>
        <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-edit-company">
          <X className="h-4 w-4 mr-1" />
          Cancel
        </Button>
      </div>
    </form>
  );
}

function BusinessUnitForm({ companyId, onCancel, onSaved }: { companyId: string; onCancel: () => void; onSaved: () => void }) {
  const [name, setName] = useState('');
  const { toast } = useToast();

  const createMutation = useMutation({
    mutationFn: (data: { orgName: string; companyId: string }) => 
      apiRequest('POST', '/api/master-data/business-units', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Business unit created successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to create business unit', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    createMutation.mutate({ orgName: name, companyId });
  };

  return (
    <form onSubmit={handleSubmit} className="flex items-center gap-2 p-2 bg-accent/20 rounded-lg mb-1">
      <Layers className="h-4 w-4 text-blue-500" />
      <Input
        placeholder="Business unit name *"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="flex-1 h-8 text-sm"
        autoFocus
        data-testid="input-bu-name"
      />
      <Button type="submit" size="sm" disabled={!name.trim() || createMutation.isPending} data-testid="button-save-bu">
        <Check className="h-3 w-3" />
      </Button>
      <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-bu">
        <X className="h-3 w-3" />
      </Button>
    </form>
  );
}

function BusinessUnitEditForm({ businessUnit, onCancel, onSaved }: { businessUnit: BusinessUnit; onCancel: () => void; onSaved: () => void }) {
  const [formData, setFormData] = useState({
    orgName: businessUnit.orgName,
    orgDescr: businessUnit.orgDescr || '',
    address1: businessUnit.address1 || '',
    contactPerson: businessUnit.contactPerson || '',
    contactEmail: businessUnit.contactEmail || '',
    contactPhone: businessUnit.contactPhone || '',
    contactPreference: businessUnit.contactPreference || '',
  });
  const { toast } = useToast();

  const updateMutation = useMutation({
    mutationFn: (data: any) => 
      apiRequest('PATCH', `/api/master-data/business-units/${businessUnit.id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Business unit updated successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to update business unit', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.orgName.trim()) return;
    updateMutation.mutate(formData);
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-2 p-3 bg-accent/20 rounded-lg" onClick={(e) => e.stopPropagation()}>
      <div className="grid grid-cols-2 gap-2">
        <div className="col-span-2">
          <Input
            placeholder="Business unit name *"
            value={formData.orgName}
            onChange={(e) => setFormData({...formData, orgName: e.target.value})}
            className="text-sm"
            autoFocus
            data-testid="input-edit-bu-name"
          />
        </div>
        <div className="col-span-2">
          <Textarea
            placeholder="Description"
            value={formData.orgDescr}
            onChange={(e) => setFormData({...formData, orgDescr: e.target.value})}
            rows={2}
            className="text-sm"
            data-testid="input-edit-bu-descr"
          />
        </div>
        <div className="col-span-2">
          <Input
            placeholder="Address"
            value={formData.address1}
            onChange={(e) => setFormData({...formData, address1: e.target.value})}
            className="text-sm"
            data-testid="input-edit-bu-address"
          />
        </div>
        <Input
          placeholder="Contact Person"
          value={formData.contactPerson}
          onChange={(e) => setFormData({...formData, contactPerson: e.target.value})}
          className="text-sm"
          data-testid="input-edit-bu-contact-person"
        />
        <Input
          placeholder="Contact Email"
          value={formData.contactEmail}
          onChange={(e) => setFormData({...formData, contactEmail: e.target.value})}
          className="text-sm"
          data-testid="input-edit-bu-contact-email"
        />
        <Input
          placeholder="Contact Phone"
          value={formData.contactPhone}
          onChange={(e) => setFormData({...formData, contactPhone: e.target.value})}
          className="text-sm"
          data-testid="input-edit-bu-contact-phone"
        />
        <Select 
          value={formData.contactPreference} 
          onValueChange={(val) => setFormData({...formData, contactPreference: val})}
        >
          <SelectTrigger className="text-sm" data-testid="select-edit-bu-contact-preference">
            <SelectValue placeholder="Contact Preference" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="email">Email</SelectItem>
            <SelectItem value="phone">Phone</SelectItem>
            <SelectItem value="both">Both</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex gap-2">
        <Button type="submit" size="sm" disabled={!formData.orgName.trim() || updateMutation.isPending} data-testid="button-update-bu">
          <Check className="h-3 w-3 mr-1" />
          Save
        </Button>
        <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-edit-bu">
          <X className="h-3 w-3 mr-1" />
          Cancel
        </Button>
      </div>
    </form>
  );
}

function LocationForm({ companyId, orgId, onCancel, onSaved }: { companyId: string; orgId: string; onCancel: () => void; onSaved: () => void }) {
  const [name, setName] = useState('');
  const { toast } = useToast();

  const createMutation = useMutation({
    mutationFn: (data: { locName: string; companyId: string; orgId: string }) => 
      apiRequest('POST', '/api/master-data/locations', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Location created successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to create location', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    createMutation.mutate({ locName: name, companyId, orgId });
  };

  return (
    <form onSubmit={handleSubmit} className="flex items-center gap-2 p-2 bg-accent/10 rounded-lg mb-1">
      <MapPin className="h-4 w-4 text-green-500" />
      <Input
        placeholder="Location name *"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="flex-1 h-8 text-sm"
        autoFocus
        data-testid="input-location-name"
      />
      <Button type="submit" size="sm" disabled={!name.trim() || createMutation.isPending} data-testid="button-save-location">
        <Check className="h-3 w-3" />
      </Button>
      <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-location">
        <X className="h-3 w-3" />
      </Button>
    </form>
  );
}

function LocationEditForm({ location, onCancel, onSaved }: { location: Location; onCancel: () => void; onSaved: () => void }) {
  const [formData, setFormData] = useState({
    locName: location.locName,
    locDescr: location.locDescr || '',
    address1: location.address1 || '',
    contactPerson: location.contactPerson || '',
    contactEmail: location.contactEmail || '',
    contactPhone: location.contactPhone || '',
    contactPreference: location.contactPreference || '',
  });
  const { toast } = useToast();

  const updateMutation = useMutation({
    mutationFn: (data: any) => 
      apiRequest('PATCH', `/api/master-data/locations/${location.id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/master-data/hierarchy'] });
      toast({ title: 'Location updated successfully' });
      onSaved();
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to update location', 
        description: error.message, 
        variant: 'destructive' 
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.locName.trim()) return;
    updateMutation.mutate(formData);
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-2 p-3 bg-accent/10 rounded-lg" onClick={(e) => e.stopPropagation()}>
      <div className="grid grid-cols-2 gap-2">
        <div className="col-span-2">
          <Input
            placeholder="Location name *"
            value={formData.locName}
            onChange={(e) => setFormData({...formData, locName: e.target.value})}
            className="text-sm"
            autoFocus
            data-testid="input-edit-location-name"
          />
        </div>
        <div className="col-span-2">
          <Textarea
            placeholder="Description"
            value={formData.locDescr}
            onChange={(e) => setFormData({...formData, locDescr: e.target.value})}
            rows={2}
            className="text-sm"
            data-testid="input-edit-location-descr"
          />
        </div>
        <div className="col-span-2">
          <Input
            placeholder="Address"
            value={formData.address1}
            onChange={(e) => setFormData({...formData, address1: e.target.value})}
            className="text-sm"
            data-testid="input-edit-location-address"
          />
        </div>
        <Input
          placeholder="Contact Person"
          value={formData.contactPerson}
          onChange={(e) => setFormData({...formData, contactPerson: e.target.value})}
          className="text-sm"
          data-testid="input-edit-location-contact-person"
        />
        <Input
          placeholder="Contact Email"
          value={formData.contactEmail}
          onChange={(e) => setFormData({...formData, contactEmail: e.target.value})}
          className="text-sm"
          data-testid="input-edit-location-contact-email"
        />
        <Input
          placeholder="Contact Phone"
          value={formData.contactPhone}
          onChange={(e) => setFormData({...formData, contactPhone: e.target.value})}
          className="text-sm"
          data-testid="input-edit-location-contact-phone"
        />
        <Select 
          value={formData.contactPreference} 
          onValueChange={(val) => setFormData({...formData, contactPreference: val})}
        >
          <SelectTrigger className="text-sm" data-testid="select-edit-location-contact-preference">
            <SelectValue placeholder="Contact Preference" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="email">Email</SelectItem>
            <SelectItem value="phone">Phone</SelectItem>
            <SelectItem value="both">Both</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex gap-2">
        <Button type="submit" size="sm" disabled={!formData.locName.trim() || updateMutation.isPending} data-testid="button-update-location">
          <Check className="h-3 w-3 mr-1" />
          Save
        </Button>
        <Button type="button" variant="ghost" size="sm" onClick={onCancel} data-testid="button-cancel-edit-location">
          <X className="h-3 w-3 mr-1" />
          Cancel
        </Button>
      </div>
    </form>
  );
}
