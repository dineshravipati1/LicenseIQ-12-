import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useRoles, useActiveRoles } from "@/hooks/use-roles";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Shield, RefreshCw, Check, X, Settings, Users, Database, Plus, Save, Trash2, Edit2, XCircle, UserCog } from "lucide-react";

type NavItem = {
  id: string;
  itemKey: string;
  itemName: string;
  href: string;
  iconName?: string;
  defaultRoles: string[];
  isActive: boolean;
  sortOrder: number;
};

type RolePermission = {
  id: string;
  role: string;
  navItemKey: string;
  isEnabled: boolean;
};

type Role = {
  id: string;
  roleName: string;
  displayName: string;
  description: string | null;
  isSystemRole: boolean;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
};

export default function Configuration() {
  const { toast } = useToast();
  const [, navigate] = useLocation();
  
  // Fetch active roles from database
  const { data: activeRolesList, isLoading: activeRolesLoading } = useActiveRoles();
  const availableRoles = activeRolesList || [];
  
  const [selectedRole, setSelectedRole] = useState<string>('');
  
  // Role management state
  const [editingRoleId, setEditingRoleId] = useState<string | null>(null);
  const [newRole, setNewRole] = useState({ roleName: '', displayName: '', description: '' });
  const [isAddingRole, setIsAddingRole] = useState(false);

  // Fetch navigation items
  const { data: navData, isLoading: navLoading } = useQuery<{ items: NavItem[] }>({
    queryKey: ['/api/config/navigation'],
  });

  // Fetch role permissions
  const { data: permData, isLoading: permLoading } = useQuery<{ permissions: RolePermission[] }>({
    queryKey: ['/api/config/navigation/roles'],
  });

  // Fetch roles
  const { data: rolesData, isLoading: rolesLoading } = useQuery<{ roles: Role[] }>({
    queryKey: ['/api/roles'],
  });

  // Seed navigation items mutation
  const seedNavMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/config/navigation/seed', {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/config/navigation'] });
      toast({
        title: "Success",
        description: "Navigation items initialized successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to initialize navigation items",
        variant: "destructive",
      });
    },
  });

  // Seed sample data mutation
  const seedDataMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/licenseiq-sample-data/seed', {});
    },
    onSuccess: (data: any) => {
      toast({
        title: "Success",
        description: `Created ${data.created} sample records across all entities`,
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to seed sample data",
        variant: "destructive",
      });
    },
  });

  // Update role permission mutation
  const updatePermMutation = useMutation({
    mutationFn: async ({ role, navItemKey, isEnabled }: { role: string; navItemKey: string; isEnabled: boolean }) => {
      return await apiRequest('POST', '/api/config/navigation/roles', { role, navItemKey, isEnabled });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/config/navigation/roles'] });
      toast({
        title: "Success",
        description: "Permission updated successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update permission",
        variant: "destructive",
      });
    },
  });

  // Create role mutation
  const createRoleMutation = useMutation({
    mutationFn: async (roleData: { roleName: string; displayName: string; description: string }) => {
      return await apiRequest('POST', '/api/roles', roleData);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/roles'] });
      setIsAddingRole(false);
      setNewRole({ roleName: '', displayName: '', description: '' });
      toast({
        title: "Success",
        description: "Role created successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to create role",
        variant: "destructive",
      });
    },
  });

  // Update role mutation
  const updateRoleMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: Partial<Role> }) => {
      return await apiRequest('PUT', `/api/roles/${id}`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/roles'] });
      setEditingRoleId(null);
      toast({
        title: "Success",
        description: "Role updated successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update role",
        variant: "destructive",
      });
    },
  });

  // Delete role mutation
  const deleteRoleMutation = useMutation({
    mutationFn: async (id: string) => {
      return await apiRequest('DELETE', `/api/roles/${id}`, {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/roles'] });
      toast({
        title: "Success",
        description: "Role deleted successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to delete role",
        variant: "destructive",
      });
    },
  });

  // Seed roles mutation
  const seedRolesMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/roles/seed', {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/roles'] });
      toast({
        title: "Success",
        description: "System roles seeded successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to seed roles",
        variant: "destructive",
      });
    },
  });

  const navItems = navData?.items || [];
  const permissions = permData?.permissions || [];
  
  // Set initial selected role when roles are loaded
  if (!selectedRole && availableRoles.length > 0) {
    setSelectedRole(availableRoles[0].roleName);
  }

  const isRoleEnabled = (itemKey: string, role: string): boolean => {
    const perm = permissions.find(p => p.navItemKey === itemKey && p.role === role);
    if (perm) return perm.isEnabled;
    
    // Check default roles
    const item = navItems.find(i => i.itemKey === itemKey);
    return item?.defaultRoles?.includes(role) || false;
  };

  const handleTogglePermission = (itemKey: string, role: string, currentValue: boolean) => {
    updatePermMutation.mutate({ role, navItemKey: itemKey, isEnabled: !currentValue });
  };

  // Update default roles mutation
  const updateDefaultRoleMutation = useMutation({
    mutationFn: async ({ itemKey, role, isDefault }: { itemKey: string; role: string; isDefault: boolean }) => {
      return await apiRequest('PATCH', `/api/config/navigation/${itemKey}/default-roles`, { role, isDefault });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/config/navigation'] });
      toast({
        title: "Success",
        description: "Default access updated successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update default access",
        variant: "destructive",
      });
    },
  });

  const handleToggleDefaultRole = (itemKey: string, role: string, currentValue: boolean) => {
    updateDefaultRoleMutation.mutate({ itemKey, role, isDefault: !currentValue });
  };

  return (
    <MainLayout
      title="Configuration"
      description="Manage roles, navigation permissions, sample data, and system settings"
    >
      <div className="space-y-6">

      <Tabs defaultValue="navigation" className="space-y-6">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="navigation" data-testid="tab-navigation">
            <Shield className="h-4 w-4 mr-2" />
            Navigation Permissions
          </TabsTrigger>
          <TabsTrigger value="roles" data-testid="tab-roles">
            <UserCog className="h-4 w-4 mr-2" />
            Role Management
          </TabsTrigger>
          <TabsTrigger value="data" data-testid="tab-data">
            <Database className="h-4 w-4 mr-2" />
            Sample Data
          </TabsTrigger>
          <TabsTrigger value="system" data-testid="tab-system">
            <Settings className="h-4 w-4 mr-2" />
            System Settings
          </TabsTrigger>
        </TabsList>

        {/* Navigation Permissions Tab */}
        <TabsContent value="navigation" className="space-y-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <Shield className="h-5 w-5" />
                    Role-Based Navigation Access
                  </CardTitle>
                  <CardDescription className="mt-2">
                    Control which menu items are visible to each user role
                  </CardDescription>
                </div>
                <Button
                  onClick={() => seedNavMutation.mutate()}
                  disabled={seedNavMutation.isPending}
                  className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                  data-testid="button-init-nav"
                >
                  <RefreshCw className={`h-4 w-4 mr-2 ${seedNavMutation.isPending ? 'animate-spin' : ''}`} />
                  {seedNavMutation.isPending ? 'Initializing...' : 'Initialize Navigation'}
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                {/* Role Selector */}
                {activeRolesLoading ? (
                  <div className="flex items-center gap-2 text-muted-foreground">
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    Loading roles...
                  </div>
                ) : availableRoles.length === 0 ? (
                  <div className="text-muted-foreground text-sm">
                    No roles found. Please create roles in the Role Management tab first.
                  </div>
                ) : (
                  <div className="flex gap-2 flex-wrap">
                    {availableRoles.map(role => (
                      <Button
                        key={role.roleName}
                        variant={selectedRole === role.roleName ? "default" : "outline"}
                        onClick={() => setSelectedRole(role.roleName)}
                        className={selectedRole === role.roleName ? "bg-gradient-to-r from-purple-600 to-pink-600" : ""}
                        data-testid={`button-role-${role.roleName}`}
                      >
                        <Users className="h-4 w-4 mr-2" />
                        {role.displayName}
                      </Button>
                    ))}
                  </div>
                )}

                {/* Permissions Table */}
                {navLoading || permLoading ? (
                  <div className="flex items-center justify-center py-12">
                    <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
                  </div>
                ) : navItems.length === 0 ? (
                  <div className="text-center py-12">
                    <Shield className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No navigation items found</p>
                    <p className="text-sm text-muted-foreground mt-2">Click "Initialize Navigation" to set up menu items</p>
                  </div>
                ) : (
                  <Card>
                    <CardHeader>
                      <CardTitle className="text-lg">
                        Permissions for: <Badge variant="outline" className="ml-2">{selectedRole}</Badge>
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <ScrollArea className="h-[500px]">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Menu Item</TableHead>
                              <TableHead>Route</TableHead>
                              <TableHead>Default Access</TableHead>
                              <TableHead className="text-center">Enabled</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {navItems.map(item => {
                              const isEnabled = isRoleEnabled(item.itemKey, selectedRole);
                              const hasDefault = item.defaultRoles?.includes(selectedRole);
                              
                              return (
                                <TableRow key={item.id} data-testid={`row-nav-${item.itemKey}`}>
                                  <TableCell className="font-medium">{item.itemName}</TableCell>
                                  <TableCell className="text-muted-foreground font-mono text-sm">{item.href}</TableCell>
                                  <TableCell className="text-center">
                                    <Switch
                                      checked={hasDefault}
                                      onCheckedChange={() => handleToggleDefaultRole(item.itemKey, selectedRole, hasDefault)}
                                      disabled={updateDefaultRoleMutation.isPending}
                                      data-testid={`switch-default-${item.itemKey}-${selectedRole}`}
                                    />
                                  </TableCell>
                                  <TableCell className="text-center">
                                    <Switch
                                      checked={isEnabled}
                                      onCheckedChange={() => handleTogglePermission(item.itemKey, selectedRole, isEnabled)}
                                      disabled={updatePermMutation.isPending}
                                      data-testid={`switch-${item.itemKey}-${selectedRole}`}
                                    />
                                  </TableCell>
                                </TableRow>
                              );
                            })}
                          </TableBody>
                        </Table>
                      </ScrollArea>
                    </CardContent>
                  </Card>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Role Management Tab */}
        <TabsContent value="roles" className="space-y-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <UserCog className="h-5 w-5" />
                    Role Management
                  </CardTitle>
                  <CardDescription className="mt-2">
                    Create and manage user roles with custom permissions
                  </CardDescription>
                </div>
                <Button
                  onClick={() => seedRolesMutation.mutate()}
                  disabled={seedRolesMutation.isPending}
                  variant="outline"
                  data-testid="button-seed-roles"
                >
                  <RefreshCw className={`h-4 w-4 mr-2 ${seedRolesMutation.isPending ? 'animate-spin' : ''}`} />
                  {seedRolesMutation.isPending ? 'Seeding...' : 'Seed System Roles'}
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {rolesLoading ? (
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : (
                <div className="space-y-4">
                  {/* Add New Role Form */}
                  {!isAddingRole && (
                    <Button
                      onClick={() => setIsAddingRole(true)}
                      className="bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-700 hover:to-pink-700"
                      data-testid="button-add-role"
                    >
                      <Plus className="h-4 w-4 mr-2" />
                      Add New Role
                    </Button>
                  )}

                  {isAddingRole && (
                    <Card className="border-2 border-purple-500/50 bg-purple-50 dark:bg-purple-950/20">
                      <CardContent className="pt-6">
                        <div className="grid gap-4">
                          <div>
                            <label className="text-sm font-medium">Role Name (Identifier)</label>
                            <Input
                              value={newRole.roleName}
                              onChange={(e) => setNewRole({ ...newRole, roleName: e.target.value })}
                              placeholder="e.g., contract_analyst"
                              className="mt-1"
                              data-testid="input-new-role-name"
                            />
                          </div>
                          <div>
                            <label className="text-sm font-medium">Display Name</label>
                            <Input
                              value={newRole.displayName}
                              onChange={(e) => setNewRole({ ...newRole, displayName: e.target.value })}
                              placeholder="e.g., Contract Analyst"
                              className="mt-1"
                              data-testid="input-new-display-name"
                            />
                          </div>
                          <div>
                            <label className="text-sm font-medium">Description</label>
                            <Textarea
                              value={newRole.description}
                              onChange={(e) => setNewRole({ ...newRole, description: e.target.value })}
                              placeholder="Brief description of this role"
                              className="mt-1"
                              data-testid="textarea-new-description"
                            />
                          </div>
                          <div className="flex gap-2">
                            <Button
                              onClick={() => createRoleMutation.mutate(newRole)}
                              disabled={!newRole.roleName || !newRole.displayName || createRoleMutation.isPending}
                              className="bg-green-600 hover:bg-green-700"
                              data-testid="button-save-new-role"
                            >
                              <Save className="h-4 w-4 mr-2" />
                              Save Role
                            </Button>
                            <Button
                              onClick={() => {
                                setIsAddingRole(false);
                                setNewRole({ roleName: '', displayName: '', description: '' });
                              }}
                              variant="outline"
                              data-testid="button-cancel-new-role"
                            >
                              <XCircle className="h-4 w-4 mr-2" />
                              Cancel
                            </Button>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Roles Table */}
                  <ScrollArea className="h-[500px]">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Role Name</TableHead>
                          <TableHead>Display Name</TableHead>
                          <TableHead>Description</TableHead>
                          <TableHead className="text-center">Type</TableHead>
                          <TableHead className="text-center">Status</TableHead>
                          <TableHead className="text-center">Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {rolesData?.roles.map((role) => (
                          <TableRow key={role.id} data-testid={`row-role-${role.roleName}`}>
                            <TableCell className="font-mono text-sm">{role.roleName}</TableCell>
                            <TableCell>
                              {editingRoleId === role.id ? (
                                <Input
                                  defaultValue={role.displayName}
                                  onBlur={(e) => {
                                    if (e.target.value !== role.displayName) {
                                      updateRoleMutation.mutate({
                                        id: role.id,
                                        data: { displayName: e.target.value }
                                      });
                                    } else {
                                      setEditingRoleId(null);
                                    }
                                  }}
                                  autoFocus
                                  data-testid={`input-edit-display-${role.id}`}
                                />
                              ) : (
                                <span>{role.displayName}</span>
                              )}
                            </TableCell>
                            <TableCell>
                              {editingRoleId === role.id ? (
                                <Textarea
                                  defaultValue={role.description || ''}
                                  onBlur={(e) => {
                                    if (e.target.value !== role.description) {
                                      updateRoleMutation.mutate({
                                        id: role.id,
                                        data: { description: e.target.value }
                                      });
                                    } else {
                                      setEditingRoleId(null);
                                    }
                                  }}
                                  className="min-h-[60px]"
                                  data-testid={`textarea-edit-desc-${role.id}`}
                                />
                              ) : (
                                <span className="text-sm text-muted-foreground">
                                  {role.description || 'No description'}
                                </span>
                              )}
                            </TableCell>
                            <TableCell className="text-center">
                              {role.isSystemRole ? (
                                <Badge variant="secondary" className="bg-blue-100 dark:bg-blue-900">
                                  System
                                </Badge>
                              ) : (
                                <Badge variant="outline">Custom</Badge>
                              )}
                            </TableCell>
                            <TableCell className="text-center">
                              {role.isActive ? (
                                <Badge variant="secondary" className="bg-green-100 dark:bg-green-900">
                                  <Check className="h-3 w-3 mr-1" />
                                  Active
                                </Badge>
                              ) : (
                                <Badge variant="secondary" className="bg-gray-100 dark:bg-gray-800">
                                  <X className="h-3 w-3 mr-1" />
                                  Inactive
                                </Badge>
                              )}
                            </TableCell>
                            <TableCell className="text-center">
                              <div className="flex items-center justify-center gap-2">
                                {editingRoleId !== role.id && (
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => setEditingRoleId(role.id)}
                                    data-testid={`button-edit-${role.id}`}
                                  >
                                    <Edit2 className="h-4 w-4" />
                                  </Button>
                                )}
                                {!role.isSystemRole && (
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => {
                                      if (confirm(`Are you sure you want to delete the role "${role.displayName}"?`)) {
                                        deleteRoleMutation.mutate(role.id);
                                      }
                                    }}
                                    disabled={deleteRoleMutation.isPending}
                                    className="text-red-600 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950"
                                    data-testid={`button-delete-${role.id}`}
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                )}
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </ScrollArea>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Sample Data Tab */}
        <TabsContent value="data" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5" />
                Sample Data Management
              </CardTitle>
              <CardDescription className="mt-2">
                Seed sample records for testing and demonstration purposes
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="bg-muted/50 p-6 rounded-lg">
                <h3 className="font-semibold mb-4">LicenseIQ Entity Sample Data</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  This will create 3 sample records for key entities including Customers, Items, Sales Orders, and AR Invoices.
                  Perfect for testing Data Management CRUD operations.
                </p>
                <Button
                  onClick={() => seedDataMutation.mutate()}
                  disabled={seedDataMutation.isPending}
                  className="bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-700 hover:to-teal-700"
                  data-testid="button-seed-sample-data"
                >
                  <Database className={`h-4 w-4 mr-2 ${seedDataMutation.isPending ? 'animate-pulse' : ''}`} />
                  {seedDataMutation.isPending ? 'Seeding...' : 'Seed Sample Data'}
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* System Settings Tab */}
        <TabsContent value="system" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Settings className="h-5 w-5" />
                System Settings
              </CardTitle>
              <CardDescription className="mt-2">
                General system configuration and preferences
              </CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">Additional system settings coming soon...</p>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
      </div>
    </MainLayout>
  );
}
