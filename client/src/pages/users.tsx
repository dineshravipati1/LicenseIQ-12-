import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { isUnauthorizedError } from "@/lib/authUtils";
import { 
  Users as UsersIcon, 
  Plus, 
  Search, 
  Edit, 
  Key, 
  Trash2,
  Send,
  ChevronDown,
  ChevronUp,
  X,
  Check,
  Building2,
  MapPin,
  UserPlus
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";

export default function Users() {
  const { user: currentUser } = useAuth();
  const queryClient = useQueryClient();
  const { toast } = useToast();

  // Search and filter states
  const [searchQuery, setSearchQuery] = useState("");
  const [roleFilter, setRoleFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");

  // Inline editing state
  const [expandedUserId, setExpandedUserId] = useState<string | null>(null);
  const [editingSection, setEditingSection] = useState<"profile" | "password" | "delete" | "organizations" | "quickassign" | null>(null);

  // Edit form states
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [email, setEmail] = useState("");
  
  // Password reset states
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");

  // Delete confirmation state
  const [deleteConfirmText, setDeleteConfirmText] = useState("");

  // Organization assignment states (for Organizations tab only)
  const [selectedCompanyId, setSelectedCompanyId] = useState("");
  const [selectedBusinessUnitId, setSelectedBusinessUnitId] = useState("");
  const [selectedLocationId, setSelectedLocationId] = useState("");
  const [selectedOrgRole, setSelectedOrgRole] = useState("viewer");
  const [showAddOrgForm, setShowAddOrgForm] = useState(false);
  
  // Quick Assign dedicated state (separate from Organizations tab)
  const [quickAssignCompanyId, setQuickAssignCompanyId] = useState("");
  const [quickAssignBusinessUnitId, setQuickAssignBusinessUnitId] = useState("");
  const [quickAssignLocationId, setQuickAssignLocationId] = useState("");
  const [quickAssignRole, setQuickAssignRole] = useState("viewer");

  // Handle Escape key to close editing panel
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && expandedUserId) {
        closeEditing();
      }
    };

    if (expandedUserId) {
      document.addEventListener("keydown", handleKeyDown);
      return () => {
        document.removeEventListener("keydown", handleKeyDown);
      };
    }
  }, [expandedUserId]);

  // Fetch users
  const { data: users = [] as any[], isLoading } = useQuery({
    queryKey: ["/api/users"],
    retry: (failureCount, error) => {
      if (isUnauthorizedError(error)) {
        return false;
      }
      return failureCount < 3;
    },
  });

  // Fetch companies for Organizations tab
  const { data: companies = [] } = useQuery({
    queryKey: ["/api/master-data/companies"],
    enabled: editingSection === "organizations",
  });

  // Fetch business units for Organizations tab
  const { data: businessUnits = [] } = useQuery({
    queryKey: ["/api/master-data/business-units", { companyId: selectedCompanyId }],
    queryFn: async () => {
      const response = await fetch(`/api/master-data/business-units?companyId=${selectedCompanyId}`);
      if (!response.ok) throw new Error('Failed to fetch business units');
      return response.json();
    },
    enabled: !!selectedCompanyId && editingSection === "organizations",
  });

  // Fetch locations for Organizations tab
  const { data: locations = [] } = useQuery({
    queryKey: ["/api/master-data/locations", { orgId: selectedBusinessUnitId }],
    queryFn: async () => {
      const response = await fetch(`/api/master-data/locations?orgId=${selectedBusinessUnitId}`);
      if (!response.ok) throw new Error('Failed to fetch locations');
      return response.json();
    },
    enabled: !!selectedBusinessUnitId && editingSection === "organizations",
  });
  
  // Quick Assign dedicated queries
  const { data: quickAssignCompanies = [] } = useQuery({
    queryKey: ["/api/master-data/companies"],
    enabled: editingSection === "quickassign",
  });

  const { data: quickAssignBusinessUnits = [] } = useQuery({
    queryKey: ["/api/master-data/business-units", { companyId: quickAssignCompanyId }],
    queryFn: async () => {
      const response = await fetch(`/api/master-data/business-units?companyId=${quickAssignCompanyId}`);
      if (!response.ok) throw new Error('Failed to fetch business units');
      return response.json();
    },
    enabled: !!quickAssignCompanyId && editingSection === "quickassign",
  });

  const { data: quickAssignLocations = [] } = useQuery({
    queryKey: ["/api/master-data/locations", { orgId: quickAssignBusinessUnitId }],
    queryFn: async () => {
      const response = await fetch(`/api/master-data/locations?orgId=${quickAssignBusinessUnitId}`);
      if (!response.ok) throw new Error('Failed to fetch locations');
      return response.json();
    },
    enabled: !!quickAssignBusinessUnitId && editingSection === "quickassign",
  });

  // Fetch user organization roles
  const { data: userOrgRoles = [], refetch: refetchOrgRoles } = useQuery({
    queryKey: ["/api/user-organization-roles/user", expandedUserId],
    enabled: !!expandedUserId && editingSection === "organizations",
  });

  // Role update mutation
  const updateUserRoleMutation = useMutation({
    mutationFn: async ({ userId, newRole }: { userId: string; newRole: string }) => {
      return apiRequest("PATCH", `/api/users/${userId}/role`, { role: newRole });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/users"] });
      toast({
        title: "Role Updated",
        description: "User role has been updated successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update user role",
        variant: "destructive",
      });
    },
  });

  // Edit user mutation
  const editUserMutation = useMutation({
    mutationFn: async ({ userId, data }: { userId: string; data: any }) => {
      return apiRequest("PATCH", `/api/users/${userId}`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/users"] });
      toast({
        title: "User Updated",
        description: "User information has been updated successfully",
      });
      closeEditing();
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update user",
        variant: "destructive",
      });
    },
  });

  // Reset password mutation
  const resetPasswordMutation = useMutation({
    mutationFn: async ({ userId, password }: { userId: string; password: string }) => {
      return apiRequest("POST", `/api/users/${userId}/reset-password`, { newPassword: password });
    },
    onSuccess: () => {
      toast({
        title: "Password Reset",
        description: "User password has been reset successfully",
      });
      closeEditing();
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to reset password",
        variant: "destructive",
      });
    },
  });

  // Delete user mutation
  const deleteUserMutation = useMutation({
    mutationFn: async (userId: string) => {
      return apiRequest("DELETE", `/api/users/${userId}`);
    },
    onSuccess: (_, userId) => {
      queryClient.invalidateQueries({ queryKey: ["/api/users"] });
      const deletedUser = users.find((u: any) => u.id === userId);
      toast({
        title: "User Deleted",
        description: `${deletedUser?.email || 'User'} has been removed from the system`,
        variant: "destructive",
      });
      closeEditing();
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to delete user",
        variant: "destructive",
      });
    },
  });

  // Create user organization role mutation
  const createOrgRoleMutation = useMutation({
    mutationFn: async (data: any) => {
      return apiRequest("POST", "/api/user-organization-roles", data);
    },
    onSuccess: (_, variables) => {
      // Invalidate all user organization role queries for the affected user
      queryClient.invalidateQueries({ 
        queryKey: ["/api/user-organization-roles/user", variables.userId] 
      });
      // Also invalidate the users list to show updated data
      queryClient.invalidateQueries({ queryKey: ["/api/users"] });
      
      toast({
        title: "Organization Assignment Created",
        description: "User has been assigned to the organization successfully",
      });
      
      // Reset form fields based on which tab we're in
      if (editingSection === "quickassign") {
        // Reset Quick Assign state only - keep user in Quick Assign for multiple entries
        setQuickAssignCompanyId("");
        setQuickAssignBusinessUnitId("");
        setQuickAssignLocationId("");
        setQuickAssignRole("viewer");
      } else if (editingSection === "organizations") {
        // Reset Organizations tab state only
        setShowAddOrgForm(false);
        setSelectedCompanyId("");
        setSelectedBusinessUnitId("");
        setSelectedLocationId("");
        setSelectedOrgRole("viewer");
      }
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to create organization assignment",
        variant: "destructive",
      });
    },
  });

  // Delete user organization role mutation
  const deleteOrgRoleMutation = useMutation({
    mutationFn: async (roleId: string) => {
      return apiRequest("DELETE", `/api/user-organization-roles/${roleId}`);
    },
    onSuccess: () => {
      // Invalidate organization roles for the currently expanded user
      if (expandedUserId) {
        queryClient.invalidateQueries({ 
          queryKey: ["/api/user-organization-roles/user", expandedUserId] 
        });
      }
      // Also invalidate users list
      queryClient.invalidateQueries({ queryKey: ["/api/users"] });
      
      toast({
        title: "Assignment Removed",
        description: "User organization assignment has been removed",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to remove organization assignment",
        variant: "destructive",
      });
    },
  });

  // Filter users based on search and filters
  const filteredUsers = users.filter((user: any) => {
    const matchesSearch = !searchQuery || 
      user.email?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      user.firstName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      user.lastName?.toLowerCase().includes(searchQuery.toLowerCase());
    
    const matchesRole = roleFilter === "all" || user.role === roleFilter;
    const matchesStatus = statusFilter === "all" || 
      (statusFilter === "active" && user.isActive) ||
      (statusFilter === "inactive" && !user.isActive);
    
    return matchesSearch && matchesRole && matchesStatus;
  });

  // Inline editing handlers
  const openEditing = (userId: string, section: "profile" | "password" | "delete" | "organizations" | "quickassign") => {
    const user = users.find((u: any) => u.id === userId);
    if (!user) return;

    setExpandedUserId(userId);
    setEditingSection(section);

    // Pre-fill form data for profile editing
    if (section === "profile") {
      setFirstName(user.firstName || "");
      setLastName(user.lastName || "");
      setEmail(user.email || "");
    }

    // Clear password fields
    if (section === "password") {
      setNewPassword("");
      setConfirmPassword("");
    }

    // Clear delete confirmation
    if (section === "delete") {
      setDeleteConfirmText("");
    }

    // Reset organization form (Organizations tab)
    if (section === "organizations") {
      setShowAddOrgForm(false);
      setSelectedCompanyId("");
      setSelectedBusinessUnitId("");
      setSelectedLocationId("");
      setSelectedOrgRole("viewer");
    }
    
    // Reset Quick Assign form (separate state)
    if (section === "quickassign") {
      setQuickAssignCompanyId("");
      setQuickAssignBusinessUnitId("");
      setQuickAssignLocationId("");
      setQuickAssignRole("viewer");
    }
  };

  const closeEditing = () => {
    setExpandedUserId(null);
    setEditingSection(null);
    setFirstName("");
    setLastName("");
    setEmail("");
    setNewPassword("");
    setConfirmPassword("");
    setDeleteConfirmText("");
    setShowAddOrgForm(false);
    setSelectedCompanyId("");
    setSelectedBusinessUnitId("");
    setSelectedLocationId("");
    setSelectedOrgRole("viewer");
    setQuickAssignCompanyId("");
    setQuickAssignBusinessUnitId("");
    setQuickAssignLocationId("");
    setQuickAssignRole("viewer");
  };

  const handleSaveProfile = async () => {
    if (!expandedUserId) return;
    
    editUserMutation.mutate({
      userId: expandedUserId,
      data: { firstName, lastName, email }
    });
  };

  const handleResetPassword = async () => {
    if (!expandedUserId) return;

    if (newPassword.length < 6) {
      toast({
        title: "Invalid Password",
        description: "Password must be at least 6 characters long",
        variant: "destructive",
      });
      return;
    }

    if (newPassword !== confirmPassword) {
      toast({
        title: "Password Mismatch",
        description: "Passwords do not match",
        variant: "destructive",
      });
      return;
    }

    resetPasswordMutation.mutate({
      userId: expandedUserId,
      password: newPassword
    });
  };

  const handleDeleteUser = async () => {
    if (!expandedUserId) return;

    const user = users.find((u: any) => u.id === expandedUserId);
    if (!user) return;

    if (deleteConfirmText !== user.email) {
      toast({
        title: "Confirmation Required",
        description: "Please type the user's email address to confirm deletion",
        variant: "destructive",
      });
      return;
    }

    if (user.id === currentUser?.id) {
      toast({
        title: "Cannot Delete",
        description: "You cannot delete your own account",
        variant: "destructive",
      });
      return;
    }

    deleteUserMutation.mutate(expandedUserId);
  };

  const handleAddOrgAssignment = async () => {
    // Use different state depending on whether we're in Quick Assign or Organizations tab
    const isQuickAssign = editingSection === "quickassign";
    const companyId = isQuickAssign ? quickAssignCompanyId : selectedCompanyId;
    const businessUnitId = isQuickAssign ? quickAssignBusinessUnitId : selectedBusinessUnitId;
    const locationId = isQuickAssign ? quickAssignLocationId : selectedLocationId;
    const role = isQuickAssign ? quickAssignRole : selectedOrgRole;
    
    if (!expandedUserId || !companyId) {
      toast({
        title: "Validation Error",
        description: "Please select at least a company",
        variant: "destructive",
      });
      return;
    }

    createOrgRoleMutation.mutate({
      userId: expandedUserId,
      companyId: companyId,
      businessUnitId: businessUnitId || null,
      locationId: locationId || null,
      role: role,
    });
  };

  // Render inline editing panel
  const renderInlineEditingPanel = (user: any) => {
    if (expandedUserId !== user.id) return null;

    return (
      <tr>
        <td colSpan={5} className="px-6 py-4 bg-muted/20 border-t">
          <div className="space-y-6">
            {/* Section Tabs */}
            <div className="flex space-x-4 border-b border-border">
              <button
                className={`pb-2 px-1 text-sm font-medium border-b-2 ${
                  editingSection === "profile" 
                    ? "border-primary text-primary" 
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => openEditing(user.id, "profile")}
                data-testid={`tab-profile-${user.id}`}
              >
                Profile
              </button>
              <button
                className={`pb-2 px-1 text-sm font-medium border-b-2 ${
                  editingSection === "quickassign" 
                    ? "border-blue-600 text-blue-600" 
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => openEditing(user.id, "quickassign")}
                data-testid={`tab-quick-assign-${user.id}`}
              >
                Quick Assign
              </button>
              <button
                className={`pb-2 px-1 text-sm font-medium border-b-2 ${
                  editingSection === "organizations" 
                    ? "border-primary text-primary" 
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => openEditing(user.id, "organizations")}
                data-testid={`tab-organizations-${user.id}`}
              >
                Organizations
              </button>
              <button
                className={`pb-2 px-1 text-sm font-medium border-b-2 ${
                  editingSection === "password" 
                    ? "border-primary text-primary" 
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => openEditing(user.id, "password")}
                data-testid={`tab-password-${user.id}`}
              >
                Reset Password
              </button>
              <button
                className={`pb-2 px-1 text-sm font-medium border-b-2 ${
                  editingSection === "delete" 
                    ? "border-destructive text-destructive" 
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => openEditing(user.id, "delete")}
                data-testid={`tab-delete-${user.id}`}
              >
                Delete User
              </button>
            </div>

            {/* Profile Section */}
            {editingSection === "profile" && (
              <div className="space-y-4 max-w-md">
                <h3 className="text-lg font-medium">Edit Profile</h3>
                <div className="space-y-3">
                  <div>
                    <Label htmlFor={`first-name-${user.id}`}>First Name</Label>
                    <Input
                      id={`first-name-${user.id}`}
                      value={firstName}
                      onChange={(e) => setFirstName(e.target.value)}
                      placeholder="First name"
                      data-testid={`input-first-name-${user.id}`}
                    />
                  </div>
                  <div>
                    <Label htmlFor={`last-name-${user.id}`}>Last Name</Label>
                    <Input
                      id={`last-name-${user.id}`}
                      value={lastName}
                      onChange={(e) => setLastName(e.target.value)}
                      placeholder="Last name"
                      data-testid={`input-last-name-${user.id}`}
                    />
                  </div>
                  <div>
                    <Label htmlFor={`email-${user.id}`}>Email</Label>
                    <Input
                      id={`email-${user.id}`}
                      type="email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      placeholder="Email address"
                      data-testid={`input-email-${user.id}`}
                    />
                  </div>
                </div>
                <div className="flex space-x-2">
                  <Button 
                    onClick={handleSaveProfile}
                    disabled={editUserMutation.isPending}
                    data-testid={`button-save-profile-${user.id}`}
                  >
                    <Check className="h-4 w-4 mr-2" />
                    {editUserMutation.isPending ? "Saving..." : "Save Changes"}
                  </Button>
                  <Button 
                    variant="outline" 
                    onClick={closeEditing}
                    data-testid={`button-cancel-profile-${user.id}`}
                  >
                    <X className="h-4 w-4 mr-2" />
                    Cancel
                  </Button>
                </div>
              </div>
            )}

            {/* Quick Assign Section */}
            {editingSection === "quickassign" && (
              <div className="space-y-4 max-w-2xl">
                <div className="flex items-center justify-between">
                  <h3 className="text-lg font-medium text-blue-600">Quick Assign to Organization</h3>
                  <span className="text-sm text-muted-foreground">Assign {user.firstName} {user.lastName} to an organization</span>
                </div>
                
                <div className="grid grid-cols-4 gap-4">
                  {/* Company Dropdown */}
                  <div className="space-y-2">
                    <Label htmlFor={`quick-company-${user.id}`}>Company *</Label>
                    <select
                      id={`quick-company-${user.id}`}
                      value={quickAssignCompanyId}
                      onChange={(e) => {
                        setQuickAssignCompanyId(e.target.value);
                        setQuickAssignBusinessUnitId("");
                        setQuickAssignLocationId("");
                      }}
                      className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm"
                      data-testid={`select-quick-company-${user.id}`}
                    >
                      <option value="">Select...</option>
                      {quickAssignCompanies.map((company: any) => (
                        <option key={company.id} value={company.id}>
                          {company.companyName}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Business Unit Dropdown */}
                  <div className="space-y-2">
                    <Label htmlFor={`quick-business-unit-${user.id}`}>Business Unit</Label>
                    <select
                      id={`quick-business-unit-${user.id}`}
                      value={quickAssignBusinessUnitId}
                      onChange={(e) => {
                        setQuickAssignBusinessUnitId(e.target.value);
                        setQuickAssignLocationId("");
                      }}
                      disabled={!quickAssignCompanyId}
                      className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm disabled:opacity-50"
                      data-testid={`select-quick-business-unit-${user.id}`}
                    >
                      <option value="">Select...</option>
                      {quickAssignBusinessUnits.map((bu: any) => (
                        <option key={bu.id} value={bu.id}>
                          {bu.orgName}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Location Dropdown */}
                  <div className="space-y-2">
                    <Label htmlFor={`quick-location-${user.id}`}>Location</Label>
                    <select
                      id={`quick-location-${user.id}`}
                      value={quickAssignLocationId}
                      onChange={(e) => setQuickAssignLocationId(e.target.value)}
                      disabled={!quickAssignBusinessUnitId}
                      className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm disabled:opacity-50"
                      data-testid={`select-quick-location-${user.id}`}
                    >
                      <option value="">Select...</option>
                      {quickAssignLocations.map((loc: any) => (
                        <option key={loc.id} value={loc.id}>
                          {loc.locName}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Role Dropdown */}
                  <div className="space-y-2">
                    <Label htmlFor={`quick-role-${user.id}`}>Role *</Label>
                    <select
                      id={`quick-role-${user.id}`}
                      value={quickAssignRole}
                      onChange={(e) => setQuickAssignRole(e.target.value)}
                      className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm"
                      data-testid={`select-quick-role-${user.id}`}
                    >
                      <option value="viewer">Viewer</option>
                      <option value="editor">Editor</option>
                      <option value="admin">Admin</option>
                      <option value="owner">Owner</option>
                      <option value="auditor">Auditor</option>
                    </select>
                  </div>
                </div>

                <div className="flex space-x-2">
                  <Button 
                    onClick={handleAddOrgAssignment}
                    disabled={createOrgRoleMutation.isPending || !quickAssignCompanyId}
                    className="bg-blue-600 hover:bg-blue-700"
                    data-testid={`button-quick-assign-submit-${user.id}`}
                  >
                    <Check className="h-4 w-4 mr-2" />
                    {createOrgRoleMutation.isPending ? "Assigning..." : "Assign to Organization"}
                  </Button>
                  <Button 
                    variant="outline" 
                    onClick={closeEditing}
                    data-testid={`button-cancel-quick-assign-${user.id}`}
                  >
                    <X className="h-4 w-4 mr-2" />
                    Cancel
                  </Button>
                </div>
              </div>
            )}

            {/* Password Section */}
            {editingSection === "password" && (
              <div className="space-y-4 max-w-md">
                <h3 className="text-lg font-medium">Reset Password</h3>
                <div className="text-sm text-muted-foreground mb-3">
                  Resetting password for: <strong>{user.email}</strong>
                </div>
                <div className="space-y-3">
                  <div>
                    <Label htmlFor={`new-password-${user.id}`}>New Password</Label>
                    <Input
                      id={`new-password-${user.id}`}
                      type="password"
                      value={newPassword}
                      onChange={(e) => setNewPassword(e.target.value)}
                      placeholder="Enter new password (min 6 characters)"
                      data-testid={`input-new-password-${user.id}`}
                    />
                  </div>
                  <div>
                    <Label htmlFor={`confirm-password-${user.id}`}>Confirm Password</Label>
                    <Input
                      id={`confirm-password-${user.id}`}
                      type="password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      placeholder="Confirm new password"
                      data-testid={`input-confirm-password-${user.id}`}
                    />
                  </div>
                </div>
                {newPassword && confirmPassword && newPassword !== confirmPassword && (
                  <p className="text-sm text-destructive">Passwords do not match</p>
                )}
                {newPassword && newPassword.length < 6 && (
                  <p className="text-sm text-destructive">Password must be at least 6 characters long</p>
                )}
                <div className="flex space-x-2">
                  <Button 
                    onClick={handleResetPassword}
                    disabled={resetPasswordMutation.isPending || !newPassword || !confirmPassword || newPassword !== confirmPassword || newPassword.length < 6}
                    data-testid={`button-reset-password-${user.id}`}
                  >
                    <Key className="h-4 w-4 mr-2" />
                    {resetPasswordMutation.isPending ? "Resetting..." : "Reset Password"}
                  </Button>
                  <Button 
                    variant="outline" 
                    onClick={closeEditing}
                    data-testid={`button-cancel-password-${user.id}`}
                  >
                    <X className="h-4 w-4 mr-2" />
                    Cancel
                  </Button>
                </div>
              </div>
            )}

            {/* Delete Section */}
            {editingSection === "delete" && (
              <div className="space-y-4 max-w-md">
                <h3 className="text-lg font-medium text-destructive">Delete User</h3>
                <div className="bg-destructive/10 p-3 rounded-lg border border-destructive/20">
                  <p className="text-sm text-foreground mb-2">
                    <strong>Warning:</strong> This action cannot be undone. The user will be permanently removed from the system.
                  </p>
                  <p className="text-sm text-muted-foreground">
                    User to delete: <strong>{user.email}</strong>
                  </p>
                </div>
                <div>
                  <Label htmlFor={`delete-confirm-${user.id}`}>
                    Type the user's email address to confirm deletion:
                  </Label>
                  <Input
                    id={`delete-confirm-${user.id}`}
                    value={deleteConfirmText}
                    onChange={(e) => setDeleteConfirmText(e.target.value)}
                    placeholder={user.email}
                    data-testid={`input-delete-confirm-${user.id}`}
                  />
                </div>
                <div className="flex space-x-2">
                  <Button 
                    variant="destructive"
                    onClick={handleDeleteUser}
                    disabled={deleteUserMutation.isPending || deleteConfirmText !== user.email}
                    data-testid={`button-confirm-delete-${user.id}`}
                  >
                    <Trash2 className="h-4 w-4 mr-2" />
                    {deleteUserMutation.isPending ? "Deleting..." : "Delete User"}
                  </Button>
                  <Button 
                    variant="outline" 
                    onClick={closeEditing}
                    data-testid={`button-cancel-delete-${user.id}`}
                  >
                    <X className="h-4 w-4 mr-2" />
                    Cancel
                  </Button>
                </div>
              </div>
            )}

            {/* Organizations Section */}
            {editingSection === "organizations" && (
              <div className="space-y-4">
                <div className="flex justify-between items-center">
                  <h3 className="text-lg font-medium">Organization Assignments</h3>
                  <Button
                    size="sm"
                    onClick={() => setShowAddOrgForm(!showAddOrgForm)}
                    data-testid={`button-add-org-${user.id}`}
                  >
                    <Plus className="h-4 w-4 mr-2" />
                    {showAddOrgForm ? "Cancel" : "Add Assignment"}
                  </Button>
                </div>

                {/* Add Organization Form */}
                {showAddOrgForm && (
                  <div className="bg-muted/20 p-4 rounded-lg border border-border space-y-3">
                    <h4 className="text-sm font-medium">New Organization Assignment</h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      <div>
                        <Label htmlFor={`company-${user.id}`}>Company *</Label>
                        <select
                          id={`company-${user.id}`}
                          value={selectedCompanyId}
                          onChange={(e) => {
                            setSelectedCompanyId(e.target.value);
                            setSelectedBusinessUnitId("");
                            setSelectedLocationId("");
                          }}
                          className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm"
                          data-testid={`select-company-${user.id}`}
                        >
                          <option value="">Select a company...</option>
                          {companies.map((company: any) => (
                            <option key={company.id} value={company.id}>
                              {company.companyName}
                            </option>
                          ))}
                        </select>
                      </div>
                      <div>
                        <Label htmlFor={`business-unit-${user.id}`}>Business Unit</Label>
                        <select
                          id={`business-unit-${user.id}`}
                          value={selectedBusinessUnitId}
                          onChange={(e) => {
                            setSelectedBusinessUnitId(e.target.value);
                            setSelectedLocationId("");
                          }}
                          disabled={!selectedCompanyId}
                          className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm disabled:opacity-50"
                          data-testid={`select-business-unit-${user.id}`}
                        >
                          <option value="">Select a business unit...</option>
                          {businessUnits.map((bu: any) => (
                            <option key={bu.id} value={bu.id}>
                              {bu.orgName}
                            </option>
                          ))}
                        </select>
                      </div>
                      <div>
                        <Label htmlFor={`location-${user.id}`}>Location</Label>
                        <select
                          id={`location-${user.id}`}
                          value={selectedLocationId}
                          onChange={(e) => setSelectedLocationId(e.target.value)}
                          disabled={!selectedBusinessUnitId}
                          className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm disabled:opacity-50"
                          data-testid={`select-location-${user.id}`}
                        >
                          <option value="">Select a location...</option>
                          {locations.map((loc: any) => (
                            <option key={loc.id} value={loc.id}>
                              {loc.locName}
                            </option>
                          ))}
                        </select>
                      </div>
                      <div>
                        <Label htmlFor={`role-${user.id}`}>Role *</Label>
                        <select
                          id={`role-${user.id}`}
                          value={selectedOrgRole}
                          onChange={(e) => setSelectedOrgRole(e.target.value)}
                          className="w-full px-3 py-2 bg-background border border-input rounded-md text-sm"
                          data-testid={`select-role-${user.id}`}
                        >
                          <option value="viewer">Viewer</option>
                          <option value="editor">Editor</option>
                          <option value="admin">Admin</option>
                          <option value="owner">Owner</option>
                          <option value="auditor">Auditor</option>
                        </select>
                      </div>
                    </div>
                    <div className="flex space-x-2">
                      <Button
                        size="sm"
                        onClick={handleAddOrgAssignment}
                        disabled={createOrgRoleMutation.isPending || !selectedCompanyId}
                        data-testid={`button-save-org-${user.id}`}
                      >
                        <Check className="h-4 w-4 mr-2" />
                        {createOrgRoleMutation.isPending ? "Adding..." : "Add Assignment"}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => setShowAddOrgForm(false)}
                        data-testid={`button-cancel-org-${user.id}`}
                      >
                        <X className="h-4 w-4 mr-2" />
                        Cancel
                      </Button>
                    </div>
                  </div>
                )}

                {/* Current Assignments List */}
                <div className="space-y-2">
                  {userOrgRoles.length === 0 ? (
                    <p className="text-sm text-muted-foreground py-4 text-center">
                      No organization assignments yet. Click "Add Assignment" to get started.
                    </p>
                  ) : (
                    <div className="space-y-2">
                      {userOrgRoles.map((role: any) => (
                        <div
                          key={role.id}
                          className="flex items-center justify-between p-3 bg-muted/20 rounded-lg border border-border"
                          data-testid={`org-role-${role.id}`}
                        >
                          <div className="flex items-center space-x-3 flex-1">
                            <Building2 className="h-5 w-5 text-blue-600" />
                            <div className="flex-1">
                              <div className="flex items-center space-x-2">
                                <span className="font-medium text-sm">{role.companyName}</span>
                                {role.businessUnitName && (
                                  <>
                                    <span className="text-muted-foreground">/</span>
                                    <span className="text-sm">{role.businessUnitName}</span>
                                  </>
                                )}
                                {role.locationName && (
                                  <>
                                    <span className="text-muted-foreground">/</span>
                                    <MapPin className="h-3 w-3 text-muted-foreground" />
                                    <span className="text-sm">{role.locationName}</span>
                                  </>
                                )}
                              </div>
                              <div className="flex items-center space-x-2 mt-1">
                                <Badge variant="secondary" className="text-xs">
                                  {role.role}
                                </Badge>
                                <Badge 
                                  variant={role.status === "active" ? "default" : "outline"} 
                                  className="text-xs"
                                >
                                  {role.status}
                                </Badge>
                              </div>
                            </div>
                          </div>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => deleteOrgRoleMutation.mutate(role.id)}
                            disabled={deleteOrgRoleMutation.isPending}
                            data-testid={`button-delete-org-${role.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </td>
      </tr>
    );
  };

  return (
    <MainLayout title="User Management">
      <div className="space-y-6">
        {/* Header */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">User Management</h1>
            <p className="text-muted-foreground">
              Manage your team members, roles, and permissions
            </p>
          </div>
          <Button 
            size="lg" 
            className="w-full sm:w-auto"
            data-testid="button-create-user"
            onClick={() => window.location.href = '/users/new'}
          >
            <Plus className="h-5 w-5 mr-2" />
            Create User
          </Button>
        </div>

        {/* Filters */}
        <Card>
          <CardContent className="p-6">
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
                <Input
                  placeholder="Search users..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                  data-testid="input-search-users"
                />
              </div>
              <Select value={roleFilter} onValueChange={setRoleFilter}>
                <SelectTrigger className="w-[180px]" data-testid="select-role-filter">
                  <SelectValue placeholder="All Roles" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Roles</SelectItem>
                  <SelectItem value="owner">Owner</SelectItem>
                  <SelectItem value="admin">Admin</SelectItem>
                  <SelectItem value="editor">Editor</SelectItem>
                  <SelectItem value="viewer">Viewer</SelectItem>
                </SelectContent>
              </Select>
              <Select value={statusFilter} onValueChange={setStatusFilter}>
                <SelectTrigger className="w-[180px]" data-testid="select-status-filter">
                  <SelectValue placeholder="All Status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Status</SelectItem>
                  <SelectItem value="active">Active</SelectItem>
                  <SelectItem value="inactive">Inactive</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>

        {/* Users Table */}
        <Card>
          <CardContent className="p-0">
            {isLoading ? (
              <div className="p-8 text-center">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary mx-auto" />
                <p className="mt-2 text-muted-foreground">Loading users...</p>
              </div>
            ) : filteredUsers.length === 0 ? (
              <div className="text-center py-12">
                <UsersIcon className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-medium mb-2">No users found</h3>
                <p className="text-muted-foreground">
                  {searchQuery || roleFilter !== "all" || statusFilter !== "all"
                    ? "Try adjusting your search criteria or filters"
                    : "Create your first user to get started"
                  }
                </p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-muted/50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        User
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        Role
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        Status
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        Last Active
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">
                        Actions
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-card divide-y divide-border">
                    {filteredUsers.map((user: any) => {
                      const initials = `${user.firstName?.[0] || ''}${user.lastName?.[0] || ''}`.toUpperCase() || user.email?.[0]?.toUpperCase() || 'U';
                      const isExpanded = expandedUserId === user.id;
                      
                      return [
                        <tr key={user.id} className={`hover:bg-muted/30 ${isExpanded ? 'bg-muted/20' : ''}`}>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="flex items-center">
                              <div className="h-10 w-10 bg-primary rounded-full flex items-center justify-center">
                                <span className="text-sm font-medium text-primary-foreground">
                                  {initials}
                                </span>
                              </div>
                              <div className="ml-4">
                                <div className="text-sm font-medium text-foreground">
                                  {user.firstName && user.lastName 
                                    ? `${user.firstName} ${user.lastName}`
                                    : user.email
                                  }
                                </div>
                                <div className="text-xs text-muted-foreground font-mono">
                                  @{user.username}
                                </div>
                                <div className="text-sm text-muted-foreground">
                                  {user.email}
                                </div>
                                {user.primaryCompany && (
                                  <div className="flex items-center gap-1 mt-1">
                                    <Building2 className="h-3 w-3 text-blue-500" />
                                    <span className="text-xs text-blue-600 dark:text-blue-400">
                                      {user.primaryCompany}
                                      {user.companies && user.companies.length > 1 && (
                                        <span className="text-muted-foreground ml-1">
                                          (+{user.companies.length - 1} more)
                                        </span>
                                      )}
                                    </span>
                                  </div>
                                )}
                                {!user.primaryCompany && user.isSystemAdmin && (
                                  <div className="flex items-center gap-1 mt-1">
                                    <Badge variant="outline" className="text-xs">System Admin</Badge>
                                  </div>
                                )}
                              </div>
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <Select
                              value={user.role}
                              onValueChange={(newRole) => 
                                updateUserRoleMutation.mutate({ userId: user.id, newRole })
                              }
                              disabled={updateUserRoleMutation.isPending}
                            >
                              <SelectTrigger className="w-32">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="viewer">Viewer</SelectItem>
                                <SelectItem value="editor">Editor</SelectItem>
                                <SelectItem value="admin">Admin</SelectItem>
                                {user?.role === 'owner' && (
                                  <SelectItem value="owner">Owner</SelectItem>
                                )}
                              </SelectContent>
                            </Select>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <Badge variant={user.isActive ? "default" : "secondary"}>
                              {user.isActive ? "Active" : "Inactive"}
                            </Badge>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-muted-foreground">
                            {user.updatedAt 
                              ? formatDistanceToNow(new Date(user.updatedAt), { addSuffix: true })
                              : "Never"
                            }
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                            <div className="flex items-center space-x-2">
                              <Button 
                                variant="ghost" 
                                size="sm"
                                onClick={() => openEditing(user.id, "quickassign")}
                                title="Quick Assign to Organization"
                                data-testid={`button-quick-assign-${user.id}`}
                              >
                                <UserPlus className="h-4 w-4 text-blue-600" />
                              </Button>
                              <Button 
                                variant="ghost" 
                                size="sm"
                                onClick={() => {
                                  if (isExpanded) {
                                    closeEditing();
                                  } else {
                                    openEditing(user.id, "profile");
                                  }
                                }}
                                data-testid={`button-edit-user-${user.id}`}
                              >
                                {isExpanded ? <ChevronUp className="h-4 w-4" /> : <Edit className="h-4 w-4" />}
                              </Button>
                            </div>
                          </td>
                        </tr>,
                        renderInlineEditingPanel(user)
                      ];
                    }).flat()}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

    </MainLayout>
  );
}