import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { isUnauthorizedError } from "@/lib/authUtils";
import { useToast } from "@/hooks/use-toast";
import { 
  History, 
  Search, 
  Filter, 
  Download,
  User,
  FileText,
  Eye,
  Edit,
  Trash2,
  Upload,
  LogIn,
  LogOut
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { formatDateUSA, formatDateTimeUSA } from "@/lib/dateFormat";

export default function Audit() {
  const { user } = useAuth();
  const { toast } = useToast();
  
  const [searchQuery, setSearchQuery] = useState("");
  const [actionFilter, setActionFilter] = useState("all");
  const [userFilter, setUserFilter] = useState("all");

  // Check if user has audit access
  const canViewAudit = user?.role === 'admin' || user?.role === 'owner' || user?.role === 'auditor';

  const { data: auditData, isLoading, error } = useQuery({
    queryKey: ["/api/audit"],
    enabled: canViewAudit,
    retry: false,
  });

  // Handle unauthorized errors
  if (error && isUnauthorizedError(error as Error)) {
    toast({
      title: "Unauthorized",
      description: "You are logged out. Logging in again...",
      variant: "destructive",
    });
    setTimeout(() => {
      window.location.href = "/api/login";
    }, 500);
  }

  const auditLogs = (auditData && Array.isArray((auditData as any).logs)) ? (auditData as any).logs : [];

  // Filter logs based on search and filters
  const filteredLogs = auditLogs.filter((log: any) => {
    const matchesSearch = log.action.toLowerCase().includes(searchQuery.toLowerCase()) ||
                         log.details?.toString().toLowerCase().includes(searchQuery.toLowerCase());
    const matchesAction = actionFilter === "all" || log.action === actionFilter;
    const matchesUser = userFilter === "all" || log.userId === userFilter;
    return matchesSearch && matchesAction && matchesUser;
  });

  const getActionIcon = (action: string) => {
    switch (action) {
      case 'login':
      case 'user_login':
        return LogIn;
      case 'logout':
      case 'user_logout':
        return LogOut;
      case 'contract_uploaded':
      case 'file_uploaded':
        return Upload;
      case 'contract_viewed':
      case 'user_profile_viewed':
        return Eye;
      case 'contract_analyzed':
      case 'user_role_updated':
        return Edit;
      case 'contract_deleted':
      case 'user_deleted':
        return Trash2;
      default:
        return FileText;
    }
  };

  const getActionColor = (action: string) => {
    if (action.includes('delete') || action.includes('failed')) return 'text-red-600';
    if (action.includes('upload') || action.includes('create')) return 'text-green-600';
    if (action.includes('update') || action.includes('edit')) return 'text-blue-600';
    return 'text-muted-foreground';
  };

  if (!canViewAudit) {
    return (
      <MainLayout title="Access Denied" description="You don't have permission to view this page">
        <Card>
          <CardContent className="text-center py-12">
            <History className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-medium mb-2">Access Denied</h3>
            <p className="text-muted-foreground">
              You need audit privileges to view the audit trail.
            </p>
          </CardContent>
        </Card>
      </MainLayout>
    );
  }

  return (
    <MainLayout 
      title="Audit Trail" 
      description="Complete log of all system activities and user actions"
    >
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Badge variant="outline" className="text-sm">
              {filteredLogs.length} entries
            </Badge>
          </div>
          <Button data-testid="button-export-audit">
            <Download className="h-4 w-4 mr-2" />
            Export Log
          </Button>
        </div>

        {/* Filters */}
        <Card>
          <CardContent className="p-6">
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
                <Input
                  placeholder="Search activities..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                  data-testid="input-search-audit"
                />
              </div>
              <Select value={actionFilter} onValueChange={setActionFilter}>
                <SelectTrigger className="w-[180px]" data-testid="select-action-filter">
                  <SelectValue placeholder="All Actions" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Actions</SelectItem>
                  <SelectItem value="login">Login</SelectItem>
                  <SelectItem value="logout">Logout</SelectItem>
                  <SelectItem value="contract_uploaded">Upload</SelectItem>
                  <SelectItem value="contract_viewed">View</SelectItem>
                  <SelectItem value="contract_analyzed">Analysis</SelectItem>
                  <SelectItem value="user_role_updated">Role Change</SelectItem>
                </SelectContent>
              </Select>
              <Select value={userFilter} onValueChange={setUserFilter}>
                <SelectTrigger className="w-[180px]" data-testid="select-user-filter">
                  <SelectValue placeholder="All Users" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Users</SelectItem>
                  {/* In a real app, this would be populated with actual users */}
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>

        {/* Audit Log */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <History className="h-5 w-5" />
              Activity Log
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-4">
                {[...Array(5)].map((_, i) => (
                  <div key={i} className="flex items-center space-x-4 p-4 animate-pulse">
                    <div className="h-10 w-10 bg-muted rounded-full" />
                    <div className="flex-1 space-y-2">
                      <div className="h-4 bg-muted rounded w-1/3" />
                      <div className="h-3 bg-muted rounded w-1/2" />
                    </div>
                    <div className="h-4 bg-muted rounded w-20" />
                  </div>
                ))}
              </div>
            ) : filteredLogs.length === 0 ? (
              <div className="text-center py-12">
                <History className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-medium mb-2">No audit entries found</h3>
                <p className="text-muted-foreground">
                  {searchQuery || actionFilter !== "all" || userFilter !== "all"
                    ? "Try adjusting your search criteria or filters"
                    : "System activities will appear here as they occur"
                  }
                </p>
              </div>
            ) : (
              <div className="space-y-2">
                {filteredLogs.map((log: any) => {
                  const ActionIcon = getActionIcon(log.action);
                  const actionColor = getActionColor(log.action);
                  
                  return (
                    <div 
                      key={log.id}
                      className="flex items-center space-x-4 p-4 border border-border rounded-lg hover:bg-muted/50 transition-colors"
                      data-testid={`audit-entry-${log.id}`}
                    >
                      <div className={`h-10 w-10 rounded-full flex items-center justify-center bg-muted/50 ${actionColor}`}>
                        <ActionIcon className="h-5 w-5" />
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center space-x-2">
                          <span className="font-medium text-foreground">
                            {log.action.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}
                          </span>
                          {log.resourceType && (
                            <Badge variant="outline" className="text-xs">
                              {log.resourceType}
                            </Badge>
                          )}
                        </div>
                        
                        <div className="flex items-center space-x-4 mt-1 text-sm text-muted-foreground">
                          <span className="flex items-center">
                            <User className="h-3 w-3 mr-1" />
                            User ID: {log.userId.slice(0, 8)}...
                          </span>
                          <span>
                            {formatDistanceToNow(new Date(log.createdAt), { addSuffix: true })}
                          </span>
                          {log.ipAddress && (
                            <span>IP: {log.ipAddress}</span>
                          )}
                        </div>
                        
                        {log.details && Object.keys(log.details).length > 0 && (
                          <div className="mt-2 text-xs text-muted-foreground">
                            {Object.entries(log.details).map(([key, value]) => (
                              <span key={key} className="mr-4">
                                {key}: {String(value)}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                      
                      <div className="text-right">
                        <div className="text-sm font-medium text-foreground">
                          {formatDateUSA(log.createdAt)}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          {new Date(log.createdAt).toLocaleTimeString('en-US')}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </MainLayout>
  );
}
