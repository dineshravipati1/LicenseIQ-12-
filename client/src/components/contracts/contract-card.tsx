import { useLocation } from "wouter";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { 
  FileText, 
  Eye, 
  Download, 
  MoreHorizontal,
  Calendar,
  User,
  Zap,
  AlertTriangle,
  Trash2,
  Edit
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";

interface ContractCardProps {
  contract: any;
  className?: string;
}

export default function ContractCard({ contract, className }: ContractCardProps) {
  const [, setLocation] = useLocation();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const deleteMutation = useMutation({
    mutationFn: async () => {
      console.log("Attempting to delete contract:", contract.id);
      try {
        const response = await apiRequest("DELETE", `/api/contracts/${contract.id}`);
        console.log("Delete response status:", response.status);
        
        if (!response.ok) {
          const errorData = await response.json();
          console.error("Delete failed with error:", errorData);
          throw new Error(errorData.message || "Failed to delete contract");
        }
        
        const result = await response.json();
        console.log("Delete successful:", result);
        return result;
      } catch (error) {
        console.error("Delete mutation error:", error);
        throw error;
      }
    },
    onMutate: async () => {
      // Cancel any outgoing refetches (so they don't overwrite our optimistic update)
      await queryClient.cancelQueries({ queryKey: ["/api/contracts"] });
      await queryClient.cancelQueries({ queryKey: ["/api/analytics/all"] });

      // Snapshot the previous values
      const previousContracts = queryClient.getQueryData(["/api/contracts"]);
      const previousAnalytics = queryClient.getQueryData(["/api/analytics/all"]);

      // Optimistically update to remove the contract immediately
      queryClient.setQueryData(["/api/contracts"], (old: any) => {
        if (!old?.contracts) return old;
        
        return {
          ...old,
          contracts: old.contracts.filter((c: any) => c.id !== contract.id),
          total: old.total - 1
        };
      });

      // Also update analytics metrics immediately in the new combined endpoint
      queryClient.setQueryData(["/api/analytics/all"], (old: any) => {
        if (!old?.metrics) return old;
        return {
          ...old,
          metrics: {
            ...old.metrics,
            totalContracts: Math.max(0, (old.metrics.totalContracts || 1) - 1)
          }
        };
      });

      // Return a context object with the snapshotted values
      return { previousContracts, previousAnalytics };
    },
    onSuccess: async (data) => {
      console.log("Delete mutation success:", data);
      toast({
        title: "Contract Deleted",
        description: "The contract has been permanently deleted.",
      });
      // Comprehensive cache invalidation for immediate UI updates
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["/api/contracts"] }),
        queryClient.invalidateQueries({ queryKey: ["/api/analytics/all"] }),
        queryClient.invalidateQueries({ queryKey: ["/api/analytics/metrics"] }),
        // Force immediate refetch to ensure UI updates
        queryClient.refetchQueries({ queryKey: ["/api/contracts"] }),
        queryClient.refetchQueries({ queryKey: ["/api/analytics/metrics"] })
      ]);
    },
    onError: (error: Error, variables, context) => {
      console.error("Delete mutation onError:", error);
      
      // Rollback the optimistic updates
      if (context?.previousContracts) {
        queryClient.setQueryData(["/api/contracts"], context.previousContracts);
      }
      if (context?.previousAnalytics) {
        queryClient.setQueryData(["/api/analytics/all"], context.previousAnalytics);
      }
      
      toast({
        title: "Delete Failed", 
        description: error.message || "An unknown error occurred",
        variant: "destructive",
      });
    },
  });

  const handleView = () => {
    setLocation(`/contracts/${contract.id}`);
  };

  const handleDelete = (e: React.MouseEvent) => {
    e.stopPropagation();
    console.log("Delete button clicked for contract:", contract.id);
    deleteMutation.mutate();
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'analyzed':
        return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-300';
      case 'processing':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300';
      case 'failed':
        return 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-300';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-300';
    }
  };

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'urgent':
        return 'text-red-600';
      case 'high':
        return 'text-orange-600';
      default:
        return 'text-muted-foreground';
    }
  };

  const getFileIcon = () => {
    const type = contract.fileType;
    if (type === 'application/pdf') {
      return <FileText className="h-5 w-5 text-red-500" />;
    } else if (type?.includes('word') || type?.includes('document')) {
      return <FileText className="h-5 w-5 text-blue-500" />;
    }
    return <FileText className="h-5 w-5 text-muted-foreground" />;
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <Card className={cn("card-hover cursor-pointer relative", className)} onClick={handleView}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-start space-x-3 flex-1 min-w-0 overflow-hidden">
            {getFileIcon()}
            <div className="flex-1 min-w-0 overflow-hidden">
              <div className="flex items-center gap-2 mb-1">
                <h3 className="font-medium text-foreground text-sm leading-tight break-words flex-1" title={contract.originalName}>
                  {contract.originalName}
                </h3>
              </div>
              <div className="flex items-center gap-2">
                {contract.contractNumber && (
                  <Badge variant="outline" className="text-xs font-mono">
                    {contract.contractNumber}
                  </Badge>
                )}
                <p className="text-xs text-muted-foreground capitalize">
                  {contract.contractType || 'Unknown type'}
                </p>
              </div>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            {contract.priority !== 'normal' && (
              <div className={cn("h-2 w-2 rounded-full", getPriorityColor(contract.priority))} />
            )}
            <Badge className={getStatusColor(contract.status)} variant="secondary">
              {contract.status}
            </Badge>
          </div>
        </div>
      </CardHeader>

      <CardContent className="pt-0">
        <div className="space-y-3">
          {/* File Info */}
          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>{formatFileSize(contract.fileSize)}</span>
            <div className="flex items-center space-x-1">
              <Calendar className="h-3 w-3" />
              <span>{formatDistanceToNow(new Date(contract.createdAt), { addSuffix: true })}</span>
            </div>
          </div>

          {/* Analysis Info */}
          {contract.analysis && (
            <div className="flex items-center justify-between text-sm">
              <div className="flex items-center space-x-1 text-muted-foreground">
                <Zap className="h-3 w-3" />
                <span>AI Analysis Complete</span>
              </div>
              {contract.analysis.confidence && (
                <Badge variant="outline" className="text-xs">
                  {Math.round(parseFloat(contract.analysis.confidence) * 100)}% confidence
                </Badge>
              )}
            </div>
          )}

          {/* Risk Indicators */}
          {contract.analysis?.riskAnalysis && contract.analysis.riskAnalysis.length > 0 && (
            <div className="flex items-center justify-between text-sm">
              <div className="flex items-center space-x-1 text-amber-600">
                <AlertTriangle className="h-3 w-3" />
                <span>{contract.analysis.riskAnalysis.length} risk(s) identified</span>
              </div>
            </div>
          )}

          {/* Uploaded By */}
          {contract.uploadedByUser && (
            <div className="flex items-center space-x-1 text-sm text-muted-foreground">
              <User className="h-3 w-3" />
              <span>
                {contract.uploadedByUser.firstName && contract.uploadedByUser.lastName
                  ? `${contract.uploadedByUser.firstName} ${contract.uploadedByUser.lastName}`
                  : contract.uploadedByUser.email
                }
              </span>
            </div>
          )}

          {/* Actions */}
          <div className="flex items-center justify-between pt-2 border-t border-border">
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  handleView();
                }}
                data-testid={`button-view-${contract.id}`}
              >
                <Eye className="h-4 w-4 mr-1" />
                View
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  setLocation(`/contracts/${contract.id}/manage`);
                }}
                data-testid={`button-edit-metadata-${contract.id}`}
              >
                <Edit className="h-4 w-4 mr-1" />
                Edit
              </Button>
            </div>
            
            <div className="flex items-center space-x-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={(e) => e.stopPropagation()}
                data-testid={`button-download-${contract.id}`}
              >
                <Download className="h-4 w-4" />
              </Button>
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={(e) => e.stopPropagation()}
                    className="text-red-600 hover:text-red-700 hover:bg-red-50"
                    data-testid={`button-delete-${contract.id}`}
                    disabled={deleteMutation.isPending}
                  >
                    {deleteMutation.isPending ? (
                      <div className="h-4 w-4 animate-spin rounded-full border-2 border-red-600 border-t-transparent" />
                    ) : (
                      <Trash2 className="h-4 w-4" />
                    )}
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Delete Contract</AlertDialogTitle>
                    <AlertDialogDescription>
                      Are you sure you want to delete "{contract.originalName}"? This action cannot be undone. 
                      The contract file and all analysis data will be permanently removed.
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction
                      className="bg-red-600 hover:bg-red-700"
                      onClick={handleDelete}
                      disabled={deleteMutation.isPending}
                    >
                      {deleteMutation.isPending ? "Deleting..." : "Delete Contract"}
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
