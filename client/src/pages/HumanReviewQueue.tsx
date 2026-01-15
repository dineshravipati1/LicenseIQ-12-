import { useQuery, useMutation } from '@tanstack/react-query';
import { apiRequest, queryClient } from '@/lib/queryClient';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { AlertCircle, CheckCircle, XCircle, Clock, Sparkles, ShieldAlert, RefreshCw } from 'lucide-react';
import { useState } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useAuth } from '@/hooks/useAuth';
import MainLayout from '@/components/layout/main-layout';

interface ReviewTask {
  id: string;
  contractId: string;
  extractionRunId: string;
  taskType: 'low_confidence_node' | 'low_confidence_edge' | 'low_confidence_rule' | 'validation_failure' | 'ai_disagreement';
  priority: 'low' | 'medium' | 'high' | 'urgent';
  status: 'pending' | 'in_progress' | 'approved' | 'rejected';
  targetId: string;
  targetType: 'node' | 'edge' | 'rule' | 'extraction';
  originalData: any;
  suggestedCorrection?: any;
  confidence: string;
  reviewNotes?: string;
  assignedTo?: string;
  createdAt: string;
}

export default function HumanReviewQueue() {
  const { toast } = useToast();
  const { user } = useAuth();
  const [reviewNotes, setReviewNotes] = useState<Record<string, string>>({});
  const [activeTab, setActiveTab] = useState<'all' | 'nodes' | 'edges' | 'rules'>('all');

  // Check authorization
  const isAuthorized = user?.role === 'admin' || user?.role === 'owner';

  const { data: tasks = [], isLoading, isError, error, refetch } = useQuery<ReviewTask[]>({
    queryKey: ['/api/human-review-tasks'],
    enabled: isAuthorized, // Only fetch if authorized
  });

  const approveMutation = useMutation({
    mutationFn: async ({ taskId, notes }: { taskId: string; notes?: string }) => {
      return apiRequest('PATCH', `/api/human-review-tasks/${taskId}/approve`, { reviewNotes: notes });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/human-review-tasks'] });
      toast({
        title: 'Task Approved',
        description: 'The extraction has been approved and activated.',
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Approval Failed',
        description: error.message || 'Failed to approve task',
        variant: 'destructive',
      });
    },
  });

  const rejectMutation = useMutation({
    mutationFn: async ({ taskId, notes }: { taskId: string; notes: string }) => {
      return apiRequest('PATCH', `/api/human-review-tasks/${taskId}/reject`, { reviewNotes: notes });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/human-review-tasks'] });
      toast({
        title: 'Task Rejected',
        description: 'The extraction has been rejected and will not be activated.',
      });
    },
    onError: (error: any) => {
      toast({
        title: 'Rejection Failed',
        description: error.message || 'Failed to reject task',
        variant: 'destructive',
      });
    },
  });

  const handleApprove = (taskId: string) => {
    approveMutation.mutate({ taskId, notes: reviewNotes[taskId] });
    setReviewNotes(prev => ({ ...prev, [taskId]: '' }));
  };

  const handleReject = (taskId: string) => {
    const notes = reviewNotes[taskId]?.trim();
    if (!notes) {
      toast({
        title: 'Review Notes Required',
        description: 'Please provide a reason for rejecting this task',
        variant: 'destructive',
      });
      return;
    }
    rejectMutation.mutate({ taskId, notes });
    setReviewNotes(prev => ({ ...prev, [taskId]: '' }));
  };

  const filteredTasks = tasks.filter(task => {
    if (activeTab === 'all') return true;
    if (activeTab === 'nodes') return task.targetType === 'node';
    if (activeTab === 'edges') return task.targetType === 'edge';
    if (activeTab === 'rules') return task.targetType === 'rule';
    return true;
  });

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'urgent': return 'destructive';
      case 'high': return 'destructive';
      case 'medium': return 'default';
      case 'low': return 'secondary';
      default: return 'default';
    }
  };

  const getTaskTypeLabel = (type: string) => {
    switch (type) {
      case 'low_confidence_node': return 'Low Confidence Entity';
      case 'low_confidence_edge': return 'Low Confidence Relationship';
      case 'low_confidence_rule': return 'Low Confidence Rule';
      case 'validation_failure': return 'Validation Failure';
      case 'ai_disagreement': return 'AI Disagreement';
      default: return type;
    }
  };

  // Authorization check
  if (!isAuthorized) {
    return (
      <MainLayout
        title="Human Review Queue"
        description="Review and approve AI-extracted contract data"
      >
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <ShieldAlert className="h-16 w-16 text-amber-500 mb-4" />
            <h3 className="text-xl font-semibold mb-2">Access Restricted</h3>
            <p className="text-muted-foreground text-center">
              Only administrators and owners can access the human review queue.
            </p>
          </CardContent>
        </Card>
      </MainLayout>
    );
  }

  if (isLoading) {
    return (
      <MainLayout
        title="Human Review Queue"
        description="Review and approve AI-extracted contract data"
      >
        <div className="flex items-center justify-center h-64">
          <Clock className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </MainLayout>
    );
  }

  if (isError) {
    return (
      <MainLayout
        title="Human Review Queue"
        description="Review and approve AI-extracted contract data"
      >
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <AlertCircle className="h-16 w-16 text-red-500 mb-4" />
            <h3 className="text-xl font-semibold mb-2">Failed to Load Tasks</h3>
            <p className="text-muted-foreground mb-4">
              {(error as any)?.message || 'An error occurred while fetching review tasks'}
            </p>
            <Button onClick={() => refetch()} data-testid="button-retry">
              <RefreshCw className="h-4 w-4 mr-2" />
              Try Again
            </Button>
          </CardContent>
        </Card>
      </MainLayout>
    );
  }

  return (
    <MainLayout
      title="Human Review Queue"
      description="Review and approve AI-extracted contract data"
    >
      <div className="mb-6 flex items-center gap-4">
        <Badge variant="outline" className="text-lg px-4 py-2" data-testid="badge-pending-count">
          {tasks.length} Pending Tasks
        </Badge>
      </div>

      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as any)} className="mb-6">
        <TabsList>
          <TabsTrigger value="all" data-testid="tab-all">All ({tasks.length})</TabsTrigger>
          <TabsTrigger value="nodes" data-testid="tab-nodes">
            Entities ({tasks.filter(t => t.targetType === 'node').length})
          </TabsTrigger>
          <TabsTrigger value="edges" data-testid="tab-edges">
            Relationships ({tasks.filter(t => t.targetType === 'edge').length})
          </TabsTrigger>
          <TabsTrigger value="rules" data-testid="tab-rules">
            Rules ({tasks.filter(t => t.targetType === 'rule').length})
          </TabsTrigger>
        </TabsList>
      </Tabs>

      {filteredTasks.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <CheckCircle className="h-16 w-16 text-green-500 mb-4" />
            <h3 className="text-xl font-semibold mb-2">All Caught Up!</h3>
            <p className="text-muted-foreground">No pending review tasks at the moment.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-6">
          {filteredTasks.map((task) => (
            <Card key={task.id} data-testid={`card-task-${task.id}`}>
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <CardTitle data-testid={`text-task-type-${task.id}`}>
                        {getTaskTypeLabel(task.taskType)}
                      </CardTitle>
                      <Badge variant={getPriorityColor(task.priority)} data-testid={`badge-priority-${task.id}`}>
                        {task.priority.toUpperCase()}
                      </Badge>
                    </div>
                    <CardDescription>
                      Contract ID: {task.contractId} • Target: {task.targetType}
                    </CardDescription>
                  </div>
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-5 w-5 text-amber-500" />
                    <span className="text-sm font-medium" data-testid={`text-confidence-${task.id}`}>
                      {task.confidence ? parseFloat(task.confidence).toFixed(0) : '0'}% confidence
                    </span>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label className="text-sm font-semibold">Extracted Data:</Label>
                  <pre className="mt-2 p-3 bg-muted rounded-md text-sm overflow-auto max-h-48">
                    {JSON.stringify(task.originalData, null, 2)}
                  </pre>
                </div>

                {task.suggestedCorrection && (
                  <div>
                    <Label className="text-sm font-semibold text-green-600">AI Suggested Correction:</Label>
                    <pre className="mt-2 p-3 bg-green-50 dark:bg-green-950 rounded-md text-sm overflow-auto max-h-48">
                      {JSON.stringify(task.suggestedCorrection, null, 2)}
                    </pre>
                  </div>
                )}

                <div>
                  <Label htmlFor={`notes-${task.id}`}>Review Notes (optional)</Label>
                  <Textarea
                    id={`notes-${task.id}`}
                    data-testid={`input-notes-${task.id}`}
                    placeholder="Add notes about this review..."
                    value={reviewNotes[task.id] || ''}
                    onChange={(e) => setReviewNotes(prev => ({ ...prev, [task.id]: e.target.value }))}
                    className="mt-2"
                    rows={3}
                  />
                </div>
              </CardContent>
              <CardFooter className="flex justify-end gap-3">
                <Button
                  variant="outline"
                  onClick={() => handleReject(task.id)}
                  disabled={rejectMutation.isPending}
                  data-testid={`button-reject-${task.id}`}
                >
                  <XCircle className="h-4 w-4 mr-2" />
                  Reject
                </Button>
                <Button
                  onClick={() => handleApprove(task.id)}
                  disabled={approveMutation.isPending}
                  data-testid={`button-approve-${task.id}`}
                >
                  <CheckCircle className="h-4 w-4 mr-2" />
                  Approve & Activate
                </Button>
              </CardFooter>
            </Card>
          ))}
        </div>
      )}
    </MainLayout>
  );
}
