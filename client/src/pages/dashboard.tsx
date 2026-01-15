import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import MetricsCard from "@/components/analytics/metrics-card";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useLocation } from "wouter";
import { 
  File, 
  Settings, 
  CheckCircle, 
  DollarSign,
  FileText,
  Eye,
  Lightbulb,
  AlertTriangle,
  TrendingUp
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";

export default function Dashboard() {
  const { user } = useAuth();
  const [, setLocation] = useLocation();

  const { data: metrics } = useQuery({
    queryKey: ["/api/analytics/metrics"],
    staleTime: 30000, // Consider stale after 30 seconds
    refetchInterval: 60000, // Auto-refresh every minute
    refetchOnWindowFocus: true, // Refresh when user returns to tab
  });

  const { data: contractsData } = useQuery({
    queryKey: ["/api/contracts"],
    staleTime: 30000, // Consider stale after 30 seconds  
    refetchOnWindowFocus: true,
  });

  const recentContracts = contractsData?.contracts?.slice(0, 3) || [];

  const handleUpload = () => {
    setLocation("/upload");
  };

  return (
    <MainLayout title="Dashboard" description="Welcome back, manage your contracts and insights">
      <div className="space-y-6">

        {/* Metrics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <MetricsCard
            title="Total Contracts"
            value={metrics?.totalContracts || 0}
            icon={File}
            trend={metrics?.recentUploads ? `+${metrics.recentUploads}` : "+0"}
            trendLabel="recent uploads"
            data-testid="metric-total-contracts"
          />
          <MetricsCard
            title="Processing"
            value={metrics?.processing || 0}
            icon={Settings}
            trend={metrics?.processing > 0 ? "Active" : "Idle"}
            trendLabel="status"
            variant="processing"
            data-testid="metric-processing"
          />
          <MetricsCard
            title="Analyzed"
            value={metrics?.analyzed || 0}
            icon={CheckCircle}
            trend={metrics?.totalContracts > 0 ? `${Math.round((metrics.analyzed / metrics.totalContracts) * 100)}%` : "0%"}
            trendLabel="completion rate"
            variant="success"
            data-testid="metric-analyzed"
          />
          <MetricsCard
            title="Active Users"
            value={metrics?.activeUsers || 0}
            icon={DollarSign}
            trend={metrics?.activeUsers > 1 ? "Multi-user" : "Single-user"}
            trendLabel="activity"
            variant="revenue"
            data-testid="metric-active-users"
          />
        </div>

        {/* Main Content Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Recent Contracts */}
          <Card>
            <CardHeader>
              <CardTitle>Recent Contracts</CardTitle>
            </CardHeader>
            <CardContent>
              {recentContracts.length === 0 ? (
                <div className="text-center py-8">
                  <File className="h-12 w-12 text-blue-400 mx-auto mb-4" />
                  <p className="text-muted-foreground">No contracts uploaded yet</p>
                  <Button variant="outline" className="mt-4" onClick={handleUpload}>
                    Upload your first contract
                  </Button>
                </div>
              ) : (
                <div className="space-y-4">
                  {recentContracts.map((contract: any) => (
                    <div 
                      key={contract.id} 
                      className="flex items-center justify-between p-3 bg-muted/50 rounded-lg"
                      data-testid={`contract-item-${contract.id}`}
                    >
                      <div className="flex items-center space-x-3">
                        <div className="h-8 w-8 bg-primary/10 rounded-full flex items-center justify-center">
                          <FileText className="h-4 w-4 text-blue-400" />
                        </div>
                        <div>
                          <p className="font-medium text-foreground">{contract.originalName}</p>
                          <p className="text-sm text-muted-foreground">
                            {formatDistanceToNow(new Date(contract.createdAt), { addSuffix: true })}
                          </p>
                        </div>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Badge 
                          variant={contract.status === 'analyzed' ? 'default' : 
                                  contract.status === 'processing' ? 'secondary' : 'outline'}
                        >
                          {contract.status}
                        </Badge>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setLocation(`/contracts/${contract.id}`)}
                          data-testid={`button-view-contract-${contract.id}`}
                        >
                          <Eye className="h-4 w-4 text-green-400" />
                        </Button>
                      </div>
                    </div>
                  ))}
                  
                  <Button 
                    variant="link" 
                    className="w-full mt-4" 
                    onClick={() => setLocation("/contracts")}
                    data-testid="button-view-all-contracts"
                  >
                    View all contracts
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Contract Statistics */}
          <Card>
            <CardHeader>
              <CardTitle>System Overview</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {metrics?.totalContracts === 0 ? (
                  <div className="text-center py-8">
                    <Lightbulb className="h-12 w-12 text-yellow-400 mx-auto mb-4" />
                    <p className="text-muted-foreground">No contracts uploaded yet</p>
                    <p className="text-sm text-muted-foreground mt-2">
                      Upload your first contract to see AI-powered insights and analysis.
                    </p>
                    <Button variant="outline" className="mt-4" onClick={handleUpload}>
                      Upload Contract
                    </Button>
                  </div>
                ) : (
                  <>
                    <div className="p-4 bg-blue-50 dark:bg-blue-950/20 rounded-lg border border-blue-200 dark:border-blue-800">
                      <div className="flex items-start space-x-3">
                        <div className="h-8 w-8 bg-blue-400/20 rounded-full flex items-center justify-center flex-shrink-0">
                          <Lightbulb className="h-4 w-4 text-blue-400" />
                        </div>
                        <div>
                          <h4 className="font-medium text-blue-900 dark:text-blue-100">Contract Analytics</h4>
                          <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
                            {metrics?.analyzed || 0} of {metrics?.totalContracts || 0} contracts have been analyzed using AI.
                          </p>
                        </div>
                      </div>
                    </div>
                    
                    {metrics?.processing > 0 && (
                      <div className="p-4 bg-amber-50 dark:bg-amber-950/20 rounded-lg border border-amber-200 dark:border-amber-800">
                        <div className="flex items-start space-x-3">
                          <div className="h-8 w-8 bg-amber-400/20 rounded-full flex items-center justify-center flex-shrink-0">
                            <AlertTriangle className="h-4 w-4 text-amber-400" />
                          </div>
                          <div>
                            <h4 className="font-medium text-amber-900 dark:text-amber-100">Processing Status</h4>
                            <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
                              {metrics?.processing || 0} contracts are currently being processed by AI analysis.
                            </p>
                          </div>
                        </div>
                      </div>
                    )}
                    
                    <div className="p-4 bg-green-50 dark:bg-green-950/20 rounded-lg border border-green-200 dark:border-green-800">
                      <div className="flex items-start space-x-3">
                        <div className="h-8 w-8 bg-emerald-400/20 rounded-full flex items-center justify-center flex-shrink-0">
                          <TrendingUp className="h-4 w-4 text-emerald-400" />
                        </div>
                        <div>
                          <h4 className="font-medium text-green-900 dark:text-green-100">System Status</h4>
                          <p className="text-sm text-green-700 dark:text-green-300 mt-1">
                            Platform is operational with {metrics?.recentUploads || 0} recent uploads.
                          </p>
                        </div>
                      </div>
                    </div>
                  </>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </MainLayout>
  );
}
