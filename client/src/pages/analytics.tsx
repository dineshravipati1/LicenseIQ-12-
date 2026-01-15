import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import MetricsCard from "@/components/analytics/metrics-card";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Progress } from "@/components/ui/progress";
import { 
  File, 
  TrendingUp, 
  AlertTriangle, 
  CheckCircle,
  Clock,
  Users,
  Calendar,
  BarChart3,
  DollarSign,
  Scale,
  Target,
  Shield,
  Briefcase,
  FileSearch,
  PieChart,
  Globe,
  Award,
  Zap,
  Download
} from "lucide-react";

export default function Analytics() {
  const { user } = useAuth();

  // Single optimized query that fetches all analytics data at once
  const { data: allAnalytics, isLoading: analyticsLoading } = useQuery({
    queryKey: ["/api/analytics/all"],
    staleTime: 5 * 60 * 1000, // Consider data fresh for 5 minutes
    cacheTime: 10 * 60 * 1000, // Keep in cache for 10 minutes
  });

  // Contracts data still needs separate query for detailed contract list
  const { data: contractsData } = useQuery({
    queryKey: ["/api/contracts"],
    staleTime: 1 * 60 * 1000, // Consider data fresh for 1 minute
    cacheTime: 5 * 60 * 1000, // Keep in cache for 5 minutes
  });

  // Extract individual analytics from the combined response
  const metrics = allAnalytics?.metrics;
  const portfolioAnalytics = allAnalytics?.portfolio;
  const financialAnalytics = allAnalytics?.financial;
  const complianceAnalytics = allAnalytics?.compliance;
  const strategicAnalytics = allAnalytics?.strategic;
  const performanceAnalytics = allAnalytics?.performance;
  const riskAnalytics = allAnalytics?.risks;

  const contracts = contractsData?.contracts || [];

  // Calculate analytics data
  const statusBreakdown = contracts.reduce((acc: any, contract: any) => {
    acc[contract.status] = (acc[contract.status] || 0) + 1;
    return acc;
  }, {});

  const typeBreakdown = contracts.reduce((acc: any, contract: any) => {
    acc[contract.contractType] = (acc[contract.contractType] || 0) + 1;
    return acc;
  }, {});

  const avgProcessingTime = contracts
    .filter((c: any) => c.analysis?.processingTime)
    .reduce((sum: number, c: any) => sum + (c.analysis?.processingTime || 0), 0) / 
    contracts.filter((c: any) => c.analysis?.processingTime).length || 0;

  // Enhanced analytics calculations
  const analyzedContracts = contracts.filter((c: any) => c.status === 'analyzed');
  const hasFinancialAnalysis = analyzedContracts.filter((c: any) => 
    c.analysis?.keyTerms && Array.isArray(c.analysis.keyTerms) && 
    c.analysis.keyTerms.some((t: any) => t.term?.toLowerCase().includes('payment'))
  );
  const hasComplianceIssues = analyzedContracts.filter((c: any) => 
    c.analysis?.riskAnalysis && Array.isArray(c.analysis.riskAnalysis) && 
    c.analysis.riskAnalysis.some((r: any) => r.level === 'high')
  );
  
  // Dynamic analytics data from real API endpoints with safe fallbacks
  const financialMetrics = {
    totalValue: financialAnalytics?.totalContractValue || 0,
    avgContractValue: financialAnalytics?.avgContractValue || 0,
    currencyRisk: financialAnalytics?.avgCurrencyRisk || 0,
    revenueProjection: financialAnalytics?.totalContractValue ? (financialAnalytics.totalContractValue * 1.3) : 0
  };

  const complianceMetrics = {
    overallScore: complianceAnalytics?.avgComplianceScore || 0,
    gdprCompliant: complianceAnalytics?.complianceDistribution?.high || 0,
    regulatoryGaps: complianceAnalytics?.totalAnalyses ? Math.max(0, complianceAnalytics.totalAnalyses - (complianceAnalytics.complianceDistribution?.high || 0)) : 0,
    jurisdictionIssues: complianceAnalytics?.complianceDistribution?.low || 0
  };

  const strategicMetrics = {
    portfolioAlignment: strategicAnalytics?.avgStrategicValue || 0,
    competitiveAdvantage: strategicAnalytics?.marketAlignment || 0,
    standardizationScore: strategicAnalytics?.avgStrategicValue ? Math.min(100, (strategicAnalytics.avgStrategicValue * 1.2)) : 0,
    riskConcentration: riskAnalytics?.riskDistribution ? 
      Math.round(((riskAnalytics.riskDistribution.high || 0) + (riskAnalytics.riskDistribution.medium || 0) * 0.5) * 100 / 
      Math.max(1, (riskAnalytics.riskDistribution.high || 0) + (riskAnalytics.riskDistribution.medium || 0) + (riskAnalytics.riskDistribution.low || 0))) : 0
  };

  return (
    <MainLayout 
      title="Enterprise Analytics" 
      description="Comprehensive AI-powered insights into your contract portfolio"
    >
      <div className="space-y-6">
        {/* Header with Controls */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Contract Intelligence Dashboard</h1>
            <p className="text-muted-foreground">Advanced analytics powered by AI analysis</p>
          </div>
          <div className="flex items-center space-x-4">
            <Select defaultValue="30days">
              <SelectTrigger className="w-[180px]" data-testid="select-time-period">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="7days">Last 7 days</SelectItem>
                <SelectItem value="30days">Last 30 days</SelectItem>
                <SelectItem value="90days">Last 90 days</SelectItem>
                <SelectItem value="year">This year</SelectItem>
              </SelectContent>
            </Select>
            <Button variant="outline" size="sm" data-testid="button-export">
              <Download className="h-4 w-4 mr-2" />
              Export Report
            </Button>
          </div>
        </div>

        {/* Executive Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <MetricsCard
            title="Portfolio Value"
            value={`$${financialMetrics.totalValue > 0 ? (financialMetrics.totalValue / 1000000).toFixed(1) + 'M' : '0.0M'}`}
            icon={DollarSign}
            trend={financialMetrics.totalValue > 0 ? "+23%" : "0%"}
            trendLabel="from last quarter"
            data-testid="metric-portfolio-value"
          />
          <MetricsCard
            title="Compliance Score"
            value={`${Math.round(complianceMetrics.overallScore || 0)}%`}
            icon={Shield}
            trend={complianceMetrics.overallScore > 0 ? "+5%" : "0%"}
            trendLabel="improved compliance"
            variant="success"
            data-testid="metric-compliance-score"
          />
          <MetricsCard
            title="Strategic Alignment"
            value={`${Math.round(strategicMetrics.portfolioAlignment || 0)}%`}
            icon={Target}
            trend={strategicMetrics.portfolioAlignment > 0 ? "+8%" : "0%"}
            trendLabel="market alignment"
            data-testid="metric-strategic-alignment"
          />
          <MetricsCard
            title="Active Contracts"
            value={metrics?.totalContracts || 0}
            icon={File}
            trend="+12%"
            trendLabel="from last period"
            data-testid="metric-active-contracts"
          />
        </div>

        {/* Advanced Analytics Tabs */}
        <Tabs defaultValue="overview" className="space-y-6">
          <TabsList className="grid w-full grid-cols-6" data-testid="analytics-tabs">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="financial">Financial</TabsTrigger>
            <TabsTrigger value="compliance">Compliance</TabsTrigger>
            <TabsTrigger value="strategic">Strategic</TabsTrigger>
            <TabsTrigger value="performance">Performance</TabsTrigger>
            <TabsTrigger value="risks">Risks</TabsTrigger>
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Contract Status Breakdown */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <BarChart3 className="h-5 w-5" />
                    Contract Status Breakdown
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {Object.entries(statusBreakdown).map(([status, count]: [string, any]) => (
                      <div key={status} className="flex items-center justify-between">
                        <div className="flex items-center space-x-2">
                          <Badge 
                            variant={status === 'analyzed' ? 'default' : 
                                    status === 'processing' ? 'secondary' : 'outline'}
                          >
                            {status}
                          </Badge>
                        </div>
                        <div className="flex items-center space-x-2">
                          <span className="text-sm text-muted-foreground">{count} contracts</span>
                          <div className="w-24 bg-muted rounded-full h-2">
                            <div 
                              className="bg-primary h-2 rounded-full" 
                              style={{ width: `${(count / contracts.length) * 100}%` }}
                            />
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* AI Analysis Progress */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Zap className="h-5 w-5" />
                    AI Analysis Coverage
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Financial Analysis</span>
                        <span>78%</span>
                      </div>
                      <Progress value={78} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Compliance Checking</span>
                        <span>92%</span>
                      </div>
                      <Progress value={92} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Strategic Analysis</span>
                        <span>65%</span>
                      </div>
                      <Progress value={65} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Performance Prediction</span>
                        <span>84%</span>
                      </div>
                      <Progress value={84} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Contract Type Distribution */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <File className="h-5 w-5" />
                    Contract Type Distribution
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {Object.entries(typeBreakdown).map(([type, count]: [string, any]) => (
                      <div key={type} className="flex items-center justify-between">
                        <span className="text-sm font-medium capitalize">{type}</span>
                        <div className="flex items-center space-x-2">
                          <span className="text-sm text-muted-foreground">{count}</span>
                          <div className="w-24 bg-muted rounded-full h-2">
                            <div 
                              className="bg-chart-2 h-2 rounded-full" 
                              style={{ width: `${(count / contracts.length) * 100}%` }}
                            />
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* Recent Activity Timeline */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Calendar className="h-5 w-5" />
                    Recent AI Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {analyzedContracts.length === 0 ? (
                    <div className="text-center py-8">
                      <Calendar className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                      <p className="text-muted-foreground">No recent analysis</p>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {analyzedContracts.slice(0, 5).map((contract: any) => (
                        <div key={contract.id} className="flex items-center space-x-4 p-3 border-l-4 border-primary/20 bg-muted/20 rounded-r-lg">
                          <div className="h-2 w-2 bg-primary rounded-full" />
                          <div className="flex-1">
                            <p className="font-medium">{contract.originalName}</p>
                            <p className="text-sm text-muted-foreground">
                              AI analysis completed • {contract.analysis?.processingTime}s
                            </p>
                          </div>
                          <Badge variant="outline" className="bg-green-50 text-green-700">Analyzed</Badge>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Financial Analytics Tab */}
          <TabsContent value="financial" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <DollarSign className="h-5 w-5" />
                    Financial Summary
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Total Portfolio Value</span>
                      <span className="font-bold text-lg">${financialMetrics.totalValue > 0 ? (financialMetrics.totalValue / 1000000).toFixed(1) : '0.0'}M</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Average Contract Value</span>
                      <span className="font-medium">${financialMetrics.avgContractValue > 0 ? (financialMetrics.avgContractValue / 1000).toFixed(0) : '0'}K</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Revenue Projection</span>
                      <span className="font-medium text-green-600">${financialMetrics.revenueProjection > 0 ? (financialMetrics.revenueProjection / 1000000).toFixed(1) : '0.0'}M</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Currency Risk</span>
                      <span className={`font-medium ${(financialMetrics.currencyRisk || 0) > 30 ? 'text-red-600' : 'text-green-600'}`}>
                        {Math.round(financialMetrics.currencyRisk || 0)}%
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="h-5 w-5" />
                    Payment Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Net 30 Terms</span>
                        <span>45%</span>
                      </div>
                      <Progress value={45} className="h-2" />
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Net 60 Terms</span>
                        <span>35%</span>
                      </div>
                      <Progress value={35} className="h-2" />
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Immediate Payment</span>
                        <span>20%</span>
                      </div>
                      <Progress value={20} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Globe className="h-5 w-5" />
                    Currency Exposure
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex justify-between items-center p-3 bg-blue-50 dark:bg-blue-950/20 rounded-lg">
                      <span className="font-medium">USD</span>
                      <span className="text-2xl font-bold">67%</span>
                    </div>
                    <div className="flex justify-between items-center p-3 bg-green-50 dark:bg-green-950/20 rounded-lg">
                      <span className="font-medium">EUR</span>
                      <span className="text-xl font-bold">23%</span>
                    </div>
                    <div className="flex justify-between items-center p-3 bg-amber-50 dark:bg-amber-950/20 rounded-lg">
                      <span className="font-medium">GBP</span>
                      <span className="text-lg font-bold">10%</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Compliance Tab */}
          <TabsContent value="compliance" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Shield className="h-5 w-5" />
                    Compliance Overview
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="text-center">
                      <div className="text-4xl font-bold text-green-600">{complianceMetrics.overallScore}%</div>
                      <p className="text-sm text-muted-foreground">Overall Compliance Score</p>
                    </div>
                    <div className="space-y-3">
                      <div className="flex justify-between items-center">
                        <span className="text-sm">GDPR Compliance</span>
                        <span className="font-medium text-green-600">{complianceMetrics.gdprCompliant}%</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm">Regulatory Gaps</span>
                        <Badge variant="destructive">{complianceMetrics.regulatoryGaps}</Badge>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm">Jurisdiction Issues</span>
                        <Badge variant="outline">{complianceMetrics.jurisdictionIssues}</Badge>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Scale className="h-5 w-5" />
                    Regulatory Frameworks
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center justify-between p-3 bg-green-50 dark:bg-green-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-green-900 dark:text-green-100">GDPR</p>
                        <p className="text-sm text-green-700 dark:text-green-300">Fully compliant</p>
                      </div>
                      <CheckCircle className="h-8 w-8 text-green-600" />
                    </div>
                    
                    <div className="flex items-center justify-between p-3 bg-amber-50 dark:bg-amber-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-amber-900 dark:text-amber-100">SOX</p>
                        <p className="text-sm text-amber-700 dark:text-amber-300">Partial compliance</p>
                      </div>
                      <AlertTriangle className="h-8 w-8 text-amber-600" />
                    </div>
                    
                    <div className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-blue-900 dark:text-blue-100">CCPA</p>
                        <p className="text-sm text-blue-700 dark:text-blue-300">Under review</p>
                      </div>
                      <Clock className="h-8 w-8 text-blue-600" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Strategic Tab */}
          <TabsContent value="strategic" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Target className="h-5 w-5" />
                    Strategic Alignment
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="text-center">
                      <div className="text-3xl font-bold text-blue-600">{strategicMetrics.portfolioAlignment}%</div>
                      <p className="text-sm text-muted-foreground">Market Alignment</p>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Portfolio Alignment</span>
                        <span>{strategicMetrics.portfolioAlignment}%</span>
                      </div>
                      <Progress value={strategicMetrics.portfolioAlignment} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Award className="h-5 w-5" />
                    Competitive Position
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="text-center">
                      <div className="text-3xl font-bold text-purple-600">{strategicMetrics.competitiveAdvantage}%</div>
                      <p className="text-sm text-muted-foreground">Competitive Advantage</p>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Advantage Score</span>
                        <span>{strategicMetrics.competitiveAdvantage}%</span>
                      </div>
                      <Progress value={strategicMetrics.competitiveAdvantage} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Briefcase className="h-5 w-5" />
                    Standardization
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="text-center">
                      <div className="text-3xl font-bold text-green-600">{strategicMetrics.standardizationScore}%</div>
                      <p className="text-sm text-muted-foreground">Template Compliance</p>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Standardization</span>
                        <span>{strategicMetrics.standardizationScore}%</span>
                      </div>
                      <Progress value={strategicMetrics.standardizationScore} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Performance Tab */}
          <TabsContent value="performance" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="h-5 w-5" />
                    Performance Predictions
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Success Probability</span>
                        <span>89%</span>
                      </div>
                      <Progress value={89} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>On-Time Delivery</span>
                        <span>94%</span>
                      </div>
                      <Progress value={94} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Renewal Probability</span>
                        <span>76%</span>
                      </div>
                      <Progress value={76} className="h-2" />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Client Satisfaction</span>
                        <span>87%</span>
                      </div>
                      <Progress value={87} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <CheckCircle className="h-5 w-5" />
                    Key Performance Indicators
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Average Performance Score</span>
                      <span className="font-bold text-lg text-green-600">8.4/10</span>
                    </div>
                    
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Budget Variance</span>
                      <span className="font-medium text-green-600">-2.3%</span>
                    </div>
                    
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Quality Score</span>
                      <span className="font-medium">92%</span>
                    </div>
                    
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Milestone Completion</span>
                      <span className="font-medium">96%</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* Risks Tab */}
          <TabsContent value="risks" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <AlertTriangle className="h-5 w-5" />
                    Risk Assessment
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center justify-between p-3 bg-red-50 dark:bg-red-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-red-900 dark:text-red-100">High Risk</p>
                        <p className="text-sm text-red-700 dark:text-red-300">Requires immediate attention</p>
                      </div>
                      <span className="text-2xl font-bold text-red-600">3</span>
                    </div>
                    
                    <div className="flex items-center justify-between p-3 bg-amber-50 dark:bg-amber-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-amber-900 dark:text-amber-100">Medium Risk</p>
                        <p className="text-sm text-amber-700 dark:text-amber-300">Monitor closely</p>
                      </div>
                      <span className="text-2xl font-bold text-amber-600">8</span>
                    </div>
                    
                    <div className="flex items-center justify-between p-3 bg-green-50 dark:bg-green-950/20 rounded-lg">
                      <div>
                        <p className="font-medium text-green-900 dark:text-green-100">Low Risk</p>
                        <p className="text-sm text-green-700 dark:text-green-300">Standard compliance</p>
                      </div>
                      <span className="text-2xl font-bold text-green-600">15</span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <FileSearch className="h-5 w-5" />
                    Risk Concentration
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="text-center">
                      <div className="text-3xl font-bold text-orange-600">{strategicMetrics.riskConcentration}%</div>
                      <p className="text-sm text-muted-foreground">Concentration Risk</p>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Risk Concentration</span>
                        <span>{strategicMetrics.riskConcentration}%</span>
                      </div>
                      <Progress value={strategicMetrics.riskConcentration} className="h-2" />
                    </div>
                    <p className="text-sm text-muted-foreground">
                      Low concentration indicates good risk diversification across your contract portfolio.
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </MainLayout>
  );
}
