import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { format } from "date-fns";
import { 
  FileText, 
  Download, 
  Filter, 
  Calendar as CalendarIcon,
  BarChart3,
  FileSpreadsheet,
  FileDown,
  Eye
} from "lucide-react";

export default function Reports() {
  const { user } = useAuth();
  const [reportType, setReportType] = useState("contract-summary");
  const [dateRange, setDateRange] = useState<{ from: Date; to: Date }>({
    from: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
    to: new Date(),
  });
  const [exportFormat, setExportFormat] = useState("pdf");

  const { data: contractsData } = useQuery({
    queryKey: ["/api/contracts"],
  });

  const contracts = contractsData?.contracts || [];

  // Generate sample reports data
  const reports = [
    {
      id: "1",
      name: "Monthly Contract Summary",
      type: "contract-summary",
      generatedAt: new Date(),
      status: "ready",
      size: "2.4 MB",
    },
    {
      id: "2", 
      name: "Risk Analysis Report",
      type: "risk-analysis",
      generatedAt: new Date(Date.now() - 24 * 60 * 60 * 1000),
      status: "ready", 
      size: "1.8 MB",
    },
    {
      id: "3",
      name: "Compliance Audit Trail",
      type: "audit-trail",
      generatedAt: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000),
      status: "ready",
      size: "3.1 MB",
    },
  ];

  const handleGenerateReport = () => {
    // In a real app, this would trigger report generation
    console.log("Generating report:", {
      type: reportType,
      dateRange,
      format: exportFormat,
    });
  };

  const handleDownloadReport = (reportId: string) => {
    // In a real app, this would download the report
    console.log("Downloading report:", reportId);
  };

  const reportTypes = [
    { value: "contract-summary", label: "Contract Summary" },
    { value: "risk-analysis", label: "Risk Analysis" },
    { value: "compliance", label: "Compliance Report" },
    { value: "audit-trail", label: "Audit Trail" },
    { value: "financial", label: "Financial Summary" },
    { value: "performance", label: "Performance Metrics" },
  ];

  return (
    <MainLayout 
      title="Reports" 
      description="Generate and manage compliance and analytics reports"
    >
      <div className="space-y-6">
        {/* Report Generation */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              Generate New Report
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div>
                <Label htmlFor="report-type">Report Type</Label>
                <Select value={reportType} onValueChange={setReportType}>
                  <SelectTrigger data-testid="select-report-type">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {reportTypes.map((type) => (
                      <SelectItem key={type.value} value={type.value}>
                        {type.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label>Date Range</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button 
                      variant="outline" 
                      className="w-full justify-start text-left font-normal"
                      data-testid="button-date-range"
                    >
                      <CalendarIcon className="mr-2 h-4 w-4" />
                      {dateRange.from ? (
                        dateRange.to ? (
                          <>
                            {format(dateRange.from, "LLL dd, y")} -{" "}
                            {format(dateRange.to, "LLL dd, y")}
                          </>
                        ) : (
                          format(dateRange.from, "LLL dd, y")
                        )
                      ) : (
                        <span>Pick a date</span>
                      )}
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0" align="start">
                    <Calendar
                      initialFocus
                      mode="range"
                      defaultMonth={dateRange.from}
                      selected={{ from: dateRange.from, to: dateRange.to }}
                      onSelect={(range) => {
                        if (range?.from && range?.to) {
                          setDateRange({ from: range.from, to: range.to });
                        }
                      }}
                      numberOfMonths={2}
                    />
                  </PopoverContent>
                </Popover>
              </div>

              <div>
                <Label htmlFor="export-format">Export Format</Label>
                <Select value={exportFormat} onValueChange={setExportFormat}>
                  <SelectTrigger data-testid="select-export-format">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="pdf">PDF</SelectItem>
                    <SelectItem value="excel">Excel</SelectItem>
                    <SelectItem value="csv">CSV</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="flex items-end">
                <Button 
                  onClick={handleGenerateReport} 
                  className="w-full"
                  data-testid="button-generate-report"
                >
                  <FileDown className="h-4 w-4 mr-2" />
                  Generate Report
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Available Reports */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <BarChart3 className="h-5 w-5" />
              Available Reports
            </CardTitle>
          </CardHeader>
          <CardContent>
            {reports.length === 0 ? (
              <div className="text-center py-8">
                <FileText className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground">No reports generated yet</p>
                <p className="text-sm text-muted-foreground mt-2">
                  Generate your first report to see it here
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {reports.map((report) => (
                  <div 
                    key={report.id}
                    className="flex items-center justify-between p-4 border border-border rounded-lg hover:bg-muted/50 transition-colors"
                    data-testid={`report-item-${report.id}`}
                  >
                    <div className="flex items-center space-x-4">
                      <div className="h-10 w-10 bg-primary/10 rounded-lg flex items-center justify-center">
                        {report.type.includes('excel') || exportFormat === 'excel' ? (
                          <FileSpreadsheet className="h-5 w-5 text-green-600" />
                        ) : (
                          <FileText className="h-5 w-5 text-primary" />
                        )}
                      </div>
                      <div>
                        <h4 className="font-medium">{report.name}</h4>
                        <div className="flex items-center space-x-4 text-sm text-muted-foreground">
                          <span>Generated {format(report.generatedAt, "MMM dd, yyyy")}</span>
                          <span>{report.size}</span>
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-center space-x-2">
                      <Badge 
                        variant={report.status === 'ready' ? 'default' : 'secondary'}
                      >
                        {report.status}
                      </Badge>
                      <Button 
                        variant="ghost" 
                        size="sm"
                        data-testid={`button-view-report-${report.id}`}
                      >
                        <Eye className="h-4 w-4" />
                      </Button>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        onClick={() => handleDownloadReport(report.id)}
                        data-testid={`button-download-report-${report.id}`}
                      >
                        <Download className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Report Templates */}
        <Card>
          <CardHeader>
            <CardTitle>Quick Report Templates</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {[
                {
                  title: "Monthly Summary",
                  description: "Comprehensive overview of contract activities",
                  icon: BarChart3,
                },
                {
                  title: "Risk Assessment",
                  description: "Identify and analyze potential risks",
                  icon: FileText,
                },
                {
                  title: "Compliance Status",
                  description: "Track compliance across all contracts",
                  icon: FileSpreadsheet,
                },
              ].map((template) => (
                <Button
                  key={template.title}
                  variant="outline"
                  className="h-auto p-4 flex-col items-start text-left"
                  data-testid={`template-${template.title.toLowerCase().replace(' ', '-')}`}
                >
                  <template.icon className="h-6 w-6 mb-2" />
                  <h4 className="font-medium">{template.title}</h4>
                  <p className="text-sm text-muted-foreground mt-1">
                    {template.description}
                  </p>
                </Button>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </MainLayout>
  );
}
