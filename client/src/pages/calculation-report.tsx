import { useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ArrowLeft, Download, Printer } from "lucide-react";
import { useLocation, useParams } from "wouter";
import MainLayout from "@/components/layout/main-layout";
import CalculationReportView from "@/components/CalculationReportView";

export default function CalculationReportPage() {
  const { id } = useParams<{ id: string }>();
  const [_, setLocation] = useLocation();
  const calculationId = id || '';

  const { data: calculation, isLoading } = useQuery({
    queryKey: ['/api/calculations', calculationId, 'details'],
    enabled: !!calculationId
  });

  if (isLoading) {
    return (
      <MainLayout title="Calculation Report" description="Loading...">
        <div className="space-y-4">
          <Skeleton className="h-10 w-64" />
          <Skeleton className="h-96 w-full" />
        </div>
      </MainLayout>
    );
  }

  const calc = calculation as any;

  return (
    <MainLayout 
      title="Calculation Report" 
      description={calc?.name || 'Multi-dimensional calculation analysis'}
    >
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <Button 
            variant="outline" 
            onClick={() => setLocation("/calculations")}
            data-testid="button-back-calculations"
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Calculations
          </Button>
          <div className="flex gap-2">
            <Button 
              variant="outline"
              onClick={() => window.open(`/api/royalty-calculations/${calculationId}/invoice/detailed`, '_blank')}
              data-testid="button-download-pdf"
            >
              <Download className="mr-2 h-4 w-4" />
              Download PDF
            </Button>
            <Button 
              variant="outline"
              onClick={() => window.print()}
              data-testid="button-print"
            >
              <Printer className="mr-2 h-4 w-4" />
              Print
            </Button>
          </div>
        </div>

        <CalculationReportView 
          calculationId={calculationId} 
          contractId={calc?.contractId}
        />
      </div>
    </MainLayout>
  );
}
