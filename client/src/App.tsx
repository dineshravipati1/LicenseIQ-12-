import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { SidebarProvider } from "@/contexts/sidebar-context";
import { ThemeProvider } from "@/contexts/theme-context";
import { useAuth } from "@/hooks/useAuth";
import { ProtectedRoute } from "@/lib/protected-route";
import { FloatingAIAssistant } from "@/components/floating-ai-assistant";
import Landing from "@/pages/landing";
import AuthPage from "@/pages/auth-page";
import Dashboard from "@/pages/dashboard";
import Contracts from "@/pages/contracts";
import Upload from "@/pages/upload";
import Analytics from "@/pages/analytics";
import Reports from "@/pages/reports";
import Users from "@/pages/users";
import CreateUser from "@/pages/create-user";
import Audit from "@/pages/audit";
import ContractAnalysis from "@/pages/contract-analysis";
import NotFound from "@/pages/not-found";
import RoyaltyDashboard from "@/pages/royalty-dashboard";
import SalesUpload from "@/pages/sales-upload";
import RulesManagement from "@/pages/rules-management";
import ContractQnA from "@/pages/contract-qna";
import RAGDashboard from "@/pages/rag-dashboard";
import HumanReviewQueue from "@/pages/HumanReviewQueue";
import AdminLeads from "@/pages/admin-leads";
import CalculationsPage from "@/pages/calculations";
import CalculationReportPage from "@/pages/calculation-report";
import ContractManagement from "@/pages/contract-management";
import MasterDataMapping from "@/pages/master-data-mapping";
import ErpCatalog from "@/pages/erp-catalog";
import ErpDataImport from "@/pages/erp-data-import";
import LicenseiqSchema from "@/pages/licenseiq-schema";
import DataManagement from "@/pages/data-management";
import Configuration from "@/pages/configuration";
import MasterData from "@/pages/master-data";
import NavigationManager from "@/pages/navigation-manager";
import KnowledgeBase from "@/pages/knowledge-base";
import ErpIntegration from "@/pages/erp-integration";
import ErpHub from "@/pages/erp-hub";
import MasterDataItems from "@/pages/master-data-items";
import ErpMappingRules from "@/pages/erp-mapping-rules";
import RulesWorkspace from "@/pages/rules-workspace";

function Router() {
  const { isAuthenticated, isLoading } = useAuth();

  return (
    <>
      <Switch>
        {/* Public routes */}
        <Route path="/auth" component={AuthPage} />
        
        {/* Loading or unauthenticated users see landing page */}
        {isLoading || !isAuthenticated ? (
          <Route path="/" component={Landing} />
        ) : (
          <SidebarProvider>
            {/* Protected routes for authenticated users */}
            <ProtectedRoute path="/" component={Dashboard} />
            <ProtectedRoute path="/contracts" component={Contracts} />
            <ProtectedRoute path="/upload" component={Upload} />
            <ProtectedRoute path="/analytics" component={Analytics} />
            <ProtectedRoute path="/reports" component={Reports} />
            <ProtectedRoute path="/users" component={Users} />
            <ProtectedRoute path="/users/new" component={CreateUser} />
            <ProtectedRoute path="/audit" component={Audit} />
            {/* License Fee Calculations */}
            <ProtectedRoute path="/sales-upload" component={SalesUpload} />
            <ProtectedRoute path="/calculations/:id/report" component={CalculationReportPage} />
            <ProtectedRoute path="/calculations" component={CalculationsPage} />
            <ProtectedRoute path="/royalty-dashboard/:id" component={RoyaltyDashboard} />
            <ProtectedRoute path="/contracts/:id/rules" component={RulesManagement} />
            <ProtectedRoute path="/rules/:id" component={RulesManagement} />
            <ProtectedRoute path="/rules-workspace" component={RulesWorkspace} />
            {/* liQ AI */}
            <ProtectedRoute path="/contract-qna" component={ContractQnA} />
            <ProtectedRoute path="/rag-dashboard" component={RAGDashboard} />
            {/* Dynamic Extraction */}
            <ProtectedRoute path="/review-queue" component={HumanReviewQueue} />
            {/* Admin */}
            <ProtectedRoute path="/admin/leads" component={AdminLeads} />
            {/* ERP Integration */}
            <ProtectedRoute path="/erp-hub" component={ErpHub} />
            <ProtectedRoute path="/erp-mapping-rules" component={ErpMappingRules} />
            <ProtectedRoute path="/master-data-mapping" component={MasterDataMapping} />
            <ProtectedRoute path="/erp-catalog" component={ErpCatalog} />
            <ProtectedRoute path="/erp-import" component={ErpDataImport} />
            <ProtectedRoute path="/erp-integration" component={ErpIntegration} />
            <ProtectedRoute path="/licenseiq-schema" component={LicenseiqSchema} />
            <ProtectedRoute path="/data-management" component={DataManagement} />
            <ProtectedRoute path="/master-data" component={MasterData} />
            <ProtectedRoute path="/master-data/items" component={MasterDataItems} />
            <ProtectedRoute path="/configuration" component={Configuration} />
            <ProtectedRoute path="/navigation-manager" component={NavigationManager} />
            <ProtectedRoute path="/knowledge-base" component={KnowledgeBase} />
            {/* Contract Management - must come before /contracts/:id */}
            <ProtectedRoute path="/contracts/:id/manage" component={ContractManagement} />
            <ProtectedRoute path="/contracts/:id" component={ContractAnalysis} />
            
            {/* Floating AI Assistant - Omnipresent across all authenticated pages */}
            <FloatingAIAssistant />
          </SidebarProvider>
        )}
        
        <Route component={NotFound} />
      </Switch>
    </>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider>
          <Toaster />
          <Router />
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;
