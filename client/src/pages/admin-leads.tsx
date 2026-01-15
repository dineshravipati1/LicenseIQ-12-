import { useQuery } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Mail, Building2, Calendar, Star } from "lucide-react";
import { format } from "date-fns";

interface EarlyAccessSignup {
  id: string;
  email: string;
  name: string | null;
  company: string | null;
  createdAt: Date;
}

interface DemoRequest {
  id: string;
  email: string;
  planTier: string;
  createdAt: Date;
}

export default function AdminLeads() {
  const { data: earlyAccessSignups, isLoading: loadingEarlyAccess } = useQuery<EarlyAccessSignup[]>({
    queryKey: ['/api/admin/early-access-signups'],
  });

  const { data: demoRequests, isLoading: loadingDemos } = useQuery<DemoRequest[]>({
    queryKey: ['/api/admin/demo-requests'],
  });

  return (
    <MainLayout
      title="Lead Management"
      description="View and manage early access signups and demo requests"
    >
      <div className="space-y-6">

      <Tabs defaultValue="early-access" className="w-full">
        <TabsList className="grid w-full max-w-md grid-cols-2">
          <TabsTrigger value="early-access" data-testid="tab-early-access">
            Early Access
            {earlyAccessSignups && (
              <Badge variant="secondary" className="ml-2">
                {earlyAccessSignups.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="demo-requests" data-testid="tab-demo-requests">
            Demo Requests
            {demoRequests && (
              <Badge variant="secondary" className="ml-2">
                {demoRequests.length}
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>

        {/* Early Access Signups */}
        <TabsContent value="early-access" className="space-y-4">
          {loadingEarlyAccess ? (
            <Card>
              <CardContent className="p-8 text-center text-slate-600 dark:text-slate-400">
                Loading early access signups...
              </CardContent>
            </Card>
          ) : earlyAccessSignups && earlyAccessSignups.length > 0 ? (
            <div className="grid gap-4">
              {earlyAccessSignups.map((signup) => (
                <Card key={signup.id} data-testid={`card-signup-${signup.id}`}>
                  <CardHeader>
                    <CardTitle className="flex items-start justify-between">
                      <div className="flex items-center space-x-2">
                        <Mail className="h-5 w-5 text-blue-600" />
                        <span className="text-lg">{signup.name || "No Name"}</span>
                      </div>
                      <Badge variant="outline" className="bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300">
                        Early Access
                      </Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <div className="flex items-center space-x-2 text-sm">
                        <Mail className="h-4 w-4 text-slate-500" />
                        <span className="text-slate-900 dark:text-white font-medium" data-testid={`text-email-${signup.id}`}>
                          {signup.email}
                        </span>
                      </div>
                      {signup.company && (
                        <div className="flex items-center space-x-2 text-sm">
                          <Building2 className="h-4 w-4 text-slate-500" />
                          <span className="text-slate-900 dark:text-white" data-testid={`text-company-${signup.id}`}>
                            {signup.company}
                          </span>
                        </div>
                      )}
                      <div className="flex items-center space-x-2 text-sm">
                        <Calendar className="h-4 w-4 text-slate-500" />
                        <span className="text-slate-600 dark:text-slate-400" data-testid={`text-date-${signup.id}`}>
                          {format(new Date(signup.createdAt), 'MMM d, yyyy h:mm a')}
                        </span>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : (
            <Card>
              <CardContent className="p-8 text-center text-slate-600 dark:text-slate-400">
                No early access signups yet.
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* Demo Requests */}
        <TabsContent value="demo-requests" className="space-y-4">
          {loadingDemos ? (
            <Card>
              <CardContent className="p-8 text-center text-slate-600 dark:text-slate-400">
                Loading demo requests...
              </CardContent>
            </Card>
          ) : demoRequests && demoRequests.length > 0 ? (
            <div className="grid gap-4">
              {demoRequests.map((request) => (
                <Card key={request.id} data-testid={`card-demo-${request.id}`}>
                  <CardHeader>
                    <CardTitle className="flex items-start justify-between">
                      <div className="flex items-center space-x-2">
                        <Star className="h-5 w-5 text-teal-600" />
                        <span className="text-lg">{request.email}</span>
                      </div>
                      <Badge 
                        variant="outline" 
                        className={
                          request.planTier === 'licenseiq_plus'
                            ? 'bg-teal-50 dark:bg-teal-950 text-teal-700 dark:text-teal-300'
                            : request.planTier === 'licenseiq_ultra'
                            ? 'bg-purple-50 dark:bg-purple-950 text-purple-700 dark:text-purple-300'
                            : 'bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300'
                        }
                      >
                        {request.planTier === 'licenseiq_plus' 
                          ? 'Plus' 
                          : request.planTier === 'licenseiq_ultra' 
                          ? 'Ultra' 
                          : 'Basic'}
                      </Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="flex items-center space-x-2 text-sm">
                        <Mail className="h-4 w-4 text-slate-500" />
                        <span className="text-slate-900 dark:text-white font-medium" data-testid={`text-demo-email-${request.id}`}>
                          {request.email}
                        </span>
                      </div>
                      <div className="flex items-center space-x-2 text-sm">
                        <Calendar className="h-4 w-4 text-slate-500" />
                        <span className="text-slate-600 dark:text-slate-400" data-testid={`text-demo-date-${request.id}`}>
                          {format(new Date(request.createdAt), 'MMM d, yyyy h:mm a')}
                        </span>
                      </div>
                    </div>
                    <div className="text-sm">
                      <span className="text-slate-600 dark:text-slate-400">Plan Tier: </span>
                      <span className="text-slate-900 dark:text-white font-medium" data-testid={`text-plan-${request.id}`}>
                        {request.planTier}
                      </span>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : (
            <Card>
              <CardContent className="p-8 text-center text-slate-600 dark:text-slate-400">
                No demo requests yet.
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
      </div>
    </MainLayout>
  );
}
