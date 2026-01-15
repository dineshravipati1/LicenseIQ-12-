import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Building2, ChevronDown, MapPin } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";

interface OrgContext {
  id: string;
  companyId: string;
  companyName: string;
  businessUnitId: string | null;
  businessUnitName: string | null;
  locationId: string | null;
  locationName: string | null;
  role: string;
}

export function ContextSwitcher() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [isOpen, setIsOpen] = useState(false);

  // Fetch all available contexts for user
  const { data: contexts = [] } = useQuery<OrgContext[]>({
    queryKey: ['/api/user/org-contexts'],
  });

  // Fetch current active context
  const { data: activeContextData } = useQuery<any>({
    queryKey: ['/api/user/active-context'],
    retry: false,
  });

  const activeContext = activeContextData?.activeContext;

  // Switch context mutation
  const switchContextMutation = useMutation({
    mutationFn: async (orgRoleId: string) => {
      return apiRequest('POST', '/api/user/active-context', { orgRoleId });
    },
    onSuccess: async () => {
      // Invalidate ALL queries to refresh data with new context
      // This includes navigation, contracts, and user data
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['/api/user/active-context'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/navigation/allowed'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/user'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/contracts'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/analytics'] }),
        queryClient.invalidateQueries({ queryKey: ['/api/sales'] }),
      ]);
      
      // Refetch navigation immediately to update sidebar
      await queryClient.refetchQueries({ queryKey: ['/api/navigation/categorized'] });
      
      toast({
        title: "Context Switched",
        description: "Your active location and navigation have been updated.",
      });
      
      setIsOpen(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error?.message || "Failed to switch context",
        variant: "destructive",
      });
    },
  });

  // Format context display string
  const formatContext = (ctx: OrgContext) => {
    const parts = [ctx.companyName];
    if (ctx.businessUnitName) parts.push(ctx.businessUnitName);
    if (ctx.locationName) parts.push(ctx.locationName);
    return `${parts.join(' → ')} [${ctx.role}]`;
  };

  // Format short context for display
  const formatShortContext = (ctx: OrgContext) => {
    if (ctx.locationName) return ctx.locationName;
    if (ctx.businessUnitName) return ctx.businessUnitName;
    return ctx.companyName;
  };

  // Don't show if user has no contexts at all
  if (!contexts || contexts.length === 0) {
    // Still show active context if available (fallback display)
    if (activeContext) {
      return (
        <div className="flex items-center gap-2 px-3 py-1.5 bg-muted/50 rounded-md border" data-testid="context-display">
          <MapPin className="h-4 w-4 text-muted-foreground flex-shrink-0" />
          <span className="text-sm font-medium truncate max-w-[200px]">
            {formatShortContext(activeContext)}
          </span>
          <Badge variant="secondary" className="text-xs capitalize">
            {activeContext.role}
          </Badge>
        </div>
      );
    }
    return null;
  }

  // If user has only one context, show it as a static display (no dropdown)
  if (contexts.length === 1) {
    const ctx = activeContext || contexts[0];
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 bg-muted/50 rounded-md border" data-testid="context-display-single">
        <MapPin className="h-4 w-4 text-primary flex-shrink-0" />
        <div className="flex flex-col">
          <span className="text-sm font-medium truncate max-w-[200px]">
            {ctx.companyName}
            {ctx.businessUnitName && ` → ${ctx.businessUnitName}`}
            {ctx.locationName && ` → ${ctx.locationName}`}
          </span>
        </div>
        <Badge variant="outline" className="text-xs capitalize ml-1">
          {ctx.role}
        </Badge>
      </div>
    );
  }

  // User has multiple contexts - show dropdown switcher
  return (
    <DropdownMenu open={isOpen} onOpenChange={setIsOpen}>
      <DropdownMenuTrigger asChild>
        <Button 
          variant="outline" 
          className="flex items-center gap-2 max-w-xs"
          data-testid="button-context-switcher"
        >
          <MapPin className="h-4 w-4 flex-shrink-0 text-primary" />
          <span className="truncate text-sm">
            {activeContext ? formatContext(activeContext) : 'Select Location'}
          </span>
          <ChevronDown className="h-4 w-4 flex-shrink-0" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-80">
        <DropdownMenuLabel className="flex items-center gap-2">
          <Building2 className="h-4 w-4" />
          Switch Location/Context
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        
        {contexts.map((context) => {
          const isActive = activeContext?.id === context.id;
          
          return (
            <DropdownMenuItem
              key={context.id}
              onClick={() => {
                if (!isActive) {
                  switchContextMutation.mutate(context.id);
                }
              }}
              disabled={isActive || switchContextMutation.isPending}
              className={isActive ? "bg-primary/10 font-medium" : ""}
              data-testid={`context-item-${context.id}`}
            >
              <div className="flex flex-col gap-1 w-full">
                <div className="flex items-center gap-2">
                  <Building2 className="h-4 w-4 text-muted-foreground" />
                  <span className="font-medium">{context.companyName}</span>
                </div>
                {(context.businessUnitName || context.locationName) && (
                  <div className="text-xs text-muted-foreground ml-6">
                    {[context.businessUnitName, context.locationName]
                      .filter(Boolean)
                      .join(' → ')}
                  </div>
                )}
                <div className="text-xs text-muted-foreground ml-6">
                  Role: <span className="font-medium capitalize">{context.role}</span>
                  {isActive && <span className="ml-2 text-primary">● Active</span>}
                </div>
              </div>
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
