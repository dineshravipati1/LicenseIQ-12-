import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Search, FileText, Upload, Receipt, Calculator, Database, Layers, Table, Building2, Brain, Sparkles, TrendingUp, Users, History, Settings, Mail, ClipboardCheck, BarChart3, File, Loader2, ArrowRight, Eye, Edit, Plus, Download, Trash2, List } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";

interface NavigationItem {
  itemKey: string;
  itemName: string;
  href: string;
  iconName: string;
  sortOrder: number;
}

interface SearchSubItem {
  label: string;
  href: string;
  icon: string;
  badge?: string;
}

interface EnhancedSearchItem extends NavigationItem {
  subItems?: SearchSubItem[];
}

const iconMap: Record<string, any> = {
  BarChart3,
  File,
  Upload,
  Receipt,
  Calculator,
  Database,
  Layers,
  Table,
  Building2,
  Brain,
  Sparkles,
  TrendingUp,
  FileText,
  Mail,
  ClipboardCheck,
  Users,
  History,
  Settings,
  Eye,
  Edit,
  Plus,
  Download,
  Trash2,
  List,
};

// Define sub-items for each main navigation item
const getSubItems = (itemKey: string, mainHref: string): SearchSubItem[] => {
  const subItemsMap: Record<string, SearchSubItem[]> = {
    'contracts': [
      { label: 'View All Contracts', href: '/contracts', icon: 'List' },
      { label: 'Upload New Contract', href: '/contracts/upload', icon: 'Upload', badge: 'New' },
      { label: 'View Contract', href: '/contracts', icon: 'Eye' },
      { label: 'Edit Contract', href: '/contracts', icon: 'Edit' },
    ],
    'sales-data': [
      { label: 'View Sales Data', href: '/sales-upload', icon: 'List' },
      { label: 'Upload Sales Data', href: '/sales-upload', icon: 'Upload', badge: 'New' },
      { label: 'Download Template', href: '/sales-upload', icon: 'Download' },
    ],
    'payment-runs': [
      { label: 'View All Payments', href: '/payment-runs', icon: 'List' },
      { label: 'Create Payment Run', href: '/payment-runs/new', icon: 'Plus', badge: 'New' },
      { label: 'View History', href: '/payment-runs/history', icon: 'History' },
    ],
    'royalty-calculator': [
      { label: 'Calculate License Fee', href: '/calculations', icon: 'Calculator' },
      { label: 'View Saved Calculations', href: '/calculations', icon: 'List' },
    ],
    'master-data': [
      { label: 'View Master Data', href: '/master-data', icon: 'List' },
      { label: 'Manage Companies', href: '/master-data/companies', icon: 'Building2' },
      { label: 'Manage Products', href: '/master-data/products', icon: 'Database' },
      { label: 'Manage Territories', href: '/master-data/territories', icon: 'Database' },
    ],
    'rules': [
      { label: 'View All Rules', href: '/rules', icon: 'List' },
      { label: 'Create New Rule', href: '/rules/new', icon: 'Plus', badge: 'New' },
      { label: 'Edit Rule', href: '/rules', icon: 'Edit' },
    ],
    'erp-catalog': [
      { label: 'View ERP Systems', href: '/erp-catalog', icon: 'List' },
      { label: 'Configure ERP', href: '/erp-catalog', icon: 'Settings' },
    ],
    'contract-qna': [
      { label: 'Ask Questions', href: '/contract-qna', icon: 'Brain' },
      { label: 'View Chat History', href: '/contract-qna', icon: 'History' },
    ],
    'analytics': [
      { label: 'View Dashboard', href: '/analytics', icon: 'BarChart3' },
      { label: 'Revenue Trends', href: '/analytics', icon: 'TrendingUp' },
      { label: 'Contract Analytics', href: '/analytics', icon: 'FileText' },
    ],
    'user-management': [
      { label: 'View All Users', href: '/user-management', icon: 'List' },
      { label: 'Add New User', href: '/user-management', icon: 'Plus', badge: 'New' },
      { label: 'Manage Roles', href: '/user-management', icon: 'Settings' },
    ],
    'audit-trail': [
      { label: 'View Audit Logs', href: '/audit-trail', icon: 'List' },
      { label: 'Filter by User', href: '/audit-trail', icon: 'Users' },
      { label: 'Filter by Date', href: '/audit-trail', icon: 'History' },
    ],
  };

  return subItemsMap[itemKey] || [];
};

export default function GlobalSearch() {
  const [open, setOpen] = useState(false);
  const [, setLocation] = useLocation();
  const [searchQuery, setSearchQuery] = useState("");
  const { toast } = useToast();
  const inputRef = useRef<HTMLInputElement>(null);

  const { data: navigationData, isLoading, isError } = useQuery<{ items: NavigationItem[] }>({
    queryKey: ['/api/navigation/allowed'],
  });

  const navigationItems = navigationData?.items || [];

  // Enhance navigation items with sub-items
  const enhancedItems: EnhancedSearchItem[] = navigationItems.map(item => ({
    ...item,
    subItems: getSubItems(item.itemKey, item.href),
  }));

  // Show error toast if navigation fetch fails
  useEffect(() => {
    if (isError) {
      toast({
        title: "Search Error",
        description: "Failed to load navigation items. Please try again.",
        variant: "destructive",
      });
    }
  }, [isError, toast]);

  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      // Ignore if user is typing in an input, textarea, or contenteditable
      const target = e.target as HTMLElement;
      const isTyping = 
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.isContentEditable;

      if (e.key === "k" && (e.metaKey || e.ctrlKey) && !isTyping) {
        e.preventDefault();
        setOpen(true);
        setTimeout(() => inputRef.current?.focus(), 100);
      }

      // Close on Escape
      if (e.key === "Escape" && open) {
        setOpen(false);
        setSearchQuery("");
      }
    };

    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [open]);

  // Filter main items and their sub-items
  const filteredItems = enhancedItems
    .map(item => {
      const query = searchQuery.toLowerCase();
      const itemMatches = !searchQuery || 
        item.itemName.toLowerCase().includes(query) ||
        item.itemKey.toLowerCase().includes(query) ||
        item.href.toLowerCase().includes(query);

      // Filter sub-items
      const filteredSubItems = item.subItems?.filter(subItem => 
        !searchQuery || subItem.label.toLowerCase().includes(query)
      ) || [];

      // Include item if main item matches or any sub-item matches
      if (itemMatches || filteredSubItems.length > 0) {
        return {
          ...item,
          subItems: itemMatches ? item.subItems : filteredSubItems,
        };
      }
      return null;
    })
    .filter(Boolean) as EnhancedSearchItem[];

  const handleSelect = (href: string) => {
    setOpen(false);
    setSearchQuery("");
    setLocation(href);
  };

  const getIcon = (iconName: string) => {
    const Icon = iconMap[iconName] || Search;
    return Icon;
  };

  const totalResults = filteredItems.reduce((acc, item) => acc + 1 + (item.subItems?.length || 0), 0);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className="relative h-9 w-9 p-0 md:h-10 md:w-64 md:justify-start md:px-3 md:py-2"
          onClick={() => setOpen(true)}
          data-testid="button-global-search"
        >
          <Search className="h-4 w-4 md:mr-2" />
          <span className="hidden md:inline-flex text-sm text-muted-foreground">
            Search...
          </span>
          <kbd className="pointer-events-none absolute right-1.5 top-2 hidden h-6 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium opacity-100 md:flex">
            <span className="text-xs">⌘</span>K
          </kbd>
        </Button>
      </PopoverTrigger>

      <PopoverContent 
        className="w-[600px] p-0" 
        align="start"
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          inputRef.current?.focus();
        }}
      >
        {/* Search Input */}
        <div className="flex items-center border-b px-4 py-3 bg-muted/30">
          <Search className="h-4 w-4 text-muted-foreground mr-2" />
          <Input
            ref={inputRef}
            placeholder="Search pages, features, and actions..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="border-0 focus-visible:ring-0 focus-visible:ring-offset-0 h-9 text-sm bg-transparent"
            data-testid="input-global-search"
          />
          {searchQuery && (
            <div className="flex items-center gap-2 ml-2">
              <Badge variant="secondary" className="text-xs">
                {totalResults} {totalResults === 1 ? 'result' : 'results'}
              </Badge>
              <kbd className="h-5 select-none items-center gap-1 rounded border bg-background px-1.5 font-mono text-[10px] font-medium text-muted-foreground hidden md:inline-flex">
                ESC
              </kbd>
            </div>
          )}
        </div>

        {/* Results */}
        <ScrollArea className="max-h-[500px]">
          {isLoading ? (
            <div className="flex items-center justify-center p-12">
              <Loader2 className="h-6 w-6 animate-spin text-blue-600" />
              <span className="ml-2 text-sm text-muted-foreground">Loading...</span>
            </div>
          ) : isError ? (
            <div className="flex items-center justify-center p-12 text-sm text-destructive">
              Failed to load navigation
            </div>
          ) : filteredItems.length === 0 ? (
            <div className="flex flex-col items-center justify-center p-12 text-center">
              <Search className="h-12 w-12 text-muted-foreground/50 mb-3" />
              <p className="text-sm font-medium text-foreground mb-1">No results found</p>
              <p className="text-xs text-muted-foreground">Try searching for contracts, sales, or payments</p>
            </div>
          ) : (
            <div className="p-2">
              {filteredItems.map((item) => {
                const Icon = getIcon(item.iconName);
                return (
                  <div key={item.itemKey} className="mb-1">
                    {/* Main Item */}
                    <button
                      onClick={() => handleSelect(item.href)}
                      className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-accent/50 transition-all text-left group"
                      data-testid={`search-result-${item.itemKey}`}
                    >
                      <div className="flex items-center justify-center w-9 h-9 rounded-lg bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-950/50 dark:to-blue-900/30 group-hover:from-blue-100 group-hover:to-blue-200 dark:group-hover:from-blue-900/50 dark:group-hover:to-blue-800/40 transition-all">
                        <Icon className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-semibold text-foreground group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                          {item.itemName}
                        </p>
                        <p className="text-xs text-muted-foreground truncate">
                          {item.href}
                        </p>
                      </div>
                      <ArrowRight className="h-4 w-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                    </button>

                    {/* Sub-Items */}
                    {item.subItems && item.subItems.length > 0 && (
                      <div className="ml-12 mt-1 space-y-0.5">
                        {item.subItems.map((subItem, idx) => {
                          const SubIcon = getIcon(subItem.icon);
                          return (
                            <button
                              key={idx}
                              onClick={() => handleSelect(subItem.href)}
                              className="w-full flex items-center gap-2.5 px-3 py-2 rounded-md hover:bg-accent/80 transition-all text-left group/sub"
                              data-testid={`search-sub-${item.itemKey}-${idx}`}
                            >
                              <SubIcon className="h-3.5 w-3.5 text-muted-foreground group-hover/sub:text-blue-500 transition-colors" />
                              <span className="text-xs text-muted-foreground group-hover/sub:text-foreground transition-colors flex-1">
                                {subItem.label}
                              </span>
                              {subItem.badge && (
                                <Badge variant="default" className="text-[10px] h-4 px-1.5 bg-blue-600">
                                  {subItem.badge}
                                </Badge>
                              )}
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </ScrollArea>

        {/* Footer with keyboard shortcuts */}
        {!isLoading && !isError && filteredItems.length > 0 && (
          <div className="border-t px-4 py-2.5 bg-muted/20 flex items-center justify-between text-xs text-muted-foreground">
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-1">
                <kbd className="px-1.5 py-0.5 rounded border bg-background font-mono text-[10px]">↑↓</kbd>
                <span>Navigate</span>
              </div>
              <div className="flex items-center gap-1">
                <kbd className="px-1.5 py-0.5 rounded border bg-background font-mono text-[10px]">Enter</kbd>
                <span>Open</span>
              </div>
            </div>
            <div className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 rounded border bg-background font-mono text-[10px]">ESC</kbd>
              <span>Close</span>
            </div>
          </div>
        )}
      </PopoverContent>
    </Popover>
  );
}
