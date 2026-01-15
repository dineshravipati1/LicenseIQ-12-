import { useLocation } from "wouter";
import { useAuth } from "@/hooks/useAuth";
import { useSidebar } from "@/contexts/sidebar-context";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useQuery } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useState, useRef, useEffect, useLayoutEffect } from "react";
import { 
  File, 
  BarChart3, 
  Upload, 
  TrendingUp, 
  FileText, 
  Users, 
  History,
  Building2,
  Receipt,
  Calculator,
  Brain,
  Database,
  PlayCircle,
  Sparkles,
  ClipboardCheck,
  Mail,
  Layers,
  Table,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  LucideIcon,
  LayoutDashboard,
  Scale,
  ArrowLeftRight,
  BookOpen,
  Bot,
  Home,
  Settings,
  DollarSign
} from "lucide-react";
import logoSymbol from "@assets/Transparent Logo (1)_1763596083942.png";

interface SidebarProps {
  className?: string;
  isOpen?: boolean;
  onClose?: () => void;
}

// Icon mapping from database icon names to actual icon components
const iconMap: Record<string, LucideIcon> = {
  'BarChart3': BarChart3,
  'File': File,
  'Upload': Upload,
  'Receipt': Receipt,
  'Calculator': Calculator,
  'Database': Database,
  'Layers': Layers,
  'Table': Table,
  'Brain': Brain,
  'Sparkles': Sparkles,
  'TrendingUp': TrendingUp,
  'FileText': FileText,
  'Mail': Mail,
  'ClipboardCheck': ClipboardCheck,
  'Users': Users,
  'History': History,
  'Building2': Building2,
  'LayoutDashboard': LayoutDashboard,
  'Scale': Scale,
  'ArrowLeftRight': ArrowLeftRight,
  'BookOpen': BookOpen,
  'Bot': Bot,
  'Home': Home,
  'Settings': Settings,
  'DollarSign': DollarSign,
};

export default function Sidebar({ className, isOpen, onClose }: SidebarProps) {
  const [location, setLocation] = useLocation();
  const { user } = useAuth();
  const { isCollapsed, toggleCollapse, scrollTop, setScrollTop } = useSidebar();
  const navRef = useRef<HTMLElement>(null);

  // Fetch categorized navigation from database
  const { data: categorizedData } = useQuery<{ categories: any[] }>({
    queryKey: ['/api/navigation/categorized'],
    enabled: !!user,
  });

  const userInitials = user?.firstName && user?.lastName 
    ? `${user.firstName[0]}${user.lastName[0]}`.toUpperCase()
    : user?.email?.[0]?.toUpperCase() || 'U';

  // Restore sidebar scroll position from context BEFORE paint
  useLayoutEffect(() => {
    if (navRef.current && scrollTop > 0) {
      navRef.current.scrollTop = scrollTop;
    }
  }, []);

  // Save scroll position to context on unmount
  useEffect(() => {
    return () => {
      if (navRef.current) {
        setScrollTop(navRef.current.scrollTop);
      }
    };
  }, [setScrollTop]);

  const handleNavClick = (href: string) => {
    // Save sidebar scroll position to context before navigation
    if (navRef.current) {
      setScrollTop(navRef.current.scrollTop);
    }
    
    setLocation(href);
    
    if (onClose) {
      onClose();
    }
  };

  const toggleCategory = async (categoryKey: string, currentState: boolean) => {
    try {
      const response = await fetch(`/api/navigation/category-state/${categoryKey}`, {
        method: 'PATCH',
        body: JSON.stringify({ isExpanded: !currentState }),
        headers: { 'Content-Type': 'application/json' },
      });
      if (response.ok) {
        queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
      }
    } catch (error) {
      console.error('Failed to toggle category:', error);
    }
  };

  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div 
          className="fixed inset-0 bg-black/50 z-40 md:hidden"
          onClick={onClose}
          data-testid="sidebar-overlay"
        />
      )}
      
      {/* Sidebar */}
      <aside className={cn(
        "fixed inset-y-0 left-0 bg-sidebar border-r border-sidebar-border shadow-xl sidebar-transition z-50 transform transition-all duration-300 ease-in-out",
        "md:translate-x-0",
        isCollapsed ? "w-16" : "w-64",
        isOpen ? "translate-x-0" : "-translate-x-full",
        className
      )}>
      <div className="flex flex-col h-full">
        {/* Logo & Toggle */}
        <div className="flex items-center justify-center px-3 py-0 border-b border-sidebar-border">
          {!isCollapsed && (
            <div className="flex items-center justify-center w-full relative">
              <img src={logoSymbol} alt="LicenseIQ" className="h-52 w-52 object-contain" />
              <button
                onClick={toggleCollapse}
                className="absolute right-0 p-2 text-sidebar-primary hover:bg-sidebar-accent rounded-md transition-all duration-300"
                data-testid="button-toggle-sidebar"
                aria-label="Collapse sidebar"
              >
                <ChevronLeft className="h-5 w-5" />
              </button>
            </div>
          )}
          {isCollapsed && (
            <button
              onClick={toggleCollapse}
              className="p-2 text-sidebar-primary hover:bg-sidebar-accent rounded-md transition-all duration-300"
              data-testid="button-toggle-sidebar"
              aria-label="Expand sidebar"
            >
              <ChevronRight className="h-5 w-5" />
            </button>
          )}
        </div>
        
        {/* Navigation with Categories */}
        <nav ref={navRef} className="flex-1 px-2 py-2 space-y-1 overflow-y-auto">
          {(categorizedData?.categories || []).map((category: any) => {
            const CategoryIcon = category.iconName ? iconMap[category.iconName] || BarChart3 : BarChart3;
            const isExpanded = category.isExpanded ?? true;

            return (
              <div key={category.categoryKey}>
                {/* Category Header - looks like a regular nav item */}
                {!isCollapsed && (
                  <button
                    onClick={() => category.isCollapsible && toggleCategory(category.categoryKey, isExpanded)}
                    className={cn(
                      "w-full flex items-center justify-between px-3 py-2.5 rounded-lg transition-colors",
                      "text-sidebar-foreground hover:bg-sidebar-accent/50",
                      isExpanded && category.isCollapsible && "bg-sidebar-accent/30"
                    )}
                    data-testid={`category-${category.categoryKey}`}
                  >
                    <div className="flex items-center gap-3 flex-1 min-w-0">
                      <CategoryIcon className="h-5 w-5 flex-shrink-0 text-sidebar-foreground/70" />
                      <span className="text-sm font-medium text-left truncate">{category.categoryName}</span>
                    </div>
                    {category.isCollapsible && (
                      <div className="flex-shrink-0 ml-2">
                        {isExpanded 
                          ? <ChevronDown className="h-4 w-4 text-sidebar-foreground/50" /> 
                          : <ChevronRight className="h-4 w-4 text-sidebar-foreground/50" />
                        }
                      </div>
                    )}
                  </button>
                )}

                {/* Collapsed sidebar - show category icon */}
                {isCollapsed && (
                  <button
                    onClick={() => category.isCollapsible && toggleCategory(category.categoryKey, isExpanded)}
                    className={cn(
                      "w-full flex items-center justify-center px-2 py-2.5 rounded-lg transition-colors",
                      "text-sidebar-foreground hover:bg-sidebar-accent/50"
                    )}
                    data-testid={`category-${category.categoryKey}`}
                    title={category.categoryName}
                  >
                    <CategoryIcon className="h-5 w-5 text-sidebar-foreground/70" />
                  </button>
                )}

                {/* Category Items - indented when expanded */}
                {isExpanded && !isCollapsed && (
                  <div className="mt-1 ml-6 space-y-0.5 border-l-2 border-sidebar-foreground/20 pl-4">
                    {category.items.map((item: any) => {
                      const Icon = item.iconName ? iconMap[item.iconName] || BarChart3 : BarChart3;
                      const isCurrent = location === item.href || 
                                       (item.href !== "/" && location.startsWith(item.href)) ||
                                       (item.href === "/calculations" && location.startsWith("/royalty-dashboard/"));

                      return (
                        <button
                          key={item.itemKey}
                          className={cn(
                            "w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-colors text-sm",
                            "text-sidebar-foreground/80 hover:bg-sidebar-accent/40",
                            isCurrent && "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                          )}
                          onClick={() => handleNavClick(item.href)}
                          data-testid={`nav-${item.itemName.toLowerCase().replace(/\s+/g, '-')}`}
                        >
                          <span>{item.itemName}</span>
                        </button>
                      );
                    })}
                  </div>
                )}

                {/* Collapsed sidebar - show all items */}
                {isCollapsed && (
                  <div className="space-y-0.5">
                    {category.items.map((item: any) => {
                      const Icon = item.iconName ? iconMap[item.iconName] || BarChart3 : BarChart3;
                      const isCurrent = location === item.href || 
                                       (item.href !== "/" && location.startsWith(item.href)) ||
                                       (item.href === "/calculations" && location.startsWith("/royalty-dashboard/"));

                      return (
                        <button
                          key={item.itemKey}
                          className={cn(
                            "w-full flex items-center justify-center px-2 py-2.5 rounded-lg transition-colors",
                            "text-sidebar-foreground/80 hover:bg-sidebar-accent/50",
                            isCurrent && "bg-sidebar-accent text-sidebar-accent-foreground"
                          )}
                          onClick={() => handleNavClick(item.href)}
                          data-testid={`nav-${item.itemName.toLowerCase().replace(/\s+/g, '-')}`}
                          title={item.itemName}
                        >
                          <Icon className="h-5 w-5" />
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </nav>
      </div>
    </aside>
    </>
  );
}
