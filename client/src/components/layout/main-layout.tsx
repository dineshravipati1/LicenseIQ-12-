import { useState } from "react";
import { useSidebar } from "@/contexts/sidebar-context";
import { cn } from "@/lib/utils";
import Sidebar from "./sidebar";
import Header from "./header";
import licenseIQLogo from "@assets/Transparent Logo_1761867914841.png";

interface MainLayoutProps {
  children: React.ReactNode;
  title: string;
  description?: string;
  actions?: React.ReactNode;
}

export default function MainLayout({ children, title, description, actions }: MainLayoutProps) {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const { isCollapsed } = useSidebar();

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Sidebar 
        isOpen={isSidebarOpen} 
        onClose={() => setIsSidebarOpen(false)} 
      />
      <main className={cn(
        "flex-1 flex flex-col transition-all duration-300",
        isCollapsed ? "md:ml-16" : "md:ml-64"
      )}>
        <Header 
          title={title} 
          description={description}
          onMenuClick={() => setIsSidebarOpen(true)}
          actions={actions}
        />
        <div className="p-4 md:p-6 flex-1" data-testid="main-content">
          {children}
        </div>
        
        {/* Footer */}
        <footer className="border-t bg-background/50 px-4 md:px-6 py-4">
          <div className="flex flex-col md:flex-row items-center justify-between gap-3 text-xs text-muted-foreground">
            <div className="flex flex-col md:flex-row items-center md:space-x-4 gap-2 md:gap-0">
              <div className="flex items-center space-x-2">
                <img src={licenseIQLogo} alt="LicenseIQ Logo" className="h-16 md:h-24" />
              </div>
              <span className="hidden md:inline">•</span>
              <p className="text-center md:text-left">&copy; 2025 LicenseIQ. All rights reserved.</p>
            </div>
            <p className="text-center md:text-right">Agentic AI for Financial Contracts</p>
          </div>
        </footer>
      </main>
    </div>
  );
}
