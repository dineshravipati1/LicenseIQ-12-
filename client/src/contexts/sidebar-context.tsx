import { createContext, useContext, useState, useEffect, ReactNode, useRef } from 'react';

interface SidebarContextType {
  isCollapsed: boolean;
  toggleCollapse: () => void;
  scrollTop: number;
  setScrollTop: (value: number) => void;
}

const SidebarContext = createContext<SidebarContextType | undefined>(undefined);

export function SidebarProvider({ children }: { children: ReactNode }) {
  const [isCollapsed, setIsCollapsed] = useState(() => {
    const saved = localStorage.getItem('sidebar-collapsed');
    return saved === 'true';
  });
  
  const scrollTopRef = useRef(0);

  useEffect(() => {
    localStorage.setItem('sidebar-collapsed', String(isCollapsed));
  }, [isCollapsed]);

  const toggleCollapse = () => {
    setIsCollapsed(!isCollapsed);
  };
  
  const setScrollTop = (value: number) => {
    scrollTopRef.current = value;
  };

  return (
    <SidebarContext.Provider value={{ 
      isCollapsed, 
      toggleCollapse, 
      scrollTop: scrollTopRef.current,
      setScrollTop
    }}>
      {children}
    </SidebarContext.Provider>
  );
}

export function useSidebar() {
  const context = useContext(SidebarContext);
  if (context === undefined) {
    throw new Error('useSidebar must be used within a SidebarProvider');
  }
  return context;
}
