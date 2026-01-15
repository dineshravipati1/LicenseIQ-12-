import { createContext, useContext, useEffect, useState, ReactNode } from "react";

export type ThemeName = 
  | "light" 
  | "dark" 
  | "blue-ocean" 
  | "forest" 
  | "sunset" 
  | "purple-dream" 
  | "rose-gold" 
  | "midnight" 
  | "emerald" 
  | "mocha" 
  | "arctic"
  | "crimson"
  | "system";

export interface ThemeOption {
  name: ThemeName;
  label: string;
  description: string;
  preview: string; // gradient for preview
  isDark: boolean;
}

export const THEME_OPTIONS: ThemeOption[] = [
  { name: "light", label: "Light", description: "Clean & bright", preview: "from-slate-100 to-slate-200", isDark: false },
  { name: "dark", label: "Dark", description: "Easy on eyes", preview: "from-slate-800 to-slate-900", isDark: true },
  { name: "blue-ocean", label: "Blue Ocean", description: "Calm & professional", preview: "from-blue-400 to-cyan-500", isDark: false },
  { name: "forest", label: "Forest", description: "Natural & refreshing", preview: "from-green-600 to-emerald-700", isDark: true },
  { name: "sunset", label: "Sunset", description: "Warm & energetic", preview: "from-orange-400 to-pink-500", isDark: false },
  { name: "purple-dream", label: "Purple Dream", description: "Creative & elegant", preview: "from-purple-500 to-pink-600", isDark: true },
  { name: "rose-gold", label: "Rose Gold", description: "Sophisticated & modern", preview: "from-rose-300 to-amber-400", isDark: false },
  { name: "midnight", label: "Midnight", description: "Deep & focused", preview: "from-indigo-900 to-blue-950", isDark: true },
  { name: "emerald", label: "Emerald", description: "Fresh & vibrant", preview: "from-emerald-400 to-teal-500", isDark: false },
  { name: "mocha", label: "Mocha", description: "Cozy & warm", preview: "from-amber-700 to-stone-800", isDark: true },
  { name: "arctic", label: "Arctic", description: "Cool & crisp", preview: "from-cyan-100 to-blue-200", isDark: false },
  { name: "crimson", label: "Crimson", description: "Bold & powerful", preview: "from-red-600 to-rose-700", isDark: true },
  { name: "system", label: "System", description: "Match OS theme", preview: "from-gray-400 to-gray-600", isDark: false },
];

interface ThemeContextType {
  theme: ThemeName;
  setTheme: (theme: ThemeName) => void;
  isDarkMode: boolean;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<ThemeName>(() => {
    const saved = localStorage.getItem('theme') as ThemeName;
    return saved || 'light';
  });

  const [isDarkMode, setIsDarkMode] = useState(false);

  useEffect(() => {
    localStorage.setItem('theme', theme);
    
    const root = document.documentElement;
    
    // Remove all theme classes first
    const allThemes = THEME_OPTIONS.map(t => t.name).filter(t => t !== 'system');
    root.classList.remove(...allThemes, 'dark');
    
    if (theme === 'system') {
      const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      setIsDarkMode(systemPrefersDark);
      if (systemPrefersDark) {
        root.classList.add('dark');
      }
    } else {
      const themeOption = THEME_OPTIONS.find(t => t.name === theme);
      const isDark = themeOption?.isDark || false;
      setIsDarkMode(isDark);
      
      root.classList.add(theme);
      if (isDark) {
        root.classList.add('dark');
      }
    }
  }, [theme]);

  useEffect(() => {
    if (theme === 'system') {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      const handleChange = (e: MediaQueryListEvent) => {
        const systemPrefersDark = e.matches;
        setIsDarkMode(systemPrefersDark);
        const root = document.documentElement;
        if (systemPrefersDark) {
          root.classList.add('dark');
        } else {
          root.classList.remove('dark');
        }
      };
      
      mediaQuery.addEventListener('change', handleChange);
      return () => mediaQuery.removeEventListener('change', handleChange);
    }
  }, [theme]);

  return (
    <ThemeContext.Provider value={{ theme, setTheme, isDarkMode }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}
