import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Bell, Plus, Menu, User, LogOut, Settings, Sun, Moon, Monitor, Check, Palette } from "lucide-react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/useAuth";
import { useTheme, THEME_OPTIONS, ThemeName } from "@/contexts/theme-context";
import GlobalSearch from "@/components/ui/global-search";
import { ContextSwitcher } from "@/components/context-switcher";

interface HeaderProps {
  title: string;
  description?: string;
  onMenuClick?: () => void;
  actions?: React.ReactNode;
}

export default function Header({ title, description, onMenuClick, actions }: HeaderProps) {
  const [, setLocation] = useLocation();
  const { user, logoutMutation } = useAuth();
  const { theme, setTheme } = useTheme();

  const handleNewContract = () => {
    setLocation("/upload");
  };

  const handleLogout = () => {
    logoutMutation.mutate();
  };

  const handleSettings = () => {
    setLocation("/configuration");
  };

  // Hide New Contract button on Analytics page since it's already in main dashboard
  const showNewContractButton = title !== "Enterprise Analytics";

  const userInitials = user?.firstName && user?.lastName 
    ? `${user.firstName[0]}${user.lastName[0]}`.toUpperCase()
    : user?.email?.[0]?.toUpperCase() || 'U';

  const currentTheme = THEME_OPTIONS.find(t => t.name === theme);
  
  const handleThemeChange = (themeName: ThemeName) => {
    setTheme(themeName);
  };

  return (
    <header className="bg-card border-b border-border px-4 md:px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Mobile Menu Button */}
          <Button
            variant="ghost"
            size="sm"
            className="md:hidden"
            onClick={onMenuClick}
            data-testid="button-mobile-menu"
          >
            <Menu className="h-5 w-5" />
          </Button>
          
          <div>
            <h2 className="text-xl md:text-2xl font-semibold text-foreground">{title}</h2>
            {description && (
              <p className="text-sm text-muted-foreground hidden sm:block">{description}</p>
            )}
          </div>
        </div>
        <div className="flex items-center space-x-2 md:space-x-4">
          {/* Global Search */}
          <GlobalSearch />
          
          {/* Custom Actions */}
          {actions && (
            <div className="flex items-center gap-2">
              {actions}
            </div>
          )}
          
          <Button
            variant="ghost"
            size="sm"
            className="relative"
            data-testid="button-notifications"
          >
            <Bell className="h-4 w-4" />
            <span className="absolute top-0 right-0 h-2 w-2 bg-destructive rounded-full" />
          </Button>
          {!actions && showNewContractButton && (
            <Button onClick={handleNewContract} data-testid="button-header-new-contract" size="sm" className="md:size-default">
              <Plus className="h-4 w-4 md:mr-2" />
              <span className="hidden md:inline">New Contract</span>
            </Button>
          )}
          
          {/* Context/Location Switcher */}
          <ContextSwitcher />
          
          {/* User Profile Dropdown */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button 
                variant="ghost" 
                size="icon"
                className="relative h-10 w-10 rounded-full p-0 hover:bg-transparent"
                data-testid="button-user-profile"
              >
                <div className="h-10 w-10 bg-blue-600 rounded-full flex items-center justify-center shadow-md">
                  <span className="text-sm font-semibold text-white">
                    {userInitials}
                  </span>
                </div>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56">
              <DropdownMenuLabel>
                <div className="flex flex-col space-y-1">
                  <p className="text-sm font-medium leading-none">
                    {user?.firstName && user?.lastName 
                      ? `${user.firstName} ${user.lastName}`
                      : user?.email
                    }
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {user?.email}
                  </p>
                  <p className="text-xs text-muted-foreground capitalize">
                    {user?.role || 'User'}
                  </p>
                </div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              {(user?.role === 'admin' || user?.role === 'owner') && (
                <DropdownMenuItem onClick={handleSettings} data-testid="menu-settings">
                  <Settings className="mr-2 h-4 w-4" />
                  <span>Settings</span>
                </DropdownMenuItem>
              )}
              <DropdownMenuSeparator />
              <DropdownMenuLabel className="flex items-center gap-2 text-xs text-muted-foreground">
                <Palette className="h-3 w-3" />
                <span>Choose Theme</span>
              </DropdownMenuLabel>
              <div className="max-h-[400px] overflow-y-auto px-1">
                <div className="grid grid-cols-1 gap-1 py-1">
                  {THEME_OPTIONS.map((themeOption) => (
                    <DropdownMenuItem
                      key={themeOption.name}
                      onClick={() => handleThemeChange(themeOption.name)}
                      className="cursor-pointer hover:bg-accent p-2"
                      data-testid={`menu-theme-${themeOption.name}`}
                    >
                      <div className="flex items-center gap-3 w-full">
                        <div className={`h-8 w-8 rounded-md bg-gradient-to-br ${themeOption.preview} flex-shrink-0 border border-border`} />
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium truncate">{themeOption.label}</p>
                          <p className="text-xs text-muted-foreground truncate">{themeOption.description}</p>
                        </div>
                        {theme === themeOption.name && (
                          <Check className="h-4 w-4 text-primary flex-shrink-0" />
                        )}
                      </div>
                    </DropdownMenuItem>
                  ))}
                </div>
              </div>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleLogout} data-testid="menu-logout">
                <LogOut className="mr-2 h-4 w-4 text-red-500" />
                <span>Log out</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  );
}
