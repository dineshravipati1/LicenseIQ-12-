import { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import MainLayout from "@/components/layout/main-layout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/hooks/use-toast";
import { DndContext, DragEndEvent, DragOverlay, DragStartEvent, PointerSensor, useSensor, useSensors, closestCorners } from "@dnd-kit/core";
import { SortableContext, arrayMove, verticalListSortingStrategy, rectSortingStrategy, useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { 
  BarChart3, File, Upload, Receipt, Calculator, Database, Layers, Table, 
  Brain, Sparkles, TrendingUp, FileText, Mail, ClipboardCheck, Users, 
  History, Building2, Plus, GripVertical, Trash2, Edit, Save, RotateCcw
} from "lucide-react";
import { cn } from "@/lib/utils";

const iconMap: Record<string, any> = {
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
};

const availableIcons = Object.keys(iconMap);

interface NavigationItem {
  itemKey: string;
  itemName: string;
  href: string;
  iconName: string;
}

interface Category {
  id: string;
  categoryKey: string;
  categoryName: string;
  iconName: string | null;
  defaultSortOrder: number;
  isCollapsible: boolean;
  defaultExpanded: boolean;
  items: NavigationItem[];
}

interface SortableItemProps {
  item: NavigationItem;
  categoryKey: string;
  index: number;
}

function SortableItem({ item, categoryKey, index }: SortableItemProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ 
    id: item.itemKey,
    data: { categoryKey, item, index }
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  const Icon = item.iconName ? iconMap[item.iconName] || File : File;

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={cn(
        "flex items-center gap-3 p-3 bg-white dark:bg-gray-800 border rounded-lg",
        isDragging && "opacity-50"
      )}
      data-testid={`draggable-item-${item.itemKey}`}
    >
      <div
        {...attributes}
        {...listeners}
        className="cursor-grab active:cursor-grabbing"
        data-testid={`drag-handle-${item.itemKey}`}
      >
        <GripVertical className="h-4 w-4 text-gray-400" />
      </div>
      <Icon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
      <div className="flex-1">
        <div className="font-medium text-sm">{item.itemName}</div>
        <div className="text-xs text-gray-500">{item.href}</div>
      </div>
    </div>
  );
}

interface CategoryCardProps {
  category: Category;
  onEditCategory: (category: Category) => void;
  onDeleteCategory: (categoryKey: string) => void;
}

function CategoryCard({ category, onEditCategory, onDeleteCategory }: CategoryCardProps) {
  const CategoryIcon = category.iconName ? iconMap[category.iconName] || BarChart3 : BarChart3;
  
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id: `category-${category.categoryKey}`,
    data: { categoryKey: category.categoryKey, isCategory: true }
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <Card 
      ref={setNodeRef} 
      style={style}
      className={cn(isDragging && "opacity-50 shadow-2xl")}
      data-testid={`category-card-${category.categoryKey}`}
    >
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div
              {...attributes}
              {...listeners}
              className="cursor-grab active:cursor-grabbing touch-none"
              data-testid={`drag-handle-category-${category.categoryKey}`}
            >
              <GripVertical className="h-5 w-5 text-gray-400 hover:text-gray-600" />
            </div>
            <CategoryIcon className="h-5 w-5 text-primary" />
            <div>
              <CardTitle className="text-lg">{category.categoryName}</CardTitle>
              <CardDescription className="text-xs">Key: {category.categoryKey}</CardDescription>
            </div>
          </div>
          <div className="flex gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onEditCategory(category)}
              data-testid={`button-edit-category-${category.categoryKey}`}
            >
              <Edit className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onDeleteCategory(category.categoryKey)}
              data-testid={`button-delete-category-${category.categoryKey}`}
            >
              <Trash2 className="h-4 w-4 text-destructive" />
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <SortableContext
          items={category.items.map(item => item.itemKey)}
          strategy={verticalListSortingStrategy}
        >
          <div className="space-y-2 min-h-[100px]">
            {category.items.map((item, index) => (
              <SortableItem key={item.itemKey} item={item} categoryKey={category.categoryKey} index={index} />
            ))}
            {category.items.length === 0 && (
              <div className="text-center py-8 text-gray-400 text-sm">
                No items in this category. Drag items here.
              </div>
            )}
          </div>
        </SortableContext>
      </CardContent>
    </Card>
  );
}

export default function NavigationManager() {
  const { toast } = useToast();
  const [activeId, setActiveId] = useState<string | null>(null);
  const [categories, setCategories] = useState<Category[]>([]);
  const [showInlineForm, setShowInlineForm] = useState(false);
  const [editingCategory, setEditingCategory] = useState<Category | null>(null);
  const [newCategory, setNewCategory] = useState({
    categoryKey: '',
    categoryName: '',
    iconName: 'BarChart3',
    isCollapsible: true,
    defaultExpanded: true,
  });

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8,
      },
    })
  );

  // Fetch categorized navigation
  const { data: categorizedData, isLoading } = useQuery<{ categories: Category[] }>({
    queryKey: ['/api/navigation/categorized'],
  });

  // Update local state when data changes
  useEffect(() => {
    if (categorizedData?.categories) {
      setCategories(categorizedData.categories);
    }
  }, [categorizedData]);

  // Save preferences mutation
  const savePreferencesMutation = useMutation({
    mutationFn: async (preferences: any[]) => {
      return apiRequest('POST', '/api/navigation/user-preferences', { preferences });
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: "Navigation preferences saved successfully",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
    },
    onError: () => {
      toast({
        title: "Error",
        description: "Failed to save preferences",
        variant: "destructive",
      });
    },
  });

  // Reset preferences mutation
  const resetPreferencesMutation = useMutation({
    mutationFn: async () => {
      return apiRequest('POST', '/api/navigation/reset-preferences');
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: "Navigation preferences reset to defaults",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
    },
  });

  const handleDragStart = (event: DragStartEvent) => {
    setActiveId(event.active.id as string);
  };

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveId(null);

    console.log('🔷 Drag End Event:', { 
      activeId: active.id, 
      overId: over?.id,
      activeData: active.data.current,
      overData: over?.data.current
    });

    const activeData = active.data.current;
    if (!activeData) {
      console.log('❌ No active data');
      return;
    }

    // Check if dragging a category
    if (activeData.isCategory) {
      console.log('✅ Dragging a category');
      const activeId = active.id as string;
      const activeCategoryKey = activeId.replace('category-', '');
      const oldIndex = categories.findIndex(c => c.categoryKey === activeCategoryKey);
      
      console.log('📍 Old Index:', oldIndex);
      if (oldIndex === -1) return;

      // If no over target or dropped on same position, do nothing
      if (!over || over.id === active.id) {
        console.log('⚠️ No over target or same position');
        return;
      }

      const overData = over.data.current;
      console.log('📦 Over Data:', overData);
      
      if (!overData?.isCategory) {
        console.log('❌ Over target is not a category');
        return;
      }

      // Calculate new index
      const overId = over.id as string;
      const overCategoryKey = overId.replace('category-', '');
      const newIndex = categories.findIndex(c => c.categoryKey === overCategoryKey);
      
      if (newIndex === -1 || oldIndex === newIndex) return;

      // Reorder categories
      const newCategories = arrayMove(categories, oldIndex, newIndex);
      setCategories(newCategories);

      // Auto-save category order to database
      try {
        const categoryOrder = newCategories.map((cat, index) => ({
          categoryKey: cat.categoryKey,
          sortOrder: index + 1
        }));
        
        await apiRequest('POST', '/api/navigation/category-order', { categoryOrder });
        
        // Invalidate cache to get fresh data
        queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
        
        toast({
          title: "Success",
          description: "Category order saved",
        });
      } catch (error: any) {
        const message = error?.message || 'Failed to save category order';
        toast({
          title: "Error",
          description: message.includes('administrators') 
            ? "Only administrators can reorder categories"
            : "Failed to save category order",
          variant: "destructive",
        });
        // Revert on error
        queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
      }
      return;
    }

    // Handle item dragging (existing logic)
    if (!over || active.id === over.id) return;
    const overData = over.data.current;

    // Handle item dragging (existing logic)
    const activeItemKey = active.id as string;
    const activeCategoryKey = activeData.categoryKey;

    // Clone categories array
    const newCategories = categories.map(cat => ({
      ...cat,
      items: [...cat.items]
    }));

    // Find source category
    const sourceCategoryIndex = newCategories.findIndex(c => c.categoryKey === activeCategoryKey);
    if (sourceCategoryIndex === -1) return;

    const sourceCategory = newCategories[sourceCategoryIndex];

    // Find the active item index
    const activeIndex = sourceCategory.items.findIndex(item => item.itemKey === activeItemKey);
    if (activeIndex === -1) return;

    // Determine destination category and index
    let destCategoryKey: string;
    let overIndex: number;

    if (overData && overData.isCategory) {
      // Dropped on empty category
      destCategoryKey = overData.categoryKey;
      overIndex = 0;
    } else if (overData) {
      // Dropped on another item
      destCategoryKey = overData.categoryKey;
      overIndex = overData.index;
      
      // For inter-category moves, check pointer position relative to hovered item
      // to determine if we should insert before or after
      if (activeCategoryKey !== destCategoryKey && over.rect) {
        const overRect = over.rect;
        const activeRect = active.rect.current.translated;
        
        if (activeRect && overRect) {
          // Calculate the vertical center of the hovered item
          const overCenter = overRect.top + (overRect.height / 2);
          // Use the active item's center position to determine placement
          const activeCenter = activeRect.top + (activeRect.height / 2);
          
          // If active center is below the over center, insert after
          if (activeCenter > overCenter) {
            overIndex = overIndex + 1;
          }
        }
      }
    } else {
      return;
    }

    const destCategoryIndex = newCategories.findIndex(c => c.categoryKey === destCategoryKey);
    if (destCategoryIndex === -1) return;

    const destCategory = newCategories[destCategoryIndex];

    if (activeCategoryKey === destCategoryKey) {
      // Moving within the same category - use arrayMove
      sourceCategory.items = arrayMove(sourceCategory.items, activeIndex, overIndex);
    } else {
      // Moving to a different category
      const [movedItem] = sourceCategory.items.splice(activeIndex, 1);
      
      // Clamp overIndex to valid range
      const insertIndex = Math.min(overIndex, destCategory.items.length);
      destCategory.items.splice(insertIndex, 0, movedItem);
    }

    setCategories(newCategories);
  };

  // Save category order mutation (separate from item preferences)
  const saveCategoryOrderMutation = useMutation({
    mutationFn: async (categoryOrder: any[]) => {
      return apiRequest('POST', '/api/navigation/category-order', { categoryOrder });
    },
  });

  const handleSaveChanges = async () => {
    // Build preferences array for navigation items
    const preferences: any[] = [];
    
    categories.forEach((category) => {
      category.items.forEach((item, index) => {
        preferences.push({
          navItemKey: item.itemKey,
          categoryKey: category.categoryKey,
          sortOrder: index,
          isVisible: true,
        });
      });
    });

    // Build category order array
    const categoryOrder = categories.map((category, index) => ({
      categoryKey: category.categoryKey,
      sortOrder: index + 1, // 1-based order to match default_sort_order
    }));

    try {
      // Save both atomically - if either fails, both should fail
      await Promise.all([
        savePreferencesMutation.mutateAsync(preferences),
        saveCategoryOrderMutation.mutateAsync(categoryOrder)
      ]);
      
      toast({
        title: "Success",
        description: "Navigation preferences and category order saved successfully",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
    } catch (error: any) {
      const message = error?.message || 'Failed to save preferences';
      toast({
        title: "Error",
        description: message.includes('administrators') 
          ? "Only administrators can reorder categories. You can still reorder items within categories."
          : "Failed to save preferences",
        variant: "destructive",
      });
    }
  };

  const handleResetToDefaults = () => {
    if (confirm('Are you sure you want to reset all navigation preferences to defaults? This cannot be undone.')) {
      resetPreferencesMutation.mutate();
    }
  };

  const createCategoryMutation = useMutation({
    mutationFn: async (category: any) => {
      if (editingCategory) {
        return apiRequest('PATCH', `/api/navigation/categories/${editingCategory.categoryKey}`, category);
      } else {
        return apiRequest('POST', '/api/navigation/categories', category);
      }
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: editingCategory ? "Category updated successfully" : "Category created successfully",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
      setShowInlineForm(false);
      setEditingCategory(null);
      setNewCategory({
        categoryKey: '',
        categoryName: '',
        iconName: 'BarChart3',
        isCollapsible: true,
        defaultExpanded: true,
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error?.message || 'Failed to save category',
        variant: "destructive",
      });
    },
  });

  const handleCreateCategory = () => {
    if (!newCategory.categoryKey || !newCategory.categoryName) {
      toast({
        title: "Validation Error",
        description: "Category key and name are required",
        variant: "destructive",
      });
      return;
    }

    createCategoryMutation.mutate(newCategory);
  };

  const handleEditCategory = (category: Category) => {
    setEditingCategory(category);
    setNewCategory({
      categoryKey: category.categoryKey,
      categoryName: category.categoryName,
      iconName: category.iconName || 'BarChart3',
      isCollapsible: category.isCollapsible,
      defaultExpanded: category.defaultExpanded,
    });
    setShowInlineForm(true);
    // Scroll to the form
    setTimeout(() => {
      document.getElementById('category-form')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }, 100);
  };

  const handleCancelForm = () => {
    setShowInlineForm(false);
    setEditingCategory(null);
    setNewCategory({
      categoryKey: '',
      categoryName: '',
      iconName: 'BarChart3',
      isCollapsible: true,
      defaultExpanded: true,
    });
  };

  const deleteCategoryMutation = useMutation({
    mutationFn: async (categoryKey: string) => {
      return apiRequest('DELETE', `/api/navigation/categories/${categoryKey}`);
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: "Category deleted successfully",
      });
      queryClient.invalidateQueries({ queryKey: ['/api/navigation/categorized'] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error?.message || 'Failed to delete category',
        variant: "destructive",
      });
    },
  });

  const handleDeleteCategory = (categoryKey: string) => {
    if (confirm('Are you sure you want to delete this category? This action cannot be undone.')) {
      deleteCategoryMutation.mutate(categoryKey);
    }
  };

  if (isLoading) {
    return (
      <MainLayout title="Navigation Manager" description="Organize your navigation menu">
        <div className="flex items-center justify-center h-64">
          <div className="text-gray-500">Loading...</div>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout 
      title="Navigation Manager" 
      description="Drag and drop to organize your navigation menu"
    >
      <div className="space-y-6">
        {/* Action Buttons */}
        <div className="flex justify-between items-center">
          <div className="flex gap-3">
            <Button
              onClick={handleSaveChanges}
              disabled={savePreferencesMutation.isPending}
              data-testid="button-save-changes"
            >
              <Save className="h-4 w-4 mr-2" />
              Save Changes
            </Button>
            <Button
              variant="outline"
              onClick={handleResetToDefaults}
              disabled={resetPreferencesMutation.isPending}
              data-testid="button-reset-defaults"
            >
              <RotateCcw className="h-4 w-4 mr-2" />
              Reset to Defaults
            </Button>
          </div>
          
          <Button 
            variant="outline" 
            onClick={() => setShowInlineForm(!showInlineForm)}
            data-testid="button-create-category"
          >
            <Plus className="h-4 w-4 mr-2" />
            New Category
          </Button>
        </div>

        {/* Inline Category Form */}
        {showInlineForm && (
          <Card id="category-form" className="border-2 border-primary/20">
            <CardHeader>
              <CardTitle>
                {editingCategory ? 'Edit Category' : 'Create New Category'}
              </CardTitle>
              <CardDescription>
                {editingCategory ? 'Modify category details' : 'Add a new category to organize navigation items'}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="categoryKey">Category Key</Label>
                  <Input
                    id="categoryKey"
                    value={newCategory.categoryKey}
                    onChange={(e) => setNewCategory({ ...newCategory, categoryKey: e.target.value })}
                    placeholder="e.g., my_category"
                    disabled={!!editingCategory}
                    data-testid="input-category-key"
                  />
                </div>
                <div>
                  <Label htmlFor="categoryName">Category Name</Label>
                  <Input
                    id="categoryName"
                    value={newCategory.categoryName}
                    onChange={(e) => setNewCategory({ ...newCategory, categoryName: e.target.value })}
                    placeholder="e.g., My Category"
                    data-testid="input-category-name"
                  />
                </div>
                <div>
                  <Label htmlFor="iconName">Icon</Label>
                  <select
                    id="iconName"
                    value={newCategory.iconName}
                    onChange={(e) => setNewCategory({ ...newCategory, iconName: e.target.value })}
                    className="w-full p-2 border rounded-md dark:bg-gray-800 dark:border-gray-700"
                    data-testid="select-icon"
                  >
                    {availableIcons.map((icon) => (
                      <option key={icon} value={icon}>{icon}</option>
                    ))}
                  </select>
                </div>
                <div className="flex items-center justify-between">
                  <Label htmlFor="isCollapsible">Collapsible</Label>
                  <Switch
                    id="isCollapsible"
                    checked={newCategory.isCollapsible}
                    onCheckedChange={(checked) => setNewCategory({ ...newCategory, isCollapsible: checked })}
                    data-testid="switch-collapsible"
                  />
                </div>
                <div className="flex items-center justify-between">
                  <Label htmlFor="defaultExpanded">Expanded by Default</Label>
                  <Switch
                    id="defaultExpanded"
                    checked={newCategory.defaultExpanded}
                    onCheckedChange={(checked) => setNewCategory({ ...newCategory, defaultExpanded: checked })}
                    data-testid="switch-expanded"
                  />
                </div>
                <div className="flex gap-3 pt-4">
                  <Button onClick={handleCreateCategory} data-testid="button-confirm-category">
                    {editingCategory ? 'Update' : 'Create'} Category
                  </Button>
                  <Button variant="outline" onClick={handleCancelForm} data-testid="button-cancel-category">
                    Cancel
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Drag and Drop Categories */}
        <DndContext
          sensors={sensors}
          collisionDetection={closestCorners}
          onDragStart={handleDragStart}
          onDragEnd={handleDragEnd}
        >
          {/* Separate SortableContext for categories with grid strategy */}
          <SortableContext
            items={categories.map(c => `category-${c.categoryKey}`)}
            strategy={rectSortingStrategy}
          >
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {categories.map((category) => (
                <CategoryCard
                  key={category.categoryKey}
                  category={category}
                  onEditCategory={handleEditCategory}
                  onDeleteCategory={handleDeleteCategory}
                />
              ))}
            </div>
          </SortableContext>

          <DragOverlay>
            {activeId ? (
              <div className="flex items-center gap-3 p-3 bg-white dark:bg-gray-800 border rounded-lg shadow-lg">
                <GripVertical className="h-4 w-4 text-gray-400" />
                <div className="font-medium text-sm">Dragging...</div>
              </div>
            ) : null}
          </DragOverlay>
        </DndContext>
      </div>
    </MainLayout>
  );
}
