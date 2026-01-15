import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Package, Search, RefreshCw, FileDown, Eye, Database, CheckCircle, XCircle } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { format } from "date-fns";

interface ItemRecord {
  id: string;
  entityId: string;
  recordData: {
    itemCode?: string;
    itemName?: string;
    category?: string;
    unitPrice?: number;
    uom?: string;
    isActive?: boolean | string;
    [key: string]: any;
  };
  grpId: string;
  orgId: string;
  locId: string;
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
}

interface LicenseiqEntity {
  id: string;
  name: string;
  technicalName: string;
}

export default function MasterDataItems() {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedItem, setSelectedItem] = useState<ItemRecord | null>(null);

  // Find the Items entity first
  const { data: entitiesData } = useQuery<{ entities: LicenseiqEntity[] }>({
    queryKey: ['/api/licenseiq-entities'],
  });

  const itemsEntity = entitiesData?.entities?.find(e => e.technicalName === 'items');

  // Fetch items records
  const { data: recordsData, isLoading, refetch } = useQuery<{ records: ItemRecord[] }>({
    queryKey: ['/api/licenseiq-records', itemsEntity?.id],
    queryFn: async () => {
      if (!itemsEntity?.id) return { records: [] };
      const response = await fetch(`/api/licenseiq-records?entityId=${itemsEntity.id}`, {
        credentials: 'include'
      });
      if (!response.ok) throw new Error('Failed to fetch items');
      return response.json();
    },
    enabled: !!itemsEntity?.id,
  });

  const items = recordsData?.records || [];

  // Filter items based on search
  const filteredItems = items.filter(item => {
    if (!searchQuery) return true;
    const data = item.recordData;
    const searchLower = searchQuery.toLowerCase();
    return (
      (data.itemCode?.toLowerCase()?.includes(searchLower) ?? false) ||
      (data.itemName?.toLowerCase()?.includes(searchLower) ?? false) ||
      (data.category?.toLowerCase()?.includes(searchLower) ?? false)
    );
  });

  return (
    <MainLayout
      title="Items"
      description="View and manage imported item master data"
    >
      <div className="space-y-6">
        {/* Header Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Total Items</p>
                  <p className="text-2xl font-bold">{items.length}</p>
                </div>
                <Package className="h-8 w-8 text-blue-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Active Items</p>
                  <p className="text-2xl font-bold text-green-600">
                    {items.filter(i => i.recordData.isActive === true || i.recordData.isActive === 'Yes').length}
                  </p>
                </div>
                <CheckCircle className="h-8 w-8 text-green-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Categories</p>
                  <p className="text-2xl font-bold">
                    {new Set(items.map(i => i.recordData.category).filter(Boolean)).size}
                  </p>
                </div>
                <Database className="h-8 w-8 text-purple-500" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Avg. Price</p>
                  <p className="text-2xl font-bold">
                    ${(items.reduce((sum, i) => sum + (Number(i.recordData.unitPrice) || 0), 0) / (items.length || 1)).toFixed(2)}
                  </p>
                </div>
                <Package className="h-8 w-8 text-orange-500" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Search and Actions */}
        <Card>
          <CardHeader>
            <div className="flex flex-col sm:flex-row gap-4 justify-between">
              <div className="relative flex-1 max-w-md">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                <Input
                  placeholder="Search by item code, name, or category..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                  data-testid="input-search-items"
                />
              </div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetch()}
                  data-testid="button-refresh-items"
                >
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Refresh
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  data-testid="button-export-items"
                >
                  <FileDown className="h-4 w-4 mr-2" />
                  Export
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="flex justify-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
              </div>
            ) : filteredItems.length === 0 ? (
              <div className="text-center py-12">
                <Package className="h-16 w-16 text-gray-300 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">
                  {searchQuery ? "No items match your search" : "No items imported yet"}
                </h3>
                <p className="text-gray-500 dark:text-gray-400 mb-4">
                  {searchQuery 
                    ? "Try adjusting your search criteria" 
                    : "Import items from your ERP system using the ERP Integration Hub"}
                </p>
                {!searchQuery && (
                  <Button variant="outline" onClick={() => window.location.href = '/erp-integration'}>
                    Go to ERP Integration Hub
                  </Button>
                )}
              </div>
            ) : (
              <ScrollArea className="h-[500px]">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Item Code</TableHead>
                      <TableHead>Item Name</TableHead>
                      <TableHead>Category</TableHead>
                      <TableHead>Unit Price</TableHead>
                      <TableHead>UoM</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredItems.map((item) => (
                      <TableRow key={item.id} data-testid={`row-item-${item.id}`}>
                        <TableCell className="font-mono font-medium">
                          {item.recordData.itemCode || '—'}
                        </TableCell>
                        <TableCell>{item.recordData.itemName || '—'}</TableCell>
                        <TableCell>
                          {item.recordData.category ? (
                            <Badge variant="outline">{item.recordData.category}</Badge>
                          ) : '—'}
                        </TableCell>
                        <TableCell>
                          {item.recordData.unitPrice !== undefined 
                            ? `$${Number(item.recordData.unitPrice).toFixed(2)}`
                            : '—'}
                        </TableCell>
                        <TableCell>{item.recordData.uom || 'EA'}</TableCell>
                        <TableCell>
                          {item.recordData.isActive === true || item.recordData.isActive === 'Yes' ? (
                            <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                              Active
                            </Badge>
                          ) : (
                            <Badge variant="secondary">Inactive</Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => setSelectedItem(item)}
                            data-testid={`button-view-item-${item.id}`}
                          >
                            <Eye className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </ScrollArea>
            )}
          </CardContent>
        </Card>

        {/* Item Details Dialog */}
        <Dialog open={!!selectedItem} onOpenChange={() => setSelectedItem(null)}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Package className="h-5 w-5" />
                Item Details
              </DialogTitle>
            </DialogHeader>
            {selectedItem && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Item Code</p>
                    <p className="font-mono font-medium">{selectedItem.recordData.itemCode || '—'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Item Name</p>
                    <p className="font-medium">{selectedItem.recordData.itemName || '—'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Category</p>
                    <p>{selectedItem.recordData.category || '—'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Unit Price</p>
                    <p>{selectedItem.recordData.unitPrice !== undefined 
                      ? `$${Number(selectedItem.recordData.unitPrice).toFixed(2)}`
                      : '—'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Unit of Measure</p>
                    <p>{selectedItem.recordData.uom || 'EA'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Status</p>
                    <p>{selectedItem.recordData.isActive === true || selectedItem.recordData.isActive === 'Yes' ? 'Active' : 'Inactive'}</p>
                  </div>
                </div>
                
                <Separator />
                
                <div>
                  <p className="text-sm text-muted-foreground mb-2">All Record Data</p>
                  <pre className="bg-muted p-3 rounded-lg text-xs overflow-auto max-h-48">
                    {JSON.stringify(selectedItem.recordData, null, 2)}
                  </pre>
                </div>
                
                <Separator />
                
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <p className="text-muted-foreground">Record ID</p>
                    <p className="font-mono text-xs">{selectedItem.id}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Created</p>
                    <p>{format(new Date(selectedItem.createdAt), 'MM/dd/yyyy HH:mm')}</p>
                  </div>
                </div>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </MainLayout>
  );
}
