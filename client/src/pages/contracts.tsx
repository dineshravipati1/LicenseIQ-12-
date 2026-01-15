import { useState, useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import ContractCard from "@/components/contracts/contract-card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { useLocation } from "wouter";
import { Search, Plus, Filter } from "lucide-react";

export default function Contracts() {
  const { user } = useAuth();
  const [, setLocation] = useLocation();
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [typeFilter, setTypeFilter] = useState("all");

  // Debounce search query to avoid excessive API calls
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery);
    }, 300); // 300ms debounce

    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Fetch all contracts (when no search query)
  const { data: allContractsData, isLoading: isLoadingAll } = useQuery({
    queryKey: ["/api/contracts"],
    enabled: !debouncedSearchQuery.trim(),
    staleTime: 30000,
    refetchInterval: 30000,
    refetchOnWindowFocus: true,
  });

  // Search contracts (when search query exists)
  const { data: searchResultsData, isLoading: isLoadingSearch } = useQuery({
    queryKey: ["/api/contracts/search", debouncedSearchQuery],
    queryFn: async () => {
      if (!debouncedSearchQuery.trim()) return { contracts: [] };
      const response = await fetch(`/api/contracts/search?q=${encodeURIComponent(debouncedSearchQuery)}`);
      if (!response.ok) throw new Error('Search failed');
      return response.json();
    },
    enabled: !!debouncedSearchQuery.trim(),
    staleTime: 30000,
  });

  // Determine which data source to use
  const contracts = useMemo(() => {
    if (debouncedSearchQuery.trim()) {
      return searchResultsData?.contracts || [];
    }
    return allContractsData?.contracts || [];
  }, [debouncedSearchQuery, searchResultsData, allContractsData]);

  const isLoading = debouncedSearchQuery.trim() ? isLoadingSearch : isLoadingAll;
  const total = allContractsData?.total || contracts.length;

  // Filter contracts based on status and type (search is handled by backend)
  const filteredContracts = contracts.filter((contract: any) => {
    const matchesStatus = statusFilter === "all" || contract.status === statusFilter;
    const matchesType = typeFilter === "all" || contract.contractType === typeFilter;
    return matchesStatus && matchesType;
  });

  const handleUpload = () => {
    setLocation("/upload");
  };

  return (
    <MainLayout 
      title="Contracts" 
      description="Manage and analyze your contract portfolio"
    >
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Badge variant="outline" className="text-sm">
              {total} contracts
            </Badge>
          </div>
          <Button onClick={handleUpload} data-testid="button-upload-contract">
            <Plus className="h-4 w-4 mr-2" />
            Upload Contract
          </Button>
        </div>

        {/* Filters */}
        <div className="bg-card p-6 rounded-lg border border-border">
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
              <Input
                placeholder="Search contracts, parties, terms, rules, analysis..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
                data-testid="input-search-contracts"
              />
            </div>
            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[180px]" data-testid="select-status-filter">
                <SelectValue placeholder="All Status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Status</SelectItem>
                <SelectItem value="uploaded">Uploaded</SelectItem>
                <SelectItem value="processing">Processing</SelectItem>
                <SelectItem value="analyzed">Analyzed</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
              </SelectContent>
            </Select>
            <Select value={typeFilter} onValueChange={setTypeFilter}>
              <SelectTrigger className="w-[180px]" data-testid="select-type-filter">
                <SelectValue placeholder="All Types" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                <SelectItem value="license">License Agreement</SelectItem>
                <SelectItem value="service">Service Agreement</SelectItem>
                <SelectItem value="partnership">Partnership Agreement</SelectItem>
                <SelectItem value="employment">Employment Contract</SelectItem>
                <SelectItem value="other">Other</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Contracts Grid */}
        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[...Array(6)].map((_, i) => (
              <div key={i} className="h-48 bg-muted/50 rounded-lg animate-pulse" />
            ))}
          </div>
        ) : filteredContracts.length === 0 ? (
          <div className="text-center py-12">
            <div className="h-24 w-24 bg-muted rounded-full flex items-center justify-center mx-auto mb-4">
              <Filter className="h-12 w-12 text-muted-foreground" />
            </div>
            <h3 className="text-lg font-medium mb-2">
              {searchQuery || statusFilter !== "all" || typeFilter !== "all" 
                ? "No contracts match your filters" 
                : "No contracts uploaded yet"
              }
            </h3>
            <p className="text-muted-foreground mb-6">
              {searchQuery || statusFilter !== "all" || typeFilter !== "all"
                ? "Try adjusting your search criteria or filters"
                : "Upload your first contract to get started with AI-powered analysis"
              }
            </p>
            <Button onClick={handleUpload}>
              <Plus className="h-4 w-4 mr-2" />
              Upload Contract
            </Button>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {filteredContracts.map((contract: any) => (
              <ContractCard 
                key={contract.id} 
                contract={contract} 
                data-testid={`contract-card-${contract.id}`}
              />
            ))}
          </div>
        )}
      </div>
    </MainLayout>
  );
}
