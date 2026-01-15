import { useParams, useLocation } from "wouter";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { ArrowLeft, Plus, Edit, Trash2, Check, X, Minus, Sparkles, Database, User, FileText } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useState, useEffect } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";

interface RoyaltyRule {
  id: string;
  contractId: string;
  ruleType: string;
  ruleName: string;
  description: string | null;
  productCategories: string[];
  territories: string[];
  containerSizes: string[];
  seasonalAdjustments: Record<string, number> | any;
  territoryPremiums: Record<string, number> | any;
  volumeTiers: any[];
  baseRate: string | null;
  minimumGuarantee: string | null;
  formulaDefinition: any | null;
  isActive: boolean;
  priority: number;
  sourceSection: string | null;
  sourceText: string | null;
  confidence: string | null;
}

type RuleSource = 'ai_extracted' | 'manual' | 'erp_mapping';

function getRuleSource(rule: RoyaltyRule): { source: RuleSource; label: string; icon: any; color: string; tooltip: string } {
  const hasSourceText = rule.sourceText && rule.sourceText.trim().length > 0;
  const hasSourceSection = rule.sourceSection && rule.sourceSection.trim().length > 0;
  const hasConfidence = rule.confidence && parseFloat(rule.confidence) > 0;
  
  if (hasSourceText || hasSourceSection || hasConfidence) {
    return {
      source: 'ai_extracted',
      label: 'AI Extracted',
      icon: Sparkles,
      color: 'bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-950 dark:text-purple-300 dark:border-purple-700',
      tooltip: `This rule was automatically extracted from the contract by AI${hasSourceSection ? ` (Section: ${rule.sourceSection})` : ''}${hasConfidence ? ` with ${parseFloat(rule.confidence!).toFixed(0)}% confidence` : ''}`
    };
  }
  
  return {
    source: 'manual',
    label: 'Manual',
    icon: User,
    color: 'bg-blue-100 text-blue-700 border-blue-300 dark:bg-blue-950 dark:text-blue-300 dark:border-blue-700',
    tooltip: 'This rule was manually created by a user'
  };
}

// Helper function to extract volume tiers from formula definition
function extractVolumeTiersFromFormula(formulaDefinition: any): any[] {
  if (!formulaDefinition?.formula) return [];
  
  const findTierNode = (node: any): any => {
    if (!node) return null;
    if (node.type === 'tier' && node.tiers) return node;
    if (node.operands && Array.isArray(node.operands)) {
      for (const operand of node.operands) {
        const result = findTierNode(operand);
        if (result) return result;
      }
    }
    return null;
  };
  
  const tierNode = findTierNode(formulaDefinition.formula);
  return tierNode?.tiers || [];
}

// Helper function to extract seasonal adjustments from formula definition
function extractSeasonalAdjustments(formulaDefinition: any): Record<string, number> {
  if (!formulaDefinition?.formula) return {};
  
  const findSeasonalNode = (node: any): any => {
    if (!node) return null;
    if (node.type === 'lookup' && node.description?.toLowerCase().includes('seasonal')) {
      return node.table;
    }
    if (node.operands && Array.isArray(node.operands)) {
      for (const operand of node.operands) {
        const result = findSeasonalNode(operand);
        if (result) return result;
      }
    }
    return null;
  };
  
  return findSeasonalNode(formulaDefinition.formula) || {};
}

// Helper function to detect contract category from existing rules
function detectContractCategory(rules: RoyaltyRule[]): 'royalty' | 'payment' | 'service' {
  if (!rules || rules.length === 0) return 'payment';
  
  // Check if contract has royalty-specific features
  const hasRoyaltyFeatures = rules.some(r => 
    r.volumeTiers?.length > 0 || 
    (r.seasonalAdjustments && Object.keys(r.seasonalAdjustments).length > 0) ||
    (r.territoryPremiums && Object.keys(r.territoryPremiums).length > 0) ||
    r.containerSizes?.length > 0 ||
    r.ruleType === 'percentage' ||
    r.ruleType === 'tiered_pricing' ||
    r.ruleType === 'formula_based'
  );
  
  if (hasRoyaltyFeatures) return 'royalty';
  
  // Check if contract has service/payment features
  const hasPaymentFeatures = rules.some(r =>
    r.ruleType === 'payment_schedule' ||
    r.ruleType === 'payment_method' ||
    r.ruleType === 'rate_structure' ||
    r.ruleType === 'invoice_requirements' ||
    r.ruleType === 'milestone_payment' ||
    r.ruleType === 'late_payment_penalty'
  );
  
  if (hasPaymentFeatures) return 'payment';
  
  return 'service';
}

export default function RulesManagement() {
  const { id } = useParams<{ id: string }>();
  const [, navigate] = useLocation();
  const { toast } = useToast();
  
  // Track which rule is being edited (null = none, 'new' = adding new rule, or rule.id)
  const [editingRuleId, setEditingRuleId] = useState<string | null>(null);
  const [showAddForm, setShowAddForm] = useState(false);
  
  // Form state
  const [editForm, setEditForm] = useState({
    ruleName: "",
    description: "",
    ruleType: "rate_structure",
    productCategories: [] as string[],
    territories: [] as string[],
    containerSizes: [] as string[],
    baseRate: "",
    minimumGuarantee: "",
    volumeTiers: [] as Array<{ min: number; max: number | null; rate: number }>,
    seasonalAdjustments: {} as Record<string, number>,
    territoryPremiums: {} as Record<string, number>,
    priority: 1,
  });
  
  // Temporary input states for arrays
  const [newCategory, setNewCategory] = useState("");
  const [newTerritory, setNewTerritory] = useState("");
  const [newContainerSize, setNewContainerSize] = useState("");
  
  // Fetch contract details
  const { data: contract } = useQuery({
    queryKey: [`/api/contracts/${id}`],
  });

  // Fetch royalty rules
  const { data: rulesData, isLoading } = useQuery<{ rules: RoyaltyRule[] }>({
    queryKey: [`/api/contracts/${id}/rules`],
    enabled: !!id,
  });
  
  // Detect contract category from existing rules
  const contractCategory = detectContractCategory(rulesData?.rules || []);

  // Delete rule mutation
  const deleteMutation = useMutation({
    mutationFn: async (ruleId: string) => {
      return apiRequest("DELETE", `/api/contracts/${id}/rules/${ruleId}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/rules`] });
      toast({
        title: "Rule deleted",
        description: "The royalty rule has been deleted successfully.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to delete rule",
        variant: "destructive",
      });
    },
  });

  // Toggle active mutation
  const toggleActiveMutation = useMutation({
    mutationFn: async ({ ruleId, isActive }: { ruleId: string; isActive: boolean }) => {
      return apiRequest("PATCH", `/api/contracts/${id}/rules/${ruleId}`, { isActive });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/rules`] });
      toast({
        title: "Rule updated",
        description: "The rule status has been updated.",
      });
    },
  });
  
  // Save/Update rule mutation
  const saveRuleMutation = useMutation({
    mutationFn: async (ruleData: any) => {
      if (editingRuleId && editingRuleId !== 'new') {
        // Update existing rule
        return apiRequest("PATCH", `/api/contracts/${id}/rules/${editingRuleId}`, ruleData);
      } else {
        // Create new rule
        return apiRequest("POST", `/api/contracts/${id}/rules`, { ...ruleData, contractId: id });
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [`/api/contracts/${id}/rules`] });
      toast({
        title: "Rule saved",
        description: "The royalty rule has been saved successfully.",
      });
      setEditingRuleId(null);
      setShowAddForm(false);
      resetForm();
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to save rule",
        variant: "destructive",
      });
    },
  });

  const resetForm = () => {
    setEditForm({
      ruleName: "",
      description: "",
      ruleType: "rate_structure",
      productCategories: [],
      territories: [],
      containerSizes: [],
      baseRate: "",
      minimumGuarantee: "",
      volumeTiers: [],
      seasonalAdjustments: {},
      territoryPremiums: {},
      priority: 1,
    });
    setNewCategory("");
    setNewTerritory("");
    setNewContainerSize("");
  };

  const startEdit = (rule: RoyaltyRule) => {
    setShowAddForm(false); // Close add form if open
    setEditingRuleId(rule.id);
    
    // Extract volume tiers from formula_definition if available, otherwise use legacy volumeTiers
    const extractedTiers = rule.formulaDefinition 
      ? extractVolumeTiersFromFormula(rule.formulaDefinition)
      : (rule.volumeTiers || []);
    
    // Extract seasonal adjustments from formula_definition if available
    const extractedSeasonalAdj = rule.formulaDefinition
      ? extractSeasonalAdjustments(rule.formulaDefinition)
      : (rule.seasonalAdjustments || {});
    
    setEditForm({
      ruleName: rule.ruleName || "",
      description: rule.description || "",
      ruleType: rule.ruleType || "tiered_pricing",
      productCategories: rule.productCategories || [],
      territories: rule.territories || [],
      containerSizes: rule.containerSizes || [],
      baseRate: rule.baseRate || "",
      minimumGuarantee: rule.minimumGuarantee || "",
      volumeTiers: extractedTiers,
      seasonalAdjustments: extractedSeasonalAdj,
      territoryPremiums: rule.territoryPremiums || {},
      priority: rule.priority || 1,
    });
  };

  const cancelEdit = () => {
    setEditingRuleId(null);
    setShowAddForm(false);
    resetForm();
  };

  const handleSaveRule = () => {
    if (!editForm.ruleName.trim()) {
      toast({
        title: "Validation Error",
        description: "Rule name is required",
        variant: "destructive",
      });
      return;
    }
    
    // Sanitize data: convert empty strings to null for numeric fields
    const sanitizedData = {
      ...editForm,
      baseRate: (typeof editForm.baseRate === 'string' && editForm.baseRate.trim() === '') 
        ? null 
        : editForm.baseRate,
      minimumGuarantee: (typeof editForm.minimumGuarantee === 'string' && editForm.minimumGuarantee.trim() === '') 
        ? null 
        : editForm.minimumGuarantee,
    };
    
    saveRuleMutation.mutate(sanitizedData);
  };

  const startAddNew = () => {
    setEditingRuleId(null); // Close any open edit form
    resetForm();
    setShowAddForm(true);
    setEditingRuleId('new');
  };

  const getRuleTypeLabel = (type: string) => {
    const labels: { [key: string]: string } = {
      tiered_pricing: "Tiered Pricing",
      minimum_guarantee: "Minimum Guarantee",
      seasonal_adjustment: "Seasonal Adjustment",
      territory_premium: "Territory Premium",
    };
    return labels[type] || type;
  };
  
  // Helper functions for managing form arrays
  const addCategory = () => {
    if (newCategory.trim()) {
      setEditForm({ ...editForm, productCategories: [...editForm.productCategories, newCategory.trim()] });
      setNewCategory("");
    }
  };
  
  const removeCategory = (index: number) => {
    setEditForm({ ...editForm, productCategories: editForm.productCategories.filter((_, i) => i !== index) });
  };
  
  const addTerritory = () => {
    if (newTerritory.trim()) {
      setEditForm({ ...editForm, territories: [...editForm.territories, newTerritory.trim()] });
      setNewTerritory("");
    }
  };
  
  const removeTerritory = (index: number) => {
    setEditForm({ ...editForm, territories: editForm.territories.filter((_, i) => i !== index) });
  };
  
  const addContainerSize = () => {
    if (newContainerSize.trim()) {
      setEditForm({ ...editForm, containerSizes: [...editForm.containerSizes, newContainerSize.trim()] });
      setNewContainerSize("");
    }
  };
  
  const removeContainerSize = (index: number) => {
    setEditForm({ ...editForm, containerSizes: editForm.containerSizes.filter((_, i) => i !== index) });
  };
  
  const addVolumeTier = () => {
    setEditForm({ 
      ...editForm, 
      volumeTiers: [...editForm.volumeTiers, { min: 0, max: null, rate: 0 }] 
    });
  };
  
  const updateVolumeTier = (index: number, field: 'min' | 'max' | 'rate', value: number | null) => {
    const newTiers = [...editForm.volumeTiers];
    newTiers[index][field] = value as any;
    setEditForm({ ...editForm, volumeTiers: newTiers });
  };
  
  const removeVolumeTier = (index: number) => {
    setEditForm({ ...editForm, volumeTiers: editForm.volumeTiers.filter((_, i) => i !== index) });
  };

  // Render edit form component
  const renderEditForm = () => (
    <div className="space-y-6 p-4 bg-slate-50 dark:bg-slate-900 rounded-lg border-2 border-violet-200 dark:border-violet-800">
      {/* Contract Category Indicator */}
      <div className="bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-lg p-3">
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="bg-blue-100 dark:bg-blue-900">
            {contractCategory === 'royalty' ? 'License Fee/Licensing Contract' : 
             contractCategory === 'payment' ? 'Service/Payment Contract' : 
             'General Contract'}
          </Badge>
          <p className="text-sm text-muted-foreground">
            {contractCategory === 'royalty' ? 'Form adapted for licensing agreements (volume tiers, seasonal adjustments, territories)' : 
             contractCategory === 'payment' ? 'Form adapted for service agreements (payment schedules, milestones, invoicing)' : 
             'Form adapted for general agreements'}
          </p>
        </div>
      </div>
      
      {/* Basic Information */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Basic Information</h3>
        <div className="grid grid-cols-2 gap-4">
          <div className="col-span-2">
            <Label htmlFor="ruleName">Rule Name *</Label>
            <Input
              id="ruleName"
              value={editForm.ruleName}
              onChange={(e) => setEditForm({ ...editForm, ruleName: e.target.value })}
              placeholder="e.g., Tier 1 — Ornamental Trees"
              data-testid="input-rule-name"
            />
          </div>
          
          <div className="col-span-2">
            <Label htmlFor="description">Description</Label>
            <Textarea
              id="description"
              value={editForm.description}
              onChange={(e) => setEditForm({ ...editForm, description: e.target.value })}
              placeholder="Describe this rule..."
              rows={2}
              data-testid="input-rule-description"
            />
          </div>
          
          <div>
            <Label htmlFor="ruleType">Rule Type</Label>
            <Select value={editForm.ruleType} onValueChange={(value) => setEditForm({ ...editForm, ruleType: value })}>
              <SelectTrigger data-testid="select-rule-type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="percentage">Percentage</SelectItem>
                <SelectItem value="tiered_pricing">Tiered Pricing</SelectItem>
                <SelectItem value="minimum_guarantee">Minimum Guarantee</SelectItem>
                <SelectItem value="fixed_fee">Fixed Fee</SelectItem>
                <SelectItem value="cap">Cap</SelectItem>
                <SelectItem value="payment_schedule">Payment Schedule</SelectItem>
                <SelectItem value="payment_method">Payment Method</SelectItem>
                <SelectItem value="rate_structure">Rate Structure</SelectItem>
                <SelectItem value="invoice_requirements">Invoice Requirements</SelectItem>
                <SelectItem value="late_payment_penalty">Late Payment Penalty</SelectItem>
                <SelectItem value="advance_payment">Advance Payment</SelectItem>
                <SelectItem value="milestone_payment">Milestone Payment</SelectItem>
              </SelectContent>
            </Select>
          </div>
          
          <div>
            <Label htmlFor="priority">Priority (1 = highest)</Label>
            <Input
              id="priority"
              type="number"
              value={editForm.priority}
              onChange={(e) => setEditForm({ ...editForm, priority: parseInt(e.target.value) || 1 })}
              data-testid="input-priority"
            />
          </div>
        </div>
      </div>

      <Separator />

      {/* Rates & Calculations - DYNAMIC BASED ON RULE TYPE */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">
          {['payment_schedule', 'payment_method', 'rate_structure', 'invoice_requirements', 'late_payment_penalty', 'advance_payment', 'milestone_payment'].includes(editForm.ruleType)
            ? 'Payment Term Details'
            : 'Rates & Calculations'}
        </h3>
        
        {/* ROYALTY RULES - Percentage/Fixed Amounts */}
        {(editForm.ruleType === 'percentage' || editForm.ruleType === 'fixed_fee' || editForm.ruleType === 'minimum_guarantee' || editForm.ruleType === 'cap') && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="baseRate">Base Rate ($)</Label>
              <Input
                id="baseRate"
                type="number"
                step="0.01"
                value={editForm.baseRate}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="1.25"
                data-testid="input-base-rate"
              />
              <p className="text-xs text-muted-foreground mt-1">Per-unit base license fee rate</p>
            </div>
            
            <div>
              <Label htmlFor="minimumGuarantee">Minimum Guarantee ($)</Label>
              <Input
                id="minimumGuarantee"
                type="number"
                step="0.01"
                value={editForm.minimumGuarantee}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="85000"
                data-testid="input-minimum-guarantee"
              />
              <p className="text-xs text-muted-foreground mt-1">Annual minimum payment</p>
            </div>
          </div>
        )}

        {/* PAYMENT SCHEDULE */}
        {editForm.ruleType === 'payment_schedule' && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="payment-terms">Payment Terms</Label>
              <Input
                id="payment-terms"
                value={editForm.baseRate || ''}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="e.g., Net 45, Net 30, Upon receipt"
                data-testid="input-payment-terms"
              />
              <p className="text-xs text-muted-foreground mt-1">When payment is due</p>
            </div>
            <div>
              <Label htmlFor="schedule-type">Schedule Type</Label>
              <Input
                id="schedule-type"
                value={editForm.minimumGuarantee || ''}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="e.g., Monthly, Quarterly, Milestone-based"
                data-testid="input-schedule-type"
              />
              <p className="text-xs text-muted-foreground mt-1">Frequency of payments</p>
            </div>
          </div>
        )}

        {/* PAYMENT METHOD */}
        {editForm.ruleType === 'payment_method' && (
          <div>
            <Label htmlFor="payment-method">Payment Method</Label>
            <Input
              id="payment-method"
              value={editForm.baseRate || ''}
              onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
              placeholder="e.g., Direct deposit, Wire transfer, ACH, Check"
              data-testid="input-payment-method"
            />
            <p className="text-xs text-muted-foreground mt-1">How payment will be made</p>
          </div>
        )}

        {/* RATE STRUCTURE */}
        {editForm.ruleType === 'rate_structure' && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="rate-amount">Rate Amount ($)</Label>
              <Input
                id="rate-amount"
                type="number"
                step="0.01"
                value={editForm.baseRate}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="e.g., 125.00"
                data-testid="input-rate-amount"
              />
              <p className="text-xs text-muted-foreground mt-1">Dollar amount per unit</p>
            </div>
            <div>
              <Label htmlFor="rate-unit">Rate Unit</Label>
              <Input
                id="rate-unit"
                value={editForm.minimumGuarantee || ''}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="e.g., per hour, per day, per month"
                data-testid="input-rate-unit"
              />
              <p className="text-xs text-muted-foreground mt-1">Time unit for the rate</p>
            </div>
          </div>
        )}

        {/* INVOICE REQUIREMENTS */}
        {editForm.ruleType === 'invoice_requirements' && (
          <div>
            <Label htmlFor="invoice-reqs">Invoice Requirements</Label>
            <Textarea
              id="invoice-reqs"
              value={editForm.baseRate || ''}
              onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
              placeholder="e.g., Itemized invoice with timesheets, W-9 form required"
              rows={3}
              data-testid="input-invoice-requirements"
            />
            <p className="text-xs text-muted-foreground mt-1">Documentation needed for payment</p>
          </div>
        )}

        {/* LATE PAYMENT PENALTY */}
        {editForm.ruleType === 'late_payment_penalty' && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="penalty-rate">Penalty Rate (%)</Label>
              <Input
                id="penalty-rate"
                type="number"
                step="0.01"
                value={editForm.baseRate}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="e.g., 1.5"
                data-testid="input-penalty-rate"
              />
              <p className="text-xs text-muted-foreground mt-1">Interest rate per month</p>
            </div>
            <div>
              <Label htmlFor="penalty-details">Penalty Details</Label>
              <Input
                id="penalty-details"
                value={editForm.minimumGuarantee || ''}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="e.g., per month after due date"
                data-testid="input-penalty-details"
              />
              <p className="text-xs text-muted-foreground mt-1">When penalty applies</p>
            </div>
          </div>
        )}

        {/* ADVANCE PAYMENT */}
        {editForm.ruleType === 'advance_payment' && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="advance-amount">Advance Amount ($)</Label>
              <Input
                id="advance-amount"
                type="number"
                step="0.01"
                value={editForm.baseRate}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="e.g., 5000"
                data-testid="input-advance-amount"
              />
              <p className="text-xs text-muted-foreground mt-1">Dollar amount of advance</p>
            </div>
            <div>
              <Label htmlFor="advance-percentage">Percentage (Optional)</Label>
              <Input
                id="advance-percentage"
                type="number"
                step="0.01"
                value={editForm.minimumGuarantee}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="e.g., 25 (for 25% down)"
                data-testid="input-advance-percentage"
              />
              <p className="text-xs text-muted-foreground mt-1">Percentage of total</p>
            </div>
          </div>
        )}

        {/* MILESTONE PAYMENT */}
        {editForm.ruleType === 'milestone_payment' && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="milestone-amount">Payment Amount ($)</Label>
              <Input
                id="milestone-amount"
                type="number"
                step="0.01"
                value={editForm.baseRate}
                onChange={(e) => setEditForm({ ...editForm, baseRate: e.target.value })}
                placeholder="e.g., 15000"
                data-testid="input-milestone-amount"
              />
              <p className="text-xs text-muted-foreground mt-1">Dollar amount for milestone</p>
            </div>
            <div>
              <Label htmlFor="milestone-trigger">Milestone Trigger</Label>
              <Input
                id="milestone-trigger"
                value={editForm.minimumGuarantee || ''}
                onChange={(e) => setEditForm({ ...editForm, minimumGuarantee: e.target.value })}
                placeholder="e.g., Upon project completion, Phase 1 delivery"
                data-testid="input-milestone-trigger"
              />
              <p className="text-xs text-muted-foreground mt-1">What triggers this payment</p>
            </div>
          </div>
        )}
      </div>

      {/* Only show royalty-specific sections for royalty/licensing contracts */}
      {contractCategory === 'royalty' && !['payment_schedule', 'payment_method', 'rate_structure', 'invoice_requirements', 'late_payment_penalty', 'advance_payment', 'milestone_payment'].includes(editForm.ruleType) && (
        <>
          <Separator />

          {/* Volume Tiers */}
          <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-lg">Volume Tiers</h3>
          <Button type="button" size="sm" onClick={addVolumeTier} data-testid="button-add-volume-tier">
            <Plus className="h-4 w-4 mr-1" />
            Add Tier
          </Button>
        </div>
        {editForm.volumeTiers.map((tier, index) => (
          <div key={index} className="grid grid-cols-4 gap-2 items-end">
            <div>
              <Label>Min Quantity</Label>
              <Input
                type="number"
                value={tier.min ?? 0}
                onChange={(e) => updateVolumeTier(index, 'min', parseInt(e.target.value) || 0)}
                data-testid={`input-tier-min-${index}`}
              />
            </div>
            <div>
              <Label>Max Quantity</Label>
              <Input
                type="number"
                value={tier.max || ""}
                onChange={(e) => updateVolumeTier(index, 'max', e.target.value ? parseInt(e.target.value) : null)}
                placeholder="∞"
                data-testid={`input-tier-max-${index}`}
              />
            </div>
            <div>
              <Label>Rate ($)</Label>
              <Input
                type="number"
                step="0.01"
                value={tier.rate}
                onChange={(e) => updateVolumeTier(index, 'rate', parseFloat(e.target.value) || 0)}
                data-testid={`input-tier-rate-${index}`}
              />
            </div>
            <Button type="button" variant="destructive" size="icon" onClick={() => removeVolumeTier(index)} data-testid={`button-remove-tier-${index}`}>
              <Minus className="h-4 w-4" />
            </Button>
          </div>
        ))}
        {editForm.volumeTiers.length === 0 && (
          <p className="text-sm text-muted-foreground">No volume tiers configured. Click "Add Tier" to create one.</p>
        )}
      </div>

      <Separator />

      {/* Seasonal Adjustments */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Seasonal Adjustments (Multipliers)</h3>
        <div className="grid grid-cols-2 gap-4">
          {['Spring', 'Summer', 'Fall', 'Winter', 'Holiday', 'Off-Season'].map((season) => (
            <div key={season}>
              <Label htmlFor={`seasonal-${season}`}>{season}</Label>
              <Input
                id={`seasonal-${season}`}
                type="number"
                step="0.01"
                value={editForm.seasonalAdjustments[season] || ""}
                onChange={(e) => setEditForm({ 
                  ...editForm, 
                  seasonalAdjustments: { ...editForm.seasonalAdjustments, [season]: parseFloat(e.target.value) || 0 }
                })}
                placeholder="1.0"
                data-testid={`input-seasonal-${season.toLowerCase()}`}
              />
              <p className="text-xs text-muted-foreground mt-1">1.0 = no change, 1.15 = +15%</p>
            </div>
          ))}
        </div>
      </div>

      <Separator />

      {/* Territory Premiums */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Territory Premiums (Multipliers)</h3>
        <div className="grid grid-cols-2 gap-4">
          {['Primary', 'Secondary', 'International', 'Organic', 'Specialty'].map((territory) => (
            <div key={territory}>
              <Label htmlFor={`territory-${territory}`}>{territory}</Label>
              <Input
                id={`territory-${territory}`}
                type="number"
                step="0.01"
                value={editForm.territoryPremiums[territory] || ""}
                onChange={(e) => setEditForm({ 
                  ...editForm, 
                  territoryPremiums: { ...editForm.territoryPremiums, [territory]: parseFloat(e.target.value) || 0 }
                })}
                placeholder="1.0"
                data-testid={`input-territory-${territory.toLowerCase()}`}
              />
            </div>
          ))}
        </div>
      </div>

      <Separator />

      {/* Product Categories */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Product Categories</h3>
        <div className="flex gap-2">
          <Input
            value={newCategory}
            onChange={(e) => setNewCategory(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && addCategory()}
            placeholder="Add category..."
            data-testid="input-new-category"
          />
          <Button type="button" onClick={addCategory} data-testid="button-add-category">Add</Button>
        </div>
        <div className="flex flex-wrap gap-2">
          {editForm.productCategories.map((cat, idx) => (
            <Badge key={idx} variant="secondary" className="gap-1" data-testid={`badge-category-${idx}`}>
              {cat}
              <X className="h-3 w-3 cursor-pointer" onClick={() => removeCategory(idx)} />
            </Badge>
          ))}
        </div>
      </div>

      <Separator />

      {/* Territories */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Territories</h3>
        <div className="flex gap-2">
          <Input
            value={newTerritory}
            onChange={(e) => setNewTerritory(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && addTerritory()}
            placeholder="Add territory..."
            data-testid="input-new-territory"
          />
          <Button type="button" onClick={addTerritory} data-testid="button-add-territory">Add</Button>
        </div>
        <div className="flex flex-wrap gap-2">
          {editForm.territories.map((terr, idx) => (
            <Badge key={idx} variant="secondary" className="gap-1" data-testid={`badge-territory-${idx}`}>
              {terr}
              <X className="h-3 w-3 cursor-pointer" onClick={() => removeTerritory(idx)} />
            </Badge>
          ))}
        </div>
      </div>

      <Separator />

      {/* Container Sizes */}
      <div className="space-y-4">
        <h3 className="font-semibold text-lg">Container Sizes</h3>
        <div className="flex gap-2">
          <Input
            value={newContainerSize}
            onChange={(e) => setNewContainerSize(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && addContainerSize()}
            placeholder="Add container size..."
            data-testid="input-new-container-size"
          />
          <Button type="button" onClick={addContainerSize} data-testid="button-add-container-size">Add</Button>
        </div>
        <div className="flex flex-wrap gap-2">
          {editForm.containerSizes.map((size, idx) => (
            <Badge key={idx} variant="secondary" className="gap-1" data-testid={`badge-container-size-${idx}`}>
              {size}
              <X className="h-3 w-3 cursor-pointer" onClick={() => removeContainerSize(idx)} />
            </Badge>
          ))}
        </div>
      </div>
        </>
      )}

      {/* Action Buttons */}
      <div className="flex justify-end gap-2 pt-4">
        <Button variant="outline" onClick={cancelEdit} data-testid="button-cancel-edit">
          <X className="h-4 w-4 mr-2" />
          Cancel
        </Button>
        <Button 
          onClick={handleSaveRule} 
          disabled={saveRuleMutation.isPending}
          className="bg-gradient-to-r from-violet-600 to-purple-600"
          data-testid="button-save-rule"
        >
          <Check className="h-4 w-4 mr-2" />
          {saveRuleMutation.isPending ? "Saving..." : "Save Rule"}
        </Button>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-violet-50 to-purple-50 dark:from-slate-950 dark:via-violet-950 dark:to-purple-950">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="mb-8">
          <Button
            variant="ghost"
            onClick={() => navigate(`/contracts/${id}`)}
            className="mb-4"
            data-testid="button-back"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Contract
          </Button>
          
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-4xl font-bold bg-gradient-to-r from-violet-600 to-purple-600 bg-clip-text text-transparent">
                License Fee Rules Management
              </h1>
              <p className="text-muted-foreground mt-2">
                {(contract as any)?.contractNumber && (
                  <span className="font-mono font-semibold text-violet-600 dark:text-violet-400 mr-2">
                    {(contract as any).contractNumber}
                  </span>
                )}
                {(contract as any)?.originalName || "Loading..."}
              </p>
            </div>
            {!showAddForm && (
              <Button
                onClick={startAddNew}
                className="bg-gradient-to-r from-violet-600 to-purple-600"
                data-testid="button-add-rule"
              >
                <Plus className="h-4 w-4 mr-2" />
                Add New Rule
              </Button>
            )}
          </div>
        </div>

        {/* Add New Rule Form (Inline) */}
        {showAddForm && (
          <Card className="mb-6 border-2 border-violet-300 dark:border-violet-700">
            <CardHeader>
              <CardTitle>Add New License Fee Rule</CardTitle>
              <CardDescription>
                Configure calculation formulas and rule parameters. All fields are optional except Rule Name.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {renderEditForm()}
            </CardContent>
          </Card>
        )}

        {/* Rules List */}
        {isLoading ? (
          <div className="text-center py-12">
            <p className="text-muted-foreground">Loading rules...</p>
          </div>
        ) : !rulesData?.rules || rulesData.rules.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center">
              <p className="text-muted-foreground mb-4">No license fee rules found for this contract.</p>
              {!showAddForm && (
                <Button
                  onClick={startAddNew}
                  className="bg-gradient-to-r from-violet-600 to-purple-600"
                >
                  <Plus className="h-4 w-4 mr-2" />
                  Add Your First Rule
                </Button>
              )}
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-4">
            {rulesData.rules.map((rule) => (
              <Card key={rule.id} className={!rule.isActive ? "opacity-60" : ""} data-testid={`card-rule-${rule.id}`}>
                {editingRuleId === rule.id ? (
                  // Inline Edit Mode
                  <CardContent className="pt-6">
                    {renderEditForm()}
                  </CardContent>
                ) : (
                  // View Mode
                  <>
                    <CardHeader>
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="flex items-center gap-3 flex-wrap">
                            <CardTitle>{rule.ruleName}</CardTitle>
                            <Badge variant={rule.isActive ? "default" : "secondary"}>
                              {rule.isActive ? "Active" : "Inactive"}
                            </Badge>
                            <Badge variant="outline">{getRuleTypeLabel(rule.ruleType)}</Badge>
                            {(() => {
                              const ruleSource = getRuleSource(rule);
                              const IconComponent = ruleSource.icon;
                              return (
                                <TooltipProvider>
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <Badge 
                                        variant="outline" 
                                        className={`flex items-center gap-1 ${ruleSource.color}`}
                                        data-testid={`badge-source-${rule.id}`}
                                      >
                                        <IconComponent className="h-3 w-3" />
                                        {ruleSource.label}
                                      </Badge>
                                    </TooltipTrigger>
                                    <TooltipContent side="bottom" className="max-w-xs">
                                      <p className="text-sm">{ruleSource.tooltip}</p>
                                    </TooltipContent>
                                  </Tooltip>
                                </TooltipProvider>
                              );
                            })()}
                          </div>
                          {rule.description && (
                            <CardDescription className="mt-2">{rule.description}</CardDescription>
                          )}
                          {rule.sourceSection && (
                            <div className="mt-3">
                              <Badge variant="outline" className="text-xs flex items-center gap-1 w-fit" title={rule.sourceText || rule.sourceSection}>
                                <FileText className="h-3 w-3" />
                                Contract: {rule.sourceSection}
                              </Badge>
                            </div>
                          )}
                        </div>
                        <div className="flex items-center gap-2">
                          <Switch
                            checked={rule.isActive}
                            onCheckedChange={(checked) => toggleActiveMutation.mutate({ ruleId: rule.id, isActive: checked })}
                            data-testid={`switch-active-${rule.id}`}
                          />
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => startEdit(rule)}
                            data-testid={`button-edit-${rule.id}`}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => {
                              if (confirm('Are you sure you want to delete this rule? This action cannot be undone.')) {
                                deleteMutation.mutate(rule.id);
                              }
                            }}
                            data-testid={`button-delete-${rule.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                      {/* Calculation Formula Display */}
                      {(() => {
                        // Try to get volume tiers from formula_definition first, then fallback to legacy volumeTiers
                        const volumeTiers = rule.formulaDefinition 
                          ? extractVolumeTiersFromFormula(rule.formulaDefinition)
                          : (rule.volumeTiers || []);
                        
                        // Try to get seasonal adjustments from formula_definition first, then fallback to legacy
                        const seasonalAdj = rule.formulaDefinition
                          ? extractSeasonalAdjustments(rule.formulaDefinition)
                          : (rule.seasonalAdjustments || {});
                        
                        const hasSeasonalAdj = Object.keys(seasonalAdj).length > 0;
                        const hasTerritoryPrem = rule.territoryPremiums && Object.keys(rule.territoryPremiums).length > 0;
                        let calculationFormula = '';
                        
                        if (volumeTiers && volumeTiers.length > 0) {
                          // Volume-based tiered pricing formula
                          let formulaParts: string[] = [];
                          volumeTiers.forEach((tier: any, idx: number) => {
                            // Handle null values for min/max
                            const minStr = tier.min != null ? tier.min.toLocaleString() : '0';
                            const maxStr = tier.max != null ? tier.max.toLocaleString() : '∞';
                            
                            const condition = tier.max != null
                              ? `if (quantity >= ${minStr} && quantity <= ${maxStr})` 
                              : `if (quantity >= ${minStr})`;
                            
                            let rateStr = typeof tier.rate === 'number' 
                              ? `$${tier.rate.toFixed(2)}`
                              : 'rate';
                            
                            let formula = `  royalty = quantity × ${rateStr}`;
                            if (hasSeasonalAdj) formula += ' × seasonalMultiplier';
                            if (hasTerritoryPrem) formula += ' × territoryMultiplier';
                            
                            formulaParts.push(`${condition} {\n${formula}\n}`);
                          });
                          calculationFormula = formulaParts.join(' else ');
                        } else if (rule.formulaDefinition?.formula) {
                          // Generate formula from FormulaNode for non-tier formulas
                          const generateFormulaText = (node: any): string => {
                            if (!node) return '';
                            
                            if (node.type === 'reference') {
                              return node.field;
                            }
                            
                            if (node.type === 'multiply' && node.operands) {
                              const parts = node.operands.map((op: any) => {
                                if (op.type === 'reference' && op.field === 'baseRate') {
                                  return 'baseRate';
                                }
                                if (op.type === 'lookup' && op.description) {
                                  const desc = op.description.toLowerCase();
                                  if (desc.includes('seasonal')) return 'seasonalMultiplier';
                                  if (desc.includes('territory')) return 'territoryMultiplier';
                                  if (desc.includes('organic')) return 'organicMultiplier';
                                  return 'multiplier';
                                }
                                return generateFormulaText(op);
                              }).filter(Boolean);
                              return parts.join(' × ');
                            }
                            
                            if (node.type === 'lookup') {
                              const entries = Object.entries(node.table || {});
                              if (entries.length > 0) {
                                return `lookup(${entries.map(([k, v]) => `${k}:${v}`).join(', ')})`;
                              }
                            }
                            
                            return '';
                          };
                          
                          const formulaText = generateFormulaText(rule.formulaDefinition.formula);
                          if (formulaText) {
                            calculationFormula = `royalty = quantity × ${formulaText}`;
                          }
                        } else if (rule.baseRate) {
                          // Simple base rate formula
                          let rateStr = `$${parseFloat(rule.baseRate).toFixed(2)}`;
                          
                          calculationFormula = `royalty = quantity × ${rateStr}`;
                          if (hasSeasonalAdj) calculationFormula += ' × seasonalMultiplier';
                          if (hasTerritoryPrem) calculationFormula += ' × territoryMultiplier';
                        } else if (rule.ruleType === 'minimum_guarantee' && rule.minimumGuarantee) {
                          calculationFormula = `royalty = max(calculated_royalty, $${parseFloat(rule.minimumGuarantee).toFixed(2)})`;
                        }
                        
                        return calculationFormula ? (
                          <div className="mb-4 p-3 bg-blue-100 dark:bg-blue-950 rounded border border-blue-300 dark:border-blue-700">
                            <div className="text-xs font-semibold text-blue-900 dark:text-blue-100 mb-1">
                              💡 Calculation Formula:
                            </div>
                            <pre className="text-xs text-blue-800 dark:text-blue-200 whitespace-pre-wrap font-mono">
                              {calculationFormula}
                            </pre>
                          </div>
                        ) : null;
                      })()}
                      
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        {rule.productCategories?.length > 0 && (
                          <div>
                            <p className="text-sm font-medium mb-1">Product Categories</p>
                            <div className="flex flex-wrap gap-1">
                              {rule.productCategories.map((cat, idx) => (
                                <Badge key={idx} variant="secondary">{cat}</Badge>
                              ))}
                            </div>
                          </div>
                        )}
                        
                        {rule.baseRate && (
                          <div>
                            <p className="text-sm font-medium mb-1">Base Rate</p>
                            <p className="text-2xl font-bold text-violet-600">${rule.baseRate}</p>
                          </div>
                        )}
                        
                        {rule.minimumGuarantee && (
                          <div>
                            <p className="text-sm font-medium mb-1">Minimum Guarantee</p>
                            <p className="text-2xl font-bold text-purple-600">${rule.minimumGuarantee}</p>
                          </div>
                        )}
                        
                        {(() => {
                          const volumeTiers = rule.formulaDefinition 
                            ? extractVolumeTiersFromFormula(rule.formulaDefinition)
                            : (rule.volumeTiers || []);
                          
                          return volumeTiers.length > 0 ? (
                            <div>
                              <p className="text-sm font-medium mb-1">Volume Tiers</p>
                              <p className="text-sm text-muted-foreground">{volumeTiers.length} tier(s) configured</p>
                            </div>
                          ) : null;
                        })()}
                      </div>
                    </CardContent>
                  </>
                )}
              </Card>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
