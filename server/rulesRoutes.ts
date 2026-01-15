import type { Express, Request, Response } from "express";
import crypto from "crypto";
import { storage, type OrgAccessContext } from "./storage";
import { isAuthenticated } from "./auth";
import { RulesEngine } from "./services/rulesEngine";
import type { RoyaltyCalculationInput } from "./services/rulesEngine";

// Helper: Check if user is a TRUE system admin (only explicit flag, not tenant admins)
// This is stricter than the routes.ts pattern to prevent tenant admins from bypassing scope
function isSystemAdmin(user: any): boolean {
  if (!user) return false;
  // Only explicit isSystemAdmin flag grants system-level access
  // user.role === 'admin' does NOT grant bypass because that can be a tenant admin
  return user.isSystemAdmin === true;
}

// Audit logging function
async function createAuditLog(req: any, action: string, resourceType?: string, resourceId?: string, details?: any) {
  if (req.user?.id) {
    try {
      await storage.createAuditLog({
        userId: req.user.id,
        action,
        resourceType,
        resourceId,
        details,
        ipAddress: req.ip,
        userAgent: req.get('User-Agent') || '',
      });
    } catch (error) {
      console.error('Failed to create audit log:', error);
    }
  }
}

// Helper: Validate contract access with proper tenant isolation
// Uses context-aware storage lookup to enforce 3-level hierarchy (Company → BU → Location)
// IMPORTANT: Does NOT grant bypass based on user.role - only isSystemAdmin === true bypasses
async function validateContractAccess(contractId: string, user: any): Promise<{ contract: any; authorized: boolean; error?: string }> {
  const userId = user.id;
  const userIsSystemAdmin = user.isSystemAdmin === true;
  
  // Only TRUE system admins bypass org context filtering
  if (userIsSystemAdmin) {
    const contract = await storage.getContract(contractId);
    if (!contract) {
      // Always return 403 to prevent ID probing (uniform error)
      return { contract: undefined, authorized: false, error: 'Access denied' };
    }
    return { contract, authorized: true };
  }
  
  const activeContext = user.activeContext;
  
  // Strict context validation: companyId is mandatory for all non-system-admin access
  if (!activeContext || !activeContext.companyId) {
    return { contract: undefined, authorized: false, error: 'Access denied' };
  }
  
  // Downgrade context role to 'user' to prevent the storage layer from including legacy contracts
  // This ensures tenant admins stay strictly scoped to their assigned hierarchy level
  const restrictedContext = {
    ...activeContext,
    role: 'user', // Force non-admin behavior in storage layer to enforce strict scoping
  };
  
  // Build org context for filtering - use 'viewer' as globalRole to prevent bypass
  const orgContext: OrgAccessContext = {
    activeContext: restrictedContext,
    globalRole: 'viewer', // Force context-based filtering for non-system-admins
    userId,
    isSystemAdmin: false,
  };

  // Get contract with org context filtering (tenant-scoped lookup)
  const contract = await storage.getContract(contractId, orgContext);
  if (!contract) {
    // Always return 403 to prevent ID probing - uniform error for not-found and unauthorized
    return { contract: undefined, authorized: false, error: 'Access denied' };
  }

  // Check if user has admin access within their ORIGINAL context (can edit any contract in scope)
  const contextRole = activeContext.role; // Use original role, not restricted
  const hasContextAdminAccess = contextRole === 'admin' || contextRole === 'owner' || contextRole === 'company_admin';
  
  // If not a context admin, must be the uploader
  if (!hasContextAdminAccess && contract.uploadedBy !== userId) {
    return { contract, authorized: false, error: 'Access denied' };
  }

  return { contract, authorized: true };
}

export function registerRulesRoutes(app: Express): void {
  // =====================================================
  // RULES ENGINE API ENDPOINTS
  // =====================================================

  // Get royalty rules for a contract (secured with full multi-tenant validation)
  app.get('/api/contracts/:id/rules', isAuthenticated, async (req: any, res) => {
    try {
      const contractId = req.params.id;

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Get royalty rules from database
      const rules = await storage.getRoyaltyRulesByContract(contractId);
      
      console.log(`📋 [RULES API] Returning ${rules.length} rules for contract ${contractId}`);
      
      // Format response
      res.json({
        rules,
        total: rules.length
      });
    } catch (error) {
      console.error('Error fetching rules:', error);
      res.status(500).json({ message: 'Failed to fetch rules' });
    }
  });

  // Delete a royalty rule (secured with full multi-tenant validation)
  app.delete('/api/contracts/:contractId/rules/:ruleId', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId, ruleId } = req.params;

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Delete the rule
      await storage.deleteRoyaltyRule(ruleId);

      // Log the deletion
      await createAuditLog(req, 'rule_deleted', 'royalty_rule', ruleId, {
        contractId
      });

      res.json({ message: 'Rule deleted successfully' });
    } catch (error) {
      console.error('Error deleting rule:', error);
      res.status(500).json({ message: 'Failed to delete rule' });
    }
  });

  // Create a new royalty rule (secured with full multi-tenant validation)
  app.post('/api/contracts/:contractId/rules', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId } = req.params;
      const ruleData = req.body;

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Create the rule
      const newRule = await storage.createRoyaltyRule({
        ...ruleData,
        contractId
      });

      // Log the creation
      await createAuditLog(req, 'rule_created', 'royalty_rule', newRule.id, {
        contractId,
        ruleName: ruleData.ruleName
      });

      res.json({ message: 'Rule created successfully', rule: newRule });
    } catch (error) {
      console.error('Error creating rule:', error);
      res.status(500).json({ message: 'Failed to create rule' });
    }
  });

  // Update a royalty rule (secured with full multi-tenant validation)
  app.patch('/api/contracts/:contractId/rules/:ruleId', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId, ruleId } = req.params;
      const updates = req.body;

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Update the rule
      await storage.updateRoyaltyRule(ruleId, updates);

      // Log the update
      await createAuditLog(req, 'rule_updated', 'royalty_rule', ruleId, {
        contractId,
        updates
      });

      res.json({ message: 'Rule updated successfully' });
    } catch (error) {
      console.error('Error updating rule:', error);
      res.status(500).json({ message: 'Failed to update rule' });
    }
  });

  // Update a specific rule within a rule set (secured with full multi-tenant validation)
  app.put('/api/contracts/:contractId/rules/:ruleSetId/rule/:ruleIndex', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId, ruleSetId, ruleIndex } = req.params;
      const updatedRule = req.body;
      const index = parseInt(ruleIndex);

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Get the rule set
      const ruleSet = await storage.getLicenseRuleSet(ruleSetId);
      if (!ruleSet || ruleSet.contractId !== contractId) {
        return res.status(404).json({ message: 'Rule set not found' });
      }

      // Get current rules
      const rulesDsl = ruleSet.rulesDsl as any;
      const rules = rulesDsl?.rules || [];
      if (index < 0 || index >= rules.length) {
        return res.status(400).json({ message: 'Invalid rule index' });
      }

      // Update the rule
      rules[index] = {
        ...rules[index],
        ...updatedRule,
        id: rules[index].id, // Preserve the ID
      };

      const updatedRulesDsl = {
        ...rulesDsl,
        rules
      };

      await storage.updateLicenseRuleSet(ruleSetId, {
        rulesDsl: updatedRulesDsl
      } as any);

      // Log the update
      await createAuditLog(req, 'rule_updated', 'license_rule', ruleSetId, {
        ruleIndex: index,
        ruleName: updatedRule.ruleName
      });

      res.json({ message: 'Rule updated successfully', rule: rules[index] });
    } catch (error) {
      console.error('Error updating rule:', error);
      res.status(500).json({ message: 'Failed to update rule' });
    }
  });

  // Add a new rule to a rule set (secured with full multi-tenant validation)
  app.post('/api/contracts/:contractId/rules/:ruleSetId/rule', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId, ruleSetId } = req.params;
      const newRule = req.body;

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Get the rule set
      const ruleSet = await storage.getLicenseRuleSet(ruleSetId);
      if (!ruleSet || ruleSet.contractId !== contractId) {
        return res.status(404).json({ message: 'Rule set not found' });
      }

      // Add the new rule
      const rulesDsl = ruleSet.rulesDsl as any;
      const rules = rulesDsl?.rules || [];
      const ruleWithId = {
        ...newRule,
        id: crypto.randomUUID(),
        priority: newRule.priority || rules.length + 1
      };
      
      rules.push(ruleWithId);

      const updatedRulesDsl = {
        ...rulesDsl,
        rules
      };

      await storage.updateLicenseRuleSet(ruleSetId, {
        rulesDsl: updatedRulesDsl
      } as any);

      // Log the addition
      await createAuditLog(req, 'rule_added', 'license_rule', ruleSetId, {
        ruleName: newRule.ruleName
      });

      res.json({ message: 'Rule added successfully', rule: ruleWithId });
    } catch (error) {
      console.error('Error adding rule:', error);
      res.status(500).json({ message: 'Failed to add rule' });
    }
  });

  // Delete a rule from a rule set (secured with full multi-tenant validation)
  app.delete('/api/contracts/:contractId/rules/:ruleSetId/rule/:ruleIndex', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId, ruleSetId, ruleIndex } = req.params;
      const index = parseInt(ruleIndex);

      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId, req.user);
      
      if (!contract) {
        return res.status(404).json({ message: error || 'Contract not found' });
      }
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }

      // Get the rule set
      const ruleSet = await storage.getLicenseRuleSet(ruleSetId);
      if (!ruleSet || ruleSet.contractId !== contractId) {
        return res.status(404).json({ message: 'Rule set not found' });
      }

      // Get current rules
      const rulesDsl = ruleSet.rulesDsl as any;
      const rules = rulesDsl?.rules || [];
      if (index < 0 || index >= rules.length) {
        return res.status(400).json({ message: 'Invalid rule index' });
      }

      // Remove the rule
      const deletedRule = rules.splice(index, 1)[0];

      const updatedRulesDsl = {
        ...rulesDsl,
        rules
      };

      await storage.updateLicenseRuleSet(ruleSetId, {
        rulesDsl: updatedRulesDsl
      } as any);

      // Log the deletion
      await createAuditLog(req, 'rule_deleted', 'license_rule', ruleSetId, {
        ruleIndex: index,
        ruleName: deletedRule.ruleName
      });

      res.json({ message: 'Rule deleted successfully' });
    } catch (error) {
      console.error('Error deleting rule:', error);
      res.status(500).json({ message: 'Failed to delete rule' });
    }
  });

  // Calculate royalties using the rules engine
  // NOTE: This route is disabled because it conflicts with the working route in routes.ts
  // and uses non-existent storage.getLicenseRuleSetsByContract() function
  // app.post('/api/contracts/:contractId/calculate-royalties', isAuthenticated, async (req: any, res) => {
  //   try {
  //     const contractId = req.params.contractId;
  //     const userId = req.user.id;
  //     const calculationInput: RoyaltyCalculationInput = req.body;

  //     // Check permissions
  //     const contract = await storage.getContract(contractId);
  //     if (!contract) {
  //       return res.status(404).json({ message: 'Contract not found' });
  //     }

  //     const userRole = (await storage.getUser(userId))?.role;
  //     const canViewAny = userRole === 'admin' || userRole === 'owner';
      
  //     if (!canViewAny && contract.uploadedBy !== userId) {
  //       return res.status(403).json({ message: 'Access denied' });
  //     }

  //     // Get rule sets for this contract
  //     const ruleSets = await storage.getLicenseRuleSetsByContract(contractId);
      
  //     if (ruleSets.length === 0) {
  //       return res.status(404).json({ message: 'No rules found for this contract' });
  //     }

  //     // Convert rule sets to RoyaltyRule format for the engine
  //     const allRules = ruleSets.flatMap(ruleSet => {
  //       const rulesDsl = ruleSet.rulesDsl as any;
  //       return (rulesDsl?.rules || []).map((rule: any) => ({
  //         id: rule.id || crypto.randomUUID(),
  //         ruleName: rule.ruleName || rule.description || 'Unnamed Rule',
  //         ruleType: rule.ruleType || 'percentage',
  //         description: rule.description || '',
  //         conditions: rule.conditions || {},
  //         calculation: rule.calculation || {},
  //         priority: rule.priority || 10,
  //         isActive: true,
  //         confidence: rule.confidence || 1.0
  //       }));
  //     });

  //     // Calculate royalties using the rules engine
  //     const result = await RulesEngine.calculateRoyalties(allRules, calculationInput);

  //     // Log the calculation
  //     await createAuditLog(req, 'royalty_calculated', 'contract', contractId, {
  //       inputData: calculationInput,
  //       totalRoyalty: result.totalRoyalty,
  //       rulesApplied: result.metadata.rulesApplied
  //     });

  //     res.json(result);
  //   } catch (error) {
  //     console.error('Error calculating royalties:', error);
  //     res.status(500).json({ message: 'Failed to calculate royalties' });
  //   }
  // });

  // Get royalty rules for a specific contract
  // Requires contractId and validates full 3-level hierarchy access (Company → BU → Location)
  app.get('/api/royalty-rules', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId } = req.query;
      
      // contractId is required for scoped access
      if (!contractId) {
        return res.status(400).json({ message: 'contractId is required' });
      }
      
      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId as string, req.user);
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }
      
      // Get rules for this specific contract
      const rules = await storage.getRoyaltyRulesByContract(contractId as string);
      const rulesWithContract = rules.map(r => ({ 
        ...r, 
        contractName: contract.displayName || contract.counterpartyName || `Contract ${contract.id.slice(0, 8)}`
      }));
      
      res.json({ rules: rulesWithContract, total: rulesWithContract.length });
    } catch (error) {
      console.error('Error fetching rules:', error);
      res.status(500).json({ message: 'Failed to fetch rules' });
    }
  });

  // Get confirmed term mappings for a specific contract
  // Requires contractId and validates full 3-level hierarchy access
  app.get('/api/confirmed-term-mappings', isAuthenticated, async (req: any, res) => {
    try {
      const { contractId } = req.query;
      
      // contractId is required for scoped access
      if (!contractId) {
        return res.status(400).json({ message: 'contractId is required' });
      }
      
      // Validate contract access with full 3-level hierarchy
      const { contract, authorized, error } = await validateContractAccess(contractId as string, req.user);
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }
      
      // Get confirmed mappings for this specific contract
      const mappings = await storage.getPendingTermMappingsByContract(contractId as string, 'confirmed');
      const mappingsWithContract = mappings.map(m => ({
        ...m,
        contractName: contract.displayName || contract.counterpartyName || `Contract ${contract.id.slice(0, 8)}`
      }));
      
      res.json({ mappings: mappingsWithContract });
    } catch (error) {
      console.error('Error fetching confirmed mappings:', error);
      res.status(500).json({ message: 'Failed to fetch confirmed mappings' });
    }
  });

  // Update a confirmed term mapping
  // Validates full 3-level hierarchy access via validateContractAccess
  app.patch('/api/confirmed-term-mappings/:id', isAuthenticated, async (req: any, res) => {
    try {
      const { id } = req.params;
      const { contractTerm, erpFieldName } = req.body;
      
      // Validate required fields
      if (!contractTerm && !erpFieldName) {
        return res.status(400).json({ message: 'At least one field (contractTerm or erpFieldName) is required' });
      }
      
      // Verify the mapping exists (uniform 403 if not to prevent enumeration)
      const mapping = await storage.getPendingTermMapping(id);
      if (!mapping) {
        return res.status(403).json({ message: 'Access denied' });
      }
      
      // Validate contract access with full 3-level hierarchy
      const { authorized, error } = await validateContractAccess(mapping.contractId, req.user);
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }
      
      // Build update object with only provided fields
      const updates: any = {};
      if (contractTerm !== undefined) updates.contractTerm = contractTerm;
      if (erpFieldName !== undefined) updates.erpFieldName = erpFieldName;
      
      const updatedMapping = await storage.updatePendingTermMapping(id, updates);
      
      await createAuditLog(req, 'mapping_updated', 'term_mapping', id, {
        contractId: mapping.contractId,
        updates
      });
      
      res.json({ mapping: updatedMapping });
    } catch (error) {
      console.error('Error updating mapping:', error);
      res.status(500).json({ message: 'Failed to update mapping' });
    }
  });

  // Delete a confirmed term mapping
  // Validates full 3-level hierarchy access via validateContractAccess (which checks admin role)
  app.delete('/api/confirmed-term-mappings/:id', isAuthenticated, async (req: any, res) => {
    try {
      const { id } = req.params;
      
      // Verify the mapping exists (uniform 403 if not to prevent enumeration)
      const mapping = await storage.getPendingTermMapping(id);
      if (!mapping) {
        return res.status(403).json({ message: 'Access denied' });
      }
      
      // Validate contract access with full 3-level hierarchy
      // validateContractAccess checks both tenant scope AND admin role requirements
      const { contract, authorized, error } = await validateContractAccess(mapping.contractId, req.user);
      
      if (!authorized) {
        return res.status(403).json({ message: error || 'Access denied' });
      }
      
      await storage.deletePendingTermMapping(id);
      
      await createAuditLog(req, 'mapping_deleted', 'term_mapping', id, {
        contractId: mapping.contractId
      });
      
      res.json({ message: 'Mapping deleted successfully' });
    } catch (error) {
      console.error('Error deleting mapping:', error);
      res.status(500).json({ message: 'Failed to delete mapping' });
    }
  });
}