import { db } from '../db';
import { 
  calculationBlueprints, 
  blueprintDimensions, 
  royaltyRules, 
  pendingTermMappings,
  erpMappingRuleSets,
  erpMappingRules,
  contracts,
  orgCalculationSettings
} from '@shared/schema';
import { eq, and, inArray, sql } from 'drizzle-orm';

interface MaterializationResult {
  blueprintId: string;
  ruleName: string;
  ruleType: string;
  isFullyMapped: boolean;
  unmappedFields: string[];
  dimensionCount: number;
}

interface MaterializationSummary {
  contractId: string;
  blueprintsCreated: number;
  fullyMapped: number;
  partiallyMapped: number;
  results: MaterializationResult[];
}

/**
 * Blueprint Materializer Service
 * 
 * Creates executable calculation blueprints by merging:
 * 1. Manual royalty rules (calculation logic)
 * 2. Confirmed ERP field mappings (data bindings)
 * 
 * The resulting blueprints can be executed by DynamicRulesEngine
 * based on the organization's calculation approach setting.
 */
export class BlueprintMaterializerService {
  
  /**
   * Check if ERP-based calculation is enabled for a company
   */
  async isErpCalculationEnabled(companyId: string): Promise<boolean> {
    const settings = await db
      .select()
      .from(orgCalculationSettings)
      .where(eq(orgCalculationSettings.companyId, companyId))
      .limit(1);
    
    if (settings.length === 0) return false;
    
    const approach = settings[0].calculationApproach;
    return approach === 'erp_mapping_rules' || approach === 'erp_rules' || approach === 'hybrid';
  }

  /**
   * Get calculation approach for a company
   */
  async getCalculationApproach(companyId: string): Promise<string> {
    const settings = await db
      .select()
      .from(orgCalculationSettings)
      .where(eq(orgCalculationSettings.companyId, companyId))
      .limit(1);
    
    return settings.length > 0 ? settings[0].calculationApproach : 'manual';
  }

  /**
   * Materialize blueprints for all rules in a contract
   */
  async materializeForContract(contractId: string): Promise<MaterializationSummary> {
    console.log(`📐 [MATERIALIZER] Starting materialization for contract ${contractId}`);
    
    // Get contract info
    const contract = await db
      .select({
        id: contracts.id,
        companyId: contracts.companyId,
        displayName: contracts.displayName,
      })
      .from(contracts)
      .where(eq(contracts.id, contractId))
      .limit(1);

    if (contract.length === 0) {
      throw new Error(`Contract not found: ${contractId}`);
    }

    const companyId = contract[0].companyId;
    if (!companyId) {
      throw new Error(`Contract ${contractId} has no company association`);
    }

    // Check if ERP calculation is enabled
    const erpEnabled = await this.isErpCalculationEnabled(companyId);
    if (!erpEnabled) {
      console.log(`⚠️ [MATERIALIZER] ERP calculation not enabled for company ${companyId}`);
      return {
        contractId,
        blueprintsCreated: 0,
        fullyMapped: 0,
        partiallyMapped: 0,
        results: [],
      };
    }

    // Get all active royalty rules for this contract
    const rules = await db
      .select()
      .from(royaltyRules)
      .where(and(
        eq(royaltyRules.contractId, contractId),
        eq(royaltyRules.isActive, true)
      ));

    console.log(`📋 [MATERIALIZER] Found ${rules.length} active royalty rules`);

    // Get all confirmed ERP mappings for this contract
    const confirmedMappings = await db
      .select()
      .from(pendingTermMappings)
      .where(and(
        eq(pendingTermMappings.contractId, contractId),
        eq(pendingTermMappings.status, 'confirmed')
      ));

    console.log(`🔗 [MATERIALIZER] Found ${confirmedMappings.length} confirmed ERP mappings`);

    // Get active ERP rule set for this company
    const erpRuleSet = await db
      .select()
      .from(erpMappingRuleSets)
      .where(and(
        eq(erpMappingRuleSets.companyId, companyId),
        eq(erpMappingRuleSets.status, 'active')
      ))
      .limit(1);

    const erpRuleSetId = erpRuleSet.length > 0 ? erpRuleSet[0].id : null;

    // Delete existing blueprints for this contract (re-materialization)
    await db
      .delete(calculationBlueprints)
      .where(eq(calculationBlueprints.contractId, contractId));

    const results: MaterializationResult[] = [];

    // Create a blueprint for each royalty rule
    for (const rule of rules) {
      const result = await this.materializeRule(rule, confirmedMappings, companyId, erpRuleSetId);
      results.push(result);
    }

    const fullyMapped = results.filter(r => r.isFullyMapped).length;
    const partiallyMapped = results.filter(r => !r.isFullyMapped && r.dimensionCount > 0).length;

    console.log(`✅ [MATERIALIZER] Created ${results.length} blueprints (${fullyMapped} fully mapped, ${partiallyMapped} partially mapped)`);

    return {
      contractId,
      blueprintsCreated: results.length,
      fullyMapped,
      partiallyMapped,
      results,
    };
  }

  /**
   * Materialize a single royalty rule into a calculation blueprint
   */
  private async materializeRule(
    rule: typeof royaltyRules.$inferSelect,
    mappings: (typeof pendingTermMappings.$inferSelect)[],
    companyId: string,
    erpRuleSetId: string | null
  ): Promise<MaterializationResult> {
    
    // Extract dimensions from the rule (product categories, territories, etc.)
    const dimensions = this.extractDimensionsFromRule(rule);
    
    // Build ERP field bindings from confirmed mappings
    const erpFieldBindings: Record<string, string> = {};
    const dualTerminologyMap: Record<string, string> = {};
    const unmappedFields: string[] = [];
    
    for (const dim of dimensions) {
      // Find a matching ERP mapping for this dimension's value
      // Using originalTerm and originalValue which are the actual field names in pendingTermMappings
      const mapping = mappings.find(m => 
        m.originalTerm?.toLowerCase() === dim.value.toLowerCase() ||
        m.originalValue?.toLowerCase() === dim.value.toLowerCase() ||
        m.originalTerm?.includes(dim.value) ||
        m.originalValue?.includes(dim.value) ||
        dim.value.includes(m.originalTerm || '') ||
        dim.value.includes(m.originalValue || '')
      );
      
      if (mapping && mapping.erpFieldName) {
        erpFieldBindings[dim.type] = mapping.erpFieldName;
        dualTerminologyMap[dim.value] = `${dim.value} (ERP: ${mapping.erpFieldName})`;
        dim.isMapped = true;
        dim.erpFieldName = mapping.erpFieldName;
        dim.mappingId = mapping.id;
        dim.confidence = String(mapping.confidence);
      } else {
        unmappedFields.push(`${dim.type}: ${dim.value}`);
      }
    }

    // Build calculation logic (merge rule formula with ERP bindings)
    const calculationLogic = this.buildCalculationLogic(rule, erpFieldBindings);
    
    // Build matching criteria (how to match sales data to this blueprint)
    const matchingCriteria = this.buildMatchingCriteria(dimensions.filter(d => d.isMapped));

    const isFullyMapped = unmappedFields.length === 0 && dimensions.length > 0;

    // Insert the blueprint using raw SQL (tables created manually)
    const blueprintResult = await db.execute(sql`
      INSERT INTO calculation_blueprints (
        contract_id, company_id, royalty_rule_id, erp_rule_set_id, name, description,
        rule_type, calculation_logic, erp_field_bindings, dual_terminology_map,
        matching_criteria, priority, status, is_fully_mapped, unmapped_fields
      ) VALUES (
        ${rule.contractId}, ${companyId}, ${rule.id}, ${erpRuleSetId}, ${rule.ruleName},
        ${rule.description || `Blueprint for ${rule.ruleName}`}, ${rule.ruleType},
        ${JSON.stringify(calculationLogic)}::jsonb, ${JSON.stringify(erpFieldBindings)}::jsonb,
        ${JSON.stringify(dualTerminologyMap)}::jsonb, ${JSON.stringify(matchingCriteria)}::jsonb,
        ${rule.priority || 10}, 'active', ${isFullyMapped},
        ${unmappedFields.length > 0 ? `{${unmappedFields.map(f => `"${f}"`).join(',')}}` : null}
      ) RETURNING id
    `);
    
    const blueprintId = (blueprintResult.rows[0] as any).id;

    // Insert dimension records using raw SQL
    for (const dim of dimensions) {
      await db.execute(sql`
        INSERT INTO blueprint_dimensions (
          blueprint_id, dimension_type, contract_term, erp_field_name,
          mapping_id, match_value, is_mapped, confidence
        ) VALUES (
          ${blueprintId}, ${dim.type}, ${dim.value}, ${dim.erpFieldName || null},
          ${dim.mappingId || null}, ${dim.value}, ${dim.isMapped},
          ${dim.confidence ? parseFloat(dim.confidence) : null}
        )
      `);
    }
    
    const blueprint = { id: blueprintId };

    console.log(`📐 [MATERIALIZER] Created blueprint "${rule.ruleName}" (${dimensions.length} dimensions, ${isFullyMapped ? 'fully' : 'partially'} mapped)`);

    return {
      blueprintId: blueprint.id,
      ruleName: rule.ruleName,
      ruleType: rule.ruleType,
      isFullyMapped,
      unmappedFields,
      dimensionCount: dimensions.length,
    };
  }

  /**
   * Extract dimensional constraints from a royalty rule
   */
  private extractDimensionsFromRule(rule: typeof royaltyRules.$inferSelect): Array<{
    type: string;
    value: string;
    isMapped: boolean;
    erpFieldName?: string;
    mappingId?: string;
    confidence?: string | null;
  }> {
    const dimensions: Array<{
      type: string;
      value: string;
      isMapped: boolean;
      erpFieldName?: string;
      mappingId?: string;
      confidence?: string | null;
    }> = [];

    // Extract product categories
    if (rule.productCategories && rule.productCategories.length > 0) {
      for (const category of rule.productCategories) {
        dimensions.push({
          type: 'product',
          value: category,
          isMapped: false,
        });
      }
    }

    // Extract territories
    if (rule.territories && rule.territories.length > 0) {
      for (const territory of rule.territories) {
        dimensions.push({
          type: 'territory',
          value: territory,
          isMapped: false,
        });
      }
    }

    // Extract container sizes
    if (rule.containerSizes && rule.containerSizes.length > 0) {
      for (const size of rule.containerSizes) {
        dimensions.push({
          type: 'container_size',
          value: size,
          isMapped: false,
        });
      }
    }

    // If rule has formula definition, extract field references
    if (rule.formulaDefinition) {
      const formula = rule.formulaDefinition as any;
      if (formula.variables) {
        for (const [varName, varDef] of Object.entries(formula.variables as Record<string, any>)) {
          if (varDef.source === 'sales_data' && varDef.field) {
            dimensions.push({
              type: 'sales_field',
              value: varDef.field,
              isMapped: false,
            });
          }
        }
      }
    }

    return dimensions;
  }

  /**
   * Build executable calculation logic from rule + ERP bindings
   */
  private buildCalculationLogic(
    rule: typeof royaltyRules.$inferSelect,
    erpFieldBindings: Record<string, string>
  ): object {
    return {
      ruleType: rule.ruleType,
      baseRate: rule.baseRate,
      minimumGuarantee: rule.minimumGuarantee,
      volumeTiers: rule.volumeTiers,
      seasonalAdjustments: rule.seasonalAdjustments,
      territoryPremiums: rule.territoryPremiums,
      formulaDefinition: rule.formulaDefinition,
      calculationFormula: rule.calculationFormula,
      erpFieldBindings,
      // Add ERP-aware selectors
      erpSelectors: Object.entries(erpFieldBindings).map(([dimension, erpField]) => ({
        dimension,
        erpField,
        matchType: 'equals', // Default match type
      })),
    };
  }

  /**
   * Build matching criteria for sales data filtering
   */
  private buildMatchingCriteria(mappedDimensions: Array<{
    type: string;
    value: string;
    erpFieldName?: string;
  }>): object {
    return {
      dimensions: mappedDimensions.map(dim => ({
        erpField: dim.erpFieldName,
        matchValue: dim.value,
        dimensionType: dim.type,
      })),
      matchMode: 'all', // All dimensions must match
    };
  }

  /**
   * Get blueprints for a contract
   */
  async getBlueprintsForContract(contractId: string): Promise<(typeof calculationBlueprints.$inferSelect)[]> {
    return db
      .select()
      .from(calculationBlueprints)
      .where(and(
        eq(calculationBlueprints.contractId, contractId),
        eq(calculationBlueprints.status, 'active')
      ));
  }

  /**
   * Get blueprint with dimensions
   */
  async getBlueprintWithDimensions(blueprintId: string) {
    const blueprint = await db
      .select()
      .from(calculationBlueprints)
      .where(eq(calculationBlueprints.id, blueprintId))
      .limit(1);

    if (blueprint.length === 0) return null;

    const dimensions = await db
      .select()
      .from(blueprintDimensions)
      .where(eq(blueprintDimensions.blueprintId, blueprintId));

    return {
      ...blueprint[0],
      dimensions,
    };
  }

  /**
   * Trigger re-materialization when mappings are confirmed
   */
  async onMappingsConfirmed(contractId: string): Promise<MaterializationSummary> {
    console.log(`🔄 [MATERIALIZER] Mappings confirmed - re-materializing for contract ${contractId}`);
    return this.materializeForContract(contractId);
  }
}

export const blueprintMaterializerService = new BlueprintMaterializerService();
