/**
 * ERP Rule Generation Service
 * Auto-generates ERP Mapping Rules when field mappings are confirmed
 * 
 * Flow: Confirmed Mapping → Auto-Generate Rule → Ready for Calculation
 * 
 * Rules include dual terminology: "Contract Term (ERP: FIELD_NAME)"
 */

import { db } from "../db";
import { 
  pendingTermMappings, 
  erpMappingRuleSets, 
  erpMappingRules,
  erpMappingOutputs,
  orgCalculationSettings,
  contracts,
} from "@shared/schema";
import { eq, and, sql } from "drizzle-orm";

export interface GeneratedRule {
  ruleSetId: string;
  ruleName: string;
  ruleDescription: string;
  contractTerm: string;
  erpFieldName: string;
  erpEntityName: string;
  dualTerminologyDisplay: string;
}

/**
 * Check if organization has ERP-based calculation enabled
 */
export async function isErpCalculationEnabled(companyId: string): Promise<boolean> {
  const settings = await db
    .select()
    .from(orgCalculationSettings)
    .where(eq(orgCalculationSettings.companyId, companyId))
    .limit(1);

  if (settings.length === 0) {
    return false;
  }

  const approach = settings[0].calculationApproach;
  // Accept multiple ERP-based approaches: erp_rules, erp_mapping_rules, hybrid
  return approach === 'erp_rules' || approach === 'erp_mapping_rules' || approach === 'hybrid';
}

/**
 * Get or create a rule set for a contract's ERP mappings
 */
async function getOrCreateRuleSetForContract(
  contractId: string,
  erpSystemId: string,
  userId: string
): Promise<string> {
  const contract = await db
    .select({
      id: contracts.id,
      displayName: contracts.displayName,
      companyId: contracts.companyId,
    })
    .from(contracts)
    .where(eq(contracts.id, contractId))
    .limit(1);

  if (contract.length === 0) {
    throw new Error(`Contract not found: ${contractId}`);
  }

  const contractData = contract[0];
  const contractName = contractData.displayName || 'Contract';
  const ruleSetName = `Auto-Rules: ${contractName}`;

  const existingRuleSet = await db
    .select()
    .from(erpMappingRuleSets)
    .where(and(
      eq(erpMappingRuleSets.companyId, contractData.companyId!),
      eq(erpMappingRuleSets.sourceSystemId, erpSystemId),
      sql`${erpMappingRuleSets.name} LIKE ${'Auto-Rules:%'}`
    ))
    .limit(1);

  if (existingRuleSet.length > 0) {
    return existingRuleSet[0].id;
  }

  const [newRuleSet] = await db.insert(erpMappingRuleSets).values({
    name: ruleSetName,
    companyId: contractData.companyId!,
    sourceSystemId: erpSystemId,
    status: 'active', // Auto-generated rule sets are active by default
  }).returning();

  console.log(`✅ [RULE GENERATION] Created new rule set: ${ruleSetName} (${newRuleSet.id})`);
  return newRuleSet.id;
}

/**
 * Generate a rule from a confirmed mapping
 */
export async function generateRuleFromMapping(
  mappingId: string,
  userId: string
): Promise<GeneratedRule | null> {
  const mapping = await db
    .select()
    .from(pendingTermMappings)
    .where(eq(pendingTermMappings.id, mappingId))
    .limit(1);

  if (mapping.length === 0) {
    console.log(`⚠️ [RULE GENERATION] Mapping not found: ${mappingId}`);
    return null;
  }

  const mappingData = mapping[0];

  if (mappingData.status !== 'confirmed') {
    console.log(`⚠️ [RULE GENERATION] Mapping not confirmed: ${mappingId}`);
    return null;
  }

  if (!mappingData.erpSystemId || !mappingData.erpFieldName) {
    console.log(`⚠️ [RULE GENERATION] Missing ERP info for mapping: ${mappingId}`);
    return null;
  }

  const contract = await db
    .select({ companyId: contracts.companyId })
    .from(contracts)
    .where(eq(contracts.id, mappingData.contractId))
    .limit(1);

  if (contract.length === 0 || !contract[0].companyId) {
    console.log(`⚠️ [RULE GENERATION] Contract/company not found for mapping: ${mappingId}`);
    return null;
  }

  const isEnabled = await isErpCalculationEnabled(contract[0].companyId);
  if (!isEnabled) {
    console.log(`ℹ️ [RULE GENERATION] ERP calculation not enabled for company, skipping rule generation`);
    return null;
  }

  const ruleSetId = await getOrCreateRuleSetForContract(
    mappingData.contractId,
    mappingData.erpSystemId,
    userId
  );

  const dualTerminology = `${mappingData.originalTerm} (ERP: ${mappingData.erpFieldName})`;
  const ruleName = `Map ${mappingData.originalTerm} → ${mappingData.erpFieldName}`;
  
  const existingRule = await db
    .select()
    .from(erpMappingRules)
    .where(and(
      eq(erpMappingRules.ruleSetId, ruleSetId),
      eq(erpMappingRules.sourceField, mappingData.originalTerm || '')
    ))
    .limit(1);

  let ruleId: string;
  
  if (existingRule.length > 0) {
    ruleId = existingRule[0].id;
    await db.update(erpMappingRules)
      .set({
        targetField: mappingData.erpFieldName,
        description: dualTerminology,
        transformationConfig: {
          contractTerm: mappingData.originalTerm,
          contractValue: mappingData.originalValue,
          erpEntity: mappingData.erpEntityName,
          erpField: mappingData.erpFieldName,
          confidence: mappingData.confidence,
          sourceMapping: mappingId,
          dualTerminology: dualTerminology,
        },
        updatedAt: new Date(),
      })
      .where(eq(erpMappingRules.id, ruleId));
    console.log(`✏️ [RULE GENERATION] Updated existing rule: ${ruleName}`);
  } else {
    const [newRule] = await db.insert(erpMappingRules).values({
      ruleSetId: ruleSetId,
      name: ruleName,
      description: dualTerminology,
      sourceField: mappingData.originalTerm || '',
      targetField: mappingData.erpFieldName || '',
      transformationConfig: {
        contractTerm: mappingData.originalTerm,
        contractValue: mappingData.originalValue,
        erpEntity: mappingData.erpEntityName,
        erpField: mappingData.erpFieldName,
        confidence: mappingData.confidence,
        sourceMapping: mappingId,
        dualTerminology: dualTerminology,
      },
    }).returning();
    ruleId = newRule.id;
    console.log(`✅ [RULE GENERATION] Created new rule: ${ruleName}`);
  }

  if (mappingData.originalValue) {
    const existingOutput = await db
      .select()
      .from(erpMappingOutputs)
      .where(and(
        eq(erpMappingOutputs.ruleId, ruleId),
        eq(erpMappingOutputs.outputField, mappingData.erpFieldName || '')
      ))
      .limit(1);

    if (existingOutput.length === 0) {
      await db.insert(erpMappingOutputs).values({
        ruleId: ruleId,
        outputField: mappingData.erpFieldName || '',
        calculationType: 'direct',
      });
      console.log(`✅ [RULE GENERATION] Created output for: ${mappingData.erpFieldName}`);
    }
  }

  return {
    ruleSetId,
    ruleName,
    ruleDescription: dualTerminology,
    contractTerm: mappingData.originalTerm || '',
    erpFieldName: mappingData.erpFieldName || '',
    erpEntityName: mappingData.erpEntityName || '',
    dualTerminologyDisplay: dualTerminology,
  };
}

/**
 * Generate rules for multiple confirmed mappings (bulk operation)
 */
export async function generateRulesFromMappings(
  mappingIds: string[],
  userId: string
): Promise<GeneratedRule[]> {
  const results: GeneratedRule[] = [];

  for (const mappingId of mappingIds) {
    try {
      const rule = await generateRuleFromMapping(mappingId, userId);
      if (rule) {
        results.push(rule);
      }
    } catch (error) {
      console.error(`❌ [RULE GENERATION] Error generating rule for mapping ${mappingId}:`, error);
    }
  }

  console.log(`✅ [RULE GENERATION] Generated ${results.length} rules from ${mappingIds.length} mappings`);
  return results;
}

/**
 * Get all generated rules for a contract with dual terminology display
 */
export async function getContractErpRules(contractId: string): Promise<any[]> {
  const contract = await db
    .select({ companyId: contracts.companyId, displayName: contracts.displayName })
    .from(contracts)
    .where(eq(contracts.id, contractId))
    .limit(1);

  if (contract.length === 0 || !contract[0].companyId) {
    return [];
  }

  const ruleSets = await db
    .select()
    .from(erpMappingRuleSets)
    .where(and(
      eq(erpMappingRuleSets.companyId, contract[0].companyId),
      eq(erpMappingRuleSets.status, 'active')
    ));

  const allRules = [];
  for (const ruleSet of ruleSets) {
    const rules = await db
      .select()
      .from(erpMappingRules)
      .where(and(
        eq(erpMappingRules.ruleSetId, ruleSet.id),
        eq(erpMappingRules.isActive, true)
      ));

    for (const rule of rules) {
      const dualTerminology = `${rule.sourceField} (ERP: ${rule.targetField})`;
      allRules.push({
        ...rule,
        ruleSetName: ruleSet.name,
        dualTerminology: dualTerminology,
      });
    }
  }

  return allRules;
}
