import { db } from '../db';
import { ruleDefinitions, pendingTermMappings } from '@shared/schema';
import { ExtractedEntity } from './zeroShotExtractionService';
import { GroqService } from './groqService';
import { eq, and } from 'drizzle-orm';

/**
 * Rule Synthesis Service
 * 
 * Dynamically generates FormulaNode expression trees from contract entities.
 * Handles ANY royalty structure - percentages, fixed fees, tiers, hybrids, seasonal, etc.
 * 
 * Enhanced with dual terminology support - displays both contract terms and ERP field names.
 */

// Create a reusable Groq instance
const groqService = new GroqService();

// Helper function to call Groq with simple interface
async function callGroq(prompt: string, options: { temperature?: number; maxTokens?: number } = {}): Promise<string> {
  const messages = [
    { role: 'system' as const, content: 'You are a royalty calculation expert. Always respond with valid JSON.' },
    { role: 'user' as const, content: prompt }
  ];
  
  const response = await (groqService as any).makeRequest(messages, options.temperature || 0.1, options.maxTokens || 2000);
  return response;
}

export interface FormulaNode {
  type: string; // 'percentage', 'fixed', 'tier', 'conditional', 'arithmetic', etc.
  [key: string]: any; // Flexible properties based on type
}

export interface TermMapping {
  contractTerm: string;
  erpFieldName: string;
  erpEntityName?: string;
  confidence: number;
}

export interface SynthesizedRule {
  id?: string;
  ruleType: string;
  ruleName: string;
  description: string;
  formulaDefinition: FormulaNode;
  applicabilityFilters: Record<string, any>;
  confidence: number;
  linkedNodeId?: string;
  termMappings?: TermMapping[];
}

export interface RuleSynthesisResult {
  rules: SynthesizedRule[];
  lowConfidenceRules: SynthesizedRule[];
  averageConfidence: number;
}

const CONFIDENCE_THRESHOLD = 0.70;

/**
 * Synthesize royalty rules from extracted entities
 */
export async function synthesizeRules(
  entities: ExtractedEntity[],
  graphNodes: any[],
  contractId: string,
  runId: string
): Promise<RuleSynthesisResult> {
  console.log(`[RuleSynthesis] Synthesizing rules from ${entities.length} entities`);

  // Find royalty-related entities
  const royaltyEntities = entities.filter(e => 
    e.type.toLowerCase().includes('royalty') ||
    e.type.toLowerCase().includes('payment') ||
    e.type.toLowerCase().includes('fee') ||
    (e.properties.rate !== undefined) ||
    (e.properties.percentage !== undefined)
  );

  console.log(`[RuleSynthesis] Found ${royaltyEntities.length} royalty-related entities`);

  if (royaltyEntities.length === 0) {
    // Try to synthesize from context
    return await synthesizeRulesFromContext(entities, graphNodes, contractId, runId);
  }

  const rules: SynthesizedRule[] = [];
  const lowConfidenceRules: SynthesizedRule[] = [];

  for (const entity of royaltyEntities) {
    try {
      let synthesizedRule = await synthesizeRuleFromEntity(entity, graphNodes);
      
      if (synthesizedRule) {
        // Enrich rule with ERP term mappings for dual terminology
        synthesizedRule = await enrichRuleWithErpMappings(synthesizedRule, contractId);
        
        // Insert into database with term mappings stored in formulaDefinition metadata
        const formulaWithMappings = synthesizedRule.termMappings?.length 
          ? { ...synthesizedRule.formulaDefinition, _termMappings: synthesizedRule.termMappings }
          : synthesizedRule.formulaDefinition;
        
        const [dbRule] = await db.insert(ruleDefinitions).values({
          contractId,
          extractionRunId: runId,
          linkedGraphNodeId: synthesizedRule.linkedNodeId,
          ruleType: synthesizedRule.ruleType,
          ruleName: synthesizedRule.ruleName,
          description: synthesizedRule.description,
          formulaDefinition: formulaWithMappings,
          applicabilityFilters: synthesizedRule.applicabilityFilters,
          confidence: synthesizedRule.confidence.toFixed(2),
          validationStatus: synthesizedRule.confidence >= CONFIDENCE_THRESHOLD ? 'validated' : 'pending',
          isActive: synthesizedRule.confidence >= CONFIDENCE_THRESHOLD,
        }).returning();

        synthesizedRule.id = dbRule.id;
        rules.push(synthesizedRule);

        if (synthesizedRule.confidence < CONFIDENCE_THRESHOLD) {
          lowConfidenceRules.push(synthesizedRule);
        }
        
        if (synthesizedRule.termMappings?.length) {
          console.log(`[RuleSynthesis] Rule enriched with ${synthesizedRule.termMappings.length} ERP term mappings`);
        }
      }

    } catch (error) {
      console.error(`[RuleSynthesis] Failed to synthesize rule from entity ${entity.label}:`, error);
    }
  }

  const averageConfidence = rules.length > 0
    ? rules.reduce((sum, r) => sum + r.confidence, 0) / rules.length
    : 0;

  console.log(`[RuleSynthesis] ✓ Synthesized ${rules.length} rules`);
  console.log(`[RuleSynthesis] Average confidence: ${(averageConfidence * 100).toFixed(1)}%`);

  return { rules, lowConfidenceRules, averageConfidence };
}

/**
 * Synthesize a single rule from an entity using LLM
 */
async function synthesizeRuleFromEntity(
  entity: ExtractedEntity,
  graphNodes: any[]
): Promise<SynthesizedRule | null> {
  const prompt = `You are a royalty calculation expert. Convert this extracted entity into a FormulaNode expression tree.

ENTITY:
${JSON.stringify(entity, null, 2)}

AVAILABLE FORMULA NODE TYPES:
- percentage: { type: "percentage", rate: 0.15, base: "netSales" }
- fixed: { type: "fixed", amount: 1000, currency: "USD" }
- tier: { type: "tier", tiers: [{ min: 0, max: 10000, rate: 0.10 }, { min: 10000, max: null, rate: 0.15 }] }
- conditional: { type: "conditional", condition: { field: "territory", operator: "equals", value: "US" }, trueFormula: {...}, falseFormula: {...} }
- arithmetic: { type: "arithmetic", operator: "+", operands: [{...}, {...}] }
- minimum: { type: "minimum", amount: 500 }
- maximum: { type: "maximum", amount: 100000 }

Create a FormulaNode tree that represents this royalty rule.

Respond with JSON:
{
  "ruleType": "inferred type (e.g., 'percentage_of_sales', 'tiered_volume', 'fixed_quarterly')",
  "ruleName": "descriptive name",
  "description": "what this rule does",
  "formulaDefinition": { FormulaNode tree },
  "applicabilityFilters": { "product": "...", "territory": "...", etc. },
  "confidence": 0.85
}`;

  try {
    const response = await callGroq(prompt, { temperature: 0.1, maxTokens: 1500 });
    
    const jsonMatch = response.match(/```json\s*([\s\S]*?)\s*```/) || 
                     response.match(/```\s*([\s\S]*?)\s*```/);
    const jsonText = jsonMatch ? jsonMatch[1] : response;
    const result = JSON.parse(jsonText);

    // Find linked graph node
    const linkedNode = graphNodes.find(n => n.label === entity.label);

    return {
      ruleType: result.ruleType,
      ruleName: result.ruleName,
      description: result.description,
      formulaDefinition: result.formulaDefinition,
      applicabilityFilters: result.applicabilityFilters || {},
      confidence: parseFloat(result.confidence) || entity.confidence,
      linkedNodeId: linkedNode?.id,
    };

  } catch (error) {
    console.error(`[RuleSynthesis] LLM synthesis failed for ${entity.label}:`, error);
    return null;
  }
}

/**
 * Fallback: Synthesize rules from general context when no explicit royalty entities found
 */
async function synthesizeRulesFromContext(
  entities: ExtractedEntity[],
  graphNodes: any[],
  contractId: string,
  runId: string
): Promise<RuleSynthesisResult> {
  console.log(`[RuleSynthesis] No explicit royalty entities found, analyzing context...`);

  const contextPrompt = `Based on these contract entities, infer likely royalty rules.

ENTITIES:
${JSON.stringify(entities.slice(0, 20), null, 2)}

Even if no explicit royalty clauses are mentioned, infer reasonable rules based on:
- Contract type
- Payment terms mentioned
- Product/service pricing
- Industry standards

Respond with JSON array of rules (may be empty if truly no royalty structure):
[
  {
    "ruleType": "...",
    "ruleName": "...",
    "description": "...",
    "formulaDefinition": {...},
    "applicabilityFilters": {},
    "confidence": 0.5
  }
]`;

  try {
    const response = await callGroq(contextPrompt, { temperature: 0.2, maxTokens: 2000 });
    const jsonMatch = response.match(/```json\s*([\s\S]*?)\s*```/) || 
                     response.match(/```\s*([\s\S]*?)\s*```/) ||
                     response.match(/\[[\s\S]*\]/);
    const jsonText = jsonMatch ? (jsonMatch[1] || jsonMatch[0]) : response;
    const inferredRules = JSON.parse(jsonText);

    const rules: SynthesizedRule[] = [];
    const lowConfidenceRules: SynthesizedRule[] = [];

    for (const rule of inferredRules) {
      // Enrich rule with ERP term mappings for dual terminology
      let synthesizedRule: SynthesizedRule = {
        ...rule,
        confidence: parseFloat(rule.confidence) * 0.8,
      };
      synthesizedRule = await enrichRuleWithErpMappings(synthesizedRule, contractId);
      
      // Insert into database with term mappings stored in formulaDefinition metadata
      const formulaWithMappings = synthesizedRule.termMappings?.length 
        ? { ...synthesizedRule.formulaDefinition, _termMappings: synthesizedRule.termMappings }
        : synthesizedRule.formulaDefinition;
      
      const [dbRule] = await db.insert(ruleDefinitions).values({
        contractId,
        extractionRunId: runId,
        ruleType: rule.ruleType,
        ruleName: rule.ruleName,
        description: rule.description,
        formulaDefinition: formulaWithMappings,
        applicabilityFilters: rule.applicabilityFilters || {},
        confidence: (parseFloat(rule.confidence) * 0.8).toFixed(2), // Reduce confidence for inferred rules
        validationStatus: 'pending',
        isActive: false,
      }).returning();

      synthesizedRule.id = dbRule.id;

      rules.push(synthesizedRule);
      lowConfidenceRules.push(synthesizedRule); // All inferred rules need review
      
      if (synthesizedRule.termMappings?.length) {
        console.log(`[RuleSynthesis] Inferred rule enriched with ${synthesizedRule.termMappings.length} ERP term mappings`);
      }
    }

    return {
      rules,
      lowConfidenceRules,
      averageConfidence: rules.length > 0
        ? rules.reduce((sum, r) => sum + r.confidence, 0) / rules.length
        : 0,
    };

  } catch (error) {
    console.error('[RuleSynthesis] Context-based synthesis failed:', error);
    return { rules: [], lowConfidenceRules: [], averageConfidence: 0 };
  }
}

/**
 * Get confirmed ERP term mappings for a contract
 * Returns dual terminology data (contract term + ERP field name)
 */
export async function getConfirmedTermMappings(contractId: string): Promise<TermMapping[]> {
  try {
    const mappings = await db
      .select()
      .from(pendingTermMappings)
      .where(
        and(
          eq(pendingTermMappings.contractId, contractId),
          eq(pendingTermMappings.status, 'confirmed')
        )
      );
    
    return mappings.map(m => ({
      contractTerm: m.originalTerm,
      erpFieldName: m.erpFieldName,
      erpEntityName: m.erpEntityName || undefined,
      confidence: parseFloat(String(m.confidence || 0)),
    }));
  } catch (error) {
    console.error('[RuleSynthesis] Failed to get confirmed term mappings:', error);
    return [];
  }
}

/**
 * Enrich a rule with ERP term mappings for dual terminology display
 * Looks for term matches in the rule's formula and adds the corresponding ERP field names
 */
export async function enrichRuleWithErpMappings(
  rule: SynthesizedRule,
  contractId: string
): Promise<SynthesizedRule> {
  const confirmedMappings = await getConfirmedTermMappings(contractId);
  
  if (confirmedMappings.length === 0) {
    return rule;
  }
  
  const termMappings: TermMapping[] = [];
  
  const findTermsInFormula = (formula: any, path: string = ''): void => {
    if (!formula || typeof formula !== 'object') return;
    
    for (const [key, value] of Object.entries(formula)) {
      if (typeof value === 'string') {
        const matchingMapping = confirmedMappings.find(m => 
          m.contractTerm.toLowerCase() === value.toLowerCase() ||
          value.toLowerCase().includes(m.contractTerm.toLowerCase())
        );
        
        if (matchingMapping) {
          termMappings.push(matchingMapping);
        }
      } else if (typeof value === 'object') {
        findTermsInFormula(value, `${path}.${key}`);
      }
    }
  };
  
  findTermsInFormula(rule.formulaDefinition);
  findTermsInFormula(rule.applicabilityFilters);
  
  const uniqueMappings = termMappings.filter(
    (m, idx, arr) => arr.findIndex(x => x.contractTerm === m.contractTerm) === idx
  );
  
  return {
    ...rule,
    termMappings: uniqueMappings,
  };
}

/**
 * Format a rule term with dual terminology display
 * Returns "Contract Term (ERP: Field Name)" format
 */
export function formatDualTerminology(
  contractTerm: string,
  termMappings: TermMapping[]
): string {
  const mapping = termMappings.find(m => 
    m.contractTerm.toLowerCase() === contractTerm.toLowerCase()
  );
  
  if (mapping) {
    return `${contractTerm} (ERP: ${mapping.erpFieldName})`;
  }
  
  return contractTerm;
}
