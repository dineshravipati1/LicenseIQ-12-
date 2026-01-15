import { GroqService } from './groqService';
import { mapTermsToErpFields, loadErpVocabulary } from './erpVocabularyService';
import { storage } from '../storage';

/**
 * Zero-Shot Extraction Service
 * 
 * Uses LLMs to extract entities and relationships from contracts WITHOUT predefined schemas.
 * The AI determines what's important and structures the data dynamically.
 * 
 * Enhanced with ERP-aware extraction that maps contract terms to ERP fields.
 */

// Create a reusable Groq instance
const groqService = new GroqService();

// Helper function to call Groq with simple interface
async function callGroq(prompt: string, options: { temperature?: number; maxTokens?: number } = {}): Promise<string> {
  const messages = [
    { role: 'system' as const, content: 'You are an expert contract analysis AI. Always respond with valid JSON.' },
    { role: 'user' as const, content: prompt }
  ];
  
  const response = await (groqService as any).makeRequest(messages, options.temperature || 0.1, options.maxTokens || 2000);
  return response;
}

export interface ExtractedEntity {
  type: string; // e.g., 'party', 'product', 'territory', 'payment_term', 'royalty_clause'
  label: string; // Human-readable name
  properties: Record<string, any>; // Flexible JSON properties
  confidence: number; // 0-1
  sourceText: string; // Original text this was extracted from
}

export interface ExtractedRelationship {
  sourceLabel: string;
  targetLabel: string;
  relationshipType: string; // e.g., 'applies_to', 'references', 'requires', 'modifies'
  properties?: Record<string, any>;
  confidence: number;
}

export interface ZeroShotExtractionResult {
  entities: ExtractedEntity[];
  relationships: ExtractedRelationship[];
  confidence: number; // Overall extraction confidence
  metadata: {
    contractType: string; // Detected type: 'licensing', 'saas', 'real_estate', etc.
    keyTerms: string[];
    extractionTime: number;
  };
}

/**
 * Extract entities and relationships from contract using zero-shot prompting
 */
export async function extractContractEntitiesZeroShot(
  contractText: string,
  contractId: string
): Promise<ZeroShotExtractionResult> {
  const startTime = Date.now();

  const prompt = `You are an expert contract analysis AI. Analyze the following contract and extract ALL important entities and relationships.

CRITICAL INSTRUCTIONS:
1. Be comprehensive - extract EVERY entity that could be important (parties, products, terms, clauses, obligations, etc.)
2. Determine entity types dynamically based on what you find
3. Extract ALL properties for each entity as structured JSON
4. Identify relationships between entities
5. Provide confidence scores (0-1) for each extraction
6. DO NOT assume a fixed schema - adapt to whatever contract type this is

CONTRACT TEXT:
${contractText.substring(0, 12000)} ${contractText.length > 12000 ? '...[truncated]' : ''}

Respond ONLY with valid JSON in this exact format:
{
  "contractType": "detected contract type (e.g., 'licensing', 'saas_subscription', 'real_estate', 'manufacturing')",
  "entities": [
    {
      "type": "entity type (e.g., 'party', 'product', 'payment_term', 'royalty_clause', 'territory')",
      "label": "human-readable name",
      "properties": {
        "key1": "value1",
        "key2": "value2"
      },
      "confidence": 0.95,
      "sourceText": "exact text from contract this was extracted from"
    }
  ],
  "relationships": [
    {
      "sourceLabel": "entity label",
      "targetLabel": "entity label",
      "relationshipType": "type (e.g., 'applies_to', 'references', 'requires', 'modifies')",
      "properties": {},
      "confidence": 0.90
    }
  ],
  "keyTerms": ["list", "of", "important", "contract", "terms"],
  "overallConfidence": 0.85
}`;

  try {
    const response = await callGroq(prompt, {
      temperature: 0.1, // Low temperature for consistency
      maxTokens: 4000,
    });

    // Parse JSON response
    let result: any;
    try {
      // Extract JSON from markdown code blocks if present
      const jsonMatch = response.match(/```json\s*([\s\S]*?)\s*```/) || 
                       response.match(/```\s*([\s\S]*?)\s*```/);
      const jsonText = jsonMatch ? jsonMatch[1] : response;
      result = JSON.parse(jsonText);
    } catch (parseError) {
      console.error('[ZeroShotExtraction] Failed to parse JSON response:', response);
      throw new Error('Failed to parse extraction results as JSON');
    }

    // Validate and structure the result
    const entities: ExtractedEntity[] = (result.entities || []).map((e: any) => ({
      type: e.type || 'unknown',
      label: e.label || 'Unnamed Entity',
      properties: e.properties || {},
      confidence: parseFloat(e.confidence) || 0.5,
      sourceText: e.sourceText || '',
    }));

    const relationships: ExtractedRelationship[] = (result.relationships || []).map((r: any) => ({
      sourceLabel: r.sourceLabel,
      targetLabel: r.targetLabel,
      relationshipType: r.relationshipType || 'relates_to',
      properties: r.properties || {},
      confidence: parseFloat(r.confidence) || 0.5,
    }));

    const extractionTime = Date.now() - startTime;

    console.log(`[ZeroShotExtraction] Extracted ${entities.length} entities and ${relationships.length} relationships`);
    console.log(`[ZeroShotExtraction] Detected contract type: ${result.contractType}`);

    return {
      entities,
      relationships,
      confidence: parseFloat(result.overallConfidence) || 0.7,
      metadata: {
        contractType: result.contractType || 'unknown',
        keyTerms: result.keyTerms || [],
        extractionTime,
      },
    };

  } catch (error) {
    console.error('[ZeroShotExtraction] Extraction failed:', error);
    throw error;
  }
}

/**
 * Validate extraction with a second LLM call (cross-validation)
 */
export async function validateExtraction(
  extraction: ZeroShotExtractionResult,
  extractedRules: any[],
  originalText: string
): Promise<{ confidence: number; issues: string[]; recommendations: string[] }> {
  const validationPrompt = `You are a contract validation expert. Review this AI-extracted data and verify its accuracy.

ORIGINAL CONTRACT (excerpt):
${originalText.substring(0, 5000)}

EXTRACTED ENTITIES:
${JSON.stringify(extraction.entities.slice(0, 10), null, 2)}

EXTRACTED RELATIONSHIPS:
${JSON.stringify(extraction.relationships.slice(0, 10), null, 2)}

EXTRACTED RULES:
${JSON.stringify(extractedRules.slice(0, 5), null, 2)}

Validate:
1. Are the entities accurate and complete?
2. Are the relationships correct?
3. Are the rules properly structured?
4. What's missing or incorrect?

Respond with JSON:
{
  "overallConfidence": 0.85,
  "issues": ["list of problems found"],
  "recommendations": ["suggested improvements"],
  "validationNotes": "brief summary"
}`;

  try {
    const response = await callGroq(validationPrompt, {
      temperature: 0.1,
      maxTokens: 1500,
    });

    const jsonMatch = response.match(/```json\s*([\s\S]*?)\s*```/) || 
                     response.match(/```\s*([\s\S]*?)\s*```/);
    const jsonText = jsonMatch ? jsonMatch[1] : response;
    const result = JSON.parse(jsonText);

    return {
      confidence: parseFloat(result.overallConfidence) || 0.7,
      issues: result.issues || [],
      recommendations: result.recommendations || [],
    };

  } catch (error) {
    console.error('[ZeroShotExtraction] Validation failed:', error);
    // Return moderate confidence if validation fails
    return {
      confidence: 0.65,
      issues: ['Validation check failed'],
      recommendations: ['Manual review recommended'],
    };
  }
}

/**
 * Extract specific entity type on-demand (for focused extraction)
 */
export async function extractSpecificEntityType(
  contractText: string,
  entityType: string,
  context?: string
): Promise<ExtractedEntity[]> {
  const prompt = `Extract all "${entityType}" entities from this contract.

${context ? `Context: ${context}\n\n` : ''}

CONTRACT:
${contractText.substring(0, 8000)}

Respond with JSON array:
[
  {
    "type": "${entityType}",
    "label": "name",
    "properties": {},
    "confidence": 0.9,
    "sourceText": "excerpt"
  }
]`;

  try {
    const response = await callGroq(prompt, { temperature: 0.1, maxTokens: 2000 });
    const jsonMatch = response.match(/```json\s*([\s\S]*?)\s*```/) || 
                     response.match(/```\s*([\s\S]*?)\s*```/) ||
                     response.match(/\[[\s\S]*\]/);
    const jsonText = jsonMatch ? (jsonMatch[1] || jsonMatch[0]) : response;
    return JSON.parse(jsonText);
  } catch (error) {
    console.error(`[ZeroShotExtraction] Failed to extract ${entityType}:`, error);
    return [];
  }
}

/**
 * ERP-Aware Extraction Result
 * Extends the base extraction with ERP mapping information
 */
export interface ErpAwareExtractionResult extends ZeroShotExtractionResult {
  erpMappings: {
    mappingCount: number;
    pendingCount: number;
    autoConfirmedCount: number;
  };
}

/**
 * Perform extraction with ERP field mapping
 * 
 * This enhanced extraction:
 * 1. Performs standard zero-shot entity extraction
 * 2. Maps extracted terms to ERP fields using AI
 * 3. Stores mappings in pending_term_mappings table for user confirmation
 * 4. Auto-confirms high-confidence mappings based on threshold
 */
export async function extractWithErpMapping(
  contractText: string,
  contractId: string,
  erpSystemId: string,
  extractionRunId?: string,
  options?: {
    confidenceThreshold?: number;
    requireConfirmation?: boolean;
    autoConfirmHighConfidence?: boolean;
  }
): Promise<ErpAwareExtractionResult> {
  const {
    confidenceThreshold = 0.70,
    requireConfirmation = true,
    autoConfirmHighConfidence = true
  } = options || {};

  console.log(`[ErpAwareExtraction] Starting extraction for contract ${contractId} with ERP ${erpSystemId}`);
  
  // Step 1: Perform standard zero-shot extraction
  const baseExtraction = await extractContractEntitiesZeroShot(contractText, contractId);
  
  // Step 2: Check if ERP vocabulary is available
  const vocabulary = await loadErpVocabulary(erpSystemId);
  if (vocabulary.length === 0) {
    console.log(`[ErpAwareExtraction] No ERP vocabulary found for system ${erpSystemId}, skipping mapping`);
    return {
      ...baseExtraction,
      erpMappings: {
        mappingCount: 0,
        pendingCount: 0,
        autoConfirmedCount: 0
      }
    };
  }
  
  console.log(`[ErpAwareExtraction] Found ${vocabulary.length} ERP fields in vocabulary`);
  
  // Step 3: Convert extracted entities to terms for mapping
  const termsToMap = baseExtraction.entities.map(entity => ({
    term: entity.label,
    value: entity.properties?.value || entity.properties?.name || null,
    sourceText: entity.sourceText
  }));
  
  console.log(`[ErpAwareExtraction] Mapping ${termsToMap.length} extracted terms to ERP fields`);
  
  // Step 4: Use AI to map terms to ERP fields
  const erpMappings = await mapTermsToErpFields(termsToMap, erpSystemId);
  
  console.log(`[ErpAwareExtraction] AI mapped ${erpMappings.length} terms to ERP fields`);
  
  // Step 5: Store mappings in pending_term_mappings table
  let pendingCount = 0;
  let autoConfirmedCount = 0;
  
  for (const mapping of erpMappings) {
    if (mapping.confidence < 0.1) {
      // Skip very low confidence mappings
      continue;
    }
    
    const shouldAutoConfirm = 
      !requireConfirmation || 
      (autoConfirmHighConfidence && mapping.confidence >= confidenceThreshold);
    
    try {
      const pendingMapping = await storage.createPendingTermMapping({
        contractId,
        extractionRunId: extractionRunId || null,
        originalTerm: mapping.contractTerm,
        originalValue: mapping.contractValue,
        sourceText: mapping.sourceText,
        erpSystemId,
        erpEntityId: mapping.erpEntityId || null,
        erpFieldId: mapping.erpFieldId || null,
        erpFieldName: mapping.erpFieldName,
        erpEntityName: mapping.erpEntityName,
        confidence: mapping.confidence,
        mappingMethod: mapping.mappingMethod,
        alternativeMappings: mapping.alternatives as any,
        status: shouldAutoConfirm ? 'confirmed' : 'pending',
        confirmedBy: null, // Auto-confirmed mappings have null user - system-confirmed
        confirmedAt: shouldAutoConfirm ? new Date() : null,
      });
      
      if (shouldAutoConfirm) {
        autoConfirmedCount++;
        console.log(`[ErpAwareExtraction] Auto-confirmed: ${mapping.contractTerm} → ${mapping.erpFieldName} (${Math.round(mapping.confidence * 100)}%)`);
      } else {
        pendingCount++;
        console.log(`[ErpAwareExtraction] Pending: ${mapping.contractTerm} → ${mapping.erpFieldName} (${Math.round(mapping.confidence * 100)}%)`);
      }
    } catch (error) {
      console.error(`[ErpAwareExtraction] Failed to save mapping for ${mapping.contractTerm}:`, error);
    }
  }
  
  console.log(`[ErpAwareExtraction] Saved ${erpMappings.length} mappings (${autoConfirmedCount} auto-confirmed, ${pendingCount} pending)`);
  
  return {
    ...baseExtraction,
    erpMappings: {
      mappingCount: erpMappings.length,
      pendingCount,
      autoConfirmedCount
    }
  };
}

/**
 * Get pending mappings summary for a contract
 */
export async function getPendingMappingsSummary(contractId: string): Promise<{
  total: number;
  pending: number;
  confirmed: number;
  rejected: number;
  modified: number;
}> {
  const allMappings = await storage.getPendingTermMappingsByContract(contractId);
  
  return {
    total: allMappings.length,
    pending: allMappings.filter(m => m.status === 'pending').length,
    confirmed: allMappings.filter(m => m.status === 'confirmed').length,
    rejected: allMappings.filter(m => m.status === 'rejected').length,
    modified: allMappings.filter(m => m.status === 'modified').length,
  };
}
