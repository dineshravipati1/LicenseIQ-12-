/**
 * ERP & LicenseIQ Vocabulary Service
 * Loads field definitions from both ERP and LicenseIQ schema tables
 * and provides them for AI-powered contract term mapping during extraction
 * 
 * Supports dual terminology: Contract Term → LicenseIQ Field + Master Data Value
 */

import { db } from "../db";
import { erpSystems, erpEntities, erpFields, licenseiqEntities, licenseiqFields, licenseiqEntityRecords, erpLicenseiqFieldMappings } from "@shared/schema";
import { eq, and, ilike, sql } from "drizzle-orm";
import Groq from "groq-sdk";

const groq = new Groq({
  apiKey: process.env.GROQ_API_KEY || "",
});

export interface ErpFieldVocabulary {
  fieldId: string;
  fieldName: string;
  displayName: string;
  description: string | null;
  dataType: string | null;
  entityId: string;
  entityName: string;
  systemId: string;
  systemName: string;
}

export interface TermToFieldMapping {
  contractTerm: string;
  contractValue: string | null;
  sourceText: string | null;
  erpFieldId: string;
  erpFieldName: string;
  erpEntityId: string;
  erpEntityName: string;
  confidence: number;
  mappingMethod: string;
  alternatives: Array<{
    erpFieldId: string;
    erpFieldName: string;
    erpEntityName: string;
    confidence: number;
  }>;
}

/**
 * Load all ERP field vocabulary for a specific ERP system
 */
export async function loadErpVocabulary(erpSystemId: string): Promise<ErpFieldVocabulary[]> {
  const result = await db
    .select({
      fieldId: erpFields.id,
      fieldName: erpFields.fieldName,
      description: erpFields.description,
      dataType: erpFields.dataType,
      entityId: erpEntities.id,
      entityName: erpEntities.name,
      systemId: erpSystems.id,
      systemName: erpSystems.name,
    })
    .from(erpFields)
    .innerJoin(erpEntities, eq(erpFields.entityId, erpEntities.id))
    .innerJoin(erpSystems, eq(erpEntities.systemId, erpSystems.id))
    .where(eq(erpSystems.id, erpSystemId));

  return result.map(row => ({
    ...row,
    displayName: row.fieldName, // Use fieldName as displayName since table doesn't have it
  }));
}

/**
 * Build a vocabulary prompt for AI to understand available ERP fields
 */
function buildErpVocabularyPrompt(vocabulary: ErpFieldVocabulary[]): string {
  const byEntity = vocabulary.reduce((acc, field) => {
    if (!acc[field.entityName]) {
      acc[field.entityName] = [];
    }
    acc[field.entityName].push(field);
    return acc;
  }, {} as Record<string, ErpFieldVocabulary[]>);

  let prompt = "Available ERP Fields by Entity:\n\n";
  
  for (const [entityName, fields] of Object.entries(byEntity)) {
    prompt += `## ${entityName}\n`;
    for (const field of fields) {
      prompt += `- ${field.fieldName}`;
      if (field.displayName !== field.fieldName) {
        prompt += ` (${field.displayName})`;
      }
      if (field.description) {
        prompt += `: ${field.description}`;
      }
      if (field.dataType) {
        prompt += ` [${field.dataType}]`;
      }
      prompt += `\n`;
    }
    prompt += `\n`;
  }
  
  return prompt;
}

/**
 * Use AI to map extracted contract terms to ERP fields
 */
export async function mapTermsToErpFields(
  extractedTerms: Array<{ term: string; value: string | null; sourceText?: string }>,
  erpSystemId: string
): Promise<TermToFieldMapping[]> {
  const vocabulary = await loadErpVocabulary(erpSystemId);
  
  if (vocabulary.length === 0) {
    console.log(`No ERP vocabulary found for system ${erpSystemId}`);
    return [];
  }

  const vocabularyPrompt = buildErpVocabularyPrompt(vocabulary);
  
  const termsJson = JSON.stringify(extractedTerms.map(t => ({
    term: t.term,
    value: t.value,
    context: t.sourceText?.substring(0, 200)
  })), null, 2);

  const prompt = `You are an expert at mapping contract terminology to ERP system fields.

Given these extracted contract terms:
${termsJson}

And these available ERP fields:
${vocabularyPrompt}

Map each contract term to the most appropriate ERP field. Consider:
1. Semantic similarity (e.g., "Licensor" → "SupplierName" or "VendorName")
2. Data type compatibility (dates to date fields, names to text fields)
3. Business context (license fee terms → revenue/royalty fields)
4. For product/item NAMES or DESCRIPTIONS, use "ItemDescription" or "Description" from the "Items" entity (NOT ItemNumber - that's for codes/SKUs)
5. For product CODES, SKUs, or NUMBERS, use "ItemNumber" from the "Items" entity
6. For vendor/licensor names, use "SupplierName" from "Suppliers" entity
7. For territory/location info, use "SiteName" from "Sites" or "OperatingUnitName" from "OperatingUnits"

CRITICAL: You MUST use the EXACT field names from the vocabulary above. Do NOT use descriptions or paraphrased versions.
For example, use "ItemNumber" NOT "User-defined item number", use "SupplierName" NOT "Supplier name field".

Return a JSON array where each object has:
{
  "contractTerm": "the original term name",
  "erpFieldId": "leave empty string",
  "erpFieldName": "EXACT field name from vocabulary (e.g., ItemNumber, SupplierName, SiteName)", 
  "erpEntityName": "EXACT entity name from vocabulary (e.g., Items, Suppliers, Sites)",
  "confidence": 0.0-1.0,
  "mappingMethod": "semantic" or "exact" or "contextual",
  "alternatives": []
}

If no good mapping exists, set confidence to 0.
Return ONLY the JSON array, no explanation.`;

  try {
    const response = await groq.chat.completions.create({
      model: "llama-3.3-70b-versatile",
      messages: [{ role: "user", content: prompt }],
      temperature: 0.1,
      max_tokens: 4000,
    });

    const content = response.choices[0]?.message?.content || "[]";
    
    // Extract JSON from response
    let jsonStr = content;
    const jsonMatch = content.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      jsonStr = jsonMatch[0];
    }

    const mappings = JSON.parse(jsonStr) as Array<{
      contractTerm: string;
      erpFieldId: string;
      erpFieldName: string;
      erpEntityName: string;
      confidence: number;
      mappingMethod: string;
      alternatives: Array<{
        erpFieldId: string;
        erpFieldName: string;
        erpEntityName: string;
        confidence: number;
      }>;
    }>;

    // Enrich with original term data and resolve entity IDs
    // IMPORTANT: Only return mappings where we found a valid field (with UUID) in vocabulary
    return mappings
      .map(mapping => {
        const originalTerm = extractedTerms.find(t => t.term === mapping.contractTerm);
        // Try to find field by exact ID match first, then by name (case-insensitive)
        const matchedField = vocabulary.find(v => 
          v.fieldId === mapping.erpFieldId || 
          v.fieldName.toLowerCase() === mapping.erpFieldName?.toLowerCase()
        );
        
        // Skip mappings where no valid field was found in vocabulary
        if (!matchedField) {
          console.log(`[ERP Mapping] Skipping term "${mapping.contractTerm}" - no matching field found for "${mapping.erpFieldName}"`);
          return null;
        }
        
        return {
          contractTerm: mapping.contractTerm,
          contractValue: originalTerm?.value || null,
          sourceText: originalTerm?.sourceText || null,
          erpFieldId: matchedField.fieldId, // Always use the valid UUID from vocabulary
          erpFieldName: matchedField.fieldName,
          erpEntityId: matchedField.entityId,
          erpEntityName: matchedField.entityName,
          confidence: Math.min(1, Math.max(0, mapping.confidence)),
          mappingMethod: mapping.mappingMethod || "ai",
          alternatives: (mapping.alternatives || [])
            .map(alt => {
              const altField = vocabulary.find(v => 
                v.fieldId === alt.erpFieldId || 
                v.fieldName.toLowerCase() === alt.erpFieldName?.toLowerCase()
              );
              // Only include alternatives with valid field matches
              if (!altField) return null;
              return {
                erpFieldId: altField.fieldId,
                erpFieldName: altField.fieldName,
                erpEntityName: altField.entityName,
                confidence: Math.min(1, Math.max(0, alt.confidence)),
              };
            })
            .filter((alt): alt is NonNullable<typeof alt> => alt !== null),
        };
      })
      .filter((mapping): mapping is NonNullable<typeof mapping> => mapping !== null);
  } catch (error) {
    console.error("Error mapping terms to ERP fields:", error);
    return [];
  }
}

/**
 * Get all available ERP systems for selection
 */
export async function getAvailableErpSystems(): Promise<Array<{ id: string; name: string; description: string | null }>> {
  const systems = await db
    .select({
      id: erpSystems.id,
      name: erpSystems.name,
      description: erpSystems.description,
    })
    .from(erpSystems)
    .where(eq(erpSystems.status, "active"));

  return systems;
}

/**
 * Validate that an ERP system exists and has vocabulary configured
 */
export async function validateErpSystem(erpSystemId: string): Promise<{ valid: boolean; fieldCount: number }> {
  const vocabulary = await loadErpVocabulary(erpSystemId);
  return {
    valid: vocabulary.length > 0,
    fieldCount: vocabulary.length,
  };
}

// ============================================
// LICENSEIQ SCHEMA VOCABULARY SUPPORT
// ============================================

export interface LicenseIQFieldVocabulary {
  fieldId: string;
  fieldName: string;
  description: string | null;
  dataType: string | null;
  entityId: string;
  entityName: string;
  entityTechnicalName: string;
}

export interface LicenseIQTermMapping {
  contractTerm: string;
  contractValue: string | null;
  sourceText: string | null;
  licenseiqFieldId: string;
  licenseiqFieldName: string;
  licenseiqEntityId: string;
  licenseiqEntityName: string;
  matchedMasterDataValue: string | null;
  matchedMasterDataId: string | null;
  matchedMasterDataField: string | null;
  matchedRecordData: Record<string, any> | null;
  confidence: number;
  mappingMethod: string;
  alternatives: Array<{
    licenseiqFieldId: string;
    licenseiqFieldName: string;
    licenseiqEntityName: string;
    confidence: number;
  }>;
}

/**
 * Load all LicenseIQ field vocabulary
 */
export async function loadLicenseIQVocabulary(): Promise<LicenseIQFieldVocabulary[]> {
  const result = await db
    .select({
      fieldId: licenseiqFields.id,
      fieldName: licenseiqFields.fieldName,
      description: licenseiqFields.description,
      dataType: licenseiqFields.dataType,
      entityId: licenseiqEntities.id,
      entityName: licenseiqEntities.name,
      entityTechnicalName: licenseiqEntities.technicalName,
    })
    .from(licenseiqFields)
    .innerJoin(licenseiqEntities, eq(licenseiqFields.entityId, licenseiqEntities.id));

  return result;
}

/**
 * Build a vocabulary prompt for AI to understand available LicenseIQ fields
 */
function buildLicenseIQVocabularyPrompt(vocabulary: LicenseIQFieldVocabulary[]): string {
  const byEntity = vocabulary.reduce((acc, field) => {
    if (!acc[field.entityName]) {
      acc[field.entityName] = [];
    }
    acc[field.entityName].push(field);
    return acc;
  }, {} as Record<string, LicenseIQFieldVocabulary[]>);

  let prompt = "Available LicenseIQ Schema Fields by Entity:\n\n";
  
  for (const [entityName, fields] of Object.entries(byEntity)) {
    prompt += `## ${entityName}\n`;
    for (const field of fields) {
      prompt += `- ${field.fieldName}`;
      if (field.description) {
        prompt += `: ${field.description}`;
      }
      if (field.dataType) {
        prompt += ` [${field.dataType}]`;
      }
      prompt += `\n`;
    }
    prompt += `\n`;
  }
  
  return prompt;
}

/**
 * Lookup matching master data value from LicenseIQ entity records
 * Searches by organization (business unit) context
 */
export async function lookupMasterDataValue(
  entityId: string,
  searchValue: string,
  orgId?: string
): Promise<{ recordId: string; matchedValue: string; matchedField: string; recordData: Record<string, any> } | null> {
  try {
    // Get records for this entity, optionally filtered by org
    let query = db
      .select()
      .from(licenseiqEntityRecords)
      .where(eq(licenseiqEntityRecords.entityId, entityId));

    if (orgId) {
      query = db
        .select()
        .from(licenseiqEntityRecords)
        .where(and(
          eq(licenseiqEntityRecords.entityId, entityId),
          eq(licenseiqEntityRecords.orgId, orgId)
        ));
    }
    
    const records = await query;

    // Search through record_data JSON for matching value
    for (const record of records) {
      const recordData = record.recordData as Record<string, any>;
      for (const [key, value] of Object.entries(recordData)) {
        if (typeof value === 'string' && 
            value.toLowerCase().includes(searchValue.toLowerCase())) {
          return {
            recordId: record.id,
            matchedValue: value,
            matchedField: key,
            recordData: recordData,
          };
        }
      }
    }
    return null;
  } catch (error) {
    console.error("Error looking up master data:", error);
    return null;
  }
}

/**
 * Get all master data records for an entity within an organization
 */
export async function getMasterDataForEntity(
  entityTechnicalName: string,
  orgId: string
): Promise<Array<{ recordId: string; recordData: Record<string, any> }>> {
  try {
    // Find entity by technical name
    const entity = await db
      .select()
      .from(licenseiqEntities)
      .where(eq(licenseiqEntities.technicalName, entityTechnicalName))
      .limit(1);

    if (entity.length === 0) return [];

    const records = await db
      .select()
      .from(licenseiqEntityRecords)
      .where(and(
        eq(licenseiqEntityRecords.entityId, entity[0].id),
        eq(licenseiqEntityRecords.orgId, orgId)
      ));

    return records.map(r => ({
      recordId: r.id,
      recordData: r.recordData as Record<string, any>,
    }));
  } catch (error) {
    console.error("Error getting master data:", error);
    return [];
  }
}

/**
 * Use AI to map extracted contract terms to LicenseIQ schema fields
 * @param extractedTerms - Terms extracted from contract PDF
 * @param orgId - Organization (business unit) ID for master data lookup
 */
export async function mapTermsToLicenseIQFields(
  extractedTerms: Array<{ term: string; value: string | null; sourceText?: string }>,
  orgId?: string
): Promise<LicenseIQTermMapping[]> {
  const vocabulary = await loadLicenseIQVocabulary();
  
  if (vocabulary.length === 0) {
    console.log("No LicenseIQ vocabulary found");
    return [];
  }

  const vocabularyPrompt = buildLicenseIQVocabularyPrompt(vocabulary);
  
  const termsJson = JSON.stringify(extractedTerms.map(t => ({
    term: t.term,
    value: t.value,
    context: t.sourceText?.substring(0, 200)
  })), null, 2);

  const prompt = `You are an expert at mapping contract terminology to LicenseIQ schema fields.

Given these extracted contract terms:
${termsJson}

And these available LicenseIQ fields:
${vocabularyPrompt}

Map each contract term to the most appropriate LicenseIQ field. Consider:
1. Semantic similarity (e.g., "Licensor" → "vendor_name" or "licensor_name")
2. Data type compatibility (dates to date fields, names to text fields)
3. Business context (license fee terms → license_fee_category, vendor info → vendor fields)

Return a JSON array where each object has:
{
  "contractTerm": "the original term name",
  "licenseiqFieldId": "the field ID",
  "licenseiqFieldName": "the field name", 
  "licenseiqEntityName": "the entity name (Items or Vendors)",
  "confidence": 0.0-1.0,
  "mappingMethod": "semantic" or "exact" or "contextual",
  "alternatives": [{"licenseiqFieldId": "...", "licenseiqFieldName": "...", "licenseiqEntityName": "...", "confidence": 0.0-1.0}]
}

If no good mapping exists, set confidence to 0 and include empty alternatives.
Return ONLY the JSON array, no explanation.`;

  try {
    const response = await groq.chat.completions.create({
      model: "llama-3.3-70b-versatile",
      messages: [{ role: "user", content: prompt }],
      temperature: 0.1,
      max_tokens: 4000,
    });

    const content = response.choices[0]?.message?.content || "[]";
    
    // Extract JSON from response
    let jsonStr = content;
    const jsonMatch = content.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      jsonStr = jsonMatch[0];
    }

    const mappings = JSON.parse(jsonStr) as Array<{
      contractTerm: string;
      licenseiqFieldId: string;
      licenseiqFieldName: string;
      licenseiqEntityName: string;
      confidence: number;
      mappingMethod: string;
      alternatives: Array<{
        licenseiqFieldId: string;
        licenseiqFieldName: string;
        licenseiqEntityName: string;
        confidence: number;
      }>;
    }>;

    // Enrich with original term data and lookup master data values
    const enrichedMappings: LicenseIQTermMapping[] = [];
    
    for (const mapping of mappings) {
      const originalTerm = extractedTerms.find(t => t.term === mapping.contractTerm);
      const matchedField = vocabulary.find(v => 
        v.fieldId === mapping.licenseiqFieldId || v.fieldName === mapping.licenseiqFieldName
      );
      
      // Try to find matching master data value from LicenseIQ entities
      let masterDataMatch: { recordId: string; matchedValue: string; matchedField: string; recordData: Record<string, any> } | null = null;
      if (matchedField && originalTerm?.value) {
        masterDataMatch = await lookupMasterDataValue(
          matchedField.entityId,
          originalTerm.value,
          orgId
        );
      }
      
      enrichedMappings.push({
        contractTerm: mapping.contractTerm,
        contractValue: originalTerm?.value || null,
        sourceText: originalTerm?.sourceText || null,
        licenseiqFieldId: matchedField?.fieldId || mapping.licenseiqFieldId,
        licenseiqFieldName: matchedField?.fieldName || mapping.licenseiqFieldName,
        licenseiqEntityId: matchedField?.entityId || "",
        licenseiqEntityName: matchedField?.entityName || mapping.licenseiqEntityName,
        matchedMasterDataValue: masterDataMatch?.matchedValue || null,
        matchedMasterDataId: masterDataMatch?.recordId || null,
        matchedMasterDataField: masterDataMatch?.matchedField || null,
        matchedRecordData: masterDataMatch?.recordData || null,
        confidence: Math.min(1, Math.max(0, mapping.confidence)),
        mappingMethod: mapping.mappingMethod || "ai",
        alternatives: (mapping.alternatives || []).map(alt => {
          const altField = vocabulary.find(v => 
            v.fieldId === alt.licenseiqFieldId || v.fieldName === alt.licenseiqFieldName
          );
          return {
            licenseiqFieldId: altField?.fieldId || alt.licenseiqFieldId,
            licenseiqFieldName: altField?.fieldName || alt.licenseiqFieldName,
            licenseiqEntityName: altField?.entityName || alt.licenseiqEntityName,
            confidence: Math.min(1, Math.max(0, alt.confidence)),
          };
        }),
      });
    }

    return enrichedMappings;
  } catch (error) {
    console.error("Error mapping terms to LicenseIQ fields:", error);
    return [];
  }
}

/**
 * Get LicenseIQ entity summary with record counts
 */
export async function getLicenseIQEntitySummary(): Promise<Array<{
  entityId: string;
  entityName: string;
  technicalName: string;
  fieldCount: number;
  recordCount: number;
}>> {
  const entities = await db
    .select({
      entityId: licenseiqEntities.id,
      entityName: licenseiqEntities.name,
      technicalName: licenseiqEntities.technicalName,
    })
    .from(licenseiqEntities);

  const result = [];
  for (const entity of entities) {
    // Get field count
    const fields = await db
      .select()
      .from(licenseiqFields)
      .where(eq(licenseiqFields.entityId, entity.entityId));

    // Get record count
    const records = await db
      .select()
      .from(licenseiqEntityRecords)
      .where(eq(licenseiqEntityRecords.entityId, entity.entityId));

    result.push({
      ...entity,
      fieldCount: fields.length,
      recordCount: records.length,
    });
  }

  return result;
}

// ============================================
// ERP TO LICENSEIQ FIELD MAPPING LOOKUP
// ============================================

export interface ErpToLicenseIQMapping {
  erpSystemId: string;
  erpSystemName: string;
  erpEntityId: string;
  erpEntityName: string;
  erpFieldId: string;
  erpFieldName: string;
  licenseiqEntityId: string;
  licenseiqEntityName: string;
  licenseiqFieldId: string;
  licenseiqFieldName: string;
  mappingType: string;
}

/**
 * Get all ERP to LicenseIQ field mappings for an ERP system
 */
export async function getErpToLicenseIQMappings(erpSystemId: string): Promise<ErpToLicenseIQMapping[]> {
  const mappings = await db
    .select({
      erpSystemId: erpSystems.id,
      erpSystemName: erpSystems.name,
      erpEntityId: erpEntities.id,
      erpEntityName: erpEntities.name,
      erpFieldId: erpFields.id,
      erpFieldName: erpFields.fieldName,
      licenseiqEntityId: licenseiqEntities.id,
      licenseiqEntityName: licenseiqEntities.name,
      licenseiqFieldId: licenseiqFields.id,
      licenseiqFieldName: licenseiqFields.fieldName,
      mappingType: erpLicenseiqFieldMappings.mappingType,
    })
    .from(erpLicenseiqFieldMappings)
    .innerJoin(erpSystems, eq(erpLicenseiqFieldMappings.erpSystemId, erpSystems.id))
    .innerJoin(erpEntities, eq(erpLicenseiqFieldMappings.erpEntityId, erpEntities.id))
    .innerJoin(erpFields, eq(erpLicenseiqFieldMappings.erpFieldId, erpFields.id))
    .innerJoin(licenseiqEntities, eq(erpLicenseiqFieldMappings.licenseiqEntityId, licenseiqEntities.id))
    .innerJoin(licenseiqFields, eq(erpLicenseiqFieldMappings.licenseiqFieldId, licenseiqFields.id))
    .where(eq(erpLicenseiqFieldMappings.erpSystemId, erpSystemId));

  return mappings as ErpToLicenseIQMapping[];
}

/**
 * Build dual terminology display string
 * Format: "Contract Term (LicenseIQ Entity: Master Data Value)"
 * Example: "Green Valley Nurseries (LicenseIQ Vendors: V-1001)"
 */
export function buildDualTerminologyDisplay(
  contractTerm: string,
  licenseiqEntityName: string,
  masterDataValue: string | null,
  erpFieldName?: string
): string {
  if (masterDataValue) {
    return `${contractTerm} (LicenseIQ ${licenseiqEntityName}: ${masterDataValue})`;
  } else if (erpFieldName) {
    return `${contractTerm} (ERP: ${erpFieldName})`;
  }
  return contractTerm;
}

/**
 * Get ERP field name from LicenseIQ field mapping
 */
export async function getErpFieldForLicenseIQField(
  erpSystemId: string,
  licenseiqFieldId: string
): Promise<{ erpFieldName: string; erpEntityName: string } | null> {
  const mapping = await db
    .select({
      erpFieldName: erpFields.fieldName,
      erpEntityName: erpEntities.name,
    })
    .from(erpLicenseiqFieldMappings)
    .innerJoin(erpFields, eq(erpLicenseiqFieldMappings.erpFieldId, erpFields.id))
    .innerJoin(erpEntities, eq(erpLicenseiqFieldMappings.erpEntityId, erpEntities.id))
    .where(and(
      eq(erpLicenseiqFieldMappings.erpSystemId, erpSystemId),
      eq(erpLicenseiqFieldMappings.licenseiqFieldId, licenseiqFieldId)
    ))
    .limit(1);

  return mapping.length > 0 ? mapping[0] : null;
}
