/**
 * ERP Field Mapping Service
 * AI-powered dynamic field mapping for various ERP systems (SAP, Oracle, NetSuite, QuickBooks)
 * Uses Groq LLaMA 3.1 for semantic field recognition
 */

import Groq from "groq-sdk";

const groq = new Groq({
  apiKey: process.env.GROQ_API_KEY || "",
});

// ERP System Templates
export const erpTemplates = {
  sap: {
    name: "SAP ERP",
    commonFields: {
      productName: ["MATERIAL_DESC", "PRODUCT_NAME", "MAKTX", "ITEM_DESC"],
      productCode: ["MATERIAL_NO", "MATNR", "SKU", "ITEM_CODE"],
      quantity: ["QUANTITY", "QTY", "MENGE", "ORDER_QTY"],
      grossAmount: ["GROSS_AMOUNT", "SALES_AMT", "NETWR", "TOTAL_AMT"],
      transactionDate: ["POSTING_DATE", "BUDAT", "DOC_DATE", "SALES_DATE"],
      transactionId: ["DOCUMENT_NO", "BELNR", "ORDER_NO", "INVOICE_NO"],
      territory: ["SALES_ORG", "VKORG", "REGION", "TERRITORY"],
      category: ["PRODUCT_HIER", "MATKL", "CATEGORY", "PRODUCT_LINE"],
      currency: ["CURRENCY", "WAERS", "CURR"],
    },
  },
  oracle: {
    name: "Oracle ERP",
    commonFields: {
      productName: ["ITEM_DESCRIPTION", "PRODUCT_DESC", "DESCRIPTION"],
      productCode: ["ITEM_NUMBER", "INVENTORY_ITEM_ID", "PRODUCT_ID"],
      quantity: ["ORDERED_QUANTITY", "QUANTITY", "QTY_SHIPPED"],
      grossAmount: ["LINE_AMOUNT", "EXTENDED_AMOUNT", "REVENUE_AMOUNT"],
      transactionDate: ["TRANSACTION_DATE", "ORDERED_DATE", "GL_DATE"],
      transactionId: ["TRANSACTION_NUMBER", "ORDER_NUMBER", "INVOICE_NUM"],
      territory: ["SALES_TERRITORY", "REGION_NAME", "TERRITORY_CODE"],
      category: ["CATEGORY_SET", "ITEM_CATEGORY", "PRODUCT_CATEGORY"],
      currency: ["TRANSACTION_CURRENCY", "INVOICE_CURRENCY_CODE"],
    },
  },
  netsuite: {
    name: "NetSuite",
    commonFields: {
      productName: ["Item: Display Name", "Item Name", "Product"],
      productCode: ["Item: Name", "Item ID", "SKU"],
      quantity: ["Quantity", "Qty Ordered", "Amount"],
      grossAmount: ["Amount", "Gross Amount", "Total"],
      transactionDate: ["Date", "Transaction Date", "Invoice Date"],
      transactionId: ["Document Number", "Transaction ID", "Reference No"],
      territory: ["Territory", "Subsidiary", "Location"],
      category: ["Item: Category", "Class", "Department"],
      currency: ["Currency", "Transaction Currency"],
    },
  },
  quickbooks: {
    name: "QuickBooks",
    commonFields: {
      productName: ["Product/Service", "Item", "Description"],
      productCode: ["SKU", "Item Code", "Product ID"],
      quantity: ["Qty", "Quantity"],
      grossAmount: ["Amount", "Total", "Sales"],
      transactionDate: ["Transaction Date", "Date", "Invoice Date"],
      transactionId: ["Num", "Invoice Number", "Transaction ID"],
      territory: ["Location", "Class", "Territory"],
      category: ["Product/Service Type", "Category", "Item Type"],
      currency: ["Currency"],
    },
  },
  generic_csv: {
    name: "Generic CSV",
    commonFields: {
      productName: ["product", "item", "product_name", "item_name", "description"],
      productCode: ["sku", "product_code", "item_code", "product_id"],
      quantity: ["quantity", "qty", "units", "amount"],
      grossAmount: ["amount", "total", "sales", "revenue", "gross_amount"],
      transactionDate: ["date", "transaction_date", "sale_date", "invoice_date"],
      transactionId: ["id", "transaction_id", "invoice_number", "order_number"],
      territory: ["territory", "region", "location", "market"],
      category: ["category", "type", "product_category"],
      currency: ["currency", "curr"],
    },
  },
};

/**
 * Use AI to detect field mappings from column headers
 */
export async function detectFieldMappings(
  columnHeaders: string[],
  erpSystem: string = "generic_csv",
  sampleData?: Record<string, any>
): Promise<Record<string, {field: string, confidence: number, method: string}>> {
  const template = erpTemplates[erpSystem as keyof typeof erpTemplates] || erpTemplates.generic_csv;
  
  const mappings: Record<string, {field: string, confidence: number, method: string}> = {};
  
  // Step 1: Exact match with known templates
  for (const [targetField, sourceOptions] of Object.entries(template.commonFields)) {
    for (const header of columnHeaders) {
      const normalized = header.toLowerCase().replace(/[_\s-]/g, "");
      const matchFound = sourceOptions.some(option => 
        normalized === option.toLowerCase().replace(/[_\s-]/g, "")
      );
      
      if (matchFound) {
        mappings[targetField] = {
          field: header,
          confidence: 0.98,
          method: "exact_match"
        };
        break;
      }
    }
  }
  
  // Step 2: AI semantic matching for unmapped fields
  const unmappedTargets = Object.keys(template.commonFields).filter(k => !mappings[k]);
  const unmappedHeaders = columnHeaders.filter(h => 
    !Object.values(mappings).some(m => m.field === h)
  );
  
  if (unmappedTargets.length > 0 && unmappedHeaders.length > 0) {
    const aiMappings = await aiSemanticMapping(
      unmappedHeaders,
      unmappedTargets,
      sampleData || {}
    );
    
    for (const [target, mapping] of Object.entries(aiMappings)) {
      if (mapping.confidence >= 0.7) {
        mappings[target] = mapping;
      }
    }
  }
  
  return mappings;
}

/**
 * Use Groq LLaMA to semantically map fields
 */
async function aiSemanticMapping(
  columnHeaders: string[],
  targetFields: string[],
  sampleData: Record<string, any>
): Promise<Record<string, {field: string, confidence: number, method: string}>> {
  try {
    const prompt = `You are an ERP data integration expert. Map the following CSV column headers to standard sales data fields.

**Available Column Headers:**
${columnHeaders.map((h, i) => `${i + 1}. "${h}"`).join('\n')}

**Sample Data (first row):**
${columnHeaders.map(h => `"${h}": "${sampleData[h] || 'N/A'}"`).join('\n')}

**Target Fields to Map:**
${targetFields.map(f => `- ${f}: (examples: transaction date, product name, sales amount, quantity, SKU, territory, etc.)`).join('\n')}

**Instructions:**
1. Analyze the column headers and sample data
2. Map each target field to the most appropriate column header
3. Provide a confidence score (0-1) for each mapping
4. Return JSON array of mappings

**Output Format (JSON only, no explanations):**
[
  {"targetField": "productName", "sourceField": "Item Description", "confidence": 0.95},
  {"targetField": "grossAmount", "sourceField": "Total Sales", "confidence": 0.90}
]`;

    const completion = await groq.chat.completions.create({
      messages: [{ role: "user", content: prompt }],
      model: "llama-3.1-8b-instant",
      temperature: 0.1,
      max_tokens: 1000,
    });

    const content = completion.choices[0]?.message?.content || "[]";
    const jsonMatch = content.match(/\[[\s\S]*\]/);
    const aiMappings = jsonMatch ? JSON.parse(jsonMatch[0]) : [];
    
    const result: Record<string, {field: string, confidence: number, method: string}> = {};
    for (const mapping of aiMappings) {
      if (mapping.targetField && mapping.sourceField && mapping.confidence) {
        result[mapping.targetField] = {
          field: mapping.sourceField,
          confidence: mapping.confidence,
          method: "ai_semantic"
        };
      }
    }
    
    return result;
  } catch (error) {
    console.error("AI field mapping error:", error);
    return {};
  }
}

/**
 * Transform raw ERP data row using field mappings
 */
export function transformDataRow(
  rawRow: Record<string, any>,
  fieldMappings: Record<string, {field: string, confidence: number, method: string}>
): Record<string, any> {
  const transformed: Record<string, any> = {};
  
  for (const [targetField, mapping] of Object.entries(fieldMappings)) {
    const sourceValue = rawRow[mapping.field];
    transformed[targetField] = sourceValue !== undefined ? sourceValue : null;
  }
  
  // Add metadata
  transformed._mappingConfidence = calculateAverageMappingConfidence(fieldMappings);
  transformed._mappingMethod = determinePrimaryMappingMethod(fieldMappings);
  
  return transformed;
}

/**
 * Calculate average mapping confidence
 */
function calculateAverageMappingConfidence(
  mappings: Record<string, {field: string, confidence: number, method: string}>
): number {
  const confidences = Object.values(mappings).map(m => m.confidence);
  return confidences.length > 0 
    ? confidences.reduce((sum, c) => sum + c, 0) / confidences.length 
    : 0;
}

/**
 * Determine primary mapping method
 */
function determinePrimaryMappingMethod(
  mappings: Record<string, {field: string, confidence: number, method: string}>
): string {
  const methods = Object.values(mappings).map(m => m.method);
  const exactMatchCount = methods.filter(m => m === "exact_match").length;
  const aiSemanticCount = methods.filter(m => m === "ai_semantic").length;
  
  if (exactMatchCount > aiSemanticCount) return "exact_match";
  if (aiSemanticCount > exactMatchCount) return "ai_semantic";
  return "hybrid";
}

/**
 * Validate mapped data quality
 */
export function validateMappedData(
  mappedData: Record<string, any>[]
): {valid: boolean, issues: string[], validRowCount: number} {
  const requiredFields = ["productName", "grossAmount", "transactionDate"];
  const issues: string[] = [];
  let validRowCount = 0;
  
  for (let i = 0; i < mappedData.length; i++) {
    const row = mappedData[i];
    const missing = requiredFields.filter(field => !row[field]);
    
    if (missing.length === 0) {
      validRowCount++;
    } else {
      issues.push(`Row ${i + 1}: Missing required fields: ${missing.join(", ")}`);
    }
    
    // Validate data types
    if (row.grossAmount && isNaN(parseFloat(row.grossAmount))) {
      issues.push(`Row ${i + 1}: grossAmount must be a number`);
    }
    
    if (row.transactionDate && isNaN(Date.parse(row.transactionDate))) {
      issues.push(`Row ${i + 1}: transactionDate must be a valid date`);
    }
  }
  
  return {
    valid: issues.length === 0,
    issues,
    validRowCount
  };
}

export default {
  erpTemplates,
  detectFieldMappings,
  transformDataRow,
  validateMappedData,
};
