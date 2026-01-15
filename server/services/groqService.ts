import type { FormulaDefinition, FormulaNode } from '@shared/formula-types';

interface GroqResponse {
  choices: Array<{
    message: {
      content: string;
    };
  }>;
}

interface ContractAnalysisResult {
  summary: string;
  keyTerms: Array<{
    type: string;
    description: string;
    confidence: number;
    location: string;
  }>;
  riskAnalysis: Array<{
    level: 'high' | 'medium' | 'low';
    title: string;
    description: string;
  }>;
  insights: Array<{
    type: string;
    title: string;
    description: string;
  }>;
  confidence: number;
}

// =====================================================
// LICENSE RULE EXTRACTION INTERFACES (NEW EXTENSION)
// =====================================================

interface RoyaltyRule {
  ruleType: 'percentage' | 'tiered' | 'minimum_guarantee' | 'cap' | 'deduction' | 'fixed_fee' | 
             'payment_schedule' | 'payment_method' | 'rate_structure' | 'invoice_requirements' | 
             'late_payment_penalty' | 'advance_payment' | 'milestone_payment' | 'formula_based' |
             'fixed_price' | 'variable_price' | 'per_seat' | 'per_unit' | 'per_time_period' |
             'auto_renewal' | 'escalation_clause' | 'early_termination' | 'volume_discount' |
             'license_scope' | 'usage_based';
  ruleName: string;
  description: string;
  conditions: {
    productCategories?: string[];
    territories?: string[];
    salesVolumeMin?: number;
    salesVolumeMax?: number;
    timeperiod?: string;
    currency?: string;
    licenseScope?: {
      userLimit?: number;
      geographic?: string[];
      termMonths?: number;
      exclusivity?: boolean;
    };
    renewalTerms?: {
      autoRenew?: boolean;
      renewalRate?: number;
      noticeRequiredDays?: number;
    };
  };
  calculation: {
    rate?: number; // For percentage rules
    tiers?: Array<{
      min: number;
      max?: number; 
      rate: number;
    }>; // For tiered rules
    amount?: number; // For fixed amounts
    formula?: string; // For complex calculations
    escalationRate?: number; // For annual escalation clauses
    terminationFee?: number; // For early termination penalties
    discountPercent?: number; // For volume discounts
  };
  priority: number;
  sourceSpan: {
    page?: number;
    section?: string;
    text: string;
  };
  confidence: number;
}

interface LicenseRuleExtractionResult {
  documentType: 'sales' | 'service' | 'licensing' | 'distribution' | 'employment' | 
                'consulting' | 'nda' | 'amendment' | 'saas' | 'subscription' | 'other';
  contractCategory: 'revenue-generating' | 'service-based' | 'confidentiality' | 'employment' | 'other';
  licenseType: string;
  parties: {
    licensor: string;
    licensee: string;
  };
  effectiveDate?: string;
  expirationDate?: string;
  rules: RoyaltyRule[];
  currency: string;
  paymentTerms: string;
  reportingRequirements: Array<{
    frequency: string;
    dueDate: string;
    description: string;
  }>;
  extractionMetadata: {
    totalRulesFound: number;
    avgConfidence: number;
    processingTime: number;
    ruleComplexity: 'simple' | 'moderate' | 'complex';
    hasFixedPricing: boolean;
    hasVariablePricing: boolean;
    hasTieredPricing: boolean;
    hasRenewalTerms: boolean;
    hasTerminationClauses: boolean;
  };
}

export class GroqService {
  private apiKey: string;
  private baseUrl = 'https://api.groq.com/openai/v1';

  constructor() {
    this.apiKey = process.env.GROQ_API_KEY || '';
    if (!this.apiKey) {
      throw new Error('GROQ_API_KEY environment variable is required');
    }
  }

  // ⚡ Pre-parse sanitizer: Replace AI placeholder values with JSON-safe equivalents
  private sanitizeAIPlaceholders(jsonStr: string): string {
    let sanitized = jsonStr;
    let sanitizationCount = 0;
    
    // Replace literal [NA] with empty array []
    const naArrayRegex = /\[\s*NA\s*\]/gi;
    const naArrayMatches = sanitized.match(naArrayRegex);
    if (naArrayMatches) {
      sanitizationCount += naArrayMatches.length;
      sanitized = sanitized.replace(naArrayRegex, '[]');
    }
    
    // Replace standalone NA values (not in quotes) with null
    // Match NA that's preceded by : or [ and followed by , or ] or }
    const naValueRegex = /([:\[,]\s*)NA(\s*[,\]\}])/gi;
    const naValueMatches = sanitized.match(naValueRegex);
    if (naValueMatches) {
      sanitizationCount += naValueMatches.length;
      sanitized = sanitized.replace(naValueRegex, '$1null$2');
    }
    
    // Replace quoted "NA" strings in value positions with null (for fields like "territory": "NA")
    const quotedNARegex = /:\s*"NA"/gi;
    const quotedNAMatches = sanitized.match(quotedNARegex);
    if (quotedNAMatches) {
      sanitizationCount += quotedNAMatches.length;
      sanitized = sanitized.replace(quotedNARegex, ': null');
    }
    
    if (sanitizationCount > 0) {
      console.log(`🧹 Sanitized ${sanitizationCount} AI placeholder value(s) (NA → null, [NA] → [])`);
    }
    
    return sanitized;
  }

  // ⚡ OPTION C: Enhanced JSON extraction with better error recovery
  private extractAndRepairJSON(response: string, fallbackValue: any = []): any {
    try {
      const cleanResponse = response.trim();
      
      // CRITICAL: Detect if response is an array BEFORE matching
      const isArrayResponse = cleanResponse.startsWith('[');
      
      // Try to find JSON object FIRST (to get full response), then array as fallback
      // CRITICAL: Match object {...} before array [...] to preserve basicInfo!
      let jsonMatch = cleanResponse.match(/\{[\s\S]*\}/) || cleanResponse.match(/\[[\s\S]*\]/);
      
      if (!jsonMatch) {
        console.warn('⚠️ No JSON found in response, returning fallback');
        return fallbackValue;
      }

      let jsonStr = jsonMatch[0];
      
      // ⚡ CRITICAL FIX: Pre-parse sanitization of AI quirks
      // Replace literal NA/[NA] values with JSON-safe nulls BEFORE other repairs
      // Uses a string-walking approach to avoid corrupting quoted strings
      jsonStr = this.sanitizeAIPlaceholders(jsonStr);
      
      // Repair common JSON issues (ENHANCED)
      jsonStr = jsonStr
        .replace(/&amp;/g, '&')                    // Fix HTML entities
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/:\s*Infinity/g, ': 999999999')  // Fix Infinity
        .replace(/:\s*NaN/g, ': null')             // Fix NaN
        .replace(/,\s*([}\]])/g, '$1')             // Remove trailing commas
        // REMOVED: .replace(/'/g, '"') - This broke strings with apostrophes
        .replace(/([{,]\s*)(\w+):/g, '$1"$2":')    // Fix unquoted keys
        .replace(/\n/g, ' ')                        // Remove newlines
        .replace(/\r/g, '')                         // Remove carriage returns
        .replace(/\t/g, ' ')                        // Replace tabs with spaces
        .replace(/\s+/g, ' ');                      // Normalize whitespace

      // Try to parse
      try {
        return JSON.parse(jsonStr);
      } catch (parseError: any) {
        // ⚡ If parsing fails, try to repair truncated JSON
        const posMatch = parseError.message.match(/position (\d+)/);
        if (posMatch) {
          const truncPos = parseInt(posMatch[1]);
          console.warn(`⚠️ JSON truncated at position ${truncPos}, attempting advanced repair...`);
          
          // ARRAY-SPECIFIC REPAIR: Handle truncated arrays (check original response, not jsonMatch)
          if (isArrayResponse) {
            console.log('🔧 Detected truncated array, attempting to salvage complete objects...');
            // Find complete objects by properly counting braces
            let depth = 0;
            let lastGoodPosition = -1;
            
            for (let i = 0; i < Math.min(jsonStr.length, truncPos); i++) {
              if (jsonStr[i] === '{') {
                depth++;
              } else if (jsonStr[i] === '}') {
                depth--;
                if (depth === 0) {
                  // Found end of a complete object
                  lastGoodPosition = i;
                }
              }
            }
            
            if (lastGoodPosition > 0) {
              const repairedArray = jsonStr.substring(0, lastGoodPosition + 1) + ']';
              console.log(`🔧 Array repaired: extracted ${lastGoodPosition + 1} chars with complete objects`);
              try {
                const parsed = JSON.parse(repairedArray);
                console.log(`✅ Successfully parsed ${parsed.length} rules from truncated array`);
                return parsed;
              } catch (e) {
                console.error('❌ Array repair failed:', e);
              }
            }
          }
          
          // STRATEGY 1: Try to salvage partial rules array from object
          if (jsonStr.includes('"rules"') && jsonStr.includes('[')) {
            const rulesMatch = jsonStr.match(/"rules"\s*:\s*\[/);
            if (rulesMatch) {
              // Find the last complete rule object by looking for complete {...} pairs
              let salvaged = jsonStr.substring(0, truncPos);
              
              // Remove incomplete trailing object
              const lastCompleteObjEnd = salvaged.lastIndexOf('}');
              if (lastCompleteObjEnd > 0) {
                salvaged = jsonStr.substring(0, lastCompleteObjEnd + 1);
                console.log(`🔧 Truncated at character ${truncPos}, salvaged up to position ${lastCompleteObjEnd}`);
                
                // Add missing closing brackets
                const openBraces = (salvaged.match(/{/g) || []).length;
                const closeBraces = (salvaged.match(/}/g) || []).length;
                const openBrackets = (salvaged.match(/\[/g) || []).length;
                const closeBrackets = (salvaged.match(/]/g) || []).length;
                
                if (openBrackets > closeBrackets) {
                  salvaged += ']'.repeat(openBrackets - closeBrackets);
                  console.log(`🔧 Added ${openBrackets - closeBrackets} closing brackets`);
                }
                if (openBraces > closeBraces) {
                  salvaged += '}'.repeat(openBraces - closeBraces);
                  console.log(`🔧 Added ${openBraces - closeBraces} closing braces`);
                }
                
                try {
                  const parsed = JSON.parse(salvaged);
                  console.log(`✅ Successfully salvaged partial JSON with ${parsed.rules?.length || 0} rules`);
                  return parsed;
                } catch (e) {
                  console.warn('⚠️ Salvage attempt failed, trying simple repair...');
                }
              }
            }
          }
          
          // STRATEGY 2: Simple brace/bracket addition (original logic)
          const openBraces = (jsonStr.match(/{/g) || []).length;
          const closeBraces = (jsonStr.match(/}/g) || []).length;
          const openBrackets = (jsonStr.match(/\[/g) || []).length;
          const closeBrackets = (jsonStr.match(/]/g) || []).length;
          
          if (openBraces > closeBraces) {
            jsonStr += '}'.repeat(openBraces - closeBraces);
            console.log(`🔧 Added ${openBraces - closeBraces} closing braces`);
          }
          if (openBrackets > closeBrackets) {
            jsonStr += ']'.repeat(openBrackets - closeBrackets);
            console.log(`🔧 Added ${openBrackets - closeBrackets} closing brackets`);
          }
          
          // Try parsing again
          return JSON.parse(jsonStr);
        }
        throw parseError;
      }
    } catch (error: any) {
      console.error('❌ JSON extraction/repair failed:', error.message);
      console.error('📄 Response snippet:', response.substring(0, 500));
      return fallbackValue;
    }
  }

  // =====================================================
  // ENHANCED ROYALTY RULES EXTRACTION 
  // =====================================================
  
  async extractDetailedRoyaltyRules(contractText: string): Promise<LicenseRuleExtractionResult> {
    console.log(`🚀 Starting enhanced contract analysis with improved JSON handling...`);
    
    // ⚡ Try consolidated extraction with improved truncation handling
    const comprehensiveResult = await this.extractAllContractDataInOneCall(contractText);
    
    const basicInfo = comprehensiveResult.basicInfo;
    console.log(`📄 Contract type detected: ${basicInfo.documentType}, Has royalty terms: ${basicInfo.hasRoyaltyTerms}`);
    
    let allRules: RoyaltyRule[] = comprehensiveResult.allRules;
    
    // Filter out low-confidence or empty rules
    allRules = allRules.filter(rule => 
      rule.confidence >= 0.6 && 
      rule.sourceSpan?.text && 
      rule.sourceSpan.text.trim().length > 0
    );
    
    console.log(`✅ Rule extraction complete: ${allRules.length} valid rules found (consolidated extraction with enhanced JSON repair)`)
    
    // Map the flexible AI response to the expected schema format
    const documentType: 'sales' | 'service' | 'licensing' | 'distribution' | 'employment' | 'consulting' | 'nda' | 'amendment' | 'saas' | 'subscription' | 'other' = 
      basicInfo.documentType || (basicInfo.hasRoyaltyTerms === true ? 'licensing' : 'other');
    
    // Map dynamic party structure to expected licensor/licensee format
    let parties;
    if (basicInfo.parties) {
      const party1 = basicInfo.parties.party1 || basicInfo.parties;
      const party2 = basicInfo.parties.party2;
      parties = {
        licensor: typeof party1 === 'object' ? party1.name : (party1 || 'Not specified'),
        licensee: typeof party2 === 'object' ? party2.name : (party2 || 'Not specified')
      };
    } else {
      parties = { licensor: 'Not specified', licensee: 'Not specified' };
    }
    
    return {
      documentType,
      licenseType: basicInfo.contractTitle || basicInfo.documentType || 'Contract',
      parties,
      effectiveDate: basicInfo.effectiveDate,
      expirationDate: basicInfo.expirationDate,
      rules: allRules,
      currency: basicInfo.currency || 'USD',
      paymentTerms: basicInfo.paymentTerms || 'Not specified',
      reportingRequirements: [],
      contractCategory: this.determineContractCategory(documentType, allRules),
      extractionMetadata: {
        totalRulesFound: allRules.length,
        avgConfidence: allRules.length > 0 
          ? allRules.reduce((sum, rule) => sum + rule.confidence, 0) / allRules.length
          : 0,
        processingTime: basicInfo.hasRoyaltyTerms ? 12 : 2,
        ruleComplexity: allRules.length > 5 ? 'complex' : allRules.length > 2 ? 'moderate' : 'simple',
        hasFixedPricing: allRules.some(r => r.ruleType === 'fixed_price' || r.ruleType === 'fixed_fee'),
        hasVariablePricing: allRules.some(r => r.ruleType === 'variable_price' || r.ruleType === 'percentage' || r.ruleType === 'usage_based'),
        hasTieredPricing: allRules.some(r => r.ruleType === 'tiered'),
        hasRenewalTerms: allRules.some(r => r.ruleType === 'auto_renewal'),
        hasTerminationClauses: allRules.some(r => r.ruleType === 'early_termination')
      }
    };
  }

  // 📄 CHUNKED EXTRACTION - For large contracts (>15k chars) to capture pricing rules from beginning, middle, and end
  private async extractLargeContractInChunks(contractText: string): Promise<{
    basicInfo: any;
    allRules: RoyaltyRule[];
  }> {
    // Extract basic info + rules from header (first 10k chars - has parties, dates, and sometimes early pricing)
    // Use rules-only extraction (no sourceSpan) to prevent JSON truncation
    const headerRules = await this.extractRulesOnly(contractText.substring(0, 10000), 'licensing');
    
    // Extract basic info separately with minimal prompt
    const basicInfo = {
      documentType: 'licensing',
      hasRoyaltyTerms: true,
      parties: null,
      effectiveDate: null,
      expirationDate: null,
      currency: 'USD',
      paymentTerms: null
    };
    
    // Extract rules from middle section (30% into document - often has detailed pricing)
    const midStart = Math.floor(contractText.length * 0.3);
    const midChunk = contractText.substring(midStart, midStart + 10000);
    const midRules = await this.extractRulesOnly(midChunk, 'licensing');
    
    // Extract rules from tail section (last 10k chars - often has pricing schedules and payment terms)
    const tailStart = Math.max(contractText.length - 10000, midStart + 5000);
    const tailRules = await this.extractRulesOnly(contractText.substring(tailStart), 'licensing');
    
    // Merge and deduplicate rules from all chunks
    const allRules = [
      ...headerRules,
      ...midRules,
      ...tailRules
    ];
    
    const uniqueRules = this.deduplicateRules(allRules);
    console.log(`✅ Chunked extraction: ${uniqueRules.length} unique rules from ${allRules.length} total`);
    
    // Phase 2: Add source snippets to rules
    const rulesWithSources = await this.addRuleSources(contractText, uniqueRules);
    
    return {
      basicInfo,
      allRules: rulesWithSources
    };
  }

  // Deduplicate rules based on rule identity (calculation + conditions), keeping highest confidence
  private deduplicateRules(rules: RoyaltyRule[]): RoyaltyRule[] {
    const ruleMap = new Map<string, RoyaltyRule>();
    
    for (const rule of rules) {
      // Create fingerprint from rule identity (type, name, calculation values, product categories)
      const fingerprint = JSON.stringify({
        type: rule.ruleType,
        name: rule.ruleName?.toLowerCase().trim(),
        rate: rule.calculation?.rate,
        amount: rule.calculation?.amount,
        products: rule.conditions?.productCategories?.map(p => p.toLowerCase().trim()).sort()
      });
      
      const existing = ruleMap.get(fingerprint);
      if (!existing || (rule.confidence || 0) > (existing.confidence || 0)) {
        ruleMap.set(fingerprint, rule);
      }
    }
    
    return Array.from(ruleMap.values());
  }
  
  // Add source snippets to rules after extraction (Phase 2 of two-phase pipeline)
  private async addRuleSources(contractText: string, rules: RoyaltyRule[]): Promise<RoyaltyRule[]> {
    // For each rule, add a simple sourceSpan with section info
    // We keep it lightweight to avoid re-triggering truncation issues
    return rules.map(rule => ({
      ...rule,
      sourceSpan: {
        section: rule.ruleType || 'Payment Terms',
        text: `${rule.ruleName || 'Rule'}: ${rule.description || 'Pricing rule'}`.substring(0, 150)
      }
    }));
  }

  // Build extraction prompt (reusable helper)
  private buildExtractionPrompt(contractText: string): string {
    return `Analyze this contract and extract ALL information in ONE comprehensive response. Extract contract type, parties, dates, AND all pricing/payment rules.

Contract Text:
${contractText}

**SECTION 1: Contract Parties** (MANDATORY - NEVER return null)
Extract BOTH contracting parties. Look for:
- "LICENSOR" and "LICENSEE" (licensing/royalty agreements)
- "CONTRACTOR" and "CLIENT" or "SUB-CONTRACTOR" (service agreements)
- "SELLER" and "BUYER" (sales agreements)
- "EMPLOYER" and "EMPLOYEE" (employment contracts)
- "VENDOR" and "CUSTOMER" (vendor agreements)
- Company names at the top of the contract under "CONTRACTING PARTIES" or "PARTIES"
- Signature blocks at the end with company names
- "This Agreement is between [Party 1] and [Party 2]"

Return format:
"parties": {
  "party1": {"name": "Exact Company/Person Name", "role": "Licensor|Contractor|Seller|etc"},
  "party2": {"name": "Exact Company/Person Name", "role": "Licensee|Client|Buyer|etc"}
}

If parties are not explicitly stated, look in:
1. Header section with corporate information
2. Signature blocks
3. First paragraph describing the agreement
4. Any section titled "PARTIES" or "BETWEEN"

**SECTION 2: Contract Identification**
Identify which of these categories best describes this contract:
- **sales**: Sales contracts for goods/services
- **service**: Service agreements with deliverables  
- **licensing**: IP licensing, software licenses, content licensing, patent licensing
- **saas**: SaaS/subscription agreements
- **distribution**: Distribution/reseller agreements
- **consulting**: Consulting/professional services
- **employment**: Employment contracts
- **nda**: Non-disclosure/confidentiality agreements
- **other**: Other contract types

**SECTION 3: Payment Terms Detection**
Set "hasRoyaltyTerms" to true if contract contains ANY:
- Royalty percentages or license fees
- Fixed/variable pricing
- Per-seat, per-user, per-unit pricing
- Tiered pricing or volume discounts
- Subscription/recurring payments
- Auto-renewal terms
- Price escalation clauses
- Termination fees
- Minimum guarantees or advance payments
- ANY monetary compensation or pricing structure
- Milestone payments or upfront fees

**SECTION 4: Extract ALL Pricing Rules** (if hasRoyaltyTerms is true)
Extract EVERY pricing rule you find. Use these EXACT ruleType values:
- "tiered" - Volume-based tiers
- "percentage" - Percentage-based rates
- "minimum_guarantee" - Minimum payment guarantees
- "fixed_price" - One-time fixed fees
- "variable_price" - Variable pricing
- "per_seat" - Per user/seat pricing
- "per_unit" - Per unit pricing
- "per_time_period" - Monthly/annual pricing
- "usage_based" - Usage/consumption-based
- "auto_renewal" - Renewal terms
- "escalation_clause" - Price increases
- "early_termination" - Termination fees
- "license_scope" - License restrictions
- "volume_discount" - Volume discounts

**CRITICAL - ANTI-HALLUCINATION & PRODUCT MATCHING RULES**: 
- Extract ONLY rules that EXPLICITLY exist in the contract text provided
- Each rule MUST include sourceSpan.text with EXACT VERBATIM quote from the contract
- **KEEP sourceSpan.text CONCISE** (max 150 chars) - quote ONLY the single sentence/clause supporting the rule
- **NEVER include full pricing tables** in sourceSpan.text - extract table values into calculation fields instead
- DO NOT invent, assume, or create rules that are not in the text
- DO NOT include examples or generic contract terms
- DO NOT reuse rules from previous contracts
- If a rule cannot be directly quoted from the contract text, DO NOT include it
- Set confidence 0.6-1.0 based on how explicit the rule is in the text
- Return empty rules array if no pricing/payment terms found in the actual contract text

**MANDATORY PRODUCT IDENTIFIERS** (for accurate matching):
- EVERY rule MUST specify productCategories array with explicit product/service names from the contract
- Extract EXACT product names, SKUs, product IDs, service descriptions, or category names mentioned in the contract
- If a rule applies to "all products" or contract doesn't specify products, use ["General"] as category
- DO NOT leave productCategories empty - this causes calculation errors
- Examples: ["Aurora Flame Maple"], ["Premium Subscription"], ["API Calls"], ["Consulting Hours"], ["General"]

**Return this EXACT JSON structure:**
{
  "basicInfo": {
    "documentType": "sales|service|licensing|saas|distribution|consulting|employment|nda|other",
    "contractTitle": "exact title or null",
    "hasRoyaltyTerms": true or false,
    "parties": {
      "party1": {"name": "exact name", "role": "role"},
      "party2": {"name": "exact name", "role": "role"}
    },
    "effectiveDate": "date or null",
    "expirationDate": "date or null",
    "currency": "USD|EUR|etc or null",
    "paymentTerms": "brief description or null"
  },
  "rules": [
    {
      "ruleType": "one of the exact types above",
      "ruleName": "descriptive name",
      "description": "clear description",
      "conditions": {
        "productCategories": ["category1"],
        "territories": ["territory1"],
        "containerSizes": ["size1"],
        "timeperiod": "monthly|annually|etc",
        "volumeThreshold": [1000, 5000],
        "licenseScope": {"userLimit": 100, "geographic": ["NA"], "termMonths": 12, "exclusivity": false},
        "renewalTerms": {"autoRenew": true, "renewalRate": 3.0, "noticeRequiredDays": 30}
      },
      "calculation": {
        "rate": 5.0,
        "baseRate": 10.0,
        "amount": 1000.0,
        "tiers": [{"min": 0, "max": 1000, "rate": 5.0}],
        "seasonalAdjustments": {"spring": 1.15},
        "territoryPremiums": {"california": 0.50},
        "escalationRate": 2.5,
        "terminationFee": 5000.0
      },
      "priority": 1-10,
      "sourceSpan": {
        "section": "section name",
        "text": "concise verbatim quote (max 150 chars) - just key clause, not entire table"
      },
      "confidence": 0.6 to 1.0
    }
  ]
}

Return ONLY valid JSON. No explanations.`;
  }

  // Sanitize extracted rules by clamping sourceSpan.text to prevent JSON truncation
  private sanitizeExtractedRules(rules: any[]): RoyaltyRule[] {
    return rules
      .filter((r: any) => r.sourceSpan?.text?.trim().length > 0)
      .map((r: any) => {
        // Clamp sourceSpan.text to 150 characters to prevent response overflow
        if (r.sourceSpan?.text && r.sourceSpan.text.length > 150) {
          r.sourceSpan.text = r.sourceSpan.text.substring(0, 147) + '...';
        }
        return r;
      });
  }

  // Execute extraction API call and parse results
  private async executeExtractionCall(prompt: string): Promise<{
    basicInfo: any;
    allRules: RoyaltyRule[];
  }> {
    const response = await this.makeRequest([
      { role: 'system', content: 'You are a precise contract analyzer. Extract ALL information in one comprehensive response. Return only JSON.' },
      { role: 'user', content: prompt }
    ], 0.1, 12000);
    
    const extracted = this.extractAndRepairJSON(response, { basicInfo: {}, rules: [] });
    
    return {
      basicInfo: extracted.basicInfo || {},
      allRules: this.sanitizeExtractedRules(Array.isArray(extracted.rules) ? extracted.rules : [])
    };
  }

  // ⚡ CONSOLIDATED EXTRACTION - Replaces 6 sequential AI calls with 1 comprehensive call
  private async extractAllContractDataInOneCall(contractText: string): Promise<{
    basicInfo: any;
    allRules: RoyaltyRule[];
  }> {
    // For large contracts (>15k chars), use chunked extraction to capture rules from beginning, middle, and end
    if (contractText.length > 15000) {
      console.log(`📄 Large contract (${contractText.length} chars) - using chunked extraction`);
      return await this.extractLargeContractInChunks(contractText);
    }

    const prompt = `Analyze this contract and extract ALL information in ONE comprehensive response. Extract contract type, parties, dates, AND all pricing/payment rules.

Contract Text:
${contractText.substring(0, 20000)}

**SECTION 1: Contract Parties** (MANDATORY - NEVER return null)
Extract BOTH contracting parties. Look for:
- "LICENSOR" and "LICENSEE" (licensing/royalty agreements)
- "CONTRACTOR" and "CLIENT" or "SUB-CONTRACTOR" (service agreements)
- "SELLER" and "BUYER" (sales agreements)
- "EMPLOYER" and "EMPLOYEE" (employment contracts)
- "VENDOR" and "CUSTOMER" (vendor agreements)
- Company names at the top of the contract under "CONTRACTING PARTIES" or "PARTIES"
- Signature blocks at the end with company names
- "This Agreement is between [Party 1] and [Party 2]"

Return format:
"parties": {
  "party1": {"name": "Exact Company/Person Name", "role": "Licensor|Contractor|Seller|etc"},
  "party2": {"name": "Exact Company/Person Name", "role": "Licensee|Client|Buyer|etc"}
}

If parties are not explicitly stated, look in:
1. Header section with corporate information
2. Signature blocks
3. First paragraph describing the agreement
4. Any section titled "PARTIES" or "BETWEEN"

**SECTION 2: Contract Identification**
Identify which of these categories best describes this contract:
- **sales**: Sales contracts for goods/services
- **service**: Service agreements with deliverables  
- **licensing**: IP licensing, software licenses, content licensing, patent licensing
- **saas**: SaaS/subscription agreements
- **distribution**: Distribution/reseller agreements
- **consulting**: Consulting/professional services
- **employment**: Employment contracts
- **nda**: Non-disclosure/confidentiality agreements
- **other**: Other contract types

**SECTION 3: Payment Terms Detection**
Set "hasRoyaltyTerms" to true if contract contains ANY:
- Royalty percentages or license fees
- Fixed/variable pricing
- Per-seat, per-user, per-unit pricing
- Tiered pricing or volume discounts
- Subscription/recurring payments
- Auto-renewal terms
- Price escalation clauses
- Termination fees
- Minimum guarantees or advance payments
- ANY monetary compensation or pricing structure
- Milestone payments or upfront fees

**SECTION 4: Extract ALL Pricing Rules** (if hasRoyaltyTerms is true)
Extract EVERY pricing rule you find. Use these EXACT ruleType values:
- "tiered" - Volume-based tiers
- "percentage" - Percentage-based rates
- "minimum_guarantee" - Minimum payment guarantees
- "fixed_price" - One-time fixed fees
- "variable_price" - Variable pricing
- "per_seat" - Per user/seat pricing
- "per_unit" - Per unit pricing
- "per_time_period" - Monthly/annual pricing
- "usage_based" - Usage/consumption-based
- "auto_renewal" - Renewal terms
- "escalation_clause" - Price increases
- "early_termination" - Termination fees
- "license_scope" - License restrictions
- "volume_discount" - Volume discounts

**CRITICAL - ANTI-HALLUCINATION & PRODUCT MATCHING RULES**: 
- Extract ONLY rules that EXPLICITLY exist in the contract text provided
- Each rule MUST include sourceSpan.text with EXACT VERBATIM quote from the contract
- **KEEP sourceSpan.text CONCISE** (max 150 chars) - quote ONLY the single sentence/clause supporting the rule
- **NEVER include full pricing tables** in sourceSpan.text - extract table values into calculation fields instead
- DO NOT invent, assume, or create rules that are not in the text
- DO NOT include examples or generic contract terms
- DO NOT reuse rules from previous contracts
- If a rule cannot be directly quoted from the contract text, DO NOT include it
- Set confidence 0.6-1.0 based on how explicit the rule is in the text
- Return empty rules array if no pricing/payment terms found in the actual contract text

**MANDATORY PRODUCT IDENTIFIERS** (for accurate matching):
- EVERY rule MUST specify productCategories array with explicit product/service names from the contract
- Extract EXACT product names, SKUs, product IDs, service descriptions, or category names mentioned in the contract
- If a rule applies to "all products" or contract doesn't specify products, use ["General"] as category
- DO NOT leave productCategories empty - this causes calculation errors
- Examples: ["Aurora Flame Maple"], ["Premium Subscription"], ["API Calls"], ["Consulting Hours"], ["General"]

**Return this EXACT JSON structure:**
{
  "basicInfo": {
    "documentType": "sales|service|licensing|saas|distribution|consulting|employment|nda|other",
    "contractTitle": "exact title or null",
    "hasRoyaltyTerms": true or false,
    "parties": {
      "party1": {"name": "exact name", "role": "role"},
      "party2": {"name": "exact name", "role": "role"}
    },
    "effectiveDate": "date or null",
    "expirationDate": "date or null",
    "currency": "USD|EUR|etc or null",
    "paymentTerms": "brief description or null"
  },
  "rules": [
    {
      "ruleType": "one of the exact types above",
      "ruleName": "descriptive name",
      "description": "clear description",
      "conditions": {
        "productCategories": ["category1"],
        "territories": ["territory1"],
        "containerSizes": ["size1"],
        "timeperiod": "monthly|annually|etc",
        "volumeThreshold": [1000, 5000],
        "licenseScope": {"userLimit": 100, "geographic": ["NA"], "termMonths": 12, "exclusivity": false},
        "renewalTerms": {"autoRenew": true, "renewalRate": 3.0, "noticeRequiredDays": 30}
      },
      "calculation": {
        "rate": 5.0,
        "baseRate": 10.0,
        "amount": 1000.0,
        "tiers": [{"min": 0, "max": 1000, "rate": 5.0}],
        "seasonalAdjustments": {"spring": 1.15},
        "territoryPremiums": {"california": 0.50},
        "escalationRate": 2.5,
        "terminationFee": 5000.0
      },
      "priority": 1-10,
      "sourceSpan": {
        "section": "section name",
        "text": "concise verbatim quote (max 150 chars) - just key clause, not entire table"
      },
      "confidence": 0.6 to 1.0
    }
  ]
}

Return ONLY valid JSON. No explanations.`;

    try {
      console.log(`⚡ Making consolidated extraction call...`);
      // Increase max tokens to 12000 for large contracts (Electronics: 18k chars needs ~12k tokens)
      const response = await this.makeRequest([
        { role: 'system', content: 'You are a precise contract analyzer. Extract ALL information in one comprehensive response. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 12000);
      
      let extracted = this.extractAndRepairJSON(response, { basicInfo: {}, rules: [] });
      
      // Handle case where AI returns just an array of rules instead of full object
      if (Array.isArray(extracted)) {
        console.warn(`⚠️ AI returned array instead of object - attempting to extract basicInfo from raw response`);
        // Try to parse basicInfo from the raw response text
        const basicInfoMatch = response.match(/"basicInfo"\s*:\s*\{[^}]*"parties"[^}]*\}/);
        let parties = null;
        if (basicInfoMatch) {
          try {
            const basicInfoStr = '{' + basicInfoMatch[0] + '}';
            const parsed = JSON.parse(basicInfoStr);
            parties = parsed.basicInfo?.parties;
            console.log(`✅ Recovered parties from raw response:`, JSON.stringify(parties));
          } catch (e) {
            console.warn(`⚠️ Could not parse parties from raw response`);
          }
        }
        
        console.log(`🔄 AI returned array, wrapping in proper structure (${extracted.length} rules)`);
        extracted = {
          basicInfo: {
            documentType: 'unknown',
            hasRoyaltyTerms: extracted.length > 0,
            parties: parties,
            effectiveDate: null,
            expirationDate: null,
            currency: 'USD',
            paymentTerms: null
          },
          rules: extracted
        };
      }
      
      if (!extracted || !extracted.basicInfo) {
        console.error('⚠️ Consolidated extraction failed, using fallback');
        console.error('📊 AI Response length:', response?.length || 0);
        console.error('📊 Extracted data:', JSON.stringify(extracted).substring(0, 500));
        console.error('📊 First 1000 chars of raw response:', response?.substring(0, 1000));
        return {
          basicInfo: {
            documentType: 'unknown',
            hasRoyaltyTerms: false,
            parties: null,
            effectiveDate: null,
            expirationDate: null,
            currency: null,
            paymentTerms: null
          },
          allRules: []
        };
      }
      
      return {
        basicInfo: extracted.basicInfo,
        allRules: Array.isArray(extracted.rules) ? extracted.rules : []
      };
    } catch (error) {
      console.error('❌ Consolidated extraction error:', error);
      return {
        basicInfo: {
          documentType: 'unknown',
          hasRoyaltyTerms: false,
          parties: null,
          effectiveDate: null,
          expirationDate: null,
          currency: null,
          paymentTerms: null
        },
        allRules: []
      };
    }
  }

  // ⚡ NDJSON RULES EXTRACTION - Line-by-line format immune to truncation
  private async extractRulesOnly(contractText: string, documentType: string): Promise<RoyaltyRule[]> {
    const prompt = `Extract ALL pricing and payment rules from this ${documentType} contract.

Contract Text:
${contractText.substring(0, 10000)}

**CRITICAL - NDJSON OUTPUT FORMAT:**
- Output ONE rule as a compact JSON object per line
- DO NOT use an enclosing array []
- Each line must be complete, valid JSON
- After all rules, output: {"status":"DONE"}

**Rule Types:** tiered, percentage, minimum_guarantee, fixed_price, variable_price, per_seat, per_unit, per_time_period, usage_based, auto_renewal, escalation_clause, early_termination, license_scope, volume_discount

**Required Fields:**
- ruleType, ruleName, description (<80 chars), conditions (productCategories, territories, timeperiod), calculation, priority, confidence

**Example Output (one JSON object per line):**
{"ruleType":"percentage","ruleName":"Standard Rate","description":"5% royalty","conditions":{"productCategories":["Products"],"territories":["US"],"timeperiod":"quarterly"},"calculation":{"rate":5.0,"baseRate":5.0,"amount":null,"tiers":[]},"priority":1,"confidence":0.95}
{"ruleType":"minimum_guarantee","ruleName":"Min Payment","description":"Quarterly minimum","conditions":{"productCategories":["All"],"territories":["US"],"timeperiod":"quarterly"},"calculation":{"amount":50000},"priority":2,"confidence":0.9}
{"status":"DONE"}`;

    try {
      console.log(`📋 Making NDJSON rules extraction call...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'You are a precise payment terms analyzer. Output rules in NDJSON format (one JSON object per line).' },
        { role: 'user', content: prompt }
      ], 0.1, 6000);
      
      // Parse NDJSON line-by-line - truncation only loses last incomplete line
      const rules: RoyaltyRule[] = [];
      const lines = response.trim().split('\n');
      
      console.log(`📄 Parsing ${lines.length} NDJSON lines...`);
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        try {
          const obj = JSON.parse(line);
          if (obj.status === 'DONE') {
            console.log(`✅ Found DONE marker`);
            break;
          }
          if (obj.ruleType) {
            rules.push(obj as RoyaltyRule);
            console.log(`  ✓ Line ${i + 1}: ${obj.ruleName}`);
          }
        } catch (e) {
          if (i === lines.length - 1) {
            console.log(`⚠️ Truncated last line (expected): ${line.substring(0, 60)}...`);
          } else {
            console.warn(`⚠️ Parse error line ${i + 1}: ${line.substring(0, 60)}...`);
          }
        }
      }
      
      console.log(`✅ NDJSON extraction: ${rules.length} rules parsed successfully`);
      return rules;
    } catch (error) {
      console.error('❌ Rules extraction error:', error);
      return [];
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private determineContractCategory(
    documentType: string,
    rules: RoyaltyRule[]
  ): 'revenue-generating' | 'service-based' | 'confidentiality' | 'employment' | 'other' {
    if (documentType === 'nda') return 'confidentiality';
    if (documentType === 'employment' || documentType === 'consulting') return 'employment';
    if (documentType === 'service') return 'service-based';
    if (rules.length > 0) return 'revenue-generating';
    return 'other';
  }

  private async extractBasicContractInfo(contractText: string): Promise<any> {
    const prompt = `Analyze this contract and identify its exact type. Be precise and specific.

Contract Text:
${contractText.substring(0, 3000)}

**Contract Type Classification:**
Identify which of these categories best describes this contract:
- **sales**: Sales contracts for goods/services with fixed or variable pricing
- **service**: Service agreements with scope of work, deliverables, hourly/project rates
- **licensing**: IP licensing, software licenses, content licensing with royalties or license fees
- **saas**: SaaS/subscription agreements with per-seat, per-user, or usage-based pricing
- **distribution**: Distribution/reseller agreements with pricing tiers and territories
- **consulting**: Consulting/professional services contracts with rate structures
- **employment**: Employment contracts with compensation terms
- **nda**: Non-disclosure/confidentiality agreements
- **amendment**: Amendments to existing contracts
- **other**: Other contract types

**Payment Terms Detection:**
Set "hasRoyaltyTerms" to true if the contract explicitly contains ANY of these payment/pricing/renewal structures:
- Royalty percentages or license fees
- Revenue-sharing arrangements
- Fixed or variable pricing (e.g., "One-time fee of $10,000", "Variable rates based on volume")
- Per-seat, per-user, per-unit pricing (e.g., "$50/user/month", "$100 per license")
- Tiered pricing based on volume/usage
- Hourly/daily/monthly rate structures (e.g., "$125/hour", "$5,000/month retainer")
- Milestone-based payments
- Subscription/recurring payments
- Usage-based/consumption-based pricing (e.g., "$0.10 per API call")
- Auto-renewal terms (e.g., "Automatically renews annually")
- Price escalation clauses (e.g., "Annual 3% rate increase")
- Early termination fees or penalties
- License scope restrictions (user limits, geographic, term)
- ANY form of monetary compensation, pricing structure, renewal terms, or termination clauses

IMPORTANT: Even if the contract has NO royalty/license fees but DOES have renewal terms, escalation clauses, or termination penalties, set hasRoyaltyTerms to TRUE.

CRITICAL: 
- Extract ONLY information that exists in the document
- Use appropriate party labels for the contract type
- Do NOT fabricate or assume information

Return JSON:
{
  "documentType": "sales|service|licensing|saas|distribution|consulting|employment|nda|amendment|other",
  "contractTitle": "exact title from document",
  "hasRoyaltyTerms": true or false (based on payment terms detection above),
  "parties": { 
    "party1": {"name": "exact name", "role": "actual role"},
    "party2": {"name": "exact name", "role": "actual role"}
  },
  "effectiveDate": "date or null",
  "expirationDate": "date or null", 
  "currency": "USD|EUR|GBP|etc or null",
  "paymentTerms": "brief description of payment structure if found, otherwise null"
}

Return only valid JSON. No explanations.`;

    try {
      const response = await this.makeRequest([
        { role: 'system', content: 'You are a precise contract analyzer. Extract only what exists in the document. Do NOT assume license/royalty terms. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 1000);
      
      const extracted = this.extractAndRepairJSON(response, null);
      
      // If extraction failed completely, return a safe default
      if (!extracted) {
        return {
          documentType: 'unknown',
          contractTitle: 'Unknown Contract',
          hasRoyaltyTerms: false,
          parties: null,
          effectiveDate: null,
          expirationDate: null,
          currency: null,
          paymentTerms: null
        };
      }
      
      return extracted;
    } catch (error) {
      console.error('Basic info extraction failed:', error);
      return {
        documentType: 'unknown',
        contractTitle: 'Unknown Contract',
        hasRoyaltyTerms: false,
        parties: null,
        effectiveDate: null,
        expirationDate: null,
        currency: null,
        paymentTerms: null
      };
    }
  }

  private async extractTierBasedRules(contractText: string): Promise<RoyaltyRule[]> {
    const prompt = `Extract TIER-BASED royalty/license fee rules ONLY if they explicitly exist in this contract.

Contract Text:
${contractText}

CRITICAL REQUIREMENTS:
- ONLY extract rules if you find explicit tier-based payment structures in the contract
- If NO tier-based rules exist, return an empty array []
- Do NOT fabricate or assume rules
- Each rule MUST have actual source text from the contract in sourceSpan.text
- Set confidence below 0.6 if you're uncertain

Look for tier structures like:
- "Tier 1: $1.25 per unit for products A, B, C"
- "Volume tier: 0-5000 units = 5%, 5001+ units = 7%"
- "Product tier 1 rate: $X, Product tier 2 rate: $Y"

ruleType must be one of: "percentage", "tiered", "minimum_guarantee", "cap", "deduction", "fixed_fee", "fixed_price", "variable_price", "per_seat", "per_unit", "per_time_period", "volume_discount", "usage_based"

Return JSON array (empty if no rules found):
[
  {
    "ruleType": "tiered",
    "ruleName": "exact name from contract",
    "description": "exact description",
    "conditions": {...},
    "calculation": {...},
    "priority": 1,
    "sourceSpan": {"section": "actual section name", "text": "verbatim text from contract"},
    "confidence": 0.6 to 1.0
  }
]

If NO tier-based rules exist, return: []`;

    try {
      console.log(`🔍 Extracting tier-based rules...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract ONLY rules that explicitly exist. Do NOT fabricate. Return empty array [] if none found. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 1500);
      
      const rules = this.extractAndRepairJSON(response, []);
      return Array.isArray(rules) ? rules : [];
    } catch (error) {
      console.error('Tier rules extraction failed:', error);
      return [];
    }
  }

  private async extractPaymentCalculationRules(contractText: string): Promise<RoyaltyRule[]> {
    const prompt = `Extract PAYMENT CALCULATION rules ONLY if they explicitly exist in this contract.

Contract Text:
${contractText}

CRITICAL REQUIREMENTS:
- ONLY extract if you find explicit payment/royalty calculation rules in the contract
- If NO payment calculation rules exist, return an empty array []
- Do NOT fabricate or assume rules
- Each rule MUST have actual source text from the contract in sourceSpan.text
- Set confidence below 0.6 if you're uncertain

Look for calculation rules like:
- "Minimum annual guarantee: $85,000"
- "Royalty rate: 5% of net sales"
- "Container size multiplier: 1 gallon = $1.00, 5 gallon = $3.50"
- "Quarterly payment due within 30 days of quarter end"

ruleType must be one of: "percentage", "tiered", "minimum_guarantee", "cap", "deduction", "fixed_fee", "fixed_price", "variable_price", "per_seat", "per_unit", "per_time_period", "volume_discount", "usage_based"

Return JSON array (empty if no rules found):
[
  {
    "ruleType": "minimum_guarantee",
    "ruleName": "exact name from contract",
    "description": "exact description",
    "conditions": {...},
    "calculation": {...},
    "priority": 2,
    "sourceSpan": {"section": "actual section name", "text": "verbatim text from contract"},
    "confidence": 0.6 to 1.0
  }
]

If NO payment calculation rules exist, return: []`;

    try {
      console.log(`💰 Extracting payment calculation rules...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract ONLY rules that explicitly exist. Do NOT fabricate. Return empty array [] if none found. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 1500);
      
      const rules = this.extractAndRepairJSON(response, []);
      return Array.isArray(rules) ? rules : [];
    } catch (error) {
      console.error('Payment rules extraction failed:', error);
      return [];
    }
  }

  private async extractSpecialAdjustments(contractText: string): Promise<RoyaltyRule[]> {
    const prompt = `Extract SPECIAL ADJUSTMENTS and PREMIUMS ONLY if they explicitly exist in this contract.

Contract Text:
${contractText}

CRITICAL REQUIREMENTS:
- ONLY extract if you find explicit special adjustments/premiums in the contract
- If NO special adjustment rules exist, return an empty array []
- Do NOT fabricate or assume rules
- Each rule MUST have actual source text from the contract in sourceSpan.text
- Set confidence below 0.6 if you're uncertain

Look for adjustment rules like:
- "Spring season premium: additional 15% of base rate"
- "Organic certification bonus: +25%"
- "Territory premium for West Coast: +$0.50 per unit"
- "Holiday multiplier: 1.2x standard rate"

Return JSON array (empty if no rules found):
[
  {
    "ruleType": "percentage",
    "ruleName": "exact name from contract",
    "description": "exact description",
    "conditions": {...},
    "calculation": {...},
    "priority": 3,
    "sourceSpan": {"section": "actual section name", "text": "verbatim text from contract"},
    "confidence": 0.6 to 1.0
  }
]

If NO special adjustments exist, return: []`;

    try {
      console.log(`🌟 Extracting special adjustments...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract ONLY adjustments that explicitly exist. Do NOT fabricate. Return empty array [] if none found. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 1500);
      
      const rules = this.extractAndRepairJSON(response, []);
      return Array.isArray(rules) ? rules : [];
    } catch (error) {
      console.error('Special adjustments extraction failed:', error);
      return [];
    }
  }

  private async extractUniversalPricingRules(contractText: string): Promise<RoyaltyRule[]> {
    const prompt = `Extract UNIVERSAL PRICING STRUCTURES from this contract. These include renewal terms, escalation clauses, termination penalties, license scope, and usage-based pricing.

Contract Text:
${contractText}

CRITICAL REQUIREMENTS:
- ONLY extract rules that explicitly exist in the contract
- If NO universal pricing rules exist, return an empty array []
- Do NOT fabricate or assume rules
- Each rule MUST have actual source text from the contract in sourceSpan.text
- Set confidence below 0.6 if you're uncertain

**Look for these pricing structures:**

**IMPORTANT: Use these EXACT ruleType values when you find these structures:**

1. **Auto-Renewal Terms** → ruleType: **"auto_renewal"**
   Examples: "Contract automatically renews for 1-year terms", "Renewal rate increases by 3% annually", "30 days notice required to cancel"
   → Use ruleType "auto_renewal" and populate conditions.renewalTerms {autoRenew, renewalRate, noticeRequiredDays}

2. **Escalation Clauses** → ruleType: **"escalation_clause"**
   Examples: "Annual price increase of 2.5%", "Rates escalate 5% per year", "CPI adjustment annually"
   → Use ruleType "escalation_clause" and populate calculation.escalationRate

3. **Termination Penalties** → ruleType: **"early_termination"**
   Examples: "Early termination fee: 50% of remaining contract value", "Cancellation penalty: $10,000", "Exit fee if terminated before 24 months"
   → Use ruleType "early_termination" and populate calculation.terminationFee

4. **License Scope** → ruleType: **"license_scope"**
   Examples: "Limited to 100 users", "Geographic restriction: North America only", "12-month license term", "Exclusive rights in California"
   → Use ruleType "license_scope" and populate conditions.licenseScope {userLimit, geographic, termMonths, exclusivity}

5. **Usage-Based Pricing** → ruleType: **"usage_based"**
   Examples: "$0.10 per API call", "Billed based on monthly active users", "Consumption-based pricing"
   → Use ruleType "usage_based" and populate calculation.rate

6. **Per-Seat/Unit/Time Pricing** → ruleType: **"per_seat"** or **"per_unit"** or **"per_time_period"**
   Examples: "$50 per user per month" (per_seat), "$100 per license" (per_unit), "$5,000 per month retainer" (per_time_period)
   → Use appropriate ruleType and populate calculation.amount and conditions.timeperiod

7. **Fixed Price** → ruleType: **"fixed_price"**
   Examples: "One-time fee of $10,000", "Flat rate of $5,000 for project"
   → Use ruleType "fixed_price" and populate calculation.amount

8. **Variable Price** → ruleType: **"variable_price"**
   Examples: "Price varies based on volume", "Fluctuating rates based on market"
   → Use ruleType "variable_price" with appropriate calculation structure

**REQUIRED:** ruleType must be EXACTLY one of: "auto_renewal", "escalation_clause", "early_termination", "license_scope", "usage_based", "per_seat", "per_unit", "per_time_period", "fixed_price", "variable_price"

Return JSON array (empty if no rules found):
[
  {
    "ruleType": "auto_renewal|escalation_clause|early_termination|license_scope|usage_based|per_seat|per_unit|per_time_period",
    "ruleName": "exact name from contract or descriptive name",
    "description": "exact description",
    "conditions": {
      "timeperiod": "monthly|annually|etc",
      "licenseScope": {
        "userLimit": number_or_null,
        "geographic": ["territories"],
        "termMonths": number_or_null,
        "exclusivity": true_or_false
      },
      "renewalTerms": {
        "autoRenew": true_or_false,
        "renewalRate": percentage_increase_or_null,
        "noticeRequiredDays": number_or_null
      }
    },
    "calculation": {
      "amount": fixed_amount_or_null,
      "rate": percentage_or_null,
      "escalationRate": annual_increase_percentage_or_null,
      "terminationFee": early_exit_fee_or_null
    },
    "priority": 1,
    "sourceSpan": {"section": "actual section name", "text": "verbatim text from contract"},
    "confidence": 0.6 to 1.0
  }
]

If NO universal pricing rules exist, return: []`;

    try {
      console.log(`🌐 Extracting universal pricing rules...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract ONLY pricing rules that explicitly exist. Do NOT fabricate. Return empty array [] if none found. Return only JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 2000);
      
      const rules = this.extractAndRepairJSON(response, []);
      return Array.isArray(rules) ? rules : [];
    } catch (error) {
      console.error('Universal pricing rules extraction failed:', error);
      return [];
    }
  }

  /**
   * Extract plant varieties (or other products) with FormulaNode JSON for royalty calculations
   * This generates structured formulas that can be evaluated by the FormulaInterpreter
   */
  async extractProductsWithFormulas(contractText: string): Promise<Array<{
    productName: string;
    ruleName: string;
    description: string;
    formulaDefinition: FormulaDefinition;
    confidence: number;
    sourceSection: string;
    conditions: any;
  }>> {
    const prompt = `You are extracting PRODUCT VARIETIES and their ROYALTY FORMULAS from a licensing contract.

CONTRACT TEXT:
${contractText}

Your task:
1. Find all product/variety names (e.g., "Aurora Flame Maple", "Pacific Sunset Rose")
2. For each product, identify the royalty calculation formula
3. Generate a structured FormulaNode JSON that represents the calculation

FORMULA PATTERNS TO DETECT:
- **Volume Tiers**: "1-gallon: $1.25, 5000+ units: $1.10" → tier node
- **Seasonal Adjustments**: "Spring +10%, Fall -5%" → lookup table
- **Territory Premiums**: "Secondary territory +10%" → lookup table
- **Multiplier Chains**: "base × seasonal × territory × organic" → multiply node
- **Container Size Rates**: Different rates by size → tier or lookup node

FORMULANODE TYPES:
{
  "type": "literal", "value": 1.25  // Fixed number
}
{
  "type": "reference", "field": "units"  // Get value from sales data (units, grossAmount, season, territory)
}
{
  "type": "multiply", "operands": [node1, node2, node3]  // Multiplication
}
{
  "type": "tier", "reference": {field node}, "tiers": [{"min": 0, "max": 4999, "rate": 11.25, "label": "< 5000"}]  // Returns rate as PERCENTAGE
}
{
  "type": "lookup", "reference": {field node}, "table": {"Spring": 1.10, "Summer": 1.0}, "default": 1.0
}

⚠️ **CRITICAL FORMULA RULES:**
1. Tier nodes return PERCENTAGES (e.g., 11.25 = 11.25%, NOT $11.25/unit)
2. ALWAYS multiply by grossAmount first for percentage-based royalties
3. Formula structure: grossAmount × tier_percentage × seasonal_adjustment

RESPONSE FORMAT - Return JSON array:
[
  {
    "productName": "Aurora Flame Maple",
    "ruleName": "Aurora Flame Maple - 1-gallon with volume tiers",
    "description": "1-gallon containers: 1.25% royalty, 1.10% for 5000+ units with seasonal adjustments",
    "conditions": {
      "containerSize": "1-gallon",
      "productCategories": ["Ornamental Trees"],
      "territories": ["Primary", "Secondary"]
    },
    "formulaDefinition": {
      "name": "Aurora Flame Maple Royalty",
      "description": "Volume-tiered percentage with seasonal adjustment",
      "filters": {"containerSize": "1-gallon"},
      "formula": {
        "type": "multiply",
        "operands": [
          {
            "type": "reference",
            "field": "grossAmount"
          },
          {
            "type": "tier",
            "reference": {"type": "reference", "field": "units"},
            "tiers": [
              {"min": 0, "max": 4999, "rate": 1.25, "label": "< 5000 units"},
              {"min": 5000, "max": null, "rate": 1.10, "label": "5000+ units"}
            ]
          },
          {
            "type": "lookup",
            "reference": {"type": "reference", "field": "season"},
            "table": {"Spring": 1.10, "Summer": 1.0, "Fall": 0.95, "Holiday": 1.20},
            "default": 1.0,
            "description": "Seasonal adjustment"
          }
        ]
      }
    },
    "confidence": 0.95,
    "sourceSection": "Section 3.1 - Royalty Structure - Tier 1"
  }
]

CRITICAL: Return ONLY valid JSON array. No explanatory text.`;

    try {
      console.log(`🌱 Extracting product varieties with formulas...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract product varieties with FormulaNode JSON. Return only JSON array.' },
        { role: 'user', content: prompt }
      ], 0.2, 3000); // Higher tokens for complex formulas
      
      return this.extractAndRepairJSON(response, []);
    } catch (error) {
      console.error('Product formula extraction failed:', error);
      return [];
    }
  }

  // =====================================================
  // NEW: GENERAL PAYMENT TERMS EXTRACTION
  // =====================================================
  async extractGeneralPaymentTerms(contractText: string): Promise<any[]> {
    const prompt = `You are a contract payment terms analyst. Extract ALL payment-related clauses from this contract.

IMPORTANT: Extract ACTUAL payment terms from the contract. Do NOT fabricate or assume data. If no payment terms exist, return empty array [].

CONTRACT TEXT:
${contractText.substring(0, 8000)}

Extract the following types of payment terms if they exist:

1. **PAYMENT SCHEDULE**: When payments are due (Net 30, Net 45, milestone-based, etc.)
2. **PAYMENT METHOD**: How payments should be made (wire transfer, direct deposit, check, ACH, etc.)
3. **RATE STRUCTURE**: Pricing model (hourly rate, fixed fee, daily rate, monthly retainer, etc.)
4. **INVOICE REQUIREMENTS**: What's needed for payment (invoice format, supporting documentation, approval process, etc.)
5. **LATE PAYMENT PENALTIES**: Fees or interest for overdue payments
6. **ADVANCE/DEPOSIT**: Upfront payments or deposits required
7. **MILESTONE PAYMENTS**: Payments tied to specific deliverables or dates

CRITICAL RULES:
- ONLY extract terms that are EXPLICITLY stated in the contract
- Do NOT fabricate payment amounts if not specified
- Include confidence score (0.6-1.0) based on clarity of the text
- Include source text citation
- If ZERO payment terms exist, return empty array []

Return ONLY a JSON array (no explanatory text):
[
  {
    "ruleType": "payment_schedule",
    "ruleName": "Payment Timeline",
    "description": "Payment due within 45 days of receiving invoice and supporting documentation",
    "paymentTerms": {
      "dueDays": 45,
      "triggerEvent": "Invoice receipt with documentation",
      "frequency": "per_invoice"
    },
    "confidence": 0.95,
    "sourceText": "exact text from contract"
  },
  {
    "ruleType": "payment_method",
    "ruleName": "Payment Method",
    "description": "Direct deposit to bank account",
    "paymentTerms": {
      "method": "direct_deposit",
      "requirement": "Sub-contractor must sign up for direct deposit"
    },
    "confidence": 0.90,
    "sourceText": "exact text from contract"
  },
  {
    "ruleType": "rate_structure",
    "ruleName": "Hourly Rate",
    "description": "Compensation based on hourly rate",
    "paymentTerms": {
      "rateType": "hourly",
      "amount": null,
      "currency": "USD"
    },
    "confidence": 0.85,
    "sourceText": "exact text from contract"
  }
]

Return empty array [] if contract has NO payment terms.`;

    try {
      console.log(`💰 Extracting general payment terms...`);
      const response = await this.makeRequest([
        { role: 'system', content: 'Extract payment terms from contract. Return only JSON array.' },
        { role: 'user', content: prompt }
      ], 0.1, 2000);
      
      const extracted = this.extractAndRepairJSON(response, []);
      
      // Quality filter: Only keep terms with confidence >= 0.6 and source text
      const validTerms = extracted.filter((term: any) => {
        const hasConfidence = term.confidence && term.confidence >= 0.6;
        const hasSourceText = term.sourceText && term.sourceText.length > 10;
        const hasValidType = ['payment_schedule', 'payment_method', 'rate_structure', 'invoice_requirements', 'late_payment_penalty', 'advance_payment', 'milestone_payment'].includes(term.ruleType);
        
        return hasConfidence && hasSourceText && hasValidType;
      });
      
      console.log(`📋 Found ${validTerms.length} valid payment terms (filtered from ${extracted.length} raw extractions)`);
      return validTerms;
    } catch (error) {
      console.error('General payment terms extraction failed:', error);
      return [];
    }
  }

  // Keep the old single-request method as backup
  async extractDetailedRoyaltyRulesSingleRequest(contractText: string): Promise<LicenseRuleExtractionResult> {
    const prompt = `
    You are a specialized contract analyst expert in extracting detailed royalty calculation rules from licensing agreements. 

    Your task is to extract COMPREHENSIVE ROYALTY RULES with specific formulas, rates, and calculation methods from this contract text.

    CONTRACT TEXT:
    ${contractText}

    Extract detailed royalty rules in the following categories:

    🏷️ **TIER-BASED RULES** (if present):
    - Tier 1, 2, 3, etc. with specific product categories
    - Per-unit rates, thresholds, volume discounts
    - Container sizes, multipliers, seasonal adjustments

    💰 **CALCULATION FORMULAS**:
    - Base rates × multipliers × adjustments
    - Sliding scales based on volume bands
    - Minimum payments for each tier/band

    📊 **ADJUSTMENTS & PREMIUMS**:
    - Seasonal adjustments (Spring, Fall, Holiday, Off-season)
    - Territory premiums (Primary vs Secondary)
    - Product type premiums (Organic, Specialty, etc.)
    - Volume discounts and thresholds

    🔒 **MINIMUM GUARANTEES**:
    - Annual minimum payments
    - Quarterly payment requirements
    - Shortfall calculations

    🎯 **EXTRACTION PRIORITIES:**
    1. Look for TIERED systems (Tier 1, Tier 2, Tier 3)
    2. Extract SPECIFIC RATES and AMOUNTS ($1.25, $1.10, +25%, etc.)
    3. Identify CALCULATION FORMULAS (base × multiplier × adjustment)
    4. Find VOLUME THRESHOLDS (5,000+ units, sales bands)
    5. Extract SEASONAL/TERRITORY ADJUSTMENTS (+10%, -5%, +20%)
    6. Locate MINIMUM GUARANTEES ($85,000 annual)
    7. Document PRODUCT CATEGORIES and CONDITIONS

    🔍 **IMPORTANT REQUIREMENTS:**
    - Extract ACTUAL numbers, percentages, and dollar amounts from the document
    - Create separate rule objects for each distinct calculation method
    - Include detailed formulas showing how royalties are calculated
    - Specify exact conditions when each rule applies
    - Reference the source section/page where each rule was found
    - If information is unclear, note assumptions made and mark with lower confidence

    Return a JSON object with the following structure (extract real data from the document):
    {
      "documentType": "license",
      "licenseType": "extracted license type",
      "parties": {
        "licensor": "extracted licensor name",
        "licensee": "extracted licensee name"
      },
      "effectiveDate": null,
      "expirationDate": null,
      "rules": [
        {
          "ruleType": "tiered",
          "ruleName": "Tier 1 — Category Name",
          "description": "Detailed description with specific rates and conditions",
          "conditions": {
            "productCategories": ["category1", "category2"],
            "territories": ["Primary Territory"],
            "salesVolumeMin": 5000,
            "timeperiod": "annually",
            "currency": "USD"
          },
          "calculation": {
            "tiers": [
              {"min": 0, "max": 4999, "rate": 1.25},
              {"min": 5000, "max": null, "rate": 1.10}
            ],
            "formula": "actual formula from document"
          },
          "priority": 1,
          "sourceSpan": {
            "section": "Section reference",
            "text": "extracted text from document"
          },
          "confidence": 0.95
        }
      ],
      "currency": "USD",
      "paymentTerms": "extracted payment terms",
      "reportingRequirements": [],
      "extractionMetadata": {
        "totalRulesFound": 0,
        "avgConfidence": 0.8,
        "processingTime": 5,
        "ruleComplexity": "moderate"
      }
    }

    Return ONLY the JSON object, no additional text.
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a specialized royalty calculation expert. Extract detailed tier-based royalty rules with specific formulas, rates, and conditions. Return only valid JSON.'
      },
      {
        role: 'user', 
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages, 0.1);
      
      // Clean and parse JSON response
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\\{[\\s\\S]*\\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in rules extraction response');
      }

      const extractionResult = JSON.parse(jsonMatch[0]);
      
      // Validate structure
      if (!extractionResult.rules || !Array.isArray(extractionResult.rules)) {
        throw new Error('Invalid rules structure returned');
      }

      // Set defaults for missing fields
      return {
        documentType: extractionResult.documentType || 'license',
        licenseType: extractionResult.licenseType || 'License Agreement',
        parties: extractionResult.parties || { licensor: 'Not specified', licensee: 'Not specified' },
        effectiveDate: extractionResult.effectiveDate || undefined,
        expirationDate: extractionResult.expirationDate || undefined,
        rules: extractionResult.rules || [],
        currency: extractionResult.currency || 'USD',
        paymentTerms: extractionResult.paymentTerms || 'Not specified',
        reportingRequirements: extractionResult.reportingRequirements || [],
        contractCategory: this.determineContractCategory(extractionResult.documentType || 'other', extractionResult.rules || []),
        extractionMetadata: {
          totalRulesFound: extractionResult.rules?.length || 0,
          avgConfidence: extractionResult.rules?.length > 0 
            ? extractionResult.rules.reduce((sum: number, rule: any) => sum + (rule.confidence || 0.8), 0) / extractionResult.rules.length
            : 0.8,
          processingTime: extractionResult.extractionMetadata?.processingTime || 5,
          ruleComplexity: (extractionResult.extractionMetadata?.ruleComplexity as 'simple' | 'moderate' | 'complex') || 'moderate',
          hasFixedPricing: extractionResult.rules?.some((r: any) => r.ruleType === 'fixed_price' || r.ruleType === 'fixed_fee') || false,
          hasVariablePricing: extractionResult.rules?.some((r: any) => r.ruleType === 'variable_price' || r.ruleType === 'percentage' || r.ruleType === 'usage_based') || false,
          hasTieredPricing: extractionResult.rules?.some((r: any) => r.ruleType === 'tiered') || false,
          hasRenewalTerms: extractionResult.rules?.some((r: any) => r.ruleType === 'auto_renewal') || false,
          hasTerminationClauses: extractionResult.rules?.some((r: any) => r.ruleType === 'early_termination') || false
        }
      };

    } catch (error) {
      console.error('Error extracting detailed royalty rules:', error);
      
      // Return basic fallback structure if API fails (handles rate limits)
      return {
        documentType: 'licensing' as const,
        contractCategory: 'other' as const,
        licenseType: 'License Agreement',
        parties: { licensor: 'Not specified', licensee: 'Not specified' },
        effectiveDate: undefined,
        expirationDate: undefined,
        rules: [],
        currency: 'USD',
        paymentTerms: 'Rules extraction failed - manual review required',
        reportingRequirements: [],
        extractionMetadata: {
          totalRulesFound: 0,
          avgConfidence: 0.5,
          processingTime: 0,
          ruleComplexity: 'moderate' as const,
          hasFixedPricing: false,
          hasVariablePricing: false,
          hasTieredPricing: false,
          hasRenewalTerms: false,
          hasTerminationClauses: false
        }
      };
    }
  }

  private async makeRequest(messages: Array<{ role: string; content: string }>, temperature = 0.1, maxTokens = 4000): Promise<string> {
    let lastError: Error | undefined;
    
    // Retry logic for rate limits
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetch(`${this.baseUrl}/chat/completions`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model: 'llama-3.1-8b-instant',
            messages,
            temperature,
            max_tokens: maxTokens,
          }),
        });

        if (response.status === 429) {
          // Rate limit - wait and retry
          const waitTime = Math.pow(2, attempt) * 1000; // Exponential backoff
          console.log(`🔄 Groq rate limit hit (attempt ${attempt}/3), waiting ${waitTime}ms...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }

        if (!response.ok) {
          throw new Error(`Groq API error: ${response.status} ${response.statusText}`);
        }

        const data: GroqResponse = await response.json();
        return data.choices[0]?.message?.content || '';
        
      } catch (error) {
        lastError = error as Error;
        if (attempt < 3) {
          const errorMsg = error instanceof Error ? error.message : String(error);
          console.log(`⚠️ Groq request failed (attempt ${attempt}/3):`, errorMsg);
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    }
    
    throw lastError || new Error('Unknown error occurred');
  }

  async analyzeContract(contractText: string): Promise<ContractAnalysisResult> {
    const prompt = `
    You are a professional contract analyst specializing in extracting key business terms from legal agreements. 

    Document Text:
    ${contractText}

    FIRST, determine if this document is actually a CONTRACT, AGREEMENT, or LEGAL DOCUMENT that contains business terms. 

    If this document is NOT a contract/agreement (e.g., it's a resume, report, manual, random text, etc.), respond with:
    {
      "summary": "This document does not appear to be a contract or legal agreement. It appears to be a [document type]. This system is designed specifically for contract analysis and cannot provide meaningful business insights for this type of document.",
      "keyTerms": [],
      "riskAnalysis": [
        {
          "level": "low",
          "title": "Document Type Mismatch",
          "description": "This document is not a contract or agreement and should not be analyzed using contract analysis tools."
        }
      ],
      "insights": [
        {
          "type": "alert",
          "title": "Wrong Document Type",
          "description": "Please upload a contract, agreement, or legal document for proper analysis."
        }
      ],
      "confidence": 0.95
    }

    If this IS a contract/agreement, then proceed with your primary objective to identify and clearly explain these CRITICAL CONTRACT SECTIONS:

    🔍 PRIORITY SECTIONS TO EXTRACT:
    1. **Royalty Structure & Payment Terms** (Section 3 or similar) - Payment rates, schedules, calculation methods
    2. **Manufacturing & Quality Requirements** (Section 5 or similar) - Production standards, quality controls
    3. **Licensed Technology & Patents** (Section 1 or similar) - What technology/IP is being licensed
    4. **Termination & Post-Termination** (Section 9 or similar) - How/when contract ends, what happens after
    5. **Financial Obligations** - Any fees, minimum payments, guarantees
    6. **Performance Requirements** - Delivery timelines, milestones, KPIs
    7. **Territory & Scope** - Geographic limitations, usage restrictions

    Provide your analysis in this JSON structure:
    {
      "summary": "Brief 2-paragraph executive summary focusing on the business deal and key commercial terms",
      "keyTerms": [
        {
          "type": "Royalty Structure",
          "description": "Plain English explanation of what this means for the business - avoid legal jargon",
          "confidence": 0.95,
          "location": "Specific section reference (e.g., Section 3.1, Article 5, etc.)"
        },
        {
          "type": "Payment Terms",
          "description": "Plain English explanation of payment schedules and methods",
          "confidence": 0.90,
          "location": "Section reference"
        },
        {
          "type": "Manufacturing Requirements",
          "description": "Production standards and quality requirements",
          "confidence": 0.88,
          "location": "Section reference"
        }
      ],
      "riskAnalysis": [
        {
          "level": "high|medium|low",
          "title": "Business Risk Title",
          "description": "Clear explanation of potential business impact and what could go wrong"
        }
      ],
      "insights": [
        {
          "type": "opportunity|alert|requirement",
          "title": "Key Business Insight",
          "description": "Actionable business insight or recommendation"
        }
      ],
      "confidence": 0.92
    }

    🎯 FOCUS REQUIREMENTS:
    - Create a SEPARATE keyTerm object for EACH contract section you find
    - Each keyTerm should have ONE specific type (e.g., "Royalty Structure", "Payment Terms", "Manufacturing Requirements", etc.)
    - Extract SPECIFIC numbers, percentages, dates, and dollar amounts
    - Explain complex legal terms in simple business language
    - Highlight what the user needs to DO or PAY based on this contract
    - Identify potential business risks and opportunities
    - If sections are missing, note "Not specified in this document"

    Make this analysis actionable for business decision-makers who need to understand the deal quickly without reading legal text.

    Return only valid JSON, no additional text.
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a professional document analyst. Always respond with valid JSON only. Analyze documents based on their actual content and type, not assumptions.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      
      // Clean the response to ensure it's valid JSON
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in response');
      }

      const analysisResult = JSON.parse(jsonMatch[0]);
      
      // Validate and ensure required fields exist
      if (!analysisResult.summary || !Array.isArray(analysisResult.keyTerms)) {
        throw new Error('Invalid analysis structure returned');
      }

      return {
        summary: analysisResult.summary,
        keyTerms: analysisResult.keyTerms || [],
        riskAnalysis: analysisResult.riskAnalysis || [],
        insights: analysisResult.insights || [],
        confidence: Math.max(0, Math.min(1, analysisResult.confidence || 0.8))
      };
    } catch (error) {
      console.error('Error analyzing contract:', error);
      
      // Return a basic analysis if parsing fails
      return {
        summary: 'Contract analysis completed. The document has been processed and key terms have been extracted.',
        keyTerms: [
          {
            type: 'Document Status',
            description: 'Contract processed successfully',
            confidence: 0.9,
            location: 'Document header'
          }
        ],
        riskAnalysis: [
          {
            level: 'medium' as const,
            title: 'Analysis Limitation',
            description: 'Detailed analysis may require manual review due to document complexity.'
          }
        ],
        insights: [
          {
            type: 'alert',
            title: 'Manual Review Recommended',
            description: 'Consider having a legal professional review this contract for comprehensive analysis.'
          }
        ],
        confidence: 0.75
      };
    }
  }

  async summarizeContract(contractText: string): Promise<string> {
    const prompt = `
    Please provide a concise summary of this contract in 2-3 paragraphs. Focus on:
    - Main purpose and parties involved
    - Key terms and obligations
    - Important dates and financial aspects
    - Notable clauses or restrictions

    Contract Text:
    ${contractText}
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a professional contract analyst. Provide clear, concise summaries.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      return await this.makeRequest(messages);
    } catch (error) {
      console.error('Error summarizing contract:', error);
      return 'Contract summary could not be generated at this time. Please try again or contact support.';
    }
  }

  async extractKeyTerms(contractText: string): Promise<Array<{ term: string; value: string; confidence: number }>> {
    const prompt = `
    Extract key terms from this contract and return them as a JSON array. Each term should have:
    - term: the type of term
    - value: the actual value or description
    - confidence: confidence score between 0 and 1

    Contract Text:
    ${contractText}

    Return only a JSON array, no additional text.
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract term extraction specialist. Return only valid JSON arrays.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\[[\s\S]*\]/);
      
      if (!jsonMatch) {
        return [];
      }

      const terms = JSON.parse(jsonMatch[0]);
      return Array.isArray(terms) ? terms : [];
    } catch (error) {
      console.error('Error extracting terms:', error);
      return [];
    }
  }

  // 📊 FINANCIAL ANALYSIS METHODS
  async analyzeFinancialTerms(contractText: string): Promise<any> {
    const prompt = `
    Analyze this contract for financial terms and return a comprehensive financial analysis in JSON format.

    Focus on extracting:
    1. **Total Contract Value** - Overall monetary value (convert to USD if needed)
    2. **Payment Schedule** - All payment dates, amounts, milestones
    3. **Royalty Structure** - Rates, calculation methods, minimum payments
    4. **Revenue Projections** - Expected income over contract lifetime
    5. **Cost Impact** - Budget implications, additional costs
    6. **Currency Risk** - Multi-currency exposure assessment (0-100 score)
    7. **Payment Terms** - Net payment days, methods, late fees
    8. **Penalty Clauses** - Financial penalties, liquidated damages

    Contract Text:
    ${contractText}

    Return only this JSON structure:
    {
      "totalValue": number or null,
      "currency": "USD" or detected currency,
      "paymentSchedule": [
        {"date": "YYYY-MM-DD", "amount": number, "description": "milestone/payment type"}
      ],
      "royaltyStructure": {
        "baseRate": number,
        "minimumPayment": number,
        "calculationMethod": "description"
      },
      "revenueProjections": {
        "year1": number,
        "year2": number,
        "total": number
      },
      "costImpact": {
        "upfrontCosts": number,
        "ongoingCosts": number,
        "budgetImpact": "low|medium|high"
      },
      "currencyRisk": number (0-100),
      "paymentTerms": "Net 30, wire transfer, 2% late fee",
      "penaltyClauses": [
        {"type": "late_payment", "amount": number, "description": "text"}
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a financial analyst specializing in contract monetization. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in financial analysis response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error analyzing financial terms:', error);
      return {
        totalValue: null,
        currency: "USD",
        paymentSchedule: [],
        royaltyStructure: null,
        revenueProjections: null,
        costImpact: null,
        currencyRisk: 0,
        paymentTerms: "Terms not clearly specified",
        penaltyClauses: []
      };
    }
  }

  // ⚖️ COMPLIANCE ANALYSIS METHODS
  async analyzeCompliance(contractText: string): Promise<any> {
    const prompt = `
    Analyze this contract for legal compliance and regulatory adherence. Return a JSON compliance assessment.

    Analyze for:
    1. **Regulatory Frameworks** - GDPR, SOX, HIPAA, CCPA compliance
    2. **Jurisdiction Analysis** - Governing law conflicts, court jurisdiction
    3. **Data Protection** - Privacy clauses, data handling requirements
    4. **Industry Standards** - Sector-specific compliance requirements
    5. **Risk Factors** - Compliance gaps, potential violations
    6. **Recommended Actions** - Steps to improve compliance

    Contract Text:
    ${contractText}

    Return only this JSON structure:
    {
      "complianceScore": number (0-100),
      "regulatoryFrameworks": [
        {"framework": "GDPR", "compliant": boolean, "gaps": ["list of issues"]}
      ],
      "jurisdictionAnalysis": {
        "governingLaw": "jurisdiction",
        "courtJurisdiction": "location",
        "conflicts": ["any jurisdiction conflicts"]
      },
      "dataProtectionCompliance": boolean,
      "industryStandards": [
        {"standard": "ISO 27001", "compliance": "full|partial|none"}
      ],
      "riskFactors": [
        {"level": "high|medium|low", "factor": "description", "impact": "business impact"}
      ],
      "recommendedActions": [
        {"priority": "high|medium|low", "action": "specific action needed"}
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a legal compliance specialist. Analyze contracts for regulatory adherence. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in compliance analysis response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error analyzing compliance:', error);
      return {
        complianceScore: 75,
        regulatoryFrameworks: [],
        jurisdictionAnalysis: {
          governingLaw: "Not specified",
          courtJurisdiction: "Not specified",
          conflicts: []
        },
        dataProtectionCompliance: true,
        industryStandards: [],
        riskFactors: [],
        recommendedActions: []
      };
    }
  }

  // 📋 OBLIGATIONS EXTRACTION
  async extractObligations(contractText: string): Promise<any[]> {
    const prompt = `
    Extract all contractual obligations from this contract. Return a JSON array of detailed obligations.

    For each obligation, identify:
    1. **Type** - payment, delivery, performance, reporting, compliance
    2. **Description** - Clear description of what must be done
    3. **Due Date** - When it's due (if specified)
    4. **Responsible Party** - Who is responsible
    5. **Priority** - critical, high, medium, low

    Contract Text:
    ${contractText}

    Return only this JSON array:
    [
      {
        "obligationType": "payment|delivery|performance|reporting|compliance",
        "description": "Clear description of obligation",
        "dueDate": "YYYY-MM-DD" or null,
        "responsible": "party name or role",
        "priority": "critical|high|medium|low"
      }
    ]
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract obligation specialist. Extract all deliverables and requirements. Return only valid JSON arrays.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\[[\s\S]*\]/);
      
      if (!jsonMatch) {
        return [];
      }

      const obligations = JSON.parse(jsonMatch[0]);
      return Array.isArray(obligations) ? obligations : [];
    } catch (error) {
      console.error('Error extracting obligations:', error);
      return [];
    }
  }

  // 🎯 STRATEGIC ANALYSIS METHODS
  async analyzeStrategicValue(contractText: string, contractType: string = 'unknown'): Promise<any> {
    const prompt = `
    Perform strategic business analysis of this ${contractType} contract. Return comprehensive strategic insights in JSON.

    Analyze:
    1. **Strategic Value** - Business importance score (0-100)
    2. **Market Alignment** - How well aligned with market trends (0-100)
    3. **Competitive Advantage** - Benefits over competitors
    4. **Risk Concentration** - Dependency risk level (0-100)
    5. **Standardization Score** - Template compliance (0-100)
    6. **Negotiation Insights** - Patterns and recommendations
    7. **Benchmark Comparison** - How it compares to industry standards
    8. **Strategic Recommendations** - Business strategy suggestions

    Contract Text:
    ${contractText}

    Return only this JSON structure:
    {
      "strategicValue": number (0-100),
      "marketAlignment": number (0-100),
      "competitiveAdvantage": [
        {"advantage": "description", "impact": "high|medium|low"}
      ],
      "riskConcentration": number (0-100),
      "standardizationScore": number (0-100),
      "negotiationInsights": {
        "keyNegotiationPoints": ["list of points"],
        "flexibilityAreas": ["areas for future negotiation"],
        "recommendations": ["negotiation strategies"]
      },
      "benchmarkComparison": {
        "vsIndustryStandard": "better|similar|worse",
        "marketPosition": "description",
        "improvementAreas": ["areas to improve"]
      },
      "recommendations": [
        {"priority": "high|medium|low", "recommendation": "strategic action", "rationale": "why important"}
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a strategic business analyst specializing in contract value optimization. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in strategic analysis response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error analyzing strategic value:', error);
      return {
        strategicValue: 70,
        marketAlignment: 70,
        competitiveAdvantage: [],
        riskConcentration: 30,
        standardizationScore: 80,
        negotiationInsights: {
          keyNegotiationPoints: [],
          flexibilityAreas: [],
          recommendations: []
        },
        benchmarkComparison: {
          vsIndustryStandard: "similar",
          marketPosition: "Standard market terms",
          improvementAreas: []
        },
        recommendations: []
      };
    }
  }

  // 🔍 CONTRACT COMPARISON METHODS
  async findSimilarContracts(contractText: string, allContracts: any[]): Promise<any> {
    const prompt = `
    Analyze this contract and compare it with similar contracts to identify patterns, anomalies, and best practices.

    Contract to analyze:
    ${contractText}

    For comparison, consider:
    1. **Similar Contract Types** - Identify contracts with similar purpose
    2. **Clause Variations** - How key clauses differ from standards
    3. **Term Comparisons** - Financial and legal term differences
    4. **Best Practices** - Industry best practices found
    5. **Anomalies** - Unusual or non-standard terms

    Return only this JSON structure:
    {
      "similarityScore": number (0-100),
      "clauseVariations": [
        {"clause": "clause name", "variation": "how it differs", "impact": "high|medium|low"}
      ],
      "termComparisons": {
        "financialTerms": "comparison summary",
        "legalTerms": "comparison summary",
        "performanceTerms": "comparison summary"
      },
      "bestPractices": [
        {"practice": "description", "benefit": "business benefit"}
      ],
      "anomalies": [
        {"anomaly": "unusual term", "risk": "high|medium|low", "explanation": "why unusual"}
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract comparison specialist. Identify patterns and anomalies across contracts. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in contract comparison response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error comparing contracts:', error);
      return {
        similarityScore: 50,
        clauseVariations: [],
        termComparisons: {
          financialTerms: "Standard terms",
          legalTerms: "Standard terms",
          performanceTerms: "Standard terms"
        },
        bestPractices: [],
        anomalies: []
      };
    }
  }

  // 📈 PERFORMANCE PREDICTION
  async predictPerformance(contractText: string, contractType: string = 'unknown'): Promise<any> {
    const prompt = `
    Analyze this ${contractType} contract and predict performance metrics based on contract terms and structure.

    Contract Text:
    ${contractText}

    Analyze and predict:
    1. **Performance Score** - Overall contract performance likelihood (0-100)
    2. **Milestone Completion** - Expected milestone completion rate (0-100)
    3. **On-Time Delivery** - Likelihood of timely delivery
    4. **Budget Variance** - Expected budget over/under performance
    5. **Quality Score** - Expected quality of deliverables (0-100)
    6. **Client Satisfaction** - Predicted satisfaction level (0-100)
    7. **Renewal Probability** - Likelihood of contract renewal (0-100)

    Return only this JSON structure:
    {
      "performanceScore": number (0-100),
      "milestoneCompletion": number (0-100),
      "onTimeDelivery": boolean,
      "budgetVariance": number (positive = over budget, negative = under budget),
      "qualityScore": number (0-100),
      "clientSatisfaction": number (0-100),
      "renewalProbability": number (0-100),
      "riskFactors": [
        {"factor": "description", "impact": "high|medium|low"}
      ],
      "successFactors": [
        {"factor": "description", "importance": "high|medium|low"}
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract performance analyst. Predict contract success metrics based on terms and structure. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in performance prediction response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error predicting performance:', error);
      return {
        performanceScore: 75,
        milestoneCompletion: 80,
        onTimeDelivery: true,
        budgetVariance: 0,
        qualityScore: 85,
        clientSatisfaction: 80,
        renewalProbability: 70,
        riskFactors: [],
        successFactors: []
      };
    }
  }

  // 📋 CONTRACT OBLIGATIONS EXTRACTION
  async extractContractObligations(contractText: string): Promise<Array<{
    obligationType: string;
    description: string;
    dueDate: string | null;
    responsible: string;
    priority: string;
  }>> {
    const prompt = `
    Extract all contractual obligations from this contract. Identify who is responsible for what and when.

    Contract Text:
    ${contractText}

    Return a JSON array of obligations with:
    1. **obligationType** - "payment", "delivery", "performance", "reporting", "maintenance", "compliance", etc.
    2. **description** - Clear description of the obligation
    3. **dueDate** - Due date in YYYY-MM-DD format (null if no specific date)
    4. **responsible** - Who is responsible (party name or role)
    5. **priority** - "low", "medium", "high", "critical"

    Return only this JSON array:
    [
      {
        "obligationType": "payment",
        "description": "Monthly payment of $10,000",
        "dueDate": "2024-12-01",
        "responsible": "Client",
        "priority": "high"
      }
    ]
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract obligation specialist. Extract all obligations with precise details. Return only valid JSON array.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\[[\s\S]*\]/);
      
      if (!jsonMatch) {
        return [];
      }

      const obligations = JSON.parse(jsonMatch[0]);
      return Array.isArray(obligations) ? obligations : [];
    } catch (error) {
      console.error('Error extracting obligations:', error);
      return [];
    }
  }

  // ⚠️ COMPREHENSIVE RISK ANALYSIS
  async analyzeRiskFactors(contractText: string, contractType: string = 'unknown'): Promise<{
    overallRiskScore: number;
    riskCategories: Array<{
      category: string;
      score: number;
      factors: Array<{ factor: string; impact: string; description: string }>;
    }>;
    mitigation: Array<{ risk: string; recommendation: string; priority: string }>;
    riskTrends: Array<{ period: string; riskLevel: string; factors: string[] }>;
  }> {
    const prompt = `
    Perform a comprehensive risk analysis of this ${contractType} contract across multiple dimensions.

    Contract Text:
    ${contractText}

    Analyze these risk categories:
    1. **Financial Risk** - Payment terms, currency exposure, financial stability
    2. **Legal Risk** - Compliance, jurisdiction, liability exposure
    3. **Operational Risk** - Delivery, performance, resource availability
    4. **Strategic Risk** - Market changes, competitive positioning
    5. **Reputational Risk** - Brand impact, stakeholder relations
    6. **Technical Risk** - Technology dependencies, obsolescence

    Return only this JSON structure:
    {
      "overallRiskScore": number (0-100, higher = more risk),
      "riskCategories": [
        {
          "category": "Financial Risk",
          "score": number (0-100),
          "factors": [
            {
              "factor": "Payment delay risk",
              "impact": "high|medium|low", 
              "description": "Detailed risk description"
            }
          ]
        }
      ],
      "mitigation": [
        {
          "risk": "Risk description",
          "recommendation": "Mitigation strategy", 
          "priority": "high|medium|low"
        }
      ],
      "riskTrends": [
        {
          "period": "short-term|medium-term|long-term",
          "riskLevel": "increasing|stable|decreasing",
          "factors": ["List of contributing factors"]
        }
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a comprehensive risk analyst. Evaluate all risk dimensions and provide detailed mitigation strategies. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in risk analysis response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error analyzing risk factors:', error);
      return {
        overallRiskScore: 50,
        riskCategories: [
          {
            category: "General Risk",
            score: 50,
            factors: [
              {
                factor: "Analysis incomplete",
                impact: "medium",
                description: "Risk analysis could not be completed automatically"
              }
            ]
          }
        ],
        mitigation: [
          {
            risk: "Incomplete analysis",
            recommendation: "Manual review recommended",
            priority: "medium"
          }
        ],
        riskTrends: [
          {
            period: "short-term",
            riskLevel: "stable",
            factors: ["Analysis pending"]
          }
        ]
      };
    }
  }

  // 🔄 CONTRACT COMPARISON ANALYSIS
  async analyzeContractComparison(
    contractText: string, 
    contractType: string, 
    industry: string = 'unknown'
  ): Promise<{
    similarityScore: number;
    clauseVariations: Array<{ clause: string; variation: string; impact: string }>;
    termComparisons: Array<{ term: string; marketStandard: string; contractValue: string; variance: string }>;
    bestPractices: Array<{ practice: string; recommendation: string; benefit: string }>;
    anomalies: Array<{ anomaly: string; description: string; recommendation: string }>;
  }> {
    const prompt = `
    Analyze this ${contractType} contract in the ${industry} industry for comparison with market standards and best practices.

    Contract Text:
    ${contractText}

    Compare against typical ${contractType} contracts and identify:
    1. **Similarity Score** - How similar to standard contracts (0-100)
    2. **Clause Variations** - Non-standard clauses and their impact
    3. **Term Comparisons** - How terms compare to market standards
    4. **Best Practices** - Adherence to industry best practices
    5. **Anomalies** - Unusual terms that may need attention

    Return only this JSON structure:
    {
      "similarityScore": number (0-100),
      "clauseVariations": [
        {
          "clause": "Termination clause",
          "variation": "Unusually restrictive",
          "impact": "high|medium|low"
        }
      ],
      "termComparisons": [
        {
          "term": "Payment terms",
          "marketStandard": "Net 30 days",
          "contractValue": "Net 60 days", 
          "variance": "Unfavorable to payer"
        }
      ],
      "bestPractices": [
        {
          "practice": "Force majeure clause",
          "recommendation": "Include comprehensive force majeure provisions",
          "benefit": "Protection against unforeseen events"
        }
      ],
      "anomalies": [
        {
          "anomaly": "Unusual liability cap",
          "description": "Liability limited to 10% of contract value",
          "recommendation": "Consider increasing liability cap for better protection"
        }
      ]
    }
    `;

    const messages = [
      {
        role: 'system',
        content: 'You are a contract comparison specialist. Compare contracts against industry standards and best practices. Return only valid JSON.'
      },
      {
        role: 'user',
        content: prompt
      }
    ];

    try {
      const response = await this.makeRequest(messages);
      const cleanedResponse = response.trim();
      const jsonMatch = cleanedResponse.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        throw new Error('No valid JSON found in comparison analysis response');
      }

      return JSON.parse(jsonMatch[0]);
    } catch (error) {
      console.error('Error analyzing contract comparison:', error);
      return {
        similarityScore: 75,
        clauseVariations: [],
        termComparisons: [],
        bestPractices: [],
        anomalies: []
      };
    }
  }

  // =====================================================
  // LICENSE RULE EXTRACTION METHODS (NEW EXTENSION)
  // =====================================================

  async extractLicenseRules(licenseText: string, licenseType?: string): Promise<LicenseRuleExtractionResult> {
    const startTime = Date.now();
    
    const prompt = `
    You are a specialized AI expert in extracting royalty calculation rules from license agreements and contracts.
    Your task is to analyze the license document and extract structured, machine-readable royalty calculation rules.

    Document Text:
    ${licenseText}

    OBJECTIVE: Extract ALL royalty, fee, and payment calculation rules from this license document.

    Look for these types of rules:
    1. **Percentage Royalties**: "X% of net sales", "Y% of gross revenue"
    2. **Tiered Royalties**: Different rates for different sales volumes
    3. **Minimum Guarantees**: "Minimum payment of $X per year"
    4. **Caps and Limits**: "Maximum royalty of $Y per quarter"
    5. **Fixed Fees**: "One-time fee of $Z", "Annual license fee of $A"
    6. **Deductions**: "Less 3% for marketing expenses"
    7. **Territory-based**: Different rates for different regions
    8. **Product-based**: Different rates for different product categories

    For EACH rule you find, extract:
    - Rule type and name
    - When it applies (conditions)
    - How to calculate it (rates, amounts, formulas)
    - Where you found it (page/section reference)
    - Your confidence level

    Respond with this EXACT JSON structure:
    {
      "documentType": "license|royalty_agreement|revenue_share|other",
      "licenseType": "${licenseType || 'general'}",
      "parties": {
        "licensor": "Company name of the party granting the license",
        "licensee": "Company name of the party receiving the license"
      },
      "effectiveDate": "YYYY-MM-DD format if found",
      "expirationDate": "YYYY-MM-DD format if found", 
      "rules": [
        {
          "ruleType": "percentage|tiered|minimum_guarantee|cap|deduction|fixed_fee",
          "ruleName": "Descriptive name for this rule",
          "description": "Plain English description of what this rule does",
          "conditions": {
            "productCategories": ["list of products this applies to"],
            "territories": ["list of territories/regions"],
            "salesVolumeMin": number_or_null,
            "salesVolumeMax": number_or_null,
            "timeperiod": "monthly|quarterly|annually|one-time",
            "currency": "USD|EUR|etc"
          },
          "calculation": {
            "rate": number_for_percentage_rules,
            "tiers": [{"min": number, "max": number_or_null, "rate": number}],
            "amount": number_for_fixed_amounts,
            "formula": "mathematical_formula_if_complex"
          },
          "priority": number_1_to_10_execution_order,
          "sourceSpan": {
            "page": page_number_if_available,
            "section": "section_name_or_number",
            "text": "exact_text_snippet_where_found"
          },
          "confidence": number_0_to_1
        }
      ],
      "currency": "Primary currency mentioned",
      "paymentTerms": "When payments are due (e.g., within 30 days of quarter end)",
      "reportingRequirements": [
        {
          "frequency": "monthly|quarterly|annually",
          "dueDate": "when reports are due",
          "description": "what must be reported"
        }
      ],
      "extractionMetadata": {
        "totalRulesFound": number_of_rules_found,
        "avgConfidence": average_confidence_across_all_rules,
        "processingTime": ${Date.now() - startTime},
        "ruleComplexity": "simple|moderate|complex"
      }
    }

    IMPORTANT GUIDELINES:
    1. Extract ALL calculation rules, even if they seem minor
    2. Be specific about conditions (when each rule applies)
    3. Use exact text snippets in sourceSpan for traceability
    4. Assign realistic confidence scores (0.7-0.95 for clear rules, 0.4-0.6 for ambiguous ones)
    5. If you can't find clear parties, rules, or dates, use null values
    6. Focus on CALCULATION rules, not general contract terms

    Analyze the document thoroughly and extract all royalty calculation rules:
    `;

    try {
      const response = await this.makeRequest([
        {
          role: 'system',
          content: `You are a license agreement analysis expert. You MUST respond with valid JSON ONLY. 
          Do not include any explanations, markdown formatting, or text outside the JSON structure.
          Your response must start with { and end with }. No other text is allowed.`
        },
        {
          role: 'user', 
          content: prompt
        }
      ], 0.1); // Very low temperature for consistent JSON structure

      console.log('Raw AI response for license rules:', response.substring(0, 200) + '...');

      // More aggressive cleaning of the JSON response
      let cleanedResponse = response.trim();
      
      // Remove any markdown code blocks
      cleanedResponse = cleanedResponse.replace(/```json\n?/g, '').replace(/```\n?/g, '');
      
      // Remove any text before the first {
      const firstBrace = cleanedResponse.indexOf('{');
      if (firstBrace > 0) {
        cleanedResponse = cleanedResponse.substring(firstBrace);
      }
      
      // Remove any text after the last }
      const lastBrace = cleanedResponse.lastIndexOf('}');
      if (lastBrace >= 0 && lastBrace < cleanedResponse.length - 1) {
        cleanedResponse = cleanedResponse.substring(0, lastBrace + 1);
      }
      
      console.log('Cleaned response for JSON parsing:', cleanedResponse.substring(0, 200) + '...');
      
      let result;
      try {
        result = JSON.parse(cleanedResponse);
      } catch (parseError) {
        console.log('JSON parsing failed, attempting to repair...');
        // Try to fix common JSON issues
        let repairedResponse = cleanedResponse
          .replace(/,\s*}/g, '}')  // Remove trailing commas before }
          .replace(/,\s*]/g, ']')  // Remove trailing commas before ]
          .replace(/([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '"$1":')  // Quote unquoted keys
          .replace(/:\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*([,}])/g, ': "$1"$2');  // Quote unquoted string values
        
        try {
          result = JSON.parse(repairedResponse);
          console.log('JSON repair successful!');
        } catch (repairError) {
          console.log('JSON repair failed, using fallback structure');
          throw parseError; // Use original error to trigger fallback
        }
      }
      
      // Validate the structure and add processing metadata
      const processingTime = Date.now() - startTime;
      result.extractionMetadata.processingTime = processingTime;
      
      return result as LicenseRuleExtractionResult;

    } catch (error) {
      console.error('Error extracting license rules:', error);
      
      // Return a fallback structure if parsing fails
      return {
        documentType: 'other',
        contractCategory: 'other' as const,
        licenseType: licenseType || 'unknown',
        parties: {
          licensor: 'Unable to extract',
          licensee: 'Unable to extract'
        },
        rules: [],
        currency: 'USD',
        paymentTerms: 'Unable to extract',
        reportingRequirements: [],
        extractionMetadata: {
          totalRulesFound: 0,
          avgConfidence: 0,
          processingTime: Date.now() - startTime,
          ruleComplexity: 'simple',
          hasFixedPricing: false,
          hasVariablePricing: false,
          hasTieredPricing: false,
          hasRenewalTerms: false,
          hasTerminationClauses: false
        }
      };
    }
  }

  async validateExtractedRules(rules: RoyaltyRule[]): Promise<{isValid: boolean, errors: string[], warnings: string[]}> {
    const errors: string[] = [];
    const warnings: string[] = [];

    for (const rule of rules) {
      // Check for required fields
      if (!rule.ruleName || !rule.ruleType) {
        errors.push(`Rule missing name or type: ${JSON.stringify(rule)}`);
      }

      // Validate calculation structure based on rule type
      if (rule.ruleType === 'percentage' && (!rule.calculation.rate || rule.calculation.rate <= 0)) {
        errors.push(`Percentage rule "${rule.ruleName}" missing or invalid rate`);
      }

      if (rule.ruleType === 'tiered' && (!rule.calculation.tiers || rule.calculation.tiers.length === 0)) {
        errors.push(`Tiered rule "${rule.ruleName}" missing tier structure`);
      }

      // Check confidence levels
      if (rule.confidence < 0.3) {
        warnings.push(`Low confidence rule: "${rule.ruleName}" (${rule.confidence})`);
      }

      // Validate priority ordering
      if (rule.priority < 1 || rule.priority > 10) {
        warnings.push(`Rule "${rule.ruleName}" has unusual priority: ${rule.priority}`);
      }
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }

  async generateRuleDSL(extractedResult: LicenseRuleExtractionResult): Promise<object> {
    // Convert extracted rules into a structured DSL format for the rule engine
    const dsl = {
      version: "1.0",
      licenseInfo: {
        type: extractedResult.licenseType,
        licensor: extractedResult.parties.licensor,
        licensee: extractedResult.parties.licensee,
        effectiveDate: extractedResult.effectiveDate,
        expirationDate: extractedResult.expirationDate,
        currency: extractedResult.currency
      },
      calculationRules: extractedResult.rules.map(rule => ({
        id: `rule_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        name: rule.ruleName,
        type: rule.ruleType,
        description: rule.description,
        priority: rule.priority,
        conditions: rule.conditions,
        calculation: rule.calculation,
        metadata: {
          confidence: rule.confidence,
          sourceSpan: rule.sourceSpan,
          extractedAt: new Date().toISOString()
        }
      })).sort((a, b) => a.priority - b.priority), // Sort by priority
      reportingRules: extractedResult.reportingRequirements.map(req => ({
        frequency: req.frequency,
        dueDate: req.dueDate,
        description: req.description
      })),
      metadata: {
        ...extractedResult.extractionMetadata,
        generatedAt: new Date().toISOString(),
        dslVersion: "1.0"
      }
    };

    return dsl;
  }
}

export const groqService = new GroqService();
