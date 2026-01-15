import { GroqService } from './groqService.js';
import { OpenAIService } from './openaiService';

export interface ContractAnalysisResult {
  summary: string;
  keyTerms: { type: string; description: string; confidence: number; location: string; }[];
  riskAnalysis: string[];
  insights: string[];
  confidence: number;
}

export interface LicenseRuleExtractionResult {
  documentType: string;
  licenseType: string;
  parties: {
    licensor: string;
    licensee: string;
  };
  effectiveDate?: string;
  expirationDate?: string;
  rules: Array<{
    ruleType: string;
    ruleName: string;
    description: string;
    conditions: any;
    calculation: any;
    priority: number;
    sourceSpan: any;
    confidence: number;
  }>;
  currency: string;
  paymentTerms: string;
  reportingRequirements: any[];
  extractionMetadata: {
    totalRulesFound: number;
    avgConfidence: number;
    processingTime: number;
    ruleComplexity: 'simple' | 'moderate' | 'complex';
  };
}

export interface AIProvider {
  analyzeContract(contractText: string): Promise<ContractAnalysisResult>;
  extractDetailedRoyaltyRules(contractText: string): Promise<LicenseRuleExtractionResult>;
}

export class AIProviderService {
  private groqService: GroqService;
  private openaiService: OpenAIService | null = null;
  private groqFailureCount = 0;
  private groqCooldownUntil: number = 0;
  private readonly MAX_FAILURES = 3;
  private readonly COOLDOWN_DURATION = 5 * 60 * 1000; // 5 minutes

  constructor() {
    this.groqService = new GroqService();
    // Initialize OpenAI service lazily only when needed
  }

  private getOpenAIService(): OpenAIService {
    if (!this.openaiService) {
      try {
        this.openaiService = new OpenAIService();
      } catch (error) {
        throw new Error('OpenAI service not available - API key not configured. Please add OPENAI_API_KEY to environment variables for backup extraction.');
      }
    }
    return this.openaiService;
  }

  async analyzeContract(contractText: string): Promise<ContractAnalysisResult> {
    return this.executeWithFailover(
      () => this.groqService.analyzeContract(contractText),
      () => this.getOpenAIService().analyzeContract(contractText),
      'analyzeContract'
    );
  }

  async extractDetailedRoyaltyRules(contractText: string): Promise<LicenseRuleExtractionResult> {
    // Pre-filter text to royalty-relevant sections to reduce payload
    const royaltyRelevantText = this.extractRoyaltyRelevantSections(contractText);
    
    return this.executeWithFailover(
      () => this.groqService.extractDetailedRoyaltyRules(royaltyRelevantText),
      () => this.getOpenAIService().extractDetailedRoyaltyRules(royaltyRelevantText),
      'extractDetailedRoyaltyRules'
    );
  }

  private async executeWithFailover<T>(
    primaryFn: () => Promise<T>,
    fallbackFn: () => Promise<T>,
    operation: string
  ): Promise<T> {
    const now = Date.now();
    
    // Check if Groq is in cooldown
    if (now < this.groqCooldownUntil) {
      console.log(`🔄 Groq in cooldown, using OpenAI for ${operation}`);
      return fallbackFn();
    }

    try {
      console.log(`🚀 Attempting ${operation} with Groq (primary)`);
      const result = await primaryFn();
      
      // Reset failure count on success
      this.groqFailureCount = 0;
      return result;
      
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      console.log(`⚠️ Groq ${operation} failed: ${errorMsg}`);
      
      // Check if it's a rate limit or retriable error
      if (errorMsg.includes('429') || errorMsg.includes('Too Many Requests') || 
          errorMsg.includes('5')) {
        
        this.groqFailureCount++;
        console.log(`📈 Groq failure count: ${this.groqFailureCount}/${this.MAX_FAILURES}`);
        
        // If too many failures, put Groq in cooldown
        if (this.groqFailureCount >= this.MAX_FAILURES) {
          this.groqCooldownUntil = now + this.COOLDOWN_DURATION;
          console.log(`🚫 Groq in cooldown for ${this.COOLDOWN_DURATION/1000/60} minutes`);
        }
        
        // Fallback to OpenAI
        console.log(`🔄 Falling back to OpenAI for ${operation}`);
        try {
          return fallbackFn();
        } catch (openaiError) {
          console.error(`❌ OpenAI fallback also failed: ${openaiError}`);
          throw new Error(`Both Groq and OpenAI failed. Groq: ${errorMsg}, OpenAI: ${openaiError}`);
        }
      }
      
      // For non-retriable errors, throw immediately
      throw error;
    }
  }

  private extractRoyaltyRelevantSections(contractText: string): string {
    const royaltyKeywords = [
      'royalty', 'royalties', 'tier', 'tier 1', 'tier 2', 'tier 3', 
      'per unit', 'per-unit', 'minimum', 'guarantee', 'payment', 'payments',
      'seasonal', 'spring', 'fall', 'holiday', 'premium', 'organic',
      'container', 'multiplier', 'schedule', 'exhibit', 'calculation',
      'territory', 'primary', 'secondary', 'volume', 'threshold',
      'ornamental', 'perennials', 'shrubs', 'roses', 'hydrangea'
    ];

    const lines = contractText.split('\n');
    const relevantLines: string[] = [];
    const contextWindow = 2; // Include 2 lines before and after relevant content

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].toLowerCase();
      
      if (royaltyKeywords.some(keyword => line.includes(keyword))) {
        // Add context lines before
        for (let j = Math.max(0, i - contextWindow); j < i; j++) {
          if (!relevantLines.includes(lines[j])) {
            relevantLines.push(lines[j]);
          }
        }
        
        // Add the relevant line
        if (!relevantLines.includes(lines[i])) {
          relevantLines.push(lines[i]);
        }
        
        // Add context lines after
        for (let j = i + 1; j <= Math.min(lines.length - 1, i + contextWindow); j++) {
          if (!relevantLines.includes(lines[j])) {
            relevantLines.push(lines[j]);
          }
        }
      }
    }

    const filteredText = relevantLines.join('\n');
    
    // If filtered text is too short, use original (but truncated)
    if (filteredText.length < 1000) {
      console.log(`📝 Filtered text too short (${filteredText.length}), using truncated original`);
      return contractText.substring(0, 8000); // Truncate to reasonable size
    }
    
    console.log(`📝 Filtered text: ${filteredText.length} chars (from ${contractText.length})`);
    return filteredText;
  }

  // Get status for debugging
  getStatus() {
    return {
      groqFailureCount: this.groqFailureCount,
      groqCooldownUntil: this.groqCooldownUntil,
      isGroqInCooldown: Date.now() < this.groqCooldownUntil
    };
  }
}