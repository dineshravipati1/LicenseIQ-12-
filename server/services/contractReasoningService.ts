import OpenAI from 'openai';

// the newest OpenAI model is "gpt-5" which was released August 7, 2025. do not change this unless explicitly requested by the user
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export interface ContractValidationResult {
  isValid: boolean;
  confidence: number; // 0-1 score
  reasoning: string;
  concerns?: string[];
  recommendations?: string[];
}

export class ContractReasoningService {
  /**
   * Use LLM to validate if a matched contract is appropriate for the sales data
   * Provides reasoning and confidence scoring
   */
  static async validateContractMatch(
    salesData: {
      productCode?: string;
      productName?: string;
      category?: string;
      territory?: string;
      transactionDate?: Date;
      quantity?: number;
      grossAmount?: number;
    },
    contractData: {
      summary?: string;
      keyTerms?: any;
      productDescription?: string;
      territories?: string[];
      validFrom?: Date;
      validUntil?: Date;
    },
    similarityScore: number
  ): Promise<ContractValidationResult> {
    try {
      const prompt = this.buildValidationPrompt(salesData, contractData, similarityScore);

      const response = await openai.chat.completions.create({
        model: 'gpt-5',
        messages: [
          {
            role: 'system',
            content: `You are an expert contract analyst. Validate if a contract matches sales data by analyzing:
1. Product/service alignment
2. Territory compatibility
3. Date validity
4. Terms and conditions relevance
5. Semantic similarity score

Provide a confidence score (0-1), reasoning, and any concerns. Respond in JSON format:
{
  "isValid": boolean,
  "confidence": number,
  "reasoning": string,
  "concerns": string[],
  "recommendations": string[]
}`,
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        response_format: { type: 'json_object' },
      });

      const result = JSON.parse(response.choices[0].message.content || '{}');

      return {
        isValid: result.isValid || false,
        confidence: Math.min(1, Math.max(0, result.confidence || 0)),
        reasoning: result.reasoning || 'No reasoning provided',
        concerns: result.concerns || [],
        recommendations: result.recommendations || [],
      };
    } catch (error: any) {
      console.error('Contract validation error:', error);
      // Return low confidence result on error
      return {
        isValid: false,
        confidence: 0,
        reasoning: `Validation failed: ${error.message}`,
        concerns: ['Unable to validate contract match'],
      };
    }
  }

  /**
   * Build prompt for contract validation
   */
  private static buildValidationPrompt(
    salesData: any,
    contractData: any,
    similarityScore: number
  ): string {
    return `
Sales Transaction:
- Product: ${salesData.productName || salesData.productCode || 'N/A'}
- Category: ${salesData.category || 'N/A'}
- Territory: ${salesData.territory || 'N/A'}
- Date: ${salesData.transactionDate ? new Date(salesData.transactionDate).toISOString().split('T')[0] : 'N/A'}
- Quantity: ${salesData.quantity || 'N/A'}
- Amount: ${salesData.grossAmount ? `$${salesData.grossAmount}` : 'N/A'}

Matched Contract:
- Summary: ${contractData.summary || 'N/A'}
- Product Description: ${contractData.productDescription || 'N/A'}
- Territories: ${contractData.territories?.join(', ') || 'N/A'}
- Valid From: ${contractData.validFrom ? new Date(contractData.validFrom).toISOString().split('T')[0] : 'N/A'}
- Valid Until: ${contractData.validUntil ? new Date(contractData.validUntil).toISOString().split('T')[0] : 'N/A'}
- Key Terms: ${JSON.stringify(contractData.keyTerms || {}, null, 2)}

Semantic Similarity Score: ${(similarityScore * 100).toFixed(1)}%

Validate if this contract is appropriate for the sales transaction. Consider:
1. Does the product/service match?
2. Is the territory compatible?
3. Is the transaction date within the contract validity period?
4. Are the terms and conditions relevant?
5. Is the similarity score acceptable (>50% is good)?

Provide your analysis with confidence score and reasoning.
`.trim();
  }

  /**
   * Batch validate multiple contract matches
   * More efficient for processing large datasets
   */
  static async batchValidateMatches(
    matches: Array<{
      salesData: any;
      contractData: any;
      similarityScore: number;
    }>
  ): Promise<ContractValidationResult[]> {
    // Process in parallel batches of 5 to avoid rate limits
    const batchSize = 5;
    const results: ContractValidationResult[] = [];

    for (let i = 0; i < matches.length; i += batchSize) {
      const batch = matches.slice(i, i + batchSize);
      
      const batchResults = await Promise.all(
        batch.map(({ salesData, contractData, similarityScore }) =>
          this.validateContractMatch(salesData, contractData, similarityScore)
        )
      );

      results.push(...batchResults);
    }

    return results;
  }

  /**
   * Generate explanation for why a match might need human review
   */
  static generateReviewReason(validation: ContractValidationResult, similarityScore: number): string | null {
    const reasons: string[] = [];

    // Low confidence
    if (validation.confidence < 0.6) {
      reasons.push(`Low confidence (${(validation.confidence * 100).toFixed(0)}%)`);
    }

    // Low similarity
    if (similarityScore < 0.5) {
      reasons.push(`Low similarity (${(similarityScore * 100).toFixed(0)}%)`);
    }

    // Has concerns
    if (validation.concerns && validation.concerns.length > 0) {
      reasons.push(`Concerns: ${validation.concerns.join('; ')}`);
    }

    // Not valid
    if (!validation.isValid) {
      reasons.push('Contract not validated');
    }

    return reasons.length > 0 ? reasons.join(' | ') : null;
  }
}
