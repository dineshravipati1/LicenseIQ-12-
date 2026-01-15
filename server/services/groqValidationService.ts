/**
 * Groq LLaMA Validation Service
 * Uses Groq's LLaMA model for contract validation (FREE alternative to OpenAI GPT)
 */

export interface ContractValidationResult {
  isValid: boolean;
  confidence: number; // 0-1 score
  reasoning: string;
  concerns?: string[];
  recommendations?: string[];
}

export class GroqValidationService {
  /**
   * Use Groq LLaMA to validate if a matched contract is appropriate for the sales data
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
      const apiKey = process.env.GROQ_API_KEY;
      if (!apiKey) {
        throw new Error('GROQ_API_KEY is not set');
      }

      const prompt = this.buildValidationPrompt(salesData, contractData, similarityScore);

      const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'llama-3.1-8b-instant',
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
  "isValid": true/false,
  "confidence": 0.85,
  "reasoning": "explanation here",
  "concerns": ["concern 1", "concern 2"],
  "recommendations": ["recommendation 1"]
}`,
            },
            {
              role: 'user',
              content: prompt,
            },
          ],
          response_format: { type: 'json_object' },
          temperature: 0.3,
        }),
      });

      if (!response.ok) {
        const error = await response.text();
        throw new Error(`Groq API error: ${response.status} - ${error}`);
      }

      const data = await response.json();
      const result = JSON.parse(data.choices[0].message.content || '{}');

      return {
        isValid: result.isValid || false,
        confidence: Math.min(1, Math.max(0, result.confidence || 0)),
        reasoning: result.reasoning || 'No reasoning provided',
        concerns: result.concerns || [],
        recommendations: result.recommendations || [],
      };
    } catch (error: any) {
      console.error('Groq validation error:', error);
      // Return low confidence result on error
      return {
        isValid: false,
        confidence: 0,
        reasoning: `Validation failed: ${error.message}`,
        concerns: ['Automatic validation failed'],
        recommendations: ['Manual review required'],
      };
    }
  }

  /**
   * Build validation prompt
   */
  private static buildValidationPrompt(
    salesData: any,
    contractData: any,
    similarityScore: number
  ): string {
    const parts: string[] = [];

    parts.push(`Semantic Similarity Score: ${(similarityScore * 100).toFixed(1)}%`);
    parts.push('\n--- SALES DATA ---');
    if (salesData.productName) parts.push(`Product: ${salesData.productName}`);
    if (salesData.productCode) parts.push(`Code: ${salesData.productCode}`);
    if (salesData.category) parts.push(`Category: ${salesData.category}`);
    if (salesData.territory) parts.push(`Territory: ${salesData.territory}`);
    if (salesData.transactionDate) parts.push(`Date: ${salesData.transactionDate}`);
    if (salesData.quantity) parts.push(`Quantity: ${salesData.quantity}`);
    if (salesData.grossAmount) parts.push(`Amount: ${salesData.grossAmount}`);

    parts.push('\n--- CONTRACT DATA ---');
    if (contractData.summary) parts.push(`Summary: ${contractData.summary}`);
    if (contractData.productDescription) parts.push(`Products: ${contractData.productDescription}`);
    if (contractData.territories?.length) parts.push(`Territories: ${contractData.territories.join(', ')}`);
    if (contractData.validFrom) parts.push(`Valid From: ${contractData.validFrom}`);
    if (contractData.validUntil) parts.push(`Valid Until: ${contractData.validUntil}`);
    if (contractData.keyTerms) {
      parts.push(`Key Terms: ${JSON.stringify(contractData.keyTerms, null, 2)}`);
    }

    parts.push('\n--- VALIDATION REQUEST ---');
    parts.push('Analyze if this contract is appropriate for the sales transaction.');
    parts.push('Consider product alignment, territory match, date validity, and similarity score.');
    parts.push('Provide confidence (0-1), reasoning, concerns, and recommendations in JSON format.');

    return parts.join('\n');
  }

  /**
   * Batch validate multiple matches
   */
  static async validateBatchMatches(
    matches: Array<{
      salesData: any;
      contractData: any;
      similarityScore: number;
    }>
  ): Promise<ContractValidationResult[]> {
    const results: ContractValidationResult[] = [];
    
    for (const match of matches) {
      const result = await this.validateContractMatch(
        match.salesData,
        match.contractData,
        match.similarityScore
      );
      results.push(result);
      // Small delay to respect rate limits
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    return results;
  }
}
