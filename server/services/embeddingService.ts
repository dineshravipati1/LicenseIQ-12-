import OpenAI from 'openai';

// the newest OpenAI model is "gpt-5" which was released August 7, 2025. do not change this unless explicitly requested by the user
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export interface EmbeddingResult {
  embedding: number[];
  tokens: number;
}

export class EmbeddingService {
  /**
   * Generate embedding for a single text using OpenAI's text-embedding-3-small model
   * This model produces 1536-dimensional vectors
   */
  static async generateEmbedding(text: string): Promise<EmbeddingResult> {
    try {
      const response = await openai.embeddings.create({
        model: 'text-embedding-3-small',
        input: text,
        encoding_format: 'float',
      });

      return {
        embedding: response.data[0].embedding,
        tokens: response.usage.total_tokens,
      };
    } catch (error: any) {
      console.error('Embedding generation error:', error);
      throw new Error(`Failed to generate embedding: ${error.message}`);
    }
  }

  /**
   * Generate embeddings for multiple texts in batch
   * More efficient than calling generateEmbedding multiple times
   */
  static async generateBatchEmbeddings(texts: string[]): Promise<EmbeddingResult[]> {
    try {
      const response = await openai.embeddings.create({
        model: 'text-embedding-3-small',
        input: texts,
        encoding_format: 'float',
      });

      return response.data.map((item, index) => ({
        embedding: item.embedding,
        tokens: Math.round(response.usage.total_tokens / texts.length), // Approximate tokens per text
      }));
    } catch (error: any) {
      console.error('Batch embedding generation error:', error);
      throw new Error(`Failed to generate batch embeddings: ${error.message}`);
    }
  }

  /**
   * Prepare text for embedding by cleaning and normalizing
   */
  static prepareText(text: string): string {
    return text
      .trim()
      .replace(/\s+/g, ' ') // Normalize whitespace
      .replace(/[^\w\s.,;:!?-]/g, '') // Remove special characters but keep punctuation
      .slice(0, 8000); // OpenAI embedding models have ~8k token limit
  }

  /**
   * Create searchable text from contract data
   * Combines product info, territories, and other metadata for better matching
   */
  static createContractSearchText(data: {
    productDescription?: string;
    productCategories?: string[];
    territories?: string[];
    contractType?: string;
    additionalTerms?: string[];
  }): string {
    const parts: string[] = [];

    if (data.productDescription) {
      parts.push(data.productDescription);
    }

    if (data.productCategories && data.productCategories.length > 0) {
      parts.push(`Products: ${data.productCategories.join(', ')}`);
    }

    if (data.territories && data.territories.length > 0) {
      parts.push(`Territories: ${data.territories.join(', ')}`);
    }

    if (data.contractType) {
      parts.push(`Type: ${data.contractType}`);
    }

    if (data.additionalTerms && data.additionalTerms.length > 0) {
      parts.push(data.additionalTerms.join('. '));
    }

    return this.prepareText(parts.join('. '));
  }

  /**
   * Create searchable text from sales data
   * Combines product, category, territory for semantic matching
   */
  static createSalesSearchText(data: {
    productCode?: string;
    productName?: string;
    category?: string;
    territory?: string;
  }): string {
    const parts: string[] = [];

    if (data.productName) {
      parts.push(data.productName);
    } else if (data.productCode) {
      parts.push(data.productCode);
    }

    if (data.category) {
      parts.push(`Category: ${data.category}`);
    }

    if (data.territory) {
      parts.push(`Territory: ${data.territory}`);
    }

    return this.prepareText(parts.join('. '));
  }
}
