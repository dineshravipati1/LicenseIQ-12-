import { db } from '../db';
import { contractEmbeddings, contracts } from '@shared/schema';
import { cosineDistance, desc, sql, eq, and } from 'drizzle-orm';
import { HuggingFaceEmbeddingService } from './huggingFaceEmbedding';

export interface ContractMatch {
  contractId: string;
  similarity: number;
  embeddingType: string;
  sourceText: string;
  metadata: any;
  contractDetails?: any;
}

export interface SemanticSearchOptions {
  minSimilarity?: number; // Default 0.5 (50% similarity)
  limit?: number; // Default 10
  embeddingTypes?: string[]; // Filter by embedding type
  dateRange?: { start: Date; end: Date }; // Filter by contract validity
}

export class SemanticSearchService {
  /**
   * Find contracts that match the given query text using semantic search
   */
  static async findMatchingContracts(
    queryText: string,
    options: SemanticSearchOptions = {}
  ): Promise<ContractMatch[]> {
    const {
      minSimilarity = 0.5,
      limit = 10,
      embeddingTypes,
    } = options;

    try {
      // Generate embedding for the query using Hugging Face (FREE)
      const { embedding: queryEmbedding } = await HuggingFaceEmbeddingService.generateEmbedding(queryText);

      // Convert array to PostgreSQL vector format: '[1,2,3,...]'::vector
      const vectorString = `[${queryEmbedding.join(',')}]`;
      
      // Use <=> operator for HNSW index optimization
      // Distance metric (lower is better), convert to similarity (higher is better)
      const distance = sql<number>`${contractEmbeddings.embedding} <=> ${vectorString}::vector`;
      const similarity = sql<number>`1 - (${contractEmbeddings.embedding} <=> ${vectorString}::vector)`;

      // Convert minSimilarity to maxDistance for filtering
      const maxDistance = 1 - minSimilarity;

      // Build query with optional filters
      let query = db
        .select({
          id: contractEmbeddings.id,
          contractId: contractEmbeddings.contractId,
          embeddingType: contractEmbeddings.embeddingType,
          sourceText: contractEmbeddings.sourceText,
          metadata: contractEmbeddings.metadata,
          distance,
          similarity,
          contractStatus: contracts.status,
        })
        .from(contractEmbeddings)
        .innerJoin(contracts, eq(contractEmbeddings.contractId, contracts.id))
        .where(
          and(
            sql`${distance} < ${maxDistance}`, // Filter by distance for index usage
            eq(contracts.status, 'analyzed'), // Only search analyzed contracts
            embeddingTypes && embeddingTypes.length > 0
              ? sql`${contractEmbeddings.embeddingType} = ANY(${embeddingTypes})`
              : undefined
          )
        )
        .orderBy(sql`${distance} ASC`) // Order by distance (ASC) to use HNSW index
        .limit(limit);

      const results = await query;

      return results.map((r: any) => ({
        contractId: r.contractId,
        similarity: Number(r.similarity),
        embeddingType: r.embeddingType || '',
        sourceText: r.sourceText || '',
        metadata: r.metadata as any,
      }));
    } catch (error: any) {
      console.error('Semantic search error:', error);
      throw new Error(`Semantic search failed: ${error.message}`);
    }
  }

  /**
   * Find best matching contract for sales data
   * Returns top match with reasoning
   */
  static async findBestContractForSales(salesData: {
    productCode?: string;
    productName?: string;
    category?: string;
    territory?: string;
    transactionDate?: Date;
  }): Promise<{
    contractId: string;
    confidence: number;
    reasoning: string;
    matches: ContractMatch[];
  } | null> {
    try {
      // Create search text from sales data
      const searchText = HuggingFaceEmbeddingService.createSalesSearchText(salesData);

      // Find candidate contracts
      const matches = await this.findMatchingContracts(searchText, {
        minSimilarity: 0.3, // Lower threshold to get more candidates
        limit: 5,
      });

      if (matches.length === 0) {
        return null;
      }

      // Get the best match (highest similarity)
      const bestMatch = matches[0];

      // Generate reasoning
      const reasoning = this.generateMatchReasoning(salesData, bestMatch, matches);

      return {
        contractId: bestMatch.contractId,
        confidence: bestMatch.similarity,
        reasoning,
        matches,
      };
    } catch (error: any) {
      console.error('Contract matching error:', error);
      return null;
    }
  }

  /**
   * Generate human-readable reasoning for why a contract was matched
   */
  private static generateMatchReasoning(
    salesData: any,
    bestMatch: ContractMatch,
    allMatches: ContractMatch[]
  ): string {
    const reasons: string[] = [];

    // Similarity score
    const confidencePercent = (bestMatch.similarity * 100).toFixed(1);
    reasons.push(`${confidencePercent}% semantic similarity`);

    // Product/category match
    if (salesData.productName || salesData.category) {
      reasons.push(`product match: "${salesData.productName || salesData.category}"`);
    }

    // Territory match
    if (salesData.territory && bestMatch.metadata?.territories) {
      const territories = bestMatch.metadata.territories;
      if (territories.includes(salesData.territory)) {
        reasons.push(`exact territory match: ${salesData.territory}`);
      }
    }

    // Alternative matches
    if (allMatches.length > 1) {
      reasons.push(`${allMatches.length - 1} alternative contract(s) found`);
    }

    return reasons.join('; ');
  }

  /**
   * Batch process sales data to find matching contracts
   * More efficient for large datasets
   */
  static async batchFindContracts(
    salesDataArray: Array<{
      id: string;
      productCode?: string;
      productName?: string;
      category?: string;
      territory?: string;
    }>
  ): Promise<Map<string, { contractId: string; confidence: number; reasoning: string }>> {
    const results = new Map();

    // Process in parallel batches of 10
    const batchSize = 10;
    for (let i = 0; i < salesDataArray.length; i += batchSize) {
      const batch = salesDataArray.slice(i, i + batchSize);
      
      const batchPromises = batch.map(async (salesData) => {
        const match = await this.findBestContractForSales(salesData);
        if (match) {
          results.set(salesData.id, {
            contractId: match.contractId,
            confidence: match.confidence,
            reasoning: match.reasoning,
          });
        }
      });

      await Promise.all(batchPromises);
    }

    return results;
  }
}
