import { db } from '../db';
import { systemEmbeddings } from '@shared/schema';
import { sql } from 'drizzle-orm';
import { HuggingFaceEmbeddingService } from './huggingFaceEmbedding';

export interface SystemMatch {
  documentId: string;
  category: string;
  title: string;
  sourceText: string;
  similarity: number;
  metadata: any;
}

export interface SystemSearchOptions {
  minSimilarity?: number;
  limit?: number;
  category?: string;
}

/**
 * Search service for system documentation embeddings
 * Enables LIQ AI to answer questions about the LicenseIQ platform
 */
export class SystemSearchService {
  /**
   * Find system documentation that matches the query
   */
  static async findMatchingDocumentation(
    queryText: string,
    options: SystemSearchOptions = {}
  ): Promise<SystemMatch[]> {
    const {
      minSimilarity = 0.5,
      limit = 10,
      category,
    } = options;

    try {
      // Generate embedding for the query
      const { embedding: queryEmbedding } = await HuggingFaceEmbeddingService.generateEmbedding(queryText);

      // Convert array to PostgreSQL vector format
      const vectorString = `[${queryEmbedding.join(',')}]`;
      
      // Calculate similarity
      const distance = sql<number>`${systemEmbeddings.embedding} <=> ${vectorString}::vector`;
      const similarity = sql<number>`1 - (${systemEmbeddings.embedding} <=> ${vectorString}::vector)`;
      const maxDistance = 1 - minSimilarity;

      // Build query
      let query = db
        .select({
          documentId: systemEmbeddings.documentId,
          category: systemEmbeddings.category,
          title: systemEmbeddings.title,
          sourceText: systemEmbeddings.sourceText,
          metadata: systemEmbeddings.metadata,
          distance,
          similarity,
        })
        .from(systemEmbeddings)
        .where(
          category 
            ? sql`${distance} < ${maxDistance} AND ${systemEmbeddings.category} = ${category}`
            : sql`${distance} < ${maxDistance}`
        )
        .orderBy(sql`${distance} ASC`)
        .limit(limit);

      const results = await query;

      return results.map((r: any) => ({
        documentId: r.documentId,
        category: r.category || '',
        title: r.title || '',
        sourceText: r.sourceText || '',
        similarity: Number(r.similarity),
        metadata: r.metadata as any,
      }));
    } catch (error: any) {
      console.error('System documentation search error:', error);
      // Return empty array instead of throwing to allow graceful degradation
      return [];
    }
  }
}
