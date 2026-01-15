import { db } from '../db';
import { systemEmbeddings } from '@shared/schema';
import { eq } from 'drizzle-orm';
import { HuggingFaceEmbeddingService } from './huggingFaceEmbedding';
import { systemKnowledgeBase } from '../data/systemKnowledgeBase';

/**
 * Seeds system knowledge base into embeddings table
 * This enables LIQ AI to answer questions about the platform itself
 */
export class SystemKnowledgeSeeder {
  /**
   * Seed all system documentation into embeddings
   * Skips if already seeded to avoid duplicate work
   */
  static async seedKnowledgeBase(): Promise<void> {
    try {
      console.log('🌱 Seeding System Knowledge Base for LIQ AI...');
      
      let seededCount = 0;
      let skippedCount = 0;
      
      for (const knowledge of systemKnowledgeBase) {
        // Check if already exists
        const existing = await db
          .select()
          .from(systemEmbeddings)
          .where(eq(systemEmbeddings.documentId, knowledge.id))
          .limit(1);
        
        if (existing.length > 0) {
          skippedCount++;
          continue;
        }
        
        // Generate embedding for the content
        const fullText = `${knowledge.title}\n\n${knowledge.content}`;
        const { embedding } = await HuggingFaceEmbeddingService.generateEmbedding(fullText);
        
        // Store in database
        const { sql } = await import('drizzle-orm');
        const vectorString = `[${embedding.join(',')}]`;
        await db.insert(systemEmbeddings).values({
          documentId: knowledge.id,
          category: knowledge.category,
          title: knowledge.title,
          sourceText: knowledge.content,
          embedding: sql`${vectorString}::vector`,
          metadata: {
            category: knowledge.category,
            title: knowledge.title,
          },
        });
        
        seededCount++;
      }
      
      console.log(`✅ System Knowledge Base seeding complete: ${seededCount} new, ${skippedCount} existing`);
    } catch (error) {
      console.error('❌ Error seeding system knowledge base:', error);
      // Don't throw - allow server to start even if seeding fails
    }
  }
  
  /**
   * Re-embed all system documentation (useful after content updates)
   */
  static async reseedAll(): Promise<void> {
    try {
      console.log('🔄 Re-seeding entire System Knowledge Base...');
      
      // Delete all existing system embeddings
      await db.delete(systemEmbeddings);
      
      // Seed fresh
      await this.seedKnowledgeBase();
      
      console.log('✅ System Knowledge Base re-seeded successfully');
    } catch (error) {
      console.error('❌ Error re-seeding system knowledge base:', error);
      throw error;
    }
  }
}
