import { db } from '../db';
import { contractGraphNodes, contractGraphEdges } from '@shared/schema';
import { ExtractedEntity, ExtractedRelationship } from './zeroShotExtractionService';
import { HuggingFaceEmbeddingService } from './huggingFaceEmbedding';
import { sql } from 'drizzle-orm';

/**
 * Knowledge Graph Service
 * 
 * Builds structured knowledge graphs from extracted entities and relationships.
 * Creates nodes for entities and edges for relationships with semantic embeddings.
 */

export interface GraphBuildResult {
  nodes: any[];
  edges: any[];
  lowConfidenceNodes: any[];
  statistics: {
    totalNodes: number;
    totalEdges: number;
    nodeTypeDistribution: Record<string, number>;
    averageConfidence: number;
  };
}

const CONFIDENCE_THRESHOLD = 0.70;

/**
 * Build knowledge graph from extracted entities and relationships
 */
export async function buildKnowledgeGraph(
  entities: ExtractedEntity[],
  relationships: ExtractedRelationship[],
  contractId: string,
  runId: string,
  documentSegments: any[]
): Promise<GraphBuildResult> {
  console.log(`[KnowledgeGraph] Building graph with ${entities.length} entities and ${relationships.length} relationships`);

  const nodes: any[] = [];
  const lowConfidenceNodes: any[] = [];
  const nodeMap = new Map<string, string>(); // label -> node ID mapping

  // Step 1: Create nodes for each entity
  for (const entity of entities) {
    try {
      // Generate semantic embedding for the entity
      const embeddingText = `${entity.type}: ${entity.label} - ${JSON.stringify(entity.properties)}`;
      const result = await HuggingFaceEmbeddingService.generateEmbedding(embeddingText);
      const embedding = result.embedding;

      // Find source document
      const sourceDoc = documentSegments.find(seg => 
        seg.rawText.includes(entity.sourceText.substring(0, 100))
      );

      const [node] = await db.insert(contractGraphNodes).values({
        contractId,
        extractionRunId: runId,
        nodeType: entity.type,
        label: entity.label,
        properties: entity.properties,
        confidence: entity.confidence.toFixed(2),
        sourceDocumentId: sourceDoc?.id,
        sourceText: entity.sourceText,
        embedding,
      }).returning();

      nodes.push(node);
      nodeMap.set(entity.label, node.id);

      // Track low confidence nodes
      if (entity.confidence < CONFIDENCE_THRESHOLD) {
        lowConfidenceNodes.push(node);
      }

    } catch (error) {
      console.error(`[KnowledgeGraph] Failed to create node for ${entity.label}:`, error);
    }
  }

  // Step 2: Create edges for relationships
  const edges: any[] = [];

  for (const rel of relationships) {
    try {
      const sourceId = nodeMap.get(rel.sourceLabel);
      const targetId = nodeMap.get(rel.targetLabel);

      if (!sourceId || !targetId) {
        console.warn(`[KnowledgeGraph] Skipping relationship - nodes not found: ${rel.sourceLabel} -> ${rel.targetLabel}`);
        continue;
      }

      const [edge] = await db.insert(contractGraphEdges).values({
        contractId,
        extractionRunId: runId,
        sourceNodeId: sourceId,
        targetNodeId: targetId,
        relationshipType: rel.relationshipType,
        properties: rel.properties || {},
        confidence: rel.confidence.toFixed(2),
      }).returning();

      edges.push(edge);

    } catch (error) {
      console.error(`[KnowledgeGraph] Failed to create edge ${rel.sourceLabel} -> ${rel.targetLabel}:`, error);
    }
  }

  // Step 3: Calculate statistics
  const nodeTypeDistribution: Record<string, number> = {};
  let totalConfidence = 0;

  for (const node of nodes) {
    nodeTypeDistribution[node.nodeType] = (nodeTypeDistribution[node.nodeType] || 0) + 1;
    totalConfidence += parseFloat(node.confidence);
  }

  const averageConfidence = nodes.length > 0 ? totalConfidence / nodes.length : 0;

  console.log(`[KnowledgeGraph] ✓ Created ${nodes.length} nodes and ${edges.length} edges`);
  console.log(`[KnowledgeGraph] Average confidence: ${(averageConfidence * 100).toFixed(1)}%`);
  console.log(`[KnowledgeGraph] Low confidence nodes: ${lowConfidenceNodes.length}`);

  return {
    nodes,
    edges,
    lowConfidenceNodes,
    statistics: {
      totalNodes: nodes.length,
      totalEdges: edges.length,
      nodeTypeDistribution,
      averageConfidence,
    },
  };
}

/**
 * Query knowledge graph for specific entity types
 */
export async function queryGraphByType(contractId: string, nodeType: string) {
  return db.query.contractGraphNodes.findMany({
    where: (nodes, { eq, and }) => and(
      eq(nodes.contractId, contractId),
      eq(nodes.nodeType, nodeType)
    ),
  });
}

/**
 * Get all relationships for a specific node
 */
export async function getNodeRelationships(nodeId: string) {
  const outgoing = await db.query.contractGraphEdges.findMany({
    where: (edges, { eq }) => eq(edges.sourceNodeId, nodeId),
    with: {
      targetNode: true,
    },
  });

  const incoming = await db.query.contractGraphEdges.findMany({
    where: (edges, { eq }) => eq(edges.targetNodeId, nodeId),
    with: {
      sourceNode: true,
    },
  });

  return { outgoing, incoming };
}

/**
 * Find semantically similar nodes using vector search
 */
export async function findSimilarNodes(
  contractId: string,
  queryEmbedding: number[],
  limit: number = 5
) {
  // PostgreSQL vector similarity search
  const result = await db.execute(sql`
    SELECT 
      id,
      node_type,
      label,
      properties,
      confidence,
      embedding <=> ${`[${queryEmbedding.join(',')}]`} AS distance
    FROM contract_graph_nodes
    WHERE contract_id = ${contractId}
    ORDER BY embedding <=> ${`[${queryEmbedding.join(',')}]`}
    LIMIT ${limit}
  `);

  return result.rows;
}
