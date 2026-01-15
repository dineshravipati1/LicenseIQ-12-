import { db } from '../db';
import { 
  extractionRuns, 
  contractDocuments,
  contractGraphNodes,
  contractGraphEdges,
  ruleDefinitions,
  humanReviewTasks,
  semanticIndexEntries,
  InsertExtractionRun,
  InsertContractDocument,
  InsertHumanReviewTask
} from '@shared/schema';
import { eq } from 'drizzle-orm';
import { extractContractEntitiesZeroShot, validateExtraction } from './zeroShotExtractionService';
import { buildKnowledgeGraph } from './knowledgeGraphService';
import { synthesizeRules } from './ruleSynthesisService';
import { validateRules } from './validationService';
import { HuggingFaceEmbeddingService } from './huggingFaceEmbedding';

/**
 * Document Orchestrator Service
 * 
 * Coordinates the entire AI-powered contract extraction pipeline:
 * 1. Parse PDF into structured document segments
 * 2. Zero-shot entity extraction using LLM
 * 3. Build knowledge graph (nodes + edges)
 * 4. Synthesize dynamic royalty rules
 * 5. Validate with confidence scoring
 * 6. Queue low-confidence items for human review
 */

interface ExtractionResult {
  runId: string;
  status: 'completed' | 'pending_review' | 'failed';
  overallConfidence: number;
  nodesExtracted: number;
  edgesExtracted: number;
  rulesExtracted: number;
  reviewTasksCreated: number;
  processingTime: number;
  errors?: string[];
}

const CONFIDENCE_THRESHOLD = 0.70; // Auto-approve >= 70%, human review < 70%

/**
 * Main orchestration function - processes a contract through the full pipeline
 */
export async function processContractDynamic(
  contractId: string,
  rawText: string,
  userId: string
): Promise<ExtractionResult> {
  const startTime = Date.now();
  const errors: string[] = [];

  try {
    // Step 1: Create extraction run record
    const [run] = await db.insert(extractionRuns).values({
      contractId,
      runType: 'initial',
      status: 'processing',
      triggeredBy: userId,
      aiModel: 'llama-3.1-8b',
    }).returning();

    const runId = run.id;

    // Step 2: Parse document into structured segments
    console.log(`[DocumentOrchestrator] Parsing contract ${contractId} into segments`);
    const documentSegments = await parseDocumentIntoSegments(rawText, contractId, runId);
    console.log(`[DocumentOrchestrator] Created ${documentSegments.length} document segments`);

    // Step 3: Zero-shot entity extraction
    console.log(`[DocumentOrchestrator] Starting zero-shot entity extraction`);
    const extractionResult = await extractContractEntitiesZeroShot(rawText, contractId);
    
    // Step 4: Build knowledge graph
    console.log(`[DocumentOrchestrator] Building knowledge graph`);
    const graphResult = await buildKnowledgeGraph(
      extractionResult.entities,
      extractionResult.relationships,
      contractId,
      runId,
      documentSegments
    );

    // Step 5: Synthesize rules from extracted data
    console.log(`[DocumentOrchestrator] Synthesizing royalty rules`);
    const rulesResult = await synthesizeRules(
      extractionResult.entities,
      graphResult.nodes,
      contractId,
      runId
    );

    // Step 6: Validate extractions with cross-checks
    console.log(`[DocumentOrchestrator] Running validation checks`);
    const validationResult = await validateExtraction(
      extractionResult,
      rulesResult.rules,
      rawText
    );

    // Step 7: Calculate overall confidence
    const overallConfidence = calculateOverallConfidence(
      extractionResult.confidence,
      validationResult.confidence,
      rulesResult.averageConfidence
    );

    // Step 8: Create human review tasks for low-confidence items
    const reviewTasks = await createReviewTasks(
      contractId,
      runId,
      graphResult.lowConfidenceNodes,
      rulesResult.lowConfidenceRules,
      userId
    );

    // Step 9: Generate semantic embeddings for GraphRAG
    console.log(`[DocumentOrchestrator] Generating semantic embeddings`);
    await generateSemanticIndex(contractId, graphResult.nodes, documentSegments);

    // Step 10: Update extraction run with results
    const processingTime = Date.now() - startTime;
    const finalStatus = overallConfidence >= CONFIDENCE_THRESHOLD ? 'completed' : 'pending_review';

    await db.update(extractionRuns)
      .set({
        status: finalStatus,
        overallConfidence: overallConfidence.toFixed(2),
        nodesExtracted: graphResult.nodes.length,
        edgesExtracted: graphResult.edges.length,
        rulesExtracted: rulesResult.rules.length,
        validationResults: validationResult,
        processingTime,
        completedAt: new Date(),
      })
      .where(eq(extractionRuns.id, runId));

    console.log(`[DocumentOrchestrator] ✓ Extraction complete in ${processingTime}ms`);
    console.log(`[DocumentOrchestrator] Confidence: ${(overallConfidence * 100).toFixed(1)}%`);
    console.log(`[DocumentOrchestrator] Status: ${finalStatus}`);

    return {
      runId,
      status: finalStatus,
      overallConfidence,
      nodesExtracted: graphResult.nodes.length,
      edgesExtracted: graphResult.edges.length,
      rulesExtracted: rulesResult.rules.length,
      reviewTasksCreated: reviewTasks.length,
      processingTime,
      errors: errors.length > 0 ? errors : undefined,
    };

  } catch (error) {
    const processingTime = Date.now() - startTime;
    console.error('[DocumentOrchestrator] Extraction failed:', error);
    
    errors.push(error instanceof Error ? error.message : String(error));

    // Update run as failed if we have a runId
    const existingRun = await db.query.extractionRuns.findFirst({
      where: eq(extractionRuns.contractId, contractId),
      orderBy: (runs, { desc }) => [desc(runs.createdAt)],
    });

    if (existingRun) {
      await db.update(extractionRuns)
        .set({
          status: 'failed',
          errorLog: errors.join('\n'),
          processingTime,
          completedAt: new Date(),
        })
        .where(eq(extractionRuns.id, existingRun.id));
    }

    throw error;
  }
}

/**
 * Parse raw contract text into structured document segments
 */
async function parseDocumentIntoSegments(
  rawText: string,
  contractId: string,
  runId: string
): Promise<any[]> {
  // Split by common section markers
  const sectionPatterns = [
    /RECITALS?/i,
    /DEFINITIONS?/i,
    /PARTIES/i,
    /TERMS AND CONDITIONS/i,
    /PAYMENT/i,
    /ROYALTIES?/i,
    /TERMINATION/i,
    /CONFIDENTIALITY/i,
    /INTELLECTUAL PROPERTY/i,
    /SIGNATURES?/i,
  ];

  const segments: InsertContractDocument[] = [];
  const lines = rawText.split('\n');
  let currentSection = 'header';
  let currentText: string[] = [];
  let sectionOrder = 0;
  let pageNumber = 1;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Detect page breaks (simple heuristic)
    if (line.includes('Page ') || line.match(/^\d+$/)) {
      const match = line.match(/\d+/);
      if (match) pageNumber = parseInt(match[0]);
    }

    // Check for section headers
    let foundSection = false;
    for (const pattern of sectionPatterns) {
      if (pattern.test(line)) {
        // Save previous section
        if (currentText.length > 0) {
          segments.push({
            contractId,
            extractionRunId: runId,
            documentSection: currentSection,
            sectionOrder: sectionOrder++,
            rawText: currentText.join('\n'),
            normalizedText: currentText.join('\n').replace(/\s+/g, ' ').trim(),
            pageNumber,
            metadata: { lineStart: i - currentText.length, lineEnd: i },
          });
        }

        // Start new section
        currentSection = line.toLowerCase().replace(/[^a-z0-9]+/g, '_');
        currentText = [line];
        foundSection = true;
        break;
      }
    }

    if (!foundSection && line.length > 0) {
      currentText.push(line);
    }
  }

  // Save final section
  if (currentText.length > 0) {
    segments.push({
      contractId,
      extractionRunId: runId,
      documentSection: currentSection,
      sectionOrder: sectionOrder++,
      rawText: currentText.join('\n'),
      normalizedText: currentText.join('\n').replace(/\s+/g, ' ').trim(),
      pageNumber,
      metadata: { lineStart: lines.length - currentText.length, lineEnd: lines.length },
    });
  }

  // Insert segments into database
  if (segments.length > 0) {
    await db.insert(contractDocuments).values(segments);
  }

  return segments;
}

/**
 * Calculate weighted overall confidence from multiple sources
 */
function calculateOverallConfidence(
  extractionConfidence: number,
  validationConfidence: number,
  rulesConfidence: number
): number {
  // Weighted average: extraction 40%, validation 35%, rules 25%
  return (
    extractionConfidence * 0.40 +
    validationConfidence * 0.35 +
    rulesConfidence * 0.25
  );
}

/**
 * Create human review tasks for low-confidence extractions
 */
async function createReviewTasks(
  contractId: string,
  runId: string,
  lowConfidenceNodes: any[],
  lowConfidenceRules: any[],
  userId: string
): Promise<any[]> {
  const tasks: InsertHumanReviewTask[] = [];

  // Create tasks for low-confidence nodes
  for (const node of lowConfidenceNodes) {
    tasks.push({
      contractId,
      extractionRunId: runId,
      taskType: 'node_review',
      priority: node.confidence < 0.5 ? 'high' : 'normal',
      status: 'pending',
      targetId: node.id,
      targetType: 'graph_node',
      originalData: node,
      confidence: node.confidence.toFixed(2),
      assignedTo: userId, // Assign to the user who uploaded
    });
  }

  // Create tasks for low-confidence rules
  for (const rule of lowConfidenceRules) {
    tasks.push({
      contractId,
      extractionRunId: runId,
      taskType: 'rule_review',
      priority: rule.confidence < 0.5 ? 'high' : 'normal',
      status: 'pending',
      targetId: rule.id,
      targetType: 'rule_definition',
      originalData: rule,
      confidence: rule.confidence.toFixed(2),
      assignedTo: userId,
    });
  }

  // Insert tasks if any
  if (tasks.length > 0) {
    await db.insert(humanReviewTasks).values(tasks);
  }

  return tasks;
}

/**
 * Generate semantic embeddings for enhanced GraphRAG search
 */
async function generateSemanticIndex(
  contractId: string,
  nodes: any[],
  documentSegments: any[]
): Promise<void> {
  const indexEntries = [];

  // Index graph nodes
  for (const node of nodes) {
    const content = `${node.label}: ${JSON.stringify(node.properties)}`;
    const result = await HuggingFaceEmbeddingService.generateEmbedding(content);
    const embedding = result.embedding;
    
    indexEntries.push({
      contractId,
      indexType: 'graph_node',
      sourceId: node.id,
      content,
      embedding,
      metadata: { nodeType: node.nodeType },
    });
  }

  // Index document chunks
  for (const segment of documentSegments) {
    const result = await HuggingFaceEmbeddingService.generateEmbedding(segment.normalizedText);
    const embedding = result.embedding;
    
    indexEntries.push({
      contractId,
      indexType: 'document_chunk',
      sourceId: segment.id,
      content: segment.normalizedText,
      embedding,
      metadata: { section: segment.documentSection, page: segment.pageNumber },
    });
  }

  // Batch insert for efficiency
  if (indexEntries.length > 0) {
    await db.insert(semanticIndexEntries).values(indexEntries);
  }

  console.log(`[DocumentOrchestrator] Generated ${indexEntries.length} semantic index entries`);
}

/**
 * Get extraction run status and results
 */
export async function getExtractionStatus(runId: string) {
  const run = await db.query.extractionRuns.findFirst({
    where: eq(extractionRuns.id, runId),
  });

  if (!run) {
    throw new Error(`Extraction run ${runId} not found`);
  }

  return run;
}

/**
 * Retry failed extraction
 */
export async function retryExtraction(contractId: string, userId: string) {
  const contract = await db.query.contracts.findFirst({
    where: (contracts, { eq }) => eq(contracts.id, contractId),
    with: { analysis: true },
  });

  if (!contract?.analysis?.fullText) {
    throw new Error('Contract or full text not found - run analysis first');
  }

  return processContractDynamic(contractId, contract.analysis.fullText, userId);
}
