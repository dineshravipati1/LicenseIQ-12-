import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import multer from "multer";
import fs from "fs";
import path from "path";
import crypto, { randomUUID } from "crypto";
import { storage } from "./storage";
import { setupAuth, isAuthenticated, hashPassword } from "./auth";
import { fileService } from "./services/fileService";
import { groqService } from "./services/groqService";
import { registerRulesRoutes } from "./rulesRoutes";
import { SalesDataParser } from "./services/salesDataParser";
import { PDFInvoiceService } from "./services/pdfInvoiceService";
import { HuggingFaceEmbeddingService } from "./services/huggingFaceEmbedding";
import { RAGService } from "./services/ragService";
import { extractWithErpMapping } from "./services/zeroShotExtractionService";
import { db, pool } from "./db";
import { applyFilters, validateFilterConfig, extractFieldsFromData, type FilterConfig } from "./filter-engine";
import { contracts, contractEmbeddings, royaltyRules, navigationPermissions, roleNavigationPermissions, userNavigationOverrides, navigationCategories, navigationItemCategories, userCategoryPreferences, userCategoryState, roles, insertRoleSchema, InsertRoyaltyRule, licenseiqEntities, erpMappingRuleSets, erpMappingRules, orgCalculationSettings, calculationBlueprints, blueprintDimensions } from "@shared/schema";
import { 
  insertContractSchema, 
  insertContractAnalysisSchema, 
  insertAuditTrailSchema,
  insertSalesDataSchema,
  insertLicenseiqEntityRecordSchema
} from "@shared/schema";
import { and, eq, count, desc, sql, inArray } from "drizzle-orm";

// Configure multer for secure file uploads with disk storage
const uploadDir = path.join(process.cwd(), 'uploads');
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
      // Generate secure filename with UUID
      const fileExtension = path.extname(file.originalname);
      const fileName = `${randomUUID()}${fileExtension}`;
      cb(null, fileName);
    },
  }),
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/pdf',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'text/plain'
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only PDF, DOC, DOCX, and TXT files are allowed.'));
    }
  },
});

// Configure multer for CSV/Excel uploads
const dataUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
      const fileExtension = path.extname(file.originalname);
      const fileName = `${randomUUID()}${fileExtension}`;
      cb(null, fileName);
    },
  }),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit for data files
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ];
    const allowedExts = ['.csv', '.xlsx', '.xls'];
    const fileExt = path.extname(file.originalname).toLowerCase();
    
    if (allowedTypes.includes(file.mimetype) || allowedExts.includes(fileExt)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only CSV and Excel files are allowed.'));
    }
  },
});

// Audit logging middleware
async function createAuditLog(req: any, action: string, resourceType?: string, resourceId?: string, details?: any) {
  if (req.user?.id) {
    try {
      await storage.createAuditLog({
        userId: req.user.id,
        action,
        resourceType,
        resourceId,
        details,
        ipAddress: req.ip,
        userAgent: req.get('User-Agent') || '',
      });
    } catch (error) {
      console.error('Failed to create audit log:', error);
    }
  }
}

// Helper function to check if user is system admin
function isSystemAdmin(user: any): boolean {
  return user?.isSystemAdmin === true;
}

// Helper function to get user's company ID from active context
function getUserCompanyId(user: any): string | null {
  return user?.activeContext?.companyId || null;
}

// Helper function to check if user is admin/owner (for their context)
function isContextAdmin(user: any): boolean {
  const contextRole = user?.activeContext?.role;
  return contextRole === 'admin' || contextRole === 'owner';
}

export async function registerRoutes(app: Express): Promise<Server> {
  // Auth middleware
  setupAuth(app);

  // Health check endpoint
  app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // Landing page chatbot endpoint (no authentication required)
  app.post('/api/landing-chat', async (req, res) => {
    try {
      const { message, conversationHistory } = req.body;
      
      if (!message || typeof message !== 'string') {
        return res.status(400).json({ error: 'Message is required' });
      }

      const apiKey = process.env.GROQ_API_KEY;
      if (!apiKey) {
        return res.status(500).json({ error: 'AI service not configured' });
      }

      // LicenseIQ platform knowledge base for context
      const licenseIQKnowledge = `
## About LicenseIQ Research Platform

LicenseIQ is an AI-powered contract management and analysis SaaS platform designed for organizations with complex licensing agreements. Here's what you need to know:

### Core Features:
1. **AI Contract Analysis** - Upload contracts (PDF, Word, etc.) and get automated extraction of key terms, dates, parties, and obligations using advanced AI (powered by Groq's LLaMA 3.3 70B model).

2. **License Fee Calculations** - Automatically calculate royalties and license fees based on extracted contract rules. Supports percentage-based, tiered, minimum guarantees, caps, and complex formulas.

3. **liQ AI Assistant** - An intelligent Q&A system that lets you ask questions about your contracts. Get instant answers with source citations from your uploaded documents.

4. **ERP Integration Hub** - Connect with major ERP systems (Oracle, SAP, Salesforce, QuickBooks, etc.) to import and map data. AI-powered field mapping suggestions make integration seamless.

5. **Dynamic Rule Engine** - Create and manage payment rules with a visual formula builder. Supports complex business logic for revenue calculations.

6. **Risk Assessment** - Automatic identification of high, medium, and low-risk clauses in contracts with AI-powered analysis.

7. **Multi-Tenant Architecture** - Supports Companies → Business Units → Locations hierarchy for enterprise-level organization management.

8. **Audit Logging** - Complete audit trail of all actions for compliance and governance.

### Pricing Plans:
- **Basic**: $99/month - Essential contract analysis and storage
- **Plus**: $299/month - Advanced AI features, ERP integration, team collaboration
- **Ultra**: $499/month - Enterprise features, unlimited contracts, priority support, custom integrations

### Industries Served:
- Manufacturing & Distribution
- Entertainment & Media (Royalty Management)
- Software & Technology (License Compliance)
- Healthcare & Pharmaceuticals
- Retail & Consumer Goods

### Security:
- Role-Based Access Control (RBAC) with 5 permission levels
- Encrypted data storage
- Secure session management
- SOC 2 compliance ready

### Getting Started:
1. Sign up for early access or request a demo
2. Upload your first contract
3. Let AI analyze and extract key information
4. Set up license fee rules
5. Connect your ERP for automated data flow

### Contact:
- Email: info@licenseiq.ai
- Website: licenseiq.ai
`;

      // Build conversation messages for context
      const messages: Array<{ role: string; content: string }> = [
        {
          role: 'system',
          content: `You are liQ, the friendly AI assistant for LicenseIQ Research Platform. You help visitors learn about the platform and answer both general questions and LicenseIQ-specific questions.

Your personality:
- Friendly, helpful, and professional
- Knowledgeable about AI, contract management, and business software
- Can answer general questions like ChatGPT (coding, writing, research, etc.)
- Expert on LicenseIQ platform features, pricing, and capabilities

When answering questions about LicenseIQ, use this knowledge base:
${licenseIQKnowledge}

Guidelines:
1. For LicenseIQ questions: Provide accurate information from the knowledge base above
2. For general questions: Answer helpfully as a general AI assistant
3. If unsure about LicenseIQ specifics, encourage them to request a demo
4. Keep responses concise but informative (2-4 paragraphs max for complex topics)
5. Use bullet points or numbered lists when helpful
6. Be conversational and engaging
7. End responses with a helpful follow-up question or suggestion when appropriate`
        }
      ];

      // Add conversation history if provided
      if (conversationHistory && Array.isArray(conversationHistory)) {
        for (const msg of conversationHistory.slice(-10)) { // Keep last 10 messages for context
          if (msg.role === 'user' || msg.role === 'assistant') {
            messages.push({ role: msg.role, content: msg.content });
          }
        }
      }

      // Add current user message
      messages.push({ role: 'user', content: message });

      // Call Groq API
      const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'llama-3.3-70b-versatile',
          messages,
          temperature: 0.7,
          max_tokens: 1024,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Groq API error:', errorText);
        return res.status(500).json({ error: 'AI service temporarily unavailable' });
      }

      const data = await response.json() as { choices: Array<{ message: { content: string } }> };
      const aiResponse = data.choices?.[0]?.message?.content || 'I apologize, but I could not generate a response. Please try again.';

      res.json({ 
        response: aiResponse,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('Landing chat error:', error);
      res.status(500).json({ error: 'Failed to process chat message' });
    }
  });

  // Emergency fix: Reprocess specific contract
  app.post('/api/emergency-reprocess', async (req, res) => {
    try {
      const contractId = 'af86280e-143e-4865-8f02-f762954fa05a';
      const filePath = '/home/runner/workspace/uploads/a9384579-61ff-482f-8da0-91db501030dc.pdf';
      
      console.log(`🚨 Emergency reprocessing contract ${contractId}`);
      
      // Update status to processing
      await storage.updateContractStatus(contractId, 'processing');
      
      res.json({ message: 'Emergency reprocessing started', contractId });
      
      // Trigger analysis with fixed code
      processContractAnalysis(contractId, filePath);
      
    } catch (error) {
      console.error('Emergency reprocess error:', error);
      res.status(500).json({ error: 'Failed to emergency reprocess' });
    }
  });

  // Vendor routes
//   app.get('/api/vendors', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const search = req.query.search as string;
//       const vendors = await storage.getVendors(search);
//       
//       // Fetch contracts for each vendor
//       const vendorsWithContracts = await Promise.all(
//         vendors.map(async (vendor) => {
//           const contracts = await storage.getContractsByVendor(vendor.id);
//           return {
//             ...vendor,
//             licenses: contracts.map(contract => ({
//               id: contract.id,
//               name: contract.originalName,
//               status: contract.status
//             }))
//           };
//         })
//       );
//       
//       res.json({ vendors: vendorsWithContracts });
//     } catch (error: any) {
//       console.error('Get vendors error:', error);
//       res.status(500).json({ error: error.message || 'Failed to fetch vendors' });
//     }
//   });

//   app.post('/api/vendors', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const vendor = await storage.createVendor(req.body);
//       
//       await createAuditLog(req, 'vendor_create', 'vendor', vendor.id, {
//         name: vendor.name
//       });
//       
//       res.json(vendor);
//     } catch (error: any) {
//       console.error('Create vendor error:', error);
//       res.status(500).json({ error: error.message || 'Failed to create vendor' });
//     }
//   });

  // Contract upload endpoint
  app.post('/api/contracts/upload', isAuthenticated, upload.single('file'), async (req: any, res: Response) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      // Get organizational context from user's active context
      const activeContext = req.user?.activeContext;
      const companyId = activeContext?.companyId || null;
      const businessUnitId = activeContext?.businessUnitId || null;
      const locationId = activeContext?.locationId || null;

      // Create contract record with organizational context
      const contractData = insertContractSchema.parse({
        vendorId: req.body.vendorId || null, // Optional vendor link for royalty contracts
        originalName: req.file.originalname,
        fileName: req.file.filename,
        fileSize: req.file.size,
        filePath: req.file.path,
        fileType: req.file.mimetype,
        uploadedBy: req.user.id,
        status: 'processing',
        // Set organizational context from user's active context
        companyId,
        businessUnitId,
        locationId,
      });

      const contract = await storage.createContract(contractData);

      // Log the upload
      await createAuditLog(req, 'contract_upload', 'contract', contract.id, {
        originalName: req.file.originalname,
        fileSize: req.file.size
      });

      res.json({ 
        id: contract.id,  // Frontend expects 'id' not 'contractId'
        contractId: contract.id,  // Keep both for backward compatibility
        status: 'uploaded',
        message: 'Contract uploaded successfully' 
      });

      // Process contract analysis in background
      processContractAnalysis(contract.id, req.file.path);

    } catch (error) {
      console.error('Upload error:', error);
      res.status(500).json({ error: 'Failed to upload contract' });
    }
  });

  // Dynamic extraction endpoints
  app.post('/api/contracts/:id/extract-dynamic', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user.id;

      // Fetch contract to get file path
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      // Extract text from file
      const mimeType = contract.fileType || 'application/pdf';
      const rawText = await fileService.extractTextFromFile(contract.filePath, mimeType);
      
      if (!rawText || rawText.trim().length === 0) {
        return res.status(400).json({ error: 'Failed to extract text from contract file' });
      }

      // Import orchestrator service dynamically
      const { processContractDynamic } = await import('./services/documentOrchestratorService');
      
      // Start extraction in background
      processContractDynamic(contractId, rawText, userId)
        .then(() => console.log(`✅ Dynamic extraction completed for contract ${contractId}`))
        .catch(err => console.error(`❌ Dynamic extraction failed for contract ${contractId}:`, err));

      await createAuditLog(req, 'dynamic_extraction_triggered', 'contract', contractId);

      res.json({ 
        message: 'Dynamic extraction started',
        contractId 
      });
    } catch (error: any) {
      console.error('Dynamic extraction trigger error:', error);
      res.status(500).json({ error: error.message || 'Failed to start dynamic extraction' });
    }
  });

  app.get('/api/extraction-runs/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const run = await storage.getExtractionRun(req.params.id);
      if (!run) {
        return res.status(404).json({ error: 'Extraction run not found' });
      }
      res.json(run);
    } catch (error: any) {
      console.error('Get extraction run error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch extraction run' });
    }
  });

  app.get('/api/contracts/:id/extraction-runs', isAuthenticated, async (req: any, res: Response) => {
    try {
      const runs = await storage.getExtractionRunsByContract(req.params.id);
      res.json(runs);
    } catch (error: any) {
      console.error('Get extraction runs error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch extraction runs' });
    }
  });

  app.get('/api/contracts/:id/knowledge-graph', isAuthenticated, async (req: any, res: Response) => {
    try {
      const graph = await storage.getContractKnowledgeGraph(req.params.id);
      res.json(graph);
    } catch (error: any) {
      console.error('Get knowledge graph error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch knowledge graph' });
    }
  });

  app.get('/api/contracts/:id/dynamic-rules', isAuthenticated, async (req: any, res: Response) => {
    try {
      const rules = await storage.getDynamicRulesByContract(req.params.id);
      res.json(rules);
    } catch (error: any) {
      console.error('Get dynamic rules error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch dynamic rules' });
    }
  });

  // Get combined rules (manual + ERP-generated) for a contract
  app.get('/api/contracts/:id/combined-rules', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const context = req.user?.activeContext;
      
      // Get contract to find companyId
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }
      
      // Multi-tenant authorization check
      const userRole = (await storage.getUser(req.user?.id))?.role;
      const isAdmin = userRole === 'admin' || userRole === 'owner';
      if (!isAdmin && context?.companyId && contract.companyId !== context.companyId) {
        return res.status(403).json({ error: 'Access denied: Contract belongs to different organization' });
      }
      
      const companyId = contract.companyId;
      
      // Get manual royalty rules
      const manualRules = await db
        .select()
        .from(royaltyRules)
        .where(eq(royaltyRules.contractId, contractId));
      
      // Get org calculation settings
      let calculationApproach = 'manual';
      if (companyId) {
        const settings = await db
          .select()
          .from(orgCalculationSettings)
          .where(eq(orgCalculationSettings.companyId, companyId))
          .limit(1);
        if (settings.length > 0) {
          calculationApproach = settings[0].calculationApproach;
        }
      }
      
      // Get ERP-generated rules if enabled
      let erpGeneratedRules: any[] = [];
      if (companyId && (calculationApproach === 'erp_rules' || calculationApproach === 'erp_mapping_rules' || calculationApproach === 'hybrid')) {
        const ruleSets = await db
          .select()
          .from(erpMappingRuleSets)
          .where(and(
            eq(erpMappingRuleSets.companyId, companyId),
            eq(erpMappingRuleSets.status, 'active')
          ));
        
        if (ruleSets.length > 0) {
          const ruleSetIds = ruleSets.map(rs => rs.id);
          const erpRules = await db
            .select()
            .from(erpMappingRules)
            .where(and(
              inArray(erpMappingRules.ruleSetId, ruleSetIds),
              eq(erpMappingRules.isActive, true)
            ));
          
          erpGeneratedRules = erpRules.map(rule => ({
            ...rule,
            isErpGenerated: true,
            ruleSource: 'erp_mapping',
            dualTerminology: rule.description || `${rule.sourceField} (ERP: ${rule.targetField})`,
          }));
        }
      }
      
      // Format manual rules
      const formattedManualRules = manualRules.map(rule => ({
        ...rule,
        isErpGenerated: false,
        ruleSource: 'manual',
        dualTerminology: null,
      }));
      
      res.json({
        contractId,
        calculationApproach,
        manualRules: formattedManualRules,
        erpGeneratedRules,
        combinedCount: formattedManualRules.length + erpGeneratedRules.length,
        summary: {
          manualCount: formattedManualRules.length,
          erpCount: erpGeneratedRules.length,
          calculationMode: calculationApproach,
        }
      });
    } catch (error: any) {
      console.error('Get combined rules error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch combined rules' });
    }
  });

  // Get calculation blueprints for a contract
  app.get('/api/contracts/:id/blueprints', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const context = req.user?.activeContext;
      
      // Get contract to verify access
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }
      
      // Multi-tenant authorization check (same as combined-rules endpoint)
      const userRole = (await storage.getUser(req.user?.id))?.role;
      const isAdmin = userRole === 'admin' || userRole === 'owner';
      if (!isAdmin && context?.companyId && contract.companyId !== context.companyId) {
        return res.status(403).json({ error: 'Access denied: Contract belongs to different organization' });
      }
      
      // Load blueprints using raw SQL
      const result = await db.execute(sql`
        SELECT cb.*, 
               json_agg(
                 json_build_object(
                   'dimensionType', bd.dimension_type,
                   'contractTerm', bd.contract_term,
                   'erpFieldName', bd.erp_field_name,
                   'matchValue', bd.match_value,
                   'isMapped', bd.is_mapped
                 )
               ) FILTER (WHERE bd.id IS NOT NULL) as dimensions
        FROM calculation_blueprints cb
        LEFT JOIN blueprint_dimensions bd ON cb.id = bd.blueprint_id
        WHERE cb.contract_id = ${contractId}
        GROUP BY cb.id
        ORDER BY cb.priority ASC
      `);
      
      const blueprints = (result.rows as any[]).map(row => ({
        id: row.id,
        name: row.name,
        ruleType: row.rule_type,
        isFullyMapped: row.is_fully_mapped,
        unmappedFields: row.unmapped_fields || [],
        dimensions: row.dimensions || [],
      }));
      
      const fullyMappedCount = blueprints.filter(bp => bp.isFullyMapped).length;
      
      res.json({
        blueprints,
        totalCount: blueprints.length,
        fullyMappedCount,
      });
    } catch (error: any) {
      console.error('Get blueprints error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch blueprints' });
    }
  });

  app.get('/api/human-review-tasks', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canViewAll = userRole === 'admin' || userRole === 'owner';
      
      const tasks = await storage.getPendingReviewTasks(canViewAll ? undefined : userId);
      res.json(tasks);
    } catch (error: any) {
      console.error('Get review tasks error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch review tasks' });
    }
  });

  app.patch('/api/human-review-tasks/:id/approve', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const taskId = req.params.id;
      
      // Get task and check authorization
      const task = await db.query.humanReviewTasks.findFirst({
        where: (tasks, { eq }) => eq(tasks.id, taskId),
      });
      
      if (!task) {
        return res.status(404).json({ error: 'Task not found' });
      }
      
      // Check if user is assigned to task or has admin/owner role
      const userRole = (await storage.getUser(userId))?.role;
      const isAuthorized = task.assignedTo === userId || userRole === 'admin' || userRole === 'owner';
      
      if (!isAuthorized) {
        return res.status(403).json({ error: 'You are not authorized to approve this task' });
      }
      
      // Validate and sanitize review notes
      let reviewData = '';
      if (req.body.reviewNotes !== undefined && req.body.reviewNotes !== null) {
        if (typeof req.body.reviewNotes !== 'string') {
          return res.status(400).json({ error: 'Review notes must be a string' });
        }
        reviewData = req.body.reviewNotes.trim();
      }
      
      await storage.approveReviewTask(taskId, userId, reviewData || 'Approved');
      await createAuditLog(req, 'review_task_approved', 'human_review_task', taskId);
      res.json({ message: 'Task approved' });
    } catch (error: any) {
      console.error('Approve task error:', error);
      res.status(500).json({ error: error.message || 'Failed to approve task' });
    }
  });

  app.patch('/api/human-review-tasks/:id/reject', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const taskId = req.params.id;
      
      // Validate request body
      if (!req.body.reviewNotes || typeof req.body.reviewNotes !== 'string' || req.body.reviewNotes.trim() === '') {
        return res.status(400).json({ error: 'Review notes are required for rejection' });
      }
      
      // Get task and check authorization
      const task = await db.query.humanReviewTasks.findFirst({
        where: (tasks, { eq }) => eq(tasks.id, taskId),
      });
      
      if (!task) {
        return res.status(404).json({ error: 'Task not found' });
      }
      
      // Check if user is assigned to task or has admin/owner role
      const userRole = (await storage.getUser(userId))?.role;
      const isAuthorized = task.assignedTo === userId || userRole === 'admin' || userRole === 'owner';
      
      if (!isAuthorized) {
        return res.status(403).json({ error: 'You are not authorized to reject this task' });
      }
      
      await storage.rejectReviewTask(taskId, userId, req.body.reviewNotes);
      await createAuditLog(req, 'review_task_rejected', 'human_review_task', taskId);
      res.json({ message: 'Task rejected' });
    } catch (error: any) {
      console.error('Reject task error:', error);
      res.status(500).json({ error: error.message || 'Failed to reject task' });
    }
  });

  app.get('/api/rules/:id/validation-events', isAuthenticated, async (req: any, res: Response) => {
    try {
      const events = await storage.getRuleValidationEvents(req.params.id);
      res.json(events);
    } catch (error: any) {
      console.error('Get validation events error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch validation events' });
    }
  });

  // Analytics API endpoints
  app.get('/api/analytics/metrics', isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canViewAll = userRole === 'admin' || userRole === 'owner';
      
      // Add caching (private for user-specific data)
      res.set('Cache-Control', 'private, max-age=300');
      
      const metrics = await storage.getContractMetrics(
        canViewAll ? undefined : userId
      );

      res.json(metrics);
    } catch (error) {
      console.error('Error fetching metrics:', error);
      res.status(500).json({ message: 'Failed to fetch metrics' });
    }
  });

  // Get audit logs
  app.get('/api/audit', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      
      // Check if user has audit access
      if (userRole !== 'admin' && userRole !== 'owner' && userRole !== 'auditor') {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      const limit = parseInt(req.query.limit as string) || 50;
      const offset = parseInt(req.query.offset as string) || 0;
      
      const result = await storage.getAuditLogs(undefined, limit, offset);
      res.json(result);
    } catch (error) {
      console.error('Error fetching audit logs:', error);
      res.status(500).json({ message: 'Failed to fetch audit logs' });
    }
  });

  // Get users list (admin only)
  // System admins see all users, other admins see users within their hierarchy scope
  app.get('/api/users', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      
      // System admins can see all users with their company assignments
      if (isSystemAdmin(user)) {
        const users = await storage.getAllUsersWithCompanies();
        return res.json(users);
      }
      
      // Check if user has admin/owner access (either global role or context role)
      const globalRole = user?.role;
      const contextRole = req.user?.activeContext?.role;
      const hasAdminAccess = 
        globalRole === 'admin' || globalRole === 'owner' || 
        contextRole === 'admin' || contextRole === 'owner';
      
      if (!hasAdminAccess) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      // Get hierarchy context
      const companyId = getUserCompanyId(req.user);
      const businessUnitId = req.user?.activeContext?.businessUnitId;
      const locationId = req.user?.activeContext?.locationId;
      
      if (!companyId) {
        return res.status(400).json({ error: 'No active company context. Please select a location first.' });
      }
      
      // Filter users based on admin's hierarchy level:
      // - Location admin: only users at that location
      // - BU admin: only users in that BU (all locations)
      // - Company admin: all users in company
      const users = await storage.getUsersByOrganization(companyId, businessUnitId, locationId);
      res.json(users);
    } catch (error) {
      console.error('Error fetching users:', error);
      res.status(500).json({ error: 'Failed to fetch users' });
    }
  });

  // Create user (admin only - with two-tier admin support)
  // System admins can create users for any company
  // Company admins can create users for their own company only
  app.post('/api/users/create', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = req.user;
      const currentUserId = user.id;
      
      // Check if user has admin access (system admin OR context admin)
      const hasAdminAccess = isSystemAdmin(user) || isContextAdmin(user);
      
      if (!hasAdminAccess) {
        return res.status(403).json({ message: 'Access denied. Admin privileges required.' });
      }

      const { username, password, email, firstName, lastName, role, companyId, businessUnitId, locationId, orgRole } = req.body;
      
      // Validate required fields
      if (!username || !password) {
        return res.status(400).json({ message: 'Username and password are required' });
      }
      
      // Security: Company admins cannot assign global admin/owner roles
      // Only system admins can create users with elevated global roles
      let safeGlobalRole = role || 'viewer';
      if (!isSystemAdmin(user)) {
        // Company admins can only create users with safe global roles
        const allowedRoles = ['viewer', 'editor', 'auditor'];
        if (!allowedRoles.includes(safeGlobalRole)) {
          safeGlobalRole = 'viewer'; // Force safe default
        }
      }
      
      // Check if username already exists
      const existingUser = await storage.getUserByUsername(username);
      if (existingUser) {
        return res.status(400).json({ message: 'Username already exists' });
      }

      // Check if email already exists (if provided)
      if (email) {
        const existingEmail = await storage.getUserByEmail(email);
        if (existingEmail) {
          return res.status(400).json({ message: 'Email already exists' });
        }
      }

      // Hash the password
      const hashedPassword = await hashPassword(password);
      
      // Create the user with the safe global role
      const newUser = await storage.createUser({
        username,
        password: hashedPassword,
        email: email || null,
        firstName: firstName || null,
        lastName: lastName || null,
        role: safeGlobalRole,
        isActive: true,
      });

      // If company admin is creating user, auto-assign to their company
      // Or if companyId is provided, create the organization role assignment
      let targetCompanyId = companyId;
      let targetBusinessUnitId = businessUnitId;
      let targetLocationId = locationId;
      let targetOrgRole = orgRole || 'viewer';
      
      // For company admins, enforce their company context
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(user);
        if (!userCompanyId) {
          return res.status(400).json({ message: 'No active company context. Please select a location first.' });
        }
        // Company admin can only assign to their own company
        targetCompanyId = userCompanyId;
        
        // If they provided different companyId, reject
        if (companyId && companyId !== userCompanyId) {
          return res.status(403).json({ message: 'Cannot assign users to another company' });
        }
      }
      
      // If we have a company to assign to, create the organization role
      if (targetCompanyId) {
        try {
          await storage.createUserOrganizationRole({
            userId: newUser.id,
            companyId: targetCompanyId,
            businessUnitId: targetBusinessUnitId || null,
            locationId: targetLocationId || null,
            role: targetOrgRole,
            status: 'A',
            createdBy: currentUserId,
            lastUpdatedBy: currentUserId,
          });
        } catch (orgError) {
          console.error('Failed to create organization role for new user:', orgError);
          // User is created, but org role failed - log but don't fail the request
        }
      }

      await createAuditLog(req, 'user_created', 'user', newUser.id, {
        username: newUser.username,
        email: newUser.email,
        role: newUser.role,
        companyId: targetCompanyId,
      });

      res.status(201).json({
        id: newUser.id,
        username: newUser.username,
        email: newUser.email,
        firstName: newUser.firstName,
        lastName: newUser.lastName,
        role: newUser.role,
        isActive: newUser.isActive,
      });
    } catch (error: any) {
      console.error('Create user error:', error);
      res.status(500).json({ message: error.message || 'Failed to create user' });
    }
  });

  // Update user role (admin only)
  app.patch('/api/users/:id/role', isAuthenticated, async (req: any, res: Response) => {
    try {
      const currentUserId = req.user.id;
      const currentUserRole = (await storage.getUser(currentUserId))?.role;
      
      // Check if user has admin access
      if (currentUserRole !== 'admin' && currentUserRole !== 'owner') {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      const { newRole } = req.body;
      const targetUserId = req.params.id;
      
      // Prevent non-owners from changing owner role
      if (newRole === 'owner' && currentUserRole !== 'owner') {
        return res.status(403).json({ error: 'Only owners can assign owner role' });
      }

      await storage.updateUserRole(targetUserId, newRole);
      
      // Log the action
      await createAuditLog(req, 'user_role_update', 'user', targetUserId, {
        newRole,
        previousRole: 'unknown' // We'd need to fetch this beforehand to track properly
      });

      res.json({ success: true });
    } catch (error) {
      console.error('Error updating user role:', error);
      res.status(500).json({ error: 'Failed to update user role' });
    }
  });

  // Update user profile (admin only)
  app.patch('/api/users/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const currentUserId = req.user.id;
      const currentUserRole = (await storage.getUser(currentUserId))?.role;
      
      // Check if user has admin access or is updating their own profile
      if (currentUserRole !== 'admin' && currentUserRole !== 'owner' && currentUserId !== req.params.id) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      const { firstName, lastName, email } = req.body;
      const targetUserId = req.params.id;
      
      await storage.updateUser(targetUserId, { firstName, lastName, email });
      
      // Log the action
      await createAuditLog(req, 'user_profile_update', 'user', targetUserId, {
        updatedFields: { firstName, lastName, email }
      });

      res.json({ success: true });
    } catch (error) {
      console.error('Error updating user profile:', error);
      res.status(500).json({ error: 'Failed to update user profile' });
    }
  });

  // Reset user password (admin only)
  app.post('/api/users/:id/reset-password', isAuthenticated, async (req: any, res: Response) => {
    try {
      const currentUserId = req.user.id;
      const currentUser = await storage.getUser(currentUserId);
      
      // Check if user has admin access
      if (currentUser?.role !== 'admin' && currentUser?.role !== 'owner') {
        return res.status(403).json({ error: 'Insufficient permissions. Admin or Owner role required.' });
      }

      const { newPassword } = req.body;
      const targetUserId = req.params.id;
      
      if (!newPassword || newPassword.length < 6) {
        return res.status(400).json({ error: 'Password must be at least 6 characters' });
      }
      
      const targetUser = await storage.getUser(targetUserId);
      if (!targetUser) {
        return res.status(404).json({ error: 'User not found' });
      }

      // Prevent non-owners from resetting owner passwords
      if (targetUser.role === 'owner' && currentUser?.role !== 'owner') {
        return res.status(403).json({ error: 'Only owners can reset other owner passwords' });
      }

      // Hash the new password
      const hashedPassword = await hashPassword(newPassword);
      
      // Update the user's password
      await storage.updateUser(targetUserId, { password: hashedPassword });
      
      // Log the action
      await createAuditLog(req, 'user_password_reset', 'user', targetUserId, {
        resetBy: currentUserId,
        targetEmail: targetUser.email
      });

      console.log(`🔐 Password reset for user: ${targetUser.email} by admin: ${currentUser?.email}`);
      res.json({ success: true, message: 'Password reset successfully' });
    } catch (error) {
      console.error('Error resetting user password:', error);
      res.status(500).json({ error: 'Failed to reset user password' });
    }
  });

  // Delete user (admin only)
  app.delete('/api/users/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const currentUserId = req.user.id;
      const currentUserRole = (await storage.getUser(currentUserId))?.role;
      
      // Check if user has admin access
      if (currentUserRole !== 'admin' && currentUserRole !== 'owner') {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      const targetUserId = req.params.id;
      
      // Prevent self-deletion
      if (currentUserId === targetUserId) {
        return res.status(400).json({ error: 'Cannot delete your own account' });
      }

      const targetUser = await storage.getUser(targetUserId);
      if (!targetUser) {
        return res.status(404).json({ error: 'User not found' });
      }

      // Prevent non-owners from deleting owners
      if (targetUser.role === 'owner' && currentUserRole !== 'owner') {
        return res.status(403).json({ error: 'Only owners can delete other owners' });
      }

      await storage.deleteUser(targetUserId);
      
      // Log the action
      await createAuditLog(req, 'user_delete', 'user', targetUserId, {
        deletedEmail: targetUser.email
      });

      res.json({ success: true });
    } catch (error) {
      console.error('Error deleting user:', error);
      res.status(500).json({ error: 'Failed to delete user' });
    }
  });

  // User Organization Roles Routes
  
  // Get all user organization role assignments
  app.get('/api/user-organization-roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      
      // System admins see all roles
      if (isSystemAdmin(user)) {
        const roles = await storage.getAllUserOrganizationRoles();
        return res.json(roles);
      }
      
      // Check if user has admin access (global or context)
      const globalRole = user?.role;
      const contextRole = req.user?.activeContext?.role;
      const hasAdminAccess = 
        globalRole === 'admin' || globalRole === 'owner' || 
        contextRole === 'admin' || contextRole === 'owner';
      
      if (!hasAdminAccess) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      // Non-system admins see roles filtered by their hierarchy
      const companyId = getUserCompanyId(req.user);
      const businessUnitId = req.user?.activeContext?.businessUnitId;
      const locationId = req.user?.activeContext?.locationId;
      
      if (!companyId) {
        return res.status(400).json({ error: 'No active company context' });
      }
      
      const roles = await storage.getUsersByOrganization(companyId, businessUnitId, locationId);
      res.json(roles);
    } catch (error) {
      console.error('Error fetching user organization roles:', error);
      res.status(500).json({ error: 'Failed to fetch user organization roles' });
    }
  });

  // Get organization roles for a specific user
  app.get('/api/user-organization-roles/user/:userId', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const targetUserId = req.params.userId;
      
      // Users can view their own roles
      if (req.user.id === targetUserId) {
        const roles = await storage.getUserOrganizationRoles(targetUserId);
        return res.json(roles);
      }
      
      // System admins can view any user's roles
      if (isSystemAdmin(user)) {
        const roles = await storage.getUserOrganizationRoles(targetUserId);
        return res.json(roles);
      }
      
      // Check if user has admin access (global or context)
      const globalRole = user?.role;
      const contextRole = req.user?.activeContext?.role;
      const hasAdminAccess = 
        globalRole === 'admin' || globalRole === 'owner' || 
        contextRole === 'admin' || contextRole === 'owner';
      
      if (!hasAdminAccess) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }

      // Company/BU/Location admins can view roles for users in their scope
      // Get all roles for the target user, then filter to show only roles within admin's scope
      const companyId = getUserCompanyId(req.user);
      const businessUnitId = req.user?.activeContext?.businessUnitId;
      const locationId = req.user?.activeContext?.locationId;
      
      const allUserRoles = await storage.getUserOrganizationRoles(targetUserId);
      
      // Filter roles to only show those within admin's hierarchy scope
      const filteredRoles = allUserRoles.filter((role: any) => {
        // Must be in the same company
        if (role.companyId !== companyId) return false;
        
        // If admin is at location level, only show roles at that location
        if (locationId && role.locationId !== locationId) return false;
        
        // If admin is at BU level, only show roles in that BU
        if (businessUnitId && !locationId && role.businessUnitId !== businessUnitId) return false;
        
        return true;
      });
      
      res.json(filteredRoles);
    } catch (error) {
      console.error('Error fetching user organization roles:', error);
      res.status(500).json({ error: 'Failed to fetch user organization roles' });
    }
  });

  // Get users by organization
  app.get('/api/user-organization-roles/organization/:companyId', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = req.user;
      const targetCompanyId = req.params.companyId;
      
      // System admins can view any company's users
      // Company admins can only view their own company's users
      if (!isSystemAdmin(user)) {
        if (!isContextAdmin(user)) {
          return res.status(403).json({ error: 'Insufficient permissions' });
        }
        const userCompanyId = getUserCompanyId(user);
        if (userCompanyId !== targetCompanyId) {
          return res.status(403).json({ error: 'Cannot view users from another company' });
        }
      }

      const { businessUnitId, locationId } = req.query;
      const users = await storage.getUsersByOrganization(
        targetCompanyId,
        businessUnitId as string | undefined,
        locationId as string | undefined
      );
      res.json(users);
    } catch (error) {
      console.error('Error fetching users by organization:', error);
      res.status(500).json({ error: 'Failed to fetch users by organization' });
    }
  });

  // Create user organization role assignment
  app.post('/api/user-organization-roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = req.user;
      const currentUserId = user.id;
      const targetCompanyId = req.body.companyId;
      
      // System admins can create roles in any company
      // Company admins can only create roles in their own company
      if (!isSystemAdmin(user)) {
        if (!isContextAdmin(user)) {
          return res.status(403).json({ error: 'Insufficient permissions' });
        }
        const userCompanyId = getUserCompanyId(user);
        if (userCompanyId !== targetCompanyId) {
          return res.status(403).json({ error: 'Cannot assign roles in another company' });
        }
      }

      const { insertUserOrganizationRoleSchema } = await import("@shared/schema");
      const roleData = insertUserOrganizationRoleSchema.parse({
        ...req.body,
        createdBy: currentUserId,
        lastUpdatedBy: currentUserId,
      });

      const role = await storage.createUserOrganizationRole(roleData);
      
      await createAuditLog(req, 'create_user_org_role', 'user_organization_role', role.id, {
        userId: role.userId,
        companyId: role.companyId,
        role: role.role,
      });

      res.json(role);
    } catch (error: any) {
      console.error('Create user organization role error:', error);
      res.status(500).json({ error: error.message || 'Failed to create user organization role' });
    }
  });

  // Update user organization role assignment
  app.patch('/api/user-organization-roles/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = req.user;
      const currentUserId = user.id;
      
      // First, get the existing role to check ownership
      const existingRole = await storage.getUserOrganizationRoleById(req.params.id);
      if (!existingRole) {
        return res.status(404).json({ error: 'Role assignment not found' });
      }
      
      // System admins can update any role
      // Company admins can only update roles in their own company
      if (!isSystemAdmin(user)) {
        if (!isContextAdmin(user)) {
          return res.status(403).json({ error: 'Insufficient permissions' });
        }
        const userCompanyId = getUserCompanyId(user);
        if (userCompanyId !== existingRole.companyId) {
          return res.status(403).json({ error: 'Cannot update roles in another company' });
        }
      }

      const role = await storage.updateUserOrganizationRole(req.params.id, req.body, currentUserId);
      
      await createAuditLog(req, 'update_user_org_role', 'user_organization_role', role.id, {
        changes: req.body,
      });

      res.json(role);
    } catch (error: any) {
      console.error('Update user organization role error:', error);
      res.status(500).json({ error: error.message || 'Failed to update user organization role' });
    }
  });

  // Delete user organization role assignment
  app.delete('/api/user-organization-roles/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = req.user;
      
      // First, get the existing role to check ownership
      const existingRole = await storage.getUserOrganizationRoleById(req.params.id);
      if (!existingRole) {
        return res.status(404).json({ error: 'Role assignment not found' });
      }
      
      // System admins can delete any role
      // Company admins can only delete roles in their own company
      if (!isSystemAdmin(user)) {
        if (!isContextAdmin(user)) {
          return res.status(403).json({ error: 'Insufficient permissions' });
        }
        const userCompanyId = getUserCompanyId(user);
        if (userCompanyId !== existingRole.companyId) {
          return res.status(403).json({ error: 'Cannot delete roles in another company' });
        }
      }

      await storage.deleteUserOrganizationRole(req.params.id);
      
      await createAuditLog(req, 'delete_user_org_role', 'user_organization_role', req.params.id, {});

      res.json({ success: true });
    } catch (error) {
      console.error('Error deleting user organization role:', error);
      res.status(500).json({ error: 'Failed to delete user organization role' });
    }
  });

  // User Context Management Routes
  
  // Get all organization contexts for current user (with full details)
  app.get('/api/user/org-contexts', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const contexts = await storage.getUserOrganizationRoles(userId);
      res.json(contexts);
    } catch (error) {
      console.error('Error fetching user org contexts:', error);
      res.status(500).json({ error: 'Failed to fetch organization contexts' });
    }
  });

  // Get current active context for user
  app.get('/api/user/active-context', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const activeContextRaw = await storage.getUserActiveContext(userId);
      
      if (!activeContextRaw) {
        return res.status(404).json({ error: 'No active context found' });
      }

      // Get full details of the active organization role (with all display names)
      const allContexts = await storage.getUserOrganizationRoles(userId);
      const contextDetails = allContexts.find(c => c.id === activeContextRaw.activeOrgRoleId);
      
      if (!contextDetails) {
        return res.status(404).json({ error: 'Active context details not found' });
      }

      // Return complete context with all fields needed by UI
      res.json({
        activeContext: {
          id: contextDetails.id,
          userId: activeContextRaw.userId,
          activeOrgRoleId: activeContextRaw.activeOrgRoleId,
          companyId: contextDetails.companyId,
          companyName: contextDetails.companyName,
          businessUnitId: contextDetails.businessUnitId,
          businessUnitName: contextDetails.businessUnitName,
          locationId: contextDetails.locationId,
          locationName: contextDetails.locationName,
          role: contextDetails.role,
          lastSwitched: activeContextRaw.lastSwitched,
        },
        contextDetails, // Keep for backward compatibility
      });
    } catch (error) {
      console.error('Error fetching active context:', error);
      res.status(500).json({ error: 'Failed to fetch active context' });
    }
  });

  // Switch to a different organization context
  app.post('/api/user/active-context', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const { orgRoleId } = req.body;

      if (!orgRoleId) {
        return res.status(400).json({ error: 'orgRoleId is required' });
      }

      // Verify that the user has this org role assigned
      const userRoles = await storage.getUserOrganizationRoles(userId);
      const hasRole = userRoles.some(r => r.id === orgRoleId);
      
      if (!hasRole) {
        return res.status(403).json({ error: 'User does not have this organization role' });
      }

      // Set the active context
      const context = await storage.setUserActiveContext(userId, orgRoleId);
      
      await createAuditLog(req, 'switch_context', 'user_active_context', context.id, {
        orgRoleId,
        previousContext: await storage.getUserActiveContext(userId)
      });

      res.json(context);
    } catch (error) {
      console.error('Error switching context:', error);
      res.status(500).json({ error: 'Failed to switch context' });
    }
  });

  // Search contracts (comprehensive content-based search)
  app.get('/api/contracts/search', isAuthenticated, async (req: any, res: Response) => {
    try {
      const query = req.query.q as string;
      
      if (!query || query.trim().length === 0) {
        return res.json({ contracts: [] });
      }

      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const isSystemAdmin = req.user.isSystemAdmin === true;
      
      // Check both global role AND context role for admin/owner access
      const contextRole = req.user.activeContext?.role;
      const hasAdminAccess = userRole === 'admin' || userRole === 'owner' || 
                             contextRole === 'admin' || contextRole === 'owner' || 
                             isSystemAdmin;
      
      // Build organizational context for filtering
      const orgContext = {
        activeContext: req.user.activeContext,
        globalRole: userRole || 'viewer',
        userId,
        isSystemAdmin,
      };

      const contracts = await storage.searchContracts(
        query.trim(),
        hasAdminAccess ? undefined : userId,
        orgContext
      );
      
      res.json({ contracts });
    } catch (error) {
      console.error('Search contracts error:', error);
      res.status(500).json({ error: 'Failed to search contracts' });
    }
  });

  // Get contracts list
  app.get('/api/contracts', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const isSystemAdmin = req.user.isSystemAdmin === true;
      
      // Check both global role AND context role for admin/owner access
      // A Company Admin might have 'viewer' global role but 'admin' context role
      const contextRole = req.user.activeContext?.role;
      const hasAdminAccess = userRole === 'admin' || userRole === 'owner' || 
                             contextRole === 'admin' || contextRole === 'owner' || 
                             isSystemAdmin;
      
      const limit = parseInt(req.query.limit as string) || 20;
      const offset = parseInt(req.query.offset as string) || 0;

      // Build organizational context for filtering
      const orgContext = {
        activeContext: req.user.activeContext,
        globalRole: userRole || 'viewer',
        userId,
        isSystemAdmin,
      };

      // Don't pass userId filter if user has admin access - let orgContext filtering handle visibility
      const contracts = await storage.getContracts(hasAdminAccess ? undefined : userId, limit, offset, orgContext);
      res.json(contracts);
    } catch (error) {
      console.error('Get contracts error:', error);
      res.status(500).json({ error: 'Failed to fetch contracts' });
    }
  });

  // Get contract details
  app.get('/api/contracts/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const isSystemAdmin = req.user.isSystemAdmin === true;
      
      // Check both global role AND context role for admin/owner access
      const contextRole = req.user.activeContext?.role;
      const hasAdminAccess = userRole === 'admin' || userRole === 'owner' || 
                             contextRole === 'admin' || contextRole === 'owner' || 
                             isSystemAdmin;

      // Build organizational context for filtering
      const orgContext = {
        activeContext: req.user.activeContext,
        globalRole: userRole || 'viewer',
        userId,
        isSystemAdmin,
      };

      // Get contract with org context filtering
      const contract = await storage.getContract(req.params.id, orgContext);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      // Additional access check: if not admin, must be uploader or in org hierarchy
      if (!hasAdminAccess && contract.uploadedBy !== userId) {
        return res.status(403).json({ error: 'Access denied' });
      }

      res.json(contract);
    } catch (error) {
      console.error('Get contract error:', error);
      res.status(500).json({ error: 'Failed to fetch contract' });
    }
  });

  // Get contract analysis
  app.get('/api/contracts/:id/analysis', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contract = await storage.getContract(req.params.id);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canViewAny = userRole === 'admin' || userRole === 'owner';
      
      if (!canViewAny && contract.uploadedBy !== userId) {
        return res.status(403).json({ error: 'Access denied' });
      }

      const analysis = await storage.getContractAnalysis(req.params.id);
      if (!analysis) {
        return res.status(404).json({ error: 'Analysis not found' });
      }

      res.json(analysis);
    } catch (error) {
      console.error('Get analysis error:', error);
      res.status(500).json({ error: 'Failed to fetch analysis' });
    }
  });

  // Export contract analysis as report
  app.get('/api/contracts/:id/report', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      
      // Get contract and analysis data
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      // Check permissions  
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canViewAny = userRole === 'admin' || userRole === 'owner';
      
      if (!canViewAny && contract.uploadedBy !== userId) {
        return res.status(403).json({ error: 'Access denied' });
      }
      
      const analysis = await storage.getContractAnalysis(contractId);
      if (!analysis) {
        return res.status(404).json({ error: 'Analysis not found' });
      }

      // Format risk analysis
      const formatRiskAnalysis = (riskData: any): string => {
        if (!riskData) return 'No risk analysis available';
        if (typeof riskData === 'string') return riskData;
        if (Array.isArray(riskData)) {
          return riskData.map((risk: any, index: number) => {
            if (typeof risk === 'string') return `${index + 1}. ${risk}`;
            if (typeof risk === 'object') {
              const title = risk.title || risk.category || risk.type || 'Risk';
              const level = risk.level ? `[${risk.level.toUpperCase()}] ` : '';
              const description = risk.description || risk.details || risk.impact || risk.mitigation || '';
              // If no description fields found, stringify the whole object
              const content = description || JSON.stringify(risk, null, 2);
              return `${index + 1}. ${level}${title}\n   ${content}`;
            }
            return `${index + 1}. ${JSON.stringify(risk)}`;
          }).join('\n\n');
        }
        if (typeof riskData === 'object') {
          const risks = [];
          if (riskData.overall) risks.push(`Overall Risk Level: ${riskData.overall}`);
          if (riskData.risks && Array.isArray(riskData.risks)) {
            risks.push(...riskData.risks.map((risk: any, index: number) => {
              const title = risk.title || risk.category || risk.type || 'Risk';
              const level = risk.level ? `[${risk.level.toUpperCase()}] ` : '';
              const description = risk.description || risk.details || risk.impact || risk.mitigation || '';
              // If no description fields found, stringify the whole object
              const content = description || JSON.stringify(risk, null, 2);
              return `${index + 1}. ${level}${title}\n   ${content}`;
            }));
          }
          return risks.length > 0 ? risks.join('\n\n') : JSON.stringify(riskData, null, 2);
        }
        return String(riskData);
      };

      // Format insights
      const formatInsights = (insightsData: any): string => {
        if (!insightsData) return 'No insights available';
        if (typeof insightsData === 'string') return insightsData;
        if (Array.isArray(insightsData)) {
          return insightsData.map((insight: any, index: number) => {
            if (typeof insight === 'string') return `${index + 1}. ${insight}`;
            if (typeof insight === 'object') {
              const title = insight.title || insight.category || insight.type || 'Insight';
              const content = insight.description || insight.recommendation || insight.details || insight.action || '';
              // If no content fields found, stringify the whole object
              const text = content || JSON.stringify(insight, null, 2);
              return `${index + 1}. ${title}\n   ${text}`;
            }
            return `${index + 1}. ${JSON.stringify(insight)}`;
          }).join('\n\n');
        }
        if (typeof insightsData === 'object') {
          const insights = [];
          if (insightsData.recommendations && Array.isArray(insightsData.recommendations)) {
            insights.push(...insightsData.recommendations.map((rec: any, index: number) => {
              const title = rec.title || rec.category || rec.type || 'Recommendation';
              const content = rec.description || rec.recommendation || rec.details || rec.action || '';
              // If no content fields found, stringify the whole object
              const text = content || JSON.stringify(rec, null, 2);
              return `${index + 1}. ${title}\n   ${text}`;
            }));
          } else if (insightsData.insights && Array.isArray(insightsData.insights)) {
            insights.push(...insightsData.insights.map((insight: any, index: number) => {
              const title = insight.title || insight.category || insight.type || 'Insight';
              const content = insight.description || insight.details || insight.action || '';
              // If no content fields found, stringify the whole object
              const text = content || JSON.stringify(insight, null, 2);
              return `${index + 1}. ${title}\n   ${text}`;
            }));
          }
          return insights.length > 0 ? insights.join('\n\n') : JSON.stringify(insightsData, null, 2);
        }
        return String(insightsData);
      };

      // Generate report content
      const reportContent = `
LICENSEIQ ANALYSIS REPORT
==========================

Contract: ${contract.originalName}
Analysis Date: ${analysis.createdAt ? new Date(analysis.createdAt).toLocaleDateString() : 'N/A'}
Confidence Score: ${Math.round(parseFloat(analysis.confidence || '0') * 100)}%

EXECUTIVE SUMMARY
=================
${analysis.summary || 'No summary available'}

KEY TERMS & CONDITIONS
======================
${Array.isArray(analysis.keyTerms) ? analysis.keyTerms.map((term: any, index: number) => {
  if (typeof term === 'string') return `${index + 1}. ${term}`;
  if (term && typeof term === 'object') return `${index + 1}. ${term.type || term.title || 'Term'}: ${term.description || term.value || JSON.stringify(term)}`;
  return `${index + 1}. ${String(term)}`;
}).join('\n') : 'No key terms extracted'}

RISK ANALYSIS
=============
${formatRiskAnalysis(analysis.riskAnalysis)}

AI INSIGHTS & RECOMMENDATIONS
==============================
${formatInsights(analysis.insights)}

---
Generated by LicenseIQ AI Analysis Platform
Report ID: ${contractId}
`;

      // Set headers for file download
      res.setHeader('Content-Type', 'text/plain');
      res.setHeader('Content-Disposition', `attachment; filename="${contract.originalName}_analysis_report.txt"`);
      
      res.send(reportContent);
    } catch (error) {
      console.error('Report generation error:', error);
      res.status(500).json({ error: 'Failed to generate report' });
    }
  });

  // Update contract metadata
  app.patch('/api/contracts/:id/metadata', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user.id;
      
      // Validate metadata with Zod
      const { updateContractMetadataSchema } = await import("@shared/schema");
      const metadata = updateContractMetadataSchema.parse(req.body);

      // Get contract and check permissions
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const user = await storage.getUser(userId);
      const canEdit = user?.role === 'admin' || user?.role === 'owner' || user?.role === 'editor' || contract.uploadedBy === userId;
      
      if (!canEdit) {
        return res.status(403).json({ error: 'You do not have permission to edit this contract' });
      }

      // Update metadata and create version
      const updatedContract = await storage.updateContractMetadata(contractId, metadata, userId);

      // Create audit log
      await createAuditLog(req, 'update_contract_metadata', 'contract', contractId, {
        changes: metadata.changeSummary,
      });

      res.json(updatedContract);
    } catch (error: any) {
      console.error('Update metadata error:', error);
      res.status(500).json({ error: error.message || 'Failed to update contract metadata' });
    }
  });

  // Update ERP matching setting for a contract
  app.patch('/api/contracts/:id/erp-matching', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { enabled } = req.body;

      if (typeof enabled !== 'boolean') {
        return res.status(400).json({ error: 'enabled must be a boolean value' });
      }

      // Get contract and check permissions
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const user = await storage.getUser(req.user.id);
      const canEdit = user?.role === 'admin' || user?.role === 'owner' || user?.role === 'editor' || contract.uploadedBy === req.user.id;
      
      if (!canEdit) {
        return res.status(403).json({ error: 'You do not have permission to edit this contract' });
      }

      // Update ERP matching setting
      const updatedContract = await storage.updateContractErpMatching(contractId, enabled);

      // Create audit log
      await createAuditLog(req, 'update_erp_matching', 'contract', contractId, {
        enabled,
        message: enabled ? 'ERP semantic matching enabled' : 'ERP semantic matching disabled',
      });

      res.json({ 
        id: updatedContract.id,
        enabled: updatedContract.useErpMatching,
      });
    } catch (error: any) {
      console.error('Update ERP matching error:', error);
      res.status(500).json({ error: error.message || 'Failed to update ERP matching setting' });
    }
  });

  // Submit contract for approval
  app.post('/api/contracts/:id/submit-approval', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user.id;

      // Get contract and check permissions
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const user = await storage.getUser(userId);
      const canSubmit = user?.role === 'admin' || user?.role === 'owner' || user?.role === 'editor' || contract.uploadedBy === userId;
      
      if (!canSubmit) {
        return res.status(403).json({ error: 'You do not have permission to submit this contract for approval' });
      }

      // Submit for approval
      const updatedContract = await storage.submitContractForApproval(contractId, userId);

      // Create audit log
      await createAuditLog(req, 'submit_contract_approval', 'contract', contractId);

      res.json(updatedContract);
    } catch (error: any) {
      console.error('Submit approval error:', error);
      res.status(500).json({ error: error.message || 'Failed to submit contract for approval' });
    }
  });

  // Get contract versions
  app.get('/api/contracts/:id/versions', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;

      // Get contract and check permissions
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const userId = req.user.id;
      const user = await storage.getUser(userId);
      const canView = user?.role === 'admin' || user?.role === 'owner' || contract.uploadedBy === userId;
      
      if (!canView) {
        return res.status(403).json({ error: 'Access denied' });
      }

      // Get versions
      const versions = await storage.getContractVersions(contractId);

      res.json({ versions });
    } catch (error: any) {
      console.error('Get versions error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch contract versions' });
    }
  });

  // Approve or reject a contract version
  app.post('/api/contracts/versions/:versionId/approve', isAuthenticated, async (req: any, res: Response) => {
    try {
      const versionId = req.params.versionId;
      const userId = req.user.id;
      const { status, decisionNotes, adminOverride } = req.body; // status: 'approved' or 'rejected', adminOverride: boolean

      // Validate status
      if (status !== 'approved' && status !== 'rejected') {
        return res.status(400).json({ error: 'Status must be either "approved" or "rejected"' });
      }

      // Get version
      const version = await storage.getContractVersion(versionId);
      if (!version) {
        return res.status(404).json({ error: 'Version not found' });
      }

      // Check if user is an approver (admin or owner only)
      const user = await storage.getUser(userId);
      const canApprove = user?.role === 'admin' || user?.role === 'owner';
      
      if (!canApprove) {
        return res.status(403).json({ error: 'Only admins and owners can approve contracts' });
      }

      // Prevent self-approval unless admin override is enabled
      if (version.editorId === userId) {
        // Allow admin override for admins only
        if (adminOverride && user?.role === 'admin') {
          console.log(`⚠️ Admin override: ${user.username} is force-approving their own changes for version ${versionId}`);
        } else {
          return res.status(403).json({ error: 'You cannot approve your own changes' });
        }
      }

      // Check if version is in pending_approval state
      if (version.approvalState !== 'pending_approval') {
        return res.status(400).json({ error: 'This version is not pending approval' });
      }

      // Create approval record
      const approval = await storage.createContractApproval({
        contractVersionId: versionId,
        approverId: userId,
        status,
        decisionNotes,
      });

      // Create audit log
      await createAuditLog(req, `contract_${status}`, 'contract_version', versionId, {
        notes: decisionNotes,
        approver: user?.username,
      });

      res.json(approval);
    } catch (error: any) {
      console.error('Approval error:', error);
      res.status(500).json({ error: error.message || 'Failed to process approval' });
    }
  });

  // Get pending approvals for the current user
  app.get('/api/approvals/pending', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;

      // Check if user is an approver
      const user = await storage.getUser(userId);
      const canApprove = user?.role === 'admin' || user?.role === 'owner';
      
      if (!canApprove) {
        return res.json({ approvals: [] }); // Return empty array if user can't approve
      }

      // Get pending approvals
      const approvals = await storage.getPendingApprovals(userId);

      res.json({ approvals });
    } catch (error: any) {
      console.error('Get pending approvals error:', error);
      res.status(500).json({ error: error.message || 'Failed to fetch pending approvals' });
    }
  });

  // Reprocess contract endpoint for testing
  app.post('/api/contracts/:id/reprocess', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      // Check permissions
      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canReprocess = userRole === 'admin' || userRole === 'owner' || contract.uploadedBy === userId;
      
      if (!canReprocess) {
        return res.status(403).json({ error: 'Access denied' });
      }

      // Clear existing rules first
      console.log(`🗑️ Clearing existing rules for contract ${contractId}`);
      // Note: Rules will be replaced during reprocessing

      res.json({ message: 'Reprocessing started', contractId });

      // Update status to processing
      await storage.updateContractStatus(contractId, 'processing');

      // Trigger analysis in background
      processContractAnalysis(contractId, contract.filePath);

    } catch (error) {
      console.error('Reprocess error:', error);
      res.status(500).json({ error: 'Failed to reprocess contract' });
    }
  });

  // Calculate royalties for all sales matched to this contract
  app.post('/api/contracts/:id/calculate-matched-royalties', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { periodStart, periodEnd } = req.body;
      
      // Get contract and check permissions
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      const userId = req.user.id;
      const userRole = (await storage.getUser(userId))?.role;
      const canView = userRole === 'admin' || userRole === 'owner' || contract.uploadedBy === userId;
      
      if (!canView) {
        return res.status(403).json({ error: 'Access denied' });
      }

      // Get all sales matched to this contract
      const { dynamicRulesEngine } = await import('./services/dynamicRulesEngine.js');
      const allSales = await storage.getSalesDataByContract(contractId);
      
      // Filter by date range if provided
      let salesToCalculate = allSales;
      if (periodStart || periodEnd) {
        salesToCalculate = allSales.filter(sale => {
          const saleDate = new Date(sale.transactionDate);
          if (periodStart && saleDate < new Date(periodStart)) return false;
          if (periodEnd && saleDate > new Date(periodEnd)) return false;
          return true;
        });
      }

      if (salesToCalculate.length === 0) {
        return res.json({ 
          success: true,
          totalRoyalty: 0, 
          salesCount: 0,
          breakdown: [],
          message: 'No sales data matched to this contract' 
        });
      }

      // Convert sales to format expected by dynamic engine
      const salesItems = salesToCalculate.map(sale => ({
        id: sale.id,
        productName: sale.productName || 'Unknown',
        category: sale.category || '',
        territory: sale.territory || 'Primary Territory',
        quantity: parseFloat(sale.quantity || '0'),
        transactionDate: new Date(sale.transactionDate),
        grossAmount: parseFloat(sale.grossAmount || '0')
      }));

      // Use dynamic rules engine to calculate royalties
      const result = await dynamicRulesEngine.calculateRoyalty(contractId, salesItems);

      const breakdown = result.breakdown.map(item => ({
        saleId: item.saleId,
        productName: item.productName,
        category: item.category,
        territory: item.territory,
        quantity: item.quantity,
        royaltyAmount: item.calculatedRoyalty,
        ruleApplied: item.ruleApplied,
        explanation: item.explanation,
        tierRate: item.tierRate,
        seasonalMultiplier: item.seasonalMultiplier,
        territoryMultiplier: item.territoryMultiplier
      }));

      res.json({
        success: true,
        contractId,
        contractName: contract.originalName,
        calculatedRoyalty: result.totalRoyalty,
        totalRoyalty: result.finalRoyalty,
        minimumGuarantee: result.minimumGuarantee,
        finalRoyalty: result.finalRoyalty,
        salesCount: salesToCalculate.length,
        periodStart,
        periodEnd,
        breakdown,
        rulesApplied: result.rulesApplied,
        currency: 'USD'
      });

    } catch (error) {
      console.error('Royalty calculation error:', error);
      res.status(500).json({ error: 'Failed to calculate royalties' });
    }
  });

  // ==========================================
  // ERP IMPORT ROUTES
  // ==========================================
  
  // Upload and import sales data from CSV/Excel (with AI-driven contract matching)
//   app.post('/api/erp-imports', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
//     try {
//       if (!req.file) {
//         return res.status(400).json({ error: 'No file uploaded' });
//       }
// 
//       // No vendor selection needed - AI will match contracts automatically using semantic search
//       const { vendorId } = req.body; // Legacy support, but not used anymore
// 
//       // Create import job (vendor matching happens via AI semantic search)
//       const importJob = await storage.createErpImportJob({
//         jobType: 'manual_upload',
//         fileName: req.file.originalname,
//         status: 'processing',
//         createdBy: req.user.id,
//         connectionId: null,
//         startedAt: new Date()
//       });
// 
//       // Parse file
//       const fileBuffer = fs.readFileSync(req.file.path);
//       const parseResult = await SalesDataParser.parseFile(fileBuffer, req.file.originalname);
// 
//       // Import SemanticSearchService and GroqValidationService dynamically
//       const { SemanticSearchService } = await import('./services/semanticSearchService.js');
//       const { GroqValidationService } = await import('./services/groqValidationService.js');
// 
//       let lowConfidenceCount = 0;
//       let highConfidenceCount = 0;
//       let noMatchCount = 0;
// 
//       // Store parsed rows in staging with AI contract matching
//       for (const row of parseResult.rows) {
//         let matchedVendorId = vendorId; // Use manual vendor if provided
//         let matchConfidence = 1.0;
//         let matchReasoning = vendorId ? 'Manual vendor selection' : null;
//         let needsReview = false;
// 
//         // AI-driven matching if no vendor specified
//         if (!vendorId && row.validationStatus === 'valid') {
//           try {
//             const salesData = row.rowData as any;
//             const match = await SemanticSearchService.findBestContractForSales({
//               productCode: salesData.productCode,
//               productName: salesData.productName,
//               category: salesData.category,
//               territory: salesData.territory,
//               transactionDate: salesData.transactionDate ? new Date(salesData.transactionDate) : undefined
//             });
// 
//             if (match) {
//               // Use Groq LLaMA to validate the match (FREE)
//               const contract = await storage.getContract(match.contractId);
//               const validation = await GroqValidationService.validateContractMatch(
//                 salesData,
//                 {
//                   summary: contract?.analysis?.summary,
//                   keyTerms: contract?.analysis?.keyTerms,
//                 },
//                 match.confidence
//               );
// 
//               matchedVendorId = match.vendorId;
//               matchConfidence = validation.confidence;
//               matchReasoning = `${match.reasoning}; AI validation: ${validation.reasoning}`;
//               needsReview = validation.confidence < 0.6 || !validation.isValid;
// 
//               if (needsReview) {
//                 lowConfidenceCount++;
//               } else {
//                 highConfidenceCount++;
//               }
//             } else {
//               noMatchCount++;
//               needsReview = true;
//               matchReasoning = 'No matching contract found';
//             }
//           } catch (matchError) {
//             console.error(`❌ Matching error for row ${row.externalId}:`, matchError);
//             needsReview = true;
//             matchConfidence = 0;
//             matchReasoning = `Matching failed: ${matchError.message}`;
//             noMatchCount++;
//           }
//         }
// 
//         await storage.createSalesStaging({
//           importJobId: importJob.id,
//           externalId: row.externalId,
//           rowData: { 
//             ...row.rowData, 
//             vendorId: matchedVendorId,
//             _aiMatch: {
//               confidence: matchConfidence,
//               reasoning: matchReasoning,
//               needsReview
//             }
//           },
//           validationStatus: needsReview ? 'needs_review' : row.validationStatus,
//           validationErrors: row.validationErrors || null
//         });
//       }
// 
//       // Update import job status with AI matching summary
//       await storage.updateErpImportJobStatus(importJob.id, 'completed', {
//         recordsImported: parseResult.validRows,
//         recordsFailed: parseResult.invalidRows,
//         aiMatchingSummary: vendorId ? null : {
//           highConfidence: highConfidenceCount,
//           lowConfidence: lowConfidenceCount,
//           noMatch: noMatchCount
//         }
//       });
// 
//       // Log the import
//       await createAuditLog(req, 'import_sales_data', 'erp_import_job', importJob.id, {
//         fileName: req.file.originalname,
//         totalRows: parseResult.totalRows,
//         validRows: parseResult.validRows,
//         invalidRows: parseResult.invalidRows,
//         aiMatchingEnabled: !vendorId
//       });
// 
//       res.json({
//         importJob,
//         summary: {
//           totalRows: parseResult.totalRows,
//           validRows: parseResult.validRows,
//           invalidRows: parseResult.invalidRows,
//           aiMatching: vendorId ? null : {
//             highConfidence: highConfidenceCount,
//             lowConfidence: lowConfidenceCount,
//             noMatch: noMatchCount
//           }
//         }
//       });
//     } catch (error) {
//       console.error('ERP import error:', error);
//       res.status(500).json({ error: 'Failed to import sales data' });
//     }
//   });

  // Get all import jobs
//   app.get('/api/erp-imports', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const { status } = req.query;
//       const jobs = await storage.getErpImportJobs(req.user.id, status as string);
//       res.json({ jobs });
//     } catch (error) {
//       console.error('Get import jobs error:', error);
//       res.status(500).json({ error: 'Failed to fetch import jobs' });
//     }
//   });

  // Get specific import job with staging data
//   app.get('/api/erp-imports/:id', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const importJob = await storage.getErpImportJob(req.params.id);
//       if (!importJob) {
//         return res.status(404).json({ error: 'Import job not found' });
//       }
// 
//       const stagingData = await storage.getSalesStaging(req.params.id);
//       res.json({ importJob, stagingData });
//     } catch (error) {
//       console.error('Get import job error:', error);
//       res.status(500).json({ error: 'Failed to fetch import job' });
//     }
//   });

  // Promote staging data to sales data
//   app.post('/api/erp-imports/:id/promote', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const importJob = await storage.getErpImportJob(req.params.id);
//       if (!importJob) {
//         return res.status(404).json({ error: 'Import job not found' });
//       }
// 
//       const promotedCount = await storage.promoteStagingToSales(req.params.id);
// 
//       // Log the promotion
//       await createAuditLog(req, 'promote_sales_data', 'erp_import_job', req.params.id, {
//         promotedCount
//       });
// 
//       res.json({ message: 'Sales data promoted successfully', promotedCount });
//     } catch (error) {
//       console.error('Promote sales data error:', error);
//       res.status(500).json({ error: 'Failed to promote sales data' });
//     }
//   });

  // ==========================================
  // ROYALTY RUN ROUTES
  // ==========================================
  
  // Create royalty run and calculate
//   app.post('/api/royalty-runs', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const { name, vendorId, ruleSetId, periodStart, periodEnd } = req.body;
// 
//       // Create royalty run
//       const run = await storage.createRoyaltyRun({
//         name,
//         vendorId,
//         ruleSetId,
//         periodStart: new Date(periodStart),
//         periodEnd: new Date(periodEnd),
//         runBy: req.user.id
//       });
// 
//       // Update status to calculating
//       await storage.updateRoyaltyRunStatus(run.id, 'calculating');
// 
//       res.json({ run });
// 
//       // Trigger async calculation (don't await)
//       calculateRoyalties(run.id, vendorId, ruleSetId, periodStart, periodEnd);
//     } catch (error) {
//       console.error('Create royalty run error:', error);
//       res.status(500).json({ error: 'Failed to create royalty run' });
//     }
//   });

  // Get all royalty runs
//   app.get('/api/royalty-runs', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const { vendorId, status } = req.query;
//       const runs = await storage.getRoyaltyRuns(vendorId as string, status as string);
//       res.json({ runs });
//     } catch (error) {
//       console.error('Get royalty runs error:', error);
//       res.status(500).json({ error: 'Failed to fetch royalty runs' });
//     }
//   });

  // Get specific royalty run
//   app.get('/api/royalty-runs/:id', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const run = await storage.getRoyaltyRun(req.params.id);
//       if (!run) {
//         return res.status(404).json({ error: 'Royalty run not found' });
//       }
// 
//       const results = await storage.getRoyaltyResults(req.params.id);
//       res.json({ run, results });
//     } catch (error) {
//       console.error('Get royalty run error:', error);
//       res.status(500).json({ error: 'Failed to fetch royalty run' });
//     }
//   });

  // Approve royalty run
//   app.post('/api/royalty-runs/:id/approve', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const run = await storage.approveRoyaltyRun(req.params.id, req.user.id);
// 
//       await createAuditLog(req, 'approve_royalty_run', 'royalty_run', req.params.id, {
//         totalRoyalty: run.totalRoyalty
//       });
// 
//       res.json({ run });
//     } catch (error) {
//       console.error('Approve royalty run error:', error);
//       res.status(500).json({ error: 'Failed to approve royalty run' });
//     }
//   });

  // Reject royalty run
//   app.post('/api/royalty-runs/:id/reject', isAuthenticated, async (req: any, res: Response) => {
//     try {
//       const { reason } = req.body;
//       const run = await storage.rejectRoyaltyRun(req.params.id, reason);
// 
//       await createAuditLog(req, 'reject_royalty_run', 'royalty_run', req.params.id, {
//         reason
//       });
// 
//       res.json({ run });
//     } catch (error) {
//       console.error('Reject royalty run error:', error);
//       res.status(500).json({ error: 'Failed to reject royalty run' });
//     }
//   });

  // Register rules engine routes
  registerRulesRoutes(app);

  // Get sales data for a contract
  app.get('/api/contracts/:id/sales', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const context = req.user?.activeContext;
      const salesData = await storage.getSalesDataByContract(contractId, context);
      
      res.json({
        salesData,
        total: salesData.length,
      });
    } catch (error: any) {
      console.error('Error fetching sales data:', error);
      res.status(500).json({ message: error.message });
    }
  });

  // Delete all sales data for a contract
  app.delete('/api/contracts/:id/sales', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user?.id;
      
      // Verify contract exists and check ownership
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }
      
      // Check permissions: admin, owner role, or contract uploader can delete sales data
      const user = await storage.getUser(userId);
      const isOwner = contract.uploadedBy === userId;
      const isAdmin = user?.role === 'admin' || user?.role === 'owner';
      const canDelete = isOwner || isAdmin;
      
      if (!canDelete) {
        return res.status(403).json({ message: 'You do not have permission to delete sales data for this contract' });
      }
      
      await storage.deleteAllSalesDataForContract(contractId);
      
      await createAuditLog(req, 'delete_sales_data', 'contract', contractId, {
        action: 'bulk_delete_sales',
        contractName: contract.originalName
      });
      
      res.json({ message: 'All sales data deleted successfully' });
    } catch (error: any) {
      console.error('Error deleting sales data:', error);
      res.status(500).json({ message: error.message || 'Failed to delete sales data' });
    }
  });

  // Preview formulas that will be applied to sales data
  app.get('/api/contracts/:id/formula-preview', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { periodStart, periodEnd } = req.query;
      
      // Get sales data and rules
      let allSales = await storage.getSalesDataByContract(contractId);
      
      // Filter sales data by date range if provided
      if (periodStart && periodEnd) {
        const startDate = new Date(periodStart as string);
        const endDate = new Date(periodEnd as string);
        
        allSales = allSales.filter(sale => {
          const saleDate = new Date(sale.transactionDate);
          return saleDate >= startDate && saleDate <= endDate;
        });
      }
      
      const rules = await db
        .select()
        .from(royaltyRules)
        .where(and(
          eq(royaltyRules.contractId, contractId),
          eq(royaltyRules.isActive, true)
        ))
        .orderBy(royaltyRules.priority);
      
      if (allSales.length === 0) {
        return res.json({ 
          samples: [], 
          totalProducts: 0,
          totalRules: rules.length,
          unmatchedSales: 0
        });
      }

      // Group sales by product and get sample from each
      const productGroups = new Map<string, typeof allSales[0]>();
      allSales.forEach(sale => {
        const key = `${sale.productName}-${sale.category}`;
        if (!productGroups.has(key)) {
          productGroups.set(key, sale);
        }
      });

      // Helper to recursively evaluate a FormulaNode to a primitive value (number or string)
      const evaluateNode = (node: any): any => {
        if (!node) return null;
        if (typeof node === 'number' || typeof node === 'string') return node;
        if (!node.type) return null;

        switch (node.type) {
          case 'literal':
            return node.value;
            
          case 'reference':
            return node.field; // Return field name as string
            
          case 'multiply':
            if (node.operands && Array.isArray(node.operands)) {
              const values = node.operands.map(evaluateNode).filter((v: any) => typeof v === 'number');
              return values.length > 0 ? values.reduce((a: any, b: any) => a * b, 1) : null;
            }
            return null;
            
          case 'add':
            if (node.operands && Array.isArray(node.operands)) {
              const values = node.operands.map(evaluateNode).filter((v: any) => typeof v === 'number');
              return values.length > 0 ? values.reduce((a: any, b: any) => a + b, 0) : null;
            }
            return null;
            
          case 'subtract':
            const left = evaluateNode(node.left);
            const right = evaluateNode(node.right);
            if (typeof left === 'number' && typeof right === 'number') {
              return left - right;
            }
            return null;
            
          case 'premium':
            const baseVal = evaluateNode(node.base);
            if (typeof baseVal === 'number' && typeof node.percentage === 'number') {
              if (node.mode === 'multiplicative') {
                return baseVal * (1 + node.percentage / 100);
              } else { // additive
                return baseVal + node.percentage;
              }
            }
            return null;
            
          case 'max':
            if (node.operands && Array.isArray(node.operands)) {
              const values = node.operands.map(evaluateNode).filter((v: any) => typeof v === 'number');
              return values.length > 0 ? Math.max(...values) : null;
            }
            return null;
            
          case 'min':
            if (node.operands && Array.isArray(node.operands)) {
              const values = node.operands.map(evaluateNode).filter((v: any) => typeof v === 'number');
              return values.length > 0 ? Math.min(...values) : null;
            }
            return null;
            
          case 'round':
            const val = evaluateNode(node.value);
            if (typeof val === 'number') {
              const multiplier = Math.pow(10, node.precision || 2);
              if (node.mode === 'floor') {
                return Math.floor(val * multiplier) / multiplier;
              } else if (node.mode === 'ceil') {
                return Math.ceil(val * multiplier) / multiplier;
              } else {
                return Math.round(val * multiplier) / multiplier;
              }
            }
            return null;
            
          default:
            // For complex nodes we can't evaluate (tier, lookup, if), return null
            return null;
        }
      };

      // Helper to parse formula definition and extract displayable details
      const parseFormulaDefinition = (def: any): any => {
        const details: any = {
          type: 'formula_based',
          baseRate: null,
          volumeTiers: [],
          seasonalAdjustments: {},
          territoryPremiums: {},
          calculationFormula: '',
          rawDefinition: def
        };

        // Recursive function to extract details from formula nodes
        const extractFromNode = (node: any) => {
          if (!node || !node.type) return;

          switch (node.type) {
            case 'literal':
              // Extract base rate from percentage or decimal literals
              if (node.unit === 'percent' && typeof node.value === 'number') {
                details.baseRate = details.baseRate || (node.value / 100); // Convert percent to decimal
              } else if (typeof node.value === 'number' && node.value < 1 && !node.unit) {
                details.baseRate = details.baseRate || node.value;
              } else if (typeof node.value === 'number' && node.value >= 1 && (node.unit === 'dollars' || node.unit === 'currency')) {
                // Per-unit royalty (e.g., $1.25 per unit)
                details.baseRate = details.baseRate || node.value;
              }
              break;

            case 'tier':
              // Extract volume tiers and evaluate rates to primitives
              if (node.tiers && Array.isArray(node.tiers)) {
                details.volumeTiers = node.tiers.map((tier: any) => ({
                  min: tier.min,
                  max: tier.max,
                  rate: evaluateNode(tier.rate), // Evaluate rate to primitive
                  label: tier.label
                }));
              }
              break;

            case 'lookup':
              // Extract seasonal adjustments or territory premiums and evaluate to primitives
              if (node.reference && node.reference.field) {
                const table = node.table || {};
                const evaluatedTable: any = {};
                
                // Evaluate each table entry to primitive value
                for (const [key, value] of Object.entries(table)) {
                  evaluatedTable[key] = evaluateNode(value);
                }

                if (node.reference.field === 'season') {
                  details.seasonalAdjustments = evaluatedTable;
                } else if (node.reference.field === 'territory') {
                  details.territoryPremiums = evaluatedTable;
                }
              }
              break;

            case 'multiply':
            case 'add':
            case 'subtract':
              // Recursively extract from operands
              if (node.operands && Array.isArray(node.operands)) {
                node.operands.forEach(extractFromNode);
              }
              if (node.left) extractFromNode(node.left);
              if (node.right) extractFromNode(node.right);
              break;

            case 'if':
              // Recursively extract from conditional branches
              if (node.then) extractFromNode(node.then);
              if (node.else) extractFromNode(node.else);
              break;

            case 'premium':
              // Extract premium percentage as base rate if not set
              if (node.percentage && !details.baseRate) {
                details.baseRate = node.percentage / 100; // Convert percentage to decimal
              }
              if (node.base) extractFromNode(node.base);
              break;

            case 'max':
            case 'min':
              // Recursively extract from operands
              if (node.operands && Array.isArray(node.operands)) {
                node.operands.forEach(extractFromNode);
              }
              break;
          }
        };

        // Start extraction from the formula root
        if (def.formula) {
          extractFromNode(def.formula);
        }

        // Generate human-readable calculation formula (matches DynamicRulesEngine logic)
        const hasSeasonalAdj = Object.keys(details.seasonalAdjustments).length > 0;
        const hasTerritoryPrem = Object.keys(details.territoryPremiums).length > 0;
        
        if (details.volumeTiers && details.volumeTiers.length > 0) {
          // Volume-based tiered pricing formula
          let formulaParts: string[] = [];
          details.volumeTiers.forEach((tier: any, idx: number) => {
            const condition = tier.max 
              ? `if (quantity >= ${tier.min.toLocaleString()} && quantity <= ${tier.max.toLocaleString()})` 
              : `if (quantity >= ${tier.min.toLocaleString()})`;
            
            // Always show rate as currency (per-unit dollar amount)
            let rateStr = typeof tier.rate === 'number' 
              ? `$${tier.rate.toFixed(2)}`
              : 'rate';
            
            let formula = `  royalty = quantity × ${rateStr}`;
            if (hasSeasonalAdj) formula += ' × seasonalMultiplier';
            if (hasTerritoryPrem) formula += ' × territoryMultiplier';
            
            formulaParts.push(`${condition} {\n${formula}\n}`);
          });
          details.calculationFormula = formulaParts.join(' else ');
        } else if (details.baseRate !== null) {
          // Simple base rate formula - always show as per-unit currency
          let rateStr = `$${details.baseRate.toFixed(2)}`;
          
          details.calculationFormula = `royalty = quantity × ${rateStr}`;
          if (hasSeasonalAdj) details.calculationFormula += ' × seasonalMultiplier';
          if (hasTerritoryPrem) details.calculationFormula += ' × territoryMultiplier';
        } else {
          details.calculationFormula = 'royalty = formula-based calculation (see rule details)';
        }

        return details;
      };

      // Helper to find matching rule (same logic as DynamicRulesEngine)
      const findMatchingRule = (sale: any) => {
        // Accept ANY royalty/payment rule type (AI returns various types)
        const validRuleTypes = ['tiered', 'tiered_pricing', 'formula_based', 'percentage', 'minimum_guarantee', 
                                'cap', 'fixed_fee', 'fixed_price', 'variable_price', 'per_seat', 'per_unit', 
                                'per_time_period', 'volume_discount', 'license_scope', 'usage_based'];
        const tierRules = rules.filter(r => validRuleTypes.includes(r.ruleType));
        
        // PHASE 1: Try to find specific rules (with product categories)
        for (const rule of tierRules) {
          // Skip global fallback rules (no categories) in first pass
          const hasCategories = Array.isArray(rule.productCategories) && rule.productCategories.length > 0;
          if (!hasCategories) continue;
          
          // Check product categories (enhanced keyword-based matching)
          const categoryMatch = rule.productCategories!.some((cat: string) => {
            const catLower = cat.toLowerCase().trim();
            const saleCategoryLower = (sale.category?.toLowerCase() || '').trim();
            const saleProductLower = (sale.productName?.toLowerCase() || '').trim();
            
            // Guard: require rule category to be non-empty
            if (!catLower) {
              return false;
            }
            
            // PRIORITY 1: Exact substring matching (fast path)
            if (saleProductLower && (saleProductLower.includes(catLower) || catLower.includes(saleProductLower))) {
              return true;
            }
            if (saleCategoryLower && (saleCategoryLower.includes(catLower) || catLower.includes(saleCategoryLower))) {
              return true;
            }
            
            // PRIORITY 2: Keyword-based semantic matching (flexible)
            // Extract keywords from both strings (ignore common words)
            const stopWords = ['and', 'or', 'the', 'a', 'an', 'of', 'in', 'to', 'for', 'with', 'on', 'at', 'by'];
            const extractKeywords = (str: string) => 
              str.split(/[\s,&/\-]+/).filter(w => w.length > 2 && !stopWords.includes(w));
            
            const ruleKeywords = extractKeywords(catLower);
            const saleKeywords = extractKeywords(saleCategoryLower + ' ' + saleProductLower);
            
            // Match if at least 1 significant keyword overlaps
            const hasKeywordMatch = ruleKeywords.some(rk => 
              saleKeywords.some(sk => sk.includes(rk) || rk.includes(sk))
            );
            
            if (hasKeywordMatch) {
              return true;
            }
            
            return false;
          });
          if (!categoryMatch) continue;

          // Check territories (flexible matching for abstract territory names)
          if (rule.territories && rule.territories.length > 0 && !rule.territories.includes('All')) {
            const saleTerritory = (sale.territory || '').toLowerCase().trim();
            
            // Skip territory check for abstract/generic territory names commonly used in sales data
            const abstractTerritories = ['primary', 'secondary', 'tertiary', 'domestic', 'international', 'north', 'south', 'east', 'west'];
            const isAbstractTerritory = saleTerritory.length > 0 && abstractTerritories.some(abs => saleTerritory === abs);
            
            if (!isAbstractTerritory) {
              // Only enforce territory matching for specific territory names (guard against empty territory)
              const territoryMatch = saleTerritory.length > 0 && rule.territories.some((terr: string) =>
                saleTerritory.includes(terr.toLowerCase()) || terr.toLowerCase().includes(saleTerritory)
              );
              if (!territoryMatch) continue;
            }
            // If abstract territory, skip strict matching and allow product match to succeed
          }

          return rule; // Found specific match
        }
        
        // PHASE 2: No specific match found, try global fallback rules (no categories)
        for (const rule of tierRules) {
          const hasCategories = Array.isArray(rule.productCategories) && rule.productCategories.length > 0;
          if (hasCategories) continue; // Skip specific rules in second pass
          
          // Check territories for global rules (flexible matching)
          if (rule.territories && rule.territories.length > 0 && !rule.territories.includes('All')) {
            const saleTerritory = (sale.territory || '').toLowerCase().trim();
            
            // Skip territory check for abstract/generic territory names
            const abstractTerritories = ['primary', 'secondary', 'tertiary', 'domestic', 'international', 'north', 'south', 'east', 'west'];
            const isAbstractTerritory = saleTerritory.length > 0 && abstractTerritories.some(abs => saleTerritory === abs);
            
            if (!isAbstractTerritory) {
              const territoryMatch = saleTerritory.length > 0 && rule.territories.some((terr: string) =>
                saleTerritory.includes(terr.toLowerCase()) || terr.toLowerCase().includes(saleTerritory)
              );
              if (!territoryMatch) continue;
            }
          }
          
          return rule; // Found global fallback
        }
        
        return null; // No match found
      };

      // Create preview samples
      const samples = Array.from(productGroups.values()).map(sale => {
        const matchingRule = findMatchingRule(sale);
        
        if (!matchingRule) {
          return {
            productName: sale.productName,
            category: sale.category,
            sampleUnits: parseInt(sale.quantity || '0'),
            matched: false,
            ruleName: null,
            ruleDescription: null,
            formulaType: null
          };
        }

        // Extract formula type and details from formula definition or legacy fields
        let formulaType = 'Standard rate';
        let ruleDescription = matchingRule.description || '';
        let formulaDetails: any = {};

        // ALWAYS read legacy columns first (these may be manually populated)
        const baseRate = matchingRule.baseRate ? parseFloat(matchingRule.baseRate) : null;
        const volumeTiers = (matchingRule.volumeTiers as any[]) || [];
        const seasonalAdj = (matchingRule.seasonalAdjustments as Record<string, any>) || {};
        const territoryPrem = (matchingRule.territoryPremiums as Record<string, any>) || {};

        if (matchingRule.formulaDefinition) {
          const def = matchingRule.formulaDefinition as any;
          formulaType = def.description || def.name || 'Formula-based';
          
          // Parse the formula definition to extract displayable details
          formulaDetails = parseFormulaDefinition(def);
          
          // MERGE: If FormulaNode didn't extract tiers/adjustments, use legacy columns
          if ((!formulaDetails.volumeTiers || formulaDetails.volumeTiers.length === 0) && volumeTiers.length > 0) {
            formulaDetails.volumeTiers = volumeTiers;
          }
          if ((!formulaDetails.seasonalAdjustments || Object.keys(formulaDetails.seasonalAdjustments).length === 0) && Object.keys(seasonalAdj).length > 0) {
            formulaDetails.seasonalAdjustments = seasonalAdj;
          }
          if ((!formulaDetails.territoryPremiums || Object.keys(formulaDetails.territoryPremiums).length === 0) && Object.keys(territoryPrem).length > 0) {
            formulaDetails.territoryPremiums = territoryPrem;
          }
          if (!formulaDetails.baseRate && baseRate !== null) {
            formulaDetails.baseRate = baseRate;
          }
          
          // Regenerate calculation formula with merged data
          const hasSeasonalAdj = Object.keys(formulaDetails.seasonalAdjustments || {}).length > 0;
          const hasTerritoryPrem = Object.keys(formulaDetails.territoryPremiums || {}).length > 0;
          
          if (formulaDetails.volumeTiers && formulaDetails.volumeTiers.length > 0) {
            // Volume-based tiered pricing formula
            let formulaParts: string[] = [];
            formulaDetails.volumeTiers.forEach((tier: any, idx: number) => {
              const condition = tier.max 
                ? `if (quantity >= ${tier.min.toLocaleString()} && quantity <= ${tier.max.toLocaleString()})` 
                : `if (quantity >= ${tier.min.toLocaleString()})`;
              
              let rateStr = typeof tier.rate === 'number' 
                ? `$${tier.rate.toFixed(2)}`
                : 'rate';
              
              let formula = `  royalty = quantity × ${rateStr}`;
              if (hasSeasonalAdj) formula += ' × seasonalMultiplier';
              if (hasTerritoryPrem) formula += ' × territoryMultiplier';
              
              formulaParts.push(`${condition} {\n${formula}\n}`);
            });
            formulaDetails.calculationFormula = formulaParts.join(' else ');
          } else if (formulaDetails.baseRate !== null) {
            let rateStr = `$${formulaDetails.baseRate.toFixed(2)}`;
            formulaDetails.calculationFormula = `royalty = quantity × ${rateStr}`;
            if (hasSeasonalAdj) formulaDetails.calculationFormula += ' × seasonalMultiplier';
            if (hasTerritoryPrem) formulaDetails.calculationFormula += ' × territoryMultiplier';
          }
          
          // Override formula type based on merged content
          if (formulaDetails.volumeTiers && formulaDetails.volumeTiers.length > 0) {
            formulaType = 'Volume-based tiered pricing';
          } else if (Object.keys(formulaDetails.seasonalAdjustments || {}).length > 0) {
            formulaType = 'Seasonal adjustments';
          } else if (Object.keys(formulaDetails.territoryPremiums || {}).length > 0) {
            formulaType = 'Territory premiums';
          }
        } else {
          // Pure legacy format - no FormulaNode
          if (volumeTiers.length > 0) {
            formulaType = 'Volume-based tiered pricing';
          } else if (Object.keys(seasonalAdj).length > 0) {
            formulaType = 'Seasonal adjustments';
          } else if (Object.keys(territoryPrem).length > 0) {
            formulaType = 'Territory premiums';
          }
          
          // Generate calculation formula for legacy rules
          let calculationFormula = '';
          const hasSeasonalAdj = Object.keys(seasonalAdj).length > 0;
          const hasTerritoryPrem = Object.keys(territoryPrem).length > 0;
          
          if (volumeTiers.length > 0) {
            let formulaParts: string[] = [];
            volumeTiers.forEach((tier: any, idx: number) => {
              const condition = tier.max 
                ? `if (quantity >= ${tier.min.toLocaleString()} && quantity <= ${tier.max.toLocaleString()})` 
                : `if (quantity >= ${tier.min.toLocaleString()})`;
              
              let rateStr = typeof tier.rate === 'number' 
                ? `$${tier.rate.toFixed(2)}`
                : 'rate';
              
              let formula = `  royalty = quantity × ${rateStr}`;
              if (hasSeasonalAdj) formula += ' × seasonalMultiplier';
              if (hasTerritoryPrem) formula += ' × territoryMultiplier';
              
              formulaParts.push(`${condition} {\n${formula}\n}`);
            });
            calculationFormula = formulaParts.join(' else ');
          } else if (baseRate !== null) {
            let rateStr = `$${baseRate.toFixed(2)}`;
            calculationFormula = `royalty = quantity × ${rateStr}`;
            if (hasSeasonalAdj) calculationFormula += ' × seasonalMultiplier';
            if (hasTerritoryPrem) calculationFormula += ' × territoryMultiplier';
          } else {
            calculationFormula = 'royalty = quantity × rate';
          }
          
          formulaDetails = {
            type: 'legacy',
            baseRate,
            volumeTiers,
            seasonalAdjustments: seasonalAdj,
            territoryPremiums: territoryPrem,
            calculationFormula
          };
        }

        return {
          productName: sale.productName,
          category: sale.category,
          sampleUnits: parseInt(sale.quantity || '0'),
          matched: true,
          ruleName: matchingRule.ruleName,
          ruleDescription,
          formulaType,
          formulaDetails,
          priority: matchingRule.priority,
          confidence: matchingRule.confidence,
          sourceSection: matchingRule.sourceSection || null,
          sourceText: matchingRule.sourceText ? matchingRule.sourceText.substring(0, 150) + '...' : null
        };
      });

      const unmatchedCount = samples.filter(s => !s.matched).length;

      res.json({
        samples: samples.slice(0, 10), // Top 10 product samples
        totalProducts: productGroups.size,
        totalRules: rules.length,
        unmatchedSales: unmatchedCount
      });

    } catch (error: any) {
      console.error('Error generating formula preview:', error);
      res.status(500).json({ message: error.message || 'Failed to generate preview' });
    }
  });

  // Calculate royalties for a contract using dynamic rules engine
  app.post('/api/contracts/:id/calculate-royalties', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { periodStart, periodEnd, name } = req.body;
      
      // Get sales data for this contract
      const { dynamicRulesEngine } = await import('./services/dynamicRulesEngine.js');
      const allSales = await storage.getSalesDataByContract(contractId);
      
      // Filter by period if provided
      let filteredSales = allSales;
      if (periodStart || periodEnd) {
        filteredSales = allSales.filter(sale => {
          const saleDate = new Date(sale.transactionDate);
          if (periodStart && saleDate < new Date(periodStart)) return false;
          if (periodEnd && saleDate > new Date(periodEnd)) return false;
          return true;
        });
      }
      
      if (filteredSales.length === 0) {
        return res.status(400).json({ 
          message: 'No sales data found for the selected period' 
        });
      }

      // Convert sales to format expected by dynamic engine
      const salesItems = filteredSales.map(sale => ({
        id: sale.id,
        productName: sale.productName || 'Unknown',
        category: sale.category || '',
        territory: sale.territory || 'Primary Territory',
        quantity: parseFloat(sale.quantity || '0'),
        transactionDate: new Date(sale.transactionDate),
        grossAmount: parseFloat(sale.grossAmount || '0')
      }));

      // Use dynamic rules engine to calculate royalties
      const result = await dynamicRulesEngine.calculateRoyalty(contractId, salesItems);
      
      // Calculate total sales amount
      const totalSalesAmount = filteredSales.reduce((sum, sale) => sum + parseFloat(sale.grossAmount), 0);
      
      // Create lookup map for sales data
      const salesMap = new Map(filteredSales.map(sale => [sale.id, sale]));
      
      // Prepare enhanced breakdown with rule details and actual sale amounts
      const breakdown = result.breakdown.map(item => {
        const sale = salesMap.get(item.saleId);
        return {
          saleId: item.saleId,
          productName: item.productName,
          category: item.category,
          territory: item.territory,
          quantity: item.quantity,
          saleAmount: sale ? parseFloat(sale.grossAmount) : 0, // Use actual gross amount
          royaltyAmount: item.calculatedRoyalty.toString(),
          ruleApplied: item.ruleApplied,
          explanation: item.explanation,
          tierRate: item.tierRate,
          seasonalMultiplier: item.seasonalMultiplier,
          territoryMultiplier: item.territoryMultiplier
        };
      });
      
      // Create calculation record with enhanced data
      const calculation = await storage.createContractRoyaltyCalculation({
        contractId,
        name: name || `Calculation ${new Date().toLocaleDateString()}`,
        periodStart: periodStart ? new Date(periodStart) : null,
        periodEnd: periodEnd ? new Date(periodEnd) : null,
        totalSalesAmount: totalSalesAmount.toString(),
        totalRoyalty: result.finalRoyalty.toString(),
        breakdown: JSON.stringify(breakdown),
        chartData: JSON.stringify({
          minimumGuarantee: result.minimumGuarantee,
          calculatedRoyalty: result.totalRoyalty,
          rulesApplied: result.rulesApplied
        }),
        calculatedBy: req.user.id,
        salesCount: filteredSales.length,
      });

      res.json({
        success: true,
        calculation: {
          ...calculation,
          minimumGuarantee: result.minimumGuarantee,
          calculatedRoyalty: result.totalRoyalty,
          totalRoyalty: result.finalRoyalty.toString(),
          rulesApplied: result.rulesApplied
        },
        message: `Calculated ${filteredSales.length} sales transactions`,
      });

    } catch (error: any) {
      console.error('Error calculating royalties:', error);
      res.status(500).json({ 
        message: error.message || 'Failed to calculate royalties' 
      });
    }
  });

  // Get royalty calculations for a contract
  app.get('/api/contracts/:id/royalty-calculations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const context = req.user?.activeContext;
      const calculations = await storage.getContractRoyaltyCalculations(contractId, context);
      
      res.json({
        calculations,
        total: calculations.length,
      });
    } catch (error: any) {
      console.error('Error fetching royalty calculations:', error);
      res.status(500).json({ message: error.message });
    }
  });

  // Delete all royalty calculations for a contract
  app.delete('/api/contracts/:id/royalty-calculations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user?.id;
      
      // Verify contract exists and check ownership
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }
      
      // Check permissions: admin, owner role, or contract uploader can delete calculations
      const user = await storage.getUser(userId);
      const isOwner = contract.uploadedBy === userId;
      const isAdmin = user?.role === 'admin' || user?.role === 'owner';
      const canDelete = isOwner || isAdmin;
      
      if (!canDelete) {
        return res.status(403).json({ message: 'You do not have permission to delete calculations for this contract' });
      }
      
      await storage.deleteAllCalculationsForContract(contractId);
      
      await createAuditLog(req, 'delete_royalty_calculations', 'contract', contractId, {
        action: 'bulk_delete_calculations',
        contractName: contract.originalName
      });
      
      res.json({ message: 'All royalty calculations deleted successfully' });
    } catch (error: any) {
      console.error('Error deleting royalty calculations:', error);
      res.status(500).json({ message: error.message || 'Failed to delete calculations' });
    }
  });

  // Delete a single royalty calculation
  app.delete('/api/royalty-calculations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      const userId = req.user?.id;
      
      // Get the calculation to find its contract
      const calculation = await storage.getContractRoyaltyCalculation(calculationId);
      if (!calculation) {
        return res.status(404).json({ message: 'Calculation not found' });
      }
      
      // Verify contract exists and check ownership
      const contract = await storage.getContract(calculation.contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }
      
      // Check permissions: admin, owner role, or contract uploader can delete calculations
      const user = await storage.getUser(userId);
      const isOwner = contract.uploadedBy === userId;
      const isAdmin = user?.role === 'admin' || user?.role === 'owner';
      const canDelete = isOwner || isAdmin;
      
      if (!canDelete) {
        return res.status(403).json({ message: 'You do not have permission to delete this calculation' });
      }
      
      await storage.deleteContractRoyaltyCalculation(calculationId);
      
      await createAuditLog(req, 'delete_royalty_calculation', 'calculation', calculationId, {
        calculationName: calculation.name,
        contractId: calculation.contractId,
        contractName: contract.originalName
      });
      
      res.json({ message: 'Calculation deleted successfully' });
    } catch (error: any) {
      console.error('Error deleting calculation:', error);
      res.status(500).json({ message: error.message || 'Failed to delete calculation' });
    }
  });

  // Get all royalty calculations across all contracts (for Calculations page)
  app.get('/api/calculations/all', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Build organizational context for filtering
      const context = {
        activeContext: req.user?.activeContext,
        globalRole: req.user?.role || 'viewer',
        userId: req.user?.id,
        isSystemAdmin: req.user?.isSystemAdmin === true
      };
      
      // Get all contracts for the user's organization (with context filtering)
      const contractsResult = await storage.getContracts(undefined, undefined, undefined, context);
      const contracts = contractsResult.contracts || [];
      
      // Fetch calculations for each contract (with context filtering)
      const allCalculationsPromises = contracts.map(async (contract) => {
        const calculations = await storage.getContractRoyaltyCalculations(contract.id, context);
        return calculations.map(calc => ({
          ...calc,
          contractName: contract.originalName,
          itemsProcessed: calc.salesCount || 0,
        }));
      });
      
      const calculationsByContract = await Promise.all(allCalculationsPromises);
      const allCalculations = calculationsByContract.flat();
      
      // Sort by creation date (newest first)
      allCalculations.sort((a, b) => {
        const dateA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const dateB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return dateB - dateA;
      });
      
      res.json({
        calculations: allCalculations,
        total: allCalculations.length,
      });
    } catch (error: any) {
      console.error('Error fetching all calculations:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch calculations' });
    }
  });

  // Get detailed calculation data (for Calculation Details dialog)
  app.get('/api/calculations/:id/details', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      const context = req.user?.activeContext;
      
      // Get calculation
      const calculation = await storage.getContractRoyaltyCalculation(calculationId, context);
      if (!calculation) {
        return res.status(404).json({ message: 'Calculation not found' });
      }
      
      // Get contract
      const contract = await storage.getContract(calculation.contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }
      
      // Get rules for the contract
      const rules = await storage.getRoyaltyRulesByContract(calculation.contractId);
      
      // Parse breakdown data
      const breakdown = typeof calculation.breakdown === 'string' 
        ? JSON.parse(calculation.breakdown) 
        : calculation.breakdown || [];
      
      // Transform breakdown into line items
      const lineItems = Array.isArray(breakdown) ? breakdown.map((item: any) => ({
        productName: item.productName || item.product || 'Unknown',
        quantity: item.quantity || 0,
        salesAmount: item.saleAmount || item.grossAmount || 0,
        ruleName: item.ruleApplied || 'Default',
        royaltyAmount: parseFloat(item.royaltyAmount) || item.royaltyOwed || item.royalty || 0,
      })) : [];
      
      // Get applied rules info
      const appliedRules = rules.map(rule => ({
        ruleName: rule.ruleName,
        description: rule.description,
        ruleType: rule.ruleType,
        rate: rule.rate,
      }));
      
      // Construct response
      const detailedData = {
        id: calculation.id,
        name: calculation.name,
        contractName: contract.originalName,
        contractId: contract.id,
        totalRoyalty: calculation.totalRoyalty,
        itemsProcessed: calculation.salesCount || 0,
        createdAt: calculation.createdAt,
        periodStart: calculation.periodStart,
        periodEnd: calculation.periodEnd,
        lineItems,
        appliedRules,
        uploadedFile: null
      };
      
      res.json(detailedData);
    } catch (error: any) {
      console.error('Error fetching calculation details:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch calculation details' });
    }
  });

  // Generate PDF invoice (detailed format)
  app.get('/api/royalty-calculations/:id/invoice/detailed', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      
      // Get calculation data
      const calculation = await storage.getContractRoyaltyCalculation(calculationId);
      if (!calculation) {
        return res.status(404).json({ message: 'Calculation not found' });
      }
      
      // Get contract data
      const contract = await storage.getContract(calculation.contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }

      // Breakdown data is already an object from the database
      const breakdown = typeof calculation.breakdown === 'string' 
        ? JSON.parse(calculation.breakdown) 
        : calculation.breakdown;
      const chartData = calculation.chartData 
        ? (typeof calculation.chartData === 'string' ? JSON.parse(calculation.chartData) : calculation.chartData)
        : {};
      
      // Extract vendor info from keyTerms if available
      const keyTerms = contract.analysis?.keyTerms as any || {};
      const vendorName = keyTerms?.licensor || keyTerms?.vendor || 'Vendor Name';
      const licensee = keyTerms?.licensee || keyTerms?.client || 'Licensee Name';
      const paymentTerms = keyTerms?.paymentTerms || 'Net 30 days';
      
      // Prepare invoice data
      const invoiceData = {
        calculationId: calculation.id,
        calculationName: calculation.name,
        contractName: contract.originalName,
        vendorName,
        licensee,
        calculationDate: calculation.createdAt || new Date(),
        periodStart: calculation.periodStart || new Date(),
        periodEnd: calculation.periodEnd || new Date(),
        totalRoyalty: parseFloat(calculation.totalRoyalty || '0'),
        minimumGuarantee: chartData.minimumGuarantee,
        finalRoyalty: parseFloat(calculation.totalRoyalty || '0'),
        breakdown: breakdown.map((item: any) => ({
          productName: item.productName,
          category: item.category,
          quantity: item.quantity,
          grossAmount: item.saleAmount || 0,
          ruleApplied: item.ruleApplied,
          calculatedRoyalty: parseFloat(item.royaltyAmount || '0'),
          explanation: item.explanation
        })),
        currency: 'USD',
        paymentTerms
      };
      
      // Generate PDF
      const pdfBuffer = await PDFInvoiceService.generateDetailedInvoice(invoiceData);
      
      // Set response headers for PDF download
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="invoice-detailed-${calculation.name.replace(/[^a-zA-Z0-9]/g, '-')}.pdf"`);
      res.send(pdfBuffer);
      
    } catch (error: any) {
      console.error('Error generating detailed invoice:', error);
      res.status(500).json({ message: error.message || 'Failed to generate invoice' });
    }
  });

  // Generate PDF invoice (summary format)
  app.get('/api/royalty-calculations/:id/invoice/summary', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      
      // Get calculation data
      const calculation = await storage.getContractRoyaltyCalculation(calculationId);
      if (!calculation) {
        return res.status(404).json({ message: 'Calculation not found' });
      }
      
      // Get contract data
      const contract = await storage.getContract(calculation.contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }

      // Breakdown data is already an object from the database
      const breakdown = typeof calculation.breakdown === 'string' 
        ? JSON.parse(calculation.breakdown) 
        : calculation.breakdown;
      const chartData = calculation.chartData 
        ? (typeof calculation.chartData === 'string' ? JSON.parse(calculation.chartData) : calculation.chartData)
        : {};
      
      // Extract vendor info from keyTerms if available
      const keyTerms = contract.analysis?.keyTerms as any || {};
      const vendorName = keyTerms?.licensor || keyTerms?.vendor || 'Vendor Name';
      const licensee = keyTerms?.licensee || keyTerms?.client || 'Licensee Name';
      const paymentTerms = keyTerms?.paymentTerms || 'Net 30 days';
      
      // Prepare invoice data
      const invoiceData = {
        calculationId: calculation.id,
        calculationName: calculation.name,
        contractName: contract.originalName,
        vendorName,
        licensee,
        calculationDate: calculation.createdAt || new Date(),
        periodStart: calculation.periodStart || new Date(),
        periodEnd: calculation.periodEnd || new Date(),
        totalRoyalty: parseFloat(calculation.totalRoyalty || '0'),
        minimumGuarantee: chartData.minimumGuarantee,
        finalRoyalty: parseFloat(calculation.totalRoyalty || '0'),
        breakdown: breakdown.map((item: any) => ({
          productName: item.productName,
          category: item.category,
          quantity: item.quantity,
          grossAmount: item.saleAmount || 0,
          ruleApplied: item.ruleApplied,
          calculatedRoyalty: parseFloat(item.royaltyAmount || '0'),
          explanation: item.explanation
        })),
        currency: 'USD',
        paymentTerms
      };
      
      // Generate PDF
      const pdfBuffer = await PDFInvoiceService.generateSummaryInvoice(invoiceData);
      
      // Set response headers for PDF download
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="invoice-summary-${calculation.name.replace(/[^a-zA-Z0-9]/g, '-')}.pdf"`);
      res.send(pdfBuffer);
      
    } catch (error: any) {
      console.error('Error generating summary invoice:', error);
      res.status(500).json({ message: error.message || 'Failed to generate invoice' });
    }
  });

  // ======================
  // CALCULATION REPORT ENDPOINTS (Multi-Dimensional Views)
  // ======================
  
  // Get calculation report with dynamic dimensions
  app.get('/api/calculations/:id/report', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      const dimensionKey = req.query.dimension as string | undefined;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const report = await calculationReportService.getCalculationReport(calculationId, dimensionKey);
      
      res.json(report);
    } catch (error: any) {
      console.error('Error fetching calculation report:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch calculation report' });
    }
  });
  
  // Get available dimensions for a contract (based on ERP mappings)
  app.get('/api/contracts/:id/dimensions', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const dimensions = await calculationReportService.getAvailableDimensions(contractId);
      
      res.json({ dimensions });
    } catch (error: any) {
      console.error('Error fetching dimensions:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch dimensions' });
    }
  });
  
  // Get aggregated calculation data by dimension
  app.get('/api/calculations/:id/aggregate/:dimensionKey', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      const dimensionKey = req.params.dimensionKey;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const data = await calculationReportService.getAggregatedByDimension(calculationId, dimensionKey);
      
      res.json({ dimensionKey, data });
    } catch (error: any) {
      console.error('Error fetching aggregated data:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch aggregated data' });
    }
  });
  
  // Get calculation summary report
  app.get('/api/calculations/:id/summary-report', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const summary = await calculationReportService.getSummaryReport(calculationId);
      
      res.json(summary);
    } catch (error: any) {
      console.error('Error fetching summary report:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch summary report' });
    }
  });
  
  // Get detailed line items for a calculation
  app.get('/api/calculations/:id/line-items', isAuthenticated, async (req: any, res: Response) => {
    try {
      const calculationId = req.params.id;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const lineItems = await calculationReportService.getDetailedLineItems(calculationId);
      
      res.json({ lineItems });
    } catch (error: any) {
      console.error('Error fetching line items:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch line items' });
    }
  });

  // Export dimension report as PDF
  app.get('/api/calculations/:id/report/:dimensionKey/pdf', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id: calculationId, dimensionKey } = req.params;
      
      const { calculationReportService } = await import('./services/calculationReportService');
      const { PDFInvoiceService } = await import('./services/pdfInvoiceService');
      
      const report = await calculationReportService.getCalculationReport(calculationId, dimensionKey);
      const aggregatedData = dimensionKey !== 'summary' && dimensionKey !== 'detail'
        ? await calculationReportService.getAggregatedByDimension(calculationId, dimensionKey)
        : [];
      
      const contract = await storage.getContract(report.contractId);
      const dimension = report.availableDimensions.find(d => d.dimensionKey === dimensionKey);
      
      const pdfBuffer = await PDFInvoiceService.generateDimensionReport({
        calculationName: report.calculationName,
        contractName: contract?.displayName || 'Contract',
        dimensionName: dimension?.displayName || dimensionKey,
        periodStart: report.periodStart,
        periodEnd: report.periodEnd,
        totalSalesAmount: report.totalSalesAmount,
        totalFee: report.totalFee,
        transactionCount: report.transactionCount,
        aggregatedData
      });
      
      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', `attachment; filename="report-${dimensionKey}-${calculationId.slice(0, 8)}.pdf"`);
      res.send(pdfBuffer);
    } catch (error: any) {
      console.error('Error generating PDF report:', error);
      res.status(500).json({ message: error.message || 'Failed to generate PDF' });
    }
  });

  // Delete a contract
  app.delete('/api/contracts/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user.id;

      // Check if contract exists and user has permission
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ message: 'Contract not found' });
      }

      // Check permissions: admin, owner, or uploader can delete
      const user = await storage.getUser(userId);
      const canDelete = user?.role === 'admin' || user?.role === 'owner' || contract.uploadedBy === userId;

      if (!canDelete) {
        return res.status(403).json({ message: 'You do not have permission to delete this contract' });
      }

      // Delete the contract
      await storage.deleteContract(contractId);

      // Create audit log
      await createAuditLog(req, 'delete_contract', 'contract', contractId, {
        fileName: contract.fileName,
      });

      res.json({ 
        success: true, 
        message: 'Contract deleted successfully' 
      });
    } catch (error: any) {
      console.error('Error deleting contract:', error);
      res.status(500).json({ 
        message: error.message || 'Failed to delete contract' 
      });
    }
  });

  // Upload sales data (with ERP semantic matching support)
  app.post('/api/sales/upload', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      const contractId = req.body.contractId;
      if (!contractId) {
        return res.status(400).json({ error: 'Contract ID is required' });
      }

      // Fetch contract to check ERP matching settings
      const contract = await storage.getContract(contractId);
      if (!contract) {
        return res.status(404).json({ error: 'Contract not found' });
      }

      // Parse file
      const fileBuffer = fs.readFileSync(req.file.path);
      const { SalesDataParser } = await import('./services/salesDataParser.js');
      const parseResult = await SalesDataParser.parseFile(fileBuffer, req.file.originalname);

      const validRows = parseResult.rows.filter(r => r.validationStatus === 'valid');
      
      let matchedRecords = 0;
      let unmatchedRecords = 0;
      let totalConfidence = 0;

      // Check if ERP semantic matching is enabled for this contract
      if (contract.useErpMatching) {
        // Import HuggingFace embedding service
        const { HuggingFaceEmbeddingService } = await import('./services/huggingFaceEmbedding');
        
        // Process each sales record with semantic matching
        for (const row of validRows) {
          const salesData = row.rowData as any;
          
          // Build a text representation for embedding (product-focused)
          const searchText = [
            salesData.productCode,
            salesData.productName,
            salesData.category,
            salesData.territory
          ].filter(Boolean).join(' ');
          
          // Generate embedding for the sales record
          const embedding = await HuggingFaceEmbeddingService.generateEmbedding(searchText);
          
          // Search for semantic matches in imported ERP records
          const matches = await storage.searchSemanticMatches(
            contractId,
            embedding,
            5, // Top 5 matches
            0.3 // Minimum similarity threshold (30%)
          );
          
          // Use the best match confidence, or 0 if no matches
          const matchConfidence = matches.length > 0 ? matches[0].similarity : 0;
          
          // Store sales data with match confidence
          await storage.createSalesData({
            matchedContractId: contractId,
            transactionId: salesData.transactionId || `TXN-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
            transactionDate: salesData.transactionDate ? new Date(salesData.transactionDate) : new Date(),
            productCode: salesData.productCode,
            productName: salesData.productName,
            category: salesData.category,
            territory: salesData.territory,
            currency: salesData.currency || 'USD',
            grossAmount: String(parseFloat(salesData.grossAmount) || 0),
            netAmount: String(parseFloat(salesData.netAmount) || 0),
            quantity: String(parseInt(salesData.quantity) || 0),
            unitPrice: String(parseFloat(salesData.unitPrice) || 0),
            matchConfidence: String(matchConfidence)
          });
          
          // Track matching statistics
          if (matchConfidence >= 0.7) {
            matchedRecords++;
          } else {
            unmatchedRecords++;
          }
          totalConfidence += matchConfidence;
        }
      } else {
        // Traditional upload without ERP matching
        for (const row of validRows) {
          const salesData = row.rowData as any;
          await storage.createSalesData({
            matchedContractId: contractId,
            transactionId: salesData.transactionId || `TXN-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
            transactionDate: salesData.transactionDate ? new Date(salesData.transactionDate) : new Date(),
            productCode: salesData.productCode,
            productName: salesData.productName,
            category: salesData.category,
            territory: salesData.territory,
            currency: salesData.currency || 'USD',
            grossAmount: String(parseFloat(salesData.grossAmount) || 0),
            netAmount: String(parseFloat(salesData.netAmount) || 0),
            quantity: String(parseInt(salesData.quantity) || 0),
            unitPrice: String(parseFloat(salesData.unitPrice) || 0)
          });
        }
      }

      // Clean up uploaded file
      fs.unlinkSync(req.file.path);

      // Create audit log
      await createAuditLog(req, 'upload_sales_data', 'sales', undefined, {
        fileName: req.file.originalname,
        rowsImported: validRows.length,
        contractId,
        erpMatchingEnabled: contract.useErpMatching,
        ...(contract.useErpMatching && {
          matchedRecords,
          unmatchedRecords,
          avgConfidence: validRows.length > 0 ? (totalConfidence / validRows.length).toFixed(2) : 0
        })
      });

      const message = contract.useErpMatching
        ? `${validRows.length} sales transactions imported with ERP semantic matching (${matchedRecords} matched, ${unmatchedRecords} unmatched)`
        : `${validRows.length} sales transactions imported successfully`;

      res.json({
        success: true,
        validRows: validRows.length,
        totalRows: parseResult.rows.length,
        errors: parseResult.rows.filter(r => r.validationStatus === 'invalid').length,
        erpMatchingEnabled: contract.useErpMatching,
        ...(contract.useErpMatching && {
          matchedRecords,
          unmatchedRecords,
          avgConfidence: validRows.length > 0 ? (totalConfidence / validRows.length).toFixed(2) : 0
        }),
        message
      });

    } catch (error: any) {
      console.error('Sales upload error:', error);
      res.status(500).json({ error: error.message || 'Failed to upload sales data' });
    }
  });

  // Download sample sales data
  app.get('/api/sales/sample-data', isAuthenticated, (req: any, res: Response) => {
    // Sample data matching Plant Variety License & Royalty Agreement for Trees & Shrubs
    const sampleData = [
      { transactionDate: '2024-03-15', transactionId: 'TXN-2024-001', productCode: 'MAPLE-001', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Primary', containerSize: '1-gallon', season: 'Spring', currency: 'USD', grossAmount: 30000, netAmount: 27000, quantity: 6200, unitPrice: 4.84 },
      { transactionDate: '2024-03-20', transactionId: 'TXN-2024-002', productCode: 'MAPLE-002', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Primary', containerSize: '5-gallon', season: 'Off-Season', currency: 'USD', grossAmount: 25000, netAmount: 22500, quantity: 1100, unitPrice: 22.73 },
      { transactionDate: '2024-10-05', transactionId: 'TXN-2024-003', productCode: 'JUNIPER-001', productName: 'Golden Spire Juniper', category: 'Ornamental Shrubs', territory: 'Secondary', containerSize: '3-gallon', season: 'Fall', currency: 'USD', grossAmount: 28000, netAmount: 25200, quantity: 1800, unitPrice: 15.56 },
      { transactionDate: '2024-04-12', transactionId: 'TXN-2024-004', productCode: 'ROSE-001', productName: 'Pacific Sunset Rose', category: 'Flowering Shrubs', territory: 'Primary', containerSize: '6-inch', season: 'Spring', currency: 'USD', grossAmount: 12000, netAmount: 10800, quantity: 3000, unitPrice: 4 },
      { transactionDate: '2024-09-18', transactionId: 'TXN-2024-005', productCode: 'HOSTA-001', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Primary', containerSize: '2-gallon', season: 'Fall', isOrganic: true, currency: 'USD', grossAmount: 18000, netAmount: 16200, quantity: 900, unitPrice: 20 },
      { transactionDate: '2024-03-25', transactionId: 'TXN-2024-006', productCode: 'HYDRANGEA-001', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Primary', containerSize: 'Mixed', season: 'Spring', currency: 'USD', grossAmount: 120000, netAmount: 108000, quantity: 20000, unitPrice: 6 },
      { transactionDate: '2024-12-15', transactionId: 'TXN-2024-007', productCode: 'ROSE-002', productName: 'Pacific Sunset Rose', category: 'Flowering Shrubs', territory: 'Primary', containerSize: '1-gallon', season: 'Holiday', currency: 'USD', grossAmount: 5000, netAmount: 4500, quantity: 250, unitPrice: 20 },
      { transactionDate: '2024-05-10', transactionId: 'TXN-2024-008', productCode: 'MAPLE-003', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Secondary', containerSize: '1-gallon', season: 'Spring', currency: 'USD', grossAmount: 15000, netAmount: 13500, quantity: 3000, unitPrice: 5 },
      { transactionDate: '2024-06-15', transactionId: 'TXN-2024-009', productCode: 'JUNIPER-002', productName: 'Golden Spire Juniper', category: 'Ornamental Shrubs', territory: 'Primary', containerSize: '5-gallon', season: 'Summer', currency: 'USD', grossAmount: 22000, netAmount: 19800, quantity: 800, unitPrice: 27.5 },
      { transactionDate: '2024-07-20', transactionId: 'TXN-2024-010', productCode: 'HYDRANGEA-002', productName: 'Cascade Blue Hydrangea', category: 'Flowering Shrubs', territory: 'Secondary', containerSize: '3-gallon', season: 'Summer', currency: 'USD', grossAmount: 18000, netAmount: 16200, quantity: 1200, unitPrice: 15 },
      { transactionDate: '2024-08-10', transactionId: 'TXN-2024-011', productCode: 'HOSTA-002', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Primary', containerSize: '1-gallon', season: 'Fall', currency: 'USD', grossAmount: 12000, netAmount: 10800, quantity: 1500, unitPrice: 8 },
      { transactionDate: '2024-09-05', transactionId: 'TXN-2024-012', productCode: 'ROSE-003', productName: 'Pacific Sunset Rose', category: 'Flowering Shrubs', territory: 'Secondary', containerSize: '2-gallon', season: 'Fall', currency: 'USD', grossAmount: 16000, netAmount: 14400, quantity: 1000, unitPrice: 16 },
      { transactionDate: '2024-10-15', transactionId: 'TXN-2024-013', productCode: 'MAPLE-004', productName: 'Aurora Flame Maple', category: 'Ornamental Trees', territory: 'Primary', containerSize: '3-gallon', season: 'Fall', currency: 'USD', grossAmount: 35000, netAmount: 31500, quantity: 2000, unitPrice: 17.5 },
      { transactionDate: '2024-11-20', transactionId: 'TXN-2024-014', productCode: 'JUNIPER-003', productName: 'Golden Spire Juniper', category: 'Ornamental Shrubs', territory: 'Primary', containerSize: '1-gallon', season: 'Off-Season', currency: 'USD', grossAmount: 8000, netAmount: 7200, quantity: 1600, unitPrice: 5 },
      { transactionDate: '2024-12-10', transactionId: 'TXN-2024-015', productCode: 'HOSTA-003', productName: 'Emerald Crown Hosta', category: 'Perennials', territory: 'Secondary', containerSize: '3-gallon', season: 'Holiday', isOrganic: true, currency: 'USD', grossAmount: 20000, netAmount: 18000, quantity: 750, unitPrice: 26.67 }
    ];

    // Convert to CSV with all relevant fields
    const headers = 'transactionDate,transactionId,productCode,productName,category,territory,containerSize,season,isOrganic,currency,grossAmount,netAmount,quantity,unitPrice\n';
    const csv = headers + sampleData.map(row => 
      `${row.transactionDate},${row.transactionId},${row.productCode},"${row.productName}",${row.category},${row.territory},${row.containerSize || ''},${row.season || ''},${row.isOrganic || ''},${row.currency},${row.grossAmount},${row.netAmount},${row.quantity},${row.unitPrice}`
    ).join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="sample_sales_data.csv"');
    res.send(csv);
  });

  // RAG Q&A endpoint - Ask questions about contracts
  app.post('/api/rag/ask', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { question, contractId } = req.body;
      
      if (!question || typeof question !== 'string') {
        return res.status(400).json({ message: 'Question is required and must be a string' });
      }
      
      console.log(`🤖 [RAG API] Question: "${question}"${contractId ? ` (contract: ${contractId})` : ''}`);
      
      const result = await RAGService.answerQuestion(question, contractId);
      
      await createAuditLog(req, 'rag_query', 'contract', contractId || 'all', {
        question,
        confidence: result.confidence,
        sourcesCount: result.sources.length
      });
      
      res.json(result);
    } catch (error: any) {
      console.error('RAG query error:', error);
      res.status(500).json({ message: error.message || 'Failed to process question' });
    }
  });

  // Knowledge Base endpoint - Get system knowledge entries
  app.get('/api/knowledge-base', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { systemKnowledgeBase } = await import('./data/systemKnowledgeBase');
      
      const entries = systemKnowledgeBase.map(entry => ({
        id: entry.id,
        category: entry.category,
        title: entry.title,
        content: entry.content,
        createdAt: new Date().toISOString(),
      }));
      
      res.json(entries);
    } catch (error: any) {
      console.error('Knowledge base fetch error:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch knowledge base' });
    }
  });

  // RAG Stats endpoint - Get embedding statistics
  app.get('/api/rag/stats', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Get total embeddings count
      const totalEmbeddingsResult = await db
        .select({ count: count() })
        .from(contractEmbeddings);
      const totalEmbeddings = totalEmbeddingsResult[0]?.count || 0;

      // Get embeddings grouped by type
      const embeddingsByTypeResult = await db
        .select({
          type: contractEmbeddings.embeddingType,
          count: count()
        })
        .from(contractEmbeddings)
        .groupBy(contractEmbeddings.embeddingType);

      // Get distinct contracts with embeddings
      const contractsWithEmbeddings = await db
        .selectDistinct({ contractId: contractEmbeddings.contractId })
        .from(contractEmbeddings);

      // Get recent embeddings with contract info
      const recentEmbeddingsData = await db
        .select({
          id: contractEmbeddings.id,
          contractId: contractEmbeddings.contractId,
          contractName: contracts.originalName,
          embeddingType: contractEmbeddings.embeddingType,
          sourceText: contractEmbeddings.sourceText,
          createdAt: contractEmbeddings.createdAt,
        })
        .from(contractEmbeddings)
        .innerJoin(contracts, eq(contractEmbeddings.contractId, contracts.id))
        .orderBy(desc(contractEmbeddings.createdAt))
        .limit(20);

      // Calculate average chunk size
      const avgChunkSize = recentEmbeddingsData.length > 0
        ? recentEmbeddingsData.reduce((sum, emb) => sum + (emb.sourceText?.length || 0), 0) / recentEmbeddingsData.length
        : 0;

      res.json({
        totalEmbeddings,
        totalChunks: totalEmbeddings,
        totalContracts: contractsWithEmbeddings.length,
        avgChunkSize: Math.round(avgChunkSize),
        embeddingsByType: embeddingsByTypeResult.map((item: any) => ({
          type: item.type || 'unknown',
          count: item.count
        })),
        recentEmbeddings: recentEmbeddingsData.map((emb: any) => ({
          id: emb.id,
          contractId: emb.contractId,
          contractName: emb.contractName || 'Unknown Contract',
          embeddingType: emb.embeddingType || 'unknown',
          sourceText: emb.sourceText || '',
          createdAt: emb.createdAt || new Date(),
        })),
      });
    } catch (error: any) {
      console.error('RAG stats error:', error);
      res.status(500).json({ message: error.message || 'Failed to fetch RAG stats' });
    }
  });

  // ========================
  // LEAD CAPTURE ENDPOINTS
  // ========================

  // Early access signup (public endpoint - no auth required)
  app.post('/api/early-access-signup', async (req, res) => {
    try {
      const { email, name, company } = req.body;

      // Basic validation
      if (!email || !email.includes('@')) {
        return res.status(400).json({ error: 'Valid email is required' });
      }

      const signup = await storage.createEarlyAccessSignup({
        email,
        name: name || null,
        company: company || null,
        source: 'landing_page',
      });

      // Send emails using Zoho Mail
      try {
        const { sendZohoEmail } = await import('./zoho-mail.js');
        
        // Send notification email to info@licenseiq.ai
        await sendZohoEmail({
          to: 'info@licenseiq.ai',
          subject: '🎯 New Early Access Signup - LicenseIQ',
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <h2 style="color: #2563eb;">New Early Access Signup</h2>
              <p>A new user has requested early access to LicenseIQ Research Platform.</p>
              
              <div style="background-color: #f3f4f6; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <p style="margin: 8px 0;"><strong>Name:</strong> ${name || 'Not provided'}</p>
                <p style="margin: 8px 0;"><strong>Email:</strong> ${email}</p>
                <p style="margin: 8px 0;"><strong>Company:</strong> ${company || 'Not provided'}</p>
                <p style="margin: 8px 0;"><strong>Source:</strong> Landing Page</p>
                <p style="margin: 8px 0;"><strong>Signup ID:</strong> ${signup.id}</p>
                <p style="margin: 8px 0;"><strong>Date:</strong> ${new Date().toLocaleString()}</p>
              </div>
              
              <p style="color: #6b7280; font-size: 14px;">
                This notification was automatically generated by the LicenseIQ system.
              </p>
            </div>
          `,
        });
        
        console.log(`📧 Early access notification sent to info@licenseiq.ai for ${email}`);

        // Send confirmation email to customer
        await sendZohoEmail({
          to: email,
          subject: 'Welcome to LicenseIQ Early Access! 🚀',
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
              <div style="text-align: center; margin-bottom: 30px;">
                <h1 style="color: #2563eb; margin-bottom: 10px;">Thank You for Your Interest!</h1>
                <p style="color: #6b7280; font-size: 18px;">You're on the list for early access to LicenseIQ</p>
              </div>
              
              <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 12px; color: white; margin: 20px 0;">
                <h2 style="margin: 0 0 15px 0; font-size: 24px;">What Happens Next?</h2>
                <p style="margin: 0; font-size: 16px; line-height: 1.6;">
                  Our team will review your request and reach out to you within 1-2 business days to schedule a personalized demo and discuss how LicenseIQ can transform your contract management.
                </p>
              </div>

              <div style="background-color: #f9fafb; padding: 25px; border-radius: 8px; margin: 25px 0;">
                <h3 style="color: #1f2937; margin-top: 0;">Your Registration Details</h3>
                <p style="margin: 8px 0; color: #4b5563;"><strong>Name:</strong> ${name || 'Not provided'}</p>
                <p style="margin: 8px 0; color: #4b5563;"><strong>Email:</strong> ${email}</p>
                ${company ? `<p style="margin: 8px 0; color: #4b5563;"><strong>Company:</strong> ${company}</p>` : ''}
              </div>

              <div style="background-color: #eff6ff; border-left: 4px solid #2563eb; padding: 20px; margin: 25px 0;">
                <h3 style="color: #1e40af; margin-top: 0;">🎯 What You'll Get:</h3>
                <ul style="color: #1e40af; line-height: 1.8; margin: 10px 0;">
                  <li>AI-powered contract analysis and risk assessment</li>
                  <li>Automated payment calculations and compliance checks</li>
                  <li>Dynamic rule engine for complex licensing agreements</li>
                  <li>RAG-powered Q&A system for instant contract insights</li>
                  <li>Seamless ERP integration with intelligent field mapping</li>
                </ul>
              </div>

              <div style="text-align: center; margin: 30px 0;">
                <p style="color: #6b7280; margin-bottom: 20px;">
                  Have questions? Reply to this email or contact us at 
                  <a href="mailto:info@licenseiq.ai" style="color: #2563eb; text-decoration: none;">info@licenseiq.ai</a>
                </p>
              </div>

              <div style="border-top: 1px solid #e5e7eb; padding-top: 20px; margin-top: 30px;">
                <p style="color: #9ca3af; font-size: 14px; text-align: center; margin: 0;">
                  This email was sent because you requested early access to LicenseIQ Research Platform.<br>
                  © ${new Date().getFullYear()} LicenseIQ. All rights reserved.
                </p>
              </div>
            </div>
          `,
        });
        
        console.log(`✅ Confirmation email sent to customer: ${email}`);
      } catch (emailError) {
        console.error('Failed to send emails:', emailError);
        // Don't fail the request if email fails - signup is already saved
      }

      res.json({ 
        success: true, 
        message: 'Thank you for your interest! We\'ll be in touch soon.',
        id: signup.id 
      });
    } catch (error) {
      console.error('Early access signup error:', error);
      res.status(500).json({ error: 'Failed to process signup' });
    }
  });

  // Demo request signup (public endpoint - no auth required)
  app.post('/api/demo-request', async (req, res) => {
    try {
      const { email, planTier } = req.body;

      // Basic validation
      if (!email || !email.includes('@')) {
        return res.status(400).json({ error: 'Valid email is required' });
      }

      if (!planTier) {
        return res.status(400).json({ error: 'Plan tier is required' });
      }

      const request = await storage.createDemoRequest({
        email,
        planTier,
        source: 'pricing_section',
      });

      // Send emails using Zoho Mail
      try {
        const { sendZohoEmail } = await import('./zoho-mail.js');
        
        // Get plan details for the email
        const planNames: Record<string, string> = {
          'licenseiq': 'LicenseIQ',
          'licenseiq_plus': 'LicenseIQ Plus',
          'licenseiq_ultra': 'LicenseIQ Ultra'
        };
        
        const planName = planNames[planTier] || planTier;
        
        // Send notification email to info@licenseiq.ai
        await sendZohoEmail({
          to: 'info@licenseiq.ai',
          subject: `📅 New Demo Request - ${planName}`,
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <h2 style="color: #2563eb;">New Demo Request</h2>
              <p>A new user has requested a personalized demo for ${planName}.</p>
              
              <div style="background-color: #f3f4f6; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <p style="margin: 8px 0;"><strong>Email:</strong> ${email}</p>
                <p style="margin: 8px 0;"><strong>Plan Tier:</strong> ${planName}</p>
                <p style="margin: 8px 0;"><strong>Source:</strong> Pricing Section</p>
                <p style="margin: 8px 0;"><strong>Request ID:</strong> ${request.id}</p>
                <p style="margin: 8px 0;"><strong>Timestamp:</strong> ${new Date().toLocaleString()}</p>
              </div>
              
              <div style="margin-top: 20px; padding: 15px; background-color: #eff6ff; border-left: 4px solid #2563eb; border-radius: 4px;">
                <p style="margin: 0; color: #1e40af;"><strong>📋 Next Steps:</strong></p>
                <ol style="margin: 10px 0; padding-left: 20px; color: #1e3a8a;">
                  <li>Review the demo request in the admin panel</li>
                  <li>Contact the user within 24 hours to schedule</li>
                  <li>Prepare personalized demo based on ${planName} features</li>
                </ol>
              </div>
              
              <p style="margin-top: 30px; font-size: 12px; color: #666; text-align: center;">
                This notification was sent from the LicenseIQ Demo Request system.
              </p>
            </div>
          `,
        });
        
        console.log(`✅ Demo request notification sent to info@licenseiq.ai for ${email}`);
        
        // Send confirmation email to the customer
        await sendZohoEmail({
          to: email,
          subject: 'Thank You for Requesting a LicenseIQ Demo! 🎯',
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 40px 20px; text-align: center; border-radius: 8px 8px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 32px;">LicenseIQ</h1>
                <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0; font-size: 18px;">AI-Powered Contract Intelligence</p>
              </div>
              
              <div style="background-color: white; padding: 40px 30px;">
                <h2 style="color: #1a202c; margin-top: 0;">Thank You for Your Interest!</h2>
                
                <p style="color: #4a5568; font-size: 16px; line-height: 1.6;">
                  We've received your request for a personalized demo of <strong>${planName}</strong>.
                </p>
                
                <div style="background-color: #f7fafc; padding: 20px; border-radius: 8px; margin: 25px 0; border-left: 4px solid #667eea;">
                  <p style="margin: 0; color: #2d3748;"><strong>🎯 What Happens Next?</strong></p>
                  <ul style="margin: 15px 0; padding-left: 20px; color: #4a5568;">
                    <li>Our team will contact you within 24 hours</li>
                    <li>We'll schedule a personalized demo at your convenience</li>
                    <li>You'll see how ${planName} can transform your contract workflows</li>
                  </ul>
                </div>
                
                <div style="background-color: #edf2f7; padding: 20px; border-radius: 8px; margin: 25px 0;">
                  <p style="margin: 0 0 10px 0; color: #2d3748;"><strong>📊 ${planName} Includes:</strong></p>
                  <p style="margin: 0; color: #4a5568; font-size: 14px; line-height: 1.6;">
                    ${planTier === 'licenseiq' ? 
                      '✓ AI contract reading & extraction<br>✓ Automated payment calculations<br>✓ Complete audit trail<br>✓ Basic integrations<br>✓ 5 contracts included' :
                      planTier === 'licenseiq_plus' ?
                      '✓ Everything in LicenseIQ<br>✓ Advanced ERP integrations<br>✓ Multi-currency & territories<br>✓ Custom rule builder<br>✓ 25 contracts included' :
                      '✓ Everything in Plus<br>✓ Enterprise SSO<br>✓ Dedicated support<br>✓ API access<br>✓ Unlimited contracts'}
                  </p>
                </div>
                
                <p style="color: #4a5568; font-size: 16px; line-height: 1.6; margin-top: 25px;">
                  In the meantime, feel free to explore our website or reply to this email with any questions.
                </p>
                
                <div style="margin-top: 30px; padding-top: 30px; border-top: 1px solid #e2e8f0; text-align: center;">
                  <p style="color: #718096; font-size: 14px; margin: 0;">
                    Looking forward to showing you the future of contract intelligence!
                  </p>
                  <p style="color: #718096; font-size: 14px; margin: 10px 0 0 0;">
                    <strong>The LicenseIQ Team</strong>
                  </p>
                </div>
              </div>
              
              <div style="background-color: #f7fafc; padding: 20px; text-align: center; border-radius: 0 0 8px 8px;">
                <p style="margin: 0; font-size: 12px; color: #718096;">
                  This email was sent because you requested a demo of LicenseIQ.<br>
                  © ${new Date().getFullYear()} LicenseIQ. All rights reserved.
                </p>
              </div>
            </div>
          `,
        });
        
        console.log(`✅ Confirmation email sent to customer: ${email}`);
      } catch (emailError) {
        console.error('Failed to send demo request emails:', emailError);
        // Don't fail the request if email fails - demo request is already saved
      }

      res.json({ 
        success: true, 
        message: 'Thank you! We\'ll contact you soon to schedule your demo.',
        id: request.id 
      });
    } catch (error) {
      console.error('Demo request error:', error);
      res.status(500).json({ error: 'Failed to process demo request' });
    }
  });

  // Get all early access signups (admin only)
  app.get('/api/admin/early-access-signups', isAuthenticated, async (req, res) => {
    try {
      // Check if user is admin or owner
      if (req.user?.role !== 'admin' && req.user?.role !== 'owner') {
        return res.status(403).json({ error: 'Access denied. Admin privileges required.' });
      }

      const { status } = req.query;
      const signups = await storage.getAllEarlyAccessSignups(status as string);

      res.json(signups);
    } catch (error) {
      console.error('Get early access signups error:', error);
      res.status(500).json({ error: 'Failed to retrieve signups' });
    }
  });

  // Get all demo requests (admin only)
  app.get('/api/admin/demo-requests', isAuthenticated, async (req, res) => {
    try {
      // Check if user is admin or owner
      if (req.user?.role !== 'admin' && req.user?.role !== 'owner') {
        return res.status(403).json({ error: 'Access denied. Admin privileges required.' });
      }

      const { status, planTier } = req.query;
      const requests = await storage.getAllDemoRequests(status as string, planTier as string);

      res.json(requests);
    } catch (error) {
      console.error('Get demo requests error:', error);
      res.status(500).json({ error: 'Failed to retrieve demo requests' });
    }
  });

  // Update early access signup status (admin only)
  app.patch('/api/admin/early-access-signups/:id', isAuthenticated, async (req, res) => {
    try {
      // Check if user is admin or owner
      if (req.user?.role !== 'admin' && req.user?.role !== 'owner') {
        return res.status(403).json({ error: 'Access denied. Admin privileges required.' });
      }

      const { id } = req.params;
      const { status, notes } = req.body;

      if (!status) {
        return res.status(400).json({ error: 'Status is required' });
      }

      const signup = await storage.updateEarlyAccessSignupStatus(id, status, notes);

      res.json(signup);
    } catch (error) {
      console.error('Update early access signup error:', error);
      res.status(500).json({ error: 'Failed to update signup' });
    }
  });

  // Update demo request status (admin only)
  app.patch('/api/admin/demo-requests/:id', isAuthenticated, async (req, res) => {
    try {
      // Check if user is admin or owner
      if (req.user?.role !== 'admin' && req.user?.role !== 'owner') {
        return res.status(403).json({ error: 'Access denied. Admin privileges required.' });
      }

      const { id} = req.params;
      const { status, notes } = req.body;

      if (!status) {
        return res.status(400).json({ error: 'Status is required' });
      }

      const request = await storage.updateDemoRequestStatus(id, status, notes);

      res.json(request);
    } catch (error) {
      console.error('Update demo request error:', error);
      res.status(500).json({ error: 'Failed to update demo request' });
    }
  });

  // ======================
  // MASTER DATA MAPPING API (ERP INTEGRATION)
  // ======================

  // Generate AI-driven schema mapping
  app.post('/api/mapping/generate', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { sourceSchema, targetSchema, entityType, erpSystem } = req.body;

      if (!sourceSchema || !targetSchema) {
        return res.status(400).json({ error: 'Source and target schemas are required' });
      }

      console.log(`🤖 [MAPPING] Generating AI mapping for ${erpSystem} - ${entityType}...`);

      // Use Groq LLaMA to generate intelligent field mappings
      const prompt = `You are an expert ERP integration specialist mapping fields FROM ERP systems TO LicenseIQ schema.

ERP SOURCE SCHEMA (${erpSystem || 'ERP System'} - ${entityType || 'Entity'}):
${JSON.stringify(sourceSchema, null, 2)}

LICENSEIQ TARGET SCHEMA:
${JSON.stringify(targetSchema, null, 2)}

TASK: Map ERP source fields to LicenseIQ target fields.

CRITICAL MAPPING RULES FOR ITEMS ENTITY:
1. ERP "ItemDescription" or "LongDescription" → LicenseIQ "full_legal_product_name" (full technical name with patent info)
2. ERP "ItemDescription" (first part before "-" or shortened) → LicenseIQ "description" (short display name)
3. ERP "ItemClass" → LicenseIQ "item_category" (broad product category like "Ornamental Trees", "Perennials")
4. ERP "ItemType" → LicenseIQ "item_type" (licensing classification like "Licensed Plant")
5. ERP "ItemNumber" → LicenseIQ "item_number" (direct mapping)
6. ERP "Licensor" or "VendorName" → LicenseIQ "vendor" (vendor name)
7. ERP "OrganizationCode" should NOT map to "vendor" - they are different concepts

CRITICAL MAPPING RULES FOR VENDORS ENTITY:
1. ERP "SupplierName" or "VendorName" → LicenseIQ "vendor_name"
2. ERP "SupplierNumber" or "VendorNumber" → LicenseIQ "vendor_number"
3. ERP "SupplierType" → LicenseIQ "vendor_type"

For each LicenseIQ target field, identify:
1. The corresponding ERP source field (or null if no match)
2. Any data transformation needed (e.g., "direct", "lowercase", "date format YYYY-MM-DD", "extract first part before hyphen")
3. Confidence score (0-100) indicating mapping accuracy

GENERAL RULES:
- Match by semantic meaning, not just field names
- Consider data types and formats
- A single ERP source field CAN map to multiple LicenseIQ target fields
- Suggest transformations when needed (date formats, case changes, extraction, etc.)
- If no good match exists, set source_field to null
- Be precise and professional

OUTPUT FORMAT (JSON array):
[
  {
    "source_field": "string or null",
    "target_field": "string",
    "transformation_rule": "string describing transformation or 'direct' if none needed",
    "confidence": number (0-100)
  }
]

Return ONLY valid JSON array, no other text.`;

      const response = await groqService.makeRequest([
        { role: 'system', content: 'You are an expert ERP integration specialist. Return only valid JSON.' },
        { role: 'user', content: prompt }
      ], 0.1, 3000);

      // Parse the AI response
      let mappingResults = [];
      try {
        const cleanedResponse = response.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        mappingResults = JSON.parse(cleanedResponse);
      } catch (parseError) {
        console.error('❌ [MAPPING] Failed to parse AI response:', parseError);
        return res.status(500).json({ error: 'Failed to parse AI mapping results' });
      }

      console.log(`✅ [MAPPING] Generated ${mappingResults.length} field mappings`);

      res.json({
        mappingResults,
        sourceSchema,
        targetSchema,
        entityType,
        erpSystem,
      });
    } catch (error) {
      console.error('❌ [MAPPING] Generation error:', error);
      res.status(500).json({ error: 'Failed to generate mapping' });
    }
  });

  // Batch generate AI-driven schema mappings for multiple entities
  app.post('/api/mapping/batch-generate', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { erpSystemId, erpEntityIds } = req.body;

      if (!erpSystemId || !erpEntityIds || !Array.isArray(erpEntityIds) || erpEntityIds.length === 0) {
        return res.status(400).json({ error: 'ERP system ID and entity IDs array are required' });
      }

      console.log(`🪄 [BATCH MAPPING] Starting batch generation for ${erpEntityIds.length} entities...`);

      // Get ERP system details
      const erpSystem = await storage.getErpSystem(erpSystemId);
      if (!erpSystem) {
        return res.status(404).json({ error: 'ERP system not found' });
      }

      // Get all LicenseIQ entities for matching
      const licenseiqEntities = await storage.getAllLicenseiqEntities();
      if (!licenseiqEntities || licenseiqEntities.length === 0) {
        return res.status(400).json({ error: 'No LicenseIQ entities available for mapping' });
      }

      // Process each ERP entity
      const suggestions = [];
      for (const entityId of erpEntityIds) {
        try {
          // Get ERP entity details
          const erpEntity = await storage.getErpEntity(entityId);
          if (!erpEntity) {
            console.warn(`⚠️ [BATCH MAPPING] Entity ${entityId} not found, skipping`);
            continue;
          }

          // Get ERP entity fields
          const erpFields = await storage.getErpFieldsByEntity(entityId);
          const erpSchema: Record<string, string> = {};
          erpFields.forEach(field => {
            erpSchema[field.fieldName] = field.dataType || 'string';
          });

          console.log(`  🔍 [BATCH MAPPING] Analyzing ${erpEntity.name} (${erpFields.length} fields)...`);

          // Use AI to find best LicenseIQ entity match
          const matchPrompt = `You are an expert ERP integration specialist. Analyze this ERP entity and find the best matching LicenseIQ entity.

ERP ENTITY:
Name: ${erpEntity.name}
Type: ${erpEntity.entityType}
Description: ${erpEntity.description || 'N/A'}
Fields: ${JSON.stringify(erpSchema, null, 2)}

AVAILABLE LICENSEIQ ENTITIES:
${licenseiqEntities.map((entity: any, idx: number) => `${idx + 1}. ${entity.name} (${entity.entityType}) - ${entity.description || 'No description'}`).join('\n')}

TASK: Determine which LicenseIQ entity is the best match for this ERP entity.

Consider:
1. Semantic similarity of names and descriptions
2. Entity types and purposes
3. Field overlap and compatibility
4. Business logic alignment

OUTPUT FORMAT (JSON object):
{
  "matched_entity_id": "string (LicenseIQ entity ID) or null if no good match",
  "confidence": number (0-100),
  "reasoning": "string explaining why this is the best match or why no match was found"
}

Return ONLY valid JSON object, no other text.`;

          const matchResponse = await groqService.makeRequest([
            { role: 'system', content: 'You are an expert ERP integration specialist. Return only valid JSON.' },
            { role: 'user', content: matchPrompt }
          ], 0.1, 1500);

          // Parse AI match response
          let matchResult: any = null;
          try {
            const cleanedMatch = matchResponse.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
            matchResult = JSON.parse(cleanedMatch);
          } catch (parseError) {
            console.error(`❌ [BATCH MAPPING] Failed to parse match result for ${erpEntity.name}:`, parseError);
            // Create default low-confidence result
            matchResult = {
              matched_entity_id: null,
              confidence: 0,
              reasoning: 'AI parsing failed'
            };
          }

          // If no match found, add to suggestions with 0% confidence
          if (!matchResult.matched_entity_id) {
            suggestions.push({
              erpEntityId: erpEntity.id,
              erpEntityName: erpEntity.name,
              erpEntityType: erpEntity.entityType,
              erpSchema,
              licenseiqEntityId: null,
              licenseiqEntityName: null,
              licenseiqSchema: null,
              fieldMappings: [],
              confidence: 0,
              reasoning: matchResult.reasoning || 'No suitable match found',
              erpFieldCount: erpFields.length,
              mappedFieldCount: 0,
            });
            console.log(`  ⚠️ [BATCH MAPPING] No match for ${erpEntity.name}: ${matchResult.reasoning}`);
            continue;
          }

          // Find the matched LicenseIQ entity
          const matchedLicenseiqEntity = licenseiqEntities.find((e: any) => e.id === matchResult.matched_entity_id);
          if (!matchedLicenseiqEntity) {
            console.warn(`⚠️ [BATCH MAPPING] AI matched entity ID ${matchResult.matched_entity_id} not found`);
            continue;
          }

          // Get LicenseIQ entity fields
          const licenseiqFields = await storage.getLicenseiqFieldsByEntity(matchedLicenseiqEntity.id);
          const licenseiqSchema: Record<string, string> = {};
          licenseiqFields.forEach((field: any) => {
            licenseiqSchema[field.fieldName] = field.dataType || 'string';
          });

          // Generate field-level mappings using AI
          const fieldMappingPrompt = `You are an expert ERP integration specialist. Create precise field mappings between these schemas.

SOURCE SCHEMA (${erpEntity.name} - ERP):
${JSON.stringify(erpSchema, null, 2)}

TARGET SCHEMA (${matchedLicenseiqEntity.name} - LicenseIQ):
${JSON.stringify(licenseiqSchema, null, 2)}

TASK: Generate comprehensive field mappings.

For each target field, identify:
1. The corresponding source field (or null if no match)
2. Any data transformation needed
3. Confidence score (0-100)

RULES:
- Match by semantic meaning, not just names
- Consider data types and formats
- Suggest transformations when needed
- If no good match, set source_field to null

OUTPUT FORMAT (JSON array):
[
  {
    "source_field": "string or null",
    "target_field": "string",
    "transformation_rule": "string or 'direct'",
    "confidence": number (0-100)
  }
]

Return ONLY valid JSON array, no other text.`;

          const fieldResponse = await groqService.makeRequest([
            { role: 'system', content: 'You are an expert ERP integration specialist. Return only valid JSON.' },
            { role: 'user', content: fieldMappingPrompt }
          ], 0.1, 2000);

          // Parse field mappings
          let fieldMappings: any[] = [];
          try {
            const cleanedFields = fieldResponse.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
            fieldMappings = JSON.parse(cleanedFields);
          } catch (parseError) {
            console.error(`❌ [BATCH MAPPING] Failed to parse field mappings for ${erpEntity.name}:`, parseError);
            fieldMappings = [];
          }

          // Calculate mapped field count
          const mappedFieldCount = fieldMappings.filter(m => m.source_field !== null).length;

          // Add to suggestions
          suggestions.push({
            erpEntityId: erpEntity.id,
            erpEntityName: erpEntity.name,
            erpEntityType: erpEntity.entityType,
            erpSchema,
            licenseiqEntityId: matchedLicenseiqEntity.id,
            licenseiqEntityName: matchedLicenseiqEntity.name,
            licenseiqSchema,
            fieldMappings,
            confidence: matchResult.confidence || 0,
            reasoning: matchResult.reasoning || 'AI-generated match',
            erpFieldCount: erpFields.length,
            mappedFieldCount,
          });

          console.log(`  ✅ [BATCH MAPPING] ${erpEntity.name} → ${matchedLicenseiqEntity.name} (${matchResult.confidence}% confidence, ${mappedFieldCount}/${erpFields.length} fields)`);

        } catch (entityError) {
          console.error(`❌ [BATCH MAPPING] Error processing entity ${entityId}:`, entityError);
        }
      }

      console.log(`🎉 [BATCH MAPPING] Completed batch generation: ${suggestions.length} suggestions created`);

      res.json({
        erpSystemId,
        erpSystemName: erpSystem.name,
        suggestions,
        totalProcessed: suggestions.length,
      });

    } catch (error) {
      console.error('❌ [BATCH MAPPING] Batch generation error:', error);
      res.status(500).json({ error: 'Failed to generate batch mappings' });
    }
  });

  // Save mapping to database
  // Supports explicit scope selection with validation
  // Scope options: 'context' (use active context), 'company', 'business_unit', 'location', 'global' (admin only)
  app.post('/api/mapping/save', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { 
        mappingName, erpSystem, entityType, sourceSchema, targetSchema, mappingResults, notes, 
        erpSystemId, licenseiqEntityId,
        // Explicit scope selection (optional - defaults to 'context')
        scopeLevel, // 'context' | 'company' | 'business_unit' | 'location' | 'global'
        targetCompanyId, targetBusinessUnitId, targetLocationId // For explicit scope override
      } = req.body;

      if (!mappingName || !erpSystem || !entityType || !sourceSchema || !targetSchema || !mappingResults) {
        return res.status(400).json({ error: 'All required fields must be provided' });
      }

      const activeContext = req.user.activeContext;
      let companyId: string | null = null;
      let businessUnitId: string | null = null;
      let locationId: string | null = null;
      let scopeDescription = '';

      // Determine scope based on scopeLevel
      const effectiveScopeLevel = scopeLevel || 'context';

      switch (effectiveScopeLevel) {
        case 'global':
          // Only system admins can create global mappings
          if (!isSystemAdmin(req.user)) {
            return res.status(403).json({ 
              error: 'Only System Admins can create global mappings. Please select a company scope.' 
            });
          }
          // All IDs remain null for global
          scopeDescription = 'Global (all companies)';
          break;

        case 'company':
          // Company-wide mapping (available to all BUs and locations in the company)
          companyId = targetCompanyId || activeContext?.companyId;
          if (!companyId) {
            return res.status(400).json({ error: 'Company ID required for company-level mapping' });
          }
          scopeDescription = 'Company-wide';
          break;

        case 'business_unit':
          // BU-level mapping (available to all locations in the BU)
          companyId = targetCompanyId || activeContext?.companyId;
          businessUnitId = targetBusinessUnitId || activeContext?.businessUnitId;
          if (!companyId || !businessUnitId) {
            return res.status(400).json({ error: 'Company and Business Unit IDs required for BU-level mapping' });
          }
          scopeDescription = 'Business Unit';
          break;

        case 'location':
          // Location-specific mapping
          companyId = targetCompanyId || activeContext?.companyId;
          businessUnitId = targetBusinessUnitId || activeContext?.businessUnitId;
          locationId = targetLocationId || activeContext?.locationId;
          if (!companyId || !businessUnitId || !locationId) {
            return res.status(400).json({ error: 'Full hierarchy (Company, BU, Location) required for location-level mapping' });
          }
          scopeDescription = 'Location-specific';
          break;

        case 'context':
        default:
          // Use active context (default behavior)
          companyId = activeContext?.companyId || null;
          businessUnitId = activeContext?.businessUnitId || null;
          locationId = activeContext?.locationId || null;
          
          // Non-admin users MUST have at least company context
          if (!companyId && !isSystemAdmin(req.user)) {
            return res.status(400).json({ 
              error: 'Please select an active company context before creating mappings. Go to the top-right context selector.' 
            });
          }
          scopeDescription = companyId ? 'From active context' : 'Global (System Admin)';
          break;
      }

      const mapping = await storage.createMasterDataMapping({
        mappingName,
        erpSystem,
        erpSystemId: erpSystemId || null,
        entityType,
        licenseiqEntityId: licenseiqEntityId || null,
        sourceSchema,
        targetSchema,
        mappingResults,
        status: 'draft', // New mappings start as draft, require approval
        aiModel: 'llama-3.3-70b-versatile',
        createdBy: req.user.id,
        notes: notes || null,
        companyId,
        businessUnitId,
        locationId,
        version: 1,
      });

      console.log(`💾 [MAPPING] Saved: ${mappingName} | Scope: ${scopeDescription} | ID: ${mapping.id}`);

      res.json({
        ...mapping,
        scopeLevel: effectiveScopeLevel,
        scopeDescription,
      });
    } catch (error) {
      console.error('❌ [MAPPING] Save error:', error);
      res.status(500).json({ error: 'Failed to save mapping' });
    }
  });

  // Get all mappings
  app.get('/api/mapping', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { erpSystem, entityType, status } = req.query;
      const mappings = await storage.getAllMasterDataMappings({
        erpSystem: erpSystem as string,
        entityType: entityType as string,
        status: status as string,
      });

      res.json({ mappings });
    } catch (error) {
      console.error('❌ [MAPPING] List error:', error);
      res.status(500).json({ error: 'Failed to retrieve mappings' });
    }
  });

  // Get single mapping by ID
  app.get('/api/mapping/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const mapping = await storage.getMasterDataMapping(id);

      if (!mapping) {
        return res.status(404).json({ error: 'Mapping not found' });
      }

      res.json(mapping);
    } catch (error) {
      console.error('❌ [MAPPING] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve mapping' });
    }
  });

  // Update mapping
  app.patch('/api/mapping/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const updates = req.body;

      const mapping = await storage.updateMasterDataMapping(id, updates);

      console.log(`✏️ [MAPPING] Updated mapping: ${id}`);

      res.json(mapping);
    } catch (error) {
      console.error('❌ [MAPPING] Update error:', error);
      res.status(500).json({ error: 'Failed to update mapping' });
    }
  });

  // Delete mapping
  app.delete('/api/mapping/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteMasterDataMapping(id);

      console.log(`🗑️ [MAPPING] Deleted mapping: ${id}`);

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [MAPPING] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete mapping' });
    }
  });

  // ======================
  // ERP CATALOG API
  // ======================

  // ERP Systems endpoints
  app.get('/api/erp-systems', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { status } = req.query;
      const systems = await storage.getAllErpSystems(status as string);
      res.json({ systems });
    } catch (error) {
      console.error('❌ [ERP SYSTEMS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP systems' });
    }
  });

  app.post('/api/erp-systems', isAuthenticated, async (req: any, res: Response) => {
    try {
      const systemData = {
        ...req.body,
        createdBy: req.user.id,
      };
      const system = await storage.createErpSystem(systemData);
      console.log(`✅ [ERP SYSTEMS] Created: ${system.name}`);
      res.json(system);
    } catch (error) {
      console.error('❌ [ERP SYSTEMS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP system' });
    }
  });

  app.patch('/api/erp-systems/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const system = await storage.updateErpSystem(id, req.body);
      console.log(`✏️ [ERP SYSTEMS] Updated: ${system.name}`);
      res.json(system);
    } catch (error) {
      console.error('❌ [ERP SYSTEMS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP system' });
    }
  });

  app.delete('/api/erp-systems/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpSystem(id);
      console.log(`🗑️ [ERP SYSTEMS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP SYSTEMS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP system' });
    }
  });

  // ERP Entities endpoints
  app.get('/api/erp-entities', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { systemId, entityType } = req.query;
      if (!systemId) {
        // Return all entities when no systemId provided (for lookups)
        const allEntities = await storage.getAllErpEntities();
        return res.json({ entities: allEntities });
      }
      const entities = await storage.getErpEntitiesBySystem(systemId as string, entityType as string);
      res.json({ entities });
    } catch (error) {
      console.error('❌ [ERP ENTITIES] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP entities' });
    }
  });

  app.post('/api/erp-entities', isAuthenticated, async (req: any, res: Response) => {
    try {
      const entityData = {
        ...req.body,
        createdBy: req.user.id,
      };
      const entity = await storage.createErpEntity(entityData);
      console.log(`✅ [ERP ENTITIES] Created: ${entity.name}`);
      res.json(entity);
    } catch (error) {
      console.error('❌ [ERP ENTITIES] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP entity' });
    }
  });

  app.patch('/api/erp-entities/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const entity = await storage.updateErpEntity(id, req.body);
      console.log(`✏️ [ERP ENTITIES] Updated: ${entity.name}`);
      res.json(entity);
    } catch (error) {
      console.error('❌ [ERP ENTITIES] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP entity' });
    }
  });

  app.delete('/api/erp-entities/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpEntity(id);
      console.log(`🗑️ [ERP ENTITIES] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP ENTITIES] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP entity' });
    }
  });

  // ERP Fields endpoints
  app.get('/api/erp-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId } = req.query;
      console.log('🔍 [ERP FIELDS] Request received - entityId:', entityId);
      if (!entityId) {
        console.log('⚠️ [ERP FIELDS] No entityId provided in query');
        return res.status(400).json({ error: 'entityId is required' });
      }
      const fields = await storage.getErpFieldsByEntity(entityId as string);
      console.log(`✅ [ERP FIELDS] Found ${fields.length} fields for entity ${entityId}`);
      res.json({ fields });
    } catch (error) {
      console.error('❌ [ERP FIELDS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP fields' });
    }
  });

  app.post('/api/erp-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const field = await storage.createErpField(req.body);
      console.log(`✅ [ERP FIELDS] Created: ${field.fieldName}`);
      res.json(field);
    } catch (error) {
      console.error('❌ [ERP FIELDS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP field' });
    }
  });

  app.patch('/api/erp-fields/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const field = await storage.updateErpField(id, req.body);
      console.log(`✏️ [ERP FIELDS] Updated: ${field.fieldName}`);
      res.json(field);
    } catch (error) {
      console.error('❌ [ERP FIELDS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP field' });
    }
  });

  app.delete('/api/erp-fields/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpField(id);
      console.log(`🗑️ [ERP FIELDS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP FIELDS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP field' });
    }
  });

  // ======================
  // ERP TO LICENSEIQ FIELD MAPPINGS API
  // ======================

  // Get ERP to LicenseIQ field mappings for testing workflow
  app.get('/api/erp-licenseiq-mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { erpSystemId } = req.query;
      if (!erpSystemId) {
        return res.status(400).json({ error: 'erpSystemId is required' });
      }
      
      const { getErpToLicenseIQMappings } = await import('./services/erpVocabularyService');
      const mappings = await getErpToLicenseIQMappings(erpSystemId as string);
      console.log(`📋 [ERP-LICENSEIQ] Found ${mappings.length} field mappings for ERP system ${erpSystemId}`);
      res.json({ mappings, count: mappings.length });
    } catch (error) {
      console.error('❌ [ERP-LICENSEIQ] Get mappings error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP to LicenseIQ mappings' });
    }
  });

  // Test dual terminology display for a contract term
  app.post('/api/erp-licenseiq-mappings/test-dual-terminology', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { contractTerm, licenseiqEntityName, masterDataValue, erpFieldName } = req.body;
      
      if (!contractTerm || !licenseiqEntityName) {
        return res.status(400).json({ error: 'contractTerm and licenseiqEntityName are required' });
      }
      
      const { buildDualTerminologyDisplay } = await import('./services/erpVocabularyService');
      const display = buildDualTerminologyDisplay(contractTerm, licenseiqEntityName, masterDataValue, erpFieldName);
      
      console.log(`✅ [DUAL-TERMINOLOGY] Generated: ${display}`);
      res.json({ 
        display,
        components: {
          contractTerm,
          licenseiqEntityName,
          masterDataValue,
          erpFieldName
        }
      });
    } catch (error) {
      console.error('❌ [DUAL-TERMINOLOGY] Test error:', error);
      res.status(500).json({ error: 'Failed to generate dual terminology display' });
    }
  });

  // Lookup master data and generate dual terminology for a contract term
  app.post('/api/erp-licenseiq-mappings/lookup', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { contractTerm, licenseiqEntityId, orgId } = req.body;
      
      if (!contractTerm || !licenseiqEntityId) {
        return res.status(400).json({ error: 'contractTerm and licenseiqEntityId are required' });
      }
      
      const { lookupMasterDataValue, buildDualTerminologyDisplay } = await import('./services/erpVocabularyService');
      
      // Lookup master data for this entity in the organization context
      const matchResult = await lookupMasterDataValue(licenseiqEntityId, contractTerm, orgId);
      
      // Get entity name
      const entityResult = await db
        .select({ name: licenseiqEntities.name })
        .from(licenseiqEntities)
        .where(eq(licenseiqEntities.id, licenseiqEntityId))
        .limit(1);
      
      const entityName = entityResult[0]?.name || 'Unknown Entity';
      
      // Build dual terminology display
      const lookupValue = matchResult?.recordData?.vendor_number || matchResult?.recordData?.item_number || matchResult?.matchedValue || null;
      const display = buildDualTerminologyDisplay(
        contractTerm,
        entityName,
        lookupValue
      );
      
      console.log(`✅ [LOOKUP] Term "${contractTerm}" → ${display}`);
      res.json({
        display,
        matched: !!matchResult,
        matchedRecord: matchResult || null,
        entityName
      });
    } catch (error) {
      console.error('❌ [LOOKUP] Error:', error);
      res.status(500).json({ error: 'Failed to lookup master data' });
    }
  });

  // ======================
  // PENDING TERM MAPPINGS API (ERP-Aware Extraction)
  // ======================

  // Get pending term mappings for a contract
  app.get('/api/contracts/:contractId/pending-mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { contractId } = req.params;
      const { status } = req.query;
      const mappings = await storage.getPendingTermMappingsByContract(contractId, status as string | undefined);
      console.log(`📋 [PENDING MAPPINGS] Found ${mappings.length} mappings for contract ${contractId}`);
      res.json({ mappings });
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve pending mappings' });
    }
  });

  // Get pending mappings by extraction run
  app.get('/api/extraction-runs/:runId/pending-mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { runId } = req.params;
      const mappings = await storage.getPendingTermMappingsByExtractionRun(runId);
      console.log(`📋 [PENDING MAPPINGS] Found ${mappings.length} mappings for run ${runId}`);
      res.json({ mappings });
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve pending mappings' });
    }
  });

  // Confirm a single pending mapping
  app.post('/api/pending-mappings/:id/confirm', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const { modifiedValue, modifiedFieldId } = req.body;
      const userId = req.user.id;
      
      const mapping = await storage.confirmPendingTermMapping(id, userId, modifiedValue, modifiedFieldId);
      console.log(`✅ [PENDING MAPPINGS] Confirmed mapping: ${id}`);
      
      // Auto-generate ERP mapping rule from confirmed mapping
      try {
        const { generateRuleFromMapping } = await import('./services/erpRuleGenerationService');
        const generatedRule = await generateRuleFromMapping(id, userId);
        if (generatedRule) {
          console.log(`✅ [AUTO-RULE] Generated rule: ${generatedRule.ruleName}`);
        }
      } catch (ruleError) {
        console.log(`ℹ️ [AUTO-RULE] Rule generation skipped:`, ruleError);
      }
      
      res.json(mapping);
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Confirm error:', error);
      res.status(500).json({ error: 'Failed to confirm mapping' });
    }
  });

  // Bulk confirm multiple mappings
  app.post('/api/pending-mappings/bulk-confirm', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { mappingIds } = req.body;
      const userId = req.user.id;
      
      if (!mappingIds || !Array.isArray(mappingIds) || mappingIds.length === 0) {
        return res.status(400).json({ error: 'mappingIds array is required' });
      }
      
      const mappings = await storage.bulkConfirmPendingTermMappings(mappingIds, userId);
      console.log(`✅ [PENDING MAPPINGS] Bulk confirmed ${mappings.length} mappings`);
      
      // Auto-generate ERP mapping rules from confirmed mappings
      try {
        const { generateRulesFromMappings } = await import('./services/erpRuleGenerationService');
        const generatedRules = await generateRulesFromMappings(mappingIds, userId);
        console.log(`✅ [AUTO-RULE] Generated ${generatedRules.length} rules from bulk confirm`);
      } catch (ruleError) {
        console.log(`ℹ️ [AUTO-RULE] Bulk rule generation skipped:`, ruleError);
      }
      
      // Trigger blueprint materialization for affected contracts
      try {
        const contractIds = [...new Set(mappings.map(m => m.contractId))];
        const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
        for (const contractId of contractIds) {
          const result = await blueprintMaterializerService.onMappingsConfirmed(contractId);
          console.log(`📐 [BLUEPRINT] Materialized ${result.blueprintsCreated} blueprints for contract ${contractId}`);
        }
      } catch (materializeError) {
        console.log(`ℹ️ [BLUEPRINT] Materialization skipped:`, materializeError);
      }
      
      res.json({ mappings, count: mappings.length });
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Bulk confirm error:', error);
      res.status(500).json({ error: 'Failed to bulk confirm mappings' });
    }
  });

  // Regenerate ERP rules for all confirmed mappings of a contract
  app.post('/api/contracts/:id/regenerate-erp-rules', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const userId = req.user.id;
      
      // Get all confirmed mappings for this contract
      const confirmedMappings = await db
        .select({ id: pendingTermMappings.id })
        .from(pendingTermMappings)
        .where(and(
          eq(pendingTermMappings.contractId, contractId),
          eq(pendingTermMappings.status, 'confirmed')
        ));
      
      if (confirmedMappings.length === 0) {
        return res.json({ message: 'No confirmed mappings found', rulesGenerated: 0 });
      }
      
      const mappingIds = confirmedMappings.map(m => m.id);
      
      const { generateRulesFromMappings } = await import('./services/erpRuleGenerationService');
      const generatedRules = await generateRulesFromMappings(mappingIds, userId);
      
      console.log(`✅ [REGENERATE] Generated ${generatedRules.length} ERP rules for contract ${contractId}`);
      
      // Also trigger blueprint materialization
      let blueprintsCreated = 0;
      try {
        const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
        const result = await blueprintMaterializerService.materializeForContract(contractId);
        blueprintsCreated = result.blueprintsCreated;
        console.log(`📐 [BLUEPRINT] Materialized ${blueprintsCreated} blueprints`);
      } catch (materializeError) {
        console.log(`ℹ️ [BLUEPRINT] Materialization skipped:`, materializeError);
      }
      
      res.json({ 
        message: `Generated ${generatedRules.length} ERP rules and ${blueprintsCreated} calculation blueprints`,
        rulesGenerated: generatedRules.length,
        blueprintsCreated,
        mappingsProcessed: mappingIds.length
      });
    } catch (error: any) {
      console.error('❌ [REGENERATE] Error:', error);
      res.status(500).json({ error: error.message || 'Failed to regenerate ERP rules' });
    }
  });

  // ===================
  // CALCULATION BLUEPRINTS API
  // ===================

  // Get calculation blueprints for a contract
  app.get('/api/contracts/:id/blueprints', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
      const blueprints = await blueprintMaterializerService.getBlueprintsForContract(contractId);
      
      res.json({ blueprints, count: blueprints.length });
    } catch (error: any) {
      console.error('❌ [BLUEPRINTS] Get error:', error);
      res.status(500).json({ error: error.message || 'Failed to get blueprints' });
    }
  });

  // Manually trigger blueprint materialization for a contract
  app.post('/api/contracts/:id/materialize-blueprints', isAuthenticated, async (req: any, res: Response) => {
    try {
      const contractId = req.params.id;
      const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
      const result = await blueprintMaterializerService.materializeForContract(contractId);
      
      console.log(`📐 [BLUEPRINT] Materialized ${result.blueprintsCreated} blueprints for contract ${contractId}`);
      res.json(result);
    } catch (error: any) {
      console.error('❌ [BLUEPRINTS] Materialization error:', error);
      res.status(500).json({ error: error.message || 'Failed to materialize blueprints' });
    }
  });

  // Get a single blueprint with dimensions
  app.get('/api/blueprints/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const blueprintId = req.params.id;
      const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
      const blueprint = await blueprintMaterializerService.getBlueprintWithDimensions(blueprintId);
      
      if (!blueprint) {
        return res.status(404).json({ error: 'Blueprint not found' });
      }
      
      res.json(blueprint);
    } catch (error: any) {
      console.error('❌ [BLUEPRINTS] Get single error:', error);
      res.status(500).json({ error: error.message || 'Failed to get blueprint' });
    }
  });

  // Delete a single blueprint
  app.delete('/api/blueprints/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const blueprintId = req.params.id;
      
      // First get the blueprint to verify it exists
      const { blueprintMaterializerService } = await import('./services/blueprintMaterializerService');
      const blueprint = await blueprintMaterializerService.getBlueprintWithDimensions(blueprintId);
      
      if (!blueprint) {
        return res.status(404).json({ error: 'Blueprint not found' });
      }
      
      // Delete the blueprint dimensions first, then the blueprint
      await db.delete(blueprintDimensions).where(eq(blueprintDimensions.blueprintId, blueprintId));
      await db.delete(calculationBlueprints).where(eq(calculationBlueprints.id, blueprintId));
      
      console.log(`🗑️ [BLUEPRINT] Deleted blueprint ${blueprintId}`);
      res.json({ message: 'Blueprint deleted successfully' });
    } catch (error: any) {
      console.error('❌ [BLUEPRINTS] Delete error:', error);
      res.status(500).json({ error: error.message || 'Failed to delete blueprint' });
    }
  });

  // Reject a pending mapping
  app.post('/api/pending-mappings/:id/reject', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const userId = req.user.id;
      
      const mapping = await storage.rejectPendingTermMapping(id, userId);
      console.log(`❌ [PENDING MAPPINGS] Rejected mapping: ${id}`);
      res.json(mapping);
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Reject error:', error);
      res.status(500).json({ error: 'Failed to reject mapping' });
    }
  });

  // Update a pending mapping
  app.patch('/api/pending-mappings/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const updates = req.body;
      
      const mapping = await storage.updatePendingTermMapping(id, updates);
      console.log(`✏️ [PENDING MAPPINGS] Updated mapping: ${id}`);
      res.json(mapping);
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Update error:', error);
      res.status(500).json({ error: 'Failed to update mapping' });
    }
  });

  // Delete a pending mapping
  app.delete('/api/pending-mappings/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deletePendingTermMapping(id);
      console.log(`🗑️ [PENDING MAPPINGS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete mapping' });
    }
  });

  // Delete all pending mappings for a contract
  app.delete('/api/contracts/:contractId/pending-mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { contractId } = req.params;
      await storage.deletePendingTermMappingsByContract(contractId);
      console.log(`🗑️ [PENDING MAPPINGS] Deleted all for contract: ${contractId}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [PENDING MAPPINGS] Delete all error:', error);
      res.status(500).json({ error: 'Failed to delete pending mappings' });
    }
  });

  // ======================
  // ERP MAPPING RULES API
  // Parallel system for automated license fee calculations
  // ======================

  // Organization Calculation Settings
  app.get('/api/org-calculation-settings/:companyId', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { companyId } = req.params;
      const settings = await storage.getOrgCalculationSettings(companyId);
      console.log(`📋 [ORG SETTINGS] Retrieved for company: ${companyId}`);
      res.json(settings || { calculationApproach: 'manual', companyId });
    } catch (error) {
      console.error('❌ [ORG SETTINGS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve organization settings' });
    }
  });

  app.put('/api/org-calculation-settings/:companyId', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { companyId } = req.params;
      const { calculationApproach } = req.body;
      
      if (!['manual', 'erp_mapping_rules', 'hybrid'].includes(calculationApproach)) {
        return res.status(400).json({ error: 'Invalid calculation approach' });
      }
      
      const settings = await storage.upsertOrgCalculationSettings(companyId, calculationApproach, req.user?.id);
      console.log(`✅ [ORG SETTINGS] Updated for company ${companyId}: ${calculationApproach}`);
      res.json(settings);
    } catch (error) {
      console.error('❌ [ORG SETTINGS] Update error:', error);
      res.status(500).json({ error: 'Failed to update organization settings' });
    }
  });

  // ERP Mapping Rule Sets
  app.get('/api/erp-mapping-rule-sets', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { companyId, businessUnitId, locationId, sourceSystemId, status } = req.query;
      const filters: any = {};
      if (companyId) filters.companyId = companyId;
      if (businessUnitId) filters.businessUnitId = businessUnitId;
      if (locationId) filters.locationId = locationId;
      if (sourceSystemId) filters.sourceSystemId = sourceSystemId;
      if (status) filters.status = status;
      
      const ruleSets = await storage.getErpMappingRuleSets(filters);
      console.log(`📋 [ERP RULE SETS] Found ${ruleSets.length} rule sets`);
      res.json({ ruleSets });
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP mapping rule sets' });
    }
  });

  app.get('/api/erp-mapping-rule-sets/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const { includeDetails } = req.query;
      
      if (includeDetails === 'true') {
        const result = await storage.getErpMappingRuleSetWithDetails(id);
        if (!result) {
          return res.status(404).json({ error: 'Rule set not found' });
        }
        return res.json(result);
      }
      
      const ruleSet = await storage.getErpMappingRuleSet(id);
      if (!ruleSet) {
        return res.status(404).json({ error: 'Rule set not found' });
      }
      res.json(ruleSet);
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Get by ID error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP mapping rule set' });
    }
  });

  app.post('/api/erp-mapping-rule-sets', isAuthenticated, async (req: any, res: Response) => {
    try {
      const ruleSetData = {
        ...req.body,
        createdBy: req.user?.id,
        companyId: req.body.companyId || req.user?.activeContext?.companyId,
        businessUnitId: req.body.businessUnitId || req.user?.activeContext?.businessUnitId,
        locationId: req.body.locationId || req.user?.activeContext?.locationId,
      };
      
      if (!ruleSetData.companyId) {
        return res.status(400).json({ error: 'Company context is required' });
      }
      if (!ruleSetData.sourceSystemId) {
        return res.status(400).json({ error: 'Source system ID is required' });
      }
      if (!ruleSetData.name) {
        return res.status(400).json({ error: 'Rule set name is required' });
      }
      
      const ruleSet = await storage.createErpMappingRuleSet(ruleSetData);
      console.log(`✅ [ERP RULE SETS] Created: ${ruleSet.name} (${ruleSet.id})`);
      res.json(ruleSet);
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP mapping rule set' });
    }
  });

  app.patch('/api/erp-mapping-rule-sets/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const ruleSet = await storage.updateErpMappingRuleSet(id, req.body);
      console.log(`✏️ [ERP RULE SETS] Updated: ${id}`);
      res.json(ruleSet);
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP mapping rule set' });
    }
  });

  app.delete('/api/erp-mapping-rule-sets/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpMappingRuleSet(id);
      console.log(`🗑️ [ERP RULE SETS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP mapping rule set' });
    }
  });

  app.post('/api/erp-mapping-rule-sets/:id/activate', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const ruleSet = await storage.activateErpMappingRuleSet(id);
      console.log(`✅ [ERP RULE SETS] Activated: ${id}`);
      res.json(ruleSet);
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Activate error:', error);
      res.status(500).json({ error: 'Failed to activate ERP mapping rule set' });
    }
  });

  app.post('/api/erp-mapping-rule-sets/:id/deactivate', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const ruleSet = await storage.deactivateErpMappingRuleSet(id);
      console.log(`⏸️ [ERP RULE SETS] Deactivated: ${id}`);
      res.json(ruleSet);
    } catch (error) {
      console.error('❌ [ERP RULE SETS] Deactivate error:', error);
      res.status(500).json({ error: 'Failed to deactivate ERP mapping rule set' });
    }
  });

  // ERP Mapping Rules (within a rule set)
  app.get('/api/erp-mapping-rule-sets/:ruleSetId/rules', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { ruleSetId } = req.params;
      const rules = await storage.getErpMappingRules(ruleSetId);
      console.log(`📋 [ERP RULES] Found ${rules.length} rules for set ${ruleSetId}`);
      res.json({ rules });
    } catch (error) {
      console.error('❌ [ERP RULES] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP mapping rules' });
    }
  });

  app.post('/api/erp-mapping-rule-sets/:ruleSetId/rules', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { ruleSetId } = req.params;
      const ruleData = { ...req.body, ruleSetId };
      
      if (!ruleData.name) {
        return res.status(400).json({ error: 'Rule name is required' });
      }
      if (!ruleData.sourceField) {
        return res.status(400).json({ error: 'Source field is required' });
      }
      if (!ruleData.targetField) {
        return res.status(400).json({ error: 'Target field is required' });
      }
      
      const rule = await storage.createErpMappingRule(ruleData);
      console.log(`✅ [ERP RULES] Created: ${rule.name} (${rule.id})`);
      res.json(rule);
    } catch (error) {
      console.error('❌ [ERP RULES] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP mapping rule' });
    }
  });

  app.get('/api/erp-mapping-rules/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const rule = await storage.getErpMappingRule(id);
      if (!rule) {
        return res.status(404).json({ error: 'Rule not found' });
      }
      
      // Get conditions and outputs
      const conditions = await storage.getErpMappingConditions(id);
      const outputs = await storage.getErpMappingOutputs(id);
      
      res.json({ ...rule, conditions, outputs });
    } catch (error) {
      console.error('❌ [ERP RULES] Get by ID error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP mapping rule' });
    }
  });

  app.patch('/api/erp-mapping-rules/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const rule = await storage.updateErpMappingRule(id, req.body);
      console.log(`✏️ [ERP RULES] Updated: ${id}`);
      res.json(rule);
    } catch (error) {
      console.error('❌ [ERP RULES] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP mapping rule' });
    }
  });

  app.delete('/api/erp-mapping-rules/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpMappingRule(id);
      console.log(`🗑️ [ERP RULES] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP RULES] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP mapping rule' });
    }
  });

  // ERP Mapping Conditions
  app.post('/api/erp-mapping-rules/:ruleId/conditions', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { ruleId } = req.params;
      const conditionData = { ...req.body, ruleId };
      const condition = await storage.createErpMappingCondition(conditionData);
      console.log(`✅ [ERP CONDITIONS] Created for rule ${ruleId}`);
      res.json(condition);
    } catch (error) {
      console.error('❌ [ERP CONDITIONS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP mapping condition' });
    }
  });

  app.patch('/api/erp-mapping-conditions/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const condition = await storage.updateErpMappingCondition(id, req.body);
      console.log(`✏️ [ERP CONDITIONS] Updated: ${id}`);
      res.json(condition);
    } catch (error) {
      console.error('❌ [ERP CONDITIONS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP mapping condition' });
    }
  });

  app.delete('/api/erp-mapping-conditions/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpMappingCondition(id);
      console.log(`🗑️ [ERP CONDITIONS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP CONDITIONS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP mapping condition' });
    }
  });

  // ERP Mapping Outputs
  app.post('/api/erp-mapping-rules/:ruleId/outputs', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { ruleId } = req.params;
      const outputData = { ...req.body, ruleId };
      const output = await storage.createErpMappingOutput(outputData);
      console.log(`✅ [ERP OUTPUTS] Created for rule ${ruleId}`);
      res.json(output);
    } catch (error) {
      console.error('❌ [ERP OUTPUTS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP mapping output' });
    }
  });

  app.patch('/api/erp-mapping-outputs/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const output = await storage.updateErpMappingOutput(id, req.body);
      console.log(`✏️ [ERP OUTPUTS] Updated: ${id}`);
      res.json(output);
    } catch (error) {
      console.error('❌ [ERP OUTPUTS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP mapping output' });
    }
  });

  app.delete('/api/erp-mapping-outputs/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpMappingOutput(id);
      console.log(`🗑️ [ERP OUTPUTS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP OUTPUTS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP mapping output' });
    }
  });

  // ERP Rule Execution Log
  app.get('/api/erp-rule-execution-logs', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { ruleSetId, calculationId, status, limit } = req.query;
      const filters: any = {};
      if (ruleSetId) filters.ruleSetId = ruleSetId;
      if (calculationId) filters.calculationId = calculationId;
      if (status) filters.status = status;
      if (limit) filters.limit = parseInt(limit as string, 10);
      
      const logs = await storage.getErpRuleExecutionLogs(filters);
      console.log(`📋 [ERP EXEC LOGS] Found ${logs.length} execution logs`);
      res.json({ logs });
    } catch (error) {
      console.error('❌ [ERP EXEC LOGS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve execution logs' });
    }
  });

  // ERP Entity Records endpoints (Data tab)
  app.get('/api/erp-entity-records', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId, limit } = req.query;
      if (!entityId) {
        return res.status(400).json({ error: 'entityId is required' });
      }
      const context = {
        activeContext: req.user?.activeContext || null,
        isAdmin: req.user?.role === 'admin' || req.user?.role === 'system_admin'
      };
      let records = await storage.getErpEntityRecordsByEntity(entityId as string, context);
      
      // Apply limit if specified (for sample data preview)
      if (limit) {
        const limitNum = parseInt(limit as string, 10);
        if (!isNaN(limitNum) && limitNum > 0) {
          records = records.slice(0, limitNum);
        }
      }
      
      res.json({ records });
    } catch (error) {
      console.error('❌ [ERP RECORDS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve ERP entity records' });
    }
  });

  app.post('/api/erp-entity-records', isAuthenticated, async (req: any, res: Response) => {
    try {
      const recordData = {
        ...req.body,
        createdBy: req.user.id,
        companyId: req.body.companyId || req.user?.activeContext?.companyId,
        businessUnitId: req.body.businessUnitId || req.user?.activeContext?.businessUnitId,
        locationId: req.body.locationId || req.user?.activeContext?.locationId,
      };
      const record = await storage.createErpEntityRecord(recordData);
      console.log(`✅ [ERP RECORDS] Created record for entity: ${recordData.entityId}`);
      res.json(record);
    } catch (error) {
      console.error('❌ [ERP RECORDS] Create error:', error);
      res.status(500).json({ error: 'Failed to create ERP entity record' });
    }
  });

  app.patch('/api/erp-entity-records/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const record = await storage.updateErpEntityRecord(id, req.body);
      console.log(`✏️ [ERP RECORDS] Updated: ${id}`);
      res.json(record);
    } catch (error) {
      console.error('❌ [ERP RECORDS] Update error:', error);
      res.status(500).json({ error: 'Failed to update ERP entity record' });
    }
  });

  app.delete('/api/erp-entity-records/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteErpEntityRecord(id);
      console.log(`🗑️ [ERP RECORDS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ERP RECORDS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete ERP entity record' });
    }
  });

  // ==========================================
  // LICENSEIQ SCHEMA CATALOG ROUTES
  // ==========================================

  // Get all LicenseIQ entities
  app.get('/api/licenseiq-entities', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { category } = req.query;
      const entities = await storage.getAllLicenseiqEntities(category);
      res.json({ entities });
    } catch (error) {
      console.error('❌ [LICENSEIQ ENTITIES] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve LicenseIQ entities' });
    }
  });

  // Create new LicenseIQ entity
  app.post('/api/licenseiq-entities', isAuthenticated, async (req: any, res: Response) => {
    try {
      const entity = await storage.createLicenseiqEntity(req.body);
      console.log(`✅ [LICENSEIQ ENTITIES] Created: ${entity.name}`);
      res.json(entity);
    } catch (error) {
      console.error('❌ [LICENSEIQ ENTITIES] Create error:', error);
      res.status(500).json({ error: 'Failed to create LicenseIQ entity' });
    }
  });

  // Update LicenseIQ entity
  app.patch('/api/licenseiq-entities/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const entity = await storage.updateLicenseiqEntity(id, req.body);
      console.log(`✏️ [LICENSEIQ ENTITIES] Updated: ${entity.name}`);
      res.json(entity);
    } catch (error) {
      console.error('❌ [LICENSEIQ ENTITIES] Update error:', error);
      res.status(500).json({ error: 'Failed to update LicenseIQ entity' });
    }
  });

  // Delete LicenseIQ entity
  app.delete('/api/licenseiq-entities/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteLicenseiqEntity(id);
      console.log(`🗑️ [LICENSEIQ ENTITIES] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [LICENSEIQ ENTITIES] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete LicenseIQ entity' });
    }
  });

  // Get fields for a LicenseIQ entity
  app.get('/api/licenseiq-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId } = req.query;
      if (!entityId) {
        return res.status(400).json({ error: 'Entity ID is required' });
      }
      const fields = await storage.getLicenseiqFieldsByEntity(entityId as string);
      res.json({ fields });
    } catch (error) {
      console.error('❌ [LICENSEIQ FIELDS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve LicenseIQ fields' });
    }
  });

  // Create new LicenseIQ field
  app.post('/api/licenseiq-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const field = await storage.createLicenseiqField(req.body);
      console.log(`✅ [LICENSEIQ FIELDS] Created: ${field.fieldName}`);
      res.json(field);
    } catch (error) {
      console.error('❌ [LICENSEIQ FIELDS] Create error:', error);
      res.status(500).json({ error: 'Failed to create LicenseIQ field' });
    }
  });

  // Update LicenseIQ field
  app.patch('/api/licenseiq-fields/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const field = await storage.updateLicenseiqField(id, req.body);
      console.log(`✏️ [LICENSEIQ FIELDS] Updated: ${field.fieldName}`);
      res.json(field);
    } catch (error) {
      console.error('❌ [LICENSEIQ FIELDS] Update error:', error);
      res.status(500).json({ error: 'Failed to update LicenseIQ field' });
    }
  });

  // Delete LicenseIQ field
  app.delete('/api/licenseiq-fields/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteLicenseiqField(id);
      console.log(`🗑️ [LICENSEIQ FIELDS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [LICENSEIQ FIELDS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete LicenseIQ field' });
    }
  });

  // Get sample data from LicenseIQ entity records (items/vendors tables)
  app.get('/api/licenseiq-entity-records', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId, limit } = req.query;
      if (!entityId) {
        return res.status(400).json({ error: 'entityId is required' });
      }
      
      // Get the entity to determine which table to query
      const entity = await storage.getLicenseiqEntity(entityId as string);
      if (!entity) {
        return res.status(404).json({ error: 'LicenseIQ entity not found' });
      }
      
      const context = {
        activeContext: req.user?.activeContext || null,
        isAdmin: req.user?.role === 'admin' || req.user?.role === 'system_admin'
      };
      
      // Query the appropriate table based on entity name
      let records: any[] = [];
      const limitNum = limit ? parseInt(limit as string, 10) : 10;
      
      if (entity.name.toLowerCase().includes('item')) {
        records = await storage.getItemsSample(context, limitNum);
      } else if (entity.name.toLowerCase().includes('vendor')) {
        records = await storage.getVendorsSample(context, limitNum);
      }
      
      res.json({ records });
    } catch (error) {
      console.error('❌ [LICENSEIQ RECORDS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve LicenseIQ entity records' });
    }
  });

  // Seed standard fields for all 25 entities
  app.post('/api/licenseiq-fields/seed', isAuthenticated, async (req: any, res: Response) => {
    try {
      console.log('🌱 [LICENSEIQ FIELDS] Starting field seeding...');
      
      const entities = await storage.getAllLicenseiqEntities();
      const entityMap = new Map(entities.map(e => [e.technicalName, e.id]));
      
      // Standard field definitions for all 28 entities
      const standardFields: Record<string, Array<{fieldName: string; dataType: string; isRequired: boolean; description?: string}>> = {
        // Organization Hierarchy entities (3)
        companies: [
          { fieldName: 'id', dataType: 'text', isRequired: true, description: 'Unique company identifier (UUID)' },
          { fieldName: 'code', dataType: 'text', isRequired: true, description: 'Company code (unique)' },
          { fieldName: 'name', dataType: 'text', isRequired: true, description: 'Company name' },
          { fieldName: 'description', dataType: 'text', isRequired: false, description: 'Company description' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Status: A(Active), I(Inactive), D(Deleted)' },
          { fieldName: 'createdBy', dataType: 'text', isRequired: true, description: 'User ID who created this record' },
          { fieldName: 'creationDate', dataType: 'date', isRequired: true, description: 'Record creation timestamp' },
          { fieldName: 'lastUpdatedBy', dataType: 'text', isRequired: false, description: 'User ID who last updated this record' },
          { fieldName: 'lastUpdateDate', dataType: 'date', isRequired: false, description: 'Last update timestamp' },
        ],
        business_units: [
          { fieldName: 'id', dataType: 'text', isRequired: true, description: 'Unique business unit identifier (UUID)' },
          { fieldName: 'companyId', dataType: 'text', isRequired: true, description: 'Parent company ID (foreign key)' },
          { fieldName: 'code', dataType: 'text', isRequired: true, description: 'Business unit code (unique)' },
          { fieldName: 'name', dataType: 'text', isRequired: true, description: 'Business unit name' },
          { fieldName: 'description', dataType: 'text', isRequired: false, description: 'Business unit description' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Status: A(Active), I(Inactive), D(Deleted)' },
          { fieldName: 'createdBy', dataType: 'text', isRequired: true, description: 'User ID who created this record' },
          { fieldName: 'creationDate', dataType: 'date', isRequired: true, description: 'Record creation timestamp' },
          { fieldName: 'lastUpdatedBy', dataType: 'text', isRequired: false, description: 'User ID who last updated this record' },
          { fieldName: 'lastUpdateDate', dataType: 'date', isRequired: false, description: 'Last update timestamp' },
        ],
        locations: [
          { fieldName: 'id', dataType: 'text', isRequired: true, description: 'Unique location identifier (UUID)' },
          { fieldName: 'businessUnitId', dataType: 'text', isRequired: true, description: 'Parent business unit ID (foreign key)' },
          { fieldName: 'code', dataType: 'text', isRequired: true, description: 'Location code (unique)' },
          { fieldName: 'name', dataType: 'text', isRequired: true, description: 'Location name' },
          { fieldName: 'description', dataType: 'text', isRequired: false, description: 'Location description' },
          { fieldName: 'address', dataType: 'text', isRequired: false, description: 'Physical address' },
          { fieldName: 'city', dataType: 'text', isRequired: false, description: 'City' },
          { fieldName: 'state', dataType: 'text', isRequired: false, description: 'State/Province' },
          { fieldName: 'country', dataType: 'text', isRequired: false, description: 'Country' },
          { fieldName: 'postalCode', dataType: 'text', isRequired: false, description: 'Postal/ZIP code' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Status: A(Active), I(Inactive), D(Deleted)' },
          { fieldName: 'createdBy', dataType: 'text', isRequired: true, description: 'User ID who created this record' },
          { fieldName: 'creationDate', dataType: 'date', isRequired: true, description: 'Record creation timestamp' },
          { fieldName: 'lastUpdatedBy', dataType: 'text', isRequired: false, description: 'User ID who last updated this record' },
          { fieldName: 'lastUpdateDate', dataType: 'date', isRequired: false, description: 'Last update timestamp' },
        ],
        customers_parties: [
          { fieldName: 'customerCode', dataType: 'text', isRequired: true, description: 'Unique customer code' },
          { fieldName: 'customerName', dataType: 'text', isRequired: true, description: 'Customer full name' },
          { fieldName: 'email', dataType: 'text', isRequired: false, description: 'Customer email address' },
          { fieldName: 'phone', dataType: 'text', isRequired: false, description: 'Contact phone number' },
          { fieldName: 'category', dataType: 'text', isRequired: false, description: 'Customer category' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        items: [
          { fieldName: 'itemCode', dataType: 'text', isRequired: true, description: 'Unique item code' },
          { fieldName: 'itemName', dataType: 'text', isRequired: true, description: 'Item description' },
          { fieldName: 'category', dataType: 'text', isRequired: false, description: 'Item category' },
          { fieldName: 'unitPrice', dataType: 'number', isRequired: false, description: 'Standard unit price' },
          { fieldName: 'uom', dataType: 'text', isRequired: false, description: 'Unit of measure' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        item_category: [
          { fieldName: 'categoryCode', dataType: 'text', isRequired: true, description: 'Category code' },
          { fieldName: 'categoryName', dataType: 'text', isRequired: true, description: 'Category name' },
          { fieldName: 'parentCategory', dataType: 'text', isRequired: false, description: 'Parent category code' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        item_class: [
          { fieldName: 'classCode', dataType: 'text', isRequired: true, description: 'Class code' },
          { fieldName: 'className', dataType: 'text', isRequired: true, description: 'Class name' },
          { fieldName: 'description', dataType: 'text', isRequired: false, description: 'Class description' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        item_catalog: [
          { fieldName: 'catalogCode', dataType: 'text', isRequired: true, description: 'Catalog code' },
          { fieldName: 'catalogName', dataType: 'text', isRequired: true, description: 'Catalog name' },
          { fieldName: 'effectiveDate', dataType: 'date', isRequired: false, description: 'Effective from date' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        item_structures: [
          { fieldName: 'structureCode', dataType: 'text', isRequired: true, description: 'Structure code' },
          { fieldName: 'parentItem', dataType: 'text', isRequired: true, description: 'Parent item code' },
          { fieldName: 'childItem', dataType: 'text', isRequired: true, description: 'Child item code' },
          { fieldName: 'quantity', dataType: 'number', isRequired: true, description: 'Component quantity' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        customer_sites: [
          { fieldName: 'siteCode', dataType: 'text', isRequired: true, description: 'Site code' },
          { fieldName: 'customerCode', dataType: 'text', isRequired: true, description: 'Customer code' },
          { fieldName: 'siteName', dataType: 'text', isRequired: true, description: 'Site name' },
          { fieldName: 'address', dataType: 'text', isRequired: false, description: 'Site address' },
          { fieldName: 'city', dataType: 'text', isRequired: false, description: 'City' },
          { fieldName: 'country', dataType: 'text', isRequired: false, description: 'Country' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        customer_site_uses: [
          { fieldName: 'siteUseCode', dataType: 'text', isRequired: true, description: 'Site use code' },
          { fieldName: 'siteCode', dataType: 'text', isRequired: true, description: 'Site code' },
          { fieldName: 'useType', dataType: 'text', isRequired: true, description: 'Use type (Bill-To, Ship-To)' },
          { fieldName: 'isPrimary', dataType: 'boolean', isRequired: false, description: 'Primary site flag' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        suppliers_vendors: [
          { fieldName: 'supplierCode', dataType: 'text', isRequired: true, description: 'Supplier code' },
          { fieldName: 'supplierName', dataType: 'text', isRequired: true, description: 'Supplier name' },
          { fieldName: 'email', dataType: 'text', isRequired: false, description: 'Contact email' },
          { fieldName: 'phone', dataType: 'text', isRequired: false, description: 'Contact phone' },
          { fieldName: 'category', dataType: 'text', isRequired: false, description: 'Supplier category' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        supplier_sites: [
          { fieldName: 'siteCode', dataType: 'text', isRequired: true, description: 'Site code' },
          { fieldName: 'supplierCode', dataType: 'text', isRequired: true, description: 'Supplier code' },
          { fieldName: 'siteName', dataType: 'text', isRequired: true, description: 'Site name' },
          { fieldName: 'address', dataType: 'text', isRequired: false, description: 'Site address' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        payment_terms: [
          { fieldName: 'code', dataType: 'text', isRequired: true, description: 'Terms code' },
          { fieldName: 'name', dataType: 'text', isRequired: true, description: 'Terms name' },
          { fieldName: 'dueDays', dataType: 'number', isRequired: true, description: 'Due in days' },
          { fieldName: 'discountPercent', dataType: 'number', isRequired: false, description: 'Discount percentage' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        organizations: [
          { fieldName: 'orgCode', dataType: 'text', isRequired: true, description: 'Organization code' },
          { fieldName: 'orgName', dataType: 'text', isRequired: true, description: 'Organization name' },
          { fieldName: 'parentOrg', dataType: 'text', isRequired: false, description: 'Parent organization code' },
          { fieldName: 'level', dataType: 'number', isRequired: false, description: 'Hierarchy level' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        business_units: [
          { fieldName: 'buCode', dataType: 'text', isRequired: true, description: 'Business unit code' },
          { fieldName: 'buName', dataType: 'text', isRequired: true, description: 'Business unit name' },
          { fieldName: 'orgCode', dataType: 'text', isRequired: false, description: 'Organization code' },
          { fieldName: 'manager', dataType: 'text', isRequired: false, description: 'Manager name' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        chart_of_accounts: [
          { fieldName: 'accountCode', dataType: 'text', isRequired: true, description: 'GL account code' },
          { fieldName: 'accountName', dataType: 'text', isRequired: true, description: 'Account name' },
          { fieldName: 'accountType', dataType: 'text', isRequired: true, description: 'Account type (Asset/Liability/Revenue/Expense)' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        sales_reps: [
          { fieldName: 'repCode', dataType: 'text', isRequired: true, description: 'Sales rep code' },
          { fieldName: 'repName', dataType: 'text', isRequired: true, description: 'Sales rep name' },
          { fieldName: 'email', dataType: 'text', isRequired: false, description: 'Email address' },
          { fieldName: 'territory', dataType: 'text', isRequired: false, description: 'Sales territory' },
          { fieldName: 'isActive', dataType: 'boolean', isRequired: true, description: 'Active status' },
        ],
        employee_master: [
          { fieldName: 'empCode', dataType: 'text', isRequired: true, description: 'Employee code' },
          { fieldName: 'empName', dataType: 'text', isRequired: true, description: 'Employee name' },
          { fieldName: 'department', dataType: 'text', isRequired: false, description: 'Department' },
          { fieldName: 'position', dataType: 'text', isRequired: false, description: 'Job position' },
          { fieldName: 'hireDate', dataType: 'date', isRequired: false, description: 'Hire date' },
        ],
        sales_orders: [
          { fieldName: 'orderNumber', dataType: 'text', isRequired: true, description: 'Sales order number' },
          { fieldName: 'customerCode', dataType: 'text', isRequired: true, description: 'Customer code' },
          { fieldName: 'orderDate', dataType: 'date', isRequired: true, description: 'Order date' },
          { fieldName: 'totalAmount', dataType: 'number', isRequired: true, description: 'Order total amount' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Order status' },
          { fieldName: 'salesRep', dataType: 'text', isRequired: false, description: 'Sales rep code' },
        ],
        sales_order_lines: [
          { fieldName: 'lineNumber', dataType: 'number', isRequired: true, description: 'Line number' },
          { fieldName: 'orderNumber', dataType: 'text', isRequired: true, description: 'Sales order number' },
          { fieldName: 'itemCode', dataType: 'text', isRequired: true, description: 'Item code' },
          { fieldName: 'quantity', dataType: 'number', isRequired: true, description: 'Ordered quantity' },
          { fieldName: 'unitPrice', dataType: 'number', isRequired: true, description: 'Unit price' },
          { fieldName: 'lineTotal', dataType: 'number', isRequired: true, description: 'Line total' },
        ],
        ar_invoices: [
          { fieldName: 'invoiceNumber', dataType: 'text', isRequired: true, description: 'Invoice number' },
          { fieldName: 'customerCode', dataType: 'text', isRequired: true, description: 'Customer code' },
          { fieldName: 'invoiceDate', dataType: 'date', isRequired: true, description: 'Invoice date' },
          { fieldName: 'amount', dataType: 'number', isRequired: true, description: 'Invoice amount' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Invoice status' },
          { fieldName: 'dueDate', dataType: 'date', isRequired: false, description: 'Payment due date' },
        ],
        ar_invoice_lines: [
          { fieldName: 'lineNumber', dataType: 'number', isRequired: true, description: 'Line number' },
          { fieldName: 'invoiceNumber', dataType: 'text', isRequired: true, description: 'Invoice number' },
          { fieldName: 'description', dataType: 'text', isRequired: true, description: 'Line description' },
          { fieldName: 'amount', dataType: 'number', isRequired: true, description: 'Line amount' },
          { fieldName: 'quantity', dataType: 'number', isRequired: true, description: 'Quantity' },
        ],
        ap_invoices: [
          { fieldName: 'invoiceNumber', dataType: 'text', isRequired: true, description: 'AP invoice number' },
          { fieldName: 'supplierCode', dataType: 'text', isRequired: true, description: 'Supplier code' },
          { fieldName: 'invoiceDate', dataType: 'date', isRequired: true, description: 'Invoice date' },
          { fieldName: 'amount', dataType: 'number', isRequired: true, description: 'Invoice amount' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'Status' },
          { fieldName: 'dueDate', dataType: 'date', isRequired: false, description: 'Payment due date' },
        ],
        ap_invoice_lines: [
          { fieldName: 'lineNumber', dataType: 'number', isRequired: true, description: 'Line number' },
          { fieldName: 'invoiceNumber', dataType: 'text', isRequired: true, description: 'AP invoice number' },
          { fieldName: 'description', dataType: 'text', isRequired: true, description: 'Line description' },
          { fieldName: 'amount', dataType: 'number', isRequired: true, description: 'Line amount' },
          { fieldName: 'quantity', dataType: 'number', isRequired: true, description: 'Quantity' },
        ],
        ap_invoice_payments: [
          { fieldName: 'paymentNumber', dataType: 'text', isRequired: true, description: 'Payment number' },
          { fieldName: 'invoiceNumber', dataType: 'text', isRequired: true, description: 'AP invoice number' },
          { fieldName: 'paymentDate', dataType: 'date', isRequired: true, description: 'Payment date' },
          { fieldName: 'paymentAmount', dataType: 'number', isRequired: true, description: 'Payment amount' },
          { fieldName: 'paymentMethod', dataType: 'text', isRequired: false, description: 'Payment method' },
        ],
        purchase_orders: [
          { fieldName: 'poNumber', dataType: 'text', isRequired: true, description: 'PO number' },
          { fieldName: 'supplierCode', dataType: 'text', isRequired: true, description: 'Supplier code' },
          { fieldName: 'orderDate', dataType: 'date', isRequired: true, description: 'Order date' },
          { fieldName: 'totalAmount', dataType: 'number', isRequired: true, description: 'Total amount' },
          { fieldName: 'status', dataType: 'text', isRequired: true, description: 'PO status' },
        ],
        purchase_order_lines: [
          { fieldName: 'lineNumber', dataType: 'number', isRequired: true, description: 'Line number' },
          { fieldName: 'poNumber', dataType: 'text', isRequired: true, description: 'PO number' },
          { fieldName: 'itemCode', dataType: 'text', isRequired: true, description: 'Item code' },
          { fieldName: 'quantity', dataType: 'number', isRequired: true, description: 'Order quantity' },
          { fieldName: 'unitPrice', dataType: 'number', isRequired: true, description: 'Unit price' },
          { fieldName: 'lineTotal', dataType: 'number', isRequired: true, description: 'Line total' },
        ],
        contract_terms: [
          { fieldName: 'termCode', dataType: 'text', isRequired: true, description: 'Term code' },
          { fieldName: 'termName', dataType: 'text', isRequired: true, description: 'Term name' },
          { fieldName: 'description', dataType: 'text', isRequired: false, description: 'Term description' },
          { fieldName: 'isStandard', dataType: 'boolean', isRequired: true, description: 'Standard term flag' },
        ],
      };

      let totalCreated = 0;
      let totalSkipped = 0;

      for (const [technicalName, fieldDefs] of Object.entries(standardFields)) {
        const entityId = entityMap.get(technicalName);
        if (!entityId) {
          console.log(`⚠️ [FIELD SEED] Entity not found: ${technicalName}`);
          continue;
        }

        // Check if fields already exist
        const existingFields = await storage.getLicenseiqFieldsByEntity(entityId);
        if (existingFields.length > 0) {
          console.log(`⏭️ [FIELD SEED] ${technicalName} already has ${existingFields.length} fields, skipping`);
          totalSkipped++;
          continue;
        }

        // Create all fields for this entity
        for (const fieldDef of fieldDefs) {
          try {
            await storage.createLicenseiqField({
              entityId,
              ...fieldDef
            });
            totalCreated++;
          } catch (err) {
            console.error(`❌ [FIELD SEED] Failed to create ${fieldDef.fieldName} for ${technicalName}:`, err);
          }
        }
        console.log(`✅ [FIELD SEED] Created ${fieldDefs.length} fields for ${technicalName}`);
      }

      console.log(`🌱 [FIELD SEED] Complete: ${totalCreated} fields created, ${totalSkipped} entities skipped`);
      res.json({ 
        created: totalCreated, 
        skipped: totalSkipped,
        message: `Successfully seeded ${totalCreated} standard fields across ${Object.keys(standardFields).length - totalSkipped} entities` 
      });
    } catch (error) {
      console.error('❌ [FIELD SEED] Error:', error);
      res.status(500).json({ error: 'Failed to seed standard fields' });
    }
  });

  // ==========================================
  // LICENSEIQ ENTITY RECORDS ROUTES
  // ==========================================

  // Get records for a LicenseIQ entity
  app.get('/api/licenseiq-records', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId } = req.query;
      if (!entityId) {
        return res.status(400).json({ error: 'Entity ID is required' });
      }
      const records = await storage.getLicenseiqEntityRecordsByEntity(entityId as string);
      res.json({ records });
    } catch (error) {
      console.error('❌ [LICENSEIQ RECORDS] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve records' });
    }
  });

  // Create new record
  app.post('/api/licenseiq-records', isAuthenticated, async (req: any, res: Response) => {
    try {
      const validated = insertLicenseiqEntityRecordSchema.parse({
        ...req.body,
        createdBy: req.user.id
      });
      const record = await storage.createLicenseiqEntityRecord(validated);
      console.log(`✅ [LICENSEIQ RECORDS] Created record for entity: ${validated.entityId}`);
      res.json(record);
    } catch (error) {
      console.error('❌ [LICENSEIQ RECORDS] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create record' });
    }
  });

  // Update record
  app.patch('/api/licenseiq-records/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      const validated = insertLicenseiqEntityRecordSchema.partial().parse(req.body);
      const record = await storage.updateLicenseiqEntityRecord(id, validated);
      console.log(`✏️ [LICENSEIQ RECORDS] Updated record: ${id}`);
      res.json(record);
    } catch (error) {
      console.error('❌ [LICENSEIQ RECORDS] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update record' });
    }
  });

  // Delete record
  app.delete('/api/licenseiq-records/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { id } = req.params;
      await storage.deleteLicenseiqEntityRecord(id);
      console.log(`🗑️ [LICENSEIQ RECORDS] Deleted: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [LICENSEIQ RECORDS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete record' });
    }
  });

  // ==========================================
  // ENTITY DATA TABLE ROUTES (Direct table access)
  // For entities that have actual database tables defined in LicenseIQ Schema Catalog
  // ==========================================

  // Helper function to validate table access against schema catalog and actual database
  const isValidEntityTable = async (tableName: string): Promise<boolean> => {
    // Validate table name format (alphanumeric and underscores only)
    if (!/^[a-z_][a-z0-9_]*$/i.test(tableName)) {
      return false;
    }
    
    // First check if the table is defined in the LicenseIQ Schema Catalog
    const entityCheck = await pool.query(
      `SELECT id FROM licenseiq_entities WHERE technical_name = $1`,
      [tableName]
    );
    if (entityCheck.rows.length === 0) {
      return false;
    }
    
    // Then verify the actual table exists in the database
    const tableCheck = await pool.query(
      `SELECT table_name FROM information_schema.tables 
       WHERE table_schema = 'public' AND table_name = $1`,
      [tableName]
    );
    return tableCheck.rows.length > 0;
  };

  // Helper function to get valid column names for a table
  const getValidColumns = async (tableName: string): Promise<Set<string>> => {
    const result = await pool.query(`
      SELECT column_name FROM information_schema.columns 
      WHERE table_name = $1 AND table_schema = 'public'
    `, [tableName]);
    return new Set(result.rows.map(r => r.column_name));
  };

  // Helper to sanitize column names (alphanumeric and underscores only)
  const isValidColumnName = (col: string): boolean => {
    return /^[a-z_][a-z0-9_]*$/i.test(col);
  };

  // Get list of entities that have actual database tables
  app.get('/api/entity-data/available-tables', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Get all entities from the schema catalog
      const entities = await pool.query(`SELECT id, name, technical_name, category FROM licenseiq_entities`);
      
      // Check which ones have actual tables in the database
      const tablesWithData = [];
      for (const entity of entities.rows) {
        const tableCheck = await pool.query(
          `SELECT table_name FROM information_schema.tables 
           WHERE table_schema = 'public' AND table_name = $1`,
          [entity.technical_name]
        );
        if (tableCheck.rows.length > 0) {
          tablesWithData.push({
            id: entity.id,
            name: entity.name,
            technicalName: entity.technical_name,
            category: entity.category,
            hasTable: true
          });
        }
      }
      
      res.json({ tables: tablesWithData });
    } catch (error) {
      console.error('❌ [ENTITY DATA] Get available tables error:', error);
      res.status(500).json({ error: 'Failed to retrieve available tables' });
    }
  });

  // Helper to check if a table has multi-tenant columns
  const hasMultiTenantColumns = async (tableName: string): Promise<{ hasCompany: boolean; hasBU: boolean; hasLocation: boolean }> => {
    const columns = await getValidColumns(tableName);
    return {
      hasCompany: columns.has('company_id'),
      hasBU: columns.has('business_unit_id'),
      hasLocation: columns.has('location_id')
    };
  };

  // Get data from an entity's actual database table with multi-tenant filtering, pagination, sorting, and column filters
  app.get('/api/entity-data/:tableName', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { tableName } = req.params;
      const { 
        page = 1, 
        pageSize = 25, 
        company_id, 
        business_unit_id, 
        location_id,
        sortColumn = 'created_at',
        sortDirection = 'desc'
      } = req.query;

      if (!await isValidEntityTable(tableName)) {
        return res.status(400).json({ error: `Table '${tableName}' is not accessible or not defined in schema catalog` });
      }

      // Check if table supports multi-tenant filtering
      const tenantCols = await hasMultiTenantColumns(tableName);
      const validColumns = await getValidColumns(tableName);
      
      // Build WHERE clause for multi-tenant filtering and column filters
      const whereClauses: string[] = [];
      const queryParams: any[] = [];
      let paramIndex = 1;
      
      // Multi-tenant filters
      if (company_id && tenantCols.hasCompany) {
        whereClauses.push(`"company_id" = $${paramIndex}`);
        queryParams.push(company_id);
        paramIndex++;
      }
      if (business_unit_id && tenantCols.hasBU) {
        whereClauses.push(`"business_unit_id" = $${paramIndex}`);
        queryParams.push(business_unit_id);
        paramIndex++;
      }
      if (location_id && tenantCols.hasLocation) {
        whereClauses.push(`"location_id" = $${paramIndex}`);
        queryParams.push(location_id);
        paramIndex++;
      }
      
      // Advanced filters with operators and AND/OR logic
      const { advancedFilters, filterLogic } = req.query;
      const filterClauses: string[] = [];
      
      if (advancedFilters) {
        try {
          const filters = JSON.parse(advancedFilters as string);
          for (const filter of filters) {
            if (!validColumns.has(filter.column)) continue;
            const col = `"${filter.column}"`;
            
            switch (filter.operator) {
              case 'equals':
                filterClauses.push(`CAST(${col} AS TEXT) = $${paramIndex}`);
                queryParams.push(filter.value);
                paramIndex++;
                break;
              case 'contains':
                filterClauses.push(`CAST(${col} AS TEXT) ILIKE $${paramIndex}`);
                queryParams.push(`%${filter.value}%`);
                paramIndex++;
                break;
              case 'starts_with':
                filterClauses.push(`CAST(${col} AS TEXT) ILIKE $${paramIndex}`);
                queryParams.push(`${filter.value}%`);
                paramIndex++;
                break;
              case 'ends_with':
                filterClauses.push(`CAST(${col} AS TEXT) ILIKE $${paramIndex}`);
                queryParams.push(`%${filter.value}`);
                paramIndex++;
                break;
              case 'gt': {
                const numVal = parseFloat(filter.value);
                if (!isNaN(numVal)) {
                  filterClauses.push(`${col} > $${paramIndex}`);
                  queryParams.push(numVal);
                  paramIndex++;
                }
                break;
              }
              case 'gte': {
                const numVal = parseFloat(filter.value);
                if (!isNaN(numVal)) {
                  filterClauses.push(`${col} >= $${paramIndex}`);
                  queryParams.push(numVal);
                  paramIndex++;
                }
                break;
              }
              case 'lt': {
                const numVal = parseFloat(filter.value);
                if (!isNaN(numVal)) {
                  filterClauses.push(`${col} < $${paramIndex}`);
                  queryParams.push(numVal);
                  paramIndex++;
                }
                break;
              }
              case 'lte': {
                const numVal = parseFloat(filter.value);
                if (!isNaN(numVal)) {
                  filterClauses.push(`${col} <= $${paramIndex}`);
                  queryParams.push(numVal);
                  paramIndex++;
                }
                break;
              }
              case 'between': {
                const numVal1 = parseFloat(filter.value);
                const numVal2 = parseFloat(filter.value2);
                if (!isNaN(numVal1) && !isNaN(numVal2)) {
                  filterClauses.push(`${col} BETWEEN $${paramIndex} AND $${paramIndex + 1}`);
                  queryParams.push(numVal1);
                  queryParams.push(numVal2);
                  paramIndex += 2;
                }
                break;
              }
              case 'is_null':
                filterClauses.push(`${col} IS NULL`);
                break;
              case 'not_null':
                filterClauses.push(`${col} IS NOT NULL`);
                break;
            }
          }
        } catch (e) {
          console.error('Failed to parse advanced filters:', e);
        }
        
        // Combine filter clauses with AND or OR
        if (filterClauses.length > 0) {
          const logic = (filterLogic as string)?.toLowerCase() === 'or' ? ' OR ' : ' AND ';
          whereClauses.push(`(${filterClauses.join(logic)})`);
        }
      } else {
        // Simple column filters (filter_columnName=value) - legacy support
        for (const [key, value] of Object.entries(req.query)) {
          if (key.startsWith('filter_') && value) {
            const columnName = key.replace('filter_', '');
            if (validColumns.has(columnName)) {
              whereClauses.push(`CAST("${columnName}" AS TEXT) ILIKE $${paramIndex}`);
              queryParams.push(`%${value}%`);
              paramIndex++;
            }
          }
        }
      }
      
      const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
      
      // Validate and sanitize sort column - use first valid column as fallback instead of hardcoded 'id'
      const validColumnsArray = Array.from(validColumns);
      const defaultSortColumn = validColumnsArray.includes('created_at') ? 'created_at' : (validColumnsArray.includes('id') ? 'id' : validColumnsArray[0] || 'id');
      const safeSortColumn = validColumns.has(sortColumn as string) ? sortColumn : defaultSortColumn;
      const safeSortDirection = (sortDirection as string)?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
      
      // Calculate pagination
      const pageNum = Math.max(1, parseInt(page as string) || 1);
      const pageSizeNum = Math.min(100, Math.max(1, parseInt(pageSize as string) || 25));
      const offset = (pageNum - 1) * pageSizeNum;
      
      // Add pagination params
      queryParams.push(pageSizeNum);
      queryParams.push(offset);
      
      const result = await pool.query(
        `SELECT * FROM "${tableName}" ${whereClause} ORDER BY "${safeSortColumn}" ${safeSortDirection} NULLS LAST LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
        queryParams
      );
      
      // Count query with same filters
      const countParams = queryParams.slice(0, -2); // Remove limit/offset
      const countResult = await pool.query(
        `SELECT COUNT(*) as total FROM "${tableName}" ${whereClause}`,
        countParams
      );
      
      const total = parseInt(countResult.rows[0].total);
      const totalPages = Math.ceil(total / pageSizeNum);
      
      res.json({ 
        records: result.rows, 
        total,
        page: pageNum,
        pageSize: pageSizeNum,
        totalPages,
        filters: { company_id, business_unit_id, location_id },
        tenantColumns: tenantCols
      });
    } catch (error) {
      console.error('❌ [ENTITY DATA] Get error:', error);
      res.status(500).json({ error: 'Failed to retrieve data' });
    }
  });

  // Get columns/schema for an entity's table
  app.get('/api/entity-data/:tableName/columns', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { tableName } = req.params;

      if (!await isValidEntityTable(tableName)) {
        return res.status(400).json({ error: `Table '${tableName}' is not accessible or not defined in schema catalog` });
      }

      const result = await pool.query(`
        SELECT column_name, data_type, is_nullable, column_default 
        FROM information_schema.columns 
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position
      `, [tableName]);
      
      res.json({ columns: result.rows });
    } catch (error) {
      console.error('❌ [ENTITY DATA] Get columns error:', error);
      res.status(500).json({ error: 'Failed to retrieve columns' });
    }
  });

  // Create record in entity table
  app.post('/api/entity-data/:tableName', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { tableName } = req.params;
      const data = req.body;

      if (!await isValidEntityTable(tableName)) {
        return res.status(400).json({ error: `Table '${tableName}' is not accessible or not defined in schema catalog` });
      }

      // Get valid columns for this table to validate against
      const validColumns = await getValidColumns(tableName);
      
      // Filter to only valid columns and sanitize column names
      const columns = Object.keys(data).filter(k => 
        data[k] !== undefined && 
        data[k] !== '' && 
        validColumns.has(k) && 
        isValidColumnName(k) &&
        !['id', 'created_at', 'updated_at'].includes(k) // Exclude auto-generated columns
      );
      
      if (columns.length === 0) {
        return res.status(400).json({ error: 'No valid columns provided' });
      }
      
      const values = columns.map(k => data[k]);
      const placeholders = columns.map((_, i) => `$${i + 1}`);
      
      // Use quoted identifiers to prevent SQL injection
      const quotedColumns = columns.map(c => `"${c}"`).join(', ');
      const query = `INSERT INTO "${tableName}" (${quotedColumns}) VALUES (${placeholders.join(', ')}) RETURNING *`;
      const result = await pool.query(query, values);
      
      console.log(`✅ [ENTITY DATA] Created record in ${tableName}`);
      res.json(result.rows[0]);
    } catch (error) {
      console.error('❌ [ENTITY DATA] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create record' });
    }
  });

  // Update record in entity table
  app.patch('/api/entity-data/:tableName/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { tableName, id } = req.params;
      const data = req.body;

      if (!await isValidEntityTable(tableName)) {
        return res.status(400).json({ error: `Table '${tableName}' is not accessible or not defined in schema catalog` });
      }

      // Get valid columns for this table to validate against
      const validColumns = await getValidColumns(tableName);
      
      // Filter to only valid columns and sanitize
      const columns = Object.keys(data).filter(k => 
        k !== 'id' && 
        data[k] !== undefined && 
        validColumns.has(k) && 
        isValidColumnName(k) &&
        !['created_at', 'updated_at'].includes(k) // Exclude auto-generated columns (updated_at set explicitly below)
      );
      
      if (columns.length === 0) {
        return res.status(400).json({ error: 'No valid columns provided for update' });
      }
      
      // Use quoted identifiers to prevent SQL injection
      const setClause = columns.map((col, i) => `"${col}" = $${i + 1}`).join(', ');
      const values = [...columns.map(k => data[k]), id];
      
      const query = `UPDATE "${tableName}" SET ${setClause}, "updated_at" = NOW() WHERE "id" = $${columns.length + 1} RETURNING *`;
      const result = await pool.query(query, values);
      
      console.log(`✏️ [ENTITY DATA] Updated record in ${tableName}: ${id}`);
      res.json(result.rows[0]);
    } catch (error) {
      console.error('❌ [ENTITY DATA] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update record' });
    }
  });

  // Delete record from entity table
  app.delete('/api/entity-data/:tableName/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { tableName, id } = req.params;

      if (!await isValidEntityTable(tableName)) {
        return res.status(400).json({ error: `Table '${tableName}' is not accessible or not defined in schema catalog` });
      }

      // Use quoted identifiers for safety
      await pool.query(`DELETE FROM "${tableName}" WHERE "id" = $1`, [id]);
      console.log(`🗑️ [ENTITY DATA] Deleted record from ${tableName}: ${id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ENTITY DATA] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete record' });
    }
  });

  // Seed 25 standard entities
  app.post('/api/licenseiq-entities/seed', isAuthenticated, async (req: any, res: Response) => {
    try {
      const entities = [
        // Organization Hierarchy (1) - Locations only (Companies and Business Units managed in Master Data)
        { name: 'Locations', technicalName: 'locations', category: 'Organization Hierarchy', description: 'Physical or logical locations within business units' },
        // Master Data (15)
        { name: 'Customers/Parties', technicalName: 'customers_parties', category: 'Master Data', description: 'Customer and party master data' },
        { name: 'Items', technicalName: 'items', category: 'Master Data', description: 'Item master data' },
        { name: 'Item Category', technicalName: 'item_category', category: 'Master Data', description: 'Item categories' },
        { name: 'Item Class', technicalName: 'item_class', category: 'Master Data', description: 'Item classifications' },
        { name: 'Item Catalog', technicalName: 'item_catalog', category: 'Master Data', description: 'Item catalog' },
        { name: 'Item Structures', technicalName: 'item_structures', category: 'Master Data', description: 'Item structures and hierarchies' },
        { name: 'Customer Sites', technicalName: 'customer_sites', category: 'Master Data', description: 'Customer site locations' },
        { name: 'Customer Site Uses', technicalName: 'customer_site_uses', category: 'Master Data', description: 'Customer site uses' },
        { name: 'Suppliers/Vendors', technicalName: 'suppliers_vendors', category: 'Master Data', description: 'Supplier and vendor master data' },
        { name: 'Supplier Sites', technicalName: 'supplier_sites', category: 'Master Data', description: 'Supplier site locations' },
        { name: 'Payment Terms', technicalName: 'payment_terms', category: 'Master Data', description: 'Payment terms master data' },
        { name: 'Chart of Accounts', technicalName: 'chart_of_accounts', category: 'Master Data', description: 'General ledger accounts' },
        { name: 'Sales Reps', technicalName: 'sales_reps', category: 'Master Data', description: 'Sales representatives' },
        { name: 'Employee Master', technicalName: 'employee_master', category: 'Master Data', description: 'Employee master data' },
        // Transactions (9)
        { name: 'Sales Orders', technicalName: 'sales_orders', category: 'Transactions', description: 'Sales order headers' },
        { name: 'Sales Order Lines', technicalName: 'sales_order_lines', category: 'Transactions', description: 'Sales order line items' },
        { name: 'AR Invoices', technicalName: 'ar_invoices', category: 'Transactions', description: 'Accounts receivable invoice headers' },
        { name: 'AR Invoice Lines', technicalName: 'ar_invoice_lines', category: 'Transactions', description: 'Accounts receivable invoice lines' },
        { name: 'AP Invoices', technicalName: 'ap_invoices', category: 'Transactions', description: 'Accounts payable invoice headers' },
        { name: 'AP Invoice Lines', technicalName: 'ap_invoice_lines', category: 'Transactions', description: 'Accounts payable invoice lines' },
        { name: 'AP Invoice Payments', technicalName: 'ap_invoice_payments', category: 'Transactions', description: 'Accounts payable payments' },
        { name: 'Purchase Orders', technicalName: 'purchase_orders', category: 'Transactions', description: 'Purchase order headers' },
        { name: 'Purchase Order Lines', technicalName: 'purchase_order_lines', category: 'Transactions', description: 'Purchase order line items' },
      ];

      // Get existing entities once
      const existing = await storage.getAllLicenseiqEntities();
      const existingTechnicalNames = new Set(existing.map(e => e.technicalName));

      const created = [];
      for (const entity of entities) {
        try {
          if (!existingTechnicalNames.has(entity.technicalName)) {
            const result = await storage.createLicenseiqEntity(entity);
            created.push(result);
          }
        } catch (err) {
          console.log(`⚠️ Entity ${entity.name} may already exist, skipping`);
        }
      }

      console.log(`🌱 [LICENSEIQ SEED] Created ${created.length} new entities (${entities.length - created.length} already existed)`);
      res.json({ 
        created: created.length, 
        skipped: entities.length - created.length,
        total: entities.length,
        entities: created 
      });
    } catch (error) {
      console.error('❌ [LICENSEIQ SEED] Error:', error);
      res.status(500).json({ error: 'Failed to seed entities' });
    }
  });

  // Seed sample records for all entities
  app.post('/api/licenseiq-sample-data/seed', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Admin/owner authorization check
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const entities = await storage.getAllLicenseiqEntities();
      const sampleData: Record<string, any[]> = {
        // Master Data entities (17)
        customers_parties: [
          { code: 'CUST001', name: 'Acme Corporation', type: 'Customer', status: 'Active', city: 'New York', creditLimit: 100000 },
          { code: 'CUST002', name: 'Tech Solutions Inc', type: 'Customer', status: 'Active', city: 'San Francisco', creditLimit: 150000 },
          { code: 'CUST003', name: 'Global Enterprises', type: 'Customer', status: 'Inactive', city: 'London', creditLimit: 200000 }
        ],
        items: [
          { code: 'ITEM001', name: 'Software License', type: 'Service', category: 'Licensing', unitPrice: 99.99, uom: 'Each' },
          { code: 'ITEM002', name: 'Hardware Device', type: 'Product', category: 'Electronics', unitPrice: 299.99, uom: 'Each' },
          { code: 'ITEM003', name: 'Consulting Service', type: 'Service', category: 'Professional Services', unitPrice: 150.00, uom: 'Hour' }
        ],
        item_category: [
          { code: 'CAT001', name: 'Licensing', description: 'Software and IP licensing', isActive: true },
          { code: 'CAT002', name: 'Electronics', description: 'Electronic devices and equipment', isActive: true },
          { code: 'CAT003', name: 'Professional Services', description: 'Consulting and advisory services', isActive: true }
        ],
        item_class: [
          { code: 'CLASS001', name: 'Digital Products', description: 'Software and digital goods', parentClass: null },
          { code: 'CLASS002', name: 'Physical Products', description: 'Tangible goods', parentClass: null },
          { code: 'CLASS003', name: 'Services', description: 'Professional and managed services', parentClass: null }
        ],
        item_catalog: [
          { catalogCode: 'CATALOG001', catalogName: 'Standard Product Catalog', effectiveDate: '2024-01-01', status: 'Active' },
          { catalogCode: 'CATALOG002', catalogName: 'Premium Services Catalog', effectiveDate: '2024-02-01', status: 'Active' },
          { catalogCode: 'CATALOG003', catalogName: 'Enterprise Solutions Catalog', effectiveDate: '2024-03-01', status: 'Draft' }
        ],
        item_structures: [
          { structureId: 'STR001', parentItem: 'ITEM001', childItem: 'ITEM002', quantity: 1, effectiveDate: '2024-01-01' },
          { structureId: 'STR002', parentItem: 'ITEM002', childItem: 'ITEM003', quantity: 2, effectiveDate: '2024-01-15' },
          { structureId: 'STR003', parentItem: 'ITEM001', childItem: 'ITEM003', quantity: 5, effectiveDate: '2024-02-01' }
        ],
        customer_sites: [
          { siteCode: 'SITE001', customerCode: 'CUST001', siteName: 'HQ Office', address: '123 Main St', city: 'New York', country: 'USA' },
          { siteCode: 'SITE002', customerCode: 'CUST002', siteName: 'Tech Campus', address: '456 Innovation Dr', city: 'San Francisco', country: 'USA' },
          { siteCode: 'SITE003', customerCode: 'CUST003', siteName: 'London Office', address: '789 Thames Rd', city: 'London', country: 'UK' }
        ],
        customer_site_uses: [
          { siteUseId: 'USE001', siteCode: 'SITE001', useType: 'Bill To', isPrimary: true, isActive: true },
          { siteUseId: 'USE002', siteCode: 'SITE002', useType: 'Ship To', isPrimary: true, isActive: true },
          { siteUseId: 'USE003', siteCode: 'SITE003', useType: 'Bill To', isPrimary: false, isActive: true }
        ],
        suppliers_vendors: [
          { code: 'SUPP001', name: 'ABC Supplies Inc', type: 'Supplier', status: 'Approved', paymentTerms: 'Net 30' },
          { code: 'SUPP002', name: 'XYZ Manufacturing', type: 'Manufacturer', status: 'Approved', paymentTerms: 'Net 60' },
          { code: 'SUPP003', name: 'Global Logistics Ltd', type: 'Vendor', status: 'Pending', paymentTerms: 'Net 45' }
        ],
        supplier_sites: [
          { siteCode: 'SUPPSITE001', supplierCode: 'SUPP001', siteName: 'Main Warehouse', city: 'Chicago', country: 'USA' },
          { siteCode: 'SUPPSITE002', supplierCode: 'SUPP002', siteName: 'Factory', city: 'Detroit', country: 'USA' },
          { siteCode: 'SUPPSITE003', supplierCode: 'SUPP003', siteName: 'Distribution Center', city: 'Miami', country: 'USA' }
        ],
        payment_terms: [
          { code: 'NET30', name: 'Net 30 Days', dueDays: 30, discountPercent: 0, isActive: true },
          { code: 'NET60', name: 'Net 60 Days', dueDays: 60, discountPercent: 0, isActive: true },
          { code: '2/10NET30', name: '2% 10, Net 30', dueDays: 30, discountPercent: 2, isActive: true }
        ],
        organizations: [
          { orgCode: 'ORG001', orgName: 'Corporate HQ', parentOrg: null, level: 1, isActive: true },
          { orgCode: 'ORG002', orgName: 'North America Division', parentOrg: 'ORG001', level: 2, isActive: true },
          { orgCode: 'ORG003', orgName: 'EMEA Division', parentOrg: 'ORG001', level: 2, isActive: true }
        ],
        business_units_template: [
          { buCode: 'BU001', buName: 'Sales', orgCode: 'ORG001', manager: 'John Doe', isActive: true },
          { buCode: 'BU002', buName: 'Engineering', orgCode: 'ORG002', manager: 'Jane Smith', isActive: true },
          { buCode: 'BU003', buName: 'Operations', orgCode: 'ORG003', manager: 'Bob Johnson', isActive: true }
        ],
        chart_of_accounts: [
          { accountCode: '1000', accountName: 'Cash', accountType: 'Asset', isActive: true },
          { accountCode: '4000', accountName: 'Revenue', accountType: 'Revenue', isActive: true },
          { accountCode: '5000', accountName: 'Cost of Goods Sold', accountType: 'Expense', isActive: true }
        ],
        sales_reps: [
          { repCode: 'REP001', repName: 'Alice Williams', email: 'alice@example.com', territory: 'East Coast', isActive: true },
          { repCode: 'REP002', repName: 'Bob Davis', email: 'bob@example.com', territory: 'West Coast', isActive: true },
          { repCode: 'REP003', repName: 'Carol Brown', email: 'carol@example.com', territory: 'Central', isActive: true }
        ],
        employee_master: [
          { empCode: 'EMP001', empName: 'Michael Chen', department: 'Engineering', position: 'Senior Engineer', hireDate: '2020-03-15' },
          { empCode: 'EMP002', empName: 'Sarah Martinez', department: 'Sales', position: 'Sales Manager', hireDate: '2019-07-22' },
          { empCode: 'EMP003', empName: 'David Lee', department: 'Operations', position: 'Operations Analyst', hireDate: '2021-01-10' }
        ],
        // Transaction entities (9)
        sales_orders: [
          { orderNumber: 'SO-001', customerCode: 'CUST001', orderDate: '2024-01-15', totalAmount: 5000, status: 'Completed', salesRep: 'REP001' },
          { orderNumber: 'SO-002', customerCode: 'CUST002', orderDate: '2024-02-20', totalAmount: 7500, status: 'Processing', salesRep: 'REP002' },
          { orderNumber: 'SO-003', customerCode: 'CUST003', orderDate: '2024-03-10', totalAmount: 3200, status: 'Pending', salesRep: 'REP003' }
        ],
        sales_order_lines: [
          { lineNumber: 1, orderNumber: 'SO-001', itemCode: 'ITEM001', quantity: 50, unitPrice: 99.99, lineTotal: 4999.50 },
          { lineNumber: 1, orderNumber: 'SO-002', itemCode: 'ITEM002', quantity: 25, unitPrice: 299.99, lineTotal: 7499.75 },
          { lineNumber: 1, orderNumber: 'SO-003', itemCode: 'ITEM003', quantity: 20, unitPrice: 150.00, lineTotal: 3000.00 }
        ],
        ar_invoices: [
          { invoiceNumber: 'INV-2024-001', customerCode: 'CUST001', invoiceDate: '2024-01-20', amount: 5000, status: 'Paid', dueDate: '2024-02-19' },
          { invoiceNumber: 'INV-2024-002', customerCode: 'CUST002', invoiceDate: '2024-02-25', amount: 7500, status: 'Outstanding', dueDate: '2024-03-26' },
          { invoiceNumber: 'INV-2024-003', customerCode: 'CUST003', invoiceDate: '2024-03-15', amount: 3200, status: 'Overdue', dueDate: '2024-04-14' }
        ],
        ar_invoice_lines: [
          { lineNumber: 1, invoiceNumber: 'INV-2024-001', description: 'Software License', amount: 5000, quantity: 50 },
          { lineNumber: 1, invoiceNumber: 'INV-2024-002', description: 'Hardware Device', amount: 7500, quantity: 25 },
          { lineNumber: 1, invoiceNumber: 'INV-2024-003', description: 'Consulting Service', amount: 3000, quantity: 20 }
        ],
        ap_invoices: [
          { invoiceNumber: 'AP-2024-001', supplierCode: 'SUPP001', invoiceDate: '2024-01-10', amount: 2500, status: 'Paid', dueDate: '2024-02-09' },
          { invoiceNumber: 'AP-2024-002', supplierCode: 'SUPP002', invoiceDate: '2024-02-15', amount: 4800, status: 'Pending', dueDate: '2024-04-15' },
          { invoiceNumber: 'AP-2024-003', supplierCode: 'SUPP003', invoiceDate: '2024-03-05', amount: 3100, status: 'Approved', dueDate: '2024-04-19' }
        ],
        ap_invoice_lines: [
          { lineNumber: 1, invoiceNumber: 'AP-2024-001', description: 'Office Supplies', amount: 2500, quantity: 100 },
          { lineNumber: 1, invoiceNumber: 'AP-2024-002', description: 'Manufacturing Parts', amount: 4800, quantity: 200 },
          { lineNumber: 1, invoiceNumber: 'AP-2024-003', description: 'Shipping Services', amount: 3100, quantity: 50 }
        ],
        ap_invoice_payments: [
          { paymentNumber: 'PAY-001', invoiceNumber: 'AP-2024-001', paymentDate: '2024-02-09', paymentAmount: 2500, paymentMethod: 'Wire Transfer' },
          { paymentNumber: 'PAY-002', invoiceNumber: 'AP-2024-002', paymentDate: '2024-03-15', paymentAmount: 2400, paymentMethod: 'Check' },
          { paymentNumber: 'PAY-003', invoiceNumber: 'AP-2024-003', paymentDate: '2024-03-20', paymentAmount: 3100, paymentMethod: 'ACH' }
        ],
        purchase_orders: [
          { poNumber: 'PO-001', supplierCode: 'SUPP001', orderDate: '2024-01-05', totalAmount: 2500, status: 'Received' },
          { poNumber: 'PO-002', supplierCode: 'SUPP002', orderDate: '2024-02-10', totalAmount: 4800, status: 'In Transit' },
          { poNumber: 'PO-003', supplierCode: 'SUPP003', orderDate: '2024-03-01', totalAmount: 3100, status: 'Approved' }
        ],
        purchase_order_lines: [
          { lineNumber: 1, poNumber: 'PO-001', itemCode: 'ITEM001', quantity: 25, unitPrice: 100, lineTotal: 2500 },
          { lineNumber: 1, poNumber: 'PO-002', itemCode: 'ITEM002', quantity: 16, unitPrice: 300, lineTotal: 4800 },
          { lineNumber: 1, poNumber: 'PO-003', itemCode: 'ITEM003', quantity: 20, unitPrice: 155, lineTotal: 3100 }
        ]
      };

      let totalCreated = 0;
      for (const entity of entities) {
        const records = sampleData[entity.technicalName] || [];
        for (const recordData of records) {
          try {
            await storage.createLicenseiqEntityRecord({
              entityId: entity.id,
              recordData,
              createdBy: req.user.id
            });
            totalCreated++;
          } catch (err) {
            console.log(`⚠️ Failed to create record for ${entity.name}`);
          }
        }
      }

      console.log(`🌱 [SAMPLE DATA] Created ${totalCreated} sample records`);
      res.json({ created: totalCreated });
    } catch (error) {
      console.error('❌ [SAMPLE DATA] Error:', error);
      res.status(500).json({ error: 'Failed to seed sample data' });
    }
  });

  // ==========================================
  // INTEGRATION CONNECTIONS ROUTES
  // ==========================================

  // Get all connections with optional filters
  app.get('/api/integration-connections', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { erpSystemId, companyId, businessUnitId, locationId, status } = req.query;
      const connections = await storage.getIntegrationConnections({
        erpSystemId: erpSystemId as string,
        companyId: companyId as string,
        businessUnitId: businessUnitId as string,
        locationId: locationId as string,
        status: status as string
      });
      res.json(connections);
    } catch (error) {
      console.error('❌ [CONNECTIONS] List error:', error);
      res.status(500).json({ error: 'Failed to list connections' });
    }
  });

  // Get single connection
  app.get('/api/integration-connections/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const connection = await storage.getIntegrationConnection(req.params.id);
      if (!connection) {
        return res.status(404).json({ error: 'Connection not found' });
      }
      res.json(connection);
    } catch (error) {
      console.error('❌ [CONNECTIONS] Get error:', error);
      res.status(500).json({ error: 'Failed to get connection' });
    }
  });

  // Create connection
  app.post('/api/integration-connections', isAuthenticated, async (req: any, res: Response) => {
    try {
      const connectionData = {
        ...req.body,
        createdBy: req.user.id
      };
      const connection = await storage.createIntegrationConnection(connectionData);
      console.log(`✅ [CONNECTIONS] Created: ${connection.name}`);
      res.status(201).json(connection);
    } catch (error) {
      console.error('❌ [CONNECTIONS] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create connection' });
    }
  });

  // Update connection
  app.patch('/api/integration-connections/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const connection = await storage.updateIntegrationConnection(req.params.id, req.body);
      console.log(`✏️ [CONNECTIONS] Updated: ${connection.name}`);
      res.json(connection);
    } catch (error) {
      console.error('❌ [CONNECTIONS] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update connection' });
    }
  });

  // Delete connection
  app.delete('/api/integration-connections/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      await storage.deleteIntegrationConnection(req.params.id);
      console.log(`🗑️ [CONNECTIONS] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [CONNECTIONS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete connection' });
    }
  });

  // Test connection - REAL validation with actual HTTP calls
  // Accepts optional credentials in request body for OAuth2 testing (not stored)
  app.post('/api/integration-connections/:id/test', isAuthenticated, async (req: any, res: Response) => {
    try {
      const connection = await storage.getIntegrationConnection(req.params.id);
      if (!connection) {
        return res.status(404).json({ error: 'Connection not found' });
      }

      // Optional: Get credentials from request body for OAuth2 testing
      // These are NOT stored - only used for this test
      const { clientSecret, apiKey, basicUsername, basicPassword } = req.body || {};

      // REAL connection test - make actual HTTP calls to validate
      const startTime = Date.now();
      let testResult = { success: false, message: '', statusCode: 0 };

      try {
        // OAuth2 Client Credentials - attempt real token acquisition if secret provided
        if (connection.authType === 'oauth2_client_credentials' && clientSecret && connection.tokenUrl && connection.clientId) {
          console.log(`🔐 [CONNECTIONS] Testing OAuth2 token endpoint: ${connection.tokenUrl}`);
          
          const tokenBody = new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: connection.clientId,
            client_secret: clientSecret,
            ...(connection.scopes && { scope: connection.scopes })
          });
          
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 15000);
          
          try {
            const tokenResponse = await fetch(connection.tokenUrl, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
              },
              body: tokenBody.toString(),
              signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            testResult.statusCode = tokenResponse.status;
            
            if (tokenResponse.ok) {
              const tokenData = await tokenResponse.json();
              if (tokenData.access_token) {
                testResult = { 
                  success: true, 
                  message: `OAuth2 token obtained successfully (expires in ${tokenData.expires_in || 'unknown'}s)`, 
                  statusCode: 200 
                };
                
                // Test the API endpoint with the token
                if (connection.baseUrl) {
                  try {
                    console.log(`🌐 [CONNECTIONS] Testing API with token: ${connection.baseUrl}`);
                    const apiResponse = await fetch(connection.baseUrl, {
                      method: 'GET',
                      headers: {
                        'Authorization': `Bearer ${tokenData.access_token}`,
                        'Accept': 'application/json'
                      }
                    });
                    testResult.message += ` | API: ${apiResponse.status} ${apiResponse.statusText}`;
                  } catch (apiErr: any) {
                    testResult.message += ` | API test: ${apiErr.message}`;
                  }
                }
              } else {
                testResult = { 
                  success: false, 
                  message: 'Token response missing access_token', 
                  statusCode: tokenResponse.status 
                };
              }
            } else {
              const errorText = await tokenResponse.text();
              let errorMsg = errorText;
              try {
                const errorJson = JSON.parse(errorText);
                errorMsg = errorJson.error_description || errorJson.error || errorText;
              } catch {}
              testResult = { 
                success: false, 
                message: `OAuth2 token failed: ${errorMsg.slice(0, 200)}`, 
                statusCode: tokenResponse.status 
              };
            }
          } catch (fetchErr: any) {
            clearTimeout(timeoutId);
            if (fetchErr.name === 'AbortError') {
              testResult = { success: false, message: 'OAuth2 token request timed out', statusCode: 408 };
            } else {
              testResult = { success: false, message: `OAuth2 error: ${fetchErr.message}`, statusCode: 0 };
            }
          }
        } 
        // API Key authentication - test with provided key
        else if (connection.authType === 'api_key' && apiKey && connection.baseUrl) {
          console.log(`🔑 [CONNECTIONS] Testing API key at: ${connection.baseUrl}`);
          
          const headerName = connection.apiKeyHeader || 'X-API-Key';
          const response = await fetch(connection.baseUrl, {
            method: 'GET',
            headers: {
              [headerName]: apiKey,
              'Accept': 'application/json'
            }
          });
          
          testResult = {
            success: response.ok,
            message: response.ok 
              ? `API key validated - ${response.status} ${response.statusText}` 
              : `API key failed - ${response.status} ${response.statusText}`,
            statusCode: response.status
          };
        }
        // Basic Auth - test with provided credentials or stored credentials
        else if (connection.authType === 'basic_auth' && connection.baseUrl) {
          // Use inline credentials if provided, otherwise use stored credentials
          const testUsername = basicUsername || connection.basicUsername;
          const testPassword = basicPassword || connection.basicPassword;
          
          if (testUsername && testPassword) {
            // Build the full test URL - use healthCheckEndpoint if available
            let testUrl = connection.baseUrl;
            if (connection.healthCheckEndpoint) {
              if (connection.healthCheckEndpoint.startsWith('http')) {
                testUrl = connection.healthCheckEndpoint;
              } else {
                const base = connection.baseUrl.replace(/\/+$/, '');
                const path = connection.healthCheckEndpoint.startsWith('/') 
                  ? connection.healthCheckEndpoint 
                  : '/' + connection.healthCheckEndpoint;
                testUrl = base + path;
              }
            }
            
            console.log(`🔓 [CONNECTIONS] Testing Basic Auth at: ${testUrl}`);
            
            const authHeader = Buffer.from(`${testUsername}:${testPassword}`).toString('base64');
            const response = await fetch(testUrl, {
              method: 'GET',
              headers: {
                'Authorization': `Basic ${authHeader}`,
                'Accept': 'application/json'
              },
              redirect: 'manual' // Don't follow redirects - treat them as auth failures
            });
            
            // For Basic Auth APIs, redirects usually mean auth failed
            if (response.status >= 300 && response.status < 400) {
              testResult = {
                success: false,
                message: `Authentication failed - server redirected (${response.status}). Check credentials.`,
                statusCode: response.status
              };
            } else {
              testResult = {
                success: response.ok,
                message: response.ok 
                  ? `Basic auth validated - ${response.status}` 
                  : `Basic auth failed - ${response.status} ${response.statusText}`,
                statusCode: response.status
              };
            }
          } else {
            testResult = {
              success: false,
              message: 'Basic Auth requires username and password. Please provide credentials.',
              statusCode: 400
            };
          }
        }
        // Default: Test endpoint reachability only
        else {
          // Combine baseUrl with healthCheckEndpoint if it's a relative path
          let testUrl = connection.baseUrl;
          if (connection.healthCheckEndpoint) {
            if (connection.healthCheckEndpoint.startsWith('http')) {
              testUrl = connection.healthCheckEndpoint;
            } else if (connection.baseUrl) {
              // Remove trailing slash from baseUrl and ensure healthCheckEndpoint starts with /
              const base = connection.baseUrl.replace(/\/+$/, '');
              const path = connection.healthCheckEndpoint.startsWith('/') 
                ? connection.healthCheckEndpoint 
                : '/' + connection.healthCheckEndpoint;
              testUrl = base + path;
            }
          }
          
          if (!testUrl) {
            testResult = { 
              success: false, 
              message: 'No endpoint configured for health check', 
              statusCode: 400 
            };
          } else {
            console.log(`🔌 [CONNECTIONS] Testing endpoint reachability: ${testUrl}`);
            
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000);
            
            try {
              const response = await fetch(testUrl, {
                method: 'GET',
                headers: {
                  'Accept': 'application/json',
                  'User-Agent': 'LicenseIQ-HealthCheck/1.0'
                },
                signal: controller.signal
              });
              
              clearTimeout(timeoutId);
              testResult.statusCode = response.status;
              
              if (response.ok) {
                testResult = {
                  success: true,
                  message: `Endpoint reachable - ${response.status} ${response.statusText}`,
                  statusCode: response.status
                };
              } else if (response.status === 401 || response.status === 403) {
                testResult = {
                  success: true,
                  message: `Endpoint reachable (auth required) - ${response.status}`,
                  statusCode: response.status
                };
                // Hint about providing credentials
                if (connection.authType === 'oauth2_client_credentials') {
                  testResult.message += ' | Provide clientSecret to test OAuth2 authentication';
                }
              } else {
                testResult = {
                  success: false,
                  message: `Endpoint error - ${response.status} ${response.statusText}`,
                  statusCode: response.status
                };
              }
            } catch (fetchError: any) {
              clearTimeout(timeoutId);
              if (fetchError.name === 'AbortError') {
                testResult = { success: false, message: 'Connection timed out (10s)', statusCode: 408 };
              } else if (fetchError.cause?.code === 'ENOTFOUND') {
                testResult = { success: false, message: 'DNS resolution failed - hostname not found', statusCode: 0 };
              } else if (fetchError.cause?.code === 'ECONNREFUSED') {
                testResult = { success: false, message: 'Connection refused - server may be down', statusCode: 0 };
              } else {
                testResult = { success: false, message: `Network error: ${fetchError.message}`, statusCode: 0 };
              }
            }
          }
        }
      } catch (err: any) {
        console.error('❌ [CONNECTIONS] Real connection test error:', err);
        testResult = { 
          success: false, 
          message: `Connection test failed: ${err.message || 'Network error'}`, 
          statusCode: 500 
        };
      }

      const latency = Date.now() - startTime;

      // Update connection health status
      await storage.updateConnectionHealth(
        connection.id,
        testResult.success ? 'healthy' : 'error',
        testResult.message
      );

      // Log health event
      await storage.createHealthEvent({
        connectionId: connection.id,
        status: testResult.success ? 'healthy' : 'error',
        statusCode: testResult.statusCode,
        message: testResult.message,
        latencyMs: latency,
        eventType: 'connection_test'
      });

      if (testResult.success) {
        await storage.updateIntegrationConnection(connection.id, {
          lastConnectedAt: new Date()
        });
      }

      console.log(`🔌 [CONNECTIONS] Test result for ${connection.name}: ${testResult.success ? 'SUCCESS' : 'FAILED'}`);
      res.json({
        success: testResult.success,
        message: testResult.message,
        latencyMs: latency,
        statusCode: testResult.statusCode
      });
    } catch (error) {
      console.error('❌ [CONNECTIONS] Test error:', error);
      res.status(500).json({ error: 'Failed to test connection' });
    }
  });

  // Get health events for a connection
  app.get('/api/integration-connections/:id/health', isAuthenticated, async (req: any, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const events = await storage.getHealthEvents(req.params.id, limit);
      res.json(events);
    } catch (error) {
      console.error('❌ [CONNECTIONS] Health events error:', error);
      res.status(500).json({ error: 'Failed to get health events' });
    }
  });

  // ==========================================
  // INTEGRATION ENDPOINT TEMPLATES ROUTES
  // ==========================================

  // Get endpoint templates
  app.get('/api/integration-endpoint-templates', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { erpSystemId, entityId, operationType } = req.query;
      const templates = await storage.getEndpointTemplates({
        erpSystemId: erpSystemId as string,
        entityId: entityId as string,
        operationType: operationType as string
      });
      res.json(templates);
    } catch (error) {
      console.error('❌ [TEMPLATES] List error:', error);
      res.status(500).json({ error: 'Failed to list templates' });
    }
  });

  // Get single template
  app.get('/api/integration-endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }
      res.json(template);
    } catch (error) {
      console.error('❌ [TEMPLATES] Get error:', error);
      res.status(500).json({ error: 'Failed to get template' });
    }
  });

  // Create template
  app.post('/api/integration-endpoint-templates', isAuthenticated, async (req: any, res: Response) => {
    try {
      const template = await storage.createEndpointTemplate(req.body);
      console.log(`✅ [TEMPLATES] Created: ${template.name}`);
      res.status(201).json(template);
    } catch (error) {
      console.error('❌ [TEMPLATES] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create template' });
    }
  });

  // Update template
  app.patch('/api/integration-endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const template = await storage.updateEndpointTemplate(req.params.id, req.body);
      console.log(`✏️ [TEMPLATES] Updated: ${template.name}`);
      res.json(template);
    } catch (error) {
      console.error('❌ [TEMPLATES] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update template' });
    }
  });

  // Delete template
  app.delete('/api/integration-endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      await storage.deleteEndpointTemplate(req.params.id);
      console.log(`🗑️ [TEMPLATES] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [TEMPLATES] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete template' });
    }
  });

  // Extract fields from template sample response
  app.post('/api/integration-endpoint-templates/:id/extract-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }

      const sampleResponse = template.sampleResponse as any;
      if (!sampleResponse) {
        return res.status(400).json({ error: 'Template has no sample response to extract fields from' });
      }

      // Extract fields from sample response
      const extractFields = (obj: any, prefix: string = ''): Array<{ name: string; path: string; type: string; sample: any }> => {
        const fields: Array<{ name: string; path: string; type: string; sample: any }> = [];
        
        if (Array.isArray(obj)) {
          if (obj.length > 0) {
            fields.push(...extractFields(obj[0], prefix));
          }
          return fields;
        }
        
        if (obj && typeof obj === 'object') {
          for (const [key, value] of Object.entries(obj)) {
            const path = prefix ? `${prefix}.${key}` : key;
            
            if (Array.isArray(value)) {
              if (value.length > 0 && typeof value[0] === 'object') {
                fields.push({ name: key, path, type: 'array', sample: `[${value.length} items]` });
                fields.push(...extractFields(value[0], `${path}[]`));
              } else {
                fields.push({ name: key, path, type: 'array', sample: value.slice(0, 3) });
              }
            } else if (value && typeof value === 'object') {
              fields.push(...extractFields(value, path));
            } else {
              let type = typeof value;
              if (value === null) type = 'null';
              else if (type === 'number' && Number.isInteger(value)) type = 'integer';
              else if (type === 'number') type = 'decimal';
              else if (type === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value as string)) type = 'date';
              
              fields.push({ name: key, path, type, sample: value });
            }
          }
        }
        
        return fields;
      };

      const dataPath = template.responseDataPath || '';
      let dataToExtract = sampleResponse;
      
      if (dataPath) {
        const pathParts = dataPath.split('.');
        for (const part of pathParts) {
          if (dataToExtract && dataToExtract[part] !== undefined) {
            dataToExtract = dataToExtract[part];
          }
        }
      }

      const extractedFields = extractFields(dataToExtract);
      
      console.log(`📊 [TEMPLATES] Extracted ${extractedFields.length} fields from template ${template.name}`);
      
      res.json({
        templateId: template.id,
        templateName: template.name,
        entityId: template.erpEntityId,
        dataPath: dataPath,
        fields: extractedFields,
        fieldCount: extractedFields.length
      });
    } catch (error) {
      console.error('❌ [TEMPLATES] Extract fields error:', error);
      res.status(500).json({ error: 'Failed to extract fields from template' });
    }
  });

  // Generate ERP entity fields from template
  app.post('/api/integration-endpoint-templates/:id/generate-entity-fields', isAuthenticated, async (req: any, res: Response) => {
    try {
      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }

      if (!template.erpEntityId) {
        return res.status(400).json({ error: 'Template is not linked to an ERP entity' });
      }

      const sampleResponse = template.sampleResponse as any;
      if (!sampleResponse) {
        return res.status(400).json({ error: 'Template has no sample response to extract fields from' });
      }

      const extractFields = (obj: any, prefix: string = ''): Array<{ name: string; path: string; type: string; sample: any }> => {
        const fields: Array<{ name: string; path: string; type: string; sample: any }> = [];
        
        if (Array.isArray(obj)) {
          if (obj.length > 0) {
            fields.push(...extractFields(obj[0], prefix));
          }
          return fields;
        }
        
        if (obj && typeof obj === 'object') {
          for (const [key, value] of Object.entries(obj)) {
            const path = prefix ? `${prefix}.${key}` : key;
            
            if (Array.isArray(value)) {
              // Skip nested arrays
            } else if (value && typeof value === 'object') {
              fields.push(...extractFields(value, path));
            } else {
              let type = typeof value;
              if (value === null) type = 'string';
              else if (type === 'number' && Number.isInteger(value)) type = 'integer';
              else if (type === 'number') type = 'decimal';
              else if (type === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value as string)) type = 'date';
              else if (type === 'boolean') type = 'boolean';
              else type = 'string';
              
              fields.push({ name: key, path, type, sample: value });
            }
          }
        }
        
        return fields;
      };

      const dataPath = template.responseDataPath || '';
      let dataToExtract = sampleResponse;
      
      if (dataPath) {
        const pathParts = dataPath.split('.');
        for (const part of pathParts) {
          if (dataToExtract && dataToExtract[part] !== undefined) {
            dataToExtract = dataToExtract[part];
          }
        }
      }

      const extractedFields = extractFields(dataToExtract);
      const createdFields: any[] = [];
      const skippedFields: string[] = [];

      // Get existing fields once before the loop
      const existingFields = await storage.getErpFieldsByEntity(template.erpEntityId);

      for (const field of extractedFields) {
        if (field.path.includes('[]')) {
          skippedFields.push(field.path);
          continue;
        }

        try {
          // Check if field already exists by fieldName
          const exists = existingFields.some((f: any) => f.fieldName === field.name || f.fieldName === field.path);
          
          if (exists) {
            skippedFields.push(`${field.name} (already exists)`);
            continue;
          }

          const typeMap: Record<string, string> = {
            'string': 'varchar',
            'integer': 'number',
            'decimal': 'number',
            'number': 'number',
            'boolean': 'boolean',
            'date': 'date',
            'array': 'varchar'
          };

          const fieldData = {
            entityId: template.erpEntityId,
            fieldName: field.name,
            dataType: typeMap[field.type] || 'varchar',
            isRequired: false,
            isPrimaryKey: field.name.toLowerCase().includes('id') && !field.path.includes('.'),
            sampleValues: field.sample !== null && field.sample !== undefined ? String(field.sample).substring(0, 200) : null,
            description: field.path !== field.name ? `JSON path: ${field.path}` : null
          };

          const createdField = await storage.createErpField(fieldData);
          createdFields.push(createdField);
        } catch (fieldError) {
          console.error(`Field creation error for ${field.name}:`, fieldError);
          skippedFields.push(`${field.name} (error)`);
        }
      }

      console.log(`✅ [TEMPLATES] Generated ${createdFields.length} entity fields from template ${template.name}`);
      
      res.json({
        success: true,
        templateId: template.id,
        entityId: template.erpEntityId,
        createdFields: createdFields.length,
        skippedFields: skippedFields,
        fields: createdFields
      });
    } catch (error) {
      console.error('❌ [TEMPLATES] Generate fields error:', error);
      res.status(500).json({ error: 'Failed to generate entity fields' });
    }
  });

  // ==========================================
  // INTEGRATION OPERATIONS ROUTES
  // ==========================================

  // Get operations
  app.get('/api/integration-operations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { connectionId, companyId, operationMode, isEnabled } = req.query;
      const operations = await storage.getIntegrationOperations({
        connectionId: connectionId as string,
        companyId: companyId as string,
        operationMode: operationMode as string,
        isEnabled: isEnabled === 'true' ? true : isEnabled === 'false' ? false : undefined
      });
      res.json(operations);
    } catch (error) {
      console.error('❌ [OPERATIONS] List error:', error);
      res.status(500).json({ error: 'Failed to list operations' });
    }
  });

  // Get single operation
  app.get('/api/integration-operations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const operation = await storage.getIntegrationOperation(req.params.id);
      if (!operation) {
        return res.status(404).json({ error: 'Operation not found' });
      }
      res.json(operation);
    } catch (error) {
      console.error('❌ [OPERATIONS] Get error:', error);
      res.status(500).json({ error: 'Failed to get operation' });
    }
  });

  // Create operation
  app.post('/api/integration-operations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const operationData = {
        ...req.body,
        createdBy: req.user.id
      };
      const operation = await storage.createIntegrationOperation(operationData);
      console.log(`✅ [OPERATIONS] Created: ${operation.name}`);
      res.status(201).json(operation);
    } catch (error) {
      console.error('❌ [OPERATIONS] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create operation' });
    }
  });

  // Update operation
  app.patch('/api/integration-operations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const operation = await storage.updateIntegrationOperation(req.params.id, req.body);
      console.log(`✏️ [OPERATIONS] Updated: ${operation.name}`);
      res.json(operation);
    } catch (error) {
      console.error('❌ [OPERATIONS] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update operation' });
    }
  });

  // Delete operation
  app.delete('/api/integration-operations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      await storage.deleteIntegrationOperation(req.params.id);
      console.log(`🗑️ [OPERATIONS] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [OPERATIONS] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete operation' });
    }
  });

  // Trigger operation (manual run)
  app.post('/api/integration-operations/:id/run', isAuthenticated, async (req: any, res: Response) => {
    try {
      const operation = await storage.getIntegrationOperation(req.params.id);
      if (!operation) {
        return res.status(404).json({ error: 'Operation not found' });
      }

      const dryRun = req.query.dryRun === 'true';
      console.log(`▶️ [OPERATIONS] Manual run triggered for ${operation.name} (dry-run: ${dryRun})`);

      // Update run status to running
      await storage.updateOperationRunStatus(operation.id, 'running');

      // In real implementation, this would trigger the actual sync job
      // For now, simulate a successful run
      const startTime = Date.now();
      
      // Simulate processing time
      await new Promise(resolve => setTimeout(resolve, 500));
      
      const duration = Date.now() - startTime;
      const recordsProcessed = Math.floor(Math.random() * 100) + 1;

      await storage.updateOperationRunStatus(operation.id, 'completed', {
        recordsProcessed,
        recordsFailed: 0,
        durationMs: duration
      });

      res.json({
        success: true,
        dryRun,
        recordsProcessed,
        recordsFailed: 0,
        durationMs: duration
      });
    } catch (error) {
      console.error('❌ [OPERATIONS] Run error:', error);
      res.status(500).json({ error: 'Failed to run operation' });
    }
  });

  // ==========================================
  // LICENSEIQ API ENDPOINTS ROUTES
  // ==========================================

  // Get LicenseIQ API endpoints
  app.get('/api/licenseiq-api-endpoints', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { entityId } = req.query;
      const endpoints = await storage.getLicenseiqApiEndpoints(entityId as string);
      res.json(endpoints);
    } catch (error) {
      console.error('❌ [LIQ API] List error:', error);
      res.status(500).json({ error: 'Failed to list API endpoints' });
    }
  });

  // Get single endpoint
  app.get('/api/licenseiq-api-endpoints/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const endpoint = await storage.getLicenseiqApiEndpoint(req.params.id);
      if (!endpoint) {
        return res.status(404).json({ error: 'Endpoint not found' });
      }
      res.json(endpoint);
    } catch (error) {
      console.error('❌ [LIQ API] Get error:', error);
      res.status(500).json({ error: 'Failed to get endpoint' });
    }
  });

  // Create endpoint
  app.post('/api/licenseiq-api-endpoints', isAuthenticated, async (req: any, res: Response) => {
    try {
      const endpoint = await storage.createLicenseiqApiEndpoint(req.body);
      console.log(`✅ [LIQ API] Created: ${endpoint.name}`);
      res.status(201).json(endpoint);
    } catch (error) {
      console.error('❌ [LIQ API] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create endpoint' });
    }
  });

  // Update endpoint
  app.patch('/api/licenseiq-api-endpoints/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const endpoint = await storage.updateLicenseiqApiEndpoint(req.params.id, req.body);
      console.log(`✏️ [LIQ API] Updated: ${endpoint.name}`);
      res.json(endpoint);
    } catch (error) {
      console.error('❌ [LIQ API] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update endpoint' });
    }
  });

  // Delete endpoint
  app.delete('/api/licenseiq-api-endpoints/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      await storage.deleteLicenseiqApiEndpoint(req.params.id);
      console.log(`🗑️ [LIQ API] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [LIQ API] Delete error:', error);
      res.status(500).json({ error: 'Failed to delete endpoint' });
    }
  });

  // ==========================================
  // NAVIGATION PERMISSIONS ROUTES
  // ==========================================

  // Seed navigation items
  app.post('/api/config/navigation/seed', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const navItems = [
        { itemKey: 'dashboard', itemName: 'Dashboard', href: '/', iconName: 'BarChart3', defaultRoles: ['user', 'analyst', 'admin', 'owner'], sortOrder: 1 },
        { itemKey: 'contracts', itemName: 'Contracts', href: '/contracts', iconName: 'File', defaultRoles: ['user', 'analyst', 'admin', 'owner'], sortOrder: 2 },
        { itemKey: 'upload', itemName: 'Upload', href: '/upload', iconName: 'Upload', defaultRoles: ['user', 'analyst', 'admin', 'owner'], sortOrder: 3 },
        { itemKey: 'sales-data', itemName: 'Sales Data', href: '/sales-upload', iconName: 'Receipt', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 4 },
        { itemKey: 'royalty-calculator', itemName: 'License Fee Calculator', href: '/calculations', iconName: 'Calculator', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 5 },
        { itemKey: 'master-data-mapping', itemName: 'Master Data Mapping', href: '/master-data-mapping', iconName: 'Database', defaultRoles: ['admin', 'owner'], sortOrder: 6 },
        { itemKey: 'erp-catalog', itemName: 'ERP Catalog', href: '/erp-catalog', iconName: 'Database', defaultRoles: ['admin', 'owner'], sortOrder: 7 },
        { itemKey: 'licenseiq-schema', itemName: 'LicenseIQ Schema', href: '/licenseiq-schema', iconName: 'Layers', defaultRoles: ['admin', 'owner'], sortOrder: 8 },
        { itemKey: 'data-management', itemName: 'Data Management', href: '/data-management', iconName: 'Table', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 9 },
        { itemKey: 'master-data', itemName: 'Master Data', href: '/master-data', iconName: 'Building2', defaultRoles: ['admin', 'owner', 'editor'], sortOrder: 10 },
        { itemKey: 'erp-import', itemName: 'ERP Data Import', href: '/erp-import', iconName: 'Upload', defaultRoles: ['admin', 'owner'], sortOrder: 11 },
        { itemKey: 'contract-qna', itemName: 'LIQ AI', href: '/contract-qna', iconName: 'Brain', defaultRoles: ['user', 'analyst', 'admin', 'owner'], sortOrder: 12 },
        { itemKey: 'rag-dashboard', itemName: 'RAG Dashboard', href: '/rag-dashboard', iconName: 'Sparkles', defaultRoles: ['admin', 'owner'], sortOrder: 13 },
        { itemKey: 'analytics', itemName: 'Analytics', href: '/analytics', iconName: 'TrendingUp', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 14 },
        { itemKey: 'reports', itemName: 'Reports', href: '/reports', iconName: 'FileText', defaultRoles: ['analyst', 'admin', 'owner'], sortOrder: 15 },
        { itemKey: 'lead-management', itemName: 'Lead Management', href: '/admin/leads', iconName: 'Mail', defaultRoles: ['admin', 'owner'], sortOrder: 16 },
        { itemKey: 'review-queue', itemName: 'Review Queue', href: '/review-queue', iconName: 'ClipboardCheck', defaultRoles: ['admin', 'owner'], sortOrder: 17 },
        { itemKey: 'user-management', itemName: 'User Management', href: '/users', iconName: 'Users', defaultRoles: ['admin', 'owner'], sortOrder: 18 },
        { itemKey: 'audit-trail', itemName: 'Audit Trail', href: '/audit', iconName: 'History', defaultRoles: ['auditor', 'admin', 'owner'], sortOrder: 19 },
        { itemKey: 'configuration', itemName: 'Configuration', href: '/configuration', iconName: 'Sparkles', defaultRoles: ['admin', 'owner'], sortOrder: 20 },
      ];

      const created = [];
      for (const item of navItems) {
        try {
          const result = await db.insert(navigationPermissions).values(item).onConflictDoUpdate({
            target: navigationPermissions.itemKey,
            set: { itemName: item.itemName, href: item.href, defaultRoles: item.defaultRoles, sortOrder: item.sortOrder, updatedAt: new Date() }
          }).returning();
          created.push(result[0]);
        } catch (err) {
          console.error(`Error seeding nav item ${item.itemKey}:`, err);
        }
      }

      // Enable ALL navigation items for admin role by default
      console.log('🌱 [NAV PERMISSIONS] Enabling all items for admin role...');
      let adminPermissionsCreated = 0;
      for (const item of navItems) {
        try {
          await db.insert(roleNavigationPermissions).values({
            role: 'admin',
            navItemKey: item.itemKey,
            isEnabled: true
          }).onConflictDoNothing();
          adminPermissionsCreated++;
        } catch (err) {
          console.error(`Error creating admin permission for ${item.itemKey}:`, err);
        }
      }

      console.log(`🌱 [NAV PERMISSIONS] Seeded ${created.length} navigation items`);
      console.log(`🌱 [NAV PERMISSIONS] Created ${adminPermissionsCreated} admin role permissions`);
      res.json({ created: created.length, items: created, adminPermissions: adminPermissionsCreated });
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Seed error:', error);
      res.status(500).json({ error: 'Failed to seed navigation items' });
    }
  });

  // Get all navigation items
  app.get('/api/config/navigation', isAuthenticated, async (req: any, res: Response) => {
    try {
      const items = await db.select().from(navigationPermissions).orderBy(navigationPermissions.sortOrder);
      res.json({ items });
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Get error:', error);
      res.status(500).json({ error: 'Failed to get navigation items' });
    }
  });

  // Get role permissions for navigation
  app.get('/api/config/navigation/roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      const permissions = await db.select().from(roleNavigationPermissions);
      res.json({ permissions });
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Get roles error:', error);
      res.status(500).json({ error: 'Failed to get role permissions' });
    }
  });

  // Update role permission for navigation item
  app.post('/api/config/navigation/roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const { role, navItemKey, isEnabled } = req.body;
      
      const existing = await db.select().from(roleNavigationPermissions)
        .where(and(
          eq(roleNavigationPermissions.role, role),
          eq(roleNavigationPermissions.navItemKey, navItemKey)
        ));

      if (existing.length > 0) {
        const updated = await db.update(roleNavigationPermissions)
          .set({ isEnabled, updatedAt: new Date() })
          .where(and(
            eq(roleNavigationPermissions.role, role),
            eq(roleNavigationPermissions.navItemKey, navItemKey)
          ))
          .returning();
        res.json(updated[0]);
      } else {
        const created = await db.insert(roleNavigationPermissions)
          .values({ role, navItemKey, isEnabled })
          .returning();
        res.json(created[0]);
      }
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Update role error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update role permission' });
    }
  });

  // Update default roles for a navigation item
  app.patch('/api/config/navigation/:itemKey/default-roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const { itemKey } = req.params;
      const { role, isDefault } = req.body;

      // Get current item
      const [item] = await db.select().from(navigationPermissions)
        .where(eq(navigationPermissions.itemKey, itemKey));

      if (!item) {
        return res.status(404).json({ error: 'Navigation item not found' });
      }

      // Update defaultRoles array
      let currentRoles = item.defaultRoles || [];
      if (isDefault && !currentRoles.includes(role)) {
        currentRoles = [...currentRoles, role];
      } else if (!isDefault && currentRoles.includes(role)) {
        currentRoles = currentRoles.filter(r => r !== role);
      }

      // Save updated defaultRoles
      const [updated] = await db.update(navigationPermissions)
        .set({ defaultRoles: currentRoles, updatedAt: new Date() })
        .where(eq(navigationPermissions.itemKey, itemKey))
        .returning();

      res.json(updated);
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Update default roles error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update default roles' });
    }
  });

  // Get user's allowed navigation items (dynamic permissions)
  app.get('/api/navigation/allowed', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      
      // Use context role if available, otherwise fall back to global role
      const effectiveRole = req.user.activeContext?.role || req.user.role;

      // Get all active navigation items
      const allItems = await db.select().from(navigationPermissions)
        .where(eq(navigationPermissions.isActive, true))
        .orderBy(navigationPermissions.sortOrder);

      // Get role permissions for effective role (context or global)
      const rolePermissions = await db.select().from(roleNavigationPermissions)
        .where(and(
          eq(roleNavigationPermissions.role, effectiveRole),
          eq(roleNavigationPermissions.isEnabled, true)
        ));

      // Get user-specific overrides
      const userOverrides = await db.select().from(userNavigationOverrides)
        .where(eq(userNavigationOverrides.userId, userId));

      // Build maps for permission checking
      const rolePermissionMap = new Map(rolePermissions.map(p => [p.navItemKey, true]));
      const userOverrideMap = new Map(userOverrides.map(o => [o.navItemKey, o.isEnabled]));
      
      // Get all role permissions (including disabled) to check if explicit override exists
      const allRolePermissions = await db.select().from(roleNavigationPermissions)
        .where(eq(roleNavigationPermissions.role, effectiveRole));
      const explicitRolePermissionMap = new Map(allRolePermissions.map(p => [p.navItemKey, p.isEnabled]));

      // Filter items based on permissions
      const allowedItems = allItems.filter(item => {
        // Priority 1: Check if user has a specific override
        if (userOverrideMap.has(item.itemKey)) {
          return userOverrideMap.get(item.itemKey);
        }
        
        // Priority 2: Check if there's an explicit role permission (Enabled toggle)
        if (explicitRolePermissionMap.has(item.itemKey)) {
          return explicitRolePermissionMap.get(item.itemKey);
        }
        
        // Priority 3: Fall back to defaultRoles (Default Access)
        return item.defaultRoles?.includes(effectiveRole) || false;
      });

      res.json({ items: allowedItems });
    } catch (error) {
      console.error('❌ [NAV PERMISSIONS] Get allowed items error:', error);
      res.status(500).json({ error: 'Failed to get allowed navigation items' });
    }
  });

  // Get user's categorized navigation (NEW: Tree structure with categories)
  app.get('/api/navigation/categorized', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const isSystemAdmin = req.user.isSystemAdmin === true;
      
      // Use context role if available, otherwise fall back to global role (backward compatibility)
      const effectiveRole = req.user.activeContext?.role || req.user.role;

      // Get all active categories
      const categories = await db.select().from(navigationCategories)
        .where(eq(navigationCategories.isActive, true))
        .orderBy(navigationCategories.defaultSortOrder);

      // Get all active navigation items
      const allItems = await db.select().from(navigationPermissions)
        .where(eq(navigationPermissions.isActive, true));

      // Get role permissions based on effective role (context role or global role)
      const rolePermissions = await db.select().from(roleNavigationPermissions)
        .where(and(
          eq(roleNavigationPermissions.role, effectiveRole),
          eq(roleNavigationPermissions.isEnabled, true)
        ));

      // Get user-specific overrides
      const userOverrides = await db.select().from(userNavigationOverrides)
        .where(eq(userNavigationOverrides.userId, userId));

      // Get default item-to-category mappings
      const itemCategoryMappings = await db.select().from(navigationItemCategories);

      // Get user-specific category preferences
      const userPreferences = await db.select().from(userCategoryPreferences)
        .where(eq(userCategoryPreferences.userId, userId));

      // Get user category expanded/collapsed state
      const userCategoryStates = await db.select().from(userCategoryState)
        .where(eq(userCategoryState.userId, userId));

      // Build permission maps
      const userOverrideMap = new Map(userOverrides.map(o => [o.navItemKey, o.isEnabled]));
      
      // Get all role permissions (including disabled) to check if explicit override exists
      const allRolePermissions = await db.select().from(roleNavigationPermissions)
        .where(eq(roleNavigationPermissions.role, effectiveRole));
      const explicitRolePermissionMap = new Map(allRolePermissions.map(p => [p.navItemKey, p.isEnabled]));

      // Filter allowed items based on permissions
      // System Admins get access to ALL navigation items
      const allowedItems = allItems.filter(item => {
        if (isSystemAdmin) {
          return true; // System admins see everything
        }
        
        // Priority 1: Check if user has a specific override
        if (userOverrideMap.has(item.itemKey)) {
          return userOverrideMap.get(item.itemKey);
        }
        
        // Priority 2: Check if there's an explicit role permission (Enabled toggle)
        if (explicitRolePermissionMap.has(item.itemKey)) {
          return explicitRolePermissionMap.get(item.itemKey);
        }
        
        // Priority 3: Fall back to defaultRoles (Default Access)
        return item.defaultRoles?.includes(effectiveRole) || false;
      });

      // Build category state map
      const categoryStateMap = new Map(userCategoryStates.map(s => [s.categoryKey, s.isExpanded]));

      // Build user preference map (custom category assignments and sort order)
      const userPrefMap = new Map(userPreferences.map(p => [p.navItemKey, p]));

      // Build default mapping
      const defaultMappingMap = new Map(itemCategoryMappings.map(m => [m.navItemKey, m]));

      // Organize items into categories
      const categorizedNavigation = categories.map(category => {
        // Find items for this category
        const categoryItems = allowedItems
          .filter(item => {
            // Check user preference first
            const userPref = userPrefMap.get(item.itemKey);
            if (userPref) {
              return userPref.categoryKey === category.categoryKey && userPref.isVisible;
            }
            // Otherwise use default mapping
            const defaultMapping = defaultMappingMap.get(item.itemKey);
            return defaultMapping?.categoryKey === category.categoryKey;
          })
          .map(item => {
            // Get sort order (user preference or default)
            const userPref = userPrefMap.get(item.itemKey);
            const defaultMapping = defaultMappingMap.get(item.itemKey);
            const sortOrder = userPref?.sortOrder ?? defaultMapping?.sortOrder ?? 0;

            return {
              ...item,
              sortOrder
            };
          })
          .sort((a, b) => a.sortOrder - b.sortOrder);

        // Get expanded state (user preference or default)
        const isExpanded = categoryStateMap.has(category.categoryKey)
          ? categoryStateMap.get(category.categoryKey)
          : category.defaultExpanded;

        return {
          ...category,
          isExpanded,
          items: categoryItems
        };
      });

      // Filter out empty categories
      const nonEmptyCategories = categorizedNavigation.filter(cat => cat.items.length > 0);

      res.json({ categories: nonEmptyCategories });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Get categorized navigation error:', error);
      res.status(500).json({ error: 'Failed to get categorized navigation' });
    }
  });

  // Toggle category expanded/collapsed state
  app.patch('/api/navigation/category-state/:categoryKey', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const { categoryKey } = req.params;
      const { isExpanded } = req.body;

      // Check if state exists
      const existing = await db.select().from(userCategoryState)
        .where(and(
          eq(userCategoryState.userId, userId),
          eq(userCategoryState.categoryKey, categoryKey)
        ));

      if (existing.length > 0) {
        // Update existing state
        await db.update(userCategoryState)
          .set({ isExpanded, updatedAt: new Date() })
          .where(and(
            eq(userCategoryState.userId, userId),
            eq(userCategoryState.categoryKey, categoryKey)
          ));
      } else {
        // Create new state
        await db.insert(userCategoryState)
          .values({ userId, categoryKey, isExpanded });
      }

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Toggle category state error:', error);
      res.status(500).json({ error: 'Failed to toggle category state' });
    }
  });

  // Update user's category preferences (drag-and-drop result)
  app.post('/api/navigation/user-preferences', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const { preferences } = req.body; // Array of { navItemKey, categoryKey, sortOrder, isVisible }

      // Delete existing preferences for this user
      await db.delete(userCategoryPreferences)
        .where(eq(userCategoryPreferences.userId, userId));

      // Insert new preferences
      if (preferences && preferences.length > 0) {
        await db.insert(userCategoryPreferences)
          .values(preferences.map((pref: any) => ({
            userId,
            navItemKey: pref.navItemKey,
            categoryKey: pref.categoryKey,
            sortOrder: pref.sortOrder,
            isVisible: pref.isVisible ?? true
          })));
      }

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Update preferences error:', error);
      res.status(500).json({ error: 'Failed to update user preferences' });
    }
  });

  // Save user's custom category display order (ADMIN ONLY)
  app.post('/api/navigation/category-order', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;
      const userRole = req.user.role;
      const { categoryOrder } = req.body; // Array of { categoryKey, sortOrder }

      // AUTHORIZATION: Only admins and owners can reorder categories (system-wide change)
      if (userRole !== 'admin' && userRole !== 'owner') {
        return res.status(403).json({ error: 'Only administrators can reorder navigation categories' });
      }

      if (!categoryOrder || !Array.isArray(categoryOrder)) {
        return res.status(400).json({ error: 'Invalid category order data' });
      }

      // Update default_sort_order for categories based on user's order
      // Note: This updates the global category order, affecting all users
      for (const item of categoryOrder) {
        await db.update(navigationCategories)
          .set({ 
            defaultSortOrder: item.sortOrder,
            updatedAt: new Date()
          })
          .where(eq(navigationCategories.categoryKey, item.categoryKey));
      }

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Update category order error:', error);
      res.status(500).json({ error: 'Failed to update category order' });
    }
  });

  // Reset user preferences to defaults
  app.post('/api/navigation/reset-preferences', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userId = req.user.id;

      // Delete all user preferences and category states
      await db.delete(userCategoryPreferences)
        .where(eq(userCategoryPreferences.userId, userId));
      
      await db.delete(userCategoryState)
        .where(eq(userCategoryState.userId, userId));

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Reset preferences error:', error);
      res.status(500).json({ error: 'Failed to reset preferences' });
    }
  });

  // Create new category (ADMIN ONLY)
  app.post('/api/navigation/categories', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userRole = req.user.role;
      const { categoryKey, categoryName, iconName, isCollapsible, defaultExpanded } = req.body;

      // AUTHORIZATION: Only admins and owners can create categories
      if (userRole !== 'admin' && userRole !== 'owner') {
        return res.status(403).json({ error: 'Only administrators can create navigation categories' });
      }

      // Validate required fields
      if (!categoryKey || !categoryName) {
        return res.status(400).json({ error: 'Category key and name are required' });
      }

      // Check if category already exists
      const existing = await db.select().from(navigationCategories)
        .where(eq(navigationCategories.categoryKey, categoryKey))
        .limit(1);

      if (existing.length > 0) {
        return res.status(400).json({ error: 'Category with this key already exists' });
      }

      // Get max sort order
      const maxOrder = await db.select({ max: sql<number>`MAX(${navigationCategories.defaultSortOrder})` })
        .from(navigationCategories);
      const nextOrder = (maxOrder[0]?.max || 0) + 1;

      // Create category
      await db.insert(navigationCategories).values({
        categoryKey,
        categoryName,
        iconName: iconName || 'Folder',
        isCollapsible: isCollapsible ?? true,
        defaultExpanded: defaultExpanded ?? true,
        defaultSortOrder: nextOrder,
        isActive: true,
      });

      res.json({ success: true, categoryKey });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Create category error:', error);
      res.status(500).json({ error: 'Failed to create category' });
    }
  });

  // Update category (ADMIN ONLY)
  app.patch('/api/navigation/categories/:categoryKey', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userRole = req.user.role;
      const { categoryKey } = req.params;
      const { categoryName, iconName, isCollapsible, defaultExpanded } = req.body;

      // AUTHORIZATION: Only admins and owners can edit categories
      if (userRole !== 'admin' && userRole !== 'owner') {
        return res.status(403).json({ error: 'Only administrators can edit navigation categories' });
      }

      // Update category
      await db.update(navigationCategories)
        .set({ 
          categoryName,
          iconName,
          isCollapsible,
          defaultExpanded,
          updatedAt: new Date()
        })
        .where(eq(navigationCategories.categoryKey, categoryKey));

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Update category error:', error);
      res.status(500).json({ error: 'Failed to update category' });
    }
  });

  // Delete category (ADMIN ONLY)
  app.delete('/api/navigation/categories/:categoryKey', isAuthenticated, async (req: any, res: Response) => {
    try {
      const userRole = req.user.role;
      const { categoryKey } = req.params;

      // AUTHORIZATION: Only admins and owners can delete categories
      if (userRole !== 'admin' && userRole !== 'owner') {
        return res.status(403).json({ error: 'Only administrators can delete navigation categories' });
      }

      // Check if any items are mapped to this category
      const mappedItems = await db.select().from(navigationItemCategories)
        .where(eq(navigationItemCategories.categoryKey, categoryKey))
        .limit(1);

      if (mappedItems.length > 0) {
        return res.status(400).json({ error: 'Cannot delete category with mapped navigation items. Please reassign items first.' });
      }

      // Delete category
      await db.delete(navigationCategories)
        .where(eq(navigationCategories.categoryKey, categoryKey));

      res.json({ success: true });
    } catch (error) {
      console.error('❌ [NAV CATEGORIES] Delete category error:', error);
      res.status(500).json({ error: 'Failed to delete category' });
    }
  });

  // ==========================================
  // MASTER DATA MANAGEMENT ROUTES
  // ==========================================
  
  // Get full hierarchy
  // System admins see all, company admins see only their company's hierarchy
  app.get('/api/master-data/hierarchy', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { status } = req.query;
      const user = await storage.getUser(req.user.id);
      
      // System admins see full hierarchy
      if (isSystemAdmin(user)) {
        const hierarchy = await storage.getMasterDataHierarchy(status as string);
        return res.json({ companies: hierarchy });
      }
      
      // Company admins see only their company's hierarchy
      const companyId = getUserCompanyId(req.user);
      if (!companyId) {
        return res.json({ companies: [] });
      }
      
      const hierarchy = await storage.getMasterDataHierarchy(status as string);
      const filteredHierarchy = hierarchy.filter((company: any) => company.id === companyId);
      res.json({ companies: filteredHierarchy });
    } catch (error: any) {
      console.error('Get hierarchy error:', error);
      res.status(500).json({ error: error.message || 'Failed to get master data hierarchy' });
    }
  });

  // Companies
  // System admins see all companies, company admins see only their company
  app.get('/api/master-data/companies', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { status } = req.query;
      const user = await storage.getUser(req.user.id);
      
      // System admins see all companies
      if (isSystemAdmin(user)) {
        const companies = await storage.getAllCompanies(status as string);
        return res.json(companies);
      }
      
      // Company admins see only their company
      const companyId = getUserCompanyId(req.user);
      if (!companyId) {
        return res.json([]);
      }
      
      const companies = await storage.getAllCompanies(status as string);
      const filteredCompanies = companies.filter((company: any) => company.id === companyId);
      res.json(filteredCompanies);
    } catch (error: any) {
      console.error('Get companies error:', error);
      res.status(500).json({ error: error.message || 'Failed to get companies' });
    }
  });

  // Only system admins can create new companies
  app.post('/api/master-data/companies', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      
      // Only system admins can create companies
      if (!isSystemAdmin(user)) {
        return res.status(403).json({ error: 'Only system administrators can create new companies' });
      }

      const { insertCompanySchema } = await import("@shared/schema");
      const companyData = insertCompanySchema.parse({
        ...req.body,
        createdBy: req.user.id,
        lastUpdatedBy: req.user.id,
      });

      const company = await storage.createCompany(companyData);
      
      await createAuditLog(req, 'create_company', 'company', company.id, {
        companyName: company.companyName,
      });

      res.json(company);
    } catch (error: any) {
      console.error('Create company error:', error);
      res.status(500).json({ error: error.message || 'Failed to create company' });
    }
  });

  // System admins can update any company, company admins can only update their company
  app.patch('/api/master-data/companies/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      // System admins can update any company
      if (!isSystemAdmin(user)) {
        // Company admin must have owner/admin role and can only update their own company
        if (!['admin', 'owner'].includes(contextRole || '')) {
          return res.status(403).json({ error: 'Insufficient permissions' });
        }
        
        const userCompanyId = getUserCompanyId(req.user);
        if (userCompanyId !== req.params.id) {
          return res.status(403).json({ error: 'You can only modify your own company' });
        }
      }

      const company = await storage.updateCompany(req.params.id, req.body, req.user.id);
      
      await createAuditLog(req, 'update_company', 'company', company.id, {
        changes: req.body,
      });

      res.json(company);
    } catch (error: any) {
      console.error('Update company error:', error);
      res.status(500).json({ error: error.message || 'Failed to update company' });
    }
  });

  // Only system admins can delete companies
  app.delete('/api/master-data/companies/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      
      // Only system admins can delete companies
      if (!isSystemAdmin(user)) {
        return res.status(403).json({ error: 'Only system administrators can delete companies' });
      }

      await storage.deleteCompany(req.params.id);
      
      await createAuditLog(req, 'delete_company', 'company', req.params.id, {});

      res.json({ success: true });
    } catch (error: any) {
      console.error('Delete company error:', error);
      res.status(500).json({ error: error.message || 'Failed to delete company' });
    }
  });

  // Business Units
  // Company admins can only access business units in their company
  app.get('/api/master-data/business-units', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { companyId, status } = req.query;
      const user = await storage.getUser(req.user.id);
      
      // For non-system admins, enforce company filtering server-side
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        if (!userCompanyId) {
          return res.json([]); // No company access = no data
        }
        // Ignore client-provided companyId, use server-verified company
        const units = await storage.getBusinessUnitsByCompany(userCompanyId, status as string);
        return res.json(units);
      }
      
      // System admins can query any company
      const units = companyId 
        ? await storage.getBusinessUnitsByCompany(companyId as string, status as string)
        : [];
      res.json(units);
    } catch (error: any) {
      console.error('Get business units error:', error);
      res.status(500).json({ error: error.message || 'Failed to get business units' });
    }
  });

  // Company admins can create business units only in their company
  app.post('/api/master-data/business-units', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      // Check permission level
      if (!isSystemAdmin(user) && !['admin', 'owner', 'editor'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }
      
      // For non-system admins, override companyId with server-verified value
      let companyIdToUse = req.body.companyId;
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        if (!userCompanyId) {
          return res.status(403).json({ error: 'No company context available' });
        }
        // Always use server-verified companyId for non-system admins
        companyIdToUse = userCompanyId;
      }

      const { insertBusinessUnitSchema } = await import("@shared/schema");
      const unitData = insertBusinessUnitSchema.parse({
        ...req.body,
        companyId: companyIdToUse, // Override with server-verified value
        createdBy: req.user.id,
        lastUpdatedBy: req.user.id,
      });

      const unit = await storage.createBusinessUnit(unitData);
      
      await createAuditLog(req, 'create_business_unit', 'business_unit', unit.id, {
        orgName: unit.orgName,
        companyId: unit.companyId,
      });

      res.json(unit);
    } catch (error: any) {
      console.error('Create business unit error:', error);
      res.status(500).json({ error: error.message || 'Failed to create business unit' });
    }
  });

  app.patch('/api/master-data/business-units/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      if (!isSystemAdmin(user) && !['admin', 'owner', 'editor'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }
      
      // Verify company ownership for non-system admins
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        const existingUnit = await storage.getBusinessUnit(req.params.id);
        if (!existingUnit || existingUnit.companyId !== userCompanyId) {
          return res.status(403).json({ error: 'You can only modify business units in your company' });
        }
      }

      const unit = await storage.updateBusinessUnit(req.params.id, req.body, req.user.id);
      
      await createAuditLog(req, 'update_business_unit', 'business_unit', unit.id, {
        changes: req.body,
      });

      res.json(unit);
    } catch (error: any) {
      console.error('Update business unit error:', error);
      res.status(500).json({ error: error.message || 'Failed to update business unit' });
    }
  });

  app.delete('/api/master-data/business-units/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      if (!isSystemAdmin(user) && !['admin', 'owner'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Only admins and owners can delete business units' });
      }
      
      // Verify company ownership for non-system admins
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        const existingUnit = await storage.getBusinessUnit(req.params.id);
        if (!existingUnit || existingUnit.companyId !== userCompanyId) {
          return res.status(403).json({ error: 'You can only delete business units in your company' });
        }
      }

      await storage.deleteBusinessUnit(req.params.id);
      
      await createAuditLog(req, 'delete_business_unit', 'business_unit', req.params.id, {});

      res.json({ success: true });
    } catch (error: any) {
      console.error('Delete business unit error:', error);
      res.status(500).json({ error: error.message || 'Failed to delete business unit' });
    }
  });

  // Locations
  // Company admins can only access locations in their company
  app.get('/api/master-data/locations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { companyId, orgId, status } = req.query;
      const user = await storage.getUser(req.user.id);
      
      // For non-system admins, enforce company filtering server-side
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        if (!userCompanyId) {
          return res.json([]); // No company access = no data
        }
        // Ignore client-provided companyId/orgId, use server-verified company
        const locations = await storage.getLocationsByCompany(userCompanyId, status as string);
        return res.json(locations);
      }
      
      // System admins can query any company
      let locations = [];
      if (orgId) {
        locations = await storage.getLocationsByBusinessUnit(orgId as string, status as string);
      } else if (companyId) {
        locations = await storage.getLocationsByCompany(companyId as string, status as string);
      }
      
      res.json(locations);
    } catch (error: any) {
      console.error('Get locations error:', error);
      res.status(500).json({ error: error.message || 'Failed to get locations' });
    }
  });

  // Company admins can create locations only in their company
  app.post('/api/master-data/locations', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      // Check permission level
      if (!isSystemAdmin(user) && !['admin', 'owner', 'editor'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }
      
      // For non-system admins, override companyId with server-verified value
      // and validate that orgId belongs to user's company
      let companyIdToUse = req.body.companyId;
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        if (!userCompanyId) {
          return res.status(403).json({ error: 'No company context available' });
        }
        // Always use server-verified companyId for non-system admins
        companyIdToUse = userCompanyId;
        
        // Validate that the orgId (business unit) belongs to user's company
        if (req.body.orgId) {
          const businessUnit = await storage.getBusinessUnit(req.body.orgId);
          if (!businessUnit || businessUnit.companyId !== userCompanyId) {
            return res.status(403).json({ error: 'Invalid business unit for your company' });
          }
        }
      }

      const { insertLocationSchema } = await import("@shared/schema");
      const locationData = insertLocationSchema.parse({
        ...req.body,
        companyId: companyIdToUse, // Override with server-verified value
        createdBy: req.user.id,
        lastUpdatedBy: req.user.id,
      });

      const location = await storage.createLocation(locationData);
      
      await createAuditLog(req, 'create_location', 'location', location.id, {
        locName: location.locName,
        companyId: location.companyId,
        orgId: location.orgId,
      });

      res.json(location);
    } catch (error: any) {
      console.error('Create location error:', error);
      res.status(500).json({ error: error.message || 'Failed to create location' });
    }
  });

  app.patch('/api/master-data/locations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      if (!isSystemAdmin(user) && !['admin', 'owner', 'editor'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Insufficient permissions' });
      }
      
      // Verify company ownership for non-system admins
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        const existingLocation = await storage.getLocation(req.params.id);
        if (!existingLocation || existingLocation.companyId !== userCompanyId) {
          return res.status(403).json({ error: 'You can only modify locations in your company' });
        }
      }

      const location = await storage.updateLocation(req.params.id, req.body, req.user.id);
      
      await createAuditLog(req, 'update_location', 'location', location.id, {
        changes: req.body,
      });

      res.json(location);
    } catch (error: any) {
      console.error('Update location error:', error);
      res.status(500).json({ error: error.message || 'Failed to update location' });
    }
  });

  app.delete('/api/master-data/locations/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      const user = await storage.getUser(req.user.id);
      const contextRole = req.user?.activeContext?.role;
      
      if (!isSystemAdmin(user) && !['admin', 'owner'].includes(contextRole || '')) {
        return res.status(403).json({ error: 'Only admins and owners can delete locations' });
      }
      
      // Verify company ownership for non-system admins
      if (!isSystemAdmin(user)) {
        const userCompanyId = getUserCompanyId(req.user);
        const existingLocation = await storage.getLocation(req.params.id);
        if (!existingLocation || existingLocation.companyId !== userCompanyId) {
          return res.status(403).json({ error: 'You can only delete locations in your company' });
        }
      }

      await storage.deleteLocation(req.params.id);
      
      await createAuditLog(req, 'delete_location', 'location', req.params.id, {});

      res.json({ success: true });
    } catch (error: any) {
      console.error('Delete location error:', error);
      res.status(500).json({ error: error.message || 'Failed to delete location' });
    }
  });

  // ==========================================
  // ROLE MANAGEMENT ROUTES
  // ==========================================

  // Get all roles
  app.get('/api/roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      const allRoles = await db.select().from(roles).orderBy(roles.displayName);
      res.json({ roles: allRoles });
    } catch (error) {
      console.error('❌ [ROLES] Get error:', error);
      res.status(500).json({ error: 'Failed to get roles' });
    }
  });

  // Create a new role
  app.post('/api/roles', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const validatedData = insertRoleSchema.parse(req.body);
      const created = await db.insert(roles).values(validatedData).returning();
      res.json(created[0]);
    } catch (error) {
      console.error('❌ [ROLES] Create error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to create role' });
    }
  });

  // Update a role
  app.put('/api/roles/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const { id } = req.params;
      const validatedData = insertRoleSchema.partial().parse(req.body);
      
      const updated = await db.update(roles)
        .set({ ...validatedData, updatedAt: new Date() })
        .where(eq(roles.id, id))
        .returning();

      if (updated.length === 0) {
        return res.status(404).json({ error: 'Role not found' });
      }

      res.json(updated[0]);
    } catch (error) {
      console.error('❌ [ROLES] Update error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to update role' });
    }
  });

  // Delete a role
  app.delete('/api/roles/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const { id } = req.params;
      
      // Check if it's a system role
      const roleToDelete = await db.select().from(roles).where(eq(roles.id, id));
      if (roleToDelete.length > 0 && roleToDelete[0].isSystemRole) {
        return res.status(400).json({ error: 'Cannot delete system roles' });
      }

      const deleted = await db.delete(roles).where(eq(roles.id, id)).returning();
      
      if (deleted.length === 0) {
        return res.status(404).json({ error: 'Role not found' });
      }

      res.json({ success: true, deleted: deleted[0] });
    } catch (error) {
      console.error('❌ [ROLES] Delete error:', error);
      res.status(400).json({ error: error instanceof Error ? error.message : 'Failed to delete role' });
    }
  });

  // Seed default system roles
  app.post('/api/roles/seed', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (req.user.role !== 'admin' && req.user.role !== 'owner') {
        return res.status(403).json({ error: 'Admin access required' });
      }

      const defaultRoles = [
        { roleName: 'admin', displayName: 'Administrator', description: 'Full system access', isSystemRole: true },
        { roleName: 'owner', displayName: 'Owner', description: 'Business owner with full access', isSystemRole: true },
        { roleName: 'editor', displayName: 'Editor', description: 'Can edit contracts and data', isSystemRole: true },
        { roleName: 'viewer', displayName: 'Viewer', description: 'Read-only access', isSystemRole: true },
        { roleName: 'auditor', displayName: 'Auditor', description: 'Access to audit trail and reports', isSystemRole: true },
      ];

      const created = [];
      for (const role of defaultRoles) {
        try {
          const result = await db.insert(roles).values(role).onConflictDoUpdate({
            target: roles.roleName,
            set: { displayName: role.displayName, description: role.description, updatedAt: new Date() }
          }).returning();
          created.push(result[0]);
        } catch (err) {
          console.error(`Error seeding role ${role.roleName}:`, err);
        }
      }

      console.log(`🌱 [ROLES] Seeded ${created.length} system roles`);
      res.json({ created: created.length, roles: created });
    } catch (error) {
      console.error('❌ [ROLES] Seed error:', error);
      res.status(500).json({ error: 'Failed to seed roles' });
    }
  });

  // ==========================================
  // ERP DATA IMPORT ROUTES
  // ==========================================

  // Upload and import ERP master data with vector embeddings
  app.post('/api/erp-import', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      const { contractId, mappingId } = req.body;

      if (!contractId || !mappingId) {
        return res.status(400).json({ error: 'Contract ID and Mapping ID are required' });
      }

      // Get the mapping details
      const mapping = await storage.getMasterDataMapping(mappingId);
      if (!mapping) {
        return res.status(404).json({ error: 'Mapping not found' });
      }

      // Create import job
      const jobName = `${mapping.erpSystem} - ${mapping.entityType} Import - ${new Date().toISOString().split('T')[0]}`;
      const importJob = await storage.createDataImportJob({
        mappingId,
        customerId: contractId,
        jobName,
        uploadMeta: {
          fileName: req.file.originalname,
          fileSize: req.file.size,
          uploadedAt: new Date().toISOString()
        },
        status: 'processing',
        recordsTotal: 0,
        recordsProcessed: 0,
        recordsFailed: 0,
        createdBy: req.user.id,
        startedAt: new Date()
      });

      console.log(`🚀 [ERP IMPORT] Job created: ${importJob.id}`);

      // Parse file asynchronously
      (async () => {
        try {
          const fs = await import('fs');
          const fileBuffer = fs.readFileSync(req.file.path);
          
          // Parse CSV or Excel
          let parsedData: any[] = [];
          if (req.file.originalname.endsWith('.csv')) {
            const Papa = (await import('papaparse')).default;
            const fileContent = fileBuffer.toString('utf-8');
            const result = await new Promise<any>((resolve) => {
              Papa.parse(fileContent, {
                header: true,
                skipEmptyLines: true,
                dynamicTyping: true,
                complete: (results) => resolve(results)
              });
            });
            parsedData = result.data;
          } else {
            const XLSX = await import('xlsx');
            const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
            const sheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[sheetName];
            parsedData = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
          }

          console.log(`📊 [ERP IMPORT] Parsed ${parsedData.length} records`);

          // Update total count
          await storage.updateDataImportJob(importJob.id, {
            recordsTotal: parsedData.length
          });

          // Import HuggingFace embedding service
          const { HuggingFaceEmbeddingService } = await import('./services/huggingFaceEmbedding');

          // Process records with embeddings
          const records = [];
          let processed = 0;
          let failed = 0;

          for (const row of parsedData) {
            try {
              // Create searchable text from all fields
              const searchText = Object.entries(row)
                .filter(([key, value]) => value)
                .map(([key, value]) => `${key}: ${value}`)
                .join('. ');

              // Generate embedding
              const { embedding } = await HuggingFaceEmbeddingService.generateEmbedding(searchText);

              records.push({
                jobId: importJob.id,
                mappingId,
                customerId: contractId,
                sourceRecord: row,
                targetRecord: row,
                embedding,
                metadata: {
                  erpSystem: mapping.erpSystem,
                  entityType: mapping.entityType,
                  importedAt: new Date().toISOString()
                }
              });

              processed++;

              // Update progress every 10 records
              if (processed % 10 === 0) {
                await storage.updateDataImportJob(importJob.id, {
                  recordsProcessed: processed
                });
                console.log(`📈 [ERP IMPORT] Progress: ${processed}/${parsedData.length}`);
              }

            } catch (error) {
              console.error(`❌ [ERP IMPORT] Failed to process record:`, error);
              failed++;
            }
          }

          // Save all records in batch
          if (records.length > 0) {
            await storage.createImportedErpRecords(records);
          }

          // Mark job as completed
          await storage.updateDataImportJob(importJob.id, {
            status: 'completed',
            recordsProcessed: processed,
            recordsFailed: failed,
            completedAt: new Date()
          });

          console.log(`✅ [ERP IMPORT] Job completed: ${processed} processed, ${failed} failed`);

          // Log the import
          await createAuditLog(req, 'import_erp_data', 'data_import_job', importJob.id, {
            fileName: req.file.originalname,
            totalRows: parsedData.length,
            processedRows: processed,
            failedRows: failed,
            erpSystem: mapping.erpSystem,
            entityType: mapping.entityType
          });

        } catch (error) {
          console.error('❌ [ERP IMPORT] Processing error:', error);
          await storage.updateDataImportJob(importJob.id, {
            status: 'failed',
            errorLog: { message: (error as Error).message }
          });
        }
      })();

      // Return immediately with job info
      res.json({
        success: true,
        job: importJob,
        message: 'Import job started. Processing in background.'
      });

    } catch (error) {
      console.error('❌ [ERP IMPORT] Error:', error);
      res.status(500).json({ error: 'Failed to start import job' });
    }
  });

  // Get all ERP import jobs
  app.get('/api/erp-import-jobs', isAuthenticated, async (req: any, res: Response) => {
    try {
      const { contractId, status } = req.query;
      const jobs = await storage.getDataImportJobs(
        contractId as string | undefined,
        status as string | undefined
      );
      res.json(jobs);
    } catch (error) {
      console.error('❌ [ERP IMPORT JOBS] Error:', error);
      res.status(500).json({ error: 'Failed to fetch import jobs' });
    }
  });

  // ==============================================
  // ENHANCED ERP INTEGRATION APIs
  // With org-context filtering and role-based access control
  // ==============================================

  // Helper: Check if user has admin access for ERP operations
  const hasErpAdminAccess = (user: any): boolean => {
    if (isSystemAdmin(user)) return true;
    const contextRole = user.activeContext?.role;
    return ['admin', 'owner'].includes(contextRole || '');
  };

  // Helper: Get org-context filters for ERP operations
  const getErpOrgContextFilters = (user: any): { companyId?: string; businessUnitId?: string; locationId?: string } => {
    if (isSystemAdmin(user)) return {}; // System admins see all
    const context = user.activeContext;
    if (!context) return { companyId: 'none' }; // No context = no access
    return {
      companyId: context.companyId,
      businessUnitId: context.businessUnitId || undefined,
      locationId: context.locationId || undefined,
    };
  };

  // Helper: Validate mapping access (enforces full 3-level hierarchy)
  const canAccessMapping = async (mappingId: string, user: any): Promise<boolean> => {
    if (isSystemAdmin(user)) return true;
    const mapping = await storage.getMasterDataMapping(mappingId);
    if (!mapping) return false;
    const context = user.activeContext;
    if (!context) return false;
    
    // Enforce full hierarchy matching:
    // 1. Company must match
    if (mapping.companyId && mapping.companyId !== context.companyId) return false;
    
    // 2. If user has location-level context, mapping must match that location
    // (location-scoped users can only access mappings for their location)
    if (context.locationId && mapping.locationId && mapping.locationId !== context.locationId) return false;
    
    // 3. If user has BU-level context (no location), mapping must match that BU
    // (BU-scoped users can access mappings for their BU or any location within it)
    if (context.businessUnitId && !context.locationId && mapping.businessUnitId && mapping.businessUnitId !== context.businessUnitId) return false;
    
    return true;
  };

  // Helper: Validate job access (enforces full 3-level hierarchy)
  const canAccessJob = async (jobId: string, user: any): Promise<boolean> => {
    if (isSystemAdmin(user)) return true;
    const job = await storage.getDataImportJob(jobId);
    if (!job) return false;
    const context = user.activeContext;
    if (!context) return false;
    
    // Enforce full hierarchy matching:
    // 1. Company must match
    if (job.companyId && job.companyId !== context.companyId) return false;
    
    // 2. If user has location-level context, job must match that location
    if (context.locationId && job.locationId && job.locationId !== context.locationId) return false;
    
    // 3. If user has BU-level context (no location), job must match that BU
    if (context.businessUnitId && !context.locationId && job.businessUnitId && job.businessUnitId !== context.businessUnitId) return false;
    
    return true;
  };

  // Helper: Apply mapping transformation to source data
  const applyMappingTransformation = (sourceRow: any, mapping: any): any => {
    if (!mapping?.mappingResults) return sourceRow;
    
    const targetRow: any = {};
    const mappingResults = Array.isArray(mapping.mappingResults) ? mapping.mappingResults : [];
    
    for (const fieldMapping of mappingResults) {
      const sourceField = fieldMapping.source_field || fieldMapping.sourceField;
      const targetField = fieldMapping.target_field || fieldMapping.targetField;
      const transformation = fieldMapping.transformation_rule || fieldMapping.transformationRule;
      
      if (sourceField && targetField && sourceRow.hasOwnProperty(sourceField)) {
        let value = sourceRow[sourceField];
        
        // Apply simple transformations
        if (transformation) {
          switch (transformation) {
            case 'uppercase':
              value = String(value).toUpperCase();
              break;
            case 'lowercase':
              value = String(value).toLowerCase();
              break;
            case 'trim':
              value = String(value).trim();
              break;
            case 'number':
              value = parseFloat(value) || 0;
              break;
            case 'date':
              value = new Date(value).toISOString();
              break;
            // Add more transformations as needed
          }
        }
        
        targetRow[targetField] = value;
      }
    }
    
    return targetRow;
  };

  // Get mappings with company hierarchy filtering and version support
  app.get('/api/erp/mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Check admin access
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required for ERP operations' });
      }

      const { companyId, businessUnitId, locationId, erpSystemId, entityType, status, latestVersionOnly } = req.query;
      
      // Get org context filters (system admins bypass)
      const orgFilters = getErpOrgContextFilters(req.user);
      
      // Only system admins can see global mappings (prevents cross-tenant leakage)
      const includeGlobal = isSystemAdmin(req.user);
      
      const mappings = await storage.getMappingsWithFilters({
        // Apply org-context filter first, then user query filters
        companyId: orgFilters.companyId || companyId as string,
        businessUnitId: orgFilters.businessUnitId || businessUnitId as string,
        locationId: orgFilters.locationId || locationId as string,
        erpSystemId: erpSystemId as string,
        entityType: entityType as string,
        status: status as string,
        latestVersionOnly: latestVersionOnly === 'true',
        includeGlobal, // Only system admins see global mappings
      });
      
      res.json({ mappings, total: mappings.length });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error fetching mappings:', error);
      res.status(500).json({ error: 'Failed to fetch mappings' });
    }
  });

  // Alias for /api/erp/mappings - used by ERP Hub page
  app.get('/api/master-data-mappings', isAuthenticated, async (req: any, res: Response) => {
    try {
      // Get org context filters (system admins bypass)
      const orgFilters = getErpOrgContextFilters(req.user);
      const includeGlobal = isSystemAdmin(req.user);
      
      const mappings = await storage.getMappingsWithFilters({
        companyId: orgFilters.companyId,
        businessUnitId: orgFilters.businessUnitId,
        locationId: orgFilters.locationId,
        latestVersionOnly: true,
        includeGlobal,
      });
      
      res.json({ mappings, total: mappings.length });
    } catch (error) {
      console.error('❌ [MASTER DATA MAPPINGS] Error fetching mappings:', error);
      res.status(500).json({ error: 'Failed to fetch mappings' });
    }
  });

  // Get mapping version history
  app.get('/api/erp/mappings/:id/versions', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate mapping access
      if (!await canAccessMapping(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }

      const versions = await storage.getMappingVersionHistory(id);
      res.json({ versions, total: versions.length });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error fetching version history:', error);
      res.status(500).json({ error: 'Failed to fetch version history' });
    }
  });

  // Create a new mapping version (propose changes)
  app.post('/api/erp/mappings/:id/propose', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate mapping access
      if (!await canAccessMapping(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }

      const updates = req.body;
      
      const newVersion = await storage.createMappingVersion(id, updates, req.user.id);
      
      console.log(`📝 [ERP MAPPINGS] New version ${newVersion.version} created for mapping ${id}`);
      
      res.json({
        success: true,
        mapping: newVersion,
        message: `Version ${newVersion.version} created successfully`,
      });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error creating version:', error);
      res.status(500).json({ error: 'Failed to create mapping version' });
    }
  });

  // Approve a mapping version
  app.put('/api/erp/mappings/:id/approve', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate mapping access
      if (!await canAccessMapping(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }

      // Check current mapping status
      const mapping = await storage.getMasterDataMapping(id);
      if (!mapping) {
        return res.status(404).json({ error: 'Mapping not found' });
      }
      if (mapping.status === 'approved') {
        return res.status(400).json({ error: 'Mapping is already approved' });
      }
      if (mapping.status === 'deprecated') {
        return res.status(400).json({ error: 'Cannot approve a deprecated mapping' });
      }
      
      const approved = await storage.approveMappingVersion(id, req.user.id);
      
      console.log(`✅ [ERP MAPPINGS] Mapping ${id} approved by ${req.user.username}`);
      
      res.json({
        success: true,
        mapping: approved,
        message: 'Mapping approved successfully',
      });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error approving mapping:', error);
      res.status(500).json({ error: 'Failed to approve mapping' });
    }
  });

  // Revert to a specific mapping version
  app.post('/api/erp/mappings/:id/revert', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate mapping access
      if (!await canAccessMapping(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }

      const { targetVersion } = req.body;
      
      if (!targetVersion) {
        return res.status(400).json({ error: 'targetVersion is required' });
      }
      
      const reverted = await storage.revertToMappingVersion(id, targetVersion, req.user.id);
      
      console.log(`🔄 [ERP MAPPINGS] Mapping ${id} reverted to version ${targetVersion}`);
      
      res.json({
        success: true,
        mapping: reverted,
        message: `Reverted to version ${targetVersion}`,
      });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error reverting mapping:', error);
      res.status(500).json({ error: 'Failed to revert mapping' });
    }
  });

  // Deprecate a mapping
  app.put('/api/erp/mappings/:id/deprecate', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate mapping access
      if (!await canAccessMapping(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }
      
      // Check current status
      const mapping = await storage.getMasterDataMapping(id);
      if (!mapping) {
        return res.status(404).json({ error: 'Mapping not found' });
      }
      if (mapping.status === 'deprecated') {
        return res.status(400).json({ error: 'Mapping is already deprecated' });
      }
      
      const deprecated = await storage.deprecateMapping(id);
      
      console.log(`📦 [ERP MAPPINGS] Mapping ${id} deprecated`);
      
      res.json({
        success: true,
        mapping: deprecated,
        message: 'Mapping deprecated successfully',
      });
    } catch (error) {
      console.error('❌ [ERP MAPPINGS] Error deprecating mapping:', error);
      res.status(500).json({ error: 'Failed to deprecate mapping' });
    }
  });

  // ======================
  // ENDPOINT TEMPLATES (API configurations per ERP entity)
  // ======================

  // Get all endpoint templates
  app.get('/api/erp/endpoint-templates', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { erpSystemId, entityId, operationType } = req.query;
      
      const templates = await storage.getEndpointTemplates({
        erpSystemId: erpSystemId as string,
        entityId: entityId as string,
        operationType: operationType as string,
      });
      
      res.json({ templates, total: templates.length });
    } catch (error) {
      console.error('❌ [ENDPOINT TEMPLATES] Error fetching templates:', error);
      res.status(500).json({ error: 'Failed to fetch endpoint templates' });
    }
  });

  // Get single endpoint template
  app.get('/api/erp/endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Endpoint template not found' });
      }
      
      res.json(template);
    } catch (error) {
      console.error('❌ [ENDPOINT TEMPLATES] Error fetching template:', error);
      res.status(500).json({ error: 'Failed to fetch endpoint template' });
    }
  });

  // Create endpoint template
  app.post('/api/erp/endpoint-templates', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { erpSystemId, erpEntityId, operationType, name, httpMethod, pathTemplate, 
              queryDefaults, paginationType, paginationConfig, requestHeaders, 
              requestBodyTemplate, responseDataPath, responseTotalPath, description } = req.body;
      
      if (!erpSystemId || !operationType || !name || !pathTemplate) {
        return res.status(400).json({ error: 'erpSystemId, operationType, name, and pathTemplate are required' });
      }
      
      const template = await storage.createEndpointTemplate({
        erpSystemId,
        erpEntityId,
        operationType,
        name,
        httpMethod: httpMethod || 'GET',
        pathTemplate,
        queryDefaults,
        paginationType,
        paginationConfig,
        requestHeaders,
        requestBodyTemplate,
        responseDataPath,
        responseTotalPath,
        description,
      });
      
      console.log(`✅ [ENDPOINT TEMPLATES] Created: ${template.name} (${template.operationType})`);
      res.json(template);
    } catch (error) {
      console.error('❌ [ENDPOINT TEMPLATES] Error creating template:', error);
      res.status(500).json({ error: 'Failed to create endpoint template' });
    }
  });

  // Update endpoint template
  app.patch('/api/erp/endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Endpoint template not found' });
      }
      
      const updated = await storage.updateEndpointTemplate(req.params.id, req.body);
      
      console.log(`📝 [ENDPOINT TEMPLATES] Updated: ${updated.name}`);
      res.json(updated);
    } catch (error) {
      console.error('❌ [ENDPOINT TEMPLATES] Error updating template:', error);
      res.status(500).json({ error: 'Failed to update endpoint template' });
    }
  });

  // Delete endpoint template
  app.delete('/api/erp/endpoint-templates/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const template = await storage.getEndpointTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: 'Endpoint template not found' });
      }
      
      await storage.deleteEndpointTemplate(req.params.id);
      
      console.log(`🗑️ [ENDPOINT TEMPLATES] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [ENDPOINT TEMPLATES] Error deleting template:', error);
      res.status(500).json({ error: 'Failed to delete endpoint template' });
    }
  });

  // ======================
  // DATA IMPORT SOURCES (iPaaS-style configurable sources)
  // ======================

  // Get all import sources with filtering
  app.get('/api/erp/import/sources', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { companyId, businessUnitId, locationId, sourceType, status, erpSystemId } = req.query;
      
      // Get org context filters (non-system admins are scoped)
      const orgFilters = getErpOrgContextFilters(req.user);
      
      const sources = await storage.getDataImportSources({
        companyId: orgFilters.companyId || companyId as string,
        businessUnitId: orgFilters.businessUnitId || businessUnitId as string,
        locationId: orgFilters.locationId || locationId as string,
        sourceType: sourceType as string,
        status: status as string,
        erpSystemId: erpSystemId as string,
      });
      
      res.json({ sources, total: sources.length });
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error fetching sources:', error);
      res.status(500).json({ error: 'Failed to fetch import sources' });
    }
  });

  // Get single import source
  app.get('/api/erp/import/sources/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const source = await storage.getDataImportSource(req.params.id);
      if (!source) {
        return res.status(404).json({ error: 'Import source not found' });
      }
      
      // Validate access based on org context
      const orgFilters = getErpOrgContextFilters(req.user);
      if (orgFilters.companyId && source.companyId !== orgFilters.companyId) {
        return res.status(403).json({ error: 'Access denied to this import source' });
      }
      
      res.json(source);
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error fetching source:', error);
      res.status(500).json({ error: 'Failed to fetch import source' });
    }
  });

  // Create import source
  app.post('/api/erp/import/sources', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { name, description, sourceType, connectionId, endpointTemplateId, mappingId, 
              erpSystemId, entityType, licenseiqEntityId, companyId, businessUnitId, 
              locationId, filters, scheduleEnabled, scheduleType, scheduleCron, importOptions } = req.body;
      
      if (!name) {
        return res.status(400).json({ error: 'Source name is required' });
      }
      
      // Validate org context - users can only create sources in their scope
      const orgFilters = getErpOrgContextFilters(req.user);
      if (orgFilters.companyId && companyId && companyId !== orgFilters.companyId) {
        return res.status(403).json({ error: 'Cannot create source for another company' });
      }

      const source = await storage.createDataImportSource({
        name,
        description,
        sourceType: sourceType || 'file',
        connectionId,
        endpointTemplateId,
        mappingId,
        erpSystemId,
        entityType,
        licenseiqEntityId,
        companyId: companyId || orgFilters.companyId,
        businessUnitId: businessUnitId || orgFilters.businessUnitId,
        locationId: locationId || orgFilters.locationId,
        filters,
        scheduleEnabled: scheduleEnabled || false,
        scheduleType,
        scheduleCron,
        importOptions,
        status: 'active',
        createdBy: req.user.id,
      });
      
      console.log(`✅ [IMPORT SOURCES] Created: ${source.name} (${source.sourceType})`);
      res.json(source);
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error creating source:', error);
      res.status(500).json({ error: 'Failed to create import source' });
    }
  });

  // Update import source
  app.patch('/api/erp/import/sources/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const source = await storage.getDataImportSource(req.params.id);
      if (!source) {
        return res.status(404).json({ error: 'Import source not found' });
      }
      
      // Validate access
      const orgFilters = getErpOrgContextFilters(req.user);
      if (orgFilters.companyId && source.companyId !== orgFilters.companyId) {
        return res.status(403).json({ error: 'Access denied to this import source' });
      }
      
      const updated = await storage.updateDataImportSource(req.params.id, req.body);
      console.log(`✏️ [IMPORT SOURCES] Updated: ${updated.name}`);
      res.json(updated);
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error updating source:', error);
      res.status(500).json({ error: 'Failed to update import source' });
    }
  });

  // Delete import source
  app.delete('/api/erp/import/sources/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const source = await storage.getDataImportSource(req.params.id);
      if (!source) {
        return res.status(404).json({ error: 'Import source not found' });
      }
      
      // Validate access
      const orgFilters = getErpOrgContextFilters(req.user);
      if (orgFilters.companyId && source.companyId !== orgFilters.companyId) {
        return res.status(403).json({ error: 'Access denied to this import source' });
      }
      
      await storage.deleteDataImportSource(req.params.id);
      console.log(`🗑️ [IMPORT SOURCES] Deleted: ${req.params.id}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error deleting source:', error);
      res.status(500).json({ error: 'Failed to delete import source' });
    }
  });

  // Trigger import from a source (file upload or API pull)
  app.post('/api/erp/import/sources/:id/run', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const source = await storage.getDataImportSource(req.params.id);
      if (!source) {
        return res.status(404).json({ error: 'Import source not found' });
      }
      
      // Validate access
      const orgFilters = getErpOrgContextFilters(req.user);
      if (orgFilters.companyId && source.companyId !== orgFilters.companyId) {
        return res.status(403).json({ error: 'Access denied to this import source' });
      }
      
      const { dryRun = true, filterOverrides } = req.body;
      
      // Merge source filters with any overrides
      const effectiveFilters = {
        ...(source.filters as object || {}),
        ...(filterOverrides || {}),
      };
      
      if (source.sourceType === 'file') {
        // File-based import - require file upload
        if (!req.file) {
          return res.status(400).json({ error: 'File upload required for file-type sources' });
        }
        
        // Use existing dry-run logic but with source configuration
        const fs = await import('fs');
        const path = await import('path');
        const xlsx = await import('xlsx');
        const Papa = await import('papaparse');
        
        const fileBuffer = fs.default.readFileSync(req.file.path);
        const ext = path.default.extname(req.file.originalname).toLowerCase();
        
        let parsedData: any[] = [];
        if (ext === '.xlsx' || ext === '.xls') {
          const workbook = xlsx.default.read(fileBuffer, { type: 'buffer' });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          parsedData = xlsx.default.utils.sheet_to_json(firstSheet);
        } else if (ext === '.csv') {
          const csvContent = fileBuffer.toString('utf-8');
          const result = Papa.default.parse(csvContent, { header: true, skipEmptyLines: true });
          parsedData = result.data as any[];
        } else {
          return res.status(400).json({ error: 'Unsupported file format. Use CSV or Excel.' });
        }
        
        // Apply filters if configured
        let filteredData = parsedData;
        let filterStats = { totalRecords: parsedData.length, matchedRecords: parsedData.length, filteredOutRecords: 0, conditionsApplied: 0 };
        
        if (effectiveFilters && effectiveFilters.conditions && effectiveFilters.conditions.length > 0) {
          const filterResult = applyFilters(parsedData, effectiveFilters as FilterConfig);
          filteredData = filterResult.filtered;
          filterStats = filterResult.stats;
          console.log(`🔍 [IMPORT FILTER] Applied ${filterStats.conditionsApplied} filter(s): ${filterStats.matchedRecords}/${filterStats.totalRecords} records matched`);
        }
        
        // Get mapping for transformation
        const mapping = source.mappingId ? await storage.getMasterDataMapping(source.mappingId) : null;
        
        // Create import job linked to source
        const job = await storage.createDataImportJob({
          mappingId: source.mappingId || 'manual',
          mappingVersion: mapping?.version || 1,
          sourceId: source.id,
          connectionId: source.connectionId,
          endpointTemplateId: source.endpointTemplateId,
          companyId: source.companyId,
          businessUnitId: source.businessUnitId,
          locationId: source.locationId,
          erpSystemId: source.erpSystemId,
          entityType: source.entityType,
          jobName: `${source.name} - ${new Date().toISOString().split('T')[0]}`,
          jobType: dryRun ? 'dry_run' : 'import',
          uploadMeta: {
            fileName: req.file.originalname,
            fileSize: req.file.size,
            recordCount: filteredData.length,
            totalRecordsInFile: parsedData.length,
            filteredOutRecords: filterStats.filteredOutRecords,
            sourceType: 'file',
            sourceId: source.id,
            filters: effectiveFilters,
            filterStats,
          },
          status: 'processing',
          recordsTotal: filteredData.length,
          createdBy: req.user.id,
          startedAt: new Date(),
        });
        
        // Process records (using filtered data)
        const processedRecords = filteredData.map((row, idx) => ({
          jobId: job.id,
          mappingId: source.mappingId || 'manual',
          mappingVersion: mapping?.version || 1,
          companyId: source.companyId,
          businessUnitId: source.businessUnitId,
          locationId: source.locationId,
          licenseiqEntityId: source.licenseiqEntityId,
          sourceRecord: row,
          targetRecord: mapping ? applyMappingTransformation(row, mapping) : row,
          recordStatus: 'staged',
          metadata: { sourceRowNumber: idx + 1, sourceName: source.name },
        }));
        
        // Store staged records
        if (processedRecords.length > 0) {
          await storage.createImportedErpRecords(processedRecords);
        }
        
        // Update job status
        await storage.updateDataImportJob(job.id, {
          status: dryRun ? 'pending_commit' : 'completed',
          recordsProcessed: processedRecords.length,
          completedAt: new Date(),
        });
        
        // Update source stats
        await storage.updateSourceRunStats(source.id, true);
        
        console.log(`✅ [IMPORT SOURCES] Run completed: ${source.name}, ${processedRecords.length} records`);
        
        res.json({
          success: true,
          job,
          recordsProcessed: processedRecords.length,
          dryRun,
        });
        
      } else if (source.sourceType === 'api') {
        // API-based import - use connection and endpoint template
        if (!source.connectionId) {
          return res.status(400).json({ error: 'API source requires a connection to be configured' });
        }
        
        // Get connection and endpoint template details
        const connection = await storage.getIntegrationConnection(source.connectionId);
        if (!connection) {
          return res.status(400).json({ error: 'Connection not found' });
        }
        
        let endpointTemplate = null;
        if (source.endpointTemplateId && source.endpointTemplateId !== 'custom') {
          endpointTemplate = await storage.getEndpointTemplate(source.endpointTemplateId);
        }
        
        // Build the API request URL
        let apiUrl = connection.baseUrl;
        if (endpointTemplate) {
          apiUrl = connection.baseUrl.replace(/\/$/, '') + endpointTemplate.pathTemplate;
        } else if (req.body.customEndpoint) {
          // Custom endpoint URL provided in request
          const customPath = req.body.customEndpoint;
          if (customPath.startsWith('http')) {
            apiUrl = customPath;
          } else {
            apiUrl = connection.baseUrl.replace(/\/$/, '') + (customPath.startsWith('/') ? '' : '/') + customPath;
          }
        }
        
        // Build headers based on auth type
        const headers: Record<string, string> = {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        };
        
        // Add authentication headers based on connection config
        if (connection.authType === 'api_key') {
          // Note: We'd need to decrypt the API key from secrets in production
          // For now, indicate that API key auth is configured
          const apiKeyHeader = connection.apiKeyHeader || 'X-API-Key';
          headers[apiKeyHeader] = process.env[`ERP_API_KEY_${connection.id}`] || 'API_KEY_REQUIRED';
        } else if (connection.authType === 'basic') {
          const username = connection.basicUsername || '';
          const password = process.env[`ERP_BASIC_PASSWORD_${connection.id}`] || '';
          headers['Authorization'] = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
        }
        // OAuth would require token refresh logic - simplified for now
        
        // Add any custom headers from endpoint template
        if (endpointTemplate?.requestHeaders) {
          Object.assign(headers, endpointTemplate.requestHeaders);
        }
        
        console.log(`🔌 [API IMPORT] Calling: ${apiUrl}`);
        
        try {
          // Make the API request
          const apiResponse = await fetch(apiUrl, {
            method: endpointTemplate?.httpMethod || 'GET',
            headers,
            ...(endpointTemplate?.requestBodyTemplate ? { body: JSON.stringify(endpointTemplate.requestBodyTemplate) } : {}),
          });
          
          if (!apiResponse.ok) {
            const errorText = await apiResponse.text();
            console.error(`❌ [API IMPORT] API returned error: ${apiResponse.status}`, errorText);
            await storage.updateSourceRunStats(source.id, false);
            return res.status(apiResponse.status).json({ 
              error: `API returned error: ${apiResponse.status}`, 
              details: errorText.substring(0, 500) 
            });
          }
          
          const responseData = await apiResponse.json();
          
          // Extract data array from response using responseDataPath
          let records: any[] = [];
          if (endpointTemplate?.responseDataPath) {
            // Navigate to the data path (e.g., "data.items" or "results")
            const pathParts = endpointTemplate.responseDataPath.split('.');
            let current = responseData;
            for (const part of pathParts) {
              current = current?.[part];
            }
            records = Array.isArray(current) ? current : [current].filter(Boolean);
          } else if (Array.isArray(responseData)) {
            records = responseData;
          } else if (responseData.data && Array.isArray(responseData.data)) {
            records = responseData.data;
          } else if (responseData.results && Array.isArray(responseData.results)) {
            records = responseData.results;
          } else {
            records = [responseData];
          }
          
          console.log(`📥 [API IMPORT] Retrieved ${records.length} records from API`);
          
          // Apply post-fetch filters if configured
          let filteredApiRecords = records;
          let apiFilterStats = { totalRecords: records.length, matchedRecords: records.length, filteredOutRecords: 0, conditionsApplied: 0 };
          
          if (effectiveFilters && effectiveFilters.conditions && effectiveFilters.conditions.length > 0) {
            const filterResult = applyFilters(records, effectiveFilters as FilterConfig);
            filteredApiRecords = filterResult.filtered;
            apiFilterStats = filterResult.stats;
            console.log(`🔍 [API IMPORT FILTER] Applied ${apiFilterStats.conditionsApplied} filter(s): ${apiFilterStats.matchedRecords}/${apiFilterStats.totalRecords} records matched`);
          }
          
          // Get mapping for transformation
          const mapping = source.mappingId ? await storage.getMasterDataMapping(source.mappingId) : null;
          
          // Create import job
          const job = await storage.createDataImportJob({
            mappingId: source.mappingId || 'api-import',
            mappingVersion: mapping?.version || 1,
            sourceId: source.id,
            connectionId: source.connectionId,
            endpointTemplateId: source.endpointTemplateId,
            companyId: source.companyId,
            businessUnitId: source.businessUnitId,
            locationId: source.locationId,
            erpSystemId: source.erpSystemId,
            entityType: source.entityType,
            jobName: `${source.name} - API Pull - ${new Date().toISOString().split('T')[0]}`,
            jobType: dryRun ? 'dry_run' : 'import',
            uploadMeta: {
              sourceType: 'api',
              sourceId: source.id,
              apiUrl,
              recordCount: filteredApiRecords.length,
              totalRecordsFromApi: records.length,
              filteredOutRecords: apiFilterStats.filteredOutRecords,
              filters: effectiveFilters,
              filterStats: apiFilterStats,
              pulledAt: new Date().toISOString(),
            },
            status: 'processing',
            recordsTotal: filteredApiRecords.length,
            createdBy: req.user.id,
            startedAt: new Date(),
          });
          
          // Process records (using filtered data)
          const processedRecords = filteredApiRecords.map((row, idx) => ({
            jobId: job.id,
            mappingId: source.mappingId || 'api-import',
            mappingVersion: mapping?.version || 1,
            companyId: source.companyId,
            businessUnitId: source.businessUnitId,
            locationId: source.locationId,
            licenseiqEntityId: source.licenseiqEntityId,
            sourceRecord: row,
            targetRecord: mapping ? applyMappingTransformation(row, mapping) : row,
            recordStatus: 'staged',
            metadata: { sourceRowNumber: idx + 1, sourceName: source.name, sourceType: 'api' },
          }));
          
          // Store staged records
          if (processedRecords.length > 0) {
            await storage.createImportedErpRecords(processedRecords);
          }
          
          // Update job status
          await storage.updateDataImportJob(job.id, {
            status: dryRun ? 'pending_commit' : 'completed',
            recordsProcessed: processedRecords.length,
            completedAt: new Date(),
          });
          
          // Update source stats
          await storage.updateSourceRunStats(source.id, true);
          
          console.log(`✅ [API IMPORT] Completed: ${source.name}, ${processedRecords.length} records`);
          
          res.json({
            success: true,
            job,
            recordsProcessed: processedRecords.length,
            dryRun,
            sourceType: 'api',
          });
          
        } catch (apiError: any) {
          console.error(`❌ [API IMPORT] Failed to call API:`, apiError);
          await storage.updateSourceRunStats(source.id, false);
          return res.status(500).json({ error: `Failed to call API: ${apiError.message}` });
        }
        
      } else {
        return res.status(400).json({ error: 'Unknown source type' });
      }
      
    } catch (error) {
      console.error('❌ [IMPORT SOURCES] Error running import:', error);
      res.status(500).json({ error: 'Failed to run import from source' });
    }
  });

  // Filter Preview - Preview how filters will affect data before import
  app.post('/api/erp/import/filter-preview', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { filters } = req.body;
      
      // Validate filter configuration
      if (filters) {
        const parsedFilters = typeof filters === 'string' ? JSON.parse(filters) : filters;
        const validation = validateFilterConfig(parsedFilters);
        if (!validation.valid) {
          return res.status(400).json({ 
            error: 'Invalid filter configuration', 
            details: validation.errors 
          });
        }
      }

      if (!req.file) {
        return res.status(400).json({ error: 'File upload required for filter preview' });
      }

      // Parse the uploaded file
      const fsModule = await import('fs');
      const pathModule = await import('path');
      const xlsx = await import('xlsx');
      const Papa = await import('papaparse');
      
      const fileBuffer = fsModule.default.readFileSync(req.file.path);
      const ext = pathModule.default.extname(req.file.originalname).toLowerCase();
      
      let parsedData: any[] = [];
      if (ext === '.xlsx' || ext === '.xls') {
        const workbook = xlsx.default.read(fileBuffer, { type: 'buffer' });
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
        parsedData = xlsx.default.utils.sheet_to_json(firstSheet);
      } else if (ext === '.csv') {
        const csvContent = fileBuffer.toString('utf-8');
        const result = Papa.default.parse(csvContent, { header: true, skipEmptyLines: true });
        parsedData = result.data as any[];
      } else {
        return res.status(400).json({ error: 'Unsupported file format. Use CSV or Excel.' });
      }

      // Extract available fields from the data
      const availableFields = extractFieldsFromData(parsedData);

      // Apply filters if provided
      const parsedFilters = filters ? (typeof filters === 'string' ? JSON.parse(filters) : filters) : null;
      const { filtered, stats } = applyFilters(parsedData, parsedFilters);

      // Get sample of matched and filtered-out records
      const sampleSize = 5;
      const matchedSample = filtered.slice(0, sampleSize);
      const filteredOutSample = parsedData
        .filter(record => !filtered.includes(record))
        .slice(0, sampleSize);

      // Clean up temp file
      try {
        fsModule.default.unlinkSync(req.file.path);
      } catch (e) {
        // Ignore cleanup errors
      }

      console.log(`🔍 [FILTER PREVIEW] ${stats.matchedRecords}/${stats.totalRecords} records match filters`);

      res.json({
        success: true,
        stats,
        availableFields,
        sampleMatched: matchedSample,
        sampleFilteredOut: filteredOutSample,
        totalRecords: stats.totalRecords,
        matchedRecords: stats.matchedRecords,
        filteredOutRecords: stats.filteredOutRecords,
      });
    } catch (error: any) {
      console.error('❌ [FILTER PREVIEW] Error:', error);
      res.status(500).json({ error: error.message || 'Failed to preview filter results' });
    }
  });

  // Get available fields from uploaded file (for filter builder)
  app.post('/api/erp/import/detect-fields', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'File upload required' });
      }

      const fsModule = await import('fs');
      const pathModule = await import('path');
      const xlsx = await import('xlsx');
      const Papa = await import('papaparse');
      
      const fileBuffer = fsModule.default.readFileSync(req.file.path);
      const ext = pathModule.default.extname(req.file.originalname).toLowerCase();
      
      let parsedData: any[] = [];
      if (ext === '.xlsx' || ext === '.xls') {
        const workbook = xlsx.default.read(fileBuffer, { type: 'buffer' });
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
        parsedData = xlsx.default.utils.sheet_to_json(firstSheet);
      } else if (ext === '.csv') {
        const csvContent = fileBuffer.toString('utf-8');
        const result = Papa.default.parse(csvContent, { header: true, skipEmptyLines: true });
        parsedData = result.data as any[];
      } else {
        return res.status(400).json({ error: 'Unsupported file format' });
      }

      // Extract fields with detected types
      const fields = extractFieldsFromData(parsedData);

      // Clean up temp file
      try {
        fsModule.default.unlinkSync(req.file.path);
      } catch (e) {
        // Ignore cleanup errors
      }

      res.json({
        success: true,
        fields: fields.map(f => ({
          name: f.name,
          label: f.name.replace(/_/g, ' ').replace(/([A-Z])/g, ' $1').trim(),
          dataType: f.detectedType,
        })),
        totalRecords: parsedData.length,
        sampleData: parsedData.slice(0, 3),
      });
    } catch (error: any) {
      console.error('❌ [DETECT FIELDS] Error:', error);
      res.status(500).json({ error: error.message || 'Failed to detect fields' });
    }
  });

  // Get import jobs with company hierarchy filtering
  app.get('/api/erp/import/jobs', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required for ERP operations' });
      }

      const { companyId, businessUnitId, locationId, erpSystemId, entityType, status, jobType } = req.query;
      
      // Get org context filters (system admins bypass)
      const orgFilters = getErpOrgContextFilters(req.user);
      
      const jobs = await storage.getImportJobsWithFilters({
        companyId: orgFilters.companyId || companyId as string,
        businessUnitId: orgFilters.businessUnitId || businessUnitId as string,
        locationId: orgFilters.locationId || locationId as string,
        erpSystemId: erpSystemId as string,
        entityType: entityType as string,
        status: status as string,
        jobType: jobType as string,
      });
      
      res.json({ jobs, total: jobs.length });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Error fetching jobs:', error);
      res.status(500).json({ error: 'Failed to fetch import jobs' });
    }
  });

  // Get job details with records
  app.get('/api/erp/import/jobs/:id', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate job access
      if (!await canAccessJob(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this job' });
      }

      const { recordStatus } = req.query;
      
      const job = await storage.getDataImportJob(id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      
      const records = await storage.getImportedRecordsByJob(id, recordStatus as string);
      
      res.json({
        job,
        records,
        summary: {
          total: records.length,
          staged: records.filter(r => r.recordStatus === 'staged').length,
          committed: records.filter(r => r.recordStatus === 'committed').length,
          failed: records.filter(r => r.recordStatus === 'failed').length,
        },
      });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Error fetching job details:', error);
      res.status(500).json({ error: 'Failed to fetch job details' });
    }
  });

  // Dry-run import (creates staged records without committing)
  app.post('/api/erp/import/dry-run', isAuthenticated, dataUpload.single('file'), async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      const { mappingId, companyId, businessUnitId, locationId } = req.body;

      if (!mappingId) {
        return res.status(400).json({ error: 'mappingId is required' });
      }

      // Validate mapping access
      if (!await canAccessMapping(mappingId, req.user)) {
        return res.status(403).json({ error: 'Access denied to this mapping' });
      }

      const mapping = await storage.getMasterDataMapping(mappingId);
      if (!mapping) {
        return res.status(404).json({ error: 'Mapping not found' });
      }
      
      // Verify mapping is approved for use
      if (mapping.status !== 'approved') {
        return res.status(400).json({ error: 'Only approved mappings can be used for imports' });
      }

      // Create dry-run job
      const jobName = `[DRY-RUN] ${mapping.erpSystem} - ${mapping.entityType} - ${new Date().toISOString().split('T')[0]}`;
      const importJob = await storage.createDataImportJob({
        mappingId,
        mappingVersion: mapping.version,
        companyId: companyId || mapping.companyId,
        businessUnitId: businessUnitId || mapping.businessUnitId,
        locationId: locationId || mapping.locationId,
        erpSystemId: mapping.erpSystemId,
        entityType: mapping.entityType,
        jobName,
        jobType: 'dry_run',
        uploadMeta: {
          fileName: req.file.originalname,
          fileSize: req.file.size,
          uploadedAt: new Date().toISOString(),
        },
        status: 'processing',
        recordsTotal: 0,
        recordsProcessed: 0,
        recordsFailed: 0,
        recordsSkipped: 0,
        createdBy: req.user.id,
        startedAt: new Date(),
      });

      console.log(`🔍 [ERP IMPORT] Dry-run job created: ${importJob.id}`);

      // Parse file synchronously for dry-run preview
      const fs = await import('fs');
      const fileBuffer = fs.readFileSync(req.file.path);
      
      let parsedData: any[] = [];
      if (req.file.originalname.endsWith('.csv')) {
        const Papa = (await import('papaparse')).default;
        const fileContent = fileBuffer.toString('utf-8');
        const result = await new Promise<any>((resolve) => {
          Papa.parse(fileContent, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: true,
            complete: (results) => resolve(results),
          });
        });
        parsedData = result.data;
      } else {
        const XLSX = await import('xlsx');
        const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        parsedData = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
      }

      // Apply mapping and create staged records
      const mappingResults = mapping.mappingResults as any[];
      const stagedRecords: any[] = [];
      const errors: any[] = [];

      for (let i = 0; i < parsedData.length; i++) {
        const sourceRecord = parsedData[i];
        const targetRecord: any = {};
        const recordErrors: string[] = [];

        for (const rule of mappingResults) {
          if (rule.source_field && sourceRecord[rule.source_field] !== undefined) {
            let value = sourceRecord[rule.source_field];
            
            // Apply simple transformations
            if (rule.transformation_rule && rule.transformation_rule !== 'direct') {
              if (rule.transformation_rule.includes('lowercase')) {
                value = String(value).toLowerCase();
              } else if (rule.transformation_rule.includes('uppercase')) {
                value = String(value).toUpperCase();
              }
            }
            
            targetRecord[rule.target_field] = value;
          }
        }

        stagedRecords.push({
          jobId: importJob.id,
          mappingId: mapping.id,
          mappingVersion: mapping.version,
          companyId: companyId || mapping.companyId,
          businessUnitId: businessUnitId || mapping.businessUnitId,
          locationId: locationId || mapping.locationId,
          licenseiqEntityId: mapping.licenseiqEntityId,
          sourceRecord,
          targetRecord,
          recordStatus: 'staged',
          validationErrors: recordErrors.length > 0 ? recordErrors : null,
          metadata: { sourceRowNumber: i + 1 },
        });
      }

      // Save staged records
      if (stagedRecords.length > 0) {
        await storage.createImportedErpRecords(stagedRecords);
      }

      // Update job with totals
      await storage.updateDataImportJob(importJob.id, {
        status: 'pending_commit',
        recordsTotal: parsedData.length,
        recordsProcessed: stagedRecords.length,
        recordsFailed: errors.length,
      });

      // Clean up temp file
      fs.unlinkSync(req.file.path);

      res.json({
        success: true,
        job: {
          id: importJob.id,
          jobName: importJob.jobName,
          status: 'pending_commit',
        },
        preview: {
          total: parsedData.length,
          staged: stagedRecords.length,
          sampleRecords: stagedRecords.slice(0, 5).map(r => ({
            source: r.sourceRecord,
            target: r.targetRecord,
          })),
        },
        message: `Dry-run complete. ${stagedRecords.length} records staged for review.`,
      });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Dry-run error:', error);
      res.status(500).json({ error: 'Failed to process dry-run' });
    }
  });

  // Commit staged records from a dry-run job
  app.post('/api/erp/import/jobs/:id/commit', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate job access
      if (!await canAccessJob(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this job' });
      }
      
      const job = await storage.getDataImportJob(id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      
      // Validate job state for commit
      if (job.status !== 'pending_commit') {
        return res.status(400).json({ error: `Cannot commit job in '${job.status}' status. Job must be in 'pending_commit' status.` });
      }
      
      if (job.jobType !== 'dry_run') {
        return res.status(400).json({ error: 'Only dry-run jobs can be committed' });
      }
      
      const result = await storage.commitStagedRecords(id);
      
      console.log(`✅ [ERP IMPORT] Job ${id} committed: ${result.committed} records`);
      
      res.json({
        success: true,
        committed: result.committed,
        failed: result.failed,
        message: `Successfully committed ${result.committed} records`,
      });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Commit error:', error);
      res.status(500).json({ error: 'Failed to commit records' });
    }
  });

  // Discard staged records from a dry-run job
  app.post('/api/erp/import/jobs/:id/discard', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate job access
      if (!await canAccessJob(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this job' });
      }
      
      const job = await storage.getDataImportJob(id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      
      // Validate job state for discard
      if (!['pending_commit', 'processing', 'failed'].includes(job.status)) {
        return res.status(400).json({ error: `Cannot discard job in '${job.status}' status` });
      }
      
      const discarded = await storage.discardStagedRecords(id);
      
      console.log(`🗑️ [ERP IMPORT] Job ${id} discarded: ${discarded} records`);
      
      res.json({
        success: true,
        discarded,
        message: `Discarded ${discarded} staged records`,
      });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Discard error:', error);
      res.status(500).json({ error: 'Failed to discard records' });
    }
  });

  // Retry failed records in a job
  app.post('/api/erp/import/jobs/:id/retry-failed', isAuthenticated, async (req: any, res: Response) => {
    try {
      if (!hasErpAdminAccess(req.user)) {
        return res.status(403).json({ error: 'Admin or Owner access required' });
      }

      const { id } = req.params;
      
      // Validate job access
      if (!await canAccessJob(id, req.user)) {
        return res.status(403).json({ error: 'Access denied to this job' });
      }
      
      const job = await storage.getDataImportJob(id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      
      // Get failed records
      const failedRecords = await storage.getImportedRecordsByJob(id, 'failed');
      
      if (failedRecords.length === 0) {
        return res.json({
          success: true,
          retriedCount: 0,
          message: 'No failed records to retry',
        });
      }
      
      let successCount = 0;
      let stillFailedCount = 0;
      
      // Re-process each failed record by resetting to staged status
      for (const record of failedRecords) {
        try {
          // Reset the record to staged status for re-commit
          await storage.updateImportedRecordStatus(record.id, 'staged', null);
          successCount++;
        } catch (err) {
          stillFailedCount++;
          console.error(`Failed to retry record ${record.id}:`, err);
        }
      }
      
      // Update job status if all records are now staged
      if (successCount > 0) {
        await storage.updateDataImportJob(id, { status: 'pending_commit' });
      }
      
      console.log(`🔄 [ERP IMPORT] Job ${id} retry: ${successCount} records reset to staged, ${stillFailedCount} still failed`);
      
      res.json({
        success: true,
        retriedCount: successCount,
        stillFailedCount,
        message: `Reset ${successCount} failed records to staged status. You can now commit them.`,
      });
    } catch (error) {
      console.error('❌ [ERP IMPORT] Retry failed error:', error);
      res.status(500).json({ error: 'Failed to retry records' });
    }
  });

  // Return the configured app - server will be started in index.ts
  return createServer(app);
}

// ⚡ OPTION B: Minimal numeric validator - prevents database crashes
// Core goal: Extract numbers when possible, safely reject text
// Philosophy: Simple and defensive, let AI improve extraction quality
function parseNumericValue(value: any): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'number') {
    return (!isNaN(value) && isFinite(value)) ? value : null;
  }
  
  // HANDLE OBJECTS: AI sometimes returns {"amount": 5, "currency": "USD"} instead of just 5
  if (typeof value === 'object' && value !== null) {
    // Try to extract numeric value from common object keys
    const extracted = value.amount ?? value.value ?? value.rate ?? value.baseAmount ?? value.number;
    if (extracted !== undefined && extracted !== null) {
      // Recursively parse the extracted value
      return parseNumericValue(extracted);
    }
    // Object has no recognizable numeric field
    return null;
  }
  
  const str = String(value).trim();
  
  // Try direct parse first (handles: "123", "12.34", "-5.6")
  let num = parseFloat(str);
  if (!isNaN(num) && isFinite(num)) return num;
  
  // Remove common separators and try again (handles: "$1,234.56", "€1.000", "R$500")
  const cleaned = str.replace(/[$€£¥₹₽¢R$S$C$]/g, '').replace(/[,\s]/g, '');
  num = parseFloat(cleaned);
  
  // Return the number if valid, otherwise null (rejects "standard rate", "Tier 1", etc.)
  return (!isNaN(num) && isFinite(num)) ? num : null;
}

// Background contract processing
async function processContractAnalysis(contractId: string, filePath: string) {
  try {
    console.log(`🔄 Starting analysis for contract ${contractId}`);
    
    // Get contract details
    const contract = await storage.getContract(contractId);
    if (!contract) {
      console.error(`Contract ${contractId} not found`);
      return;
    }

    // Extract text from file
    const mimeType = contract.fileType || 'application/pdf';
    const extractedText = await fileService.extractTextFromFile(filePath, mimeType);
    
    console.log(`🤖 Sending contract to AI for analysis...`);
    
    // Analyze with Groq AI - ONE CONSOLIDATED CALL
    const aiAnalysis = await groqService.analyzeContract(extractedText);
    
    console.log(`✅ AI analysis complete. Validating results...`);
    
    // 🔒 TRANSACTIONAL PIPELINE: Validate ALL data before saving ANYTHING
    // This prevents partial-save race conditions
    
    // Step 1: Validate that we have summary and keyTerms
    if (!aiAnalysis.summary || !aiAnalysis.keyTerms) {
      throw new Error('AI extraction failed: Missing required summary or key terms');
    }
    
    // Step 2: Get detailed extraction with parties info and royalty rules
    console.log(`🔍 Extracting royalty rules...`);
    const detailedExtraction = await groqService.extractDetailedRoyaltyRules(extractedText);
    
    // 🔧 FIX: Map Groq RoyaltyRule[] to InsertRoyaltyRule[] for database persistence
    const royaltyRulesData: InsertRoyaltyRule[] = (detailedExtraction.rules || []).map(rule => {
      return {
        contractId, // Will be added by the loop below, but TypeScript needs it
        ruleType: rule.ruleType,
        ruleName: rule.ruleName,
        description: rule.description || '',
        
        // NEW: JSON-based formula definition - Set to null for now (use legacy calculation)
        // TODO: Build proper FormulaNode expression tree from AI data
        formulaDefinition: null,
        formulaVersion: null,
        
        // LEGACY: Map to legacy fields for backwards compatibility
        productCategories: rule.conditions?.productCategories || [],
        territories: rule.conditions?.territories || [],
        baseRate: rule.calculation?.rate?.toString() || null,
        volumeTiers: rule.calculation?.tiers || null,
        minimumGuarantee: (rule.ruleType === 'minimum_guarantee' || rule.ruleType === 'fixed_fee') && rule.calculation?.amount 
          ? rule.calculation.amount.toString() 
          : null,
        
        // Provenance & confidence
        confidence: rule.confidence?.toString() || '0.8',
        sourceSection: rule.sourceSpan?.section || (rule.sourceSpan?.page ? `Page ${rule.sourceSpan.page}` : null),
        sourceText: rule.sourceSpan?.text || null,
        priority: rule.priority || 10,
        isActive: true
      };
    }).filter(rule => rule.sourceText && rule.sourceText.trim().length > 0); // Skip rules without source text
    
    console.log(`📋 Found ${royaltyRulesData.length} royalty rules to save`);
    
    // 🔧 FIX: Add parties data to keyTerms so frontend can access it
    // keyTerms is an array, but we'll convert it to an object with licensor/licensee
    const enhancedKeyTerms = {
      terms: Array.isArray(aiAnalysis.keyTerms) ? aiAnalysis.keyTerms : [],
      licensor: detailedExtraction.parties?.licensor || 'Not specified',
      licensee: detailedExtraction.parties?.licensee || 'Not specified',
      paymentTerms: detailedExtraction.paymentTerms || null,
      effectiveDate: detailedExtraction.effectiveDate || null,
      expirationDate: detailedExtraction.expirationDate || null,
      currency: detailedExtraction.currency || 'USD'
    };
    
    console.log(`👥 Extracted parties: ${enhancedKeyTerms.licensor} → ${enhancedKeyTerms.licensee}`);
    
    // Step 3: ALL DATA VALIDATED - Now save everything in transaction-like sequence
    console.log(`💾 Saving all data in transactional sequence...`);
    
    try {
      // Save contract analysis
      await storage.createContractAnalysis({
        contractId,
        summary: aiAnalysis.summary,
        keyTerms: enhancedKeyTerms, // Use enhanced keyTerms with parties data
        riskAnalysis: aiAnalysis.riskAnalysis,
        insights: aiAnalysis.insights,
        confidence: aiAnalysis.confidence?.toString() || '0',
        processingTime: 0,
      });
      console.log(`✅ Contract analysis saved`);

      // Save all royalty rules (add contractId to each)
      for (const ruleData of royaltyRulesData) {
        await storage.createRoyaltyRule({
          ...ruleData,
          contractId, // Add contractId to each rule
        });
      }
      console.log(`✅ ${royaltyRulesData.length} royalty rules saved`);

      // ERP Field Mapping: Extract terms and map to ERP fields
      // This creates pending_term_mappings for user confirmation
      try {
        console.log(`🔗 Starting ERP field mapping extraction...`);
        
        // Get the default ERP system (Oracle Fusion Cloud) for mapping
        const defaultErpSystemId = '03c4a661-b078-4e6c-bde2-3d32dda784c2'; // Oracle Fusion Cloud ERP
        
        const erpMappingResult = await extractWithErpMapping(
          extractedText,
          contractId,
          defaultErpSystemId,  // ERP system ID
          undefined,           // extraction run ID (optional)
          {                    // options
            autoConfirmHighConfidence: true,
            confidenceThreshold: 0.85,
            requireConfirmation: true
          }
        );
        console.log(`✅ ERP mapping complete: ${erpMappingResult.erpMappings.pendingCount} pending, ${erpMappingResult.erpMappings.autoConfirmedCount} auto-confirmed`);
      } catch (erpError) {
        // Don't fail the entire analysis if ERP mapping fails
        console.warn(`⚠️ ERP field mapping failed (non-fatal):`, erpError);
      }

      // Generate embeddings for RAG/semantic search
      await generateContractEmbeddings(contractId, aiAnalysis);
      console.log(`✅ Embeddings generated`);

      // Update status to analyzed (frontend expects this)
      await storage.updateContractStatus(contractId, 'analyzed');
      console.log(`✅ Status updated to 'analyzed'`);
      
      // Auto-populate contract metadata from AI analysis (only if fields are currently empty)
      // This preserves any existing user-edited metadata
      const currentContract = await storage.getContract(contractId);
      const metadataUpdate: any = {};
      
      // Helper function to parse date strings to Date objects
      const parseDate = (dateStr: string | null | undefined): Date | null => {
        if (!dateStr) return null;
        try {
          const parsed = new Date(dateStr);
          return isNaN(parsed.getTime()) ? null : parsed;
        } catch {
          return null;
        }
      };
      
      // Only update fields that are currently null/empty AND AI extracted a value
      if (!currentContract.displayName && detailedExtraction.parties?.licensor) {
        const contractNum = currentContract.contractNumber || '';
        const originalName = currentContract.originalName || '';
        metadataUpdate.displayName = contractNum ? `${contractNum} ${originalName}` : `${detailedExtraction.parties.licensor} - ${detailedExtraction.licenseType || 'Agreement'}`;
      }
      if (!currentContract.counterpartyName && detailedExtraction.parties?.licensor) {
        metadataUpdate.counterpartyName = detailedExtraction.parties.licensor;
      }
      if (!currentContract.effectiveStart && detailedExtraction.effectiveDate) {
        const parsedDate = parseDate(detailedExtraction.effectiveDate);
        if (parsedDate) {
          metadataUpdate.effectiveStart = parsedDate;
        }
      }
      if (!currentContract.effectiveEnd && detailedExtraction.expirationDate) {
        const parsedDate = parseDate(detailedExtraction.expirationDate);
        if (parsedDate) {
          metadataUpdate.effectiveEnd = parsedDate;
        }
      }
      if (!currentContract.contractType && detailedExtraction.documentType) {
        metadataUpdate.contractType = detailedExtraction.documentType;
      }
      // Initialize version only if null (don't override existing versions)
      if (currentContract.currentVersion === null) {
        metadataUpdate.currentVersion = 0;
      }
      
      // Only update if we have fields to populate
      if (Object.keys(metadataUpdate).length > 0) {
        await db.update(contracts)
          .set(metadataUpdate)
          .where(eq(contracts.id, contractId));
        
        console.log(`✅ Auto-populated metadata fields:`, Object.keys(metadataUpdate).join(', '));
        if (metadataUpdate.displayName) console.log(`   Display Name: ${metadataUpdate.displayName}`);
        if (metadataUpdate.counterpartyName) console.log(`   Counterparty: ${metadataUpdate.counterpartyName}`);
        if (metadataUpdate.effectiveStart) console.log(`   Effective: ${metadataUpdate.effectiveStart} to ${metadataUpdate.effectiveEnd || 'N/A'}`);
      } else {
        console.log(`ℹ️ No metadata auto-population needed (fields already populated or no AI data)`);
      }
      
      console.log(`🎉 Analysis completed successfully for contract ${contractId}`);
    } catch (saveError) {
      console.error(`❌ Failed to save data:`, saveError);
      // Rollback: Delete any partial data
      try {
        await storage.deleteContractAnalysis(contractId);
        await storage.deleteRoyaltyRulesByContract(contractId);
      } catch (rollbackError) {
        console.error(`Failed to rollback:`, rollbackError);
      }
      throw new Error(`Failed to save extraction data: ${saveError instanceof Error ? saveError.message : 'Unknown error'}`);
    }
    
  } catch (error) {
    console.error(`❌ Analysis failed for contract ${contractId}:`, error);
    await storage.updateContractStatus(contractId, 'failed');
  }
}

// 🆕 NEW: Extract royalty rules data WITHOUT saving (for transactional pipeline)
async function extractRoyaltyRulesData(contractText: string, aiAnalysis: any): Promise<any[]> {
  const contractId = ''; // Will be filled during save
  const rulesData: any[] = [];
  
  try {
    // Get detailed rules from consolidated extraction
    const detailedRules = await groqService.extractDetailedRoyaltyRules(contractText);
    const hasRoyaltyTerms = detailedRules.rules && detailedRules.rules.length > 0;
    
    console.log(`📋 Found ${detailedRules.rules.length} royalty rules from AI`);
    
    // 🐛 DEBUG: Log rule types to diagnose misclassification (AI returns 'ruleType', not 'type'!)
    console.log(`🐛 [DEBUG] Rule types extracted:`, detailedRules.rules.map((r: any) => r.ruleType));
    
    // 🚫 ONLY extract product formulas for licensing/royalty contracts (NOT service/consulting contracts)
    // Requires LICENSING-SPECIFIC evidence (container sizes, volume tiers, etc.)
    const paymentOnlyRuleTypes = ['payment_schedule', 'payment_method', 'rate_structure', 
                                   'invoice_requirements', 'late_payment_penalty', 'advance_payment', 'milestone_payment'];
    const licensingRuleTypes = ['percentage', 'tiered_pricing', 'formula_based', 'minimum_guarantee', 'cap'];
    
    const hasLicensingSpecificFeatures = detailedRules.rules.some((r: any) =>
      // Must have licensing-specific features (not just product categories)
      (r.conditions?.containerSizes?.length > 0) ||
      (r.calculation?.tiers?.length > 0) ||
      (r.calculation?.seasonalAdjustments && Object.keys(r.calculation.seasonalAdjustments).length > 0) ||
      (r.calculation?.territoryPremiums && Object.keys(r.calculation.territoryPremiums).length > 0) ||
      (licensingRuleTypes.includes(r.ruleType))
    );
    
    const hasOnlyPaymentRules = detailedRules.rules.every((r: any) => 
      paymentOnlyRuleTypes.includes(r.ruleType)
    );
    
    console.log(`🐛 [DEBUG] hasLicensingSpecificFeatures: ${hasLicensingSpecificFeatures}`);
    console.log(`🐛 [DEBUG] hasOnlyPaymentRules: ${hasOnlyPaymentRules}`);
    
    let productsWithFormulas: any[] = [];
    if (hasRoyaltyTerms && hasLicensingSpecificFeatures && !hasOnlyPaymentRules) {
      console.log(`🌱 Product licensing contract detected - extracting product formulas...`);
      productsWithFormulas = await groqService.extractProductsWithFormulas(contractText);
      console.log(`📋 Found ${productsWithFormulas.length} product formulas`);
    } else if (hasRoyaltyTerms && (hasOnlyPaymentRules || !hasLicensingSpecificFeatures)) {
      console.log(`ℹ️ Service/consulting contract detected - skipping product formula extraction`);
    } else {
      console.log(`ℹ️ No royalty/payment terms detected - skipping product formula extraction`);
      
      // Prepare product formula rules for saving
      for (const product of productsWithFormulas) {
        rulesData.push({
          ruleType: 'formula_based',
          ruleName: product.ruleName,
          description: product.description,
          productCategories: product.conditions?.productCategories || [product.productName],
          territories: product.conditions?.territories || [],
          containerSizes: product.conditions?.containerSize ? [product.conditions.containerSize] : [],
          formulaDefinition: product.formulaDefinition,
          formulaVersion: '1.0',
          isActive: true,
          priority: 1,
          confidence: product.confidence ?? 0.9,
          sourceSection: product.sourceSection || null,
          sourceText: product.description,
        });
      }
    }
    
    // Convert AI-extracted rules to database format
    // Keep ALL valid rules (don't filter out payment rules!)
    const validLegacyRules = detailedRules.rules.filter((rule: any) => {
      // Keep rules that have:
      // 1. A valid name and description
      // 2. A valid rule type (AI returns 'ruleType', not 'type'!)
      return rule.ruleName && rule.description && rule.ruleType;
    });

    for (const rule of validLegacyRules) {
      const r = rule as any;
      
      // AI returns structured data in 'conditions' and 'calculation' objects
      const conditions = r.conditions || {};
      const calculation = r.calculation || {};
      
      // Validate numeric values with defensive parsing
      const baseRate = parseNumericValue(calculation.rate || calculation.baseRate);
      const volumeTiers = calculation.tiers || [];
      
      rulesData.push({
        ruleType: r.ruleType || 'tiered_pricing',
        ruleName: r.ruleName,
        description: r.description,
        baseRoyaltyRate: baseRate,
        productCategories: conditions.productCategories || [],
        containerSizes: conditions.containerSizes || [],
        territories: conditions.territories || [],
        volumeTiers: volumeTiers,
        seasonalAdjustments: calculation.seasonalAdjustments || {},
        territoryPremiums: calculation.territoryPremiums || {},
        isActive: true,
        priority: r.priority || 1,
        confidence: r.confidence ?? 0.85,
        sourceSection: r.sourceSpan?.section || null,
        sourceText: r.sourceSpan?.text || r.description,
      });
    }
    
    return rulesData;
  } catch (error) {
    console.error(`Error extracting royalty rules data:`, error);
    return []; // Return empty array on error
  }
}

// Helper function to extract and save royalty rules from AI analysis
async function extractAndSaveRoyaltyRules(contractId: string, contractText: string, aiAnalysis: any) {
  try {
    console.log(`🔍 Extracting royalty rules for contract ${contractId}...`);
    
    // 📊 FIRST: Check if contract has royalty terms
    const detailedRules = await groqService.extractDetailedRoyaltyRules(contractText);
    const hasRoyaltyTerms = detailedRules.rules && detailedRules.rules.length > 0;
    
    console.log(`📋 Found ${detailedRules.rules.length} legacy royalty rules`);
    
    // 🌱 ONLY extract product formulas if the contract actually has royalty terms
    let productsWithFormulas: any[] = [];
    if (hasRoyaltyTerms) {
      console.log(`🌱 Extracting product varieties with formula definitions...`);
      productsWithFormulas = await groqService.extractProductsWithFormulas(contractText);
      console.log(`📋 Found ${productsWithFormulas.length} product formulas from AI analysis`);
      
      // Save product formulas with FormulaNode JSON
      for (const product of productsWithFormulas) {
        const ruleData: any = {
          contractId,
          ruleType: 'formula_based',
          ruleName: product.ruleName,
          description: product.description,
          productCategories: product.conditions?.productCategories || [product.productName],
          territories: product.conditions?.territories || [],
          containerSizes: product.conditions?.containerSize ? [product.conditions.containerSize] : [],
          
          // 🚀 NEW: Store the complete FormulaDefinition JSON
          formulaDefinition: product.formulaDefinition,
          formulaVersion: '1.0',
          
          isActive: true,
          priority: 1,
          confidence: product.confidence ?? 0.9, // Keep as number for schema validation
          sourceSection: product.sourceSection || null,
          sourceText: product.description,
        };

        await storage.createRoyaltyRule(ruleData);
        console.log(`✅ Saved formula rule: ${product.ruleName} (product: ${product.productName})`);
      }
    } else {
      console.log(`ℹ️ No royalty terms detected - skipping product formula extraction`);
    }
    
    // Convert AI-extracted rules to database format (legacy flat structure)
    // Save ALL rules - global adjustments (without categories) are valid and useful!
    const validLegacyRules = detailedRules.rules.filter(rule => {
      // Type assertion for AI rule as it comes from external API
      const r = rule as any;
      
      // Keep rules that have either:
      // 1. Product categories (product-specific rules)
      // 2. Seasonal adjustments or territory premiums (global multipliers)
      // 3. Volume tiers or base rates (calculation rules)
      const hasCategories = r.conditions?.productCategories && r.conditions.productCategories.length > 0;
      const hasSeasonalAdj = r.calculation?.seasonalAdjustments || r.seasonalAdjustments;
      const hasTerritoryPrem = r.calculation?.territoryPremiums || r.territoryPremiums;
      const hasTiers = r.calculation?.tiers && r.calculation.tiers.length > 0;
      const hasRate = r.calculation?.rate || r.calculation?.baseRate || r.baseRate;
      
      const isValid = hasCategories || hasSeasonalAdj || hasTerritoryPrem || hasTiers || hasRate;
      
      if (!isValid) {
        console.log(`   ⚠️ Skipping empty rule (no data): ${r.ruleName}`);
      }
      return isValid;
    });
    
    console.log(`📋 Keeping ${validLegacyRules.length} valid legacy rules (including global adjustments)`);
    
    for (const aiRule of validLegacyRules) {
      // Type assertion for AI rule as it comes from external API
      const rule = aiRule as any;
      
      // Extract seasonal adjustments from various possible locations
      const seasonalAdj = rule.calculation?.seasonalAdjustments || 
                         rule.calculation?.seasonalMultipliers || 
                         rule.seasonalAdjustments || 
                         {};
      
      // Extract territory premiums from various possible locations
      const territoryPrem = rule.calculation?.territoryPremiums || 
                           rule.calculation?.territoryMultipliers || 
                           rule.territoryPremiums || 
                           {};
      
      // Extract volume tiers - ensure they're in correct format
      let volumeTiers = rule.calculation?.tiers || [];
      
      // ⚡ OPTION B: Fix database type error - validate numeric fields
      // Extract base rate and ensure it's numeric (handles international formats)
      const extractedRate = rule.calculation?.rate || 
                           rule.calculation?.baseRate || 
                           rule.baseRate;
      
      const numericRate = parseNumericValue(extractedRate);
      const baseRate = numericRate !== null ? numericRate.toString() : null;
      if (extractedRate !== undefined && extractedRate !== null && baseRate === null) {
        console.warn(`⚠️ Skipping non-numeric baseRate: "${extractedRate}"`);
      }
      
      const ruleData: any = {
        contractId,
        ruleType: mapRuleType(rule.ruleType),
        ruleName: rule.ruleName || 'Extracted Rule',
        description: rule.description || '',
        productCategories: rule.conditions?.productCategories || [],
        territories: rule.conditions?.territories || [],
        containerSizes: rule.conditions?.containerSizes || [],
        seasonalAdjustments: seasonalAdj,
        territoryPremiums: territoryPrem,
        volumeTiers: volumeTiers,
        baseRate: baseRate,
        minimumGuarantee: (() => {
          if (rule.ruleType === 'minimum_guarantee') {
            const amount = rule.calculation?.amount || rule.calculation?.minimum;
            const numericAmount = parseNumericValue(amount);
            if (numericAmount !== null) {
              return numericAmount.toString();
            }
            console.warn(`⚠️ Skipping non-numeric minimumGuarantee: "${amount}"`);
          }
          return null;
        })(),
        isActive: true,
        priority: rule.priority || 10, // Lower priority than formula-based rules
        confidence: rule.confidence || null,
        sourceSection: rule.sourceSpan?.section || null,
        sourceText: rule.sourceSpan?.text || null,
      };

      await storage.createRoyaltyRule(ruleData);
      console.log(`✅ Saved legacy rule: ${rule.ruleName}`);
    }
    
    // 💰 NEW: Extract general payment terms (works for all contract types)
    console.log(`💰 Extracting general payment terms (payment schedules, methods, rates)...`);
    const generalPaymentTerms = await groqService.extractGeneralPaymentTerms(contractText);
    console.log(`📋 Found ${generalPaymentTerms.length} general payment terms`);
    
    for (const term of generalPaymentTerms) {
      // Extract base rate from payment terms if possible
      let extractedBaseRate = null;
      
      if (term.paymentTerms) {
        // Try to extract a simple numeric value from complex payment terms
        const numericRate = parseNumericValue(term.paymentTerms);
        if (numericRate !== null) {
          extractedBaseRate = numericRate.toString();
        } else {
          // If payment terms is complex (e.g., tiered pricing), skip this term
          // These should be handled by the tiered rules extraction instead
          console.log(`⚠️ Skipping complex payment term: ${term.ruleName} (contains non-numeric data)`);
          continue;
        }
      }
      
      const termData: any = {
        contractId,
        ruleType: term.ruleType, // payment_schedule, payment_method, rate_structure, etc.
        ruleName: term.ruleName,
        description: term.description,
        productCategories: [],
        territories: [],
        containerSizes: [],
        
        // Only store simple numeric rates
        baseRate: extractedBaseRate,
        
        isActive: true,
        priority: 5, // Medium priority
        confidence: term.confidence || null,
        sourceSection: null,
        sourceText: term.sourceText || null,
      };

      await storage.createRoyaltyRule(termData);
      console.log(`✅ Saved payment term: ${term.ruleName} (type: ${term.ruleType})`);
    }
    
    const totalRules = productsWithFormulas.length + validLegacyRules.length + generalPaymentTerms.length;
    console.log(`🎉 Successfully extracted and saved ${totalRules} total rules (${productsWithFormulas.length} formula-based, ${validLegacyRules.length} royalty, ${generalPaymentTerms.length} payment terms)`);
  } catch (error) {
    console.error(`⚠️ Failed to extract royalty rules:`, error);
    // Don't fail the entire analysis if rule extraction fails
  }
}

// Generate embeddings for contract (for RAG Q&A)
async function generateContractEmbeddings(contractId: string, aiAnalysis: any) {
  try {
    console.log(`🔍 Generating embeddings for contract ${contractId}...`);
    
    // Generate embedding for summary
    if (aiAnalysis.summary) {
      const { embedding } = await HuggingFaceEmbeddingService.generateEmbedding(aiAnalysis.summary);
      await storage.saveContractEmbedding({
        contractId,
        embeddingType: 'summary',
        embedding,
        sourceText: aiAnalysis.summary,
        metadata: { type: 'summary' }
      });
      console.log(`✅ Generated summary embedding (${embedding.length} dimensions)`);
    }
    
    // Generate embeddings for key terms
    if (aiAnalysis.keyTerms && Array.isArray(aiAnalysis.keyTerms)) {
      const keyTermsText = aiAnalysis.keyTerms.join(', ');
      const { embedding } = await HuggingFaceEmbeddingService.generateEmbedding(keyTermsText);
      await storage.saveContractEmbedding({
        contractId,
        embeddingType: 'key_terms',
        embedding,
        sourceText: keyTermsText,
        metadata: { terms: aiAnalysis.keyTerms }
      });
      console.log(`✅ Generated key terms embedding`);
    }
    
    // Generate embeddings for insights
    if (aiAnalysis.insights && Array.isArray(aiAnalysis.insights)) {
      const insightsText = aiAnalysis.insights.join(' ');
      const { embedding } = await HuggingFaceEmbeddingService.generateEmbedding(insightsText);
      await storage.saveContractEmbedding({
        contractId,
        embeddingType: 'insights',
        embedding,
        sourceText: insightsText,
        metadata: { insights: aiAnalysis.insights }
      });
      console.log(`✅ Generated insights embedding`);
    }
    
    console.log(`✅ All embeddings generated for contract ${contractId}`);
  } catch (error) {
    console.error(`⚠️ Failed to generate embeddings:`, error);
    // Don't fail the entire analysis if embedding generation fails
  }
}

// Map AI rule types to database enum values
function mapRuleType(aiRuleType: string): string {
  const typeMap: { [key: string]: string} = {
    'percentage': 'tiered_pricing',
    'tiered': 'tiered_pricing',
    'minimum_guarantee': 'minimum_guarantee',
    'cap': 'tiered_pricing',
    'deduction': 'tiered_pricing',
    'fixed_fee': 'tiered_pricing',
  };
  return typeMap[aiRuleType] || 'tiered_pricing';
}

export default registerRoutes;
