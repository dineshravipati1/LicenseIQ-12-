/**
 * System Knowledge Base for LicenseIQ Platform
 * This data is embedded and used by LIQ AI to answer questions about the platform itself
 */

export const systemKnowledgeBase = [
  {
    id: 'platform-overview',
    category: 'Platform Overview',
    title: 'What is LicenseIQ?',
    content: `LicenseIQ Research Platform is a comprehensive SaaS web application designed for intelligent contract management and analysis. 
    
The platform specializes in automating contract processing, risk assessment, and compliance checks using AI technology. It offers features like automated payment calculations, dynamic rule engines, and a RAG-powered Q&A system (LIQ AI).

Key benefits:
- Reduces manual effort in contract analysis
- Provides actionable insights from complex agreements
- Ensures revenue assurance for industries with complex licensing agreements
- Uses an "AI-Native" architecture with 100% FREE AI services (Groq API for LLaMA models, HuggingFace for embeddings)

LicenseIQ is particularly valuable for organizations managing multiple licensing agreements across different territories, products, and pricing structures.`,
  },
  {
    id: 'contract-types',
    category: 'Contract Types',
    title: 'What contract types does LicenseIQ handle?',
    content: `LicenseIQ supports a wide range of contract types through its universal contract processing system:

1. **Licensing Agreements**
   - Technology licenses
   - Patent licenses
   - Trademark licenses
   - Software licenses
   - Content licensing

2. **Manufacturing Contracts**
   - Production agreements
   - Supply agreements
   - Distribution agreements

3. **Electronics & Semiconductor**
   - Patent portfolio licenses
   - Component royalty agreements
   - IP licensing

4. **Plant Variety & Agriculture**
   - Seed licensing
   - Plant variety royalties
   - Agricultural technology licenses

5. **Service Contracts**
   - Consulting agreements
   - Professional services
   - Maintenance contracts

6. **Payment & Fee Structures**
   - Percentage-based royalties (license fees)
   - Fixed fees
   - Volume-tiered pricing
   - Minimum guarantees
   - Caps and thresholds
   - Seasonal adjustments
   - Territory-based premiums

The system uses AI-powered zero-shot extraction to automatically identify contract types and extract relevant terms, making it adaptable to any contract structure.`,
  },
  {
    id: 'core-features',
    category: 'Features',
    title: 'What are the main features of LicenseIQ?',
    content: `LicenseIQ offers comprehensive features for contract lifecycle management:

**AI-Powered Analysis:**
- Automated contract reading and term extraction using Groq LLaMA models
- Risk assessment and compliance checking
- Confidence scoring for AI extractions
- Multi-source contract ingestion (PDF, DocuSign, contract management systems)

**License Fee Management:**
- Dynamic rule engine for payment calculations
- Support for percentage, fixed fee, tiered pricing, minimum guarantees, and caps
- Seasonal adjustments and territory premiums
- Volume-based discounting
- FormulaNode JSON expression trees for complex calculations

**Payment Calculator Dashboard:**
- Customizable calculations with run history
- Professional PDF invoice generation
- Centralized Royalty Calculator
- Support for multiple pricing structures

**LIQ AI Assistant (RAG-Powered Q&A):**
- Omnipresent AI assistant accessible via floating button
- Context-aware chat for contract inquiries
- Source citations with confidence scores
- Semantic search using vector embeddings

**Master Data Management:**
- Three-level company hierarchy (Company → Business Unit → Location)
- Universal ERP catalog system supporting any ERP (SAP, Oracle, NetSuite)
- LicenseIQ Schema Catalog for data consistency
- AI-driven field mapping with dual schema auto-population

**Security & Compliance:**
- Five-tier Role-Based Access Control (RBAC)
- Secure session management with PostgreSQL storage
- Audit logging for all critical operations
- Contract metadata versioning and approval workflows

**Advanced Capabilities:**
- AI sales data matching using semantic embeddings
- Dynamic contract processing with knowledge graph construction
- Flexible CSV import with 70+ field aliases
- Mobile-responsive design with dark mode support`,
  },
  {
    id: 'ai-services',
    category: 'Technology',
    title: 'What AI services does LicenseIQ use?',
    content: `LicenseIQ uses 100% FREE AI services to keep costs minimal:

**Groq API (Primary LLM):**
- Model: LLaMA 3.1 70B and 8B variants
- Use cases: Contract analysis, term extraction, rule identification, Q&A responses
- Features: Fast inference, high accuracy, JSON output support
- Cost: Completely FREE

**HuggingFace Inference API (Embeddings):**
- Model: BAAI/bge-small-en-v1.5
- Dimensions: 384-dimensional vectors
- Use cases: Semantic search, contract matching, sales data alignment
- Storage: PostgreSQL with pgvector extension
- Index: HNSW (Hierarchical Navigable Small World) for fast similarity search
- Cost: Completely FREE

**Architecture Benefits:**
- No API costs for core functionality
- Asynchronous processing for better UX
- Scalable semantic search capabilities
- Real-time contract analysis
- Context-aware AI assistance

**Data Flow:**
1. Contract uploads are processed by Groq for extraction
2. Text chunks are embedded using HuggingFace
3. Embeddings stored in PostgreSQL with pgvector
4. Queries use vector similarity search
5. LIQ AI generates answers using retrieved context + Groq LLaMA`,
  },
  {
    id: 'terminology',
    category: 'Terminology',
    title: 'What terminology does LicenseIQ use?',
    content: `LicenseIQ uses specific standardized terminology:

**License Fee (NOT "Royalty"):**
The platform consistently uses "License Fee" instead of "Royalty" throughout the application for payment-related features. This includes:
- License Fee Rules Management
- License Fee Calculator
- License Fee Calculations
- Per-unit license fee rate

**LIQ AI (NOT "Contract Q&A"):**
The RAG-powered AI assistant is called "LIQ AI" - a conversational assistant that can answer questions about:
- Uploaded contract documents
- The LicenseIQ platform itself
- Platform features and capabilities
- How to use various functions

**Contract ID Format:**
Contracts are assigned unique IDs in the format: CNT-YYYY-NNN
- Example: CNT-2025-001, CNT-2025-002
- YYYY = Year
- NNN = Sequential number (001, 002, etc.)

**Master Data Hierarchy:**
- Company (GRP_ID): Top-level organization
- Business Unit (ORG_ID): Division or department
- Location (LOC_ID): Physical site or region

All LicenseIQ Schema entity records must link to all three levels as mandatory foreign keys.`,
  },
  {
    id: 'data-management',
    category: 'Data Management',
    title: 'How does LicenseIQ handle data management?',
    content: `LicenseIQ implements comprehensive data management:

**Multi-Tenant Architecture:**
- Mandatory 3-level company hierarchy (Company → Business Unit → Location)
- All records link to GRP_ID, ORG_ID, and LOC_ID as foreign keys
- Supports multi-entity operations

**ERP Integration:**
- Universal ERP Catalog System supporting any ERP platform
- Frontend-managed ERP configuration
- Dynamic catalog management
- Pre-configured support for SAP, Oracle, NetSuite

**LicenseIQ Schema Catalog:**
- 28 standard schema entities for data consistency
- Mirrors ERP Catalog architecture
- Provides standardized field definitions
- Ensures data quality across the platform

**AI Master Data Mapping:**
- AI-driven field mapping between ERPs and LicenseIQ schema
- Universal ERP support through dynamic catalogs
- Dual schema auto-population
- Intelligent field normalization

**Sales Data Import:**
- Flexible CSV column name variations
- 70+ field aliases (case-insensitive, whitespace-tolerant)
- Intelligent field normalization
- Support for .csv, .xlsx, .xls formats

**Required columns:** transactionDate, grossAmount
**Optional columns:** transactionId, productName, productCode, category, territory, currency, netAmount, quantity, unitPrice

**Data Security:**
- PostgreSQL database with Drizzle ORM
- Secure session management
- Audit trail logging
- Version control for contracts
- Role-based data access controls`,
  },
  {
    id: 'user-management',
    category: 'Security',
    title: 'What security features does LicenseIQ provide?',
    content: `LicenseIQ implements enterprise-grade security:

**Five-Tier Role-Based Access Control (RBAC):**
1. **Administrator** - Full system access, user management, system configuration
2. **Owner** - Business owner with full access to their organization's data
3. **Editor** - Can edit contracts and data, create rules, run calculations
4. **Viewer** - Read-only access to contracts and reports
5. **Auditor** - Access to audit trail and compliance reports

**Authentication & Sessions:**
- Passport.js authentication middleware
- Secure password hashing with bcrypt
- PostgreSQL session store (connect-pg-simple)
- Session timeout and automatic logout
- Protection against CSRF attacks

**Data Security:**
- All API endpoints require authentication
- Role-based authorization checks
- Secure file upload validation
- SQL injection protection via Drizzle ORM
- XSS protection through React's built-in escaping

**Audit Logging:**
- Comprehensive audit trail for all critical operations
- User action tracking
- Contract modification history
- Calculation run logs
- Compliance reporting

**Contract Metadata Management:**
- Version control for contracts
- Role-based approval workflows
- AI auto-population with manual override
- Change tracking and history`,
  },
  {
    id: 'getting-started',
    category: 'Usage',
    title: 'How do I get started with LicenseIQ?',
    content: `Getting started with LicenseIQ is simple:

**Step 1: Upload a Contract**
- Navigate to "Contracts" → "Upload"
- Click "Upload Contract" button
- Select PDF file (supports multi-page contracts)
- Wait 30-40 seconds for AI analysis
- Contract is automatically analyzed and categorized

**Step 2: Review AI-Extracted Terms**
- View extracted parties, dates, and key terms
- Check license fee rules automatically identified
- Review confidence scores for AI extractions
- Edit or correct any fields as needed

**Step 3: Configure License Fee Rules**
- Click "Manage License Fee Rules" on contract detail page
- Review AI-extracted rules
- Add custom rules if needed
- Configure volume tiers, seasonal adjustments, territory premiums
- Set minimum guarantees or caps

**Step 4: Upload Sales Data**
- Navigate to "Sales Data" → "Import Sales Data"
- Select the contract from dropdown (shows CNT-YYYY-NNN format)
- Upload CSV or Excel file
- System automatically matches sales to contract rules

**Step 5: Calculate Payments**
- Click "Calculate" on contract detail page
- Review calculated license fees
- Check rule applications and adjustments
- Generate PDF invoice if needed

**Step 6: Ask LIQ AI**
- Click the floating AI button (bottom-right)
- Ask questions about contracts or the platform
- Get answers with source citations
- Use for contract research and insights

**Tips:**
- Download sample CSV files to understand format
- Use dark mode for comfortable viewing
- Check calculation history for audit trails
- Review confidence scores on AI extractions`,
  },
  {
    id: 'calculations',
    category: 'Calculations',
    title: 'How does the license fee calculation work?',
    content: `LicenseIQ supports sophisticated license fee calculations:

**Calculation Methods:**
1. **Percentage-Based:** Rate × Gross Amount
2. **Fixed Fee:** Flat amount per transaction or period
3. **Volume Tiers:** Different rates based on sales volume ranges
4. **Minimum Guarantee:** Ensures minimum payment threshold
5. **Caps:** Maximum payment limit per period

**Advanced Features:**
- **Seasonal Adjustments:** Multipliers for specific time periods (e.g., spring: 1.15)
- **Territory Premiums:** Additional rates for specific regions (e.g., California: +0.50%)
- **Escalation Rates:** Automatic rate increases over time
- **Container Size Discounts:** Volume-based adjustments

**Formula Engine:**
- Uses FormulaNode JSON expression trees
- Supports complex nested calculations
- Handles multiple conditional rules
- Applies rules in priority order

**Calculation Process:**
1. Match sales data to contract rules
2. Identify applicable rules based on product/territory/date
3. Apply volume tiers if configured
4. Calculate seasonal adjustments
5. Add territory premiums
6. Apply minimum guarantee or cap
7. Generate detailed breakdown

**Calculation Output:**
- Line-item details with applied rules
- Subtotals by rule type
- Total license fee amount
- PDF invoice generation
- Calculation history and audit trail

**Example Calculation:**
Sales: $7,680,000
Base Rate: 6.5%
Volume Tier: 0-5M @ 6.5%
Seasonal: None (1.0)
Territory: None (1.0)

License Fee = $7,680,000 × 6.5% × 1.0 × 1.0 = $499,200

The system handles all calculations automatically and provides transparent breakdown for verification.`,
  },
  {
    id: 'liq-ai-capabilities',
    category: 'LIQ AI',
    title: 'What can LIQ AI help me with?',
    content: `LIQ AI is your intelligent assistant with two main capabilities:

**Contract Document Q&A:**
LIQ AI can answer questions about your uploaded contracts:
- "What is the royalty rate in the Electronics Patent License?"
- "When does the Manufacturing contract expire?"
- "What territories are covered in this agreement?"
- "Are there any volume discounts in the Plant Variety contract?"
- "What are the payment terms?"

**Platform Information:**
LIQ AI can also help you understand the LicenseIQ platform:
- "What is LicenseIQ?"
- "What contract types are supported?"
- "How do I upload sales data?"
- "What AI services does this platform use?"
- "How does the license fee calculator work?"
- "What security features are available?"

**How It Works:**
1. **Vector Embeddings:** All contracts and platform documentation are converted to 384-dimensional vectors using HuggingFace embeddings
2. **Semantic Search:** When you ask a question, LIQ AI searches for the most relevant information using vector similarity
3. **Context Retrieval:** Top matching sections are retrieved from the database
4. **Answer Generation:** Groq LLaMA model generates a comprehensive answer using the retrieved context
5. **Source Citations:** You get answers with source references and confidence scores

**Features:**
- Omnipresent floating button (bottom-right corner)
- Context-aware responses
- Source citations with similarity scores
- Confidence indicators
- Works across all contracts
- Handles both specific and general queries

**Best Practices:**
- Be specific in your questions
- Ask one question at a time
- Review source citations for verification
- Check confidence scores (higher is better)
- Use for quick contract research and insights`,
  },
  {
    id: 'deployment',
    category: 'Deployment',
    title: 'How is LicenseIQ deployed?',
    content: `LicenseIQ can be deployed to production using comprehensive guides:

**Deployment Options:**
1. **Hostinger VPS** (Recommended for production)
2. **Replit** (For development and testing)
3. **Other VPS providers** (DigitalOcean, Linode, AWS EC2, etc.)

**Hostinger VPS Deployment:**
Two comprehensive deployment guides are available:

**Command-Line Guide** (HOSTINGER_DEPLOYMENT_GUIDE.md):
- Terminal-based deployment instructions
- VPS setup and initial server configuration
- Node.js, PostgreSQL, and pgvector installation
- PM2 process manager setup
- Nginx reverse proxy configuration
- SSL certificate setup with Let's Encrypt
- Domain configuration and DNS setup
- Database backup and maintenance procedures
- Troubleshooting common issues

**UI-Based Guide** (HOSTINGER_UI_DEPLOYMENT_GUIDE.md):
- Visual walkthrough using Hostinger's hPanel
- Browser Terminal usage for installations
- hPanel-based domain DNS configuration
- Visual firewall setup
- Snapshot management
- No SSH client required - everything through web browser
- Perfect for users preferring GUI over command-line

**Technology Stack:**
- **OS:** Ubuntu 22.04/24.04
- **Process Manager:** PM2 (auto-restart, clustering)
- **Web Server:** Nginx (reverse proxy)
- **Database:** PostgreSQL 14+ with pgvector extension
- **SSL:** Let's Encrypt (free certificates)
- **Node.js:** v20.x LTS

**Production Features:**
- Automatic restart on crashes
- Zero-downtime deployments
- SSL/TLS encryption
- Database backups
- Resource monitoring
- Firewall configuration
- Domain management

The deployment process is well-documented and can be completed in 1-2 hours depending on experience level.`,
  },
];

// Helper function to get knowledge by category
export function getKnowledgeByCategory(category: string) {
  return systemKnowledgeBase.filter(kb => kb.category === category);
}

// Helper function to search knowledge base
export function searchKnowledge(query: string) {
  const lowerQuery = query.toLowerCase();
  return systemKnowledgeBase.filter(kb =>
    kb.title.toLowerCase().includes(lowerQuery) ||
    kb.content.toLowerCase().includes(lowerQuery) ||
    kb.category.toLowerCase().includes(lowerQuery)
  );
}
