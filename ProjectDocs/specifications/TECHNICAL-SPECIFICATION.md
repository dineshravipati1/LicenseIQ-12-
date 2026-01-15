# Technical Specification Document

**Version:** 1.0.0  
**Date:** October 23, 2025  
**Author:** Technical Architecture Team  
**Status:** Production Ready

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Technical Requirements](#technical-requirements)
3. [Technology Stack](#technology-stack)
4. [System Components](#system-components)
5. [Data Models](#data-models)
6. [API Specifications](#api-specifications)
7. [Security Requirements](#security-requirements)
8. [Performance Requirements](#performance-requirements)
9. [Scalability & Reliability](#scalability--reliability)

---

## 1. System Overview

### 1.1 Purpose
LicenseIQ is an AI-powered SaaS platform for automated contract analysis and royalty calculation targeting manufacturing companies with complex licensing agreements.

### 1.2 Scope
This document specifies all technical requirements, architecture decisions, and implementation details for the LicenseIQ platform version 1.0.

### 1.3 System Architecture Pattern
**Layered Architecture:**
```
├── Presentation Layer (React Frontend)
├── API Layer (Express.js REST API)
├── Business Logic Layer (Services & Calculation Engine)
└── Data Layer (PostgreSQL + File System)
```

---

## 2. Technical Requirements

### 2.1 Functional Requirements

#### FR-001: Contract Management
- **FR-001.1:** System shall accept PDF, DOCX, and TXT contract uploads
- **FR-001.2:** Maximum file size: 10 MB per contract
- **FR-001.3:** Auto-generate unique contract numbers (CNT-YYYY-NNN format)
- **FR-001.4:** Store contracts securely on server filesystem
- **FR-001.5:** Track contract metadata (upload date, user, status)

#### FR-002: AI Contract Analysis
- **FR-002.1:** Extract text from uploaded PDFs using pdf-parse
- **FR-002.2:** Send text to Groq API for AI analysis
- **FR-002.3:** Extract key terms: parties, dates, rates, territories
- **FR-002.4:** Identify risks and compliance issues
- **FR-002.5:** Generate summary and insights
- **FR-002.6:** Confidence scoring (0.0-1.0 scale)

#### FR-003: Royalty Rule Extraction
- **FR-003.1:** Identify royalty calculation clauses
- **FR-003.2:** Extract volume tiers (e.g., 0-1000 units: 5%, 1001+: 7%)
- **FR-003.3:** Extract seasonal adjustments
- **FR-003.4:** Build FormulaNode JSON expression trees
- **FR-003.5:** Link rules to source contract sections

#### FR-004: Embedding Generation
- **FR-004.1:** Generate 384-dimensional embeddings using Hugging Face
- **FR-004.2:** Store embeddings in pgvector
- **FR-004.3:** Create HNSW index for fast similarity search
- **FR-004.4:** Support embedding types: summary, key_terms, full_text

#### FR-005: Sales Data Import
- **FR-005.1:** Parse CSV and Excel files (papaparse, xlsx)
- **FR-005.2:** Validate required fields: date, amount, product
- **FR-005.3:** Maximum file size: 50 MB per import
- **FR-005.4:** Handle up to 100,000 rows per import

#### FR-006: AI Sales Matching
- **FR-006.1:** Generate embeddings for sales records
- **FR-006.2:** Perform semantic search against contract embeddings
- **FR-006.3:** Return top 3 matches per sales record
- **FR-006.4:** Validate matches using Groq LLM
- **FR-006.5:** Assign confidence scores: high (>0.8), medium (0.5-0.8), low (<0.5)
- **FR-006.6:** Auto-assign high-confidence matches, flag low-confidence for review

#### FR-007: Royalty Calculation
- **FR-007.1:** Interpret FormulaNode expression trees
- **FR-007.2:** Apply volume tier logic
- **FR-007.3:** Apply seasonal adjustments
- **FR-007.4:** Calculate multi-party revenue splits
- **FR-007.5:** Enforce minimum guarantees and caps
- **FR-007.6:** Generate detailed calculation breakdown

#### FR-008: PDF Invoice Generation
- **FR-008.1:** Create branded invoice templates
- **FR-008.2:** Include contract details, calculation breakdown
- **FR-008.3:** Support detailed and summary report formats
- **FR-008.4:** Generate PDF using html-pdf-node
- **FR-008.5:** Response time: <3 seconds

#### FR-009: RAG Q&A System
- **FR-009.1:** Accept natural language questions
- **FR-009.2:** Generate question embeddings
- **FR-009.3:** Retrieve top 5 relevant contract chunks
- **FR-009.4:** Generate answers using Groq LLM with context
- **FR-009.5:** Return source citations (contract sections)
- **FR-009.6:** Fallback to full contract context if low similarity
- **FR-009.7:** Display confidence scores

#### FR-010: User Management
- **FR-010.1:** 5-tier RBAC: Owner, Admin, Editor, Viewer, Auditor
- **FR-010.2:** Session-based authentication (PostgreSQL store)
- **FR-010.3:** bcrypt password hashing (10 rounds)
- **FR-010.4:** Session expiry: 7 days
- **FR-010.5:** Password minimum length: 8 characters

#### FR-011: Audit Trail
- **FR-011.1:** Log all CRUD operations
- **FR-011.2:** Track user, timestamp, action, entity
- **FR-011.3:** Capture before/after state changes
- **FR-011.4:** Permanent retention for compliance
- **FR-011.5:** SOX-compliant logging format

### 2.2 Non-Functional Requirements

#### NFR-001: Performance
- **NFR-001.1:** Page load time: <2 seconds (95th percentile)
- **NFR-001.2:** API response time: <500ms (95th percentile)
- **NFR-001.3:** Vector search: <50ms for 10,000 vectors
- **NFR-001.4:** Contract upload: <1 second (file save)
- **NFR-001.5:** AI analysis: 10-30 seconds (async background)

#### NFR-002: Scalability
- **NFR-002.1:** Support 100+ concurrent users
- **NFR-002.2:** Handle 10,000+ contracts per customer
- **NFR-002.3:** Process 1M+ sales records
- **NFR-002.4:** Store 100K+ vector embeddings

#### NFR-003: Reliability
- **NFR-003.1:** System uptime: 99.9% SLA
- **NFR-003.2:** Zero data loss (ACID transactions)
- **NFR-003.3:** Automatic error recovery for AI API failures
- **NFR-003.4:** Daily database backups

#### NFR-004: Security
- **NFR-004.1:** All data encrypted in transit (HTTPS)
- **NFR-004.2:** Passwords hashed with bcrypt
- **NFR-004.3:** Session tokens HTTP-only, secure cookies
- **NFR-004.4:** Input validation on all API endpoints (Zod)
- **NFR-004.5:** CSRF protection enabled

#### NFR-005: Usability
- **NFR-005.1:** Mobile-responsive design (320px - 4K)
- **NFR-005.2:** Accessibility: WCAG 2.1 AA compliance
- **NFR-005.3:** Keyboard navigation support
- **NFR-005.4:** Dark mode support

---

## 3. Technology Stack

### 3.1 Frontend Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Framework** | React | 18.x | UI library |
| **Language** | TypeScript | 5.x | Type safety |
| **Build Tool** | Vite | 5.x | Fast bundling |
| **Router** | Wouter | 3.x | Client routing |
| **State Mgmt** | TanStack Query | 5.x | Server state |
| **Forms** | React Hook Form | 7.x | Form handling |
| **Validation** | Zod | 3.x | Schema validation |
| **Styling** | TailwindCSS | 3.x | Utility CSS |
| **Components** | shadcn/ui | Latest | UI components |
| **Icons** | Lucide React | Latest | Icon library |
| **Charts** | Recharts | 2.x | Data visualization |

### 3.2 Backend Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Runtime** | Node.js | 20.x LTS | JavaScript runtime |
| **Framework** | Express.js | 4.x | Web server |
| **Language** | TypeScript | 5.x | Type safety |
| **Auth** | Passport.js | Latest | Authentication |
| **Sessions** | express-session | Latest | Session mgmt |
| **File Upload** | Multer | Latest | File handling |
| **PDF Parse** | pdf-parse | Latest | PDF extraction |
| **PDF Gen** | html-pdf-node | Latest | PDF creation |
| **CSV Parse** | papaparse | Latest | CSV parsing |

### 3.3 Database Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **RDBMS** | PostgreSQL | 15+ | Primary database |
| **ORM** | Drizzle | Latest | Type-safe queries |
| **Migrations** | drizzle-kit | Latest | Schema management |
| **Vector Ext** | pgvector | 0.5+ | Vector search |
| **Hosting** | Neon Database | Latest | Serverless PG |

### 3.4 AI Services

| Service | Model | API | Purpose |
|---------|-------|-----|---------|
| **LLM** | LLaMA 3.1 8B Instant | Groq API | Analysis, Q&A |
| **Embeddings** | bge-small-en-v1.5 | Hugging Face | Vector generation |
| **Fallback** | GPT-4 (optional) | OpenAI API | Advanced reasoning |

---

## 4. System Components

### 4.1 Frontend Components

#### 4.1.1 Core Pages
- **Dashboard** - Overview metrics, recent activity
- **Contracts** - Contract list with search/filter
- **Upload** - Contract upload interface
- **Contract Detail** - Analysis results, rules, Q&A
- **Sales** - Sales data import and matching
- **Royalty Calculator** - Calculation interface
- **Analytics** - Charts and insights
- **Rules Management** - View/edit royalty rules
- **User Management** - RBAC administration
- **Audit Trail** - Activity logs

#### 4.1.2 Reusable Components
- Form components (Input, Select, Textarea, etc.)
- Data tables with sorting and pagination
- Modal dialogs
- Toast notifications
- Loading skeletons
- File upload dropzone
- Chart components

### 4.2 Backend Services

#### 4.2.1 Storage Service (`IStorage`)
Abstraction layer over database operations:
- Contract CRUD
- Sales data CRUD
- Royalty rules CRUD
- User management
- Audit logging
- Vector search

#### 4.2.2 AI Services
- **GroqService** - LLM inference for analysis and Q&A
- **HuggingFaceService** - Embedding generation
- **RAGService** - Retrieval-Augmented Generation pipeline

#### 4.2.3 Calculation Engine
- **FormulaInterpreter** - Evaluate FormulaNode trees
- **RoyaltyCalculator** - Apply rules to sales data
- **BreakdownGenerator** - Create detailed reports

#### 4.2.4 PDF Service
- **InvoiceGenerator** - Create branded invoices
- **ReportGenerator** - Generate calculation reports

---

## 5. Data Models

### 5.1 Core Entities

#### 5.1.1 User
```typescript
{
  id: uuid,
  username: string (unique),
  passwordHash: string,
  role: 'owner' | 'admin' | 'editor' | 'viewer' | 'auditor',
  createdAt: timestamp
}
```

#### 5.1.2 Contract
```typescript
{
  id: uuid,
  contractNumber: string (CNT-YYYY-NNN, unique),
  uploadedBy: uuid (FK users),
  originalName: string,
  filePath: string,
  fileSize: number,
  status: 'uploaded' | 'processing' | 'analyzed' | 'failed',
  metadata: jsonb,
  createdAt: timestamp,
  updatedAt: timestamp
}
```

#### 5.1.3 Contract Analysis
```typescript
{
  id: uuid,
  contractId: uuid (FK contracts, unique),
  summary: text,
  keyTerms: jsonb {
    parties: string[],
    effectiveDate: date,
    expirationDate: date,
    territory: string[],
    baseRate: number
  },
  risks: jsonb[],
  insights: jsonb[],
  confidenceScore: float (0.0-1.0),
  analyzedAt: timestamp
}
```

#### 5.1.4 Royalty Rule
```typescript
{
  id: uuid,
  contractId: uuid (FK contracts),
  ruleName: string,
  formulaNode: jsonb (FormulaNode tree),
  sourceSection: string,
  sourceText: text,
  isAiGenerated: boolean,
  createdAt: timestamp,
  updatedAt: timestamp
}
```

**FormulaNode Structure:**
```typescript
type FormulaNode = {
  type: 'literal' | 'variable' | 'operator' | 'function' | 'volumeTier' | 'seasonal',
  value?: any,
  operator?: '+' | '-' | '*' | '/',
  function?: 'sum' | 'min' | 'max' | 'if',
  children?: FormulaNode[],
  tiers?: { min: number, max: number, rate: number }[],
  seasonalRates?: { season: string, multiplier: number }[]
}
```

#### 5.1.5 Contract Embedding
```typescript
{
  id: uuid,
  contractId: uuid (FK contracts),
  embeddingType: 'summary' | 'key_terms' | 'section',
  embedding: vector(384),
  sourceText: text,
  createdAt: timestamp
}
```

#### 5.1.6 Sales Data
```typescript
{
  id: uuid,
  contractId: uuid (FK contracts, nullable),
  saleDate: date,
  amount: decimal(10, 2),
  quantity: integer,
  productCode: string,
  productDescription: string,
  matchConfidence: float (0.0-1.0, nullable),
  needsReview: boolean,
  importedAt: timestamp
}
```

#### 5.1.7 Royalty Calculation
```typescript
{
  id: uuid,
  contractId: uuid (FK contracts),
  userId: uuid (FK users),
  periodStart: date,
  periodEnd: date,
  totalAmount: decimal(12, 2),
  breakdown: jsonb {
    ruleResults: { ruleId: uuid, amount: decimal }[],
    tierBreakdown: { tier: string, salesCount: number, amount: decimal }[]
  },
  calculatedAt: timestamp
}
```

#### 5.1.8 Audit Trail
```typescript
{
  id: uuid,
  userId: uuid (FK users),
  action: string ('create' | 'update' | 'delete' | 'login' | 'calculate'),
  entityType: string ('contract' | 'sales' | 'rule' | 'calculation'),
  entityId: uuid (nullable),
  changes: jsonb { before: any, after: any },
  ipAddress: string,
  userAgent: string,
  timestamp: timestamp
}
```

### 5.2 Database Indexes

```sql
-- Performance indexes
CREATE INDEX idx_contracts_uploaded_by ON contracts(uploaded_by);
CREATE INDEX idx_contracts_status ON contracts(status);
CREATE INDEX idx_sales_contract_id ON sales_data(contract_id);
CREATE INDEX idx_sales_date ON sales_data(sale_date);
CREATE INDEX idx_audit_user_id ON audit_trail(user_id);
CREATE INDEX idx_audit_timestamp ON audit_trail(timestamp);

-- Vector index (HNSW for fast similarity search)
CREATE INDEX idx_embeddings_vector ON contract_embeddings 
USING hnsw (embedding vector_cosine_ops);
```

---

## 6. API Specifications

### 6.1 RESTful Endpoints

#### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login user
- `POST /api/auth/logout` - Logout user
- `GET /api/auth/me` - Get current user

#### Contracts
- `GET /api/contracts` - List contracts (paginated)
- `POST /api/contracts` - Upload contract
- `GET /api/contracts/:id` - Get contract details
- `DELETE /api/contracts/:id` - Delete contract
- `GET /api/contracts/:id/analysis` - Get AI analysis

#### Sales Data
- `POST /api/sales/import` - Import CSV/Excel
- `GET /api/sales` - List sales records
- `POST /api/sales/match` - Trigger AI matching
- `PATCH /api/sales/:id` - Update match assignment

#### Royalty Calculations
- `POST /api/royalty/calculate` - Run calculation
- `GET /api/royalty/calculations` - List calculations
- `GET /api/royalty/calculations/:id` - Get calculation details

#### RAG Q&A
- `POST /api/rag/query` - Ask question
- `GET /api/rag/history` - Query history

#### Rules
- `GET /api/rules` - List rules
- `POST /api/rules` - Create rule
- `PATCH /api/rules/:id` - Update rule
- `DELETE /api/rules/:id` - Delete rule

### 6.2 Request/Response Format

**Standard Success Response:**
```json
{
  "success": true,
  "data": { ... },
  "meta": {
    "timestamp": "2025-10-23T10:30:00Z",
    "requestId": "uuid"
  }
}
```

**Standard Error Response:**
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input data",
    "details": [ ... ]
  }
}
```

---

## 7. Security Requirements

### 7.1 Authentication
- Session-based authentication (not JWT)
- PostgreSQL session store (connect-pg-simple)
- Secure HTTP-only cookies
- 7-day session expiry
- bcrypt password hashing (10 rounds)

### 7.2 Authorization (RBAC)
| Role | Permissions |
|------|-------------|
| **Owner** | Full system access, manage users, billing |
| **Admin** | Manage contracts, sales, rules, view audit logs |
| **Editor** | Create/edit contracts, sales, rules |
| **Viewer** | Read-only access to all data |
| **Auditor** | View audit logs only |

### 7.3 Data Protection
- HTTPS only (TLS 1.2+)
- Input validation (Zod schemas)
- SQL injection prevention (parameterized queries)
- XSS protection (Content Security Policy)
- CSRF protection (tokens)

### 7.4 File Upload Security
- File type validation (MIME + extension)
- File size limits (10 MB contracts, 50 MB sales)
- Virus scanning (future: ClamAV integration)
- Secure file storage (outside web root)

---

## 8. Performance Requirements

### 8.1 Response Time Targets

| Operation | Target | Maximum |
|-----------|--------|---------|
| Page load | <1s | 2s |
| API call | <200ms | 500ms |
| Vector search | <50ms | 100ms |
| Contract upload | <1s | 2s |
| AI analysis | 10-20s | 30s |
| Royalty calc | <2s | 5s |
| PDF generation | <3s | 5s |

### 8.2 Throughput Targets
- 100+ concurrent users
- 50+ requests/second
- 10+ file uploads/minute
- 5+ AI analyses/minute (Groq rate limit)

### 8.3 Resource Limits
- Database: 10GB storage (Neon free tier → paid as needed)
- File storage: 50GB (expandable)
- Memory: 2GB per server instance
- CPU: 2 vCPUs per instance

---

## 9. Scalability & Reliability

### 9.1 Horizontal Scaling
- Stateless API servers (session in PostgreSQL)
- Load balancer for multiple instances
- Database read replicas for queries
- CDN for static assets

### 9.2 Caching Strategy
- TanStack Query client-side caching
- Redis for session caching (future)
- Embedding caching (reduce API calls)

### 9.3 Disaster Recovery
- Daily automated database backups
- Point-in-time recovery (PITR) capability
- File storage replication
- Recovery Time Objective (RTO): 4 hours
- Recovery Point Objective (RPO): 24 hours

### 9.4 Monitoring & Logging
- Application logs (Winston)
- Error tracking (Sentry - future)
- Performance monitoring (APM - future)
- Uptime monitoring (Pingdom - future)

---

## 10. Development & Deployment

### 10.1 Development Environment
- **Platform:** Replit
- **Runtime:** Node.js 20.x
- **Database:** Neon PostgreSQL (dev branch)
- **Environment:** `.env` file with secrets

### 10.2 CI/CD Pipeline
- **Version Control:** Git + GitHub
- **Testing:** Playwright E2E tests (future)
- **Deployment:** Replit auto-deploy → Production (Vercel/Railway)
- **Staging:** Separate Neon database branch

### 10.3 Code Quality
- **Linting:** ESLint + Prettier
- **Type Checking:** TypeScript strict mode
- **Testing:** Unit + E2E coverage >80% (future)
- **Code Review:** Required for all PRs

---

## Appendix A: Glossary

- **RBAC:** Role-Based Access Control
- **RAG:** Retrieval-Augmented Generation
- **HNSW:** Hierarchical Navigable Small World (vector index)
- **FormulaNode:** JSON expression tree for calculations
- **pgvector:** PostgreSQL extension for vector similarity search
- **SLA:** Service Level Agreement
- **ACID:** Atomicity, Consistency, Isolation, Durability

---

## Appendix B: Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-10-23 | Initial specification |

---

**Document Approval:**  
Technical Lead: ________________  
Date: ________________

Engineering Manager: ________________  
Date: ________________
