# System Architecture Document

**Version:** 1.0.0  
**Date:** October 23, 2025  
**Classification:** Technical Architecture

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [System Components](#2-system-components)
3. [Data Architecture](#3-data-architecture)
4. [Integration Architecture](#4-integration-architecture)
5. [Deployment Architecture](#5-deployment-architecture)

---

## 1. Architecture Overview

### 1.1 High-Level Architecture

```mermaid
graph TB
    subgraph "Client Tier"
        Browser[Web Browser]
        Mobile[Mobile Browser]
    end
    
    subgraph "Application Tier"
        LB[Load Balancer]
        API1[API Server 1]
        API2[API Server 2]
        APIn[API Server N]
    end
    
    subgraph "Service Tier"
        Storage[Storage Service]
        Calc[Calculation Engine]
        PDF[PDF Generator]
        AI[AI Service Layer]
    end
    
    subgraph "Data Tier"
        PG[(PostgreSQL + pgvector)]
        Files[File Storage]
        Cache[(Redis Cache - Future)]
    end
    
    subgraph "External Services"
        Groq[Groq API]
        HF[Hugging Face API]
    end
    
    Browser --> LB
    Mobile --> LB
    LB --> API1
    LB --> API2
    LB --> APIn
    
    API1 --> Storage
    API1 --> Calc
    API1 --> PDF
    API1 --> AI
    
    Storage --> PG
    Storage --> Files
    AI --> Groq
    AI --> HF
    
    Calc --> Storage
    PDF --> Storage
```

### 1.2 Architecture Principles

**1. Separation of Concerns**
- Frontend handles presentation only
- Backend handles business logic
- Database handles data persistence
- External APIs handle AI processing

**2. Type Safety**
- TypeScript across entire stack
- Drizzle ORM for type-safe database queries
- Zod for runtime validation
- No runtime type errors

**3. Stateless API Servers**
- Session state in PostgreSQL
- No in-memory session storage
- Horizontal scaling ready
- Load balancer compatible

**4. API-First Design**
- RESTful endpoints
- JSON request/response
- Versioned APIs (future: `/api/v1/...`)
- Client-agnostic (supports web, mobile, third-party)

**5. Security by Default**
- HTTPS only
- Session-based authentication
- RBAC on all endpoints
- Input validation everywhere

---

## 2. System Components

### 2.1 Frontend Architecture

```mermaid
graph LR
    subgraph "React Application"
        Router[Wouter Router]
        Pages[14 Page Components]
        Components[Reusable Components]
        State[TanStack Query]
        Forms[React Hook Form]
    end
    
    subgraph "UI Layer"
        shadcn[shadcn/ui]
        Tailwind[TailwindCSS]
        Icons[Lucide Icons]
        Charts[Recharts]
    end
    
    Router --> Pages
    Pages --> Components
    Components --> shadcn
    shadcn --> Tailwind
    Pages --> State
    Pages --> Forms
    Components --> Icons
    Components --> Charts
```

**Key Design Patterns:**

**1. Container/Presenter Pattern**
```typescript
// Container (smart component)
function ContractListContainer() {
  const { data, isLoading } = useQuery({ queryKey: ['/api/contracts'] });
  return <ContractListPresenter contracts={data} loading={isLoading} />;
}

// Presenter (dumb component)
function ContractListPresenter({ contracts, loading }) {
  if (loading) return <Skeleton />;
  return <Table data={contracts} />;
}
```

**2. Custom Hooks for Business Logic**
```typescript
function useContractUpload() {
  const mutation = useMutation({
    mutationFn: (file: File) => uploadContract(file),
    onSuccess: () => queryClient.invalidateQueries(['/api/contracts'])
  });
  return { upload: mutation.mutate, isUploading: mutation.isPending };
}
```

**3. Optimistic Updates**
```typescript
useMutation({
  mutationFn: updateContract,
  onMutate: async (newData) => {
    await queryClient.cancelQueries(['/api/contracts', id]);
    const previous = queryClient.getQueryData(['/api/contracts', id]);
    queryClient.setQueryData(['/api/contracts', id], newData);
    return { previous };
  },
  onError: (err, variables, context) => {
    queryClient.setQueryData(['/api/contracts', id], context.previous);
  }
});
```

### 2.2 Backend Architecture

```mermaid
graph TB
    subgraph "API Layer"
        Express[Express.js Server]
        Routes[Route Handlers]
        Middleware[Middleware Pipeline]
    end
    
    subgraph "Business Logic"
        Storage[Storage Interface]
        Services[AI Services]
        Calculator[Calculation Engine]
        PDFGen[PDF Generator]
    end
    
    subgraph "Data Access"
        Drizzle[Drizzle ORM]
        FileSystem[File System]
    end
    
    Express --> Middleware
    Middleware --> Routes
    Routes --> Storage
    Routes --> Services
    Routes --> Calculator
    Routes --> PDFGen
    
    Storage --> Drizzle
    Storage --> FileSystem
    Services --> Drizzle
    Calculator --> Storage
    PDFGen --> Storage
```

**Middleware Pipeline:**
```typescript
// 1. CORS
app.use(cors({ origin: allowedOrigins, credentials: true }));

// 2. Body Parsing
app.use(express.json({ limit: '50mb' }));

// 3. Session Management
app.use(session({ store: pgSession, secret, cookie: { httpOnly: true, secure: true } }));

// 4. Authentication
app.use(passport.initialize());
app.use(passport.session());

// 5. Request Logging
app.use(requestLogger);

// 6. Routes
app.use('/api', routes);

// 7. Error Handler
app.use(errorHandler);
```

**Service Layer Pattern:**
```typescript
// IStorage interface (abstraction)
interface IStorage {
  createContract(data: InsertContract): Promise<Contract>;
  getContract(id: string): Promise<Contract | null>;
  updateContract(id: string, data: Partial<Contract>): Promise<Contract>;
  deleteContract(id: string): Promise<void>;
}

// DrizzleStorage implementation
class DrizzleStorage implements IStorage {
  async createContract(data: InsertContract): Promise<Contract> {
    const [contract] = await db.insert(contracts).values(data).returning();
    return contract;
  }
  // ... other methods
}
```

### 2.3 AI Service Architecture

```mermaid
sequenceDiagram
    participant API as API Server
    participant GroqSvc as Groq Service
    participant HFSvc as HF Service
    participant RAGSvc as RAG Service
    participant DB as PostgreSQL
    
    Note over API,DB: Contract Analysis Flow
    API->>GroqSvc: analyzeContract(text)
    GroqSvc->>GroqSvc: Build AI prompt
    GroqSvc->>Groq API: POST /chat/completions
    Groq API-->>GroqSvc: JSON response
    GroqSvc->>GroqSvc: extractAndRepairJSON()
    GroqSvc-->>API: Structured analysis
    API->>DB: Save analysis
    
    Note over API,DB: Embedding Generation
    API->>HFSvc: generateEmbedding(text)
    HFSvc->>HF API: POST /models/.../infer
    HF API-->>HFSvc: 384-dim vector
    HFSvc-->>API: Embedding
    API->>DB: Save to pgvector
    
    Note over API,DB: RAG Query
    API->>RAGSvc: query(question, contractId)
    RAGSvc->>HFSvc: Embed question
    HFSvc-->>RAGSvc: Question vector
    RAGSvc->>DB: Similarity search
    DB-->>RAGSvc: Top 5 chunks
    RAGSvc->>GroqSvc: Answer with context
    GroqSvc-->>RAGSvc: AI answer
    RAGSvc-->>API: Answer + citations
```

**AI Service Error Handling:**
```typescript
class GroqService {
  async analyzeContract(text: string, retries = 3): Promise<Analysis> {
    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(GROQ_API, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${GROQ_API_KEY}` },
          body: JSON.stringify({ model: 'llama-3.1-8b-instant', messages: [...] })
        });
        
        if (!response.ok) throw new Error(`Groq API error: ${response.status}`);
        
        const data = await response.json();
        const content = data.choices[0].message.content;
        return extractAndRepairJSON(content);
        
      } catch (error) {
        if (i === retries - 1) throw error;
        await sleep(2 ** i * 1000); // Exponential backoff
      }
    }
  }
}
```

### 2.4 Calculation Engine Architecture

```mermaid
graph TB
    subgraph "Calculation Engine"
        Parser[Formula Parser]
        Interpreter[Formula Interpreter]
        Evaluator[Expression Evaluator]
    end
    
    subgraph "Formula Types"
        Literal[Literal Values]
        Variable[Variables]
        Operator[Operators]
        Function[Functions]
        VolumeTier[Volume Tiers]
        Seasonal[Seasonal Adjustments]
    end
    
    subgraph "Data Sources"
        Rules[(Royalty Rules)]
        Sales[(Sales Data)]
    end
    
    Rules --> Parser
    Parser --> Interpreter
    Sales --> Evaluator
    Interpreter --> Evaluator
    
    Literal --> Evaluator
    Variable --> Evaluator
    Operator --> Evaluator
    Function --> Evaluator
    VolumeTier --> Evaluator
    Seasonal --> Evaluator
```

**FormulaNode Evaluation:**
```typescript
function evaluateFormula(node: FormulaNode, context: SalesData[]): number {
  switch (node.type) {
    case 'literal':
      return node.value;
      
    case 'variable':
      return context[node.name]; // e.g., context.totalAmount
      
    case 'operator':
      const left = evaluateFormula(node.left, context);
      const right = evaluateFormula(node.right, context);
      return applyOperator(node.operator, left, right);
      
    case 'volumeTier':
      const quantity = context[node.variable];
      const tier = node.tiers.find(t => quantity >= t.min && quantity <= t.max);
      return quantity * tier.rate;
      
    case 'seasonal':
      const month = new Date(context.saleDate).getMonth();
      const season = getSeasonForMonth(month);
      const multiplier = node.seasonalRates[season];
      return baseAmount * multiplier;
      
    case 'function':
      return evaluateFunction(node.function, node.args, context);
  }
}
```

---

## 3. Data Architecture

### 3.1 Database Schema (ER Diagram)

```mermaid
erDiagram
    USERS ||--o{ CONTRACTS : uploads
    USERS ||--o{ ROYALTY_CALCULATIONS : performs
    USERS ||--o{ AUDIT_TRAIL : generates
    
    CONTRACTS ||--|| CONTRACT_ANALYSIS : has
    CONTRACTS ||--o{ ROYALTY_RULES : contains
    CONTRACTS ||--o{ CONTRACT_EMBEDDINGS : has
    CONTRACTS ||--o{ SALES_DATA : matches_to
    CONTRACTS ||--o{ ROYALTY_CALCULATIONS : calculates_for
    
    ROYALTY_CALCULATIONS ||--o{ CALCULATION_DETAILS : has
    ROYALTY_CALCULATIONS ||--o{ INVOICES : generates
    
    USERS {
        uuid id PK
        varchar username UK
        varchar password_hash
        enum role
        timestamp created_at
    }
    
    CONTRACTS {
        uuid id PK
        varchar contract_number UK
        uuid uploaded_by FK
        varchar file_path
        enum status
        jsonb metadata
        timestamp created_at
    }
    
    CONTRACT_ANALYSIS {
        uuid id PK
        uuid contract_id FK
        text summary
        jsonb key_terms
        jsonb risks
        jsonb insights
        float confidence_score
        timestamp analyzed_at
    }
    
    ROYALTY_RULES {
        uuid id PK
        uuid contract_id FK
        varchar rule_name
        jsonb formula_node
        varchar source_section
        text source_text
        boolean is_ai_generated
    }
    
    CONTRACT_EMBEDDINGS {
        uuid id PK
        uuid contract_id FK
        enum embedding_type
        vector_384 embedding
        text source_text
    }
    
    SALES_DATA {
        uuid id PK
        uuid contract_id FK
        date sale_date
        decimal amount
        integer quantity
        varchar product_code
        float match_confidence
        boolean needs_review
    }
    
    ROYALTY_CALCULATIONS {
        uuid id PK
        uuid contract_id FK
        uuid user_id FK
        date period_start
        date period_end
        decimal total_amount
        jsonb breakdown
        timestamp calculated_at
    }
    
    AUDIT_TRAIL {
        uuid id PK
        uuid user_id FK
        varchar action
        varchar entity_type
        uuid entity_id
        jsonb changes
        varchar ip_address
        timestamp timestamp
    }
```

### 3.2 Vector Search Architecture

**pgvector Integration:**
```sql
-- Enable extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create table with vector column
CREATE TABLE contract_embeddings (
  id UUID PRIMARY KEY,
  contract_id UUID REFERENCES contracts(id),
  embedding_type VARCHAR(50),
  embedding VECTOR(384),  -- 384 dimensions
  source_text TEXT
);

-- Create HNSW index for fast similarity search
CREATE INDEX idx_embeddings_vector 
ON contract_embeddings 
USING hnsw (embedding vector_cosine_ops);
```

**Similarity Search Query:**
```typescript
// Find similar contracts using cosine similarity
const results = await db.execute(sql`
  SELECT 
    ce.contract_id,
    ce.source_text,
    1 - (ce.embedding <=> ${questionEmbedding}::vector) AS similarity
  FROM contract_embeddings ce
  WHERE ce.embedding_type = 'summary'
  ORDER BY ce.embedding <=> ${questionEmbedding}::vector
  LIMIT 5
`);
```

### 3.3 Data Flow Patterns

**Write Pattern (CQRS-lite):**
```typescript
// Command: Create contract
async function uploadContract(file: File, userId: string) {
  // 1. Validate
  if (file.size > MAX_SIZE) throw new Error('File too large');
  
  // 2. Save file
  const filePath = await saveFile(file);
  
  // 3. Insert database record
  const contract = await storage.createContract({
    uploadedBy: userId,
    filePath,
    status: 'uploaded'
  });
  
  // 4. Trigger async analysis
  analyzeContractAsync(contract.id);
  
  // 5. Audit log
  await storage.createAuditLog({
    userId,
    action: 'contract_upload',
    entityId: contract.id
  });
  
  return contract;
}
```

**Read Pattern (Query):**
```typescript
// Query: Get contract with analysis
async function getContractDetails(contractId: string) {
  // Join contract + analysis + rules in single query
  const result = await db.query.contracts.findFirst({
    where: eq(contracts.id, contractId),
    with: {
      analysis: true,
      rules: true,
      embeddings: true
    }
  });
  
  return result;
}
```

---

## 4. Integration Architecture

### 4.1 External Service Integration

```mermaid
graph TB
    subgraph "LicenseIQ Platform"
        API[API Server]
        Cache[Result Cache]
    end
    
    subgraph "AI Services"
        Groq[Groq API<br/>LLaMA 3.1]
        HF[Hugging Face<br/>Embeddings]
        OpenAI[OpenAI<br/>GPT-4 Fallback]
    end
    
    subgraph "Future Integrations"
        SAP[SAP Connector]
        Oracle[Oracle ERP]
        NetSuite[NetSuite]
        Stripe[Stripe Payments]
    end
    
    API -->|Primary| Groq
    API -->|Embeddings| HF
    API -->|Fallback| OpenAI
    
    Groq --> Cache
    HF --> Cache
    Cache --> API
    
    API -.->|Future| SAP
    API -.->|Future| Oracle
    API -.->|Future| NetSuite
    API -.->|Future| Stripe
```

**Integration Patterns:**

**1. Circuit Breaker Pattern**
```typescript
class CircuitBreaker {
  private failures = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';
  
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === 'open') {
      throw new Error('Circuit breaker is open');
    }
    
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }
  
  private onFailure() {
    this.failures++;
    if (this.failures >= 5) {
      this.state = 'open';
      setTimeout(() => this.state = 'half-open', 60000); // 1 min
    }
  }
}
```

**2. Retry with Exponential Backoff**
```typescript
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      const delay = Math.min(1000 * 2 ** i, 10000);
      await sleep(delay);
    }
  }
}
```

### 4.2 API Versioning Strategy

**Current (v1.0):**
- No version prefix: `/api/contracts`
- All endpoints under `/api`

**Future (v2.0+):**
- Versioned prefix: `/api/v2/contracts`
- Maintain `/api/v1/...` for backward compatibility
- Deprecation warnings for old versions

---

## 5. Deployment Architecture

### 5.1 Development Environment

```
Developer Machine
├── Code Editor (VSCode)
├── Git (version control)
└── Replit Environment
    ├── Node.js 20 runtime
    ├── PostgreSQL (Neon dev branch)
    ├── File storage (/uploads)
    └── Environment variables (.env)
```

### 5.2 Production Architecture

```mermaid
graph TB
    subgraph "CDN Layer"
        CF[Cloudflare CDN]
    end
    
    subgraph "Application Layer"
        LB[Load Balancer]
        Web1[Web Server 1]
        Web2[Web Server 2]
        Web3[Web Server N]
    end
    
    subgraph "Data Layer"
        PGPrimary[(PostgreSQL Primary)]
        PGReplica1[(PG Replica 1)]
        PGReplica2[(PG Replica 2)]
        S3[S3 File Storage]
    end
    
    subgraph "Monitoring"
        Sentry[Sentry Error Tracking]
        Metrics[Metrics/APM]
    end
    
    Users --> CF
    CF --> LB
    LB --> Web1
    LB --> Web2
    LB --> Web3
    
    Web1 --> PGPrimary
    Web2 --> PGReplica1
    Web3 --> PGReplica2
    
    Web1 --> S3
    Web2 --> S3
    Web3 --> S3
    
    Web1 --> Sentry
    Web2 --> Metrics
```

### 5.3 Scaling Strategy

**Vertical Scaling (Current):**
- Single server instance
- Scale CPU/RAM as needed
- Good for 0-100 users

**Horizontal Scaling (Future):**
- Multiple API server instances
- Load balancer distribution
- Session in PostgreSQL (stateless servers)
- Good for 100-10,000 users

**Database Scaling:**
- Read replicas for query distribution
- Connection pooling (pgBouncer)
- Caching layer (Redis) for hot data

---

## Appendix: Architecture Decision Records (ADRs)

### ADR-001: Session-Based Auth vs JWT

**Decision:** Use session-based authentication  
**Rationale:**
- Better security (server-side revocation)
- Simpler implementation
- No token expiry complexity
- PostgreSQL session store (already using PG)

**Alternatives Considered:**
- JWT: Rejected due to no server-side revocation
- OAuth: Overkill for initial MVP

### ADR-002: Drizzle vs Prisma ORM

**Decision:** Use Drizzle ORM  
**Rationale:**
- Better TypeScript inference
- Zero runtime overhead
- SQL-like syntax (easier for developers)
- Better pgvector support

**Alternatives Considered:**
- Prisma: Good, but heavier runtime
- TypeORM: Less type-safe

### ADR-003: Groq vs OpenAI for Primary LLM

**Decision:** Use Groq as primary, OpenAI as fallback  
**Rationale:**
- Groq: Free tier, 500+ tok/s, sufficient for analysis
- OpenAI: Better reasoning, use for complex cases
- Cost optimization (free first)

**Alternatives Considered:**
- OpenAI only: Too expensive
- Anthropic Claude: Good, but Groq faster

---

**Document Maintained By:** Architecture Team  
**Review Cycle:** Quarterly  
**Last Reviewed:** October 23, 2025
