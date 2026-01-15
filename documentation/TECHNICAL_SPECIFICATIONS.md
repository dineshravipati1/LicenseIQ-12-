# Licence IQ Research Platform - Technical Specifications

## System Overview
The Licence IQ Research Platform is a full-stack SaaS application designed for intelligent contract management and AI-powered document analysis.

## Architecture Stack

### Frontend Technology
- **Framework:** React 18 with TypeScript
- **Build Tool:** Vite with HMR
- **UI Library:** TailwindCSS + shadcn/ui components
- **State Management:** TanStack Query (React Query)
- **Routing:** Wouter (lightweight React router)
- **Form Handling:** React Hook Form with Zod validation
- **Icons:** Lucide React

### Backend Technology
- **Runtime:** Node.js with TypeScript
- **Framework:** Express.js
- **ORM:** Drizzle ORM with PostgreSQL
- **Authentication:** Session-based with passport.js
- **File Handling:** Multer for multipart uploads
- **AI Integration:** Groq API (LLaMA 3.1 8B Instant)

### Database
- **Primary Database:** PostgreSQL 15+
- **Connection Pool:** @neondatabase/serverless
- **Migration Tool:** Drizzle migrations
- **Session Storage:** PostgreSQL with connect-pg-simple

### External Services
- **AI Provider:** Groq API
- **Database Hosting:** Neon PostgreSQL
- **File Storage:** Local filesystem (expandable to cloud storage)

## Database Schema

### Core Entities
```sql
-- Users table with role-based access control
users {
  id: uuid (primary key)
  username: varchar(255) unique
  password: varchar(255) encrypted
  role: enum('owner', 'admin', 'editor', 'viewer', 'auditor')
  email: varchar(255) unique
  firstName: varchar(255)
  lastName: varchar(255)
  isActive: boolean default true
  createdAt: timestamp
  updatedAt: timestamp
}

-- Contracts table for document management
contracts {
  id: uuid (primary key)
  originalName: varchar(500)
  filePath: varchar(1000)
  fileSize: bigint
  fileType: varchar(100)
  contractType: varchar(100)
  status: enum('pending', 'processing', 'analyzed', 'failed')
  priority: enum('low', 'normal', 'high', 'urgent')
  uploadedBy: uuid (foreign key to users)
  processingTime: integer
  createdAt: timestamp
  updatedAt: timestamp
}

-- Contract analysis results from AI processing
contractAnalysis {
  id: uuid (primary key)
  contractId: uuid (foreign key to contracts)
  summary: text
  keyTerms: jsonb
  riskAnalysis: jsonb
  insights: jsonb
  confidence: varchar(10)
  processingTime: integer
  createdAt: timestamp
}

-- Audit trail for compliance and tracking
auditLogs {
  id: uuid (primary key)
  userId: uuid (foreign key to users)
  action: varchar(255)
  entityType: varchar(100)
  entityId: uuid
  details: jsonb
  ipAddress: varchar(45)
  userAgent: text
  createdAt: timestamp
}

-- Session storage for authentication
sessions {
  sid: varchar (primary key)
  sess: jsonb
  expire: timestamp
}
```

### Relationships
- `contracts.uploadedBy` → `users.id` (many-to-one)
- `contractAnalysis.contractId` → `contracts.id` (one-to-one)
- `auditLogs.userId` → `users.id` (many-to-one)

## API Endpoints

### Authentication Endpoints
```typescript
POST   /api/register          // User registration
POST   /api/login             // User login
POST   /api/logout            // User logout
GET    /api/user              // Get current user
```

### Contract Management Endpoints
```typescript
GET    /api/contracts         // List all contracts (paginated)
GET    /api/contracts/:id     // Get specific contract with analysis
POST   /api/contracts/upload  // Upload new contract file
PUT    /api/contracts/:id/reprocess // Reprocess contract with AI
DELETE /api/contracts/:id     // Delete contract (with permissions)
GET    /api/contracts/:id/export    // Export analysis report
```

### Analytics Endpoints
```typescript
GET    /api/analytics/metrics // Dashboard metrics
GET    /api/analytics/reports // Generate reports
```

### User Management Endpoints
```typescript
GET    /api/users             // List users (admin only)
PUT    /api/users/:id         // Update user (admin only)
DELETE /api/users/:id         // Delete user (owner only)
```

## AI Integration Specifications

### Groq Service Configuration
```typescript
interface GroqConfig {
  apiKey: string;           // GROQ_API_KEY environment variable
  baseUrl: string;          // https://api.groq.com/openai/v1
  model: string;            // llama-3.1-8b-instant
  temperature: number;      // 0.1 for consistent analysis
  maxTokens: number;        // 4000 for detailed responses
}
```

### Analysis Result Schema
```typescript
interface ContractAnalysisResult {
  summary: string;                    // 2-3 paragraph overview
  keyTerms: Array<{
    type: string;                     // Term category
    description: string;              // Term description
    confidence: number;               // 0.0 to 1.0
    location: string;                 // Document section reference
  }>;
  riskAnalysis: Array<{
    level: 'high' | 'medium' | 'low'; // Risk severity
    title: string;                    // Risk title
    description: string;              // Risk details
  }>;
  insights: Array<{
    type: string;                     // insight | opportunity | alert
    title: string;                    // Insight title
    description: string;              // Insight details
  }>;
  confidence: number;                 // Overall confidence score
}
```

## Security Specifications

### Authentication & Authorization
- **Session-based Authentication:** Secure session cookies with HttpOnly flag
- **Password Security:** bcrypt hashing with salt rounds
- **Role-Based Access Control:** 5-tier permission system
- **Session Timeout:** Configurable session expiration

### Data Security
- **Input Validation:** Zod schemas for all data inputs
- **SQL Injection Protection:** Parameterized queries via Drizzle ORM
- **File Upload Security:** Type validation, size limits, path sanitization
- **XSS Protection:** Content Security Policy headers

### Permission Matrix
```typescript
interface PermissionMatrix {
  owner: ['*'];                      // Full system access
  admin: ['user:*', 'contract:*'];   // User and contract management
  editor: ['contract:create', 'contract:update', 'contract:read'];
  viewer: ['contract:read'];         // Read-only access
  auditor: ['contract:read', 'audit:read']; // Compliance access
}
```

## File Processing Pipeline

### Supported File Types
- **PDF:** Direct text extraction
- **DOCX:** Microsoft Word document processing
- **TXT:** Plain text files
- **Future:** OCR for scanned documents

### Processing Workflow
1. **Upload Validation:** File type, size, and security checks
2. **Text Extraction:** Convert file content to plain text
3. **AI Analysis:** Send text to Groq API for analysis
4. **Result Storage:** Save structured analysis to database
5. **Status Updates:** Real-time processing status updates

### Error Handling
- **File Processing Errors:** Graceful error handling with user feedback
- **AI API Errors:** Retry logic with exponential backoff
- **Storage Errors:** Transaction rollback and cleanup

## Performance Specifications

### Response Time Targets
- **File Upload:** < 5 seconds for files up to 10MB
- **AI Analysis:** < 30 seconds average processing time
- **Dashboard Load:** < 2 seconds initial page load
- **Contract List:** < 1 second with pagination

### Scalability Considerations
- **Database Connection Pooling:** Efficient connection management
- **Async Processing:** Non-blocking file processing pipeline
- **Query Optimization:** Indexed queries and efficient joins
- **Caching Strategy:** Redis for session and frequently accessed data

## Deployment Specifications

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://...
PGHOST=...
PGPORT=5432
PGUSER=...
PGPASSWORD=...
PGDATABASE=...

# Authentication
SESSION_SECRET=...

# AI Service
GROQ_API_KEY=...

# Application
NODE_ENV=production
PORT=5000
```

### Infrastructure Requirements
- **CPU:** 2+ cores for concurrent processing
- **Memory:** 4GB+ RAM for Node.js and file processing
- **Storage:** 20GB+ for application and temporary files
- **Network:** High-bandwidth for file uploads and AI API calls

## Monitoring & Observability

### Logging Strategy
- **Application Logs:** Structured JSON logs with Winston
- **Access Logs:** HTTP request/response logging
- **Error Tracking:** Comprehensive error logging with stack traces
- **Audit Logs:** All user actions tracked for compliance

### Metrics Collection
- **Performance Metrics:** Response times, throughput
- **Business Metrics:** Upload counts, processing success rates
- **System Metrics:** CPU, memory, database performance
- **AI Metrics:** Analysis confidence scores, processing times

## Testing Strategy

### Test Coverage Areas
- **Unit Tests:** Individual function and component testing
- **Integration Tests:** API endpoint and database integration
- **Security Tests:** Authentication, authorization, input validation
- **Performance Tests:** Load testing and stress testing
- **User Acceptance Tests:** End-to-end user workflows

### Test Data Requirements
- **Sample Contracts:** Various document types and sizes
- **User Accounts:** Different role levels for testing
- **Edge Cases:** Malformed files, large documents, API failures