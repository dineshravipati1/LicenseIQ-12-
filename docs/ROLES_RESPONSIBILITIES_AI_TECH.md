# LicenseIQ Research Platform
## Roles, Responsibilities & AI-Powered Capabilities

---

## Platform Overview

LicenseIQ is an AI-native contract management and analysis platform that leverages cutting-edge AI services to automate contract processing, risk assessment, and compliance. The platform is built on a modern full-stack architecture designed for enterprise scalability.

---

## Technology Stack

### Frontend
| Technology | Purpose |
|------------|---------|
| React 18 | Modern UI framework with hooks and functional components |
| TypeScript | Type-safe development for maintainable code |
| TailwindCSS | Utility-first CSS framework for responsive design |
| shadcn/ui | Accessible, customizable component library |
| TanStack Query | Server state management and caching |
| Wouter | Lightweight client-side routing |
| Recharts | Interactive data visualization |
| Framer Motion | Smooth animations and transitions |

### Backend
| Technology | Purpose |
|------------|---------|
| Node.js | Server runtime environment |
| Express.js | RESTful API framework |
| TypeScript | Type-safe backend development |
| Drizzle ORM | Type-safe database operations |
| Passport.js | Authentication middleware |
| Multer | File upload handling |
| PDFKit | PDF generation for invoices/reports |

### Database
| Technology | Purpose |
|------------|---------|
| PostgreSQL (Neon) | Primary relational database |
| pgvector | Vector embeddings for semantic search |
| HNSW Index | High-performance similarity search |

### AI Services (100% FREE APIs)
| Service | Purpose |
|---------|---------|
| Groq API | LLaMA 3.3 70B model for contract analysis |
| HuggingFace | Text embeddings for semantic matching |
| RAG Pipeline | Document Q&A with source citations |

---

## Role Hierarchy

```
SYSTEM ADMIN (Super User)
    |
COMPANY ADMIN (Per Company)
    |
+-------------------+-------------------+
|                   |                   |
MANAGER          ANALYST            AUDITOR
    |
+-------+-------+
|       |       |
EDITOR  VIEWER  (Read-only)
```

---

## Detailed Roles & Responsibilities

### 1. SYSTEM ADMIN (Owner)

**Scope:** Full platform access across ALL companies

**Core Responsibilities:**
- Platform configuration and system settings
- Multi-tenant company management
- User provisioning and role assignment
- System health monitoring and maintenance
- Security policy enforcement
- Global audit log review

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| liQ AI Assistant | Configure system-wide AI settings, review AI performance metrics |
| AI Model Selection | Choose and configure Groq/HuggingFace models for optimal performance |
| RAG Configuration | Manage knowledge base indexing and embedding generation |
| AI Audit Trail | Review all AI-generated analysis for quality assurance |

**Tech Stack Access:**
- Full database administration via Drizzle ORM
- Server configuration and environment variables
- API endpoint management
- ERP Integration Hub (all companies)
- LicenseIQ Schema management

---

### 2. COMPANY ADMIN

**Scope:** Full access within assigned company hierarchy

**Core Responsibilities:**
- Company-level configuration
- Business Unit and Location management
- User role assignment within company
- Contract approval workflows
- ERP system integration setup
- Company-specific rule engine configuration

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| AI Contract Analysis | Configure AI extraction rules for company-specific terms |
| ERP Field Mapping | AI-powered mapping between ERP systems and LicenseIQ schema |
| AI Confidence Thresholds | Set minimum confidence levels for automated approvals |
| Custom Rule Training | Fine-tune AI rules for company-specific license fee structures |

**Tech Stack Access:**
- Master Data Mapping configuration
- ERP Catalog management
- Navigation customization
- Company-scoped API endpoints
- Import/Export functionality

---

### 3. MANAGER

**Scope:** Business Unit or Location level management

**Core Responsibilities:**
- Team oversight and workflow management
- Contract review and approval
- License fee calculation approval
- Report generation and analysis
- Escalation handling
- Quality control of AI-generated outputs

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| AI Risk Assessment | Review AI-flagged high-risk contracts for manual review |
| Payment Calculation Validation | Verify AI-calculated license fees before approval |
| AI Summary Reports | Generate executive summaries from complex contracts |
| Anomaly Detection | AI alerts for unusual patterns in sales/payment data |

**Tech Stack Access:**
- Dashboard analytics (Recharts)
- Approval workflow management
- Team performance metrics
- Location-scoped data views

---

### 4. ANALYST

**Scope:** Read and analyze data within assigned hierarchy

**Core Responsibilities:**
- Contract data analysis and reporting
- Sales data matching and validation
- Payment calculation execution
- Trend analysis and forecasting
- Compliance monitoring
- AI output validation and correction

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| liQ AI Q&A | Ask natural language questions about contract terms |
| AI Sales Matching | Automatic matching of sales data against contract embeddings |
| Semantic Search | Find similar contracts using vector similarity |
| AI Term Extraction | Extract key terms, dates, and obligations automatically |
| Bulk Analysis | Process multiple contracts with AI in batch mode |

**Tech Stack Access:**
- TanStack Query for data fetching
- Advanced filtering and search
- Export to CSV/Excel
- Vector search via pgvector

---

### 5. AUDITOR

**Scope:** Read-only audit access across assigned hierarchy

**Core Responsibilities:**
- Compliance verification
- Audit trail review
- Historical data analysis
- Regulatory reporting
- AI decision auditing
- Documentation review

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| AI Audit Trail | Full history of all AI-generated analysis and decisions |
| Compliance Checking | AI validates contracts against regulatory requirements |
| Version History | Track all changes to mappings and rules |
| Source Citations | RAG system provides exact source references for AI answers |

**Tech Stack Access:**
- Read-only database views
- Comprehensive audit logs
- Historical data archives
- Report generation (PDFKit)

---

### 6. EDITOR

**Scope:** Data entry and modification within assigned hierarchy

**Core Responsibilities:**
- Contract data entry and updates
- Sales data upload and validation
- Master data maintenance
- Document upload and organization
- AI output correction and feedback

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| AI Auto-Population | Automatically fill form fields from uploaded documents |
| Smart Validation | AI validates data entry against contract terms |
| Duplicate Detection | AI identifies potential duplicate records |
| OCR Integration | Extract data from scanned documents |

**Tech Stack Access:**
- React Hook Form for data entry
- File upload (Multer)
- Validation (Zod schemas)
- Real-time updates (TanStack Query)

---

### 7. VIEWER

**Scope:** Read-only access within assigned hierarchy

**Core Responsibilities:**
- View contract information
- Access reports and dashboards
- Read documentation
- Monitor status updates

**AI-Powered Capabilities:**
| AI Feature | How It Helps |
|------------|--------------|
| liQ AI Q&A | Ask questions about contracts in natural language |
| AI Summaries | View AI-generated contract summaries |
| Search | Use semantic search to find relevant information |

**Tech Stack Access:**
- Read-only API endpoints
- Dashboard viewing
- Report access
- Document viewing

---

## AI Feature Matrix by Role

| AI Feature | System Admin | Company Admin | Manager | Analyst | Auditor | Editor | Viewer |
|------------|:------------:|:-------------:|:-------:|:-------:|:-------:|:------:|:------:|
| liQ AI Assistant | Full Config | Company Config | Use | Use | Use | Use | Use |
| Contract Analysis | Configure | Configure | Review | Execute | Audit | - | View |
| ERP Field Mapping | Full | Company | - | - | Audit | - | - |
| Sales Matching | Configure | Configure | Approve | Execute | Audit | Upload | View |
| Payment Calculation | Configure | Configure | Approve | Execute | Audit | - | View |
| RAG Q&A | Configure | Use | Use | Use | Use | Use | Use |
| Risk Assessment | Configure | Configure | Review | Execute | Audit | - | View |
| Semantic Search | Configure | Use | Use | Use | Use | Use | Use |
| AI Audit Trail | Full | Company | View | View | Full | - | - |

---

## Tech Stack Integration by Role

| Component | System Admin | Company Admin | Manager | Analyst | Auditor | Editor | Viewer |
|-----------|:------------:|:-------------:|:-------:|:-------:|:-------:|:------:|:------:|
| PostgreSQL (Direct) | Full | - | - | - | - | - | - |
| Drizzle ORM | Admin | Scoped | Scoped | Read | Read | Write | Read |
| Express API | All Routes | Company Routes | BU Routes | Read Routes | Audit Routes | Write Routes | Read Routes |
| pgvector Search | Configure | Use | Use | Use | Use | Use | Use |
| Groq AI | Configure | Use | Use | Use | Audit | - | - |
| HuggingFace | Configure | Use | Use | Use | Audit | - | - |
| File Upload (Multer) | Configure | Use | Use | Use | - | Use | - |
| PDF Generation | Configure | Use | Use | Use | Use | - | - |

---

## Security & Access Control

### Authentication
- Session-based authentication via Passport.js
- Secure password hashing with bcrypt
- Session storage in PostgreSQL (connect-pg-simple)

### Authorization
- Role-Based Access Control (RBAC) with 7-tier hierarchy
- Mandatory 3-level company hierarchy (Company → Business Unit → Location)
- Context-aware data filtering at all API endpoints
- Admin bypass for system-level operations

### Data Isolation
- Multi-tenant architecture with company scoping
- Row-level security based on user context
- API endpoint filtering by org hierarchy
- Audit logging for all data changes

---

## AI Service Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    USER REQUEST                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 EXPRESS.JS API LAYER                        │
│  • Role validation       • Context filtering                │
│  • Request validation    • Rate limiting                    │
└─────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│   GROQ API       │ │  HUGGINGFACE     │ │   POSTGRESQL     │
│   (LLaMA 3.3)    │ │  (Embeddings)    │ │   (pgvector)     │
├──────────────────┤ ├──────────────────┤ ├──────────────────┤
│ • Contract       │ │ • Text embedding │ │ • Vector storage │
│   analysis       │ │ • Semantic       │ │ • Similarity     │
│ • Q&A responses  │ │   similarity     │ │   search         │
│ • Risk scoring   │ │ • Document       │ │ • RAG retrieval  │
│ • Term extraction│ │   indexing       │ │ • HNSW index     │
└──────────────────┘ └──────────────────┘ └──────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 REACT FRONTEND                               │
│  • TanStack Query caching  • Real-time updates              │
│  • Role-based UI           • Accessible components          │
└─────────────────────────────────────────────────────────────┘
```

---

## Summary

LicenseIQ Research Platform combines enterprise-grade role-based access control with AI-powered automation to deliver:

1. **Intelligent Contract Processing** - AI extracts terms, calculates fees, and identifies risks
2. **Multi-Tenant Security** - Mandatory 3-level hierarchy with role-based permissions
3. **ERP Integration** - AI-powered field mapping with version control
4. **Natural Language Q&A** - RAG-powered assistant with source citations
5. **Scalable Architecture** - Modern tech stack built for enterprise performance

Each role is designed to maximize productivity while maintaining security and compliance through AI-assisted workflows.
