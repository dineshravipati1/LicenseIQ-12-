# Overview

License IQ Research Platform is a SaaS web application for intelligent contract management and analysis. It automates contract processing, risk assessment, and compliance using AI, offering features like automated payment calculations, dynamic rule engines, and a RAG-powered Q&A system. The platform aims to reduce manual effort, provide actionable insights, and ensure revenue assurance for industries with complex licensing agreements through an "AI-Native" architecture.

# User Preferences

Preferred communication style: Simple, everyday language.

**Terminology Standards:**
- Use "License Fee" (NOT "Royalty") throughout the application
- Use "liQ AI" (NOT "Contract Q&A") for the RAG-powered assistant - Note: lowercase 'l', lowercase 'i', uppercase 'Q'

# System Architecture

## UI/UX Decisions
The platform features a modern, responsive UI built with React, TailwindCSS, and shadcn/ui, incorporating animated gradients, sticky navigation, and a comprehensive menu. It includes an omnipresent AI assistant via a floating button. Design elements are inspired by DualEntry.com, support dark mode, and offer interactive charts for analytics. The application supports a multi-theme system with 12 unique color schemes. Consistent USA date formatting (MM/DD/YYYY) is applied across the platform.

## Technical Implementations
The frontend utilizes React, TypeScript, Vite, Wouter for routing, TanStack Query for state management, and React Hook Form with Zod for validation. The backend is an Express.js and TypeScript RESTful API with a layered service architecture. PostgreSQL, with Drizzle ORM and pgvector, serves as the primary database. File storage uses Multer. A dynamic contract processing system employs a knowledge graph database schema for AI-powered zero-shot extraction and human-in-the-loop validation. Performance is optimized with consolidated AI extraction calls, defensive numeric validation, enhanced JSON recovery from AI responses, flexible territory matching, safe date parsing, and updated HuggingFace API endpoints. Sales data upload supports flexible CSV column name variations with intelligent field normalization. Multi-location context switching is implemented for dynamic organizational context management, impacting navigation and permissions. Navigation management includes categorized, collapsible menus with user-customizable drag-and-drop reordering, persisted in the database.

## Feature Specifications
- **AI Contract Reading & Analysis**: Automated term extraction, risk assessment, and compliance using Groq API.
- **Multi-Source Contract Ingestion**: Supports various document formats and platforms.
- **AI-Powered ERP Field Mapping**: Dynamic field mapping with a fine-tuned LLaMA model for ERP integrations.
- **Dynamic Rule Engine Management**: AI-extracted and manually-created payment rules using FormulaNode JSON expression trees.
- **AI Sales Matching**: Semantic matching of sales data against contract embeddings with LLM validation.
- **Payment Calculation Dashboard**: Customizable calculations, run history, and professional PDF invoice generation, including a centralized Royalty Calculator.
- **RAG-Powered Document Q&A System**: AI-powered chat for contract inquiries with source citations.
- **Omnipresent AI Agent**: Global, context-aware AI assistant supporting both platform and contract-specific questions.
- **Security & Access Control**: Five-tier Role-Based Access Control (RBAC), secure session management, and audit logging.
- **Dynamic Contract Processing**: AI-powered pipeline for zero-shot entity extraction and knowledge graph construction.
- **Universal Contract Processing System**: AI extraction for all contract types and pricing structures.
- **Dynamic Rule Form Implementation**: Adapts fields based on selected rule type.
- **Expanded Payment Terms Extraction**: Extracts various payment-related clauses beyond royalties.
- **Contract Metadata Management**: AI auto-population, version control, and role-based approval workflows for contract metadata.
- **Universal ERP Catalog System**: Frontend-managed ERP configuration system supporting any ERP via dynamic catalog management.
- **LicenseIQ Schema Catalog**: Manageable standard schema system mirroring ERP Catalog architecture for data consistency. Includes Entity Data Browser with multi-tenant organizational context filtering (Company → Business Unit → Location), SQL injection protection via column validation/quoted identifiers, and context-aware CRUD operations that preserve tenant scope.
- **AI Master Data Mapping**: AI-driven field mapping with universal ERP support and dual schema auto-population via dynamic catalog integration.
- **Comprehensive Contract Search**: Full content-based search across contract metadata, AI analysis data, royalty rules, user fields, and dates with RBAC enforcement.
- **User-Organization Role Management**: System for assigning users to organizations with role-based access control at company, business unit, and location levels.
- **Categorized Navigation System**: Intelligent database-driven menu organization with collapsible categories, user-customizable preferences, and persistent expand/collapse state per user.
- **Multi-Location Data Filtering**: Comprehensive hierarchical access control enforcing organizational context across ALL data tables (contracts, sales, calculations) with admin bypass, preventing cross-context data leakage.
- **Two-Tier Admin System**: System Admin (super user managing all companies) and Company Admin (managing only their own company) with database-driven permissions.
- **ERP Integration Hub**: Centralized interface for managing versioned field mappings and data imports with full 3-level hierarchy access control (company/BU/location), dry-run previews, version history with revert capability, and approval workflows.
- **Pre-Import Data Filtering**: Industry-standard filter builder for import sources supporting dynamic criteria (equals, contains, greater than, between, etc.) for text, number, date, and boolean fields with AND/OR logic, filter preview endpoint for testing before import, and filter statistics in import job metadata.
- **Dual Terminology Mapping**: ERP-aware contract extraction that maps contract terms to ERP fields using AI with confidence scores. Workflow: AI extracts entities → AI auto-maps to ERP vocabulary → User confirms/modifies mappings via UI → Rules display both contract terms and ERP field names (e.g., "Net Revenue (ERP: GROSS_SALES)"). Pending mappings stored in pending_term_mappings table with auto-confirmation for high-confidence matches.
- **ERP Mapping Rules System**: Parallel automated license fee calculation system operating alongside manual rules. Features organization-level calculation approach toggle (Manual/ERP Rules/Hybrid), ERP Mapping Rule Sets management with CRUD operations, field mapping configurations, and full audit trail via execution logs. Supports gradual adoption with per-contract overrides.
- **Rule Source Indicators**: Visual badges showing rule origin (AI Extracted vs Manual) with tooltips displaying confidence scores and contract sections for AI-extracted rules. Source inferred from existing metadata (sourceText, sourceSection, confidence fields).
- **Multi-Dimensional Calculation Reports**: After running license fee calculations, results can be viewed in multiple dimensions dynamically based on ERP field mappings. Includes Summary, Detail, Vendor-wise, Item-wise, Item Class-wise, Territory-wise, and Period-wise views. Dimensions are auto-generated from confirmed ERP mappings per contract. Features tabbed interface with aggregation, percentage breakdown, and PDF export functionality.

## System Design Choices
The architecture emphasizes an AI-native design, integrating AI capabilities throughout. It prioritizes asynchronous AI processing with free APIs and uses a relational data model for core entities. The platform is designed for enterprise readiness, supporting multi-entity operations, user management, audit trails, and flexible data import. The system includes robust multi-location context filtering and a two-tier admin system for granular access control. Server startup seeding ensures consistent data across environments.

# External Dependencies

## Database Services
- **Neon PostgreSQL**: Serverless PostgreSQL hosting.
- **Drizzle ORM**: TypeScript-first ORM.

## AI Services
- **Groq API**: LLaMA model inference for contract analysis.
- **Hugging Face Embeddings**: For semantic search and vector generation.

## UI Components
- **Radix UI**: Accessible UI primitives.
- **shadcn/ui**: Pre-built components with TailwindCSS.
- **Recharts**: Charting library.

## Development Tools
- **Vite**: Modern build tool.
- **TypeScript**: Static type checking.

## Authentication & Session Management
- **Passport.js**: Authentication middleware.
- **connect-pg-simple**: PostgreSQL session store.

## File Processing
- **Multer**: File upload middleware.
- **html-pdf-node**: PDF generation.

## Utility Libraries
- **date-fns**: Date utility library.
- **React Hook Form**: Form management.
- **TanStack Query**: Data synchronization.
- **Wouter**: Client-side routing.
- **Zod**: Schema validation.