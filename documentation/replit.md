# Overview

Licence IQ Research Platform is a SaaS web application built for intelligent contract management and analysis. The platform enables users to upload, process, and analyze legal contracts using AI-powered document processing, OCR, and semantic search capabilities. It provides a comprehensive dashboard for contract portfolio management with role-based access control and audit trails.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Frontend Architecture
- **React + TypeScript**: Modern component-based frontend with strict type safety
- **Vite**: Fast build tool and development server with hot module replacement
- **TailwindCSS + shadcn/ui**: Utility-first CSS framework with pre-built component library for consistent UI
- **React Router (wouter)**: Lightweight client-side routing
- **TanStack Query**: Server state management with caching and synchronization
- **React Hook Form**: Form state management with validation

## Backend Architecture
- **Express.js + TypeScript**: RESTful API server with modular route structure
- **Session-based Authentication**: Replit OAuth integration with secure session storage
- **Layered Service Architecture**: Separation of concerns with dedicated services for file handling, AI processing, and data access
- **Middleware Pipeline**: Request logging, authentication, and error handling

## Database Layer
- **PostgreSQL with Drizzle ORM**: Type-safe database operations with schema-first approach
- **Neon Database**: Serverless PostgreSQL deployment
- **Connection Pooling**: Efficient database connection management
- **Migration System**: Version-controlled database schema changes

## File Management
- **Local File Storage**: Contract files stored on server filesystem
- **Multer**: Multipart form data handling for file uploads
- **File Validation**: Type checking, size limits, and security scanning
- **Structured Storage**: Organized file paths with metadata tracking

## AI Integration Services
- **Groq API**: LLaMA model integration for contract analysis and NLP tasks
- **OCR Pipeline**: Planned Tesseract integration for scanned document processing
- **Contract Analysis**: Automated extraction of key terms, risk assessment, and insights generation
- **Confidence Scoring**: AI model confidence levels for extracted information

## Security & Access Control
- **Role-Based Access Control (RBAC)**: Five-tier permission system (owner, admin, editor, viewer, auditor)
- **Session Management**: Secure session storage in PostgreSQL
- **Audit Logging**: Comprehensive activity tracking for compliance
- **Input Validation**: Zod schema validation across all data inputs

## Data Models
Core entities include Users, Contracts, Contract Analysis, and Audit Trails with full relational mapping and type safety through Drizzle schemas.

# External Dependencies

## Authentication Services
- **Replit OAuth**: Primary authentication provider with OpenID Connect
- **Session Storage**: PostgreSQL-backed session management with connect-pg-simple

## AI & ML Services
- **Groq API**: LLaMA model access for natural language processing and contract analysis
- **Planned Integrations**: Tesseract OCR for document processing, sentence-transformers for embeddings

## Database Services
- **Neon Database**: Serverless PostgreSQL with pgvector extension support for semantic search
- **Connection Pooling**: @neondatabase/serverless for efficient database connections

## UI Component Libraries
- **Radix UI**: Accessible, unstyled component primitives
- **Lucide Icons**: Consistent icon system
- **TailwindCSS**: Utility-first styling framework

## Development Tools
- **TypeScript**: Static type checking across the entire stack
- **Vite**: Modern build tooling with development server
- **ESBuild**: Fast production bundling for server code