# LicenseIQ Research Platform - Session State

## CURRENT TASK (IN PROGRESS)
**Add "Request Early Access" functionality to the landing page chatbot**
- Make it interactive like the existing early access form on the landing page
- Look at `client/src/pages/landing.tsx` for the existing early access submission logic
- The endpoint is likely `/api/early-access` - check the routes
- Add an email input form within the chatbot component
- The chatbot component is at `client/src/components/landing-chatbot.tsx`

## Completed Work (December 2024)

### Landing Page Chatbot Implementation
- Created `/api/landing-chat` endpoint in `server/routes.ts` (lines 139-277)
- No authentication required - public endpoint for visitors
- Uses Groq API with llama-3.3-70b-versatile model
- Includes LicenseIQ knowledge base in system prompt
- Created `client/src/components/landing-chatbot.tsx` - floating chat component
- Added chatbot to `client/src/pages/landing.tsx`
- Branding: "liQ Assistant" with "Your AI Contract Expert" subtitle, "Powered By LicenseIQ" footer

### Windows Deployment Guide
- Created `docs/windows-deployment-guide.html` - comprehensive deployment guide
- Covers PostgreSQL setup, Node.js, environment configuration
- Instructions to copy API keys from Replit Secrets

### Database Backup
- Latest backup: `database_backups/licenseiq_backup_20251216_162224.sql` (3.9 MB)

### Master Data Items Page
- Created `client/src/pages/master-data-items.tsx` for viewing imported ERP data
- Navigation entry added under Data Management category
- Route: `/master-data/items`

## Key Files for Current Task
- `client/src/components/landing-chatbot.tsx` - Landing page AI chatbot (MODIFY THIS)
- `client/src/pages/landing.tsx` - Has early access form logic (REFERENCE THIS)
- `server/routes.ts` - API endpoints including early access signup

## Active Configuration
- Workflow: "Start application" running `npm run dev` on port 5000
- Database: PostgreSQL with pgvector extension
- AI: Groq API (GROQ_API_KEY), HuggingFace embeddings (HUGGINGFACE_API_KEY)

## Important Notes
- Use "License Fee" not "Royalty" throughout the application
- Use "liQ AI" for the RAG assistant (lowercase 'l', lowercase 'i', uppercase 'Q')
- Date format: USA (MM/DD/YYYY)
- Multi-tenant: Companies → Business Units → Locations hierarchy
