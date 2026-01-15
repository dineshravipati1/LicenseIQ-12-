# LicenseIQ - AI-Powered Contract Management Platform

LicenseIQ is a comprehensive SaaS platform for intelligent contract management and analysis. It uses Groq's LLaMA AI models to analyze legal contracts, extract royalty calculation rules, assess risks, and provide actionable insights for enterprise contract management.

## 🚀 Features

- **AI-Powered Contract Analysis**: Automatic extraction of key terms, obligations, and royalty rules using Groq AI
- **Royalty Rules Engine**: Sophisticated rules extraction and calculation for licensing agreements
- **Risk Assessment**: Automated identification of potential legal and financial risks
- **Role-Based Access Control**: Five-tier permission system (Owner, Admin, Editor, Viewer, Auditor)
- **Real-time Processing**: Live contract analysis with status updates and progress tracking
- **Comprehensive Analytics**: Dashboard with insights into contract portfolio performance
- **Audit Trail**: Complete activity logging for compliance and security

## 📋 Table of Contents

- [Quick Start](#quick-start)
- [Database Setup & Restoration](#database-setup--restoration)
- [Environment Configuration](#environment-configuration)
- [Development](#development)
- [Database Backup & Restore](#database-backup--restore)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Deployment](#deployment)

## ⚡ Quick Start

### 1. Import Project to Replit

1. **Fork or Import from GitHub**:
   - Go to [Replit](https://replit.com)
   - Click "Create Repl" → "Import from GitHub"
   - Enter your repository URL
   - Click "Import from GitHub"

2. **Wait for Initial Setup**:
   - Replit will automatically detect the Node.js project
   - Dependencies will be installed automatically
   - This may take 2-3 minutes

### 2. Database Setup

The project uses PostgreSQL database. Replit provides a managed PostgreSQL database:

#### Option A: Fresh Database (New Installation)

```bash
# Run database migrations to create schema
npm run db:push
```

#### Option B: Restore from Backup (Recommended for GitHub Import)

```bash
# Restore complete database with schema and sample data
npm run db:restore:full

# Or use the interactive restore script
bash database/restore.sh
```

**The backup includes**:
- Complete database schema (all tables, indexes, constraints)
- Sample user accounts (test credentials below)
- Sample contracts and analysis data
- Royalty rules and calculation examples

### 3. Configure Environment Secrets

Set up the required API key in Replit:

1. Click on "Tools" → "Secrets" in Replit sidebar
2. Add the following secret:
   - Key: `GROQ_API_KEY`
   - Value: Your Groq API key from [https://console.groq.com](https://console.groq.com)

**Note**: `DATABASE_URL` is automatically provided by Replit's PostgreSQL database.

### 4. Start the Application

```bash
npm run dev
```

The application will be available at the Replit webview URL (typically `https://<your-repl-name>.<your-username>.repl.co`)

### 5. Login with Test Credentials

After restoring the database, you can login with these test accounts:

- **Admin Account**:
  - Username: `admin`
  - Password: `admin123`

- **Regular User**:
  - Username: `user`
  - Password: `user123`

## 🗄️ Database Setup & Restoration

### Understanding the Database Files

The project includes pre-generated database backups in `database/backups/`:

- **`full_backup.sql`**: Complete database dump (schema + data) - Use this for complete restoration
- **`schema_backup.sql`**: Database structure only (tables, indexes, constraints)
- **`data_backup.sql`**: Data only (requires existing schema)

### Restoring Database on New Replit Account

When you import this project from GitHub to a new Replit account, follow these steps:

#### Step 1: Ensure PostgreSQL Database is Created

Replit automatically creates a PostgreSQL database when you:
1. Import the project
2. Or manually add it from Tools → Database

Verify the database is ready:
```bash
echo $DATABASE_URL
```
You should see a PostgreSQL connection string.

#### Step 2: Choose Restoration Method

**Method 1: Automatic Full Restore (Recommended)**

This restores both schema and all sample data:

```bash
# Using npm script (add to package.json if needed)
npm run db:restore:full

# Or directly with psql
psql $DATABASE_URL < database/backups/full_backup.sql
```

**Method 2: Interactive Restore**

For more control over what to restore:

```bash
bash database/restore.sh
```

This will prompt you to choose:
1. Full backup (schema + data)
2. Schema only
3. Data only

**Method 3: Manual Step-by-Step Restore**

```bash
# First, restore the schema
psql $DATABASE_URL < database/backups/schema_backup.sql

# Then, restore the data
psql $DATABASE_URL < database/backups/data_backup.sql
```

#### Step 3: Verify Restoration

Check if tables were created:
```bash
psql $DATABASE_URL -c "\dt"
```

Check if data was imported:
```bash
psql $DATABASE_URL -c "SELECT COUNT(*) FROM users;"
```

### Database Schema Overview

The database includes these main tables:

- **users**: User accounts and authentication
- **contracts**: Contract metadata and files
- **contract_analysis**: AI analysis results
- **audit_trail**: Activity logging
- **vendors**: Vendor/partner information
- **license_documents**: License file management
- **license_rule_sets**: Extracted royalty rules
- **sales_data**: Transaction data for royalty calculations
- **royalty_runs**: Royalty calculation history

## 🔧 Database Backup & Restore

### Creating a New Backup

To create a fresh backup of your current database:

```bash
# Create all backup files (full, schema, data)
npm run db:backup

# Or use the script directly
bash database/backup.sh
```

This creates:
- `database/backups/full_backup.sql` - Latest full backup
- `database/backups/schema_backup.sql` - Latest schema
- `database/backups/data_backup.sql` - Latest data
- `database/backups/full_backup_YYYYMMDD_HHMMSS.sql` - Timestamped backup

### Backup Before Major Changes

**Always create a backup before**:
- Modifying the database schema
- Running data migrations
- Deleting records
- Major updates

```bash
npm run db:backup
```

### Restore from Backup

**Full Restore** (wipes and restores everything):
```bash
npm run db:restore:full
```

**Schema Only** (structure without data):
```bash
npm run db:restore:schema
```

**Interactive Restore** (choose what to restore):
```bash
bash database/restore.sh
```

## 🔐 Environment Configuration

### Required Environment Variables

The project requires these environment variables:

1. **`DATABASE_URL`** (automatically set by Replit)
   - PostgreSQL connection string
   - Format: `postgresql://user:password@host:port/database`

2. **`GROQ_API_KEY`** (you must provide)
   - Get your API key from [Groq Console](https://console.groq.com)
   - Used for AI contract analysis
   - Set in Replit Secrets: Tools → Secrets

### Optional Environment Variables

- **`SESSION_SECRET`** - Session encryption key (auto-generated if not set)
- **`NODE_ENV`** - Environment mode (`development` or `production`)

## 💻 Development

### Available Scripts

```bash
# Start development server (with hot reload)
npm run dev

# Build for production
npm run build

# Start production server
npm start

# Type check
npm check

# Push database schema changes
npm run db:push

# Create database backup
npm run db:backup

# Restore database from backup
npm run db:restore
```

### Development Workflow

1. **Make changes** to your code
2. **Test locally** with `npm run dev`
3. **Create backup** before schema changes: `npm run db:backup`
4. **Push schema changes**: `npm run db:push`
5. **Commit to git** with updated backups

### Adding New Database Tables

1. Edit `shared/schema.ts` to add your table definition
2. Create a backup of current database: `npm run db:backup`
3. Push schema changes: `npm run db:push`
4. Update storage interface in `server/storage.ts`
5. Create new backup with updated schema: `npm run db:backup`

## 📁 Project Structure

```
├── client/                 # Frontend React application
│   ├── src/
│   │   ├── components/    # Reusable UI components
│   │   ├── pages/        # Page components
│   │   ├── hooks/        # Custom React hooks
│   │   └── lib/          # Utilities and helpers
│   └── index.html        # HTML entry point
├── server/                # Backend Express application
│   ├── routes.ts         # API routes
│   ├── storage.ts        # Database abstraction layer
│   ├── index.ts          # Server entry point
│   └── services/         # Business logic services
├── shared/                # Shared code between client/server
│   └── schema.ts         # Database schema (Drizzle ORM)
├── database/              # Database related files
│   ├── backups/          # Database backup files
│   │   ├── full_backup.sql
│   │   ├── schema_backup.sql
│   │   └── data_backup.sql
│   ├── backup.sh         # Backup script
│   └── restore.sh        # Restore script
└── uploads/              # Contract file uploads (not in git)
```

## 🛠️ Tech Stack

### Frontend
- **React 18** - UI framework
- **TypeScript** - Type safety
- **TailwindCSS** - Styling
- **shadcn/ui** - Component library
- **TanStack Query** - Data fetching and caching
- **Wouter** - Client-side routing
- **Vite** - Build tool

### Backend
- **Node.js + Express** - Server framework
- **TypeScript** - Type safety
- **Drizzle ORM** - Database ORM
- **PostgreSQL (Neon)** - Database
- **Passport.js** - Authentication
- **Multer** - File upload handling

### AI/ML
- **Groq API** - LLaMA 3.1 8B model for contract analysis
- **Custom extraction pipeline** - Multi-stage royalty rule extraction

### Infrastructure
- **Replit** - Development and hosting platform
- **Neon Database** - Serverless PostgreSQL

## 🚀 Deployment

### Deploying on Replit

1. **Ensure all secrets are set** in Tools → Secrets
2. **Create a fresh backup**: `npm run db:backup`
3. **Commit backup files to git**
4. **Click "Deploy"** in Replit
5. **Configure custom domain** (optional) in deployment settings

### Database Considerations for Deployment

- Development and production databases are separate in Replit
- Always backup before deploying schema changes
- Test schema migrations in development first
- Use `npm run db:push` to sync schema to production database

## 📚 Additional Resources

### Documentation Files

- **`Agile_Documentation.md`** - Detailed technical documentation (80+ pages)
- **`LicenseIQ_Management_Summary.md`** - Executive summary (6 pages)
- **`replit.md`** - Project context and architecture notes

### Support

For issues or questions:
1. Check the documentation files
2. Review the code comments
3. Check Replit community forums
4. Review Groq API documentation

## 📝 License

MIT License - See LICENSE file for details

## 🙏 Acknowledgments

- Groq for AI model API
- Replit for development platform
- shadcn/ui for component library
- Neon for PostgreSQL hosting

---

**Built with ❤️ using Replit, TypeScript, and AI**
