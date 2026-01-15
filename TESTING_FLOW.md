# License IQ - Royalty Calculation Testing Flow

## Overview
The new AI-driven royalty calculation system uses semantic matching to automatically match sales transactions to contracts without requiring manual vendor selection. This document provides a step-by-step testing flow.

## System Architecture (100% FREE AI)
- **Hugging Face Embeddings**: BAAI/bge-small-en-v1.5 model for semantic matching (FREE, 1000 requests/hour)
- **Groq LLaMA**: Contract analysis and validation (FREE, unlimited)
- **PostgreSQL + pgvector**: Vector similarity search for contract matching

## Testing Flow

### 1. Upload & Analyze Contract
1. Navigate to **Upload** page
2. Upload a license/royalty contract (PDF format)
3. System will:
   - Extract text using PDF parser
   - Analyze contract using Groq LLaMA
   - Generate embeddings using Hugging Face
   - Store in `contracts` table with analysis in `contract_analysis`
   - Create vector embeddings in `contract_embeddings` table

**Expected Result**: Contract shows in **Contracts** page with "Analyzed" status

### 2. View Contract Analysis
1. Click on the contract from **Contracts** page
2. Review AI-generated analysis:
   - Summary
   - Key Terms (products, territories, royalty rates)
   - Risk Analysis
   - Confidence Scores

**Expected Result**: Complete analysis with products, territories, and royalty rules identified

### 3. Upload Sales Data (CSV/Excel)
1. Navigate to **Upload** page (or create dedicated sales import page)
2. Upload sales data CSV/Excel with columns:
   - Transaction Date
   - Product Name/Code
   - Territory
   - Sales Amount
   - Quantity (optional)
   - Currency

3. System will:
   - Parse the file
   - Generate embeddings for each sales row
   - Use semantic search to find matching contracts (cosine similarity)
   - Validate matches using Groq LLaMA
   - Auto-assign high-confidence matches (>60%)
   - Flag low-confidence matches for human review

**Expected Result**: Sales data imported and matched to contracts automatically

### 4. Review AI Matches (Human-in-the-Loop)
1. Navigate to **Sales Review** page (to be created)
2. View sales transactions with:
   - High confidence (>60%): Auto-matched, shown in green
   - Low confidence (<60%): Flagged for review, shown in orange
   
3. For flagged items:
   - Review AI reasoning
   - Manually confirm or override match
   - System learns from corrections

**Expected Result**: All sales transactions have confirmed contract matches

### 5. Calculate Royalties for Contract
1. Navigate to **Contracts** page
2. Click on a contract with matched sales
3. Click **"Calculate Royalties"** button
4. Enter calculation details:
   - Name (e.g., "Q1 2024 Royalties")
   - Period (start and end date)
   
5. System will:
   - Query all sales matching this contract in the period
   - Apply royalty rules from contract analysis
   - Calculate per-transaction royalties
   - Generate breakdown and chart data
   - Create calculation in `contract_royalty_calculations` table
   - Set status to "pending_approval"

**Expected Result**: Redirects to Royalty Dashboard with beautiful charts

### 6. Review Royalty Dashboard
1. View royalty calculation dashboard with:
   - **Key Metrics**: Total Royalty, Total Sales, Period, Calculated By
   - **Charts**:
     - Pie Chart: Royalty by Product Category
     - Bar Chart: Top Royalty Transactions
   - **Detailed Breakdown Table**: Per-transaction royalty details
   - **Status Badge**: Shows "PENDING APPROVAL" in orange

**Expected Result**: Beautiful dashboard with all calculated data

### 7. Approve/Reject Calculation (Human-in-the-Loop)
1. Click **"Approve"** or **"Reject"** button
2. For approval:
   - Optionally add comments
   - Confirm approval
3. For rejection:
   - Required to provide rejection reason
   - Confirm rejection

4. System updates:
   - Status changes to "approved" or "rejected"
   - Records approver/rejector user ID
   - Timestamps the decision
   - Stores comments/rejection reason

**Expected Result**: 
- Approved: Status badge turns GREEN
- Rejected: Status badge turns RED
- Action recorded in audit trail

### 8. Export & Payment
1. Click **"Export"** button (to be implemented)
2. Download calculation as:
   - PDF report
   - Excel spreadsheet
   - CSV file

**Expected Result**: Downloadable report for payment processing

## API Endpoints (To Be Implemented)

```
POST /api/contracts/:contractId/calculate-royalties
  Body: { name, periodStart?, periodEnd? }
  Returns: { calculationId }

GET /api/royalty-calculations/:id
  Returns: Complete calculation with charts

POST /api/royalty-calculations/:id/approve
  Body: { comments? }
  Returns: Updated calculation

POST /api/royalty-calculations/:id/reject
  Body: { reason }
  Returns: Updated calculation

POST /api/sales/upload
  Body: FormData with CSV/Excel file
  Returns: { importJobId, matchingResults }

GET /api/sales/flagged
  Returns: Sales transactions flagged for review

POST /api/sales/:id/confirm-match
  Body: { contractId }
  Returns: Updated sales record
```

## Database Schema

### contract_royalty_calculations
- id (primary key)
- contract_id (foreign key to contracts)
- name (e.g., "Q1 2024 Royalties")
- period_start, period_end (optional date range)
- status (pending_approval, approved, rejected, paid)
- total_sales_amount, total_royalty, currency
- sales_count (number of transactions)
- breakdown (JSONB - per-transaction details)
- chart_data (JSONB - pre-computed chart data)
- calculated_by, approved_by, rejected_by (user IDs)
- approved_at, rejected_at (timestamps)
- rejection_reason, comments (text)
- created_at, updated_at

## Current Status

✅ **Completed**:
- Database table created
- Royalty dashboard UI with charts
- Approval/rejection dialogs
- Frontend routing configured

❌ **TODO**:
- Storage methods for royalty calculations
- API endpoints in routes.ts
- "Calculate Royalties" button on contract page
- Sales upload with AI matching flow
- Human review UI for flagged matches

## Quick Test Script

Once fully implemented, run this quick test:

```bash
# 1. Upload contract
curl -X POST -F "file=@sample-contract.pdf" http://localhost:5000/api/contracts

# 2. Upload sales data
curl -X POST -F "file=@sales-q1-2024.csv" http://localhost:5000/api/sales/upload

# 3. Calculate royalties
curl -X POST http://localhost:5000/api/contracts/{contractId}/calculate-royalties \
  -H "Content-Type: application/json" \
  -d '{"name":"Q1 2024 Royalties","periodStart":"2024-01-01","periodEnd":"2024-03-31"}'

# 4. View dashboard
# Open browser to: http://localhost:5000/royalty-calculations/{calculationId}

# 5. Approve calculation
curl -X POST http://localhost:5000/api/royalty-calculations/{calculationId}/approve \
  -H "Content-Type: application/json" \
  -d '{"comments":"Looks good!"}'
```

## Notes

- **100% FREE AI**: Entire pipeline uses free APIs (Hugging Face + Groq)
- **No Vendor Management**: Old vendor-based system removed
- **AI-Driven Matching**: Automatic contract-to-sales matching via embeddings
- **Human-in-the-Loop**: Low-confidence matches flagged for review
- **Beautiful UI**: Charts, graphs, and visual dashboards
- **Approval Workflow**: Built-in approval/rejection with audit trail
