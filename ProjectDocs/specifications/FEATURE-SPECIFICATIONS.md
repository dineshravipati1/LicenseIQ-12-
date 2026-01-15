# Feature Specifications

**Version:** 1.0.0  
**Date:** October 23, 2025  
**Status:** Production Ready

---

## Table of Contents
1. [AI Contract Reading](#1-ai-contract-reading)
2. [AI Sales Matching](#2-ai-sales-matching)
3. [Royalty Calculator](#3-royalty-calculator)
4. [PDF Invoice Generation](#4-pdf-invoice-generation)
5. [Contract Q&A Chat](#5-contract-qa-chat)
6. [Rules Management](#6-rules-management)
7. [Risk Assessment](#7-risk-assessment)
8. [Analytics Dashboard](#8-analytics-dashboard)
9. [User Management](#9-user-management)
10. [Audit Trail](#10-audit-trail)

---

## 1. AI Contract Reading

### 1.1 Feature Overview
Automatically extract and analyze key information from uploaded contract documents using AI, eliminating manual review.

### 1.2 User Stories
- **As a** contract manager, **I want to** upload a PDF contract and get instant analysis, **so that** I don't spend hours reading legal documents
- **As a** compliance officer, **I want to** see identified risks automatically, **so that** I can address issues before they become problems

### 1.3 Acceptance Criteria
- ✅ Accept PDF, DOCX, TXT files up to 10 MB
- ✅ Extract text with 95%+ accuracy
- ✅ Identify parties, dates, rates, territories
- ✅ Generate summary in <30 seconds
- ✅ Provide confidence scores for extracted data
- ✅ Display source attribution (page numbers, sections)

### 1.4 Technical Implementation
```
User uploads PDF → Multer saves file → pdf-parse extracts text
→ Groq AI analyzes → Returns JSON (summary, keyTerms, risks)
→ Save to contract_analysis table → Display in UI
```

### 1.5 UI/UX Requirements
- **Upload Interface:**
  - Drag-and-drop zone
  - File type indicator
  - Upload progress bar
  - Success/error messaging

- **Analysis Display:**
  - Contract summary (3-5 sentences)
  - Key terms in structured format
  - Risk indicators with severity levels
  - Confidence scores per field

### 1.6 Success Metrics
- **Time Savings:** 95% reduction (30 min → 1.5 min)
- **Accuracy:** 95%+ correct extraction
- **User Satisfaction:** 4.5/5 rating
- **Adoption:** 100% of users upload contracts

---

## 2. AI Sales Matching

### 2.1 Feature Overview
Automatically match imported sales transactions to the correct contracts using semantic search and AI validation.

### 2.2 User Stories
- **As a** finance analyst, **I want** sales data automatically matched to contracts, **so that** I don't manually review thousands of transactions
- **As a** royalty manager, **I want** low-confidence matches flagged, **so that** I can review edge cases

### 2.3 Acceptance Criteria
- ✅ Import CSV/Excel files with 100K+ rows
- ✅ Generate embeddings for each sales record
- ✅ Match against contract embeddings using vector search
- ✅ Validate matches with AI confidence scoring
- ✅ Auto-assign high-confidence (>0.8) matches
- ✅ Flag low-confidence (<0.5) for review
- ✅ Process 10,000 sales records in <5 minutes

### 2.4 Matching Algorithm
```
1. Parse CSV/Excel → Extract product codes, descriptions
2. Generate Hugging Face embeddings (384-dim)
3. pgvector similarity search → Top 3 contracts per sale
4. Groq LLM validation → Confidence score per match
5. IF confidence > 0.8: Auto-assign
   ELIF confidence 0.5-0.8: Suggest with flag
   ELSE: Flag for manual review
```

### 2.5 UI/UX Requirements
- **Import Interface:**
  - CSV/Excel file uploader
  - Field mapping (auto-detect + manual override)
  - Data preview before import
  - Validation error reporting

- **Matching Results:**
  - Match confidence badges (High/Medium/Low)
  - Side-by-side contract comparison
  - Bulk approve/reject actions
  - Review queue for low-confidence matches

### 2.6 Success Metrics
- **Automation Rate:** 80%+ auto-matched
- **Accuracy:** 95%+ correct matches
- **Time Savings:** 90% reduction vs manual
- **Error Rate:** <2% incorrect assignments

---

## 3. Royalty Calculator

### 3.1 Feature Overview
Calculate royalty payments by interpreting complex formulas (volume tiers, seasonal adjustments, multi-party splits) from contract rules.

### 3.2 User Stories
- **As a** royalty accountant, **I want** automatic calculations with breakdowns, **so that** I eliminate Excel errors
- **As a** CFO, **I want** confidence that payments are accurate, **so that** we avoid disputes and legal issues

### 3.3 Acceptance Criteria
- ✅ Interpret FormulaNode expression trees
- ✅ Apply volume tier logic (e.g., 0-1K: 5%, 1K+: 7%)
- ✅ Apply seasonal adjustments (e.g., Q4: 1.2x multiplier)
- ✅ Handle multi-party splits (e.g., 60/40 revenue share)
- ✅ Enforce minimums and caps
- ✅ Generate detailed calculation breakdown
- ✅ Calculate for date ranges (e.g., Q1 2025)

### 3.4 Formula Interpreter
**FormulaNode Types:**
- **Literal:** Fixed values (e.g., 0.05 for 5%)
- **Variable:** References to sales data (e.g., `totalAmount`)
- **Operator:** Math operations (+, -, *, /)
- **Function:** Aggregations (sum, min, max, if)
- **VolumeTier:** Tiered rates based on quantity
- **Seasonal:** Date-based multipliers

**Example FormulaNode:**
```json
{
  "type": "volumeTier",
  "tiers": [
    { "min": 0, "max": 1000, "rate": 0.05 },
    { "min": 1001, "max": 5000, "rate": 0.07 },
    { "min": 5001, "max": null, "rate": 0.10 }
  ],
  "variable": "quantity"
}
```

### 3.5 UI/UX Requirements
- **Calculation Interface:**
  - Contract selector
  - Date range picker (start/end)
  - "Calculate" button with loading state
  - Results display with breakdown

- **Results Display:**
  - Total royalty amount (large, prominent)
  - Per-rule breakdown
  - Per-tier breakdown (volume tiers)
  - Seasonal adjustment indicators
  - Sales records included count
  - Download PDF invoice button

### 3.6 Success Metrics
- **Accuracy:** 100% (zero calculation errors)
- **Speed:** <2 seconds for 10K sales records
- **Adoption:** 90%+ of customers use feature monthly
- **Audit Pass Rate:** 100% (no disputes)

---

## 4. PDF Invoice Generation

### 4.1 Feature Overview
Generate professional, branded PDF invoices with detailed calculation breakdowns for royalty payments.

### 4.2 User Stories
- **As a** finance manager, **I want** professional invoices for vendors, **so that** payments are documented properly
- **As an** auditor, **I want** detailed breakdowns in invoices, **so that** I can verify calculations

### 4.3 Acceptance Criteria
- ✅ Generate detailed invoice (line-by-line breakdown)
- ✅ Generate summary invoice (totals only)
- ✅ Include company branding (logo, colors)
- ✅ Show contract details (number, parties, dates)
- ✅ List all calculation rules applied
- ✅ Display tier breakdowns
- ✅ Include payment terms
- ✅ Generate in <3 seconds

### 4.4 Invoice Template Structure
**Header:**
- Company logo
- Invoice number (INV-YYYY-MM-NNN)
- Invoice date
- Contract number

**Contract Details:**
- Parties
- Contract period
- Calculation period

**Calculation Breakdown:**
- Rule-by-rule totals
- Volume tier breakdowns
- Seasonal adjustments
- Grand total

**Footer:**
- Payment terms
- Bank details
- Contact information

### 4.5 UI/UX Requirements
- **Generation Interface:**
  - "Download PDF" button on calculation results
  - Format selector (Detailed / Summary)
  - Loading indicator during generation
  - Success notification with download link

- **PDF Features:**
  - Professional typography
  - Responsive layout (A4 page size)
  - Page numbers
  - Watermark (optional: "DRAFT" for previews)

### 4.6 Success Metrics
- **Usage:** 80%+ of calculations generate invoices
- **Quality:** 4.5/5 satisfaction with invoice format
- **Speed:** <3 seconds generation time
- **Accuracy:** 100% match with calculation results

---

## 5. Contract Q&A Chat

### 5.1 Feature Overview
Ask natural language questions about contracts and receive AI-powered answers with source citations using RAG (Retrieval-Augmented Generation).

### 5.2 User Stories
- **As a** sales manager, **I want** to ask questions about contract terms, **so that** I don't read entire documents
- **As a** legal assistant, **I want** answers with citations, **so that** I can verify information

### 5.3 Acceptance Criteria
- ✅ Accept natural language questions
- ✅ Search contract embeddings for relevant context
- ✅ Generate answers using Groq LLM
- ✅ Provide source citations (contract sections)
- ✅ Display confidence scores
- ✅ Fallback to full context if low similarity
- ✅ Response time <5 seconds

### 5.4 RAG Pipeline
```
User question → Generate question embedding (Hugging Face)
→ pgvector similarity search (top 5 chunks)
→ IF similarity > 0.7:
    → Groq LLM (chunks + question) → Answer
  ELSE:
    → Fetch full contract analysis
    → Groq LLM (full context + question) → Answer
→ Return answer + citations + confidence
```

### 5.5 UI/UX Requirements
- **Chat Interface:**
  - Input field with placeholder examples
  - Send button + Enter key support
  - Message bubbles (user vs AI)
  - Typing indicator during processing
  - Citations as clickable links
  - Confidence badge (High/Medium/Low)

- **Question Suggestions:**
  - "What are the payment terms?"
  - "When does this contract expire?"
  - "What are the royalty rates?"
  - "Who are the parties involved?"

### 5.6 Success Metrics
- **Usage:** 50%+ of users ask questions weekly
- **Accuracy:** 90%+ of answers rated helpful
- **Speed:** <5 seconds average response
- **Confidence:** 80%+ high-confidence answers

---

## 6. Rules Management

### 6.1 Feature Overview
View, edit, and create royalty calculation rules with source attribution to contract sections.

### 6.2 User Stories
- **As a** royalty analyst, **I want** to view AI-extracted rules, **so that** I verify accuracy
- **As a** contract admin, **I want** to edit rules when terms change, **so that** calculations stay accurate

### 6.3 Acceptance Criteria
- ✅ List all rules per contract
- ✅ Display FormulaNode as human-readable text
- ✅ Show source attribution (contract section)
- ✅ Edit existing rules (formula, tiers, seasonality)
- ✅ Create custom rules manually
- ✅ Delete rules with confirmation
- ✅ Tag rules as AI-generated vs manual

### 6.4 Formula Display Format
**Volume Tiers:**
```
0 - 1,000 units: 5.0%
1,001 - 5,000 units: 7.0%
5,001+ units: 10.0%
```

**Seasonal Adjustments:**
```
Q1 (Jan-Mar): 1.0x
Q2 (Apr-Jun): 1.0x
Q3 (Jul-Sep): 1.1x
Q4 (Oct-Dec): 1.2x (holiday boost)
```

### 6.5 UI/UX Requirements
- **Rules List:**
  - Card layout per rule
  - Rule name + description
  - Formula preview
  - Source attribution link
  - Edit/Delete buttons

- **Edit Interface:**
  - Form with formula builder
  - Visual tier editor (add/remove tiers)
  - Seasonal adjustment toggles
  - Preview calculated result
  - Save/Cancel buttons

### 6.6 Success Metrics
- **Visibility:** 100% of rules displayed accurately
- **Edit Rate:** 20% of AI rules edited/verified
- **Custom Rules:** 30% of customers create manual rules
- **Accuracy:** 100% formula interpretation correctness

---

## 7. Risk Assessment

### 7.1 Feature Overview
Automatically identify compliance issues, missing clauses, and legal risks in uploaded contracts.

### 7.2 User Stories
- **As a** compliance officer, **I want** automatic risk detection, **so that** I address issues proactively
- **As a** legal counsel, **I want** missing clause alerts, **so that** contracts are complete

### 7.3 Acceptance Criteria
- ✅ Detect missing critical clauses (termination, liability, IP)
- ✅ Flag compliance risks (GDPR, SOX, industry regulations)
- ✅ Identify anomalies (unusual rates, missing dates)
- ✅ Categorize by severity (Critical, High, Medium, Low)
- ✅ Provide remediation suggestions
- ✅ Track risk resolution status

### 7.4 Risk Categories
**Critical:**
- Missing termination clause
- No liability limitations
- Undefined IP ownership
- Regulatory non-compliance

**High:**
- Unusual royalty rates (outliers)
- Missing payment terms
- Ambiguous language
- Expiring within 30 days

**Medium:**
- Incomplete contact information
- Missing renewal terms
- No dispute resolution clause

**Low:**
- Minor formatting issues
- Suggested improvements

### 7.5 UI/UX Requirements
- **Risk Dashboard:**
  - Total risk count by severity
  - Risk list with filters (severity, category)
  - Risk detail cards with descriptions
  - "Mark Resolved" action
  - Risk trends over time

- **Risk Detail:**
  - Severity badge
  - Description
  - Contract section reference
  - Remediation suggestion
  - Status (Open/Resolved)

### 7.6 Success Metrics
- **Detection Rate:** 95%+ of actual risks identified
- **False Positives:** <10%
- **Resolution Time:** 50% reduction vs manual
- **Compliance:** 100% critical risks addressed

---

## 8. Analytics Dashboard

### 8.1 Feature Overview
Visualize financial performance, compliance scores, and strategic insights across all contracts.

### 8.2 User Stories
- **As a** CFO, **I want** financial metrics at a glance, **so that** I track royalty revenue
- **As a** business analyst, **I want** trend analysis, **so that** I identify growth opportunities

### 8.3 Acceptance Criteria
- ✅ Display total royalty revenue (YTD, QoQ, MoM)
- ✅ Show contract count and status distribution
- ✅ Visualize top revenue-generating contracts
- ✅ Track compliance scores
- ✅ Display payment accuracy metrics
- ✅ Show AI matching performance
- ✅ Interactive charts (filter by date, contract, territory)

### 8.4 Key Metrics
**Financial:**
- Total royalty revenue (current period)
- Revenue by contract
- Revenue by territory
- Revenue trends (line chart)

**Operational:**
- Contracts analyzed
- Sales records matched
- Calculations performed
- Invoices generated

**Compliance:**
- Contracts with risks
- Audit log activity
- Payment accuracy rate

**AI Performance:**
- AI matching accuracy
- High-confidence match rate
- Average confidence score

### 8.5 UI/UX Requirements
- **Dashboard Layout:**
  - Metric cards (4 key stats at top)
  - Revenue trend chart (line/area)
  - Top contracts table
  - Compliance score gauge
  - AI performance indicators

- **Interactivity:**
  - Date range selector
  - Contract filter
  - Territory filter
  - Export to CSV/Excel
  - Drill-down into details

### 8.6 Success Metrics
- **Usage:** 80%+ of users view dashboard weekly
- **Insight Value:** 4.5/5 usefulness rating
- **Decision Impact:** 60% of users report data-driven decisions
- **Performance:** <1 second chart load time

---

## 9. User Management

### 9.1 Feature Overview
5-tier Role-Based Access Control (RBAC) system for secure multi-user collaboration.

### 9.2 User Stories
- **As an** admin, **I want** to invite team members with specific roles, **so that** access is controlled
- **As an** owner, **I want** to revoke access instantly, **so that** security is maintained

### 9.3 Acceptance Criteria
- ✅ Support 5 roles: Owner, Admin, Editor, Viewer, Auditor
- ✅ Invite users via email
- ✅ Set role permissions granularly
- ✅ Deactivate users without deleting data
- ✅ Track user activity (last login, actions)
- ✅ Password reset functionality

### 9.4 Role Permissions Matrix

| Permission | Owner | Admin | Editor | Viewer | Auditor |
|------------|-------|-------|--------|--------|---------|
| **Manage Users** | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Upload Contracts** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Edit Rules** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Run Calculations** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **View Data** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **View Audit Logs** | ✅ | ✅ | ❌ | ❌ | ✅ |
| **Delete Data** | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Billing Management** | ✅ | ❌ | ❌ | ❌ | ❌ |

### 9.5 UI/UX Requirements
- **User List:**
  - Table with name, email, role, last login
  - Search and filter
  - Invite user button
  - Edit/Deactivate actions

- **Invite Flow:**
  - Email input
  - Role selector
  - Optional custom message
  - Send invitation
  - Track pending invitations

### 9.6 Success Metrics
- **Adoption:** 70% of customers add 2+ users
- **Security:** Zero unauthorized access incidents
- **Satisfaction:** 4.5/5 ease of user management
- **Activity:** 80%+ invited users activate accounts

---

## 10. Audit Trail

### 10.1 Feature Overview
Complete SOX-compliant activity logging for all user actions and system changes.

### 10.2 User Stories
- **As an** auditor, **I want** complete activity logs, **so that** I verify compliance
- **As an** owner, **I want** to see who changed what, **so that** I track accountability

### 10.3 Acceptance Criteria
- ✅ Log all CRUD operations (create, read, update, delete)
- ✅ Capture user, timestamp, IP address, user agent
- ✅ Record before/after state for changes
- ✅ Permanent retention (never delete)
- ✅ Export audit logs (CSV, PDF)
- ✅ Search and filter logs
- ✅ Real-time activity feed

### 10.4 Logged Events
**Authentication:**
- Login attempts (success/failure)
- Logout
- Password changes
- Session expiry

**Contract Management:**
- Contract upload
- Contract delete
- Analysis run

**Calculations:**
- Royalty calculation performed
- Invoice generated
- Rule created/edited/deleted

**User Management:**
- User invited
- User role changed
- User deactivated

### 10.5 UI/UX Requirements
- **Audit Log Viewer:**
  - Chronological list (newest first)
  - Filters (date, user, action, entity)
  - Search by entity ID
  - Detail modal for each entry
  - Export button

- **Log Entry Display:**
  - Timestamp
  - User (with role badge)
  - Action description
  - Entity type and ID
  - Changes (diff view for before/after)
  - IP address and user agent

### 10.6 Success Metrics
- **Coverage:** 100% of actions logged
- **Retention:** Permanent (zero data loss)
- **Compliance:** 100% audit pass rate
- **Accessibility:** <1 second log query time

---

## Feature Priority Matrix

| Feature | Priority | Impact | Complexity | MVP |
|---------|----------|--------|------------|-----|
| AI Contract Reading | P0 | High | High | ✅ |
| AI Sales Matching | P0 | High | High | ✅ |
| Royalty Calculator | P0 | High | High | ✅ |
| PDF Invoices | P1 | Medium | Low | ✅ |
| Contract Q&A | P1 | High | Medium | ✅ |
| Rules Management | P1 | Medium | Medium | ✅ |
| Risk Assessment | P2 | Medium | Medium | ✅ |
| Analytics Dashboard | P2 | Medium | Low | ✅ |
| User Management | P1 | High | Low | ✅ |
| Audit Trail | P1 | High | Low | ✅ |

---

**All features are production-ready and included in v1.0 MVP.**
