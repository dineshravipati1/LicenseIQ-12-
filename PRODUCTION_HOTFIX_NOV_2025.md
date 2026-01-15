# Production Hotfix - November 2025

## Overview
This hotfix addresses two critical production issues at licenseiq.ai:
1. **Date Parsing Error** - Causes version approval to fail with "value.toISOString is not a function"
2. **HuggingFace API Error 410** - RAG/Q&A system fails with deprecated API endpoint

**IMPORTANT**: This hotfix does **NOT** include the organizationName feature currently in development.

---

## Issue #1: Date Parsing Error in Version Approval

### Symptoms
- Error when approving contract metadata versions
- Error message: `"value.toISOString is not a function"`
- Version approval workflow breaks

### Root Cause
When version metadata snapshots are stored in JSONB and then retrieved to apply to contracts during approval, the date fields come back as Date objects. The code attempts to apply these directly to the contracts table, but Drizzle expects properly formatted dates.

### Fix Location
**File**: `server/storage.ts`  
**Function**: `createContractApproval()`  
**Line**: ~670-690

### Code Changes

**Add this helper function** inside the `createContractApproval()` method, before the contracts update:

```typescript
// Helper to safely convert date values from JSONB
const parseSnapshotDate = (value: any): Date | null | undefined => {
  if (!value) return value === null ? null : undefined;
  if (value instanceof Date) return value;
  if (typeof value === 'string') {
    try {
      const parsed = new Date(value);
      return isNaN(parsed.getTime()) ? null : parsed;
    } catch {
      return null;
    }
  }
  return null;
};
```

**Update the date fields** in the `db.update(contracts).set()` call:

```typescript
// OLD CODE:
effectiveStart: snapshot.effectiveStart,
effectiveEnd: snapshot.effectiveEnd,

// NEW CODE:
effectiveStart: parseSnapshotDate(snapshot.effectiveStart),
effectiveEnd: parseSnapshotDate(snapshot.effectiveEnd),
```

### Complete Context (Lines 654-710 in server/storage.ts)

```typescript
async createContractApproval(approval: any): Promise<any> {
  const [newApproval] = await db.insert(contractApprovals).values(approval).returning();
  
  // Update the version approval state
  await db
    .update(contractVersions)
    .set({ approvalState: approval.status })
    .where(eq(contractVersions.id, approval.contractVersionId));

  // If approved, update the contract with the approved version's metadata
  if (approval.status === 'approved') {
    const [version] = await db
      .select()
      .from(contractVersions)
      .where(eq(contractVersions.id, approval.contractVersionId));
    
    if (version && version.metadataSnapshot) {
      const snapshot: any = version.metadataSnapshot;
      
      // Helper to safely convert date values from JSONB
      const parseSnapshotDate = (value: any): Date | null | undefined => {
        if (!value) return value === null ? null : undefined;
        if (value instanceof Date) return value;
        if (typeof value === 'string') {
          try {
            const parsed = new Date(value);
            return isNaN(parsed.getTime()) ? null : parsed;
          } catch {
            return null;
          }
        }
        return null;
      };
      
      await db
        .update(contracts)
        .set({ 
          approvalState: 'approved',
          displayName: snapshot.displayName,
          effectiveStart: parseSnapshotDate(snapshot.effectiveStart),
          effectiveEnd: parseSnapshotDate(snapshot.effectiveEnd),
          renewalTerms: snapshot.renewalTerms,
          governingLaw: snapshot.governingLaw,
          counterpartyName: snapshot.counterpartyName,
          contractOwnerId: snapshot.contractOwnerId,
          contractType: snapshot.contractType,
          priority: snapshot.priority,
          notes: snapshot.notes,
          currentVersion: version.versionNumber,
        })
        .where(eq(contracts.id, version.contractId));
    }
  }

  return newApproval;
}
```

**NOTE**: DO NOT include `organizationName: snapshot.organizationName` in the update - this field doesn't exist in production yet.

---

## Issue #2: HuggingFace API Error 410

### Symptoms
- Contract Q&A system shows error 410
- RAG/embedding generation fails during contract upload
- Embeddings not created, causing Q&A to fail
- Error message: "HuggingFace API error: 410 - api-inference.huggingface.co is no longer supported"

### Root Cause
HuggingFace deprecated the `api-inference.huggingface.co` endpoint. The error message explicitly says to use `router.huggingface.co/hf-inference` instead.

### Fix Location
**File**: `server/services/huggingFaceEmbedding.ts`  
**Line**: ~18

### Code Changes

**Change the API_URL constant:**

```typescript
// OLD CODE (WRONG):
private static readonly API_URL = 'https://api-inference.huggingface.co/models/BAAI/bge-small-en-v1.5';

// NEW CODE (CORRECT):
private static readonly API_URL = 'https://router.huggingface.co/hf-inference/models/BAAI/bge-small-en-v1.5';
```

### Complete Context (Lines 15-19 in server/services/huggingFaceEmbedding.ts)

```typescript
export class HuggingFaceEmbeddingService {
  // Using BAAI/bge-small-en-v1.5 which is optimized for embedding generation (384 dimensions)
  // Updated to HuggingFace router endpoint per API deprecation notice (November 2025)
  private static readonly API_URL = 'https://router.huggingface.co/hf-inference/models/BAAI/bge-small-en-v1.5';
  private static readonly MODEL_DIMENSIONS = 384;
```

---

## Deployment Instructions

### Option 1: Manual Patch (Fastest)

1. **Access your production server** at licenseiq.ai
2. **Edit `server/storage.ts`**:
   - Add the `parseSnapshotDate` helper function
   - Update the two date field assignments
   - **Do NOT add** the organizationName line
3. **Edit `server/services/huggingFaceEmbedding.ts`**:
   - Change the API_URL to use `api-inference.huggingface.co`
4. **Restart the Node.js server**
5. **Test**:
   - Try approving a pending contract version
   - Try asking a question in the Contract Q&A system

### Option 2: Git Hotfix Branch

1. **Create hotfix branch** from your last production deploy:
   ```bash
   git checkout -b hotfix/date-parsing-and-hf-api
   ```

2. **Apply the two fixes** as described above

3. **Commit**:
   ```bash
   git add server/storage.ts server/services/huggingFaceEmbedding.ts
   git commit -m "Hotfix: Fix date parsing in approval workflow and update HuggingFace API endpoint"
   ```

4. **Deploy** the hotfix branch to licenseiq.ai

5. **Merge back** to main:
   ```bash
   git checkout main
   git merge hotfix/date-parsing-and-hf-api
   ```

---

## Verification Steps

### Test Date Parsing Fix
1. Go to a contract with pending metadata versions
2. Click "Force Approve" (admin override)
3. Verify no error appears
4. Check that the contract metadata updates successfully

### Test HuggingFace Fix
1. Go to Contract Q&A page
2. Ask a question: "What are the payment terms?"
3. Verify you get an answer without error 410
4. Check that the AI response loads successfully

---

## What's NOT Included

This hotfix **does NOT include**:
- ❌ `organizationName` field in database schema
- ❌ "Your Organization" input field in UI
- ❌ Any changes to `shared/schema.ts`
- ❌ Any changes to `client/src/pages/contract-management.tsx`

These are part of the "Universal Two-Party System" feature still in development.

---

## Files Changed

- ✅ `server/storage.ts` (date parsing fix only)
- ✅ `server/services/huggingFaceEmbedding.ts` (API endpoint fix)

**Total**: 2 files, ~20 lines changed

---

## Rollback Plan

If issues occur after deployment:

### Rollback Date Fix
Remove the `parseSnapshotDate` helper and revert to direct assignment:
```typescript
effectiveStart: snapshot.effectiveStart,
effectiveEnd: snapshot.effectiveEnd,
```

### Rollback HuggingFace Fix
Revert to old endpoint (not recommended as it's deprecated):
```typescript
private static readonly API_URL = 'https://router.huggingface.co/hf-inference/models/BAAI/bge-small-en-v1.5';
```

---

## Questions?

If you have any questions about this hotfix:
- Check the code comments in the affected files
- Review the error logs for specific error messages
- Test in development environment first if uncertain

---

**Hotfix Created**: November 4, 2025  
**Hotfix Type**: Critical bug fixes  
**Deployment Time**: ~5 minutes  
**Risk Level**: Low (isolated fixes, no schema changes)
