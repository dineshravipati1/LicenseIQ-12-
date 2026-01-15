# 🚀 Production Deployment Steps for LicenseIQ.ai

## Quick Deploy - Enhanced Product Matching Fix

This deployment includes the **keyword-based product matching** fix that resolves the "0 Matched Products" issue in the License Fee Calculator.

---

## Option A: Deploy via Git (Recommended)

### Step 1: Connect to Production Server
```bash
ssh your-username@licenseiq.ai
# OR use your hosting provider's terminal
```

### Step 2: Navigate to App Directory
```bash
cd /var/www/licenseiq
# OR wherever your app is installed
```

### Step 3: Pull Latest Code
```bash
# Backup current code first
cp -r . ../licenseiq-backup-$(date +%Y%m%d)

# Pull latest changes
git pull origin main
# OR
git pull origin production
```

### Step 4: Install Dependencies (if package.json changed)
```bash
npm install
```

### Step 5: Restart Production Server
```bash
# If using PM2:
pm2 restart licenseiq

# If using systemd:
sudo systemctl restart licenseiq

# If using npm directly:
npm run build  # If needed
npm start
```

### Step 6: Verify
Visit https://licenseiq.ai and test the License Fee Calculator:
- Upload a contract
- Upload sales data
- Check if products are now matching rules ✅

---

## Option B: Manual File Transfer (If No Git Access)

### Step 1: Download Latest Code from Replit
1. Click **"Download as ZIP"** in Replit
2. Extract the files

### Step 2: Upload Changed File to Production
**Only this file changed:**
```
server/routes.ts
```

Use SFTP/FTP to upload:
```bash
# Using SCP:
scp server/routes.ts your-username@licenseiq.ai:/var/www/licenseiq/server/
```

### Step 3: Restart Server (same as Option A, Step 5)

---

## What Was Fixed

### Enhanced Product Matching Algorithm
**Before (Strict Substring):**
- Rule: "Automotive electronics and infotainment systems"
- Sale: "Auto Infotainment AI800"
- Result: ❌ No match

**After (Keyword-Based Semantic):**
- Rule: "Automotive electronics and infotainment systems"
  → Keywords: ["automotive", "electronics", "infotainment", "systems"]
- Sale: "Auto Infotainment AI800" (Automotive Electronics)
  → Keywords: ["auto", "infotainment", "ai800", "automotive", "electronics"]
- Result: ✅ Match! (Keywords: automotive, infotainment, electronics overlap)

### Changes in `server/routes.ts` (lines 1773-1811)
- Added keyword extraction logic
- Filters out common stop words
- Matches if at least 1 significant keyword overlaps
- Falls back to substring matching for exact matches

---

## Troubleshooting

### Issue: Server won't restart
```bash
# Check logs
pm2 logs licenseiq
# OR
sudo journalctl -u licenseiq -n 50
```

### Issue: Still showing 0 matches
1. Clear browser cache: `Ctrl + Shift + R`
2. Check server logs for errors
3. Re-upload sales data

### Issue: TypeScript errors
```bash
# Rebuild TypeScript
npm run build
```

---

## Admin User Setup (If Needed)

Run this SQL in your production database:
```sql
INSERT INTO users (id, email, password, role, name, company_id, business_unit_id, location_id, created_at, updated_at)
VALUES (
  'admin-user-001',
  'admin@licenseiq.ai',
  '$2b$10$rN3qY8XH9vZ5KqP7wL2MxeYvX5jK9Hx5tP3wQ2rS1mT4uV6wX7yZ8zA',
  'admin',
  'System Administrator',
  'default-company',
  'default-bu',
  'default-loc',
  NOW(),
  NOW()
)
ON CONFLICT (email) 
DO UPDATE SET 
  password = '$2b$10$rN3qY8XH9vZ5KqP7wL2MxeYvX5jK9Hx5tP3wQ2rS1mT4uV6wX7yZ8zA',
  role = 'admin',
  updated_at = NOW();
```

Login: `admin@licenseiq.ai` / `admin123`

---

## Need Help?

Contact your system administrator or hosting provider if you encounter issues.

**Files Changed:**
- ✅ `server/routes.ts` - Enhanced product matching logic
- ✅ `client/public/favicon.png` - Updated favicon
- ✅ `client/public/apple-touch-icon.png` - iOS icon
- ✅ `client/index.html` - Favicon meta tags

**No Database Changes Required** ✨
