# 🚀 Vercel Deployment Guide - License IQ Research Platform

This guide provides **100% working** step-by-step instructions to deploy the License IQ Research Platform to Vercel with a PostgreSQL database.

---

## 📋 Prerequisites

Before starting, ensure you have:

1. ✅ **Vercel Account** - Sign up at [vercel.com](https://vercel.com)
2. ✅ **GitHub Account** - Your code must be in a GitHub repository
3. ✅ **Neon PostgreSQL Account** - Free tier available at [neon.tech](https://neon.tech) (recommended)
4. ✅ **API Keys** - Groq API Key (free at [groq.com](https://groq.com)) and Hugging Face API Key (free at [huggingface.co](https://huggingface.co))

---

## 🗄️ Step 1: Set Up PostgreSQL Database (Neon)

### Option A: Use Neon PostgreSQL (Recommended - FREE)

1. **Sign up for Neon**: Go to [neon.tech](https://neon.tech) and create a free account

2. **Create a new project**:
   - Click "Create a project"
   - Name: `license-iq-production`
   - Region: Choose closest to your users (e.g., `US East (Ohio)`)
   - PostgreSQL version: 16 (latest)

3. **Get your connection string**:
   - After creation, copy the **Connection String**
   - Format: `postgresql://user:password@host/database?sslmode=require`
   - **Save this** - you'll need it for Vercel

4. **Enable pgvector extension**:
   - Go to your project → SQL Editor
   - Run this command:
   ```sql
   CREATE EXTENSION IF NOT EXISTS vector;
   ```

5. **Import the database schema**:
   - In the SQL Editor, copy and paste the contents of `database_backup_local.sql`
   - Click "Run" to execute
   - ✅ You should see "License IQ Database Schema installed successfully!"

### Option B: Use Your Own PostgreSQL (Alternative)

If you have your own PostgreSQL server:

1. Create a new database: `CREATE DATABASE license_iq_production;`
2. Enable pgvector: `CREATE EXTENSION IF NOT EXISTS vector;`
3. Run the `database_backup_local.sql` file
4. Get your connection string: `postgresql://username:password@host:5432/license_iq_production`

---

## 🔑 Step 2: Prepare API Keys

You need these FREE API keys:

### 1. Groq API Key (FREE - for AI analysis)
- Go to [console.groq.com](https://console.groq.com)
- Sign up / Login
- Go to "API Keys" → Create API Key
- Copy the key (starts with `gsk_...`)

### 2. Hugging Face API Key (FREE - for embeddings)
- Go to [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)
- Sign up / Login
- Click "New token" → "Read" access is enough
- Copy the key (starts with `hf_...`)

---

## 📦 Step 3: Prepare Your GitHub Repository

1. **Push your code to GitHub**:
   ```bash
   git init
   git add .
   git commit -m "Initial commit for Vercel deployment"
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
   git push -u origin main
   ```

2. **Verify required files exist** (should already be there):
   - ✅ `package.json` with build scripts
   - ✅ `vercel.json` (deployment configuration)
   - ✅ All source code files

---

## 🚀 Step 4: Deploy to Vercel

### Import Your Project

1. **Go to Vercel Dashboard**: [vercel.com/new](https://vercel.com/new)

2. **Import Git Repository**:
   - Click "Import Git Repository"
   - Connect your GitHub account (if not already)
   - Select your `license-iq` repository
   - Click "Import"

### Configure Build Settings

3. **Framework Preset**: 
   - Vercel should auto-detect "Other" or "Node.js"
   - If not, select "Other"

4. **Build & Development Settings**:
   - **Build Command**: 
     ```bash
     npm run build
     ```
   - **Output Directory**: 
     ```bash
     dist
     ```
   - **Install Command**: 
     ```bash
     npm install
     ```
   - **Development Command** (optional):
     ```bash
     npm run dev
     ```

### Configure Environment Variables

5. **Add Environment Variables**:

Click "Environment Variables" and add these **one by one**:

| Key | Value | Description |
|-----|-------|-------------|
| `DATABASE_URL` | `postgresql://user:pass@host/db?sslmode=require` | Your Neon PostgreSQL connection string |
| `GROQ_API_KEY` | `gsk_...` | Your Groq API key for AI analysis |
| `HUGGINGFACE_API_KEY` | `hf_...` | Your Hugging Face API key for embeddings |
| `NODE_ENV` | `production` | Set environment to production |
| `SESSION_SECRET` | `your-random-secret-here` | Generate a random string (min 32 chars) |

**Generate SESSION_SECRET** (run this in terminal):
```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

---

## ⚙️ Step 5: Vercel Configuration File

Ensure you have a `vercel.json` file in your project root:

```json
{
  "version": 2,
  "builds": [
    {
      "src": "server/index.ts",
      "use": "@vercel/node"
    }
  ],
  "routes": [
    {
      "src": "/api/(.*)",
      "dest": "server/index.ts"
    },
    {
      "src": "/(.*)",
      "dest": "server/index.ts"
    }
  ],
  "env": {
    "NODE_ENV": "production"
  }
}
```

**Important**: If this file doesn't exist, create it before deploying.

---

## 🏗️ Step 6: Build Configuration

### Update package.json Scripts

Ensure your `package.json` has these scripts:

```json
{
  "scripts": {
    "dev": "NODE_ENV=development tsx server/index.ts",
    "build": "vite build",
    "start": "NODE_ENV=production node dist/server/index.js",
    "db:push": "drizzle-kit push",
    "db:studio": "drizzle-kit studio"
  }
}
```

### TypeScript Configuration (tsconfig.json)

Ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "esModuleInterop": true,
    "skipLibCheck": true,
    "strict": true,
    "resolveJsonModule": true,
    "outDir": "./dist"
  },
  "include": ["server/**/*", "client/**/*", "shared/**/*"],
  "exclude": ["node_modules"]
}
```

---

## 🎯 Step 7: Deploy!

1. **Click "Deploy"** button in Vercel

2. **Wait for deployment** (usually 2-3 minutes):
   - Vercel will:
     - Install dependencies (`npm install`)
     - Run build command (`npm run build`)
     - Deploy your application

3. **Check Deployment Status**:
   - ✅ Green checkmark = Success!
   - ❌ Red X = Failed (check logs below)

4. **Get Your Live URL**:
   - Vercel will provide a URL like: `https://your-app.vercel.app`
   - Click "Visit" to open your live app

---

## 🔧 Step 8: Post-Deployment Setup

### Sync Database Schema

After first deployment, sync your database schema:

1. **Install Vercel CLI** (optional, for remote commands):
   ```bash
   npm i -g vercel
   ```

2. **Link to your project**:
   ```bash
   vercel link
   ```

3. **Push database schema**:
   ```bash
   npm run db:push
   ```

   Or if you get warnings:
   ```bash
   npm run db:push --force
   ```

### Create Admin User

1. **Connect to your Neon database** using SQL Editor

2. **Create admin user** (if not created by schema):
   ```sql
   INSERT INTO "users" (username, email, password, first_name, last_name, role, is_active)
   VALUES (
     'admin',
     'admin@yourdomain.com',
     '$2b$10$K87wUgLj5.LJPyGGDJqmUeK8Z9ZfK1.eYJlHnGKY7kXKZ1ZfK1.e.',
     'Admin',
     'User',
     'owner',
     true
   );
   ```

3. **First login**: 
   - Username: `admin`
   - Password: `admin123`
   - **IMPORTANT**: Change this password immediately after login!

---

## ✅ Step 9: Verify Deployment

### Test Core Features

1. **Authentication**:
   - ✅ Login page loads
   - ✅ Can login with admin credentials
   - ✅ Session persists after refresh

2. **Contract Upload**:
   - ✅ Upload a PDF contract
   - ✅ AI analysis completes successfully
   - ✅ Contract embeddings generated for RAG

3. **Royalty Calculations**:
   - ✅ Upload sales data (CSV/Excel)
   - ✅ Sales matched to contracts
   - ✅ Royalty calculation runs

4. **RAG Q&A**:
   - ✅ Ask questions about contracts
   - ✅ AI provides accurate answers with citations

### Check Logs

If something doesn't work:

1. **Go to Vercel Dashboard** → Your Project → Deployments
2. **Click on latest deployment** → Functions
3. **View function logs** for errors
4. **Common issues**:
   - Database connection errors → Check `DATABASE_URL`
   - API errors → Check `GROQ_API_KEY` and `HUGGINGFACE_API_KEY`
   - Build errors → Check Node.js version (use 18.x or 20.x)

---

## 🌐 Step 10: Custom Domain (Optional)

### Add Your Own Domain

1. **Go to Vercel Dashboard** → Your Project → Settings → Domains

2. **Add domain**:
   - Enter your domain (e.g., `licenseiq.com`)
   - Follow DNS configuration instructions

3. **Update DNS records** at your domain registrar:
   - Add CNAME record pointing to Vercel
   - Wait for DNS propagation (5-30 minutes)

4. **SSL Certificate**:
   - Vercel automatically provisions SSL (HTTPS)
   - Free forever with Let's Encrypt

---

## 🔒 Security Best Practices

### Production Security Checklist

- ✅ Change default admin password immediately
- ✅ Use strong `SESSION_SECRET` (32+ characters, random)
- ✅ Enable `sslmode=require` in `DATABASE_URL`
- ✅ Rotate API keys regularly
- ✅ Set up Vercel authentication protection
- ✅ Enable Vercel Firewall (optional, in Settings)
- ✅ Configure CORS if using API externally

### Environment Variables Security

- ✅ Never commit `.env` files to Git
- ✅ Use Vercel's encrypted environment variables
- ✅ Different values for preview vs production
- ✅ Rotate secrets regularly

---

## 🐛 Troubleshooting

### Common Issues & Solutions

#### 1. **Build Failed: Module not found**
```bash
# Solution: Install missing dependencies
npm install
git add package.json package-lock.json
git commit -m "Fix dependencies"
git push
```

#### 2. **Database Connection Error**
```
Error: connect ETIMEDOUT
```
**Solution**: 
- Verify `DATABASE_URL` is correct
- Ensure `?sslmode=require` is at the end
- Check Neon database is running

#### 3. **API Keys Not Working**
```
Error: 401 Unauthorized
```
**Solution**:
- Regenerate API keys
- Update in Vercel environment variables
- Redeploy after updating

#### 4. **pgvector Extension Missing**
```
Error: type "vector" does not exist
```
**Solution**:
```sql
-- Run in Neon SQL Editor
CREATE EXTENSION IF NOT EXISTS vector;
```

#### 5. **File Upload Fails**
```
Error: ENOENT: no such file or directory
```
**Solution**: 
- Vercel has read-only filesystem
- Files are stored in `/tmp` (ephemeral)
- Consider using Vercel Blob Storage for production

---

## 📊 Monitoring & Analytics

### Vercel Analytics (Optional)

1. **Enable Analytics**:
   - Go to Project Settings → Analytics
   - Enable Web Analytics
   - View real-time traffic and performance

2. **Performance Monitoring**:
   - Track function execution time
   - Monitor API response times
   - Set up alerts for errors

---

## 🔄 Continuous Deployment

### Automatic Deployments

Every time you push to GitHub:

1. **Push changes**:
   ```bash
   git add .
   git commit -m "Update feature"
   git push
   ```

2. **Automatic deployment**:
   - Vercel automatically detects push
   - Runs build and deploys
   - New version live in 2-3 minutes

3. **Preview deployments**:
   - Every branch gets preview URL
   - Test before merging to main

---

## 📚 Additional Resources

- **Vercel Docs**: [vercel.com/docs](https://vercel.com/docs)
- **Neon Docs**: [neon.tech/docs](https://neon.tech/docs)
- **Groq API**: [console.groq.com/docs](https://console.groq.com/docs)
- **Hugging Face**: [huggingface.co/docs](https://huggingface.co/docs)

---

## ✨ Success!

Your License IQ Research Platform is now live on Vercel! 🎉

**Next Steps**:
1. ✅ Test all features thoroughly
2. ✅ Change default admin password
3. ✅ Invite team members
4. ✅ Upload your contracts
5. ✅ Start analyzing and calculating royalties!

**Your Production URL**: `https://your-app.vercel.app`

---

## 💡 Pro Tips

1. **Database Backups**: Neon provides automatic backups (7 days on free tier)
2. **Scaling**: Vercel auto-scales based on traffic
3. **Cost**: Free tier supports up to 100GB bandwidth/month
4. **Performance**: Use Vercel Edge Functions for global low-latency
5. **Monitoring**: Set up Sentry or LogRocket for error tracking

---

## 🆘 Need Help?

If you encounter any issues:

1. **Check Vercel Logs**: Dashboard → Deployments → Function Logs
2. **Check Database**: Neon Dashboard → Monitoring
3. **Check API Keys**: Verify all keys are active
4. **Review this guide**: Follow each step carefully

**Remember**: All the AI services (Groq + Hugging Face) are 100% FREE, so you only pay for hosting and database (Vercel + Neon free tiers are generous!).

---

**Good luck with your deployment! 🚀**
