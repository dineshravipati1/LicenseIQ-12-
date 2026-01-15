# 🚀 Quick Setup Guide for LicenseIQ

Welcome! This guide will help you get LicenseIQ up and running on Replit in under 5 minutes.

## ✅ Step-by-Step Setup

### 1️⃣ Database Setup

Your project includes a complete database backup. Choose one option:

**Option A: Restore Full Database (Recommended)**

This gives you a working app with sample data and test users:

```bash
psql $DATABASE_URL < database/backups/full_backup.sql
```

**Option B: Fresh Database**

Start from scratch with empty tables:

```bash
npm run db:push
```

### 2️⃣ Set API Key

1. Open **Tools → Secrets** in Replit
2. Add new secret:
   - **Key**: `GROQ_API_KEY`
   - **Value**: Your API key from [groq.com/console](https://console.groq.com)

### 3️⃣ Start the Application

```bash
npm run dev
```

The app will open automatically in Replit's webview!

### 4️⃣ Login

If you restored the database, use these test credentials:

- **Username**: `admin`
- **Password**: `admin123`

If you started fresh, click "Sign Up" to create your first account.

---

## 🔧 Common Commands

```bash
# Start development server
npm run dev

# Create database backup
bash database/backup.sh

# Restore database
bash database/restore.sh

# Push schema changes
npm run db:push
```

## 📚 Full Documentation

See **[README.md](./README.md)** for complete documentation including:
- Detailed feature list
- Architecture overview
- Database management
- Deployment guide

## 🆘 Troubleshooting

**Database connection error?**
```bash
echo $DATABASE_URL  # Should show PostgreSQL URL
```
If empty, add PostgreSQL database from Tools → Database

**Missing tables?**
```bash
psql $DATABASE_URL < database/backups/full_backup.sql
```

**App won't start?**
```bash
# Clear node modules and reinstall
rm -rf node_modules
npm install
```

---

**Need help?** Check the full [README.md](./README.md) or Replit community forums.

**Ready to go?** Run `npm run dev` and start managing contracts with AI! 🎉
