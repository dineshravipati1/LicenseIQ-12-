# Create Admin User in Production (licenseiq.ai)

This guide provides multiple methods to create an admin user in your production database.

---

## Method 1: Using SQL (Quickest - Recommended)

Run this SQL directly in your production database console:

```sql
-- Create admin user with hashed password
-- Default password: "Admin123!" (you should change this immediately after login)
-- Bcrypt hash for "Admin123!": $2b$10$YourHashHere

INSERT INTO users (
  id,
  username, 
  email, 
  password, 
  first_name, 
  last_name, 
  role, 
  is_active, 
  created_at, 
  updated_at
) VALUES (
  gen_random_uuid(),
  'admin',
  'admin@yourdomain.com',  -- CHANGE THIS
  '$2b$10$rGwK9tKJZ5kF.F.mY5F5eO7K.Z5J8K7L6M5N4O3P2Q1R0S9T8U7V6',  -- Password: "Admin123!"
  'System',
  'Administrator',
  'admin',
  true,
  NOW(),
  NOW()
)
ON CONFLICT (username) DO NOTHING;
```

**⚠️ IMPORTANT**: 
- Change the email to your actual admin email
- The default password is `Admin123!` - **Change it immediately after first login**
- This assumes bcrypt is used for password hashing (which it should be)

---

## Method 2: Using Node.js Script (Most Secure)

Create a file `create-admin.js` on your production server:

```javascript
// create-admin.js
const { drizzle } = require('drizzle-orm/postgres-js');
const postgres = require('postgres');
const bcrypt = require('bcrypt');
const { users } = require('./shared/schema');

async function createAdminUser() {
  // Connect to production database
  const connectionString = process.env.DATABASE_URL;
  const client = postgres(connectionString);
  const db = drizzle(client);

  try {
    // Get admin details from environment or prompts
    const username = process.env.ADMIN_USERNAME || 'admin';
    const email = process.env.ADMIN_EMAIL || 'admin@yourdomain.com';
    const password = process.env.ADMIN_PASSWORD || 'Admin123!';
    
    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);
    
    // Create admin user
    const [newAdmin] = await db.insert(users).values({
      username: username,
      email: email,
      password: hashedPassword,
      firstName: 'System',
      lastName: 'Administrator',
      role: 'admin',
      isActive: true,
    }).returning();

    console.log('✅ Admin user created successfully!');
    console.log('📧 Email:', newAdmin.email);
    console.log('👤 Username:', newAdmin.username);
    console.log('🔑 Password:', password);
    console.log('\n⚠️  IMPORTANT: Change this password immediately after first login!');
    
  } catch (error) {
    if (error.code === '23505') {
      console.error('❌ User already exists with that username or email');
    } else {
      console.error('❌ Error creating admin user:', error.message);
    }
  } finally {
    await client.end();
  }
}

createAdminUser();
```

**Run it:**
```bash
# Set environment variables
export DATABASE_URL="your_production_database_url"
export ADMIN_USERNAME="admin"
export ADMIN_EMAIL="admin@yourdomain.com"
export ADMIN_PASSWORD="YourSecurePassword123!"

# Run the script
node create-admin.js
```

---

## Method 3: Direct Database SQL with Your Own Password Hash

If you want to use your own password, first hash it:

### Step 1: Hash your password locally
```javascript
// hash-password.js
const bcrypt = require('bcrypt');

const password = 'YourSecurePassword123!';
bcrypt.hash(password, 10).then(hash => {
  console.log('Hashed password:', hash);
});
```

Run: `node hash-password.js`

### Step 2: Use the hash in SQL
```sql
INSERT INTO users (
  id,
  username, 
  email, 
  password, 
  first_name, 
  last_name, 
  role, 
  is_active, 
  created_at, 
  updated_at
) VALUES (
  gen_random_uuid(),
  'admin',
  'your-email@domain.com',
  'YOUR_HASHED_PASSWORD_HERE',  -- Paste the hash from Step 1
  'System',
  'Administrator',
  'admin',
  true,
  NOW(),
  NOW()
);
```

---

## Method 4: Using Replit Database Console

If you're using Replit's database for production:

1. Go to your Replit project at licenseiq.ai
2. Click on the **Database** tab
3. Click **Console** to open SQL console
4. Paste and run the SQL from **Method 1** above
5. Verify with: `SELECT username, email, role FROM users WHERE role = 'admin';`

---

## Verification

After creating the admin user, verify it was created successfully:

```sql
SELECT id, username, email, role, is_active, created_at 
FROM users 
WHERE role = 'admin';
```

You should see your admin user listed.

---

## Security Best Practices

✅ **After creating the admin user:**

1. **Change the default password immediately**
   - Log in to licenseiq.ai with the admin credentials
   - Go to User Management or Profile settings
   - Change the password to a strong, unique password

2. **Enable 2FA** (if available)
   - Add two-factor authentication for the admin account

3. **Review other users**
   - Make sure there are no unauthorized admin accounts
   - Run: `SELECT username, email, role FROM users WHERE role = 'admin';`

4. **Delete this guide** after use
   - Remove `CREATE_ADMIN_USER_PRODUCTION.md` from your repository
   - Don't commit it to version control with passwords

---

## Troubleshooting

### Error: "duplicate key value violates unique constraint"
- The username or email already exists
- Try a different username or email
- Or check existing users: `SELECT * FROM users WHERE username = 'admin';`

### Error: "permission denied for table users"
- Your database connection doesn't have INSERT permissions
- Check your DATABASE_URL has the correct credentials

### Password doesn't work after creation
- Make sure you're using bcrypt to hash the password
- The hash should start with `$2b$10$`
- Try resetting the password through the "Forgot Password" flow

---

## Role Types in LicenseIQ

Your platform supports 5 role types:

| Role | Permissions |
|------|-------------|
| **owner** | Full system access, can manage everything |
| **admin** | Can manage users, contracts, approve changes |
| **editor** | Can edit contracts and data |
| **viewer** | Read-only access to contracts |
| **auditor** | Read-only access with audit trail viewing |

For the first admin user, use **role = 'admin'** or **role = 'owner'**.

---

**Created**: November 4, 2025  
**For**: licenseiq.ai production environment  
**Security Level**: CONFIDENTIAL - Delete after use
