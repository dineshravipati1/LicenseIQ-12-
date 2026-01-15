# Admin User Management

## Creating the Default Admin User

### Quick Start

Run the admin user creation script:

```bash
# For local development (Neon database)
psql $DATABASE_URL -f database/create_admin_user.sql

# For Hostinger VPS
psql -h localhost -U licenseiq -d licenseiq -f database/create_admin_user.sql
```

### Default Admin Credentials

After running the script, you can login with:

- **Username:** `admin`
- **Email:** `admin@licenseiq.com`
- **Password:** `Admin@123!`
- **Role:** `admin`

**⚠️ CRITICAL SECURITY WARNING:**
Change the default password immediately after your first login!

---

## Creating Custom Admin Users

### Option 1: Using Node.js Script

Create a quick password hash:

```javascript
// generate_password_hash.js
const bcrypt = require('bcrypt');

async function generateHash() {
  const password = 'YourSecurePassword123!';
  const hash = await bcrypt.hash(password, 10);
  console.log('Password Hash:', hash);
}

generateHash();
```

Run it:
```bash
node generate_password_hash.js
```

Then use the hash in your SQL INSERT statement.

### Option 2: Direct SQL Insert

Modify the `create_admin_user.sql` script or run directly:

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
  'newadmin',
  'newadmin@company.com',
  '$2b$10$YOUR_BCRYPT_HASH_HERE',
  'First',
  'Last',
  'admin',
  true,
  NOW(),
  NOW()
);
```

---

## User Roles

The system supports 5 role levels:

| Role | Access Level | Description |
|------|--------------|-------------|
| `owner` | Full access | System owner, can manage everything |
| `admin` | High access | Administrative access, can manage users and configuration |
| `editor` | Medium access | Can create and edit contracts, calculations |
| `viewer` | Read-only | Can view data but cannot modify |
| `auditor` | Audit access | Can view audit trails and compliance data |

---

## Security Best Practices

1. **Change default passwords immediately**
2. **Use strong passwords** (12+ characters, mixed case, numbers, symbols)
3. **Enable 2FA** when implemented
4. **Rotate admin passwords regularly**
5. **Delete or secure this script file** after initial setup
6. **Monitor admin user activity** via audit trail
7. **Limit admin user accounts** to only those who need it

---

## Troubleshooting

### User Already Exists

If you get an error that the user already exists, you can:

1. **Reset the password** using the password reset flow
2. **Update the password directly** (not recommended for production):

```sql
UPDATE users 
SET password = '$2b$10$NEW_BCRYPT_HASH_HERE',
    updated_at = NOW()
WHERE username = 'admin';
```

### Connection Issues

Make sure your database connection string is correct:

```bash
# Check your DATABASE_URL
echo $DATABASE_URL

# Test connection
psql $DATABASE_URL -c "SELECT version();"
```

---

## Related Files

- `create_admin_user.sql` - Main admin user creation script
- `reset_admin_password_hostinger.sql` - Password reset script for Hostinger
- `backup.sh` - Database backup script
- `restore.sh` - Database restore script

---

**Last Updated:** 2025-11-24
