/**
 * Create Admin User Script for LicenseIQ Production
 * 
 * This script creates an admin user with the correct scrypt password hash
 * that matches the authentication system.
 * 
 * Usage:
 *   node scripts/create-admin-user.mjs
 * 
 * Or with custom credentials:
 *   ADMIN_USERNAME=admin ADMIN_PASSWORD=YourPassword123! node scripts/create-admin-user.mjs
 */

import { scrypt, randomBytes } from 'crypto';
import { promisify } from 'util';

const scryptAsync = promisify(scrypt);

async function hashPassword(password) {
  const salt = randomBytes(16).toString("hex");
  const buf = await scryptAsync(password, salt, 64);
  return `${buf.toString("hex")}.${salt}`;
}

async function generateAdminCredentials() {
  const username = process.env.ADMIN_USERNAME || 'admin';
  const password = process.env.ADMIN_PASSWORD || 'Admin123!';
  const email = process.env.ADMIN_EMAIL || 'admin@licenseiq.ai';
  
  console.log('\n🔐 Generating LicenseIQ Admin User Credentials...\n');
  
  const hashedPassword = await hashPassword(password);
  
  console.log('✅ Password hash generated successfully!\n');
  console.log('📋 SQL Script to Create Admin User:\n');
  console.log('```sql');
  console.log(`INSERT INTO users (`);
  console.log(`  id,`);
  console.log(`  username,`);
  console.log(`  email,`);
  console.log(`  password,`);
  console.log(`  first_name,`);
  console.log(`  last_name,`);
  console.log(`  role,`);
  console.log(`  is_active,`);
  console.log(`  created_at,`);
  console.log(`  updated_at`);
  console.log(`) VALUES (`);
  console.log(`  gen_random_uuid(),`);
  console.log(`  '${username}',`);
  console.log(`  '${email}',`);
  console.log(`  '${hashedPassword}',`);
  console.log(`  'System',`);
  console.log(`  'Administrator',`);
  console.log(`  'admin',`);
  console.log(`  true,`);
  console.log(`  NOW(),`);
  console.log(`  NOW()`);
  console.log(`)`)
  console.log(`ON CONFLICT (username) DO UPDATE`);
  console.log(`SET password = EXCLUDED.password,`);
  console.log(`    email = EXCLUDED.email,`);
  console.log(`    role = EXCLUDED.role,`);
  console.log(`    is_active = EXCLUDED.is_active,`);
  console.log(`    updated_at = NOW();`);
  console.log('```\n');
  
  console.log('🔑 Login Credentials:');
  console.log(`   Username: ${username}`);
  console.log(`   Password: ${password}`);
  console.log(`   Email: ${email}`);
  console.log(`   Role: admin\n`);
  
  console.log('⚠️  IMPORTANT: Change this password immediately after first login!\n');
  
  console.log('📝 Instructions:');
  console.log('1. Copy the SQL script above');
  console.log('2. Run it in your production database (Replit Database Console or pgAdmin)');
  console.log('3. Go to https://licenseiq.ai');
  console.log('4. Login with the credentials shown above');
  console.log('5. Change the password in User Management\n');
  
  // Also output just the hash for quick reference
  console.log('🔧 Password Hash (for reference):');
  console.log(`   ${hashedPassword}\n`);
}

// Run the script
generateAdminCredentials().catch(console.error);
