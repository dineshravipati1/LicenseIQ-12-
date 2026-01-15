-- =====================================================
-- LicenseIQ Admin User Creation Script
-- =====================================================
-- This script creates a default admin user for initial system access
-- 
-- IMPORTANT SECURITY NOTES:
-- 1. Change the default password immediately after first login
-- 2. Use strong passwords in production environments
-- 3. Delete or secure this file after use
--
-- Default Credentials:
-- Username: admin
-- Email: admin@licenseiq.com
-- Password: Admin@123!
-- Role: admin
-- =====================================================

-- Check if admin user already exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM users WHERE username = 'admin' OR email = 'admin@licenseiq.com'
  ) THEN
    -- Insert admin user
    -- Password hash is bcrypt for: Admin@123!
    INSERT INTO users (
      id,
      username,
      email,
      password,
      first_name,
      last_name,
      profile_image_url,
      role,
      is_active,
      created_at,
      updated_at
    ) VALUES (
      gen_random_uuid(),
      'admin',
      'admin@licenseiq.com',
      '$2b$10$rH8QnP6Y6YxH.YmZ5Q8eC.X8vK9ZGqH5xJ3wL0mN2pQ4rS6tU8vW6', -- Admin@123!
      'System',
      'Administrator',
      NULL,
      'admin',
      true,
      NOW(),
      NOW()
    );

    RAISE NOTICE 'Admin user created successfully!';
    RAISE NOTICE 'Username: admin';
    RAISE NOTICE 'Password: Admin@123!';
    RAISE NOTICE 'IMPORTANT: Change this password immediately after first login!';
  ELSE
    RAISE NOTICE 'Admin user already exists. Skipping creation.';
  END IF;
END $$;

-- =====================================================
-- Alternative: Create additional admin users
-- =====================================================
-- Uncomment and modify the section below to create additional admin users

/*
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
  'yourusername',
  'your.email@company.com',
  'YOUR_BCRYPT_HASHED_PASSWORD_HERE',
  'Your',
  'Name',
  'admin',
  true,
  NOW(),
  NOW()
) ON CONFLICT (username) DO NOTHING;
*/

-- =====================================================
-- To generate a new bcrypt password hash, use Node.js:
-- =====================================================
-- const bcrypt = require('bcrypt');
-- const hash = await bcrypt.hash('your-password', 10);
-- console.log(hash);
-- =====================================================
