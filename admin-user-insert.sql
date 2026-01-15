-- ================================================================
-- ADMIN USER INSERT SCRIPT
-- LicenseIQ Research Platform
-- ================================================================
-- 
-- This script creates a default admin user for the system
-- 
-- CREDENTIALS:
--   Username: admin
--   Password: Admin@123
--   Email:    admin@licenseiq.ai
--   Role:     admin
--
-- IMPORTANT: Change the password after first login!
-- ================================================================

-- Insert admin user
INSERT INTO users (id, username, email, password, first_name, last_name, role, is_active, created_at, updated_at)
VALUES (
  gen_random_uuid(),
  'admin',
  'admin@licenseiq.ai',
  'dc3de46f8482ad7cb25ec34e98eefdbaf6bcce45acf8d78c9e273d849e14e1574b79e329270e581d69721bf9659a0bc9bcc0a03b94820d34f7adf37d5a93365a.53936925e3462c059fa1c84e7dbf3d67',
  'System',
  'Administrator',
  'admin',
  true,
  NOW(),
  NOW()
)
ON CONFLICT (username) DO NOTHING;

-- Verify insertion
SELECT id, username, email, first_name, last_name, role, is_active, created_at 
FROM users 
WHERE username = 'admin';
