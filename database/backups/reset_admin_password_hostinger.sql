-- ================================================================
-- RESET ADMIN PASSWORD FOR HOSTINGER VPS
-- ================================================================
-- 
-- This script resets the admin password to: admin123
-- 
-- USAGE:
--   Run on your Hostinger VPS PostgreSQL:
--   sudo -u postgres psql licenseiq < reset_admin_password_hostinger.sql
--
--   OR connect via psql and run:
--   psql -U licenseiq -d licenseiq -h localhost
--   (then paste the UPDATE command below)
--
-- ================================================================

-- Reset admin password to "admin123"
UPDATE users 
SET 
  password = '6c9f5c5c3a5ac616e9def890a4976c78f3a7d763e33bb821555bafcf56402f1c3ac0a01a8b0fc39ad602bf716c78af61f1dfbf2befb54c2a745a715fa59c90a2.ee9a76c00eb5cf93c2fe572ec9af5bd3',
  updated_at = NOW()
WHERE username = 'admin';

-- Verify the update
SELECT 
  id, 
  username, 
  email, 
  first_name, 
  last_name, 
  role, 
  is_active,
  updated_at
FROM users 
WHERE username = 'admin';

-- Show confirmation message
SELECT 'Admin password successfully reset to: admin123' AS status;
