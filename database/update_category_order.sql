-- =====================================================
-- Update Navigation Category Display Order
-- =====================================================
-- This script allows you to easily reorder the navigation categories
-- in the left sidebar drag-and-drop navigation manager.
--
-- Current Default Order:
-- 1. Dashboard & Analytics
-- 2. Contract Management
-- 3. AI & Intelligence
-- 4. License Fee Management
-- 5. Data Management
-- 6. Administration
-- =====================================================

-- View current order
SELECT 
  default_sort_order as "Order",
  category_key as "Category Key",
  category_name as "Category Name",
  icon_name as "Icon",
  CASE WHEN default_expanded THEN 'Expanded' ELSE 'Collapsed' END as "Default State"
FROM navigation_categories
WHERE is_active = true
ORDER BY default_sort_order ASC;

-- =====================================================
-- Option 1: Keep Current Order (Default)
-- =====================================================
-- No changes needed - current order is optimal

-- =====================================================
-- Option 2: Move Administration Higher (After Dashboard)
-- =====================================================
-- Uncomment below to make Administration appear earlier

/*
BEGIN;

UPDATE navigation_categories SET default_sort_order = 1, updated_at = NOW() WHERE category_key = 'dashboard_analytics';
UPDATE navigation_categories SET default_sort_order = 2, updated_at = NOW() WHERE category_key = 'administration';
UPDATE navigation_categories SET default_sort_order = 3, updated_at = NOW() WHERE category_key = 'contract_management';
UPDATE navigation_categories SET default_sort_order = 4, updated_at = NOW() WHERE category_key = 'ai_intelligence';
UPDATE navigation_categories SET default_sort_order = 5, updated_at = NOW() WHERE category_key = 'license_fee_mgmt';
UPDATE navigation_categories SET default_sort_order = 6, updated_at = NOW() WHERE category_key = 'data_management';

COMMIT;
*/

-- =====================================================
-- Option 3: Group by Business Function
-- =====================================================
-- Core Business → Analytics & AI → Admin/Data

/*
BEGIN;

-- Core Business Operations
UPDATE navigation_categories SET default_sort_order = 1, updated_at = NOW() WHERE category_key = 'contract_management';
UPDATE navigation_categories SET default_sort_order = 2, updated_at = NOW() WHERE category_key = 'license_fee_mgmt';

-- Analytics & Intelligence
UPDATE navigation_categories SET default_sort_order = 3, updated_at = NOW() WHERE category_key = 'dashboard_analytics';
UPDATE navigation_categories SET default_sort_order = 4, updated_at = NOW() WHERE category_key = 'ai_intelligence';

-- Administration & Data
UPDATE navigation_categories SET default_sort_order = 5, updated_at = NOW() WHERE category_key = 'data_management';
UPDATE navigation_categories SET default_sort_order = 6, updated_at = NOW() WHERE category_key = 'administration';

COMMIT;
*/

-- =====================================================
-- Option 4: Custom Order Template
-- =====================================================
-- Modify the numbers below to your preferred order

/*
BEGIN;

UPDATE navigation_categories SET default_sort_order = 1, updated_at = NOW() WHERE category_key = 'dashboard_analytics';
UPDATE navigation_categories SET default_sort_order = 2, updated_at = NOW() WHERE category_key = 'contract_management';
UPDATE navigation_categories SET default_sort_order = 3, updated_at = NOW() WHERE category_key = 'ai_intelligence';
UPDATE navigation_categories SET default_sort_order = 4, updated_at = NOW() WHERE category_key = 'license_fee_mgmt';
UPDATE navigation_categories SET default_sort_order = 5, updated_at = NOW() WHERE category_key = 'data_management';
UPDATE navigation_categories SET default_sort_order = 6, updated_at = NOW() WHERE category_key = 'administration';

COMMIT;
*/

-- =====================================================
-- Verify the new order after running updates
-- =====================================================
/*
SELECT 
  default_sort_order as "New Order",
  category_key as "Category Key",
  category_name as "Category Name"
FROM navigation_categories
WHERE is_active = true
ORDER BY default_sort_order ASC;
*/

-- =====================================================
-- Reset User Preferences (Optional)
-- =====================================================
-- If you want all users to see the new default order,
-- delete their custom preferences:

/*
-- WARNING: This will reset ALL users' custom navigation preferences!
TRUNCATE TABLE user_category_preferences;
TRUNCATE TABLE user_category_state;
*/

-- =====================================================
-- Change Default Expanded/Collapsed State
-- =====================================================
-- Make categories expanded or collapsed by default

-- Expand Administration by default
/*
UPDATE navigation_categories 
SET default_expanded = true, updated_at = NOW() 
WHERE category_key = 'administration';
*/

-- Collapse AI & Intelligence by default
/*
UPDATE navigation_categories 
SET default_expanded = false, updated_at = NOW() 
WHERE category_key = 'ai_intelligence';
*/

-- =====================================================
-- Add a New Category
-- =====================================================
-- Template for adding a new navigation category

/*
INSERT INTO navigation_categories (
  id,
  category_key,
  category_name,
  icon_name,
  description,
  default_sort_order,
  is_collapsible,
  default_expanded,
  is_active
) VALUES (
  gen_random_uuid(),
  'your_category_key',
  'Your Category Name',
  'IconName', -- e.g., 'FileText', 'Settings', 'Archive'
  'Description of this category',
  7, -- Next available order number
  true, -- Can be collapsed?
  false, -- Expanded by default?
  true -- Is active?
);
*/
