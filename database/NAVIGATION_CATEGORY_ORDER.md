# Navigation Category Display Order

## Current Display Order (Left Sidebar)

The navigation categories are displayed in the following order from top to bottom:

| Order | Category Key | Category Name | Icon | Expanded by Default |
|-------|--------------|---------------|------|---------------------|
| 1 | `dashboard_analytics` | Dashboard & Analytics | BarChart3 📊 | ✅ Yes |
| 2 | `contract_management` | Contract Management | File 📄 | ✅ Yes |
| 3 | `ai_intelligence` | AI & Intelligence | Brain 🧠 | ✅ Yes |
| 4 | `license_fee_mgmt` | License Fee Management | Calculator 🧮 | ❌ No |
| 5 | `data_management` | Data Management | Database 💾 | ❌ No |
| 6 | `administration` | Administration | Users 👥 | ❌ No |

## How Display Order Works

### System-Level Order
- Categories are ordered by the `default_sort_order` field in the `navigation_categories` table
- Lower numbers appear first (top of sidebar)
- Higher numbers appear last (bottom of sidebar)

### User-Level Customization
- Users can reorganize categories using the **Navigation Manager** drag-and-drop interface
- User preferences are stored in the `user_category_preferences` table
- User preferences override the default system order

## Modifying Category Display Order

### Option 1: Update Default Sort Order (System-Wide)

To change the default order for all users, update the `default_sort_order` field:

```sql
-- Move Administration to position 3 (after AI & Intelligence)
UPDATE navigation_categories 
SET default_sort_order = 3,
    updated_at = NOW()
WHERE category_key = 'administration';

-- Shift other categories down
UPDATE navigation_categories 
SET default_sort_order = default_sort_order + 1,
    updated_at = NOW()
WHERE category_key IN ('license_fee_mgmt', 'data_management')
  AND default_sort_order >= 3;
```

### Option 2: Reorganize All Categories

Complete reorganization example:

```sql
-- Set new display order for all categories
UPDATE navigation_categories SET default_sort_order = 1, updated_at = NOW() WHERE category_key = 'dashboard_analytics';
UPDATE navigation_categories SET default_sort_order = 2, updated_at = NOW() WHERE category_key = 'contract_management';
UPDATE navigation_categories SET default_sort_order = 3, updated_at = NOW() WHERE category_key = 'administration';
UPDATE navigation_categories SET default_sort_order = 4, updated_at = NOW() WHERE category_key = 'data_management';
UPDATE navigation_categories SET default_sort_order = 5, updated_at = NOW() WHERE category_key = 'ai_intelligence';
UPDATE navigation_categories SET default_sort_order = 6, updated_at = NOW() WHERE category_key = 'license_fee_mgmt';
```

### Option 3: User-Level Customization (Per User)

Users can customize their own order via the UI:
1. Navigate to **Navigation Manager** page
2. Drag and drop categories to reorder them
3. Click **Save Changes**

This saves preferences to `user_category_preferences` table without affecting other users.

## Category Properties

### Collapsible Categories
All categories are collapsible (`is_collapsible = true`), allowing users to expand/collapse them.

### Default Expanded State

| Category | Default State | Reasoning |
|----------|---------------|-----------|
| Dashboard & Analytics | Expanded | Quick access to overview |
| Contract Management | Expanded | Core functionality |
| AI & Intelligence | Expanded | Key AI features |
| License Fee Management | Collapsed | Advanced feature |
| Data Management | Collapsed | Admin/power user feature |
| Administration | Collapsed | Admin-only settings |

### Change Default Expanded State

```sql
-- Make Administration expanded by default
UPDATE navigation_categories 
SET default_expanded = true,
    updated_at = NOW()
WHERE category_key = 'administration';

-- Make AI & Intelligence collapsed by default
UPDATE navigation_categories 
SET default_expanded = false,
    updated_at = NOW()
WHERE category_key = 'ai_intelligence';
```

## Best Practices

### Recommended Category Order

1. **Dashboard & Analytics** - Users typically start here
2. **Contract Management** - Core business functionality
3. **License Fee Management** - Related to contracts
4. **AI & Intelligence** - Value-added features
5. **Data Management** - Supporting features
6. **Administration** - Settings and configuration

### Guidelines

✅ **DO:**
- Place most-used categories at the top
- Group related functionality together
- Keep admin/configuration features at the bottom
- Test changes with actual users

❌ **DON'T:**
- Use gaps in sort order (use consecutive numbers: 1, 2, 3, not 1, 5, 10)
- Change order frequently (confuses users)
- Make all categories expanded by default (causes sidebar clutter)

## Adding New Categories

When adding a new category, assign an appropriate `default_sort_order`:

```sql
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
  'reporting',
  'Reporting & Analytics',
  'TrendingUp',
  'Advanced reporting and analytics features',
  7, -- Place after Administration
  true,
  false, -- Collapsed by default
  true
);
```

## Verifying Current Order

### SQL Query to Check Order

```sql
SELECT 
  default_sort_order,
  category_key,
  category_name,
  icon_name,
  CASE WHEN default_expanded THEN 'Expanded' ELSE 'Collapsed' END as default_state,
  CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status
FROM navigation_categories
WHERE is_active = true
ORDER BY default_sort_order ASC;
```

### Check User-Specific Preferences

```sql
-- See user customizations
SELECT 
  u.username,
  ucp.nav_item_key,
  ucp.category_key,
  ucp.sort_order,
  ucp.is_visible
FROM user_category_preferences ucp
JOIN users u ON u.id = ucp.user_id
WHERE u.username = 'admin'
ORDER BY ucp.category_key, ucp.sort_order;
```

## Related Database Tables

- `navigation_categories` - Category definitions and default order
- `navigation_item_categories` - Maps navigation items to categories
- `user_category_preferences` - User-specific category/item organization
- `user_category_state` - User's expand/collapse state per category

---

**Last Updated:** 2025-11-24  
**Version:** 1.0
