# Navigation Category Display Order

This shows the display order of **navigation categories** in the Navigation Manager UI.

---

## 📊 Category Display Order (Position 1-6)

| Position | Category Name | Category Key | Icon | Default State |
|----------|---------------|--------------|------|---------------|
| **1** | Dashboard & Analytics | `dashboard_analytics` | 📊 BarChart3 | Expanded |
| **2** | Contract Management | `contract_management` | 📄 File | Expanded |
| **3** | AI & Intelligence | `ai_intelligence` | 🧠 Brain | Expanded |
| **4** | License Fee Management | `license_fee_mgmt` | 🧮 Calculator | Collapsed |
| **5** | Data Management | `data_management` | 💾 Database | Collapsed |
| **6** | Administration | `administration` | 👥 Users | Collapsed |

---

## 🎯 Simple List View

1. Dashboard & Analytics
2. Contract Management
3. AI & Intelligence
4. License Fee Management
5. Data Management
6. Administration

---

## 🔢 How to Change Category Order

To change which category appears in which position, update the `default_sort_order` column:

```sql
-- Example: Move Administration to position 3
UPDATE navigation_categories 
SET default_sort_order = 3 
WHERE category_key = 'administration';

-- Then adjust other categories as needed
UPDATE navigation_categories 
SET default_sort_order = 4 
WHERE category_key = 'ai_intelligence';

UPDATE navigation_categories 
SET default_sort_order = 5 
WHERE category_key = 'license_fee_mgmt';

UPDATE navigation_categories 
SET default_sort_order = 6 
WHERE category_key = 'data_management';
```

---

**That's it! The categories display in order 1-6 as listed above.**
