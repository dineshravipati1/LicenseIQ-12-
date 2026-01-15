# Navigation Manager - Frontend UI Display Order

This document shows the **actual display order** of navigation categories as they appear in the **Navigation Manager** drag-and-drop interface.

---

## 🖥️ Frontend UI Layout

The Navigation Manager displays categories in a **2-column grid layout** (side-by-side on large screens).

### Current Display Order (Top to Bottom, Left to Right):

```
┌─────────────────────────────────────────────────────────────────────┐
│                        NAVIGATION MANAGER                            │
│  Drag and drop to organize your navigation menu                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  [💾 Save Changes]  [🔄 Reset to Defaults]     [➕ New Category]    │
│                                                                       │
├──────────────────────────────────┬──────────────────────────────────┤
│  LEFT COLUMN                     │  RIGHT COLUMN                     │
├──────────────────────────────────┼──────────────────────────────────┤
│                                  │                                   │
│  1️⃣  DASHBOARD & ANALYTICS       │  4️⃣  LICENSE FEE MANAGEMENT      │
│      📊 BarChart3                │      🧮 Calculator                │
│      Key: dashboard_analytics    │      Key: license_fee_mgmt        │
│      ─────────────────────────   │      ─────────────────────────    │
│      📍 Dashboard                │      🧮 License Fee Calculator    │
│      📈 Analytics                │      💰 Sales Data                │
│      📊 Reports                  │                                   │
│                                  │                                   │
│  ─────────────────────────────   │  ─────────────────────────────    │
│                                  │                                   │
│  2️⃣  CONTRACT MANAGEMENT         │  5️⃣  DATA MANAGEMENT             │
│      📄 File                     │      💾 Database                  │
│      Key: contract_management    │      Key: data_management         │
│      ─────────────────────────   │      ─────────────────────────    │
│      📄 Contracts                │      🗺️  Master Data Mapping      │
│      ⬆️  Upload                  │      📊 ERP Catalog               │
│      📋 Review Queue             │      📋 LicenseIQ Schema          │
│                                  │      💾 Master Data               │
│                                  │      📥 ERP Data Import           │
│  ─────────────────────────────   │                                   │
│                                  │  ─────────────────────────────    │
│  3️⃣  AI & INTELLIGENCE           │                                   │
│      🧠 Brain                    │  6️⃣  ADMINISTRATION              │
│      Key: ai_intelligence        │      👥 Users                     │
│      ─────────────────────────   │      Key: administration          │
│      🤖 liQ AI                   │      ─────────────────────────    │
│      📊 RAG Dashboard            │      👥 User Management           │
│                                  │      📜 Audit Trail               │
│                                  │      📧 Lead Management           │
│                                  │      ⚙️  Configuration            │
│                                  │      🧭 Navigation Manager        │
│                                  │                                   │
└──────────────────────────────────┴──────────────────────────────────┘
```

---

## 📋 Category Cards Display Order

### As Seen on Screen (Grid View):

**Row 1 (Categories 1-2):**
- **Column 1**: Dashboard & Analytics
- **Column 2**: Contract Management

**Row 2 (Categories 3-4):**
- **Column 1**: AI & Intelligence  
- **Column 2**: License Fee Management

**Row 3 (Categories 5-6):**
- **Column 1**: Data Management
- **Column 2**: Administration

---

## 🎨 Visual Category Card Example

Each category appears as a card:

```
┌────────────────────────────────────────────┐
│  📊 Dashboard & Analytics         [✏️] [🗑️] │
│  Key: dashboard_analytics                  │
├────────────────────────────────────────────┤
│                                            │
│  ⋮⋮ 📍 Dashboard                          │
│     /dashboard                             │
│                                            │
│  ⋮⋮ 📈 Analytics                          │
│     /analytics                             │
│                                            │
│  ⋮⋮ 📊 Reports                            │
│     /reports                               │
│                                            │
└────────────────────────────────────────────┘
```

**Card Elements:**
- **Header**: Category name with icon
- **Edit/Delete buttons**: Top-right corner
- **Navigation Items**: Draggable list with grip handles (⋮⋮)
- **Each Item Shows**: Icon, name, and route path

---

## 🔢 Numbered List View (Order 1-6)

1. **Dashboard & Analytics** 📊
   - Display Order: 1
   - Default: Expanded
   - Items: Dashboard, Analytics, Reports

2. **Contract Management** 📄
   - Display Order: 2
   - Default: Expanded
   - Items: Contracts, Upload, Review Queue

3. **AI & Intelligence** 🧠
   - Display Order: 3
   - Default: Expanded
   - Items: liQ AI, RAG Dashboard

4. **License Fee Management** 🧮
   - Display Order: 4
   - Default: Collapsed
   - Items: License Fee Calculator, Sales Data

5. **Data Management** 💾
   - Display Order: 5
   - Default: Collapsed
   - Items: Master Data Mapping, ERP Catalog, LicenseIQ Schema, Master Data, ERP Data Import

6. **Administration** 👥
   - Display Order: 6
   - Default: Collapsed
   - Items: User Management, Audit Trail, Lead Management, Configuration, Navigation Manager

---

## 🎯 How Categories are Sorted in the UI

### Frontend Sorting Logic:
```typescript
// Categories are fetched and sorted by defaultSortOrder
categories.sort((a, b) => a.defaultSortOrder - b.defaultSortOrder)

// Then displayed in a 2-column grid:
// Grid auto-fills: [1, 2] then [3, 4] then [5, 6]
```

### Grid Layout CSS:
```css
grid-template-columns: repeat(2, 1fr);  /* 2 columns */
gap: 1.5rem;                             /* Space between cards */
```

---

## 📱 Responsive Behavior

### Desktop (Large Screens):
- **2 columns** side-by-side
- Categories flow: [1,2], [3,4], [5,6]

### Tablet/Mobile:
- **1 column** stacked vertically
- Categories flow: 1, 2, 3, 4, 5, 6 (top to bottom)

---

## 🎨 UI Components

### Action Buttons (Top):
```
[💾 Save Changes]        - Saves drag-drop changes to database
[🔄 Reset to Defaults]   - Reverts to system default order
[➕ New Category]        - Creates a new category
```

### Category Card Actions:
```
[✏️ Edit]    - Modify category name, icon, settings
[🗑️ Delete]  - Remove category (coming soon)
```

### Drag Handles:
```
⋮⋮  - Grab and drag to reorder items
    - Works within category or between categories
```

---

## 🔄 How to Reorder (User Actions)

1. **Click and hold** the drag handle (⋮⋮) on any navigation item
2. **Drag** the item to a new position
3. **Drop** it:
   - Within the same category (reorder)
   - Into a different category (move to new category)
   - Into an empty category
4. **Click "Save Changes"** to persist your new layout

---

## 💡 Quick Reference

| Position | Category | Icon | Collapsed? |
|----------|----------|------|------------|
| **1** | Dashboard & Analytics | 📊 | No (Expanded) |
| **2** | Contract Management | 📄 | No (Expanded) |
| **3** | AI & Intelligence | 🧠 | No (Expanded) |
| **4** | License Fee Management | 🧮 | Yes (Collapsed) |
| **5** | Data Management | 💾 | Yes (Collapsed) |
| **6** | Administration | 👥 | Yes (Collapsed) |

---

**This is what you see in the Navigation Manager UI!**

To change this order:
- Use the drag-and-drop interface
- OR run SQL to update `default_sort_order` in the database

---

**Last Updated:** 2025-11-24  
**Document Type:** Frontend UI Reference
