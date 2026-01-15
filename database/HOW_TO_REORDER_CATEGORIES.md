# How to Reorder Navigation Categories

**🔐 Admin/Owner Only Feature**

You can now **drag and drop category cards** in the Navigation Manager to change their display order!

**Important:** Only users with **Admin** or **Owner** roles can reorder categories. Regular users can still reorder items within categories.

---

## 🎯 Quick Start Guide

### Step 1: Open Navigation Manager
1. Login to LicenseIQ with an **Admin or Owner** account
2. Navigate to **Administration** → **Navigation Manager**

### Step 2: Drag Categories to Reorder
1. **Hover** over a category card
2. **Click and hold** the **⋮⋮ grip handle** (left side of category name)
3. **Drag** the category card to a new position
4. **Drop** it where you want it

### Step 3: Save Your Changes
1. Click **💾 Save Changes** button at the top
2. The new order will be saved to the database
3. All users will see the new category order

---

## 📊 Visual Guide

### Before - Default Order:
```
┌────────────────────┐  ┌────────────────────┐
│ 1. Dashboard       │  │ 4. License Fee     │
└────────────────────┘  └────────────────────┘

┌────────────────────┐  ┌────────────────────┐
│ 2. Contract Mgmt   │  │ 5. Data Mgmt       │
└────────────────────┘  └────────────────────┘

┌────────────────────┐  ┌────────────────────┐
│ 3. AI & Intel      │  │ 6. Administration  │
└────────────────────┘  └────────────────────┘
```

### After - Your Custom Order:
```
┌────────────────────┐  ┌────────────────────┐
│ 1. Dashboard       │  │ 4. Data Mgmt       │
└────────────────────┘  └────────────────────┘

┌────────────────────┐  ┌────────────────────┐
│ 2. Administration  │  │ 5. License Fee     │
└────────────────────┘  └────────────────────┘

┌────────────────────┐  ┌────────────────────┐
│ 3. Contract Mgmt   │  │ 6. AI & Intel      │
└────────────────────┘  └────────────────────┘
```

---

## 🔍 Finding the Grip Handle

Each category card has a **drag handle** on the left side:

```
┌─────────────────────────────────────────────┐
│  ⋮⋮  📊 Dashboard & Analytics    [✏️] [🗑️]  │ ← Grab here!
│     Key: dashboard_analytics                │
├─────────────────────────────────────────────┤
│  • Dashboard                                │
│  • Analytics                                │
│  • Reports                                  │
└─────────────────────────────────────────────┘
```

**⋮⋮** = Drag handle (appears on hover)

---

## ⚡ Pro Tips

### 1. Cursor Changes
- **Hover over grip handle**: Cursor changes to ✋ (grab)
- **While dragging**: Cursor changes to ✊ (grabbing)

### 2. Visual Feedback
- **Dragging category**: Card becomes semi-transparent
- **Drop zones**: Other categories shift to show where it will go

### 3. Category vs Items
- **Drag the grip handle on the category header** = Reorder entire category
- **Drag the grip handle on individual items** = Move items between categories

### 4. Grid Layout
- Categories display in a **2-column grid**
- Order flows: [1,2], [3,4], [5,6] (top-to-bottom, left-to-right)
- On mobile: Stacks vertically 1→6

---

## 💾 Saving Changes

**IMPORTANT:** Changes are **NOT saved automatically**!

You must click **💾 Save Changes** to persist your new order.

### What Gets Saved:
✅ Category display order (position 1-6)  
✅ Navigation item assignments to categories  
✅ Item order within each category  

### What Happens After Save:
1. Changes saved to database
2. Sidebar navigation updates immediately
3. **All users see the new order** (system-wide change)

---

## 🔄 Reset to Defaults

Don't like your changes? Click **🔄 Reset to Defaults** to restore:

**Default Category Order:**
1. Dashboard & Analytics
2. Contract Management
3. AI & Intelligence
4. License Fee Management
5. Data Management
6. Administration

---

## 📱 Mobile/Tablet

On smaller screens:
- Categories stack **vertically** (1 column)
- Touch and drag works the same way
- Easier to see drag-and-drop feedback

---

## ❓ Frequently Asked Questions

### Q: Can I reorder categories?
**A:** Yes! Drag the **⋮⋮ grip handle** on the category card header.

### Q: Can I reorder items within categories?
**A:** Yes! Drag the **⋮⋮ grip handle** on individual navigation items.

### Q: Can I move items between categories?
**A:** Yes! Drag an item from one category and drop it into another.

### Q: Are changes saved automatically?
**A:** No, you must click **💾 Save Changes**.

### Q: Who can reorder categories?
**A:** Only users with **Admin** or **Owner** roles can reorder categories (system-wide change). Regular users can reorder items within categories.

### Q: Do changes affect other users?
**A:** Yes! Category order is system-wide and affects all users.

### Q: Can I undo changes?
**A:** Yes, click **🔄 Reset to Defaults** before saving.

### Q: What if I save by mistake?
**A:** Click **🔄 Reset to Defaults** to restore default order.

---

## 🎯 Common Use Cases

### Make Administration More Prominent
Drag **Administration** to position 2 (after Dashboard):
1. Dashboard & Analytics
2. **Administration** ← Moved up
3. Contract Management
4. AI & Intelligence
5. License Fee Management
6. Data Management

### Group Core Business Functions Together
1. Dashboard & Analytics
2. Contract Management ← Together
3. License Fee Management ← Together
4. AI & Intelligence
5. Data Management
6. Administration

### Put Frequently Used Categories First
Arrange based on your team's workflow!

---

## 🛠️ Technical Details

### Where is the order stored?
- Database table: `navigation_categories`
- Field: `default_sort_order` (1, 2, 3, 4, 5, 6)

### What happens when I save?
- Updates `default_sort_order` for each category
- Changes take effect immediately
- Sidebar reflects new order across the app

### Can different users have different orders?
- Currently: **No** (system-wide setting)
- Future: Per-user customization may be added

---

## ✅ Success!

You're now ready to customize your navigation category order!

**Quick Reminder:**
1. Drag **⋮⋮** grip handle on category cards
2. Drop in new position
3. Click **💾 Save Changes**
4. See new order in sidebar immediately!

---

**Happy organizing! 🎉**
