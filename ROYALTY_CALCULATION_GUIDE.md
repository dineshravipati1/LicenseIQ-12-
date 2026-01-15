# How Royalty Calculation Works in License IQ

## 📋 Overview

The system automatically calculates how much money (royalties) you owe to licensors based on your sales data and the rules extracted from your contracts.

---

## 🔄 Complete Workflow

### **Step 1: Contract Upload & AI Analysis**
1. You upload a license contract (PDF)
2. The AI (Groq LLaMA) reads the contract and extracts:
   - Product categories (e.g., "Ornamental Trees & Shrubs")
   - Royalty rates and formulas
   - Volume tiers (e.g., "5% for first 10,000 units, 7% after")
   - Seasonal adjustments (e.g., "20% bonus in spring")
   - Territory premiums (e.g., "15% extra for Europe")
   - Minimum guarantees

### **Step 2: Formula Storage**
- The AI converts contract rules into **formula definitions** (structured JSON)
- Each rule becomes a formula with conditions and calculations
- Examples:
  - Simple: `baseRate × units`
  - Complex: Volume tiers + seasonal multipliers + territory premiums

### **Step 3: Sales Data Upload**
1. You upload sales data (CSV/Excel file)
2. Each row contains:
   - Product name and category
   - Quantity sold
   - Sale date
   - Territory
   - Gross amount

### **Step 4: Smart Matching (Word-Based)**
For each sale, the system finds the matching contract rule:
- **Category Matching:** Uses word-based overlap
  - Example: "Ornamental Shrubs" matches "Ornamental Trees & Shrubs"
  - Prevents tier mismatches: "Tier 1 Shrubs" ≠ "Tier 2 Shrubs"
- **Territory Matching:** Checks if sale territory matches rule territory
- **Date Filtering:** Only includes sales within your chosen date range

### **Step 5: Formula Preview** ✨
**BEFORE** you run the calculation, you see:
- ✅ **Matched Products (Green):** Products that will generate royalties
  - Shows which formula will apply
  - Shows confidence score
  - Shows sample units
- ❌ **Unmatched Products (Red):** Products that WON'T generate royalties
  - Highlights missing rules
  - Helps you identify gaps

### **Step 6: Royalty Calculation**
When you click "Calculate Royalties":

1. **For each matched sale:**
   - Evaluates the formula with sale context:
     - `units` = quantity sold
     - `season` = determined from sale date
     - `territory` = sale territory
     - `grossAmount` = sale amount
   
2. **Formula Evaluation Examples:**
   - **Volume Tiers:** 
     ```
     If units ≤ 10,000 → 5% × grossAmount
     If units > 10,000 → 7% × grossAmount
     ```
   - **Seasonal Multiplier:**
     ```
     baseRoyalty × 1.2 (if spring)
     baseRoyalty × 1.0 (if other)
     ```
   - **Territory Premium:**
     ```
     baseRoyalty × 1.15 (if Europe)
     baseRoyalty × 1.0 (if other)
     ```

3. **Aggregation:**
   - Sums all individual royalties
   - Applies minimum guarantee (if any)
   - Final royalty = max(calculated total, minimum guarantee)

### **Step 7: Results & Breakdown**
You get:
- **Total Royalty Amount**
- **Detailed Breakdown:**
  - Each product's royalty
  - Which rule was applied
  - Formula explanation
  - Rate used
  - Multipliers applied
- **Charts & Visualizations:**
  - Sales vs Royalty comparison
  - Revenue distribution
  - Product performance

### **Step 8: Calculation History**
- Every calculation is saved with:
  - Calculation name
  - Date range
  - Status (pending, approved, rejected)
  - Full breakdown details
- You can review past calculations anytime

---

## 🎯 Key Features

### **1. Dynamic Formulas**
- Formulas are stored as expression trees (JSON)
- Can handle complex logic:
  - Conditional statements (IF/THEN)
  - Mathematical operations (+, -, ×, ÷)
  - Lookups (volume tiers, seasonal rates)
  - Multiplier chains

### **2. Smart Matching**
- **Word-Based:** Handles variations in category names
- **Tier-Aware:** Prevents "Tier 1" from matching "Tier 2"
- **Flexible:** Allows extra descriptors like "Dwarf" or "Premium"

### **3. AI-Powered (100% FREE)**
- **Groq LLaMA:** Extracts rules from contracts
- **Hugging Face:** Generates embeddings for semantic search
- No usage limits for typical workloads

---

## 💡 Example Calculation

**Contract Rule:**
- Product: "Ornamental Trees & Shrubs"
- Base Rate: 5%
- Volume Tier: 7% after 10,000 units
- Spring Bonus: 20%

**Sale:**
- Product: "Ornamental Shrubs"
- Units: 15,000
- Season: Spring
- Gross Amount: $50,000

**Calculation:**
1. Match: "Ornamental Shrubs" → "Ornamental Trees & Shrubs" ✅
2. Volume tier: 15,000 > 10,000 → use 7% rate
3. Base royalty: $50,000 × 7% = $3,500
4. Seasonal multiplier: $3,500 × 1.2 (spring) = $4,200
5. **Final Royalty: $4,200**

---

## 📊 Technical Details

### **Formula Definition Structure**
Formulas are stored as JSON expression trees with the following node types:

- **Literal:** Fixed values (e.g., `0.05` for 5%)
- **Variable:** Dynamic values from context (e.g., `units`, `season`)
- **BinaryOp:** Mathematical operations (`+`, `-`, `*`, `/`)
- **Conditional:** IF/THEN/ELSE logic
- **Lookup:** Table lookups (volume tiers, seasonal rates)

Example formula JSON:
```json
{
  "type": "multiply",
  "operation": "*",
  "left": {
    "type": "lookup",
    "lookupType": "volumeTier",
    "variable": "units",
    "table": {
      "0": 0.05,
      "10000": 0.07
    }
  },
  "right": {
    "type": "variable",
    "name": "grossAmount"
  }
}
```

### **Category Matching Algorithm**
1. Extract words from both categories (filter stop words)
2. Check tier/grade number conflicts:
   - If only ONE has numbers → NO MATCH
   - If BOTH have numbers, they must match exactly
3. Require ≥1 shared meaningful category word
4. For single-word categories: require 100% match
5. For multi-word categories: require ≥2 shared words

**Matching Examples:**

| Sale Category | Rule Category | Match? | Reason |
|--------------|---------------|--------|--------|
| "Ornamental Shrubs" | "Ornamental Trees & Shrubs" | ✅ YES | Shares "ornamental" + "shrubs" |
| "Dwarf Ornamental Shrubs" | "Ornamental Trees & Shrubs" | ✅ YES | Shares "ornamental" + "shrubs" (extra descriptor OK) |
| "Shrubs" | "Tier 2 Shrubs" | ❌ NO | Tier mismatch (one has numbers, other doesn't) |
| "Tier 1 Shrubs" | "Tier 2 Shrubs" | ❌ NO | Different tier numbers (1 ≠ 2) |
| "Tier 1 Shrubs" | "Tier 1 Trees" | ❌ NO | Different category words (shrubs ≠ trees) |

---

## ❓ Common Questions

**Q: What if a product doesn't match any rule?**
A: It appears in the "Unmatched Products" section with a red warning. No royalty is calculated for it.

**Q: Can I see what will happen before calculating?**
A: Yes! The Formula Preview shows exactly which products will match and which formulas will apply.

**Q: What if the AI extracts the wrong category?**
A: You'll see it in the preview as an unmatched product. You can manually create or update rules to include it.

**Q: How accurate is the matching?**
A: Very accurate for similar categories. The word-based matching handles variations like "Trees & Shrubs" vs "Shrubs" correctly while preventing tier mismatches.

**Q: Can I modify the formulas extracted by AI?**
A: Yes! You can view and edit the formula definitions for each rule. The system uses a visual formula builder to make complex formulas easier to understand and modify.

**Q: What happens if I have multiple rules for the same product?**
A: The system uses rule priority. Rules are evaluated in priority order, and the first matching rule is applied to each sale.

**Q: Can I calculate royalties for a specific date range?**
A: Yes! When you trigger a calculation, you can specify a start and end date. Only sales within that period will be included.

**Q: Are calculations reversible?**
A: Calculations are saved as records and can be marked as "approved", "rejected", or "pending". You can review and adjust calculations before finalizing them.

---

## 🚀 Best Practices

1. **Review the Formula Preview** before running calculations to catch matching issues early
2. **Use descriptive calculation names** (e.g., "Q1 2024 Royalties - Spring Products")
3. **Set appropriate date ranges** to match your reporting periods
4. **Check unmatched products** and create rules for common patterns
5. **Verify tier/grade categories** match exactly between sales data and contract rules
6. **Review calculation breakdowns** to ensure formulas are evaluating correctly
7. **Maintain calculation history** for audit trails and trend analysis

---

## 📝 Notes

- All calculations are performed server-side using the `DynamicRulesEngine`
- Formula evaluation is handled by the `FormulaInterpreter` which supports complex expression trees
- The system maintains a complete audit trail of all calculations
- Formulas are validated before execution to prevent errors
- The matching algorithm is deterministic - the same sales data will always produce the same matches

---

*Last Updated: 2024*
*Version: 1.0*
