# LicenseIQ Platform - Customer Demonstration Guide

## 📋 Overview
This guide provides complete instructions for demonstrating the LicenseIQ AI-powered contract analysis and license fee calculation platform using three different contract types.

---

## 🎯 Three Contract Scenarios

### 1. Electronics Patent License (High-Tech Manufacturing)
**Contract File:** `Electronics Patent License & Component Royalty Agreement_1762888473402.pdf`
**Sales Data:** `sample_sales_data/electronics_license_sales_sample.csv`

#### Contract Characteristics:
- **Industry:** Semiconductor & Electronics Manufacturing
- **License Type:** Non-Exclusive Component Technology Rights
- **Complexity:** Multi-tier royalty structure with 3 different calculation methods
- **Annual Minimum:** $2,500,000

#### Royalty Calculation Rules:
1. **Tier 1 - Per-Unit Pricing:** Component-based (ARM Processors, Power ICs, Memory Controllers, etc.)
   - Base rates: $1.75 - $6.75 per unit
   - Volume discounts at 250K - 2M units annually
   
2. **Tier 2 - Percentage of ASP:** Consumer electronics (Tablets, Laptops, Smart Home, Wearables)
   - Royalty rates: 1.8% - 4.1% of Average Selling Price
   - Min/Max caps per unit ($1.25 - $35.00)
   
3. **Tier 3 - Premium Applications:** Specialized sectors (Automotive, Medical, Industrial, Server)
   - Base rate: 1.5% - 2.5%
   - Premium multipliers: 1.10x - 1.30x

4. **Geographic Adjustments:**
   - Tier 2 Markets (EU/UK/Australia): +10% premium
   - Tier 3 Markets (Japan/Korea/Taiwan): +20% premium

#### Sample Sales Data (20 transactions):
- Mix of all three tiers and product categories
- Multiple territories (US, EU, Japan, South Korea)
- Volume ranges from 8,000 to 2,500,000 units
- **Expected Total Sales:** ~$250M+
- **Expected License Fees:** ~$12M - $15M (depending on AI extraction accuracy)

---

### 2. Plant Variety License (Agricultural/Nursery)
**Contract File:** `Plant Variety License & Royalty Agreement_1762888473403.pdf`
**Sales Data:** `sample_sales_data/plant_variety_license_sales_sample.csv`

#### Contract Characteristics:
- **Industry:** Agricultural Biotechnology / Ornamental Horticulture
- **License Type:** Exclusive Regional Plant Variety Rights
- **Complexity:** Container size + seasonal adjustments + premium multipliers
- **Annual Minimum:** $85,000

#### Royalty Calculation Rules:
1. **Tier 1 - Trees & Shrubs:** Container size-based pricing
   - 1-gallon: $1.25 (discounted to $1.10 at 5,000+ units)
   - 3-gallon: $2.85 (discounted to $2.50 at 2,000+ units)
   - 5-gallon: $4.50 (discounted to $3.95 at 1,000+ units)
   - 15-gallon specimens: $12.75 (discounted to $11.25 at 200+ units)

2. **Tier 2 - Perennials & Roses:** Base rate + premium multiplier
   - 4-inch pots: $0.75 base
   - 6-inch pots: $1.15 base (1.2x multiplier for premium roses)
   - 1-gallon: $1.85 base (1.3x multiplier for specimen grade)
   - 2-gallon: $3.25 base (1.5x multiplier for mature plants)

3. **Tier 3 - Flowering Shrubs:** Volume-tiered rates
   - 1-2,500 units: $2.25/unit
   - 2,501-7,500 units: $1.95/unit
   - 7,501-15,000 units: $1.70/unit
   - 15,001+ units: $1.45/unit

4. **Seasonal Adjustments:**
   - Spring (Mar-May): +10-15% premium
   - Fall (Sep-Nov): -5% discount
   - Holiday (December): +20% premium
   - Summer/Off-season: Standard rates

5. **Territory Adjustments:**
   - Primary Territory (OR/WA/N.CA/ID): Standard rates
   - Secondary Territory (MT/WY/UT/NV): Premium rates apply

#### Sample Sales Data (24 transactions):
- All 5 plant varieties (Aurora Flame Maple, Pacific Sunset Rose, Emerald Crown Hosta, Cascade Blue Hydrangea, Golden Spire Juniper)
- Multiple container sizes (4-inch to 15-gallon specimens)
- Seasonal variation (Spring, Summer, Fall, Holiday)
- Volume ranges from 250 to 12,000 units per transaction
- **Expected Total Sales:** ~$1.8M
- **Expected License Fees:** ~$140K - $160K

---

### 3. Manufacturing Technology License (Industrial B2B)
**Contract File:** `Technology License & Royalty Agreement - Manufacturing_1762888473403.pdf`
**Sales Data:** `sample_sales_data/manufacturing_license_sales_sample.csv`

#### Contract Characteristics:
- **Industry:** Advanced Manufacturing / Industrial Components
- **License Type:** Exclusive Manufacturing Rights
- **Complexity:** Percentage-based with net sales deductions + tiered rates
- **Annual Minimum:** $125,000 - $500,000 (based on volume tier)

#### Royalty Calculation Rules:
1. **Tier 1 - Automotive Components:** Volume-based percentage
   - $0-$5M net sales: 6.5% royalty (min $125K annual)
   - $5M-$15M net sales: 5.8% royalty (min $200K annual)
   - $15M-$50M net sales: 5.2% royalty (min $350K annual)
   - $50M+ net sales: 4.8% royalty (min $500K annual)

2. **Tier 2 - Industrial & Aerospace:** Premium rates
   - Industrial applications: 7.2% of net sales
   - Aerospace/High-Performance: 8.5% (1.18x multiplier)
   - Custom engineering projects: 9.8% + $15K engineering fee

3. **Net Sales Calculation Formula:**
   ```
   Net Sales = Gross Sales 
               - Freight Costs
               - Sales Tax
               - Returns/Credits
               - Trade Discounts (max 8%)
               - Distributor Commissions (max 12%)
   ```

4. **Geographic Coverage:**
   - Manufacturing: US, Canada, Mexico
   - Distribution: All North & South America

#### Sample Sales Data (20 transactions):
- Mix of Automotive, Industrial, and Aerospace components
- Multiple territories (US, Canada, Mexico)
- Includes all net sales deductions (freight, returns, discounts, commissions)
- Volume ranges from $2.8M to $7.7M per transaction (gross sales)
- **Expected Gross Sales:** ~$105M
- **Expected Net Sales:** ~$90M (after deductions)
- **Expected License Fees:** ~$5.5M - $6.5M

---

## 🚀 Step-by-Step Demonstration Instructions

### Phase 1: Upload and AI Analysis (All Three Contracts)

#### For Each Contract:

1. **Upload Contract PDF**
   - Navigate to "Contracts" page
   - Click "Upload New Contract"
   - Select the contract PDF file
   - Wait for initial processing

2. **Trigger AI Extraction**
   - Open the contract details page
   - Click "Reprocess" button to trigger AI analysis
   - Wait for AI extraction to complete (30-60 seconds)
   - Verify AI extraction status shows "completed"

3. **Review AI-Extracted Rules**
   - The platform should automatically extract:
     - Contract parties (Licensor/Licensee)
     - License type and scope
     - Territory definitions
     - Royalty calculation rules
     - Minimum guarantees
     - Payment terms

4. **Key Success Indicators:**
   - ✅ Contract type correctly identified
   - ✅ Royalty rules extracted with 85%+ confidence
   - ✅ Multiple calculation tiers detected
   - ✅ Territory adjustments captured
   - ✅ Minimum annual guarantees identified

---

### Phase 2: Upload Sales Data

#### For Each Contract:

1. **Navigate to License Fee Calculator**
   - Click "License Fee Dashboard" button on contract page
   - Or access via navigation menu

2. **Upload Sales Data CSV**
   - Click "Upload Sales Data" button
   - Select the corresponding sales CSV file:
     - Electronics → `electronics_license_sales_sample.csv`
     - Plant Variety → `plant_variety_license_sales_sample.csv`
     - Manufacturing → `manufacturing_license_sales_sample.csv`
   - Wait for upload and processing

3. **Verify Data Import**
   - Check transaction count matches CSV rows
   - Review sales summary statistics
   - Verify all product categories imported correctly

---

### Phase 3: Run License Fee Calculations

#### For Each Contract:

1. **Execute Calculation**
   - Click "Calculate License Fees" button
   - Enter calculation name (e.g., "Q1 2024 Electronics Royalties")
   - Optionally set date range filters
   - Click "Run Calculation"

2. **Review Calculation Results**
   - **Total Sales Amount:** Verify against expected totals
   - **Total License Fees:** Compare to expected range
   - **Transaction Breakdown:** Review per-item calculations
   - **Charts & Visualizations:** 
     - Sales vs License Fee comparison
     - Top products by royalty contribution
     - Revenue distribution

3. **Validate Calculations:**

   **Electronics Contract - Key Validations:**
   - ARM Processors (1.2M units): Should apply volume discount ($2.85 vs $3.25)
   - Power ICs (2.5M units): Should apply volume discount ($1.45 vs $1.75)
   - Tablets/Laptops: Should calculate 2.5% and 1.8% of ASP with min/max caps
   - Automotive/Medical: Should apply 15-25% premium multipliers
   - EU/Japan sales: Should apply 10-20% geographic premiums

   **Plant Variety Contract - Key Validations:**
   - Aurora Maple 1-gal (6,500 units): Volume discount to $1.10
   - Spring sales: Should include +10-15% seasonal premium
   - Fall sales: Should include -5% seasonal discount
   - December sales: Should include +20% holiday premium
   - Premium roses (6-inch): Should apply 1.2x multiplier

   **Manufacturing Contract - Key Validations:**
   - Net sales deductions: Freight, tax, returns, discounts, commissions
   - Volume tiers: Rates should decrease as cumulative sales increase
   - Aerospace products: Should apply 8.5% rate (vs 7.2% industrial)
   - Minimum guarantee: Should enforce if calculated royalty < minimum

4. **Download Invoices**
   - Click "Download Detailed Invoice" (itemized breakdown)
   - Click "Download Summary Invoice" (executive summary)
   - Verify PDF formatting and content accuracy

---

### Phase 4: Demonstrate Key Features

#### 1. Dynamic Rule Engine
- **Show:** Different calculation methods working in same system
  - Per-unit pricing (Electronics Tier 1, Plant Variety)
  - Percentage of sales (Electronics Tier 2, Manufacturing)
  - Volume-tiered rates (Electronics Tier 3, Plant Variety Tier 3, Manufacturing Tier 1)
  - Premium multipliers (all three contracts)
  - Geographic adjustments (Electronics, Plant Variety)

#### 2. AI-Powered Extraction
- **Show:** Zero manual rule configuration needed
  - Upload contract → AI extracts rules automatically
  - Complex multi-tier structures detected
  - Territory definitions captured
  - Seasonal/temporal adjustments identified

#### 3. Calculation Accuracy
- **Show:** 100% accurate calculations across:
  - Simple per-unit pricing
  - Complex percentage calculations with caps
  - Net sales deductions (Manufacturing)
  - Volume discount thresholds
  - Seasonal and geographic premiums
  - Minimum guarantee enforcement

#### 4. Multi-Contract Management
- **Show:** Platform handles diverse contract types:
  - High-tech manufacturing (Electronics)
  - Agricultural/biological (Plant Variety)
  - Industrial B2B (Manufacturing)
  - Each with completely different royalty structures

#### 5. Compliance & Auditability
- **Show:** Complete audit trail:
  - AI extraction runs with timestamps
  - Calculation history with versions
  - Detailed transaction-level breakdowns
  - PDF invoices for payment processing

---

## 📊 Expected Results Summary

### Electronics Patent License
| Metric | Expected Value |
|--------|----------------|
| Total Transactions | 20 |
| Total Sales Amount | ~$250M |
| Total License Fees | $12M - $15M |
| Average Fee per Transaction | $600K - $750K |
| Calculation Complexity | High (3 tiers, geographic adjustments) |

### Plant Variety License
| Metric | Expected Value |
|--------|----------------|
| Total Transactions | 24 |
| Total Sales Amount | ~$1.8M |
| Total License Fees | $140K - $160K |
| Average Fee per Transaction | $6K - $7K |
| Calculation Complexity | Medium (seasonal adjustments, volume discounts) |

### Manufacturing Technology License
| Metric | Expected Value |
|--------|----------------|
| Total Transactions | 20 |
| Gross Sales Amount | ~$105M |
| Net Sales Amount | ~$90M |
| Total License Fees | $5.5M - $6.5M |
| Average Fee per Transaction | $275K - $325K |
| Calculation Complexity | High (net sales deductions, tiered percentages) |

---

## 🎓 Demonstration Talking Points

### Key Value Propositions to Highlight:

1. **AI-Native Architecture**
   - "No manual rule configuration needed - our AI reads your contract and automatically extracts royalty calculation rules"
   - "Works with any contract type - electronics, agriculture, manufacturing, software, media, etc."

2. **100% Calculation Accuracy**
   - "Every calculation auditable down to individual transaction level"
   - "Handles complex multi-tier structures that would take hours manually"
   - "Automatically applies volume discounts, seasonal adjustments, and geographic premiums"

3. **Time Savings**
   - "Upload contract → Upload sales data → Calculate = 5 minutes vs. days of manual work"
   - "Process thousands of transactions instantly"
   - "Generate professional invoices automatically"

4. **Revenue Assurance**
   - "Eliminates calculation errors that cost companies millions"
   - "Ensures minimum guarantees are enforced"
   - "Catches missed royalty opportunities from volume discounts and premiums"

5. **Scalability**
   - "Manage hundreds of contracts with different royalty structures"
   - "Process millions of transactions per month"
   - "Works across all industries and contract types"

---

## ⚠️ Troubleshooting

### If AI Extraction Doesn't Work:
1. Verify contract PDF has text (not scanned image)
2. Click "Reprocess" button to retry
3. Check AI extraction confidence scores (should be >80%)

### If Sales Data Upload Fails:
1. Verify CSV format matches expected column headers
2. Check for special characters or encoding issues
3. Ensure dates are in YYYY-MM-DD format

### If Calculations Don't Match Expected:
1. Review AI-extracted rules for accuracy
2. Check if all product categories mapped correctly
3. Verify volume thresholds and discount tiers
4. Review seasonal/geographic adjustment logic

---

## 📞 Support

For issues during demonstration:
- Check logs in browser developer console
- Review backend API responses for error messages
- Verify database connection and data integrity
- Contact technical support with contract ID and error details

---

*Last Updated: November 11, 2025*
*LicenseIQ Platform Version: 2.0*
