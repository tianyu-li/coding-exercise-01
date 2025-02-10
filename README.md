# 📊 Coding Exercise 01

---

## 1️⃣ Review Existing Unstructured Data and Diagram a New Structured Relational Data Model

### **Setup Instructions:**

```bash
cd data_pipeline
pip install -r requirements.txt
python3 pipeline.py
```

### **Set Up Table Schema for Data Loading:**

```bash
bq show --format=prettyjson tli-sample-01:fetch.receipts | jq '.schema.fields' >> 'schema/receipts.sql'
bq mk --table tli-sample-01:fetch.receipts 'schema/receipts.sql'

bq show --format=prettyjson tli-sample-01:fetch.users | jq '.schema.fields' >> 'schema/users.sql'
bq mk --table tli-sample-01:fetch.users 'schema/users.sql'

bq show --format=prettyjson tli-sample-01:fetch.brands_temp | jq '.schema.fields' >> 'schema/brands.sql'
bq mk --table tli-sample-01:fetch.brands 'schema/brand.sql'
```

---

## 2️⃣ Write Queries to Answer Business Questions

### **What are the top 5 brands by receipts scanned for most recent month?**

```sql
SELECT ri.brand_code, COUNT(DISTINCT r.receipt_id) AS receipt_scanned
FROM `tli-sample-01.fetch.vw_receipt` r
INNER JOIN `tli-sample-01.fetch.vw_dim_date` d ON r.date_scanned = d.date
INNER JOIN `tli-sample-01.fetch.vw_receipt_item` ri USING (receipt_id)
WHERE 
  -- used earlier month for testing, should be updated to use CURRENT_DATE 
  -- d.month_start_date=DATE_TRUNC(CURRENT_DATE(), MONTH)
  d.month_start_date = DATE_TRUNC('2021-01-13', MONTH)
  AND ri.brand_code IS NOT NULL
GROUP BY ri.brand_code
ORDER BY receipt_scanned DESC
LIMIT 5;

/*
  A generic approach also support Month Over Month Compare
*/

WITH top_scanned_brand_by_month AS (
  SELECT d.month_start_date, ri.brand_code, COUNT(DISTINCT r.receipt_id) AS receipt_scanned
  FROM `tli-sample-01.fetch.vw_receipt` r
  INNER JOIN `tli-sample-01.fetch.vw_dim_date` d ON r.date_scanned = d.date
  INNER JOIN `tli-sample-01.fetch.vw_receipt_item` ri USING (receipt_id)
  WHERE ri.brand_code IS NOT NULL
  GROUP BY d.month_start_date, ri.brand_code
)
SELECT brand_code, receipt_scanned
FROM top_scanned_brand_by_month
WHERE 
  -- month_start_date=DATE_TRUNC(CURRENT_DATE(), MONTH)
  month_start_date = DATE_TRUNC('2021-01-13', MONTH)  
ORDER BY receipt_scanned DESC
LIMIT 5;

```

### **How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?**

```sql
WITH top_scanned_brand_by_month AS (
  SELECT d.month_start_date, ri.brand_code, COUNT(DISTINCT r.receipt_id) AS receipt_scanned
  FROM `tli-sample-01.fetch.vw_receipt` r
  INNER JOIN `tli-sample-01.fetch.vw_dim_date` d ON r.date_scanned = d.date
  INNER JOIN `tli-sample-01.fetch.vw_receipt_item` ri USING (receipt_id)
  WHERE ri.brand_code IS NOT NULL
  GROUP BY d.month_start_date, ri.brand_code
),
mom_comparison AS (
  SELECT month_start_date, brand_code, receipt_scanned,
    LAG(receipt_scanned) OVER (PARTITION BY brand_code ORDER BY month_start_date) AS previous_month_receipt_scanned
  FROM top_scanned_brand_by_month
)
SELECT brand_code, receipt_scanned, previous_month_receipt_scanned,
  (receipt_scanned - previous_month_receipt_scanned) AS receipt_scanned_change,
  ROUND(SAFE_DIVIDE(receipt_scanned - previous_month_receipt_scanned, previous_month_receipt_scanned) * 100, 2) AS receipt_scanned_change_percent
FROM mom_comparison
WHERE 
  -- month_start_date = DATE_TRUNC(CURRENT_DATE(), MONTH) -- Use this for the current month
  month_start_date = DATE_TRUNC('2021-02-13', MONTH)
ORDER BY receipt_scanned DESC
LIMIT 5;
```

### **Which brand has the most transactions among users who were created within the past 6 months?**

```sql
WITH recent_user_receipts AS (
  SELECT r.user_id, r.receipt_id
  FROM `tli-sample-01.fetch.vw_receipt` r
  INNER JOIN `tli-sample-01.fetch.vw_user` u ON r.user_id = u.user_id
  WHERE DATE(u.created_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
)
SELECT ri.brand_code, COUNT(DISTINCT receipt_id) AS transaction_counts
FROM recent_user_receipts r 
INNER JOIN `tli-sample-01.fetch.vw_receipt_item` ri USING(receipt_id)
WHERE ri.brand_code IS NOT NULL
GROUP BY ri.brand_code
ORDER BY transaction_counts DESC;
```

---

## 3️⃣ Evaluate Data Quality Issues

### **Issues Identified:**

- Missing `last_login` values in the `users` table for some records
- `signUpSource` exists in the data file but is not in the schema provided.
- Inconsistent casing for `role` values (schema uses uppercase, data uses lowercase), is this field case sensitive?
- Duplicate `user_id` values detected:

```sql
SELECT user_id, COUNT(*) 
FROM `tli-sample-01.fetch.vw_user`
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
```

- **Receipts:** Inconsistent `purchase_date` (before `create_date` or after `modify_date`):

```sql
SELECT *
FROM (
  SELECT GREATEST(
    COALESCE(create_date, DATE '1900-01-01'),
    COALESCE(modify_date, DATE '1900-01-01'),
    COALESCE(finished_date, DATE '1900-01-01'),
    COALESCE(purchase_date, DATE '1900-01-01'),
    COALESCE(date_scanned, DATE '1900-01-01')
  ) AS latest_date, modify_date, * EXCEPT(modify_date)
  FROM `tli-sample-01.fetch.vw_receipt`
)
WHERE latest_date > modify_date;
```

- **Brands:** Missing valid `brand_code` and/or `barcode` values.

---

## 4️⃣ Communicate with Stakeholders

### **Email/Slack Message Draft**

**Subject:** Data Quality Findings & Next Steps for Optimization

Hi ,

I wanted to share an update on the recent data analysis we conducted and highlight a few key data quality issues that could impact our reporting accuracy and decision-making.

### **Key Data Quality Findings:**

1. **Inconsistent Dates:**  
   Some `purchase_date` values are way earlier than the `create_date`, and a few appear way after the `modify_date`, which suggests potential data entry errors or system issues.

2. **Missing and Invalid Values:**  
   Number of records are missing valid `brand_code` and/or `barcode` values, which could affect any brand-level reporting or analysis.

3. **User Data Inconsistencies:**  
   - Missing `last_login` data for several users, making it difficult to assess user engagement accurately.
   - The data file includes a `signUpSource` field that’s not part of the official schema, raising questions about schema alignment.
   - The `role` field has inconsistent casing (upper vs. lower case), which may cause discrepancies in role-based reporting.
   - Duplicate user IDs (e.g., `'$oid=600056a3f7e5b011fce897b0'`) suggest potential issues with user record management.

4. **Deduplication Gaps:**  
   The current ELT process doesn’t deduplicate records, which was not implemented in this exercise but should be addressed in a follow-up ticket to ensure data integrity.

### **Questions to Help Resolve These Issues:**

- **Date Inconsistencies:**  
  What are the expected business rules for date sequences (e.g., should `purchase_date` always be after `create_date`)?

- **Missing `brand_code`/`barcode`:**  
  Are there fallback rules or alternative data sources we can reference to fill these gaps?

- **User Data Concerns:**  
  - Should `signUpSource` be officially added to the schema, or is it a legacy artifact?
  - Are there guidelines on handling duplicate user IDs, or should we consider a deduplication strategy based on other attributes?

### **What We Need to Optimize the Data Assets:**

- **Business Context:** Understanding how these fields are used in decision-making will help us prioritize fixes.
- **Data Ownership:** Clarification on who owns the source systems for these data points, so we can work with the right teams to address root causes.
- **Historical Data:** Access to historical data trends could help identify when these issues started and whether they’re systemic or isolated incidents.

### **Performance & Scaling Considerations for Production:**

- **Deduplication at Scale:** As data volume grows, deduplication logic will need to be optimized to handle large datasets efficiently, potentially leveraging partitioning strategies.
- **Real-time Data Integrity:** If we move toward more real-time data processing, we’ll need robust validation rules to catch these issues early without impacting performance.
- **Schema Evolution:** Handling schema changes gracefully, especially with new fields like `signUpSource`, to prevent breaking downstream systems.

Let me know if you'd like to discuss these points further or if there’s additional context that could help us address these issues effectively.

Best regards,  

---
