
/*
===============================================================================
SQL Script: Silver Layer Data Quality Checks
===============================================================================
Script Purpose:
    This comprehensive SQL script is meticulously designed to perform various data quality assessments on the
    transformed and standardized data residing within the 'silver' layer of the data warehouse. Its primary
    objective is to ensure the consistency, accuracy, completeness, and standardization of data, validating
    that the transformations applied during the silver layer loading process have been successful. The script
    executes a series of checks, including:
    - **Primary Key Integrity**: Verifying the absence of NULL values or duplicate entries in designated primary key columns.
    - **Data Cleanliness**: Identifying and flagging unwanted leading/trailing spaces in string-based fields.
    - **Data Standardization**: Confirming that categorical data (e.g., marital status, gender, product line, country)
      adheres to predefined standardized values.
    - **Temporal Integrity**: Checking for logical inconsistencies in date ranges (e.g., end date before start date,
      future dates for birthdates, or invalid date formats/ranges).
    - **Inter-Field Consistency**: Validating relationships between dependent fields (e.g., sales amount equaling quantity multiplied by price).

Usage Notes:
    - It is highly recommended to execute these quality checks immediately after successfully loading and
      transforming data into the 'silver' layer (e.g., after `EXEC silver.load_silver`).
    - Any results returned by these queries indicate data discrepancies or quality issues. Each discrepancy
      should be thoroughly investigated, and the underlying data transformation logic in the loading
      procedures (e.g., `bronze.load_bronze`, `silver.load_silver`) or source data should be
      reviewed and corrected to maintain data integrity.
    - The 'Expectation: No Results' comment implies that an ideal data state would yield no rows from
      the respective query, indicating full compliance with the quality rule.

===============================================================================
*/

USE DataWarehouse; -- Ensure the script operates within the 'DataWarehouse' context.
GO

-- ====================================================================
-- Data Quality Checks for 'silver.crm_cust_info'
-- This table contains standardized customer information from CRM sources.
-- ====================================================================

PRINT 'Performing checks for silver.crm_cust_info...';

-- Check 1.1: Identify NULLs or Duplicates in Primary Key (cst_id)
-- Purpose: Ensures the uniqueness and presence of values for the customer identifier.
-- Expectation: This query should return NO RESULTS if `cst_id` is a valid unique identifier.
SELECT
    cst_id,
    COUNT(*) AS RecordCount
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
PRINT '   - NULLs or Duplicates in cst_id check completed.';

-- Check 1.2: Identify Unwanted Leading/Trailing Spaces in cst_key
-- Purpose: Verifies that string fields are properly trimmed of whitespace.
-- Expectation: This query should return NO RESULTS, indicating clean string data.
SELECT
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);
PRINT '   - Unwanted Spaces in cst_key check completed.';

-- Check 1.3: Validate Data Standardization for cst_marital_status
-- Purpose: Confirms that marital status values conform to the predefined set ('Single', 'Married', 'n/a').
-- Expectation: Only 'Single', 'Married', and 'n/a' should be returned. Any other value indicates a standardization issue.
SELECT DISTINCT
    cst_marital_status
FROM silver.crm_cust_info;
PRINT '   - cst_marital_status Standardization check completed.';

-- Check 1.4: Validate Data Standardization for cst_gndr
-- Purpose: Confirms that gender values conform to the predefined set ('Female', 'Male', 'n/a').
-- Expectation: Only 'Female', 'Male', and 'n/a' should be returned. Any other value indicates a standardization issue.
SELECT DISTINCT
    cst_gndr
FROM silver.crm_cust_info;
PRINT '   - cst_gndr Standardization check completed.';
PRINT 'Checks for silver.crm_cust_info completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Data Quality Checks for 'silver.crm_prd_info'
-- This table contains standardized product information from CRM sources.
-- ====================================================================

PRINT 'Performing checks for silver.crm_prd_info...';

-- Check 2.1: Identify NULLs or Duplicates in Primary Key (prd_id)
-- Purpose: Ensures uniqueness and presence of values for the product identifier.
-- Expectation: This query should return NO RESULTS if `prd_id` is a valid unique identifier.
SELECT
    prd_id,
    COUNT(*) AS RecordCount
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
PRINT '   - NULLs or Duplicates in prd_id check completed.';

-- Check 2.2: Identify Unwanted Leading/Trailing Spaces in prd_nm
-- Purpose: Verifies that product names are properly trimmed of whitespace.
-- Expectation: This query should return NO RESULTS.
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
PRINT '   - Unwanted Spaces in prd_nm check completed.';

-- Check 2.3: Identify NULLs or Negative Values in prd_cost
-- Purpose: Ensures product cost is a valid non-negative numerical value.
-- Expectation: This query should return NO RESULTS.
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
PRINT '   - NULLs or Negative Values in prd_cost check completed.';

-- Check 2.4: Validate Data Standardization for prd_line
-- Purpose: Confirms that product line values adhere to the predefined set ('Mountain', 'Road', 'Other Sales', 'Touring', 'n/a').
-- Expectation: Only predefined product lines should be returned.
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;
PRINT '   - prd_line Standardization check completed.';

-- Check 2.5: Identify Invalid Date Orders (prd_start_dt > prd_end_dt)
-- Purpose: Ensures the logical consistency of product validity dates.
-- Expectation: This query should return NO RESULTS.
SELECT
    *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
PRINT '   - Invalid Date Orders (prd_start_dt > prd_end_dt) check completed.';
PRINT 'Checks for silver.crm_prd_info completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Data Quality Checks for 'silver.crm_sales_details'
-- This table contains standardized sales transaction details from CRM sources.
-- ====================================================================

PRINT 'Performing checks for silver.crm_sales_details...';

-- Check 3.1: Identify Invalid Dates (before casting to DATE) from Bronze
-- Purpose: This is a pre-check to identify problematic date strings BEFORE they are transformed into `DATE` in silver.
--          (Note: This check looks at the bronze table, as the silver table should have handled these issues.)
-- Expectation: For the silver layer, this check on the *bronze* source helps understand where issues originated.
--              Ideally, the silver load procedure should convert these to NULL or valid dates.
SELECT
    sls_ord_num, -- Added sls_ord_num for context
    sls_due_dt as Original_sls_due_dt_from_Bronze
FROM bronze.crm_sales_details -- Check against bronze to see raw issues that should be handled in silver load
WHERE TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) IS NULL -- Checks if conversion to DATE fails
    AND (sls_due_dt IS NOT NULL AND sls_due_dt != 0 AND LEN(sls_due_dt) = 8); -- Exclude already handled NULLs/zeros
PRINT '   - Invalid Dates (from Bronze source) check completed.';

-- Check 3.2: Identify Invalid Date Orders (Order Date > Shipping/Due Dates) in Silver
-- Purpose: Ensures the logical flow of sales dates (order must precede shipping and due dates).
-- Expectation: This query should return NO RESULTS.
SELECT
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;
PRINT '   - Invalid Date Orders (sls_order_dt inconsistency) check completed.';

-- Check 3.3: Verify Data Consistency: Sales = Quantity * Price
-- Purpose: Confirms the fundamental arithmetic consistency of sales data.
-- Expectation: This query should return NO RESULTS.
SELECT
    sls_ord_num, -- Added for context
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price -- Main check for inconsistency
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;
PRINT '   - Sales = Quantity * Price consistency check completed.';
PRINT 'Checks for silver.crm_sales_details completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Data Quality Checks for 'silver.erp_cust_az12'
-- This table contains standardized customer data from an ERP source.
-- ====================================================================

PRINT 'Performing checks for silver.erp_cust_az12...';

-- Check 4.1: Identify Out-of-Range Dates for Birthdates (bdate)
-- Purpose: Ensures birthdates are within a realistic and valid historical range (e.g., not in the future or extremely old).
-- Expectation: All birthdates should fall between '1924-01-01' and the current date (GETDATE()).
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();
PRINT '   - Out-of-Range Birthdates (bdate) check completed.';

-- Check 4.2: Validate Data Standardization for gen (Gender)
-- Purpose: Confirms that gender values are standardized to 'Male', 'Female', or 'n/a'.
-- Expectation: Only 'Male', 'Female', and 'n/a' should be returned.
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12;
PRINT '   - Gender Standardization check completed.';
PRINT 'Checks for silver.erp_cust_az12 completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Data Quality Checks for 'silver.erp_loc_a101'
-- This table contains standardized location data from an ERP source.
-- ====================================================================

PRINT 'Performing checks for silver.erp_loc_a101...';

-- Check 5.1: Validate Data Standardization for cntry (Country)
-- Purpose: Confirms that country names are standardized to expected values (e.g., 'Germany', 'United States', 'n/a').
-- Expectation: Only standardized country names should be returned.
SELECT DISTINCT
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
PRINT '   - Country Standardization check completed.';
PRINT 'Checks for silver.erp_loc_a101 completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Data Quality Checks for 'silver.erp_px_cat_g1v2'
-- This table contains standardized product category data from an ERP source.
-- ====================================================================

PRINT 'Performing checks for silver.erp_px_cat_g1v2...';

-- Check 6.1: Identify Unwanted Leading/Trailing Spaces in Categorical Fields
-- Purpose: Ensures that `cat`, `subcat`, and `maintenance` fields are properly trimmed.
-- Expectation: This query should return NO RESULTS.
SELECT
    *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);
PRINT '   - Unwanted Spaces in categorical fields check completed.';

-- Check 6.2: Validate Data Standardization for maintenance
-- Purpose: Confirms that the maintenance field values are consistent.
-- Expectation: All values should be consistent with expected categories for 'maintenance'.
SELECT DISTINCT
    maintenance
FROM silver.erp_px_cat_g1v2;
PRINT '   - Maintenance Standardization check completed.';
PRINT 'Checks for silver.erp_px_cat_g1v2 completed.';
PRINT '----------------------------------------------------';

PRINT 'All Silver Layer Quality Checks Completed.';
