
/*
===============================================================================
SQL Script: Gold Layer Data Quality Checks
===============================================================================
Script Purpose:
    This SQL script is specifically designed to perform critical quality checks
    on the 'gold' layer of the data warehouse. The 'gold' layer, which contains
    the final dimension and fact tables (Star Schema), is the foundation for
    business intelligence and analytical reporting. These checks are crucial
    to ensure the integrity, consistency, and accuracy of this highly refined
    data, validating:
    - **Uniqueness of Surrogate Keys**: Verifying that the generated primary keys
      in dimension tables are indeed unique, which is fundamental for correct joins
      and data aggregation.
    - **Referential Integrity**: Confirming that all foreign key relationships
      between the fact table and its associated dimension tables are intact,
      preventing orphaned records and ensuring data consistency across the model.
    - **Data Model Relationships**: Validating the proper connectivity and
      relationships within the star schema for reliable analytical query performance
      and accurate insights.

Usage Notes:
    - It is imperative to execute these quality checks after the Gold Layer views
      have been successfully created or refreshed.
    - Any results returned by these queries indicate data discrepancies or
      violations of referential integrity within the gold layer. Each such
      discrepancy must be thoroughly investigated, and corrective actions should
      be taken either in the upstream Silver-to-Gold transformation logic or
      in the Silver layer data itself, to maintain the integrity of the analytical model.
    - The 'Expectation: No results' note for a query signifies that an ideal,
      high-quality data state in the gold layer should produce no output for that specific check.
===============================================================================
*/

USE DataWarehouse; -- Ensure the script operates within the 'DataWarehouse' database context.
GO

-- ====================================================================
-- Checking 'gold.dim_customers'
-- Purpose: Verify the integrity of the customer dimension table.
-- ====================================================================

PRINT 'Performing quality check for gold.dim_customers: Uniqueness of customer_key...';

-- Check 1.1: Verify Uniqueness of Surrogate Key (customer_key) in gold.dim_customers
-- This query identifies any instances where the 'customer_key' is not unique.
-- Expectation: No results (meaning all 'customer_key' values are unique, as intended for a primary key).
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
PRINT '   - Uniqueness check for customer_key completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Checking 'gold.dim_products'
-- Purpose: Verify the integrity of the product dimension table.
-- ====================================================================

PRINT 'Performing quality check for gold.dim_products: Uniqueness of product_key...';

-- Check 2.1: Verify Uniqueness of Surrogate Key (product_key) in gold.dim_products
-- This query identifies any instances where the 'product_key' is not unique.
-- Expectation: No results (meaning all 'product_key' values are unique, as intended for a primary key).
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
PRINT '   - Uniqueness check for product_key completed.';
PRINT '----------------------------------------------------';


-- ====================================================================
-- Checking 'gold.fact_sales'
-- Purpose: Validate referential integrity and data model connectivity
--          between the fact table and its associated dimension tables.
-- ====================================================================

PRINT 'Performing quality check for gold.fact_sales: Referential Integrity with Dimensions...';

-- Check 3.1: Validate Referential Integrity and Data Model Connectivity
-- This query identifies sales records in 'fact_sales' that do not have a corresponding
-- entry in 'dim_customers' or 'dim_products' based on their respective surrogate keys.
-- It uses LEFT JOINs and checks for NULLs in the dimension keys after joining.
-- Expectation: No results (meaning every sales record successfully links to a valid
-- customer and product in the gold dimensions). Any results indicate orphaned fact records
-- or issues in the key generation/joining logic.
SELECT
    f.order_number, -- Include order_number for easy identification of problematic records
    f.customer_key AS fact_customer_key,
    c.customer_key AS dim_customer_key,
    f.product_key AS fact_product_key,
    p.product_key AS dim_product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
PRINT '   - Referential integrity check completed.';
PRINT '----------------------------------------------------';

PRINT 'All Gold Layer Quality Checks Completed.';
