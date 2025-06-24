
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This DDL (Data Definition Language) script is meticulously crafted to define and create the analytical views for the 'gold' layer within the data warehouse. The 'gold' layer represents the culmination of the data transformation process, providing highly refined dimension and fact tables that conform to a Star Schema design.

    Each view in this script performs specific transformations, consolidations, and joins on data sourced from the 'silver' layer. The goal is to produce a clean, enriched, highly denormalized (for dimensions), and business-ready dataset, optimized for direct consumption by business intelligence tools, reporting applications, and advanced analytical queries. Surrogate keys are generated to ensure efficient join operations and historical data management (where applicable).

Usage:
    - These views serve as the primary interface for business users, data analysts, and reporting tools.
    - They should be queried directly for all analytical and reporting requirements, abstracting away the complexity of the underlying 'silver' and 'bronze' data structures.
    - Re-running this script will drop and recreate the views, ensuring their definitions are always up-to-date with the latest silver layer transformations.
===============================================================================
*/

USE DataWarehouse; -- Ensures the script executes within the context of the 'DataWarehouse' database.
GO

-- =============================================================================
-- Create Dimension View: gold.dim_customers
-- Description: This view creates a conformed customer dimension table by
--              integrating and enriching customer data from CRM and ERP sources
--              in the silver layer. It generates a surrogate key and prioritizes
--              CRM data for general customer info, falling back to ERP for specific
--              attributes like gender and birthdate when CRM data is unavailable.
-- =============================================================================

-- Drop the existing view if it exists to allow for recreation or modification.
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    -- Generate a unique, incremental surrogate key for each customer.
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id                            AS customer_id,       -- Natural key for the customer.
    ci.cst_key                           AS customer_number,   -- Original customer key/number from CRM.
    ci.cst_firstname                     AS first_name,        -- Customer's first name.
    ci.cst_lastname                      AS last_name,         -- Customer's last name.
    la.cntry                             AS country,           -- Enriched country information from ERP location data.
    ci.cst_marital_status                AS marital_status,    -- Standardized marital status from CRM.
    -- Prioritize CRM gender; if 'n/a', use ERP gender as fallback.
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                                  AS gender,
    ca.bdate                             AS birthdate,         -- Birthdate from ERP customer data.
    ci.cst_create_date                   AS create_date        -- Customer creation date from CRM.
FROM silver.crm_cust_info ci             -- Primary source for customer core details.
LEFT JOIN silver.erp_cust_az12 ca        -- Join with ERP customer demographics (birthdate, ERP gender).
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la         -- Join with ERP location data (country).
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension View: gold.dim_products
-- Description: This view constructs a comprehensive product dimension table by
--              combining product details from CRM with category and maintenance
--              information from ERP. It generates a surrogate key and filters
--              out historical product versions, focusing on current product data.
-- =============================================================================

-- Drop the existing view if it exists.
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    -- Generate a unique, incremental surrogate key for each current product version.
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id           AS product_id,       -- Natural key for the product.
    pn.prd_key          AS product_number,   -- Original product key/number.
    pn.prd_nm           AS product_name,     -- Product name.
    pn.cat_id           AS category_id,      -- Category identifier from CRM.
    pc.cat              AS category,         -- Main category name from ERP.
    pc.subcat           AS subcategory,      -- Subcategory name from ERP.
    pc.maintenance      AS maintenance,      -- Maintenance status from ERP.
    pn.prd_cost         AS cost,             -- Product cost.
    pn.prd_line         AS product_line,     -- Standardized product line.
    pn.prd_start_dt     AS start_date        -- Product start date (for validity).
FROM silver.crm_prd_info pn                 -- Primary source for product core details.
LEFT JOIN silver.erp_px_cat_g1v2 pc         -- Join with ERP product categories for enrichment.
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;                -- Critical: Filters out all historical product records, keeping only the currently active version.
GO

-- =============================================================================
-- Create Fact View: gold.fact_sales
-- Description: This view constructs the central sales fact table. It combines
--              transactional sales details from the silver layer with relevant
--              attributes from the newly created gold dimension tables (customers, products).
--              This view is optimized for measuring sales performance.
-- =============================================================================

-- Drop the existing view if it exists.
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,    -- Unique identifier for each sales order.
    pr.product_key  AS product_key,     -- Surrogate key linking to the gold.dim_products dimension.
    cu.customer_key AS customer_key,    -- Surrogate key linking to the gold.dim_customers dimension.
    sd.sls_order_dt AS order_date,      -- Date when the sales order was placed.
    sd.sls_ship_dt  AS shipping_date,   -- Date when the order was shipped.
    sd.sls_due_dt   AS due_date,        -- Date when the payment for the order was due.
    sd.sls_sales    AS sales_amount,    -- Total monetary value of the sale line item.
    sd.sls_quantity AS quantity,        -- Number of units sold in the line item.
    sd.sls_price    AS price            -- Unit price of the product at the time of sale.
FROM silver.crm_sales_details sd        -- Source for core sales transaction details.
LEFT JOIN gold.dim_products pr          -- Join with products dimension using natural key to get surrogate key.
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu         -- Join with customers dimension using natural key to get surrogate key.
    ON sd.sls_cust_id = cu.customer_id;
GO
