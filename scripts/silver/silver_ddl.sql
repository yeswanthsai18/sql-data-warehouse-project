
/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This DDL (Data Definition Language) script is meticulously designed to establish the refined table structures within the 'silver' schema of the data warehouse. Its core function involves systematically dropping any existing tables in 'silver' (if they are present) before proceeding to create new ones, thereby ensuring that the schema's table definitions are precisely aligned with the script's specifications. This process is crucial for maintaining data consistency, applying initial transformations, and preparing a clean, standardized dataset for the 'gold' layer.

Operational Details:
    - This script ensures idempotency; it can be run multiple times without causing errors due to existing objects.
    - Tables are dropped based on their object ID ('U' for user table) to avoid errors if they don't exist.
    - Each table creation includes specific data types (e.g., INT, NVARCHAR, DATE, DATETIME2) suitable for the silver layer's cleansed and semi-transformed data.
    - A `dwh_create_date` column with a `DEFAULT GETDATE()` is added to each table, serving as an audit column to record when the data was loaded into the Data Warehouse.

WARNING:
    Executing this script will lead to the **permanent deletion** of all tables
    and their associated data within the 'silver' schema if they currently exist.
    This action cannot be undone. It is imperative to proceed with extreme caution
    and verify that appropriate data backups are in place, or that you explicitly
    intend to overwrite existing structures and data, especially when operating
    in a production or sensitive data environment.
===============================================================================
*/

USE DataWarehouse; -- Ensures that subsequent commands operate within the 'DataWarehouse' database context.
GO

-- Drop existing CRM Customer Info table in the 'silver' schema if it exists.
-- This ensures a clean slate for defining the table structure.
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

-- Create the 'crm_cust_info' table in the 'silver' schema.
-- This table will store cleansed and standardized customer information from CRM sources.
CREATE TABLE silver.crm_cust_info(
    cst_id             INT,             -- Unique integer identifier for the customer.
    cst_key            NVARCHAR(50),    -- Original customer key from source, potentially for linking.
    cst_firstname      NVARCHAR(50),    -- Customer's first name.
    cst_lastname       NVARCHAR(50),    -- Customer's last name.
    cst_marital_status NVARCHAR(50),    -- Marital status, standardized.
    cst_gndr           NVARCHAR(50),    -- Gender, standardized.
    cst_create_date    DATE,            -- Date of customer creation/registration, converted to DATE type.
    dwh_create_date    DATETIME2 DEFAULT GETDATE() -- Audit column: records when the record was loaded into the DWH.
);
GO

-- Drop existing CRM Product Info table in the 'silver' schema if it exists.
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

-- Create the 'crm_prd_info' table in the 'silver' schema.
-- This table will hold cleansed and standardized product details from CRM sources.
CREATE TABLE silver.crm_prd_info(
    prd_id          INT,             -- Unique integer identifier for the product.
    cat_id          NVARCHAR(50),    -- Category identifier.
    prd_key         NVARCHAR(50),    -- Original product key from source.
    prd_nm          NVARCHAR(50),    -- Product name.
    prd_cost        INT,             -- Product cost, converted to INT.
    prd_line        NVARCHAR(50),    -- Product line.
    prd_start_dt    DATE,            -- Product start date.
    prd_end_dt      DATE,            -- Product end date.
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit column.
);
GO

-- Drop existing CRM Sales Details table in the 'silver' schema if it exists.
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

-- Create the 'crm_sales_details' table in the 'silver' schema.
-- This table will store cleansed and standardized sales transaction details from CRM sources.
CREATE TABLE silver.crm_sales_details(
    sls_ord_num     NVARCHAR(50),    -- Sales order number.
    sls_prd_key     NVARCHAR(50),    -- Product key associated with the sale.
    sls_cust_id     INT,             -- Customer ID involved in the sale.
    sls_order_dt    DATE,            -- Date the order was placed.
    sls_ship_dt     DATE,            -- Date the order was shipped.
    sls_due_dt      DATE,            -- Date the payment is due.
    sls_sales       INT,             -- Total sales amount for the item, converted to INT.
    sls_quantity    INT,             -- Quantity of the product sold, converted to INT.
    sls_price       INT,             -- Unit price of the product at the time of sale, converted to INT.
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit column.
);
GO

-- Drop existing ERP Customer AZ12 table in the 'silver' schema if it exists.
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

-- Create the 'erp_cust_az12' table in the 'silver' schema.
-- This table holds cleansed customer data from the ERP AZ12 source.
CREATE TABLE silver.erp_cust_az12(
    cid             NVARCHAR(50),    -- Customer identifier from ERP system.
    bdate           DATE,            -- Birth date of the customer, converted to DATE.
    gen             NVARCHAR(50),    -- Gender, standardized.
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit column.
);
GO

-- Drop existing ERP Location A101 table in the 'silver' schema if it exists.
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

-- Create the 'erp_loc_a101' table in the 'silver' schema.
-- This table contains cleansed location data from the ERP A101 source.
CREATE TABLE silver.erp_loc_a101(
    cid             NVARCHAR(50),    -- Customer identifier from ERP system (for location linkage).
    cntry           NVARCHAR(50),    -- Country information, standardized.
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit column.
);
GO

-- Drop existing ERP Product Category G1V2 table in the 'silver' schema if it exists.
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

-- Create the 'erp_px_cat_g1v2' table in the 'silver' schema.
-- This table stores cleansed product category information from the ERP G1V2 source.
CREATE TABLE silver.erp_px_cat_g1v2(
    id          NVARCHAR(50),    -- Category identifier.
    cat         NVARCHAR(50),    -- Main category name.
    subcat      NVARCHAR(50),    -- Sub-category name.
    maintenance NVARCHAR(50),    -- Maintenance information for the category.
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit column.
);
GO
