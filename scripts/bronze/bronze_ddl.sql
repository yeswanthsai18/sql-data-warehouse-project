/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This DDL (Data Definition Language) script is designed to establish the table 
	structures within the 'bronze' schema. Its function includes removing any 
	existing tables in 'bronze' before creating new ones, ensuring that the schema's 
	table definitions are redefined according to the script. This ensures data 
	consistency and prepares the 'bronze' layer for raw data ingestion from source 
	systems.

WARNING:
    Executing this script will drop all tables within the 'bronze' schema if they exist.
    Any data currently residing in these tables will be permanently lost. Proceed with caution
    and ensure you have proper backups or are certain you want to overwrite existing structures
    before running this script in a production or sensitive environment.
===============================================================================
*/

USE DataWarehouse; -- Ensure you are using the correct database

-- Drop existing tables in the 'bronze' schema to ensure a clean slate
-- This is crucial for idempotency and re-running the script without errors
-- Drop order matters due to potential foreign key constraints (though less common in bronze)

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO


CREATE TABLE bronze.crm_cust_info(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
	prd_id			INT,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME

);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt		INT, 
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101(
	cid		NVARCHAR(50),
	cntry	NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO	
