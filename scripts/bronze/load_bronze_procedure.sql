/*
=============================================================
Stored Procedure: bronze.load_bronze
=============================================================
Procedure Purpose:
    This stored procedure, `bronze.load_bronze`, is designed to automate the initial data ingestion into the 'bronze' schema of the data warehouse. It systematically clears existing raw data and then populates the bronze tables by bulk-inserting information directly from various CSV source files. The procedure includes robust logging to track load durations for individual tables and the overall process, along with comprehensive error handling to manage and report any issues that may arise during the data transfer.

Operational Details:
    - Measures and reports the execution time for each table load and the entire batch.
    - Employs `TRUNCATE TABLE` before each `BULK INSERT` to ensure idempotency and a fresh load.
    - Utilizes `BULK INSERT` to efficiently load data from specified CSV paths, starting from the second row (skipping headers) and using a comma as a field terminator.
    - Incorporates a `TRY...CATCH` block for error management, providing detailed error messages if the loading process encounters problems.

WARNING:
    Executing this procedure will first clear (truncate) all data in the target 'bronze' tables
    before reloading from the CSV files. Any modifications or existing data in these
    bronze tables will be permanently overwritten. Ensure source CSV files are correct
    and accessible.

-- Command to execute the stored procedure:
EXEC bronze.load_bronze;
=============================================================
*/

-- Stored procedure definition:
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Declare variables for tracking start and end times for individual table loads and the overall batch process.
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- Record the start time of the entire bronze layer loading process.
        SET @batch_start_time = GETDATE();

        -- Output messages to indicate the beginning of the bronze layer loading.
        PRINT '=============================================================';
        PRINT 'Initiating data ingestion into the bronze layer.';
        PRINT '=============================================================';

        -- Section for loading CRM-related data.
        PRINT '-------------------------------------------------------------';
        PRINT 'Processing CRM data tables...';
        PRINT '-------------------------------------------------------------';

        -- Load CRM customer information.
        SET @start_time = GETDATE(); -- Capture start time for this specific table load.
        PRINT '>> Clearing existing data from: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info; -- Remove all rows to prepare for a fresh insert.

        PRINT '>> Populating table: bronze.crm_cust_info from cust_info.csv';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,           -- Skip the header row.
            FIELDTERMINATOR = ',',  -- Use comma as the column delimiter.
            TABLOCK                 -- Acquire a table-level lock for efficient bulk insertion.
        );
        SET @end_time = GETDATE(); -- Capture end time for this table load.
        PRINT '>> Load duration for crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<------------------------------------------------------------->>';

        -- Load CRM product information.
        SET @start_time = GETDATE();
        PRINT '>> Clearing existing data from: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Populating table: bronze.crm_prd_info from prd_info.csv';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration for crm_prd_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<------------------------------------------------------------->>';

        -- Load CRM sales details.
        SET @start_time = GETDATE();
        PRINT '>> Clearing existing data from: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Populating table: bronze.crm_sales_details from sales_details.csv';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration for crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<------------------------------------------------------------->>';

        -- Section for loading ERP-related data.
        PRINT '-------------------------------------------------------------';
        PRINT 'Processing ERP data tables...';
        PRINT '-------------------------------------------------------------';

        -- Load ERP customer data.
        SET @start_time = GETDATE();
        PRINT '>> Clearing existing data from: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Populating table: bronze.erp_cust_az12 from cust_az12.csv';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration for erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<------------------------------------------------------------->>';

        -- Load ERP location data.
        SET @start_time = GETDATE();
        PRINT '>> Clearing existing data from: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Populating table: bronze.erp_loc_a101 from loc_a101.csv';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration for erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<------------------------------------------------------------->>';

        -- Load ERP product category data.
        SET @start_time = GETDATE();
        PRINT '>> Clearing existing data from: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Populating table: bronze.erp_px_cat_g1v2 from px_cat_g1v2.csv';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\yeswa\Downloads\Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration for erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';

        -- Record the end time of the entire bronze layer loading process and print summary.
        SET @batch_end_time = GETDATE();
        PRINT '=============================================================';
        PRINT 'Bronze layer data ingestion completed successfully!';
        PRINT '>> Total load duration for the Bronze Layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '=============================================================';

    END TRY
    BEGIN CATCH
        -- Error handling block: Print details if an error occurs during the load process.
        PRINT '=============================================================';
        PRINT 'AN ERROR OCCURRED DURING THE BRONZE LAYER DATA INGESTION';
        PRINT 'Error Message: ' + ERROR_MESSAGE(); -- Provides the error message.
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR); -- Provides the error number.
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR); -- Provides the error state.
        PRINT '=============================================================';
    END CATCH;
END;
GO
