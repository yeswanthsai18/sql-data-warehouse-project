/*
=============================================================
Stored Procedure: silver.load_silver
=============================================================
Procedure Purpose:
    This stored procedure, `silver.load_silver`, orchestrates the crucial process of transforming and loading raw data from the 'bronze' schema into the 'silver' schema. It performs data cleansing, standardization, and initial transformations specific to each table, ensuring the data is refined and prepared for analytical consumption in the subsequent 'gold' layer. The procedure meticulously logs execution times for each table load and the overall batch, and includes robust error handling for operational resilience.

Operational Details:
    - **Execution Timing**: Variables (`@Start_time`, `@end_time`, `@batch_start_time`, `@batch_end_time`) are used to precisely measure and print the duration of each individual table transformation and the total execution time of the silver layer load.
    - **Idempotent Loading**: Each target table in the `silver` schema is first truncated (`TRUNCATE TABLE`) to clear any previous data, ensuring that every execution of the procedure starts with a clean slate and avoids data duplication.
    - **Data Cleansing and Transformation (CRM Tables)**:
        - `silver.crm_cust_info`:
            - Trims leading/trailing spaces from `cst_firstname` and `cst_lastname`.
            - Standardizes `cst_marital_status` (e.g., 'S' to 'Single', 'M' to 'Married').
            - Standardizes `cst_gndr` (e.g., 'F' to 'Female', 'M' to 'Male').
            - Selects the latest customer record based on `cst_create_date` using `ROW_NUMBER()` with `PARTITION BY cst_id`.
            - Filters out records where `cst_id` is NULL.
        - `silver.crm_prd_info`:
            - Derives `cat_id` by replacing hyphens and taking a substring from `prd_key`.
            - Extracts `prd_key` by taking a substring.
            - Handles `NULL` values for `prd_cost` by defaulting to `0`.
            - Standardizes `prd_line` values (e.g., 'M' to 'Mountain').
            - Casts `prd_start_dt` to `DATE` type.
            - Calculates `prd_end_dt` using `LEAD` window function to determine the end of a product's validity period based on the next `prd_start_dt`.
        - `silver.crm_sales_details`:
            - Converts date strings (`sls_order_dt`, `sls_ship_dt`, `sls_due_dt`) to `DATE` type, handling invalid or zero-length strings by setting them to `NULL`.
            - Validates and corrects `sls_sales` amount: if NULL, zero, or inconsistent with `quantity * price`, it recalculates the `sls_sales` amount.
            - Corrects `sls_price` if it's NULL or zero by attempting to derive it from `sls_sales` and `sls_quantity` to avoid division by zero.
    - **Data Cleansing and Transformation (ERP Tables)**:
        - `silver.erp_cust_az12`:
            - Cleans `cid` by removing 'NAS' prefix if present.
            - Validates `bdate` to ensure it's not a future date, setting future dates to `NULL`.
            - Standardizes `gen` (e.g., 'M', 'Male' to 'Male'; 'F', 'Female' to 'Female').
        - `silver.erp_loc_a101`:
            - Cleans `cid` by removing hyphens if present.
            - Standardizes `cntry` (e.g., 'DE' to 'Germany', 'US'/'USA' to 'United States', empty/NULL to 'n/a').
        - `silver.erp_px_cat_g1v2`: Performs a direct load as transformation rules are not explicitly defined in the provided `SELECT` statement.
    - **Error Handling**: A `BEGIN TRY...BEGIN CATCH` block is implemented to gracefully handle any SQL errors during execution. In case of an error, it prints a clear message along with the error number, message, and state for debugging.

WARNING:
    Executing this stored procedure will **truncate all existing data** within the tables
    of the `silver` schema before new data is inserted. This means any manual changes or
    previously loaded data in `silver.crm_cust_info`, `silver.crm_prd_info`,
    `silver.crm_sales_details`, `silver.erp_cust_az12`, `silver.erp_loc_a101`, and
    `silver.erp_px_cat_g1v2` will be permanently lost and replaced by the transformed
    data from the `bronze` schema. Ensure this behavior aligns with your data governance
    and refresh strategy.
=============================================================
*/

-- Command to execute the stored procedure for loading the silver layer:
-- EXEC silver.load_silver;

-- Stored procedure definition:
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
        -- Declare variables to track the start and end times for the entire batch and individual table loads.
        DECLARE @Start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

        -- Record the start time for the overall silver layer loading process.
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Initiating Load Process for Silver Layer...';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Commencing Load of CRM-related Tables into Silver Schema';
        PRINT '------------------------------------------------';

        -- Process silver.crm_cust_info table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info; -- Clears previous data for a fresh load.
        PRINT '>> Inserting Transformed Data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname, -- Cleanse by removing leading/trailing spaces.
            TRIM(cst_lastname) AS cst_lastname,   -- Cleanse by removing leading/trailing spaces.
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- Standardize marital status.
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- Standardize gender.
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            CAST(cst_create_date AS DATE) -- Ensure date is cast to DATE type.
        FROM (
            -- Subquery to select the latest customer record based on creation date.
            SELECT *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL -- Exclude records with no customer ID.
        ) t WHERE flag_last = 1; -- Select only the latest version of each customer.
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.crm_cust_info: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<---------------------------------------------->>';

        -- Process silver.crm_prd_info table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Transformed Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract and transform category ID.
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,      -- Extract product key.
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost, -- Handle NULL product costs by defaulting to 0.
            CASE UPPER(TRIM(prd_line))       -- Standardize product line.
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Ensure start date is DATE type.
            CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt -- Calculate end date for product validity.
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.crm_prd_info: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<---------------------------------------------->>';

        -- Process silver.crm_sales_details table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Transformed Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- Date Cleaning and Transformation: Convert string dates to DATE, handle invalid formats.
            CASE
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            -- Sales Amount Correction: Ensure sales amount is consistent with quantity and price.
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            -- Price Correction: Derive price if it's invalid or missing, avoid division by zero.
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0) -- Use NULLIF to prevent division by zero.
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.crm_sales_details: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<---------------------------------------------->>';


        PRINT '------------------------------------------------';
        PRINT 'Commencing Load of ERP-related Tables into Silver Schema';
        PRINT '------------------------------------------------';

        -- Process silver.erp_cust_az12 table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Transformed Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Cleanse customer ID prefix.
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL -- Validate birth date (no future dates).
                ELSE TRY_CAST(bdate AS DATE) -- Ensure date is cast to DATE type.
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'     -- Standardize gender.
                WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.erp_cust_az12: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<---------------------------------------------->>';

        -- Process silver.erp_loc_a101 table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Transformed Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT
            CASE
                WHEN cid LIKE '%-%' THEN REPLACE(cid, '-', '') -- Cleanse customer ID hyphens.
                ELSE cid
            END AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'       -- Standardize country names.
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.erp_loc_a101: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '<<---------------------------------------------->>';

        -- Process silver.erp_px_cat_g1v2 table
        SET @Start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2; -- Direct load; transformations typically occur based on data quality rules for this specific source.
        SET @end_time = GETDATE();
        PRINT '>> Load Duration for silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @Start_time, @end_time) AS NVARCHAR) + ' Seconds';
        PRINT '>> -------------';

        -- Record the end time for the entire silver layer loading process and print a summary.
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Silver Layer Loading Completed Successfully!';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
    END TRY
    BEGIN CATCH
        -- Error Handling: Catch and display details of any errors that occur during the procedure execution.
        PRINT '==========================================';
        PRINT 'AN ERROR OCCURRED DURING THE SILVER LAYER LOADING PROCESS!';
        PRINT 'Error Message: ' + ERROR_MESSAGE(); -- Provides the specific error message.
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR); -- Provides the error number.
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);   -- Provides the state of the error.
        PRINT '=========================================='
    END CATCH;
END;
