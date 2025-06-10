/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
SET NOCOUNT ON;
	DECLARE @load_silver_start_time DATETIME, @load_silver_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;

	SET @load_silver_start_time = GETDATE();

	BEGIN TRY 
		PRINT '========================================================';
		PRINT 'Loading Silver Layer';
		PRINT '========================================================';
		PRINT '';
		PRINT '';
		PRINT '--------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------';
		--====================================================================== Table: silver.crm_cust_info section ==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info';

		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info 
		(
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
			TRIM(cst_key) AS cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(t.cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(t.cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status, -- Normalize marital status values to readable format.
			CASE 
				WHEN UPPER(TRIM(t.cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(t.cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr, -- Normalize gender values to readable format.
		
			cst_create_date
		FROM 
		(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Tag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t 
		WHERE t.Tag = 1; -- Select the last recent record per customer.

		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		
		
		--==========================================================================================================================================


		--====================================================================== Table: silver.crm_prd_info section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info

		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO	silver.crm_prd_info
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)
		SELECT [prd_id]
			  ,[cat_id] = REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')  -- Extract category key (Derived Column)
			  ,[prd_key] = SUBSTRING(prd_key, 7, LEN(prd_key)) -- Extract product key (Derived Column)
			  ,[prd_nm]
			  ,[prd_cost] = COALESCE([prd_cost], 0) 
			  ,CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END [prd_line] -- Map product line codes to descriptive values
			  ,[prd_start_dt] = CAST(prd_start_dt AS date)
			  , prd_end_dt =    -- Calculate the end date based on the next start date 
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS date)
		  FROM [bronze].[crm_prd_info];

		  SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================


		--====================================================================== Table: silver.crm_sales_details section	==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details

		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
		(
			[sls_ord_num]
			,[sls_prd_key]
			,[sls_cust_id]
			,[sls_order_dt]
			,[sls_ship_dt]
			,[sls_due_dt]
			,[sls_sales]
			,[sls_quantity]
			,[sls_price]
		)

		SELECT [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8  THEN NULL 
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
				END AS [sls_order_dt]
			  ,CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) != 8  THEN NULL 
				ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE) 
				END AS [sls_ship_dt]
			  ,CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) != 8  THEN NULL 
				ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE) 
				END AS [sls_due_dt]
			  ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales END sls_sales
			  ,[sls_quantity]
      
			   ,CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			   END sls_price
		  FROM [bronze].[crm_sales_details]

		  SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		PRINT '';
		PRINT '';
		--==========================================================================================================================================




		PRINT '--------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------------';
		PRINT '';

		--====================================================================== Table: silver.erp_cust_az12 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12

		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
		(
			CID,
			BDATE,
			GEN
		)
		SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(cid))  -- Removed the 'NAS' prefix is present
			ELSE CID
			END AS cid,
			CASE WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
			END	AS bdate,  -- set future birhdates to null
			CASE 
				WHEN UPPER(TRIM(GEN)) in ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(GEN)) in ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END	AS gen  -- Normalize gender values and handle unkown cases
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================



		--====================================================================== Table: silver.erp_loc_a101 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101

		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
		(
			CID,
			CNTRY
		)
		SELECT
		REPLACE(CID, '-', '') AS cid,
		CASE WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'n/a'
				ELSE TRIM(CNTRY)
			END CNTRY  -- Normalize and handle missing or blank country codes
		FROM bronze.erp_loc_a101 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================



		--====================================================================== Table: silver.erp_px_cat_g1v2 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		INSERT INTO silver.erp_px_cat_g1v2
		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT 
			id,
			cat,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================

	END TRY
	BEGIN CATCH
		PRINT '=======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================================';
	END CATCH

	SET @load_silver_end_time = GETDATE();
	PRINT '';
	PRINT '';
	PRINT '===================================================================';
	PRINT 'Loading Silver Layer is Completed';
	PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @load_silver_start_time, @load_silver_end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '===================================================================';
END
