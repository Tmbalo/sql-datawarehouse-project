/*
==========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.
	
Parameters:
	None.
	This stored prodecure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;
==========================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
SET NOCOUNT ON;
	DECLARE @load_bronze_start_time DATETIME, @load_bronze_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;

	SET @load_bronze_start_time = GETDATE();
	BEGIN TRY 
		PRINT '========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================================';
		PRINT '';
		PRINT '';
		PRINT '--------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------';
		--====================================================================== Table: bronze.crm_cust_info section ==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		
		
		--==========================================================================================================================================


		--====================================================================== Table: bronze.crm_prd_info section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================


		--====================================================================== Table: bronze.crm_sales_details section	==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		PRINT '';
		PRINT '';
		--==========================================================================================================================================




		PRINT '--------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------------';
		PRINT '';

		--====================================================================== Table: bronze.erp_cust_az12 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================



		--====================================================================== Table: bronze.erp_loc_a101 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================



		--====================================================================== Table: bronze.erp_px_cat_g1v2 section		==========================
		PRINT '';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration for table bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) +  ' seconds';
		PRINT '>>--------------------------------------------------------';
		--==========================================================================================================================================

	END TRY
	BEGIN CATCH
		PRINT '=======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================================';
	END CATCH

	SET @load_bronze_end_time = GETDATE();
	PRINT '';
	PRINT '';
	PRINT '===================================================================';
	PRINT 'Loading Bronze Layer is Completed';
	PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @load_bronze_start_time, @load_bronze_end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '===================================================================';
END


