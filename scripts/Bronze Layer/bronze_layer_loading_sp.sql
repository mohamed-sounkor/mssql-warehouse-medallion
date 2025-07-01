-- Helper Procedure
CREATE OR ALTER PROCEDURE bronze.bulk_load_csv
    @table_name NVARCHAR(256),   -- e.g., 'bronze.crm_cust_info'
    @file_path NVARCHAR(512)     -- e.g., 'C:\path\to\file.csv'
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @end_time DATETIME;

    -- Split schema and table from input
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(128);
    SET @schema = PARSENAME(@table_name, 2);
    SET @table = PARSENAME(@table_name, 1);

    -- Validate presence
    IF @schema IS NULL OR @table IS NULL
    BEGIN
        PRINT 'Invalid table name format: must be schema.table (e.g., bronze.crm_cust_info)';
        RETURN;
    END

    IF OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table), 'U') IS NULL
    BEGIN
        PRINT 'Table [' + @schema + '].[' + @table + '] does not exist.';
        RETURN;
    END

    -- Truncate table
    PRINT '>> Truncating Table: [' + @schema + '].[' + @table + ']';
    SET @sql = 'TRUNCATE TABLE ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table);
    EXEC sp_executesql @sql;

    -- Bulk insert
    PRINT '>> Inserting Data Into: [' + @schema + '].[' + @table + ']';
    SET @sql = '
        BULK INSERT ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table) + '
        FROM ''' + @file_path + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            TABLOCK
        );
    ';
    EXEC sp_executesql @sql;

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
END;
GO

-- Main Procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start_time DATETIME = GETDATE();
    DECLARE @batch_end_time DATETIME;

    BEGIN TRY
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        EXEC bronze.bulk_load_csv 'bronze.crm_cust_info',      "D:\Education stuff\mssql-warehouse-medallion\datasets\source_crm\cust_info.csv";
        EXEC bronze.bulk_load_csv 'bronze.crm_prd_info',       "D:\Education stuff\mssql-warehouse-medallion\datasets\source_crm\prd_info.csv";
        EXEC bronze.bulk_load_csv 'bronze.crm_sales_details',  "D:\Education stuff\mssql-warehouse-medallion\datasets\source_crm\sales_details.csv";

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        EXEC bronze.bulk_load_csv 'bronze.erp_loc_a101',       "D:\Education stuff\mssql-warehouse-medallion\datasets\source_erp\LOC_A101.csv";
        EXEC bronze.bulk_load_csv 'bronze.erp_cust_az12',      "D:\Education stuff\mssql-warehouse-medallion\datasets\source_erp\CUST_AZ12.csv";
        EXEC bronze.bulk_load_csv 'bronze.erp_px_cat_g1v2',    "D:\Education stuff\mssql-warehouse-medallion\datasets\source_erp\PX_CAT_G1V2.csv";

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO

-- Execute the main procedure to load the bronze layer
USE DataWarehouse
GO
EXEC bronze.load_bronze;
GO


--Testing data loading:
SELECT COUNT(*) FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;