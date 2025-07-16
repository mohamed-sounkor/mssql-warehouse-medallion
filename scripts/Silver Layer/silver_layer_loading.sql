-- Finalized Silver Layer Load Script
-- Includes complete ETL with quarantine logic and dwh_create_date

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    -- CRM Customer Info
    TRUNCATE TABLE silver.crm_cust_info;
    TRUNCATE TABLE silver.crm_cust_info_quarantine;

    INSERT INTO silver.crm_cust_info
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE WHEN UPPER(TRIM(cst_marital_status)) IN ('S', 'SINGLE') THEN 'Single'
             WHEN UPPER(TRIM(cst_marital_status)) IN ('M', 'MARRIED') THEN 'Married'
             ELSE 'N/A' END,
        CASE WHEN UPPER(TRIM(cst_gndr)) IN ('M', 'MALE') THEN 'Male'
             WHEN UPPER(TRIM(cst_gndr)) IN ('F', 'FEMALE') THEN 'Female'
             ELSE 'N/A' END,
        cst_create_date,
        GETDATE()
    		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
		) t
		WHERE flag_last = 1 AND cst_id IS NOT NULL; -- Select the most recent record per customer

    INSERT INTO silver.crm_cust_info_quarantine
    SELECT
        CAST(cst_id AS VARCHAR), cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr,
        CAST(cst_create_date AS VARCHAR), 'Invalid or future create_date', GETDATE()
    FROM bronze.crm_cust_info
    WHERE cst_id IS NULL;

    -- CRM Product Info
    TRUNCATE TABLE silver.crm_prd_info;
    TRUNCATE TABLE silver.crm_prd_info_quarantine;

    INSERT INTO silver.crm_prd_info
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LEN(prd_key)),
        TRIM(prd_nm),
        prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
             ELSE 'N/A' END,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
			AS DATE
		) AS prd_end_dt, -- Calculate end date as one day before the next start date
        GETDATE()
    FROM bronze.crm_prd_info
    WHERE prd_cost IS NOT NULL AND prd_cost >= 0;

    INSERT INTO silver.crm_prd_info_quarantine
    SELECT
        CAST(prd_id AS VARCHAR), prd_key, prd_nm, CAST(prd_cost AS VARCHAR), prd_line,
        CAST(prd_start_dt AS VARCHAR), CAST(prd_end_dt AS VARCHAR),
        'Negative or null cost', GETDATE()
    FROM bronze.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost < 0;

    -- CRM Sales Details
    TRUNCATE TABLE silver.crm_sales_details;
    TRUNCATE TABLE silver.crm_sales_details_quarantine;

    INSERT INTO silver.crm_sales_details
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE),
        TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE),
        TRY_CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE),
        sls_sales,
        sls_quantity,
        sls_price,
        GETDATE()
    FROM bronze.crm_sales_details
    WHERE sls_sales IS NOT NULL AND sls_sales >= 0
      AND sls_quantity IS NOT NULL AND sls_quantity >= 0
      AND sls_price IS NOT NULL AND sls_price >= 0
      AND TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) IS NOT NULL
      AND TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) <= TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE)
      AND TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE) <= TRY_CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE);

    INSERT INTO silver.crm_sales_details_quarantine
    SELECT
        sls_ord_num, sls_prd_key, CAST(sls_cust_id AS VARCHAR),
        CAST(sls_order_dt AS VARCHAR), CAST(sls_ship_dt AS VARCHAR), CAST(sls_due_dt AS VARCHAR),
        CAST(sls_sales AS VARCHAR), CAST(sls_quantity AS VARCHAR), CAST(sls_price AS VARCHAR),
        'Invalid sales record (null, negative, or bad date)', GETDATE()
    FROM bronze.crm_sales_details
    WHERE sls_sales IS NULL OR sls_sales < 0
       OR sls_quantity IS NULL OR sls_quantity < 0
       OR sls_price IS NULL OR sls_price < 0
       OR TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) IS NULL
       OR TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) > TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE)
       OR TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE) > TRY_CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE);

    -- ERP Customer AZ12
    TRUNCATE TABLE silver.erp_cust_az12;
    TRUNCATE TABLE silver.erp_cust_az12_quarantine;

    INSERT INTO silver.erp_cust_az12
    SELECT
        REPLACE(cid, 'NAS', ''),
        bdate,
        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'N/A' END,
        GETDATE()
    FROM bronze.erp_cust_az12
    WHERE bdate IS NOT NULL AND bdate <= GETDATE();

    INSERT INTO silver.erp_cust_az12_quarantine
    SELECT
        cid, CAST(bdate AS VARCHAR), gen,
        'Future birthdate or null', GETDATE()
    FROM bronze.erp_cust_az12
    WHERE bdate IS NULL OR bdate > GETDATE();

    -- ERP Location
    TRUNCATE TABLE silver.erp_loc_a101;
    TRUNCATE TABLE silver.erp_loc_a101_quarantine;

    INSERT INTO silver.erp_loc_a101
    SELECT
        REPLACE(cid, '-', ''),
        CASE WHEN LOWER(TRIM(cntry)) IN ('us', 'usa', 'united states') THEN 'United States'
             WHEN LOWER(TRIM(cntry)) IN ('de', 'germany') THEN 'Germany'
             ELSE 'N/A' END,
        GETDATE()
    FROM bronze.erp_loc_a101
    WHERE cntry IS NOT NULL AND LTRIM(RTRIM(cntry)) <> '';

    INSERT INTO silver.erp_loc_a101_quarantine
    SELECT
        cid, cntry, 'Missing or unrecognized country', GETDATE()
    FROM bronze.erp_loc_a101
    WHERE cntry IS NULL OR LTRIM(RTRIM(cntry)) = '';

    -- ERP Product Categories
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    TRUNCATE TABLE silver.erp_px_cat_g1v2_quarantine;

    INSERT INTO silver.erp_px_cat_g1v2
    SELECT
        id, cat, subcat, maintenance, GETDATE()
    FROM bronze.erp_px_cat_g1v2
    WHERE id IS NOT NULL AND cat IS NOT NULL AND subcat IS NOT NULL AND maintenance IS NOT NULL;

    INSERT INTO silver.erp_px_cat_g1v2_quarantine
    SELECT
        id, cat, subcat, maintenance, 'Missing required fields', GETDATE()
    FROM bronze.erp_px_cat_g1v2
    WHERE id IS NULL OR cat IS NULL OR subcat IS NULL OR maintenance IS NULL;
END
GO


EXEC silver.load_silver;
GO