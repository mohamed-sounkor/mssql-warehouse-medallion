-- =============================================
-- Silver Layer Final Initialization Script
-- =============================================
-- Includes: Clean Tables + Quarantine Tables + dwh_create_date

-- -------------------------------
-- crm_cust_info
-- -------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_cust_info_quarantine', 'U') IS NOT NULL DROP TABLE silver.crm_cust_info_quarantine;
CREATE TABLE silver.crm_cust_info_quarantine (
    cst_id           VARCHAR(50),
    cst_key          VARCHAR(50),
    cst_firstname    VARCHAR(50),
    cst_lastname     VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr         VARCHAR(50),
    cst_create_date  VARCHAR(50),
    error_reason     VARCHAR(255),
    quarantine_time  DATETIME2 DEFAULT GETDATE()
);

-- -------------------------------
-- crm_prd_info
-- -------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info_quarantine', 'U') IS NOT NULL DROP TABLE silver.crm_prd_info_quarantine;
CREATE TABLE silver.crm_prd_info_quarantine (
    prd_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        VARCHAR(50),
    prd_line        VARCHAR(50),
    prd_start_dt    VARCHAR(50),
    prd_end_dt      VARCHAR(50),
    error_reason    VARCHAR(255),
    quarantine_time DATETIME2 DEFAULT GETDATE()
);

-- -------------------------------
-- crm_sales_details
-- -------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details_quarantine', 'U') IS NOT NULL DROP TABLE silver.crm_sales_details_quarantine;
CREATE TABLE silver.crm_sales_details_quarantine (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     VARCHAR(50),
    sls_order_dt    VARCHAR(50),
    sls_ship_dt     VARCHAR(50),
    sls_due_dt      VARCHAR(50),
    sls_sales       VARCHAR(50),
    sls_quantity    VARCHAR(50),
    sls_price       VARCHAR(50),
    error_reason    VARCHAR(255),
    quarantine_time DATETIME2 DEFAULT GETDATE()
);

-- -------------------------------
-- erp_cust_az12
-- -------------------------------
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12_quarantine', 'U') IS NOT NULL DROP TABLE silver.erp_cust_az12_quarantine;
CREATE TABLE silver.erp_cust_az12_quarantine (
    cid             VARCHAR(50),
    bdate           VARCHAR(50),
    gen             VARCHAR(50),
    error_reason    VARCHAR(255),
    quarantine_time DATETIME2 DEFAULT GETDATE()
);

-- -------------------------------
-- erp_loc_a101
-- -------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101_quarantine', 'U') IS NOT NULL DROP TABLE silver.erp_loc_a101_quarantine;
CREATE TABLE silver.erp_loc_a101_quarantine (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    error_reason    VARCHAR(255),
    quarantine_time DATETIME2 DEFAULT GETDATE()
);

-- -------------------------------
-- erp_px_cat_g1v2
-- -------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2_quarantine', 'U') IS NOT NULL DROP TABLE silver.erp_px_cat_g1v2_quarantine;
CREATE TABLE silver.erp_px_cat_g1v2_quarantine (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    error_reason    VARCHAR(255),
    quarantine_time DATETIME2 DEFAULT GETDATE()
);
