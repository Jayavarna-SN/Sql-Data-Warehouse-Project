/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    Cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status VARCHAR(10),
    cst_gndr VARCHAR(10),
    cst_create_date DATE,
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_pro_info','U') IS NOT NULL
    DROP TABLE silver.crm_pro_info;

CREATE TABLE silver.crm_pro_info (
    Pro_id INT,
    cat_id NVARCHAR(50),
    Pro_key NVARCHAR(50),
    Pro_nm NVARCHAR(50),
    Pro_cost INT,
    Pro_line VARCHAR(50),
    Pro_start_dt DATE,
    Pro_End_dt DATE,
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details_info','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details_info;

CREATE TABLE silver.crm_sales_details_info (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cat_info','U') IS NOT NULL
    DROP TABLE silver.erp_cat_info;

CREATE TABLE silver.erp_cat_info (
    id NVARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(10),
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cus_info','U') IS NOT NULL
    DROP TABLE silver.erp_cus_info;

CREATE TABLE silver.erp_cus_info (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(10),
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_info','U') IS NOT NULL
    DROP TABLE silver.erp_loc_info;

CREATE TABLE silver.erp_loc_info (
    CID NVARCHAR(50),
    Cntry VARCHAR(50),
    dhw_create_date DATETIME2 DEFAULT GETDATE()
);

