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

create or alter procedure silver.load_silver as
Begin

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT '================================================';
    PRINT 'Loading Silver Layer';
    PRINT '================================================';

    PRINT '------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE : SILVER.crm_cust_info';
        TRUNCATE TABLE SILVER.crm_cust_info
        PRINT '>> INSERTING DATA INTO: SILVER.crm_cust_info';

        INSERT INTO SILVER.crm_cust_info (
        CST_ID,
        CST_KEY,
        CST_FIRSTNAME,
        CST_LASTNAME,
        CST_MARITAL_STATUS,
        CST_GNDR,
        CST_CREATE_DATE
        )
        SELECT
        CST_ID,
        CST_KEY,
        TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME,
        TRIM (CST_LASTNAME) AS CST_LASTNAME,
        CASE WHEN CST_MARITAL_STATUS = 'M' THEN 'MARRIED'
        WHEN CST_MARITAL_STATUS = 'S' THEN 'SINGLE'
        ELSE 'N/A'
        END AS CST_MARITAL_STATUS
        ,
        CASE WHEN CST_GNDR = 'M' THEN 'MALE'
        WHEN CST_GNDR = 'F' THEN 'FEMALE'
        ELSE 'N/A'
        END AS CST_GNDR,
        CST_CREATE_DATE
        FROM (

        SELECT*,
        ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE desc) AS FLAG_LAST
        FROM Bronze.crm_cus_info) T

        WHERE FLAG_LAST = 1 AND CST_ID IS NOT NULL

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        --
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE : silver.crm_pro_info';
        TRUNCATE TABLE silver.crm_pro_info
        PRINT '>> INSERTING DATA INTO: silver.crm_pro_info';
        INSERT INTO silver.crm_pro_info (
        Pro_id,
        Cat_id,
        Pro_key,
        Pro_nm,
        Pro_cost,
        Pro_line,
        Pro_start_dt,
        Pro_End_dt,
        dhw_create_date
        )
        SELECT
        Pro_id,
        REPLACE(SUBSTRING(Pro_key, 1, 5), '-', '_') AS Cat_id,
        REPLACE(SUBSTRING(Pro_key, 7, LEN(Pro_key)), '_', '-') AS Pro_key,
        TRIM(Pro_nm) AS Pro_nm,
        Product_Cost,
        Product_line,
        Pro_start_dt,
        DATEADD(DAY, -1, prd_end_date) AS Product_end_date,
        GETDATE() AS dhw_create_date
        FROM (
        SELECT *,
        LEAD(Pro_start_dt) OVER (
            PARTITION BY Pro_key 
            ORDER BY Pro_start_dt
        ) AS prd_end_date,

        CASE WHEN Pro_cost IS NULL THEN 0 ELSE Pro_cost END AS Product_Cost,

        CASE TRIM(Pro_line)
            WHEN 'M' THEN 'Mountain'
            WHEN 's' THEN 'Ship'
            WHEN 'r' THEN 'Road'
            WHEN 't' THEN 'Train'
            ELSE 'n/a'
        END AS Product_line,
        ROW_NUMBER() OVER (
            PARTITION BY Pro_nm 
            ORDER BY Pro_start_dt DESC
        ) AS Flag_last
        FROM bronze.crm_pro_info
        ) t
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        --
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE : silver.crm_sales_details_info';
        TRUNCATE TABLE silver.crm_sales_details_info
        PRINT '>> INSERTING DATA INTO: silver.crm_sales_details_info';


        insert into silver.crm_sales_details_info (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)
        select
        sls_ord_num,
        sls_prd_key,
        sls_cust_id, 
        case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
        else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
        else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
        else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,
        case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
        else sls_sales
        end sls_sales
        ,sls_quantity
        ,case when sls_price is null or sls_price <= 0 then sls_sales / sls_quantity
        else sls_price
        end sls_price
        from bronze.crm_sales_details_info
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


    --
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE : silver.erp_cus_info';
    TRUNCATE TABLE silver.erp_cus_info
    PRINT '>> INSERTING DATA INTO: silver.erp_cus_info';


    insert into silver.erp_cus_info(
    cid,
    bdate,
    gen)

    select
    case when cid like 'nas%' then substring (cid,4,len(cid))
    else cid
    end as cid,
    case when bdate < '1924-01-01' then null 
    when bdate > getdate () then null
    else bdate
    end bdate,
    case when gen = 'f' then 'female'
    when gen = 'm' then 'male'
    when gen is null then 'n/a'
    when gen = ' ' then 'n/a'
    else gen
    end gen
    from bronze.erp_cus_info
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


    --

    PRINT '------------------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '------------------------------------------------';

        SET @start_time = GETDATE();

        PRINT '>> TRUNCATING TABLE : SILVER.erp_cat_info';
        TRUNCATE TABLE SILVER.erp_cat_info
        PRINT '>> INSERTING DATA INTO: SILVER.erp_cat_info';

        INSERT INTO SILVER.erp_cat_info(
        id,
        CAT,
        SUBCAT,
        MAINTENANCE)
        select
        id,
        TRIM(CAT) CAT,
        TRIM(SUBCAT) SUBCAT,
        TRIM(MAINTENANCE) MAINTENANCE
        from bronze.erp_cat_info
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        --
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE : silver.erp_loc_info';
        TRUNCATE TABLE silver.erp_loc_info
        PRINT '>> INSERTING DATA INTO: silver.erp_loc_info';

        insert into silver.erp_loc_info
        (CID,
        cntry)

        select 
        replace(cid,'-','') cid,
        case when cntry = 'us' or cntry = 'usa' then 'United States'
        when cntry = 'de' then 'Germany'
        WHEN CNTRY IS NULL OR CNTRY = '' THEN 'N/A'
        ELSE Cntry
        END Cntry
        from bronze.erp_loc_info
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
		
END TRY
BEGIN CATCH
PRINT '=========================================='
PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '=========================================='
END CATCH

end

/*--TO EXECUTE--*/
exec bronze.load_bronze
exec silver.load_silver

