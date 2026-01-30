/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure bronze.load_bronze
as
Begin

declare @start_time datetime,@end_time datetime;
set @start_time = getdate();

	begin try 
	Print '==================================';
	Print 'loading the bronze layer';
	Print '==================================';

	Print '----------------------';
	Print 'loading the CRM Tables';
	Print '----------------------';

	set @start_time = getdate();

		truncate table bronze.crm_cus_info;
		bulk insert bronze.crm_cus_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);
		set @end_time = getdate();
		Print 'loading duration : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '----------------------';

		truncate table bronze.crm_pro_info;

		set @start_time = getdate();

		bulk insert bronze.crm_pro_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);

		set @end_time = getdate();
		Print 'loading duration : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '----------------------';

		truncate table bronze.crm_sales_details_info;

		set @start_time = getdate();

		bulk insert bronze.crm_sales_details_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);

		set @end_time = getdate();
		Print 'loading duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';


		Print '----------------------';
		Print 'loading the Erp Tables';
		Print '----------------------';

		truncate table bronze.erp_cus_info;

		set @start_time = getdate();

		bulk insert bronze.erp_cus_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);

		set @end_time = getdate();
		Print 'loading duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '----------------------';

		truncate table bronze.erp_cat_info;

		set @start_time = getdate();

		bulk insert bronze.erp_cat_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);

		set @end_time = getdate();
		Print 'loading duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '----------------------';

		truncate table bronze.erp_loc_info;

		set @start_time = getdate();

		bulk insert bronze.erp_loc_info 
		from 'C:\Users\jeyavarna\Desktop\data Analyst\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
		);

		set @end_time = getdate();
		Print 'loading duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '----------------------';

		Print '=======================';
		set @end_time = getdate();
		Print 'Total loading bronze layer duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'Seconds';
		Print '=======================';

	end try
	Begin catch
	Print '==================================';
	Print 'Error occured during loading bronze layer';
	Print 'Error message: ' + error_message();
	Print 'Error message: ' + cast(error_number() as nvarchar);
	Print 'Error message: ' + cast(error_state() as nvarchar);
	Print '==================================';
	end catch
	
end
