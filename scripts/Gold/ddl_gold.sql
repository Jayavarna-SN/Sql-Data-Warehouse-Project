/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers as
select 
row_number() over (order by cst_id) as Customer_key,
ci.cst_id Customer_id,
ci.cst_key Customer_number,
ci.cst_firstname First_name,
ci.cst_lastname Last_name,
li.cntry as Country,
ca.bdate as Birth_date,
case when ci.cst_gndr !='n/a' then ci.cst_gndr
else coalesce( ca.GEN ,'n/a')
end as Gender,
ci.cst_marital_status Marital_status,
ci.cst_create_date Create_date
from silver.crm_cust_info ci
left join silver.erp_cus_info ca
on ci.cst_key = ca.CID
left join silver.erp_loc_info li
on ci.cst_key = li.CID

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


IF OBJECT_ID('gold.dim_Products', 'V') IS NOT NULL
    DROP VIEW gold.dim_Products;
GO


create view gold.dim_Products as
select
row_number() over (order by p.pro_id) as Product_key,
p.Pro_id as Product_id,
p.Pro_key Product_number,
P.Pro_nm Product_name,
p.cat_id as Category_id,
c.cat as Category,
c.subcat Subcategory,
p.Pro_cost Product_cost,
p.Pro_line Product_line,
p.Pro_start_dt Product_start_date,
C.MAINTENANCE 
from silver.crm_pro_info p
left join silver.erp_cat_info c
on c.id = p.cat_id
where Pro_end_dt is null

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO


create view gold.fact_sales as
select
p.Product_key,
c.Customer_key,
sd.sls_ord_num order_number,
sd.sls_order_dt order_date,
sd.sls_ship_dt ship_date,
sd.sls_due_dt due_date,
sd.sls_sales sales_amount,
sd.sls_quantity quantity,
sd.sls_price price
from silver.crm_sales_details_info sd
left join gold.dim_Products p
on sd.sls_prd_key = p.product_number
left join gold.dim_customers c
on sd.sls_cust_id = c.Customer_id






