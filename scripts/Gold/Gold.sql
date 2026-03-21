/* GOLDEN LAYER */

/* Final Stage of the Data Warehousing Process */

/* to Load the Data */

/* Gold Layer for Product data Begin */

create or replace procedure gold.gold_prd_procudure()
language plpgsql
as $$
begin
	drop view IF EXISTS gold.gold_product_layer
	else
	create view gold.gold_product_layer as
		select 
				prd_id as Product_ID,
				prd_key as Product_Key,
				prd_name as Product_Name,
				prd_line as Product_Line,
				prd_start_dt as Product_Start_Date,
				prd_end_dt as Product_End_Date,
				cat as Product_Cathegory,
				subcat as Product_SubCathegory,
				maintenance as PRD_Maintenance
		from silver.product_data;
end;
$$;

call gold.gold_prd_procudure()

select * from gold.gold_product_layer


/* Gold Layer for Product data End */

/* Gold Layer for Customer data Begin */


create or replace procedure gold.gold_Customer_procudure()
language plpgsql
as $$
begin
	drop view IF EXISTS gold.gold_Customer_layer
	else
	create view gold.gold_Customer_layer as
		select 
				cst_id as Customer_ID,
				cst_key as Customer_Key,
				cst_firstname as Customer_FirstName,
				cst_lastname as Customer_LastName,
				bdate as Customer_BirthDate,
				cst_marital_status as Customer_Marital_Status,
				cst_gndr as Customer_Gender,
				cst_create_date as Customer_Creation_Date,
				cntry as Customer_Country
		from silver.customer_crm;
end;
$$;

call gold.gold_Customer_procudure()

select * from gold.gold_Customer_layer

/* Gold Layer for Customer data Ends */


/* Gold Layer for All Sales Data Begins*/


create or replace procedure gold.gold_Sales_Data_Procedure()
language plpgsql
as $$
begin
	drop view IF EXISTS gold.gold_Sales_Data
	else
	create view gold.gold_Sales_Data as
		select 
				sls_ord_num as Sales_Order_Num,
				sls_prd_key as Sales_Product_Key,
				prd_name as Product_Name,
				prd_line as Product_Line,
				cat as Product_Cathegory,
				subcat as Product_SubCathegory,
				sls_cust_id as Customer_ID,
				cst_name as Customer_Name,
				cst_gndr as Customer_Gender,
				bdate as Customer_BirthDate,
				cntry as Customer_Country,
				sls_order_dt as Sales_Order_Date,
				sls_ship_dt as Sales_Ship_Date,
				sls_due_dt as Sales_Due_Date,
				sls_sales as Selling_Price,
				sls_quantity as Sold_Quantity,
				sls_price as Total_Sale_Price
		from silver.sales_data;
end;
$$;

call gold.gold_Sales_Data_Procedure()

select * from gold.gold_Sales_Data

/* Gold Layer for All Sales Data Ends*/




