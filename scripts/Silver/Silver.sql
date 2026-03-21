/* SILVER LAYER */

/* Here the Datas were involve in the process of Transforming into valid and proper Way */


/* Customer Creation Data Validation Begin */


create or replace procedure silver.silver_customer_data_all()
language plpgsql
as $$
begin
	drop table IF EXISTS silver.customer_CRM
	else
	CREATE TABLE silver.customer_CRM(
		cst_id VARCHAR(50),
		cst_key varchar(50),
		cst_firstname varchar(50),
		cst_lastname varchar(50),
		bdate varchar(50),
		cst_marital_status varchar(50),
		cst_gndr varchar(50),
		cst_create_date varchar(50),
		cntry varchar(50)
		);
	
	
	truncate silver.customer_CRM;
	insert into silver.customer_CRM (
		cst_id ,
		cst_key ,
		cst_firstname ,
		cst_lastname ,
		bdate,
		cst_marital_status ,
		cst_gndr ,
		cst_create_date,
		cntry
		
	)
	select 
		cst_id ,
		cst_key ,
		case 
			when trim(cst_firstname) is null
			then ''
			else trim(cst_firstname) 
			
		end as cst_firstname,
		case 
			when trim(cst_lastname) is null
			then ''
			else trim(cst_lastname)
		end as cst_lastname,
		case 
			when bdate is null
			then 'Not Mentioned'
			else bdate
		end as CST_Birth_Date,
		case 
			when trim(cst_marital_status) = 'M' or trim(cst_marital_status)='m'
			then 'Married'
			when trim(cst_marital_status) = 'S' or trim(cst_marital_status)='s'
			then 'Single'
			else 'Not Mentioned'
		End cst_marital_status,
		case 
			when trim(cst_gndr)= 'M' or trim(cst_gndr)='m'
			then 'Male'
			when trim(cst_gndr)='F' or trim(cst_gndr)='f'
			then 'Female'
			else 'Not Interested'
		end	cst_gndr ,
		cst_create_date,
		case 
			when cntry is null
			then 'No Entry'
			else cntry
		end as Cst_Country
		
		from (
	select * from bronze.customer_CRM AS customer_crm
	left join bronze.customer_data_erp as customer_erp
	on customer_crm.cst_key = right(cid,(length(cid)-3))
	left join bronze.customer_location_erp as bronze_location
	on customer_crm.cst_key=replace(bronze_location.cid,'-','') 
	)where cst_id is not null;
end;
$$;


call silver.silver_customer_data_all()

select * from silver.customer_CRM;

/* Customer Creation Data Validation Ended */


/* Product Creation Data Validation Begin */ 




create or replace procedure silver.silver_product_data_all()
language plpgsql
as $$
begin
	drop table IF EXISTS silver.product_data
	else
	CREATE TABLE silver.product_data(
		prd_id VARCHAR(50),
		prd_key varchar(50),
		prd_name varchar(50),
		prd_cost varchar(50),
		prd_line varchar(50),
		prd_start_dt varchar(50),
		prd_end_dt varchar(50),
		cat varchar(50),
		subcat varchar(50),
		maintenance varchar(50)
		);
	
	
	truncate silver.product_data;
	insert into silver.product_data(
		prd_id,
		prd_key,
		prd_name,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt,
		cat,
		subcat,
		maintenance		
	)
	select 
		prd_id ,
		prd_key ,
		prd_nm as prd_name,
		case 
			when prd_cost is null
			then 'not defined'
			else prd_cost
		end as prd_cost,
		case 
			when trim(lower(prd_line)) = 'm'
			then 'Mountain'
			when trim(lower(prd_line)) = 'r'
			then 'Road'
			when trim(lower(prd_line)) = 's'
			then 'Othere Sales'
			when trim(lower(prd_line)) = 't'
			then 'Touring'
			else 'N/a'
		End as prd_line,
		prd_start_dt,
		prd_end_dt,
		case 
			when cat is null
			then 'Not Assign'
			else cat
		end as cat,
		case
			when subcat is null
			then 'Not Assign'
			else subcat
		end as subcat,
		case
			when maintenance is null
			then 'No Maintenance'
			else maintenance
		end as maintenance	
		from (
	select * from bronze.product_crm AS product_crm
	left join bronze.product_cathe_erp as product_erp
	on replace(left(product_crm.prd_key,5),'-','_') = product_erp.id 
	)where prd_id is not null;
end;
$$;


call silver.silver_product_data_all()

select * from silver.product_data

/* Product Creation Data Validation End */ 

/* Sales Data Validation Starts */

create or replace procedure silver.sales_data_all()
language plpgsql
as $$
begin
	drop table IF EXISTS silver.sales_data
	else
	CREATE TABLE silver.sales_data(
		sls_ord_num varchar(50),
		sls_prd_key varchar(50),
		prd_name varchar(50),
		prd_line varchar(50),
		CAT varchar(50),
		SUBCAT varchar(50),
		sls_cust_id varchar(50),
		cst_name varchar(50),
		cst_gndr varchar(50),
		BDATE varchar(50),
		CNTRY varchar(50),
		sls_order_dt varchar(50),
		sls_ship_dt varchar(50),
		sls_due_dt varchar(50),
		sls_sales varchar(50),
		sls_quantity varchar(50),
		sls_price varchar(50)

		);
	
	
	truncate silver.sales_data;
	insert into silver.sales_data (
		sls_ord_num ,
		sls_prd_key ,
		prd_name,
		prd_line,
		CAT,
		SUBCAT,
		sls_cust_id,
		cst_name,
		cst_gndr,
		BDATE,
		CNTRY,
		sls_order_dt ,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		
	)
	select 		
		sls_ord_num ,
		sls_prd_key ,
		prd_name,
		prd_line,
		CAT,
		SUBCAT,
		sls_cust_id,
		cst_firstname ||' '|| cst_lastname as cst_name,
		cst_gndr,
		BDATE,
		CNTRY,
		CASE 
		    WHEN sls_order_dt ~ '^-?[0-9]{8}$'
		    THEN TO_DATE(REPLACE(sls_order_dt, '-', ''), 'YYYYMMDD')
		    ELSE NULL
		END AS order_date,
		CASE 
		    WHEN sls_ship_dt ~ '^-?[0-9]{8}$'
		    THEN TO_DATE(REPLACE(sls_ship_dt, '-', ''), 'YYYYMMDD')
		    ELSE NULL
		END AS Ship_date,
		CASE 
		    WHEN sls_due_dt ~ '^-?[0-9]{8}$'
		    THEN TO_DATE(REPLACE(sls_due_dt, '-', ''), 'YYYYMMDD')
		    ELSE NULL
		END AS sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_quantity::integer*sls_sales::integer as sls_price
		from (
	select * from bronze.sales_details_crm AS sales_crm
	left join silver.customer_CRM as customer_crm
	on sales_crm.sls_cust_id = customer_crm.cst_id
	left join silver.product_data as silver_product
	on sales_crm.sls_prd_key=right(silver_product.prd_key,10)
	)where prd_name is not null and bdate < 
    sls_order_dt and bdate != 'Not Mentioned'
	and bdate < 
    sls_due_dt and bdate != 'Not Mentioned'
	and bdate < 
    sls_ship_dt and bdate != 'Not Mentioned';
end;
$$;


call silver.sales_data_all()

select * from silver.sales_data


/* Sales Data Validation Ends */


