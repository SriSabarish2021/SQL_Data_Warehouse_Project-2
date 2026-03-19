/*	BRONZE LAYER */

/* BEGIN WITH THE CRM ON TH EBRONZE LAYER */




CREATE OR REPLACE PROCEDURE bronze.create_CRM_data()
LANGUAGE plpgsql
AS $body$
BEGIN
	DROP TABLE IF EXISTS bronze.customer_CRM;
    Create table bronze.customer_CRM(
		cst_id varchar(20),
		cst_key varchar(30) ,
		cst_firstname varchar(50),
		cst_lastname varchar(50) ,
		cst_marital_status varchar(50) ,
		cst_gndr varchar(10) ,
		cst_create_date date 
	);
	
	DROP TABLE IF EXISTS bronze.sales_details_CRM;
	Create table bronze.sales_details_CRM(
		sls_ord_num varchar(50) ,
		sls_prd_key varchar(50) ,
		sls_cust_id varchar(50) ,
		sls_order_dt date ,
		sls_ship_dt date ,
		sls_due_dt date ,
		sls_sales varchar(50),
		sls_quantity varchar(50) ,
		sls_price varchar(50) 
	
	);
	DROP TABLE IF EXISTS  bronze.product_CRM;
	Create table bronze.product_CRM(
		prd_id varchar(20) ,
		prd_key varchar(30),
		prd_nm varchar(50) ,
		prd_cost varchar(50),
		prd_line varchar(50) ,
		prd_start_dt date ,
		prd_end_dt date 
	);

	


END;
$body$;

call bronze.create_CRM_data();



CREATE OR REPLACE PROCEDURE bronze.create_ERP_data()
LANGUAGE plpgsql
AS $body$
BEGIN
	/* Creating Table for the ERP dateset */

	DROP TABLE IF EXISTS bronze.customer_data_ERP;
	Create table bronze.customer_data_ERP(
		CID varchar(20) ,
		BDATE varchar(30),
		GEN varchar(50) 
	);

	DROP TABLE IF EXISTS bronze.customer_location_ERP;
	Create table bronze.customer_location_ERP(
		CID varchar(20) ,
		CNTRY varchar(30)
	);


	DROP TABLE IF EXISTS bronze.product_cathe_ERP;
	Create table bronze.product_cathe_ERP(
		ID varchar(50) ,
		CAT varchar(50) ,
		SUBCAT varchar(50) ,
		MAINTENANCE varchar(50) 

	);


END;
$body$;

call bronze.create_ERP_data();

/* Bronze layer incerting a data to created Tables */

/* Here the data was imported by selecting file and importing with the data import option */






