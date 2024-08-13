{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-13        Karthika      Initial version
-#}
{{-
    config(
       materialized="incremental",
	   unique_key = "customer_HK"
 
    )
-}}

with
    customer_details_cte as (
        select
		MD5(customer_id) as customer_HK,
		customer_id,
		customer_name,
		customer_email,
        customer_phone,
		current_timestamp() as load_dts,
		src as source           
        from DBT_DB.STAGE.STAGE_CUSTOMER
           )
        
select
    customer_HK,
    customer_id,
	customer_name,
	customer_email,
    customer_phone,
    load_dts,
    source
    from customer_details_cte