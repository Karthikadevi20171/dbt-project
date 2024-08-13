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
    customer_cte as (
        select
		MD5(customer_id) as customer_HK,
		customer_id as customer_id,
		current_timestamp() as load_dts,
		src as source           
        from DBT_DB.STAGE.STAGE_CUSTOMER 
           )
        
select
    customer_HK,
    customer_id,
    load_dts,
    source
    from customer_cte