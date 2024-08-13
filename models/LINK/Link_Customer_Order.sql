{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-13        Karthika      Initial version
-#}
{{-
    config(
       materialized="incremental",
	   unique_key = "customer_order_hk"
    )
-}}

with
    cus_ord_cte as (
        select
		MD5(CONCAT(customer_id, order_id)) AS customer_order_hk,
        MD5(customer_id) AS customer_hk,
        MD5(order_id) AS order_hk,
        current_timestamp() as load_dts,
        src as Source           
        from DBT_DB.STAGE.STAGE_ORDER 
		          )
        
select
    customer_order_hk,
    customer_hk,
	order_hk,
    load_dts,
    source
    from cus_ord_cte