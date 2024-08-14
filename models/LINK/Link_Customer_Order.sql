{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-13        Karthika      Initial version
-#}
{{-
    config(
       schema = 'LINK',
	   materialized='incremental',
	   unique_key = 'customer_order_hk'
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
        from {{ source('SRC_ORDER', 'STAGE_ORDER')}} 
		          )
        
select
    customer_order_hk,
    customer_hk,
	order_hk,
    load_dts,
    source
    from cus_ord_cte
	
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
where load_dts >= (select max(load_dts)) from {{ this }} )

{% endif %}