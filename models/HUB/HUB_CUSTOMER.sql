{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-13        Karthika      Initial version
-#}
{{-
    config(
       schema = 'HUB',
	   materialized='incremental',
	   unique_key = 'customer_HK'

    )
-}}

with
    customer_cte as (
        select
		MD5(customer_id) as customer_HK,
		customer_id as customer_id,
		current_timestamp() as load_dts,
		src as source           
        from {{ source('SRC_CUSTOMER', 'STAGE_CUSTOMER')}}
           )
        
select
    customer_HK,
    customer_id,
    load_dts,
    source
    from customer_cte
	
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
where load_dts >= (select max(load_dts)) from {{ this }} )

{% endif %}