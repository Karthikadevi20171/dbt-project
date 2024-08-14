{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-13        Karthika      Initial version
-#}
{{-
    config(
       schema = 'SATELLITE',
	   materialized='incremental',
	   unique_key = 'customer_HK'
 
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
        from {{ source('SRC_CUSTOMER', 'STAGE_CUSTOMER')}}
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
	
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
where load_dts >= (select max(load_dts)) from {{ this }} )

{% endif %}