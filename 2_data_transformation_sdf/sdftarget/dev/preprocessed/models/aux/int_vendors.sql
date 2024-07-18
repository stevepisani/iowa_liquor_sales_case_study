with relevant_fields as (
  select
    vendor_id
    , vendor_name
    , dt as effective_dt
    , loaded_at
  from analytics.aux.stg_python_ingestor__iowa_liquor_sales
  where vendor_id is not null
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by vendor_id order by effective_dt desc, loaded_at desc) = 1
)

, agg_values as (
  select
    vendor_id
    , array_unique_agg(vendor_name) as vendor_names
  from relevant_fields
  group by 1
)

select
  current_values.*
  , agg_values.* exclude (vendor_id)
from current_values
inner join agg_values
  on current_values.vendor_id = agg_values.vendor_id
