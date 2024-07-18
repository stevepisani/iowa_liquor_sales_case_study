with relevant_fields as (
  select
    item_id
    , category_id
    , vendor_id
    , item_description
    , number_of_bottles_in_pack
    , bottle_volume_ml
    , state_bottle_cost
    , state_bottle_retail
    , dt as effective_dt
    , loaded_at
  from analytics.aux.stg_python_ingestor__iowa_liquor_sales
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by item_id order by effective_dt desc, loaded_at desc) = 1
)

, agg_values as (
  select
    item_id
    , array_unique_agg(category_id) as category_ids
    , array_unique_agg(vendor_id) as vendor_ids
    , array_unique_agg(item_description) as item_descriptions
    , array_unique_agg(number_of_bottles_in_pack) as number_of_bottles_in_packs
    , array_unique_agg(bottle_volume_ml) as bottle_volume_mls
    , array_unique_agg(state_bottle_cost) as state_bottle_costs
    , array_unique_agg(state_bottle_retail) as state_bottle_retails
  from relevant_fields
  group by 1
)

select
  current_values.*
  , agg_values.* exclude (item_id)
from current_values
inner join agg_values
  on current_values.item_id = agg_values.item_id