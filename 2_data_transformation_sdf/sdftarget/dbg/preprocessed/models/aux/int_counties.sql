with relevant_fields as (
  select
    county_id
    , county_name
    , dt as effective_dt
    , loaded_at
  from analytics.aux.stg_python_ingestor__iowa_liquor_sales
  where county_id is not null
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by county_id order by effective_dt desc, loaded_at desc) = 1
)

, agg_values as (
  select
    county_id
    , array_unique_agg(county_name) as county_names
  from relevant_fields
  group by 1
)

select
  current_values.*
  , agg_values.* exclude (county_id)
from current_values
inner join agg_values
  on current_values.county_id = agg_values.county_id
