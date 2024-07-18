

create or replace view ANALYTICS_DEV.AUX.INT_COUNTIES as (with relevant_fields as (
  select
    county_id
    , county_name
    , dt as effective_dt
    , loaded_at
  from ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES
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
  on current_values.county_id = agg_values.county_id);

comment if exists on view ANALYTICS_DEV.AUX.INT_COUNTIES IS 'Intermediate table for counties.';
comment if exists on column ANALYTICS_DEV.AUX.INT_COUNTIES.county_id IS 'A unique ID number for each county.';
