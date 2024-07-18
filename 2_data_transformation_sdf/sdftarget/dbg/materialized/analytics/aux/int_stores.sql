

create or replace view ANALYTICS.AUX.INT_STORES as (with relevant_fields as (
  select
    store_id
    , county_id
    , store_name
    , coalesce(
      trim(
        regexp_substr(
          case
            when store_name like '%-%' then split_part(store_name, '-', 1)
            when store_name like '%/%' then split_part(store_name, '/', 1)
            when store_name like '% of %' then split_part(store_name, ' of ', 1)
            when store_name like '% & %' then split_part(store_name, ' & ', 1)
            when store_name like '%    %' then split_part(store_name, '    ', 1)
            else store_name
          end
          , '^([^#0-9]+)'
        )
      )
      , store_name
    ) as store_brand_name
    , store_address
    , store_city
    , store_zipcode
    , store_location_point
    , dt as effective_dt
    , loaded_at
  from ANALYTICS.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES
  where store_id is not null
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by store_id order by effective_dt desc, loaded_at desc) = 1
)

, agg_values as (
  select
    store_id
    , array_unique_agg(county_id) as county_ids
    , array_unique_agg(store_name) as store_names
    , array_unique_agg(store_address) as store_addresses
    , array_unique_agg(store_city) as store_cities
    , array_unique_agg(store_zipcode) as store_zipcodes
    -- , array_unique_agg(store_location_point) as store_location_points -- geo type cannot be aggregated in array
  from relevant_fields
  group by 1
)

select
  current_values.*
  , agg_values.* exclude (store_id)
from current_values
inner join agg_values
  on current_values.store_id = agg_values.store_id);

