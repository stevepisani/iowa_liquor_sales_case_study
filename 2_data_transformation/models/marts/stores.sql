{{ config(schema='marts') }}

select
  store_id
  , county_id
  , store_name
  , store_brand_name
  , store_address
  , store_city
  , store_zipcode
  , store_location_point
  , store_id
  , county_ids
  , store_names
  , store_addresses
  , store_cities
  , store_zipcodes
  , effective_dt
  , loaded_at
from {{ ref('int_stores') }}
