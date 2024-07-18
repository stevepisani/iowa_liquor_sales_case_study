

create or replace table ANALYTICS.MARTS.STORES as (select
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
from ANALYTICS.AUX.INT_STORES);

comment if exists on table ANALYTICS.MARTS.STORES IS 'A denormalized table of stores with all relevant information.';
comment if exists on column ANALYTICS.MARTS.STORES.store_id IS 'A unique ID number for each store.';
