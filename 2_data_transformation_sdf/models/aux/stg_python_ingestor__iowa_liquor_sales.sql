with src as (
  select * from ingest_database.iowa_liquor_sales.raw_data
)

, clean_up as (
  select
    cast(
      substring("Invoice/Item Number", charindex('-', "Invoice/Item Number") + 1, len("Invoice/Item Number")) as bigint
    ) as invoice_id
    , date("Date") as dt
    , trim(lower("Store Number")) as store_id
    , trim(lower("Store Name")) as store_name
    , trim(lower("Address")) as store_address
    , trim(lower("City")) as store_city
    , "Zip Code" as store_zipcode
    , to_geography("Store Location") as store_location_point
    , "County Number" as county_id
    , trim(lower("County")) as county_name
    , "Category" as category_id
    , trim(lower("Category Name")) as category_name
    , "Vendor Number" as vendor_id
    , trim(lower("Vendor Name")) as vendor_name
    , "Item Number" as item_id
    , trim(lower("Item Description")) as item_description
    , "Pack" as number_of_bottles_in_pack
    , "Bottle Volume (ml)" as bottle_volume_ml
    , "State Bottle Cost" as state_bottle_cost
    , "State Bottle Retail" as state_bottle_retail
    , "Bottles Sold" as number_of_bottles_sold
    , "Sale (Dollars)" as sale_in_dollars
    , "Volume Sold (Liters)" as volume_sold_liters
    , "Volume Sold (Gallons)" as volume_sold_gallons
    , loaded_at
  from src
)

select * from clean_up