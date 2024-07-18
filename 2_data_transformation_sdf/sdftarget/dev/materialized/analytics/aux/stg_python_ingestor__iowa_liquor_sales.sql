

create or replace view ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES as (with src as (
  select * from INGEST_DATABASE.IOWA_LIQUOR_SALES.RAW_DATA
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

select * from clean_up);

comment if exists on view ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES IS 'This table contains all of the data from the Iowa Liquor Sales dataset with some basic transformations applied.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.vendor_name IS 'The name of the company that produced or distributed the liquor product sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.item_id IS 'A numeric ID used to identify the specific liquor product sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.invoice_id IS 'A unique ID number for each liquor sale invoice.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.number_of_bottles_sold IS 'The total number of bottles of a particular liquor product sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.sale_in_dollars IS 'The total dollar amount of a single liquor sale.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.loaded_at IS 'The timestamp when the liquor sale data was loaded into the database.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_name IS 'The name of the liquor store where the sale occurred.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.vendor_id IS 'A numeric ID used to identify the company that produced or distributed the liquor product sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.category_id IS 'A numeric ID used to categorize the type of liquor, such as wine, beer or spirits.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.volume_sold_liters IS 'The total volume of liquor sold in liters.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_location_point IS 'The geographic coordinates of the liquor store where the sale occurred.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.category_name IS 'The name of the liquor category, such as wine, beer or spirits.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.item_description IS 'A description of the specific liquor product sold, such as 'Cabernet Sauvignon' or 'Vodka'.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_address IS 'The street address of the liquor store where the sale occurred.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.county_id IS 'A numeric ID used to identify the county where the liquor was sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.bottle_volume_ml IS 'The volume in milliliters of an individual bottle of liquor sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_zipcode IS 'The ZIP code of the area where the liquor store that made the sale is located.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.state_bottle_retail IS 'The recommended retail price per bottle of a particular liquor product set by the state.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.volume_sold_gallons IS 'The total volume of liquor sold in gallons.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_city IS 'The city where the liquor store that made the sale is located.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.county_name IS 'The name of the county where the liquor was sold.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.state_bottle_cost IS 'The wholesale cost per bottle of a particular liquor product paid by the state.';
comment if exists on column ANALYTICS_DEV.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES.store_id IS 'A unique ID used to identify the liquor store where the sale occurred.';
