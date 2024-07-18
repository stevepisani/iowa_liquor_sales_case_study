

create or replace view ANALYTICS.AUX.INT_INVOICES as (with relevant_fields as (
  select
    invoice_id
    , store_id
    , county_id
    , category_id
    , vendor_id
    , item_id
    , dt as invoice_date
    , state_bottle_cost
    , state_bottle_retail
    , number_of_bottles_sold
    , sale_in_dollars
    , state_bottle_cost * number_of_bottles_sold as cost_in_dollars
    , sale_in_dollars - cost_in_dollars as profit_in_dollars
    , volume_sold_liters
    , volume_sold_gallons
    , loaded_at
  from ANALYTICS.AUX.STG_PYTHON_INGESTOR__IOWA_LIQUOR_SALES
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by invoice_id order by invoice_date desc, loaded_at desc) = 1
)

select * from current_values);

