

create or replace table ANALYTICS_DEV.MARTS.INVOICES as (select
  invoices.invoice_id
  , invoices.invoice_date
  , invoices.state_bottle_cost
  , invoices.state_bottle_retail
  , invoices.number_of_bottles_sold
  , invoices.sale_in_dollars
  , invoices.cost_in_dollars
  , invoices.profit_in_dollars
  , invoices.volume_sold_liters
  , invoices.volume_sold_gallons

  -- Store information
  , invoices.store_id
  , stores.store_name
  , stores.store_brand_name

  -- County information
  , invoices.category_id
  , categories.category_name
  , categories.liquor_type_name

  -- Vendor information
  , invoices.vendor_id
  , vendors.vendor_name

  -- Item information
  , invoices.item_id
  , items.item_description
  , items.number_of_bottles_in_pack
  , items.bottle_volume_ml

from ANALYTICS_DEV.AUX.INT_INVOICES as invoices
left join ANALYTICS_DEV.AUX.INT_CATEGORIES as categories
  on invoices.category_id = categories.category_id
left join ANALYTICS_DEV.AUX.INT_ITEMS as items
  on invoices.item_id = items.item_id
left join ANALYTICS_DEV.AUX.INT_VENDORS as vendors
  on invoices.vendor_id = vendors.vendor_id
left join ANALYTICS_DEV.AUX.INT_STORES as stores
  on invoices.store_id = stores.store_id);

comment if exists on table ANALYTICS_DEV.MARTS.INVOICES IS 'A denormalized table of invoices with all relevant information.';
comment if exists on column ANALYTICS_DEV.MARTS.INVOICES.invoice_id IS 'A unique ID number for each invoice.';
