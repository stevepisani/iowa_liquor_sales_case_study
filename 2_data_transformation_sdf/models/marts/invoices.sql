

select
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

from analytics.aux.int_invoices as invoices
left join analytics.aux.int_categories as categories
  on invoices.category_id = categories.category_id
left join analytics.aux.int_items as items
  on invoices.item_id = items.item_id
left join analytics.aux.int_vendors as vendors
  on invoices.vendor_id = vendors.vendor_id
left join analytics.aux.int_stores as stores
  on invoices.store_id = stores.store_id