with relevant_fields as (
  select
    category_id
    , category_name
    , case 
        when category_name like '%whisk%' then 'whiskies'
        when category_name like '%rum%' then 'rums'
        when category_name like '%gin%' then 'gins'
        when category_name like '%vodka%' then 'vodkas'
        when category_name like '%liqueur%' or category_name like '%cordial%' then 'liqueurs & cordials'
        when category_name like '%tequila%' or category_name like '%mezcal%' then 'tequilas & mezcal'
        else 'specialty & miscellaneous'
      end as liquor_type_name
    , dt as effective_dt
    , loaded_at
  from analytics.aux.stg_python_ingestor__iowa_liquor_sales
  where category_id is not null
    and category_name is not null
)

, current_values as (
  select * from relevant_fields
  qualify row_number() over (partition by category_name order by effective_dt desc, loaded_at desc) = 1
)

, agg_values as (
  select
    category_name
    , array_unique_agg(category_id) as category_ids
  from relevant_fields
  group by 1
)

select
  current_values.*
  , agg_values.* exclude (category_name)
from current_values
inner join agg_values
  on current_values.category_name = agg_values.category_name