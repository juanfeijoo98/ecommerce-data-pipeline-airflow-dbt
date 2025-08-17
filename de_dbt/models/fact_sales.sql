with src as (
  select
    order_id,
    product,
    quantity::int            as qty,
    price::numeric(12,2)     as unit_price_eur,
    total::numeric(12,2)     as revenue_eur
  from {{ ref('stg_orders') }}
)
select
  md5(product)::varchar(32)  as product_key,
  order_id,
  qty,
  unit_price_eur,
  revenue_eur
from src
