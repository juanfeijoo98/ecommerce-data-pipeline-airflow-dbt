-- Métrica simple: ventas por producto
with orders as (
  select * from {{ ref('stg_orders') }}
)
select
  product,
  count(*)               as n_orders,
  sum(quantity)          as qty,
  sum(total)             as revenue,
  avg(price)             as avg_price
from orders
group by 1
order by revenue desc;
