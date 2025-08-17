-- Normalizo tipos y nombres desde public.orders
select
  order_id::int        as order_id,
  product::text        as product,
  quantity::int        as quantity,
  price::numeric(10,2) as price,
  total::numeric(12,2) as total
from public.orders;

