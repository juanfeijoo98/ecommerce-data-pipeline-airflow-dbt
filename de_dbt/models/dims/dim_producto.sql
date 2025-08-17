-- Dimensión mínima de producto desde staging
with src as (
    select
        product,
        avg(price)::numeric(12,2) as avg_price
    from {{ ref('stg_orders') }}
    group by 1
)
select
    md5(product)::varchar(32)   as product_key,  -- surrogate key
    product,
    avg_price
from src
