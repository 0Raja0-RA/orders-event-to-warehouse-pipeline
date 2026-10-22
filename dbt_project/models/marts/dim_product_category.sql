-- TODO: satu baris per product_category
select distinct product_category from {{ ref('stg_orders') }}
