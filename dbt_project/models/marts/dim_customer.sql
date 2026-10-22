-- TODO: satu baris per customer_id (select distinct dari ref('stg_orders'))
select distinct customer_id from {{ ref('stg_orders') }}
