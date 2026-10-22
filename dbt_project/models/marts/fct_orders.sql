-- TODO: grain = 1 baris per order_id; kolom FK + gross_amount, net_amount, status, order_date
select * from {{ ref('stg_orders') }}
