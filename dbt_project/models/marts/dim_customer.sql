-- Dimensi pelanggan: satu baris per customer_id.
select
    customer_id,
    min(order_date) as first_order_date,
    count(*) as total_orders
from {{ ref('stg_orders') }}
group by customer_id
