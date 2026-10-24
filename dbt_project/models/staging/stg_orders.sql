-- Staging: nama kolom dan tipe data konsisten. Satu baris per order_id.
select
    order_id,
    customer_id,
    order_ts,
    cast(order_ts as date) as order_date,
    product_category,
    payment_method,
    status,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    net_amount
from {{ source('raw', 'orders') }}
