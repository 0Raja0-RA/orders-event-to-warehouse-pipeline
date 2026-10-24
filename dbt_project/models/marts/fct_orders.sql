-- Fakta pesanan: satu baris per order_id (grain), berisi foreign key dan metrik transaksi.
-- is_recognized_sale: hanya status paid dan shipped dihitung sebagai penjualan;
-- cancelled dan refunded dikecualikan dari penjualan tetapi tetap ada di fakta.
select
    order_id,
    customer_id,
    product_category,
    payment_method,
    status,
    status in ('paid', 'shipped') as is_recognized_sale,
    order_ts,
    order_date,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    net_amount
from {{ ref('stg_orders') }}
