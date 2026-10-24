-- Agregat penjualan bersih per tanggal, kategori produk, dan metode pembayaran.
-- Hanya pesanan yang diakui sebagai penjualan (paid, shipped).
select
    order_date,
    product_category,
    payment_method,
    count(*) as order_count,
    sum(net_amount) as net_sales
from {{ ref('fct_orders') }}
where is_recognized_sale
group by order_date, product_category, payment_method
