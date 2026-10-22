-- TODO: group by order_date, product_category, payment_method; sum(net_amount) sebagai net_sales
-- Terapkan aturan status penjualan yang sama dengan yang ditulis di _marts.yml
select * from {{ ref('fct_orders') }}
