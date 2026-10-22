-- TODO: select dari {{ source('raw', 'orders') }}
-- Tugas: rename/cast kolom (order_ts -> timestamp, order_date -> date),
--        pertahankan gross_amount & net_amount dari tahap transform.
select * from {{ source('raw', 'orders') }}
