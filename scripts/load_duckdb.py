"""Muat record valid ke DuckDB (schema `raw`) sebagai sumber model dbt.

Peran di pipeline : jembatan antara file pipeline dan warehouse.
Input             : data/valid/orders_transformed.parquet
Output            : warehouse/warehouse.duckdb  -> tabel raw.orders

Prinsip: hanya record VALID yang boleh masuk; load idempoten
(rerun tidak menggandakan baris, mis. CREATE OR REPLACE atau upsert order_id).

TODO:
    - [ ] connect ke config.DUCKDB_PATH, buat schema raw
    - [ ] muat parquet -> raw.orders
    - [ ] assert: COUNT(*) = jumlah valid, tidak ada order_id ganda
    - [ ] (opsional) muat ringkasan rejected ke raw.rejected_orders untuk
          menjawab pertanyaan "penyebab penolakan terbesar"
"""
