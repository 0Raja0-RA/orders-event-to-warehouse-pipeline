"""Transformasi record valid dengan Polars (lazy).

Peran di pipeline : menghitung metrik bisnis dan menstandarkan tipe sebelum
                    dimuat ke DuckDB.
Input             : data/valid/*.parquet
Output            : data/valid/orders_transformed.parquet

Rumus:
    gross_amount = quantity * unit_price
    net_amount   = gross_amount * (1 - discount_pct)   # 0.10 = diskon 10%

TODO:
    - [ ] pl.scan_parquet(...) -> with_columns(...) -> collect()
    - [ ] normalisasi order_ts ke datetime UTC, tipe numerik konsisten
    - [ ] gabungkan hasil batch + stream, dedup berdasarkan order_id
    - [ ] cek manual 2-3 baris terhadap hitungan tangan (bukti kebenaran)
"""
