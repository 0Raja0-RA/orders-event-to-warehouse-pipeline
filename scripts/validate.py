"""Validasi batch: raw Parquet -> valid + rejected.

Peran di pipeline : memisahkan data layak dari data cacat SEBELUM masuk
                    warehouse. Raw layer tidak diubah (tetap bisa diaudit).
Input             : data/raw/orders.parquet
Output            : data/valid/orders_valid.parquet
                    data/rejected/orders_rejected.csv   (payload asal + error_reason)
Dataset uji       : 36 baris, 6 di antaranya sengaja cacat (O1031..O1036).

TODO:
    - [ ] baca raw, validasi per record memakai contract.validate_record
    - [ ] tulis valid dan rejected (beserta kolom error_reason)
    - [ ] cetak ringkasan: dibaca / valid / ditolak / alasan terbanyak
    - [ ] tentukan & dokumentasikan mode: quarantine per-record (default)
          atau fail-fast per batch
"""
