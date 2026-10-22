"""Kafka consumer: topic `orders` -> valid/rejected.

Peran di pipeline : jalur streaming yang menerapkan data contract yang sama
                    dengan jalur batch, lalu menulis ke sink yang idempoten.
Output            : data/valid/stream_valid.jsonl
                    data/rejected/stream_rejected.jsonl (+ topic/partition/offset)

Jaminan yang dinyatakan (harus ditulis jujur di laporan):
    at-least-once  (commit offset SETELAH sink berhasil)  +  sink idempoten
    berdasarkan order_id  =>  efek bisnis tanpa duplikasi.
    BUKAN exactly-once end-to-end (sink adalah file/DuckDB, bukan Kafka).

TODO:
    - [ ] enable.auto.commit=False; commit manual setelah sink sukses
    - [ ] tangani JSON rusak dan validasi gagal -> rejected, bukan crash
    - [ ] catat topic, partition, offset pada setiap record
    - [ ] sink upsert berdasarkan order_id (event duplikat tidak menambah baris)
    - [ ] opsi --stop-after N untuk demo terkontrol
"""
