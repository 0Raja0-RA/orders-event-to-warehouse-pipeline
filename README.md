# Orders Event-to-Warehouse Pipeline

Pipeline data end-to-end untuk transaksi e-commerce: **ingestion batch inkremental** dan **streaming Kafka**,
**data contract** dengan Pydantic beserta audit rejected records, transformasi **Polars**, serta analytics layer
**DuckDB + dbt** lengkap dengan tests dan dokumentasi.

> Status: 🚧 dalam pengerjaan.

## Arsitektur

```text
CSV (simulasi REST API) -> batch extractor (retry + watermark) -> raw Parquet
Kafka producer -> topic "orders" -> consumer
                    \                 /
                     -> data contract (Pydantic) -> valid / rejected (+ alasan)
                                  -> transform Polars -> DuckDB -> dbt (staging, marts) -> tests + docs
```

## Struktur repositori

```text
.
├── data/
│   ├── source/        # data sumber (orders.csv)
│   ├── raw/           # raw Parquet (dibuat oleh pipeline, tidak di-commit)
│   ├── valid/         # record lolos contract
│   ├── rejected/      # record ditolak + alasan
│   └── watermark.json # checkpoint incremental loading (dibuat pipeline, tidak di-commit)
├── scripts/           # batch_ingest, validate, producer, consumer,
│                      # transform, benchmark, load_duckdb
├── warehouse/         # file DuckDB (tidak di-commit)
├── dbt_project/       # model staging & marts, tests, dokumentasi
├── docker-compose.yml # Kafka single-node (KRaft)
└── requirements.txt
```

## Menjalankan

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d

python scripts/batch_ingest.py
python scripts/validate.py
python scripts/producer.py
python scripts/consumer.py
python scripts/transform.py
python scripts/load_duckdb.py

cd dbt_project
dbt run && dbt test && dbt docs generate
```

## Keputusan desain utama

- **Idempotent by design**: watermark disimpan setelah penulisan sukses, ditambah dedup `order_id`.
- **At-least-once + sink idempoten** pada jalur Kafka; tidak mengklaim exactly-once end-to-end.
- **Satu data contract** dipakai bersama oleh jalur batch dan streaming.
- Data cacat dikarantina dengan alasan tertulis agar dapat diaudit, tidak dibuang diam-diam.

## Tech stack

Python · Pandas · Polars · PyArrow/Parquet · Pydantic · Apache Kafka · Docker Compose · DuckDB · dbt
