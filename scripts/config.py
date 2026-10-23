"""Konfigurasi terpusat: path, nama topic, dan parameter default.

Semua skrip mengimpor dari sini agar path tidak tersebar di banyak file.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# --- Data layers ---
SOURCE_CSV = ROOT / "data" / "source" / "orders.csv"
RAW_DIR = ROOT / "data" / "raw"
VALID_DIR = ROOT / "data" / "valid"
REJECTED_DIR = ROOT / "data" / "rejected"
STATE_DIR = ROOT / "data" / "state"

RAW_PARQUET = RAW_DIR / "orders.parquet"
WATERMARK_FILE = STATE_DIR / "watermark.json"
LOG_FILE = ROOT / "logs" / "ingestion.log"

# --- Warehouse ---
DUCKDB_PATH = ROOT / "warehouse" / "warehouse.duckdb"

# --- Batch ingestion ---
PAGE_SIZE = 10
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.0

# --- Kafka ---
KAFKA_BOOTSTRAP = "localhost:9092"
ORDERS_TOPIC = "orders"
CONSUMER_GROUP = "orders-consumer"
