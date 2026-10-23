"""Batch ingestion: CSV (simulasi REST API berpaginasi) -> raw Parquet.

Peran di pipeline : jalur batch. Mengambil data historis per halaman, tahan error
                    sementara, dan aman dijalankan ulang (idempotent).
Input             : data/source/orders.csv
Output            : data/raw/orders.parquet, data/state/watermark.json, logs/ingestion.log

Keputusan desain
    - Raw layer menyimpan SEMUA kolom sebagai string, apa adanya dari sumber.
      Nilai cacat (mis. "not-a-date") tidak dibuang di sini; contract yang menilainya.
    - Watermark = order_ts terbesar yang sudah tersimpan di raw. Disimpan SETELAH
      Parquet berhasil ditulis dan dibaca ulang, sehingga crash tidak menghilangkan data.
    - Idempotensi dijamin dedup order_id (upsert: versi terbaru menang), bukan watermark
      saja. Baris dengan order_ts tidak terbaca tidak disaring watermark, supaya sampai
      ke contract; dedup mencegahnya tertulis ulang.
    - Batasan: order_id kosong tidak bisa dideduplikasi (contract yang menolaknya).
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import config
from retry import TransientError, call_with_retry

logger = logging.getLogger("batch_ingest")

FAIL_PAGE = 1  # halaman tempat kegagalan sementara disimulasikan
EPOCH = "1970-01-01T00:00:00+00:00"


class CsvPageSource:
    """Adapter sumber. Ganti get_page() dengan requests.get() untuk API sungguhan:
    timeout, cek status HTTP, Retry-After, dan autentikasi diletakkan di sini."""

    def __init__(self, path, simulate_failures=0):
        self.df = pd.read_csv(path, dtype=str, keep_default_na=False)
        self._failures_left = simulate_failures

    def get_page(self, page, page_size):
        if page == FAIL_PAGE and self._failures_left > 0:
            self._failures_left -= 1
            raise TransientError("503 Service Unavailable (simulasi)")
        start = page * page_size
        return self.df.iloc[start:start + page_size].copy()


def setup_logging(log_file):
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)


def load_watermark(path):
    path = Path(path)
    if not path.exists():
        return EPOCH
    return json.loads(path.read_text(encoding="utf-8")).get("watermark", EPOCH)


def save_watermark(path, watermark):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"watermark": watermark, "updated_at": datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_ts(series):
    """String -> timestamp UTC; nilai tidak valid menjadi NaT (hanya untuk perbandingan watermark)."""
    return pd.to_datetime(series, errors="coerce", utc=True, format="ISO8601")


def fetch_all_pages(source, page_size, max_retries, base_delay):
    chunks, page = [], 0
    while True:
        chunk = call_with_retry(
            lambda: source.get_page(page, page_size),
            max_retries=max_retries, base_delay=base_delay,
            label=f"halaman {page}", logger=logger,
        )
        if chunk.empty:
            break
        logger.info("halaman %s dibaca: %s record", page, len(chunk))
        chunks.append(chunk)
        page += 1
    data = pd.concat(chunks, ignore_index=True) if chunks else source.df.iloc[0:0].copy()
    return data, page


def write_parquet_atomic(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    os.replace(tmp, path)


def run_ingest(source, raw_path, watermark_path, page_size, max_retries,
               base_delay, lookback_hours=0):
    watermark = load_watermark(watermark_path)
    cutoff = pd.Timestamp(watermark) - pd.Timedelta(hours=lookback_hours)
    logger.info("mulai ingest | watermark=%s | lookback=%sjam", watermark, lookback_hours)

    data, pages = fetch_all_pages(source, page_size, max_retries, base_delay)
    records_read = len(data)

    ts = parse_ts(data["order_ts"])
    candidates = data[ts.isna() | (ts > cutoff)]

    raw_path = Path(raw_path)
    if raw_path.exists():
        existing = pd.read_parquet(raw_path)
    else:
        existing = data.iloc[0:0]
    combined = pd.concat([existing, candidates], ignore_index=True)
    combined = combined.drop_duplicates("order_id", keep="last")
    appended = len(combined) - len(existing)

    write_parquet_atomic(combined, raw_path)
    verified = len(pd.read_parquet(raw_path))
    if verified != len(combined):
        raise RuntimeError(f"verifikasi gagal: tertulis {verified}, seharusnya {len(combined)}")

    max_ts = parse_ts(combined["order_ts"]).max()
    new_watermark = max(pd.Timestamp(watermark), max_ts).isoformat() if pd.notna(max_ts) else watermark
    save_watermark(watermark_path, new_watermark)

    summary = {
        "records_read": records_read,
        "pages": pages,
        "after_watermark": len(candidates),
        "appended": appended,
        "duplicates_skipped": len(candidates) - appended,
        "total_in_raw": len(combined),
        "watermark_before": watermark,
        "watermark_after": new_watermark,
    }
    logger.info("selesai | %s", json.dumps(summary))
    return summary


def print_summary(s):
    print("\n=== Ringkasan run ===")
    print(f"dibaca dari sumber   : {s['records_read']} record ({s['pages']} halaman)")
    print(f"lolos filter watermark: {s['after_watermark']}")
    print(f"ditambahkan          : {s['appended']}")
    print(f"duplikat (di-upsert) : {s['duplicates_skipped']}")
    print(f"total di raw         : {s['total_in_raw']}")
    print(f"watermark            : {s['watermark_before']} -> {s['watermark_after']}")
    print("ditolak contract     : dilaporkan oleh validate.py (raw tidak disaring di sini)")


def main():
    ap = argparse.ArgumentParser(description="Batch ingestion CSV -> raw Parquet")
    ap.add_argument("--source", default=str(config.SOURCE_CSV))
    ap.add_argument("--out", default=str(config.RAW_DIR), help="folder output raw Parquet (orders.parquet)")
    ap.add_argument("--watermark", "--checkpoint", default=str(config.WATERMARK_FILE))
    ap.add_argument("--page-size", type=int, default=config.PAGE_SIZE)
    ap.add_argument("--max-retries", type=int, default=config.MAX_RETRIES)
    ap.add_argument("--base-delay", type=float, default=config.BACKOFF_BASE_SECONDS)
    ap.add_argument("--lookback-hours", type=float, default=0, help="mundur dari watermark untuk data telat")
    ap.add_argument("--simulate-failures", type=int, default=0,
                    help="jumlah kegagalan 503 berturut-turut pada halaman 1 (demo retry)")
    ap.add_argument("--reset", action="store_true", help="hapus raw Parquet & watermark sebelum run (demo bersih)")
    args = ap.parse_args()

    raw_path = Path(args.out) / config.RAW_PARQUET.name
    setup_logging(config.LOG_FILE)
    if args.reset:
        for path in (raw_path, args.watermark):
            Path(path).unlink(missing_ok=True)
        logger.info("reset: raw dan watermark dihapus")

    source = CsvPageSource(args.source, args.simulate_failures)
    try:
        summary = run_ingest(source, raw_path, args.watermark, args.page_size,
                             args.max_retries, args.base_delay, args.lookback_hours)
    except TransientError:
        logger.error("ingest dihentikan; raw dan watermark TIDAK diubah")
        return 1
    print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
