"""Batch ingestion: CSV (simulasi REST API berpaginasi) -> raw Parquet.

Berdasarkan kerangka dari paket pendukung. Tambahan dari kerangka:
  - read_csv dtype=str: nilai cacat (mis. "not-a-date") tetap utuh di raw agar
    validate.py bisa menyimpan payload asal pada rejected
  - --fail-first N: simulasi N kegagalan sementara pada halaman 1 (demo retry/backoff)
  - ringkasan jumlah record dibaca dan ditambahkan
Jumlah record ditolak dilaporkan oleh validate.py (raw tidak disaring di sini).
"""
import argparse, json, time
from pathlib import Path
import pandas as pd

def get_page(df, page, page_size, retries=3, fail_first=0):
    # Pada API sungguhan: requests.get(), timeout, cek status HTTP (429/5xx),
    # Retry-After, dan exponential backoff berada di fungsi ini.
    for attempt in range(retries):
        try:
            if page == 1 and attempt < fail_first:
                raise ConnectionError("503 Service Unavailable (simulasi)")
            start = page * page_size
            return df.iloc[start:start + page_size].copy()
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"page={page} percobaan {attempt + 1} gagal: {e}; coba lagi dalam {2 ** attempt}s")
            time.sleep(2 ** attempt)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/orders.csv")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=25)
    ap.add_argument("--checkpoint", default="data/state/watermark.json")
    ap.add_argument("--fail-first", type=int, default=0, help="simulasi N kegagalan sementara pada halaman 1")
    args = ap.parse_args()
    source = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    checkpoint = Path(args.checkpoint)
    last_ts = None
    if checkpoint.exists():
        last_ts = json.loads(checkpoint.read_text()).get("watermark")
    pages = []
    page = 0
    while True:
        chunk = get_page(source, page, args.page_size, fail_first=args.fail_first)
        if chunk.empty:
            break
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk)
        page += 1
    all_rows = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()
    records_read = len(all_rows)
    if last_ts:
        # order_ts yang tidak terbaca tetap lolos agar sampai ke validate.py;
        # dedup order_id di bawah mencegahnya tertulis dua kali
        ts = pd.to_datetime(all_rows["order_ts"], errors="coerce", utc=True)
        all_rows = all_rows[ts.isna() | (ts > pd.Timestamp(last_ts))]
    target = out / "orders.parquet"
    rows_before = 0
    if target.exists():
        old = pd.read_parquet(target)
        rows_before = len(old)
        all_rows = pd.concat([old, all_rows], ignore_index=True)
    all_rows = all_rows.drop_duplicates("order_id", keep="last")
    all_rows.to_parquet(target, index=False)
    # watermark disimpan SETELAH Parquet berhasil ditulis
    watermark = pd.to_datetime(all_rows["order_ts"], errors="coerce", utc=True).max()
    if pd.notna(watermark):
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps({"watermark": watermark.isoformat()}, indent=2))
    print(f"dibaca={records_read} ditambahkan={len(all_rows) - rows_before} total_unique_rows={len(all_rows)}")
    print(f"output={target} watermark={watermark.isoformat() if pd.notna(watermark) else None}")
    print("Note: adapter ini mem-paginasi CSV lokal; ganti get_page dengan adapter requests untuk REST API sungguhan.")

if __name__ == "__main__":
    main()
