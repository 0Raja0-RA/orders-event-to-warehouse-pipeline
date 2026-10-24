"""Muat record valid hasil transformasi ke DuckDB (raw.orders), sumber untuk model dbt.

Input  : data/valid/orders_transformed.parquet, data/rejected/rejected_orders.csv, stream_rejected.jsonl
Output : warehouse/warehouse.duckdb -> tabel raw.orders (valid) dan raw.rejected_orders (audit penolakan)

Idempoten: CREATE OR REPLACE, jadi dijalankan berulang hasilnya sama.
order_ts disimpan sebagai TIMESTAMP UTC polos (bukan timestamptz): CAST(timestamptz AS DATE)
mengikuti zona waktu sesi (WIB), sehingga pesanan dekat tengah malam UTC bisa pindah hari.
"""
import argparse, json, sys
from pathlib import Path
import duckdb
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/valid/orders_transformed.parquet")
    ap.add_argument("--db", default="warehouse/warehouse.duckdb")
    ap.add_argument("--rejected", default="data/rejected/rejected_orders.csv")
    ap.add_argument("--rejected-stream", default="data/rejected/stream_rejected.jsonl")
    args = ap.parse_args()
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.orders AS
        SELECT * REPLACE (CAST(timezone('UTC', order_ts) AS TIMESTAMP) AS order_ts)
        FROM read_parquet(?)""", [args.input])

    expected = con.execute("SELECT count(*) FROM read_parquet(?)", [args.input]).fetchall()[0][0]
    total, unique_ids = con.execute("SELECT count(*), count(DISTINCT order_id) FROM raw.orders").fetchall()[0]
    rejected_ids = pd.read_csv(args.rejected, dtype=str, keep_default_na=False)["order_id"] if Path(args.rejected).exists() else pd.Series([], dtype=str)
    con.register("rejected_ids", pd.DataFrame({"order_id": rejected_ids}))
    leaked = con.execute("SELECT count(*) FROM raw.orders WHERE order_id IN (SELECT order_id FROM rejected_ids)").fetchall()[0][0]

    # raw.rejected_orders: audit penolakan (batch + stream), dipakai untuk pertanyaan analitik ke-5
    rejected = []
    if Path(args.rejected).exists():
        df = pd.read_csv(args.rejected, dtype=str, keep_default_na=False)[["order_id", "error_reason", "payload"]]
        rejected.append(df.assign(source="batch"))
    if Path(args.rejected_stream).exists():
        rows = [json.loads(l) for l in Path(args.rejected_stream).read_text(encoding="utf-8").splitlines() if l.strip()]
        rejected.append(pd.DataFrame([{"order_id": r.get("key") or "", "error_reason": r["error_reason"],
                                       "payload": r["payload"], "source": "stream"} for r in rows]))
    rejected_df = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame(
        columns=["order_id", "error_reason", "payload", "source"])
    con.register("rejected_df", rejected_df)
    con.execute("CREATE OR REPLACE TABLE raw.rejected_orders AS SELECT * FROM rejected_df")

    checks = {
        f"jumlah baris = jumlah valid ({expected})": total == expected,
        "order_id unik": total == unique_ids,
        f"tidak ada record rejected di warehouse ({len(rejected_ids)} ID dicek)": leaked == 0,
    }
    print(f"raw.orders dimuat: {total} baris, raw.rejected_orders: {len(rejected_df)} baris -> {args.db}")
    for name, ok in checks.items():
        print(f"  [{'OK' if ok else 'GAGAL'}] {name}")
    con.close()
    if not all(checks.values()):
        sys.exit(1)

if __name__ == "__main__":
    main()
