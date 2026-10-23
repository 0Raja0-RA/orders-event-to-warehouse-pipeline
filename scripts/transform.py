"""Transformasi record valid dengan Polars (lazy): gabung batch + stream, hitung metrik, simpan Parquet.

Input  : data/valid/valid_orders.parquet   (batch, hasil validate.py)
         data/valid/stream_valid.jsonl     (stream, hasil consumer.py; opsional)
Output : data/valid/orders_transformed.parquet  (raw layer tidak diubah)

Record dari dua jalur digabung lalu dideduplikasi berdasarkan order_id (versi terakhir menang).

    gross_amount = quantity * unit_price
    net_amount   = gross_amount * (1 - discount_pct)   # 0.10 = diskon 10%, bukan 0,10 persen
"""
import argparse
from pathlib import Path
import polars as pl

def load_valid(batch_path, stream_path):
    lf = pl.scan_parquet(batch_path)
    if Path(stream_path).exists():
        stream = pl.scan_ndjson(stream_path).with_columns(
            pl.col("order_ts").str.to_datetime(time_unit="ns", time_zone="UTC"))
        lf = pl.concat([lf, stream.select(lf.collect_schema().names())], how="vertical_relaxed")
    return lf.unique(subset="order_id", keep="last", maintain_order=True)

def transform(lf):
    return (
        lf.with_columns(
            pl.col("order_ts").dt.cast_time_unit("us").dt.convert_time_zone("UTC"),
            pl.col("quantity").cast(pl.Int64),
            pl.col("unit_price").cast(pl.Float64),
            pl.col("discount_pct").cast(pl.Float64),
        )
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"))
        .with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"))
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/valid/valid_orders.parquet")
    ap.add_argument("--stream", default="data/valid/stream_valid.jsonl")
    ap.add_argument("--out", default="data/valid/orders_transformed.parquet")
    args = ap.parse_args()
    pl.Config.set_tbl_formatting("ASCII_MARKDOWN")  # tabel ASCII agar aman di terminal ber-encoding cp1252
    df = transform(load_valid(args.input, args.stream)).collect()  # lazy: baru dieksekusi saat collect()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.out)
    n_batch = pl.scan_parquet(args.input).select(pl.len()).collect().item()
    n_stream = len(Path(args.stream).read_text(encoding="utf-8").splitlines()) if Path(args.stream).exists() else 0
    print(f"sumber: batch={n_batch} stream={n_stream} -> unik ditulis={len(df)} -> {args.out}")
    print(df.select("order_id", "quantity", "unit_price", "discount_pct", "gross_amount", "net_amount").head(5))
    print(df.schema)

if __name__ == "__main__":
    main()
