"""Transformasi record valid dengan Polars (lazy): hitung metrik, standarkan tipe, simpan Parquet.

Input  : data/valid/valid_orders.parquet   (hasil validate.py)
Output : data/valid/orders_transformed.parquet  (raw layer tidak diubah)

    gross_amount = quantity * unit_price
    net_amount   = gross_amount * (1 - discount_pct)   # 0.10 = diskon 10%, bukan 0,10 persen
"""
import argparse
from pathlib import Path
import polars as pl

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
    ap.add_argument("--out", default="data/valid/orders_transformed.parquet")
    args = ap.parse_args()
    pl.Config.set_tbl_formatting("ASCII_MARKDOWN")  # tabel ASCII agar aman di terminal ber-encoding cp1252
    df = transform(pl.scan_parquet(args.input)).collect()  # lazy: baru dieksekusi saat collect()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.out)
    print(f"dibaca={pl.scan_parquet(args.input).select(pl.len()).collect().item()} ditulis={len(df)} -> {args.out}")
    print(df.select("order_id", "quantity", "unit_price", "discount_pct", "gross_amount", "net_amount").head(5))
    print(df.schema)

if __name__ == "__main__":
    main()
