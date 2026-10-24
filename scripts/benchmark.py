"""Benchmark Pandas vs Polars (eager & lazy) dan eksperimen data skew.

Langkah 12: pekerjaan SAMA (baca CSV -> gross/net -> group_by kategori) di tiga mode, warm-up 1x,
            lalu --repeats kali; yang dilaporkan median. Hasil tiap engine dicek sama.
Langkah 13: dataset turunan dengan satu customer_id dominan; bandingkan distribusi grup dan
            waktu group_by (data sudah di memori, jadi yang diukur hanya group_by).

Dataset asli hanya 30 record valid, jadi data direplikasi sintetis (seed tetap) ke --rows baris.
Itu keterbatasan: hasil bergantung mesin, dan memori tidak diukur (OOM tidak diklaim).
"""
import argparse, os, platform, statistics, tempfile, time
from pathlib import Path
import numpy as np
import pandas as pd
import polars as pl

def make_dataset(base, n, seed, dominant_share=0.0, customers=1000):
    rng = np.random.default_rng(seed)
    df = base.iloc[rng.integers(0, len(base), n)].reset_index(drop=True)
    df["order_id"] = [f"B{i:07d}" for i in range(n)]
    df["customer_id"] = [f"C{c:04d}" for c in rng.integers(1, customers + 1, n)]
    if dominant_share:
        df.loc[rng.random(n) < dominant_share, "customer_id"] = "C0001"
    return df

def run_pandas(path):
    df = pd.read_csv(path)
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    return df.groupby("product_category")["net_amount"].sum().to_dict()

def polars_expr():
    return [
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount_pct"))).alias("net_amount"),
    ]

def run_polars_eager(path):
    df = pl.read_csv(path).with_columns(*polars_expr())
    out = df.group_by("product_category").agg(pl.col("net_amount").sum())
    return dict(zip(out["product_category"], out["net_amount"]))

def run_polars_lazy(path):
    out = (pl.scan_csv(path).with_columns(*polars_expr())
           .group_by("product_category").agg(pl.col("net_amount").sum()).collect())
    return dict(zip(out["product_category"], out["net_amount"]))

def timeit(fn, repeats):
    fn()  # warm-up
    times, result = [], None
    for _ in range(repeats):
        t = time.perf_counter(); result = fn(); times.append(time.perf_counter() - t)
    return statistics.median(times), min(times), max(times), result

def table(headers, rows):
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join("---" for _ in headers) + "|")
    for r in rows:
        print("| " + " | ".join(str(c) for c in r) + " |")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--valid", default="data/valid/valid_orders.parquet")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--skew-share", type=float, default=0.8, help="porsi baris milik customer dominan C0001")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    base = pd.read_parquet(args.valid)
    base["order_ts"] = base["order_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    with tempfile.TemporaryDirectory() as tmp:
        normal_csv, skew_csv = Path(tmp) / "normal.csv", Path(tmp) / "skewed.csv"
        normal = make_dataset(base, args.rows, args.seed)
        skewed = make_dataset(base, args.rows, args.seed, dominant_share=args.skew_share)
        normal.to_csv(normal_csv, index=False); skewed.to_csv(skew_csv, index=False)
        size_mb = normal_csv.stat().st_size / 1e6

        print("== Spesifikasi ==")
        print(f"mesin: {platform.platform()} | CPU: {platform.processor()} | core logis: {os.cpu_count()}")
        print(f"python {platform.python_version()} | pandas {pd.__version__} | polars {pl.__version__}")
        print(f"dataset: {args.rows:,} baris, {size_mb:.0f} MB CSV (replikasi sintetis dari {len(base)} record valid, seed={args.seed})")

        print(f"\n== Langkah 12: baca CSV -> gross/net -> group_by kategori (median dari {args.repeats} run, setelah 1x warm-up) ==")
        engines = [("Pandas", run_pandas), ("Polars eager (read_csv)", run_polars_eager), ("Polars lazy (scan_csv + collect)", run_polars_lazy)]
        results, rows = {}, []
        for name, fn in engines:
            median, lo, hi, res = timeit(lambda f=fn: f(normal_csv), args.repeats)
            results[name] = (median, res); rows.append([name, f"{median:.3f}", f"{lo:.3f}", f"{hi:.3f}"])
        base_median = results["Pandas"][0]
        for r, (name, _) in zip(rows, engines):
            r.append(f"{base_median / results[name][0]:.2f}x")
        table(["Engine", "Median (s)", "Min (s)", "Max (s)", "Kecepatan vs Pandas"], rows)
        ref = results["Pandas"][1]
        same = all(all(abs(res[k] - ref[k]) <= 1e-6 * abs(ref[k]) for k in ref) for _, res in results.values())
        print(f"hasil identik antar engine: {same}  (net per kategori: { {k: round(v, 2) for k, v in ref.items()} })")

        print(f"\n== Langkah 13: data skew ({args.skew_share:.0%} baris milik C0001), group_by customer_id ==")
        def prep(df):
            df = df.copy(); df["net_amount"] = df["quantity"] * df["unit_price"] * (1 - df["discount_pct"]); return df
        stats, rows = {}, []
        for label, df in (("normal (merata)", prep(normal)), ("skewed", prep(skewed))):
            pdf, pldf = df, pl.from_pandas(df)
            counts = pdf["customer_id"].value_counts()
            m_pd, *_ = timeit(lambda: pdf.groupby("customer_id")["net_amount"].agg(["sum", "count"]), args.repeats)
            m_pl, *_ = timeit(lambda: pldf.group_by("customer_id").agg(pl.col("net_amount").sum(), pl.len()), args.repeats)
            rows.append([label, f"{len(counts):,}", f"{counts.iloc[0]:,} ({counts.iloc[0] / len(pdf):.1%})",
                         f"{int(counts.median()):,}", f"{counts.iloc[0] / counts.median():.0f}x", f"{m_pd:.4f}", f"{m_pl:.4f}"])
        table(["Dataset", "Jumlah grup", "Grup terbesar (baris)", "Median grup", "Terbesar/median", "Pandas group_by (s)", "Polars group_by (s)"], rows)
        top = skewed["customer_id"].value_counts().head(3)
        print("3 customer teratas (skewed):", {k: int(v) for k, v in top.items()})
        print("Catatan: pada satu mesin, skew belum tentu memperlambat group_by secara nyata; pada sistem terdistribusi "
              "grup terbesar menjadi beban satu worker. Memori tidak diukur, jadi tidak ada klaim OOM.")
        print("Mitigasi: agregasi parsial (dua tahap), salting key, pemrosesan bertahap/per partisi, proyeksi kolom, "
              "hindari collect/materialisasi berlebih.")

if __name__ == "__main__":
    main()
