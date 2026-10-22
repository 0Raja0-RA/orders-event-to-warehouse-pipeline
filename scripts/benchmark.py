"""Benchmark Pandas vs Polars (eager & lazy) pada dataset identik.

Peran di pipeline : bukti pemilihan Polars untuk tahap transformasi.
Output            : docs/evidence/benchmark_results.md (atau .csv)

Aturan agar klaim sah:
    - pekerjaan yang dibandingkan SAMA (baca -> hitung gross/net -> group_by kategori)
    - warm-up lalu ulangi >= 5 kali; laporkan median
    - catat ukuran data (baris, MB) dan spesifikasi mesin
    - dataset 36 baris terlalu kecil: replikasi sintetis (mis. 1e6 baris)
      dan SEBUTKAN itu sebagai keterbatasan

TODO:
    - [ ] generate dataset besar dari orders.csv (seed tetap)
    - [ ] ukur: pandas, polars eager (read_csv), polars lazy (scan_csv + collect)
    - [ ] tulis tabel hasil + keterbatasan benchmark
"""
