"""Benchmark Pandas vs Polars (eager & lazy) dan eksperimen data skew.

Peran di pipeline : bukti terukur pemilihan Polars untuk transformasi (langkah 12)
                    dan pemahaman efek skew pada group_by (langkah 13).

Aturan agar klaim sah:
    - pekerjaan yang dibandingkan SAMA (baca -> hitung gross/net -> group_by kategori)
    - warm-up lalu ulangi >= 5 kali; laporkan median
    - catat ukuran data (baris, MB) dan spesifikasi mesin
    - dataset 36 baris terlalu kecil: replikasi sintetis dan SEBUTKAN itu sebagai keterbatasan
    - jangan menyatakan OOM kecuali benar-benar terjadi dan tercatat

TODO:
    - [ ] replikasi orders.csv menjadi dataset besar (seed tetap)
    - [ ] ukur: pandas, polars eager (read_csv), polars lazy (scan_csv + collect)
    - [ ] skew: ganti sebagian besar baris ke satu customer_id dominan, hitung distribusi
          per customer_id, bandingkan waktu group_by normal vs skewed
    - [ ] cetak tabel hasil dan keterbatasan; mitigasi skew: agregasi parsial, salting,
          pemrosesan bertahap, proyeksi kolom, hindari collect berlebih
"""
