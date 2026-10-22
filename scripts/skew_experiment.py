"""Eksperimen data skew pada kolom customer_id.

Peran di pipeline : menunjukkan efek distribusi tidak merata pada group_by
                    dan menjelaskan strategi mitigasinya.
Output            : docs/evidence/skew_results.md

Catatan: jangan menyatakan OOM kecuali benar-benar terjadi dan tercatat.

TODO:
    - [ ] buat dataset turunan: sebagian besar baris diganti ke satu customer_id dominan
    - [ ] hitung distribusi baris per customer_id (top-N + persentase)
    - [ ] bandingkan group_by normal vs skewed: ukuran data, waktu, (memori bila ada)
    - [ ] tulis mitigasi: agregasi parsial, salting key, proyeksi kolom,
          pemrosesan bertahap, hindari collect/materialisasi berlebih
"""
