"""Kafka producer: CSV -> topic `orders`.

Peran di pipeline : sumber event streaming (pesanan baru).
Input             : data/source/orders.csv
Output            : pesan JSON di topic `orders` (key = order_id)

TODO:
    - [ ] kirim minimal 20 event; key = order_id (event order yang sama
          selalu ke partition yang sama)
    - [ ] opsi --duplicate: kirim ulang order_id yang sama untuk demo
          duplikasi, dipakai untuk membuktikan sink idempoten
    - [ ] delivery callback: cetak partition & offset tiap pesan
    - [ ] flush() sebelum keluar
"""
