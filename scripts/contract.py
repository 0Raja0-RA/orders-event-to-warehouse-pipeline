"""Data contract untuk event/record pesanan (Pydantic).

Peran di pipeline : gerbang kualitas. Dipakai bersama oleh jalur batch
                    (validate.py) dan jalur streaming (consumer.py), sehingga
                    kedua jalur menolak data dengan aturan yang SAMA.
Aturan wajib      :
    order_id        string tidak kosong
    customer_id     string tidak kosong
    order_ts        dapat diparse menjadi timestamp
    product_category, payment_method  string tidak kosong
    quantity        integer > 0
    unit_price      angka >= 0
    discount_pct    angka 0..1  (0.10 = diskon 10%)
    status          salah satu: paid | shipped | cancelled | refunded
    extra field     ditolak (extra="forbid")

TODO:
    - [ ] class Order(BaseModel) dengan aturan di atas
    - [ ] fungsi validate_record(raw: dict) -> (Order | None, error: str | None)
          yang TIDAK melempar exception ke pemanggil; error berisi nama field
          + pesan agar bisa diagnosis dan diaudit
"""
