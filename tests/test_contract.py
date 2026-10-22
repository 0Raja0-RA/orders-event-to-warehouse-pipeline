"""Unit test data contract. Hapus skip setelah scripts/contract.py selesai.

Satu test per aturan, memakai baris cacat nyata dari data/source/orders.csv.
"""
import pytest

pytestmark = pytest.mark.skip(reason="contract.py belum diimplementasi")

GOOD = {
    "order_id": "O1", "customer_id": "C1", "order_ts": "2026-09-01T09:15:00Z",
    "product_category": "Books", "quantity": 1, "unit_price": 10.0,
    "discount_pct": 0.1, "payment_method": "card", "status": "paid",
}


def test_valid_record_passes():
    ...


@pytest.mark.parametrize("field,value", [
    ("customer_id", ""),                 # O1036: customer_id kosong
    ("quantity", 0),                     # O1031: quantity nol
    ("unit_price", -12.0),               # O1032: harga negatif
    ("discount_pct", 1.2),               # O1033: diskon > 1
    ("order_ts", "not-a-date"),          # O1034: timestamp tidak valid
    ("status", "pending"),               # O1035: status di luar daftar
])
def test_invalid_record_is_rejected_with_reason(field, value):
    ...
