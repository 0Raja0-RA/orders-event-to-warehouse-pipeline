"""Data contract & fail-fast: raw Parquet -> valid + rejected.

Model Order mengikuti contoh dari panduan. validate_record() juga dipakai consumer.py,
sehingga jalur batch dan streaming menolak data dengan aturan yang sama.

Mode:
  default      quarantine per-record: record cacat dipisah beserta alasannya, sisanya lanjut
  --fail-fast  berhenti pada pelanggaran pertama, tidak ada file valid/rejected yang ditulis
"""
import argparse, json, sys
from datetime import datetime
from pathlib import Path
from typing import Literal
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError

class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

def validate_record(record):
    """Kembalikan (order, None) bila lolos, atau (None, alasan) bila ditolak."""
    try:
        return Order(**record), None
    except ValidationError as e:
        reason = "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors())
        return None, reason

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw/orders.parquet")
    ap.add_argument("--valid-out", default="data/valid")
    ap.add_argument("--rejected-out", default="data/rejected")
    ap.add_argument("--fail-fast", action="store_true", help="berhenti pada pelanggaran pertama")
    args = ap.parse_args()
    raw = pd.read_parquet(args.raw)
    valid, rejected = [], []
    for record in raw.to_dict("records"):
        order, reason = validate_record(record)
        if order:
            valid.append(order.model_dump())
            continue
        if args.fail_fast:
            print(f"FAIL-FAST: order_id={record.get('order_id') or '(kosong)'} melanggar kontrak -> {reason}")
            print("Batch dihentikan; tidak ada data yang ditulis.")
            sys.exit(1)
        rejected.append({
            "order_id": record.get("order_id", ""),
            "error_reason": reason,
            "payload": json.dumps(record, ensure_ascii=False),
        })
    valid_dir, rejected_dir = Path(args.valid_out), Path(args.rejected_out)
    valid_dir.mkdir(parents=True, exist_ok=True); rejected_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid).to_parquet(valid_dir / "valid_orders.parquet", index=False)
    pd.DataFrame(rejected, columns=["order_id", "error_reason", "payload"]).to_csv(
        rejected_dir / "rejected_orders.csv", index=False)
    print(f"dibaca={len(raw)} valid={len(valid)} ditolak={len(rejected)}")
    for r in rejected:
        print(f"  ditolak {r['order_id'] or '(kosong)'}: {r['error_reason']}")
    print(f"valid   -> {valid_dir / 'valid_orders.parquet'}")
    print(f"rejected-> {rejected_dir / 'rejected_orders.csv'}")

if __name__ == "__main__":
    main()
