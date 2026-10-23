"""Kafka consumer: topic `orders` -> valid/rejected (sink idempoten).

Alur per pesan: decode JSON -> validate_record (contract yang sama dengan jalur batch)
-> tulis ke sink -> BARU commit offset. Berhenti sendiri setelah --idle-timeout detik tanpa pesan.

Jaminan: at-least-once (commit sesudah sink) + sink idempoten berdasarkan order_id.
Jika proses mati di antara sink dan commit, pesan dibaca ulang tetapi upsert tidak menambah baris.
Ini BUKAN exactly-once end-to-end: sink-nya file, bukan transaksi Kafka.

Output:
  data/valid/stream_valid.jsonl          upsert per order_id
  data/rejected/stream_rejected.jsonl    upsert per (topic, partition, offset) + alasan
"""
import argparse, json, time
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
from validate import validate_record

def load_jsonl(path, key):
    path = Path(path)
    if not path.exists():
        return {}
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {key(r): r for r in rows}

def write_jsonl(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--group", default="orders-consumer")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--valid-out", default="data/valid/stream_valid.jsonl")
    ap.add_argument("--rejected-out", default="data/rejected/stream_rejected.jsonl")
    ap.add_argument("--idle-timeout", type=float, default=10, help="berhenti setelah N detik tanpa pesan")
    args = ap.parse_args()

    valid = load_jsonl(args.valid_out, lambda r: r["order_id"])
    rejected = load_jsonl(args.rejected_out, lambda r: (r["topic"], r["partition"], r["offset"]))
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit manual, sesudah sink berhasil
    })
    consumer.subscribe([args.topic])
    processed = duplicates = 0
    last_message = time.time()
    try:
        while time.time() - last_message < args.idle_timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"error kafka: {msg.error()}")
                continue
            last_message = time.time()
            where = f"partition={msg.partition()} offset={msg.offset()}"
            raw = msg.value().decode("utf-8", errors="replace")
            try:
                record = json.loads(raw)
                order, reason = validate_record(record) if isinstance(record, dict) else (None, "payload bukan objek JSON")
            except json.JSONDecodeError as e:
                order, reason = None, f"JSON tidak valid: {e}"
            if order:
                if order.order_id in valid:
                    duplicates += 1
                    print(f"{where} key={order.order_id} -> DUPLIKAT (upsert, tidak menambah baris)")
                else:
                    print(f"{where} key={order.order_id} -> VALID")
                valid[order.order_id] = order.model_dump(mode="json")
            else:
                print(f"{where} key={msg.key().decode() if msg.key() else '-'} -> DITOLAK: {reason}")
                rejected[(msg.topic(), msg.partition(), msg.offset())] = {
                    "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                    "key": msg.key().decode() if msg.key() else None, "error_reason": reason, "payload": raw}
            write_jsonl(args.valid_out, valid.values())
            write_jsonl(args.rejected_out, rejected.values())
            consumer.commit(message=msg, asynchronous=False)  # commit SETELAH sink
            processed += 1
    finally:
        consumer.close()
    print(f"\npesan diproses={processed} valid_unik={len(valid)} ditolak={len(rejected)} duplikat_dalam_run={duplicates}")
    print(f"valid -> {args.valid_out} | rejected -> {args.rejected_out}")

if __name__ == "__main__":
    main()
