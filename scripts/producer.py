"""Kafka producer: orders.csv -> topic `orders` (key = order_id).

Key = order_id membuat event untuk order yang sama selalu masuk ke partition yang sama.
Semua baris dikirim apa adanya (termasuk 6 baris cacat) agar consumer bisa menunjukkan validasi.

  --limit N      kirim N baris pertama (default: semua baris, minimal 20 sesuai brief)
  --duplicate N  kirim ulang N baris pertama (order_id sama) untuk demo duplikasi
  --bad-json     kirim satu pesan bukan-JSON untuk menguji ketahanan consumer
"""
import argparse, csv, json
from confluent_kafka import Producer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/orders.csv")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--duplicate", type=int, default=0)
    ap.add_argument("--bad-json", action="store_true")
    args = ap.parse_args()
    with open(args.source, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    rows = rows[:args.limit] if args.limit else rows
    to_send = rows + rows[:args.duplicate]

    producer = Producer({"bootstrap.servers": args.bootstrap})
    sent = []

    def on_delivery(err, msg):
        if err:
            print(f"GAGAL key={msg.key()}: {err}")
        else:
            sent.append(msg)
            print(f"terkirim key={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")

    for row in to_send:
        producer.produce(args.topic, key=row["order_id"], value=json.dumps(row), callback=on_delivery)
        producer.poll(0)
    if args.bad_json:
        producer.produce(args.topic, key="BAD", value=b"{bukan json", callback=on_delivery)
    producer.flush()
    per_partition = {}
    for m in sent:
        per_partition[m.partition()] = per_partition.get(m.partition(), 0) + 1
    print(f"\ntotal terkirim={len(sent)} (unik={len(rows)}, duplikat={args.duplicate}, bad-json={int(args.bad_json)}) topic={args.topic}")
    print(f"per partition: {dict(sorted(per_partition.items()))}")

if __name__ == "__main__":
    main()
