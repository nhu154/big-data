import sys, json, time
import pandas as pd
from confluent_kafka import Producer

CSV = sys.argv[1]
START = pd.Timestamp(sys.argv[2] if len(sys.argv) > 2 else "2026-08-01", tz="UTC")
START_TS = int(START.timestamp())
TOPIC = "btc_klines_1m"
CHUNK = 500_000

p = Producer({"bootstrap.servers": "localhost:9092", "linger.ms": 50, "compression.type": "lz4"})
sent = total = 0
t0 = time.time()

for i, chunk in enumerate(pd.read_csv(CSV, chunksize=CHUNK)):
    total += len(chunk)
    chunk = chunk[chunk["Timestamp"] >= START_TS]
    for row in chunk.itertuples(index=False):
        ts_ms = int(row.Timestamp) * 1000
        msg = {"source": "kaggle_csv", "symbol": "BTCUSD", "open_time_ms": ts_ms,
               "open": float(row.Open), "high": float(row.High), "low": float(row.Low),
               "close": float(row.Close), "volume": float(row.Volume),
               "ingested_at": int(time.time() * 1000)}
        while True:
            try:
                p.produce(TOPIC, key=str(ts_ms), value=json.dumps(msg))
                break
            except BufferError:
                p.poll(0.5)
        sent += 1
        if sent % 10000 == 0:
            p.poll(0)
    p.flush()
    print(f"chunk {i}: đã đọc {total:,} dòng, đã gửi {sent:,} dòng vào Kafka")

print(f"XONG: đọc {total:,} dòng, gửi {sent:,} dòng trong {time.time()-t0:.1f}s")
