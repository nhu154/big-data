import json, time
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from confluent_kafka import Consumer

OUT = "datalake/btc_raw"
c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "to-parquet-1",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["btc_klines_1m"])
batch_no = total = idle = 0
t0 = time.time()
while idle < 5:
    msgs = [m for m in c.consume(num_messages=100000, timeout=2.0) if m.error() is None]
    if not msgs:
        idle += 1
        continue
    idle = 0
    df = pd.DataFrame([json.loads(m.value()) for m in msgs])
    df["datetime"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df["year"] = df["datetime"].dt.year
    pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False), OUT,
                        partition_cols=["year"],
                        basename_template=f"part-{batch_no}-{{i}}.parquet",
                        existing_data_behavior="overwrite_or_ignore")
    c.commit()
    total += len(df)
    batch_no += 1
    print(f"batch {batch_no}: ghi {len(df):,} dòng, tổng {total:,}")
c.close()
print(f"XONG: {total:,} dòng trong {time.time()-t0:.1f}s")
