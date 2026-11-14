import os, time
import pyarrow.dataset as ds

SRC = "datalake/btc_raw"
OUT = "data/btcusd_1-min_data.csv"
t0 = time.time()
if not os.path.isdir(SRC):
    raise SystemExit(f"Khong thay {SRC}")

df = (ds.dataset(SRC, format="parquet", partitioning="hive")
        .to_table(columns=["open_time_ms", "open", "high", "low", "close", "volume"])
        .to_pandas())
print(f"Doc {len(df):,} dong tu {SRC}")
df = df.drop_duplicates("open_time_ms").sort_values("open_time_ms")
df["Timestamp"] = df["open_time_ms"] // 1000
df = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
df = df[["Timestamp", "Open", "High", "Low", "Close", "Volume"]]
os.makedirs("data", exist_ok=True)
df.to_csv(OUT, index=False)
print(f"Da ghi {len(df):,} dong -> {OUT} ({os.path.getsize(OUT)/1e6:.0f} MB) trong {time.time()-t0:.1f}s")
print("Dong dau:", df.iloc[0].to_dict())
print("Dong cuoi:", df.iloc[-1].to_dict())
