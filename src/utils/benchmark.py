"""So sanh pandas va Spark tren cung mot phep tong hop: nen phut -> nen ngay (OHLCV).

Chay tu thu muc goc cua repo:  python src/utils/benchmark.py
Ket qua in ra man hinh va luu vao reports/benchmark.csv (mat khoang 5-15 phut).

Thi nghiem:
  1. pandas va Spark (local[1], local[2], local[4], local[*]) tren du lieu goc (1x = 7,8 trieu dong).
  2. Nhan ban du lieu 2x, 4x, 8x de mo phong du lieu lon dan: do thoi gian va bo nho dinh cua pandas,
     thoi gian cua Spark local[*].
Moi phep do pandas chay trong mot tien trinh rieng de do dung bo nho dinh (peak RAM) cua rieng no.
"""
import json, os, resource, subprocess, sys, time

SRC = "datalake/btc_clean"
COLS = ["open_time_ms", "date", "open", "high", "low", "close", "volume"]
SCALES = [1, 2, 4, 8]
BASE_ROWS = [0]


def peak_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e6 if sys.platform == "darwin" else r / 1e3     # macOS: byte, Linux: KB


def run_pandas(k):
    import pandas as pd
    t0 = time.time()
    df = pd.read_parquet(SRC, columns=COLS)
    if k > 1:
        df = pd.concat([df] * k, ignore_index=True)
    df = df.sort_values("open_time_ms")
    g = df.groupby("date").agg(Open=("open", "first"), High=("high", "max"), Low=("low", "min"),
                               Close=("close", "last"), Volume=("volume", "sum"),
                               Record_Count=("open_time_ms", "count"))
    return {"rows": len(df), "days": len(g), "seconds": round(time.time() - t0, 2), "peak_ram_mb": round(peak_mb())}


def spark_session(master):
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("btc-bench").master(master)
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def run_spark(spark, k):
    from functools import reduce
    from pyspark.sql import functions as F
    t0 = time.time()
    df = spark.read.parquet(SRC).select(*COLS)
    if k > 1:
        df = reduce(lambda a, b: a.unionAll(b), [df] * k)
    g = df.groupBy("date").agg(F.min_by("open", "open_time_ms").alias("Open"), F.max("high").alias("High"),
                                F.min("low").alias("Low"), F.max_by("close", "open_time_ms").alias("Close"),
                                F.sum("volume").alias("Volume"), F.count("*").alias("Record_Count"))
    days = g.count()                                   # buoc nay moi thuc su chay tinh toan (lazy evaluation)
    return {"rows": BASE_ROWS[0] * k, "days": days, "seconds": round(time.time() - t0, 2)}


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "pandas":          # tien trinh con do pandas
        try:
            print("RESULT", json.dumps(run_pandas(int(sys.argv[2]))))
        except MemoryError:
            print("RESULT", json.dumps({"error": "MemoryError"}))
        return

    if not os.path.isdir(SRC):
        raise SystemExit(f"Khong thay {SRC}. Hay chay tu thu muc goc cua repo.")
    results = []

    def pandas_child(k):
        p = subprocess.run([sys.executable, __file__, "pandas", str(k)], capture_output=True, text=True)
        line = [l for l in p.stdout.splitlines() if l.startswith("RESULT")]
        if not line:                                            # bi he dieu hanh dung vi het RAM
            return {"error": (p.stderr.strip().splitlines() or ["het bo nho / bi dung"])[-1][:80]}
        return json.loads(line[0][7:])

    # Thi nghiem 1: so nhan CPU
    print("== Thi nghiem 1: du lieu goc (1x), so sanh pandas va Spark voi so nhan khac nhau ==")
    r = pandas_child(1); r.update(tool="pandas", cores="1", scale=1); results.append(r); print(r)
    ncpu = os.cpu_count()
    for n in [1, 2, 4, "*"]:
        if n != "*" and n > ncpu:
            continue
        spark = spark_session(f"local[{n}]")
        BASE_ROWS[0] = spark.read.parquet(SRC).count()
        run_spark(spark, 1)                                     # chay nong (warm-up), khong tinh
        r = run_spark(spark, 1); r.update(tool="Spark", cores=str(n if n != "*" else f"*={ncpu}"), scale=1)
        results.append(r); print(r)
        spark.stop()

    # Thi nghiem 2: du lieu lon dan
    print("== Thi nghiem 2: nhan ban du lieu 2x, 4x, 8x ==")
    spark = spark_session("local[*]")
    run_spark(spark, 1)
    for k in SCALES[1:]:
        r = pandas_child(k); r.update(tool="pandas", cores="1", scale=k); results.append(r); print(r)
        r = run_spark(spark, k); r.update(tool="Spark", cores=f"*={ncpu}", scale=k); results.append(r); print(r)
    spark.stop()

    os.makedirs("reports", exist_ok=True)
    keys = ["tool", "cores", "scale", "rows", "days", "seconds", "peak_ram_mb", "error"]
    with open("reports/benchmark.csv", "w") as f:
        f.write(",".join(keys) + "\n")
        for r in results:
            f.write(",".join(str(r.get(c, "")) for c in keys) + "\n")
    print("\nCong cu  So nhan  Quy mo  So dong        Thoi gian (s)  RAM dinh pandas (MB)")
    for r in results:
        print(f"{r['tool']:<8} {r['cores']:<8} {r['scale']}x     {str(r.get('rows','')):<14} "
              f"{str(r.get('seconds', r.get('error',''))):<14} {r.get('peak_ram_mb','')}")
    print("\nDa luu reports/benchmark.csv")


if __name__ == "__main__":
    main()
