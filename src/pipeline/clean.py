import json, os, time
from pyspark.sql import SparkSession, functions as F, Window

spark = (SparkSession.builder.appName("btc-clean").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t0 = time.time()

df = spark.read.parquet("datalake/btc_raw").cache()
n0 = df.count()
report = {"rows_raw": n0}
cols = ["open", "high", "low", "close", "volume"]

# 1. gia tri thieu
nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()
report["null_by_column"] = {k: int(v or 0) for k, v in nulls.items()}

# 2. timestamp trung
report["duplicate_timestamps"] = n0 - df.select("open_time_ms").distinct().count()

# 3. OHLC khong hop le
bad_ohlc = ((F.col("high") < F.col("low"))
            | (F.col("high") < F.greatest("open", "close"))
            | (F.col("low") > F.least("open", "close"))
            | (F.col("open") <= 0) | (F.col("high") <= 0)
            | (F.col("low") <= 0) | (F.col("close") <= 0))
report["invalid_ohlc"] = df.filter(bad_ohlc).count()

# 4. khoi luong am
report["negative_volume"] = df.filter(F.col("volume") < 0).count()

# 5. khoang trong thoi gian (khac 60 giay)
w = Window.orderBy("open_time_ms")
gaps = (df.select("open_time_ms")
          .withColumn("diff", F.col("open_time_ms") - F.lag("open_time_ms").over(w))
          .filter(F.col("diff") > 60000))
report["time_gaps"] = gaps.count()
report["missing_minutes_in_gaps"] = int(gaps.agg(F.sum(F.col("diff") / 60000 - 1)).first()[0] or 0)

# 6. ngoai lai khoi luong theo IQR x 3 (chi dem) va phut khong giao dich
q1, q3 = df.approxQuantile("volume", [0.25, 0.75], 0.001)
upper = q3 + 3 * (q3 - q1)
report["volume_q1"], report["volume_q3"], report["volume_iqr_upper"] = q1, q3, upper
report["volume_outliers"] = df.filter(F.col("volume") > upper).count()
report["zero_volume_minutes"] = df.filter(F.col("volume") == 0).count()

# lam sach: chi loai dong chac chan sai
clean = (df.filter(~bad_ohlc & (F.col("volume") >= 0))
           .dropna(subset=cols)
           .dropDuplicates(["open_time_ms"])
           .withColumn("date", F.to_date("datetime"))
           .withColumn("hour", F.hour("datetime"))
           .withColumn("month", F.month("datetime"))
           .withColumn("weekday", F.date_format("datetime", "E"))
           .withColumn("year_month", F.date_format("datetime", "yyyy-MM"))
           .select("open_time_ms", "datetime", "date", "year", "month", "hour",
                   "weekday", "year_month", "open", "high", "low", "close", "volume", "source"))
clean.write.mode("overwrite").partitionBy("year").parquet("datalake/btc_clean")
report["rows_clean"] = spark.read.parquet("datalake/btc_clean").count()
report["rows_removed"] = n0 - report["rows_clean"]

os.makedirs("reports", exist_ok=True)
with open("reports/quality_report.json", "w") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))
print(f"XONG trong {time.time()-t0:.1f}s")
spark.stop()
