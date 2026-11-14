import csv, os, time
from pyspark.sql import SparkSession, functions as F, Window

spark = (SparkSession.builder.appName("btc-aggregate").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t0 = time.time()

df = spark.read.parquet("datalake/btc_clean")
os.makedirs("outputs", exist_ok=True)


def save(sdf, name):
    rows = sdf.collect()
    path = f"outputs/{name}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(sdf.columns)
        for r in rows:
            w.writerow(["" if v is None else (round(v, 6) if isinstance(v, float) else v) for v in r])
    print(f"{name}: {len(rows):,} dong -> {path}")


# ---------- THEO NGAY ----------
daily = (df.groupBy("date").agg(
            F.min_by("open", "open_time_ms").alias("Open"),
            F.max("high").alias("High"),
            F.min("low").alias("Low"),
            F.max_by("close", "open_time_ms").alias("Close"),
            F.sum("volume").alias("Volume"),
            F.avg("close").alias("Avg_Close"),
            F.stddev("close").alias("Std_Close"),
            F.count("*").alias("Record_Count"))
         .withColumnRenamed("date", "Date"))
wd = Window.orderBy("Date")
w7 = Window.orderBy("Date").rowsBetween(-6, 0)
w30 = Window.orderBy("Date").rowsBetween(-29, 0)
daily = (daily
         .withColumn("Daily_Return_Pct", (F.col("Close") / F.lag("Close").over(wd) - 1) * 100)
         .withColumn("High_Low_Range", F.col("High") - F.col("Low"))
         .withColumn("Price_Up", F.when(F.col("Daily_Return_Pct") > 0, 1).otherwise(0))
         .withColumn("MA7", F.when(F.count("Close").over(w7) == 7, F.avg("Close").over(w7)))
         .withColumn("MA30", F.when(F.count("Close").over(w30) == 30, F.avg("Close").over(w30)))
         .withColumn("Weekday", F.date_format("Date", "EEEE"))
         .withColumn("Month", F.month("Date"))
         .withColumn("Year", F.year("Date"))
         .orderBy("Date"))
save(daily, "btc_daily")

# ---------- THEO THANG ----------
monthly = (df.groupBy("year_month").agg(
              F.min_by("open", "open_time_ms").alias("Open"),
              F.max("high").alias("High"),
              F.min("low").alias("Low"),
              F.max_by("close", "open_time_ms").alias("Close"),
              F.sum("volume").alias("Volume"),
              F.avg("close").alias("Avg_Close"),
              F.count("*").alias("Record_Count"))
           .withColumnRenamed("year_month", "YearMonth"))
wm = Window.orderBy("YearMonth")
monthly = (monthly
           .withColumn("Monthly_Return_Pct", (F.col("Close") / F.lag("Close").over(wm) - 1) * 100)
           .withColumn("Year", F.substring("YearMonth", 1, 4).cast("int"))
           .withColumn("Month", F.substring("YearMonth", 6, 2).cast("int"))
           .orderBy("YearMonth"))
save(monthly, "btc_monthly")

# ---------- THEO NAM ----------
yearly = (df.groupBy("year").agg(
             F.min_by("open", "open_time_ms").alias("Open"),
             F.max("high").alias("High"),
             F.min("low").alias("Low"),
             F.max_by("close", "open_time_ms").alias("Close"),
             F.sum("volume").alias("Volume"),
             F.avg("close").alias("Avg_Close"),
             F.count("*").alias("Record_Count"))
          .withColumnRenamed("year", "Year"))
wy = Window.orderBy("Year")
yearly = (yearly
          .withColumn("Yearly_Return_Pct", (F.col("Close") / F.lag("Close").over(wy) - 1) * 100)
          .orderBy("Year"))
save(yearly, "btc_yearly")

# ---------- THEO GIO TRONG NGAY ----------
hourly = (df.groupBy("hour").agg(
             F.avg("volume").alias("Avg_Volume_Per_Minute"),
             F.sum("volume").alias("Total_Volume"),
             F.avg(F.col("high") - F.col("low")).alias("Avg_High_Low_Range"),
             F.avg("close").alias("Avg_Close"),
             F.count("*").alias("Record_Count"))
          .withColumnRenamed("hour", "Hour")
          .orderBy("Hour"))
save(hourly, "btc_hourly")

print(f"XONG trong {time.time()-t0:.1f}s")
spark.stop()
