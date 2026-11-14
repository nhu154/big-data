import contextlib, io, os
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("btc-catalyst").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("datalake/btc_clean")
q = (df.filter(F.col("year") == 2025)
       .filter(F.col("volume") > 0)
       .groupBy("date")
       .agg(F.max("close").alias("Max_Close"), F.sum("volume").alias("Volume"))
       .orderBy("date"))

buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    print("=" * 20, "EXPLAIN EXTENDED", "=" * 20)
    q.explain(True)
    print("=" * 20, "EXPLAIN FORMATTED", "=" * 20)
    q.explain(mode="formatted")
text = buf.getvalue()
print(text)
os.makedirs("reports", exist_ok=True)
open("reports/catalyst_plan.txt", "w", encoding="utf-8").write(text)
print("So ngay ket qua:", q.count(), "-> da luu reports/catalyst_plan.txt")
spark.stop()
