import csv, os, time
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("btc-analytics-sql").master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t0 = time.time()
os.makedirs("outputs", exist_ok=True)

spark.read.csv("outputs/btc_daily.csv", header=True, inferSchema=True).createOrReplaceTempView("daily_raw")
# chi dung nhung ngay du 1.440 phut de so sanh cong bang
spark.sql("CREATE OR REPLACE TEMP VIEW daily AS SELECT * FROM daily_raw WHERE Record_Count = 1440")
print("So ngay du du lieu:", spark.sql("SELECT COUNT(*) FROM daily").first()[0])


def run(name, query, show=20):
    sdf = spark.sql(query)
    rows = sdf.collect()
    path = f"outputs/{name}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(sdf.columns)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
    print(f"\n=== {name} ({len(rows)} dong) -> {path}")
    sdf.show(show, truncate=False)


run("analytics_yearly", """
SELECT Year,
       COUNT(*)                              AS So_ngay,
       ROUND(AVG(Daily_Return_Pct), 3)       AS Loi_suat_TB_ngay_pct,
       ROUND(STDDEV(Daily_Return_Pct), 3)    AS Bien_dong_pct,
       ROUND(100 * AVG(Price_Up), 2)         AS Ty_le_ngay_tang_pct,
       ROUND(MIN(Close), 2)                  AS Gia_thap_nhat,
       ROUND(MAX(Close), 2)                  AS Gia_cao_nhat,
       ROUND(SUM(Volume), 0)                 AS Tong_khoi_luong
FROM daily GROUP BY Year ORDER BY Year""", 20)

run("analytics_month_seasonality", """
SELECT Month,
       ROUND(AVG(Daily_Return_Pct), 3)       AS Loi_suat_TB_ngay_pct,
       ROUND(100 * AVG(Price_Up), 2)         AS Ty_le_ngay_tang_pct,
       ROUND(AVG(Volume), 1)                 AS Khoi_luong_TB_ngay
FROM daily GROUP BY Month ORDER BY Month""", 12)

run("analytics_weekday", """
SELECT Weekday,
       ROUND(AVG(Daily_Return_Pct), 3)       AS Loi_suat_TB_ngay_pct,
       ROUND(100 * AVG(Price_Up), 2)         AS Ty_le_ngay_tang_pct,
       ROUND(AVG(Volume), 1)                 AS Khoi_luong_TB_ngay
FROM daily GROUP BY Weekday
ORDER BY CASE Weekday WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
                      WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 ELSE 7 END""", 7)

run("analytics_top_moves", """
SELECT * FROM (
  (SELECT 'Tang manh nhat' AS Nhom, Date, ROUND(Close, 2) AS Close, ROUND(Daily_Return_Pct, 2) AS Daily_Return_Pct
   FROM daily ORDER BY Daily_Return_Pct DESC LIMIT 10)
  UNION ALL
  (SELECT 'Giam manh nhat' AS Nhom, Date, ROUND(Close, 2) AS Close, ROUND(Daily_Return_Pct, 2) AS Daily_Return_Pct
   FROM daily ORDER BY Daily_Return_Pct ASC LIMIT 10)
) ORDER BY Nhom, Daily_Return_Pct DESC""", 20)

print(f"\nXONG trong {time.time()-t0:.1f}s")
spark.stop()
