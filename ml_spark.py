import csv, os, time
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier

spark = (SparkSession.builder.appName("btc-ml").master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t0 = time.time()
os.makedirs("outputs", exist_ok=True)

d = (spark.read.csv("outputs/btc_daily.csv", header=True, inferSchema=True)
          .withColumn("Date", F.to_date("Date")))
w = Window.orderBy("Date")
def win(n): return Window.orderBy("Date").rowsBetween(-(n - 1), 0)
def lg(c, k=1): return F.lag(c, k).over(w)

# ---------- 19 dac trung (chi dung thong tin den het ngay t) ----------
df = (d.select("Date", "High", "Low", "Close", "Volume")
        .withColumn("daily_return", (F.col("Close") / lg("Close") - 1) * 100)
        .withColumn("hl_range", F.col("High") - F.col("Low"))
        .withColumn("hl_range_pct", F.col("hl_range") / F.col("Close") * 100)
        .withColumn("vc_raw", (F.col("Volume") / lg("Volume") - 1) * 100))   # chia cho 0 -> null
df = df.withColumn("volume_change", F.coalesce(
        "vc_raw",
        F.last("vc_raw", ignorenulls=True).over(Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)),
        F.first("vc_raw", ignorenulls=True).over(Window.orderBy("Date").rowsBetween(0, Window.unboundedFollowing)),
        F.lit(0.0)))
for k in (1, 2, 3):
    df = (df.withColumn(f"close_lag{k}", lg("Close", k))
            .withColumn(f"return_lag{k}", lg("daily_return", k))
            .withColumn(f"volume_lag{k}", lg("Volume", k)))
for n in (7, 14):
    full = F.count("Close").over(win(n)) == n
    df = (df.withColumn(f"ma{n}", F.when(full, F.avg("Close").over(win(n))))
            .withColumn(f"rolling_std{n}", F.when(full, F.stddev("Close").over(win(n)))))
df = (df.withColumn("ma7_ma14_ratio", F.col("ma7") / F.col("ma14"))
        .withColumn("weekday", (F.dayofweek("Date") + 5) % 7)          # 0 = thu Hai ... 6 = Chu nhat
        .withColumn("target", (F.lead("Close", 1).over(w) > F.col("Close")).cast("int")))

FEATURES = ["daily_return", "hl_range", "hl_range_pct", "volume_change",
            "close_lag1", "close_lag2", "close_lag3",
            "return_lag1", "return_lag2", "return_lag3",
            "volume_lag1", "volume_lag2", "volume_lag3",
            "ma7", "ma14", "ma7_ma14_ratio", "rolling_std7", "rolling_std14", "weekday"]
assert len(FEATURES) == 19
df = df.dropna(subset=FEATURES + ["target"])   # bo dong dau (thieu lag/MA) va ngay cuoi (chua co ngay mai)

# ---------- chia train/test THEO THU TU THOI GIAN 80/20 ----------
df = df.withColumn("rn", F.row_number().over(w))
n = df.count()
n_train = int(0.8 * n)
train = df.filter(F.col("rn") <= n_train).cache()
test = df.filter(F.col("rn") > n_train).cache()
n_test = n - n_train
tr_up = train.agg(F.avg("target")).first()[0]
te_up = test.agg(F.avg("target")).first()[0]
tr_rng = train.agg(F.min("Date"), F.max("Date")).first()
te_rng = test.agg(F.min("Date"), F.max("Date")).first()
print(f"N = {n:,} | train {n_train:,} ({tr_rng[0]} -> {tr_rng[1]}, {tr_up:.2%} tang)"
      f" | test {n_test:,} ({te_rng[0]} -> {te_rng[1]}, {te_up:.2%} tang)")

assembler = VectorAssembler(inputCols=FEATURES, outputCol="features_raw")
train_v, test_v = assembler.transform(train), assembler.transform(test)
scaler = StandardScaler(inputCol="features_raw", outputCol="features_scaled", withMean=True, withStd=True).fit(train_v)  # chi hoc tren train
train_s, test_s = scaler.transform(train_v).cache(), scaler.transform(test_v).cache()

models = {
    "Logistic Regression": LogisticRegression(featuresCol="features_scaled", labelCol="target", maxIter=1000,
                                              regParam=1.0 / (0.1 * n_train), elasticNetParam=0.0, standardization=False),
    "Decision Tree": DecisionTreeClassifier(featuresCol="features_raw", labelCol="target",
                                            maxDepth=5, minInstancesPerNode=20, seed=42),
    "Random Forest": RandomForestClassifier(featuresCol="features_raw", labelCol="target",
                                            numTrees=100, maxDepth=6, minInstancesPerNode=10, seed=42),
}


def score(name, tp, fp, tn, fn):
    tot = tp + fp + tn + fn
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return {"Model": name, "Accuracy": round((tp + tn) / tot, 4), "Precision": round(prec, 4),
            "Recall": round(rec, 4), "F1": round(f1, 4), "TP": tp, "FP": fp, "TN": tn, "FN": fn}


results, preds, importance = [], None, []
# muc co so: luon doan lop xuat hien nhieu nhat trong train
maj = 1 if tr_up >= 0.5 else 0
n_up = int(round(te_up * n_test)); n_dn = n_test - n_up
results.append(score("Baseline (lop da so)", n_up if maj else 0, n_dn if maj else 0,
                     0 if maj else n_dn, 0 if maj else n_up))

for name, est in models.items():
    m = est.fit(train_s)
    p = m.transform(test_s).select("Date", "target", F.col("prediction").cast("int").alias("prediction"))
    c = {(r["target"], r["prediction"]): r["count"] for r in p.groupBy("target", "prediction").count().collect()}
    results.append(score(name, c.get((1, 1), 0), c.get((0, 1), 0), c.get((0, 0), 0), c.get((1, 0), 0)))
    short = {"Logistic Regression": "pred_lr", "Decision Tree": "pred_dt", "Random Forest": "pred_rf"}[name]
    p = p.select("Date", "target", F.col("prediction").alias(short))
    preds = p if preds is None else preds.join(p.select("Date", short), "Date")
    if name != "Logistic Regression":
        for f_name, v in zip(FEATURES, m.featureImportances.toArray()):
            importance.append({"Model": name, "Feature": f_name, "Importance": round(float(v), 4)})

def write(path, rows, cols):
    with open(path, "w", newline="") as f:
        wr = csv.writer(f); wr.writerow(cols)
        for r in rows: wr.writerow([r[c] for c in cols])

cols = ["Model", "Accuracy", "Precision", "Recall", "F1", "TP", "FP", "TN", "FN"]
write("outputs/ml_metrics.csv", results, cols)
write("outputs/ml_feature_importance.csv",
      sorted(importance, key=lambda r: (r["Model"], -r["Importance"])), ["Model", "Feature", "Importance"])
prow = [r.asDict() for r in preds.orderBy("Date").collect()]
write("outputs/ml_predictions.csv", prow, ["Date", "target", "pred_lr", "pred_dt", "pred_rf"])

print("\n" + " | ".join(f"{c:>20}" if i == 0 else f"{c:>9}" for i, c in enumerate(cols)))
for r in results:
    print(" | ".join(f"{r[c]:>20}" if i == 0 else f"{r[c]:>9}" for i, c in enumerate(cols)))
print("\nTop dac trung Random Forest:")
for r in [x for x in sorted(importance, key=lambda r: -r["Importance"]) if x["Model"] == "Random Forest"][:5]:
    print(f"  {r['Feature']:<16}{r['Importance']:.4f}")
print(f"\nXONG trong {time.time()-t0:.1f}s")
spark.stop()
