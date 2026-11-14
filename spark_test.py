from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("btc").master("local[*]").getOrCreate()
df = spark.read.parquet("datalake/btc_raw")
print("Số dòng:", df.count())
df.printSchema()
df.groupBy("year").count().orderBy("year").show(20)
spark.stop()
