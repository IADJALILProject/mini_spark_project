from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, avg, count, when

spark = SparkSession.builder \
    .appName("ProcessUsers") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .getOrCreate()

df = spark.read.option("header", True).csv("/dbfs/users.csv")

df = df.withColumn("balanced", col("balance").cast("double")) \
       .withColumn("salary", col("salary").cast("double")) \
       .withColumn("expenses", col("expenses").cast("double")) \
       .withColumn("age", col("age").cast("int"))

df = df.withColumn("age_group", floor(col("age") / 10) * 10)

df_negative = df.filter(col("balanced") < 0)
df_negative.write.mode("overwrite").parquet("/dbfs/results/negative_balance")

df_age_agg = df.groupBy("age_group").agg(
    count("*").alias("nb_users"),
    avg("salary").alias("avg_salary"),
    avg("expenses").alias("avg_expenses"),
    avg("balanced").alias("avg_balance")
)
df_age_agg.write.mode("overwrite").parquet("/dbfs/results/aggregated_by_age")

spark.stop()
