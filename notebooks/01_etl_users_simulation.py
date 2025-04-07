from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, floor

spark = SparkSession.builder.appName("ETL Notebook Simulation").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("/dbfs/users.csv")
df = df.withColumn("balance", spark_round(col("salary") - col("expenses"), 2))
df = df.withColumn("age_group", (floor(col("age") / 10) * 10))

# ➕ Affichage interactif pour exploration
print("✅ Solde négatif")
df.filter(col("balance") < 0).show()

print("✅ Agrégations")
df.groupBy("age_group").avg("salary", "expenses", "balance").orderBy("age_group").show()
df.write.mode("overwrite").parquet("/dbfs/test_output")
df.write.mode("overwrite").csv("/dbfs/test_output")