#!/bin/bash

echo "🔄 Attente de la présence du fichier /dbfs/users.csv..."

for i in {1..60}; do
  if [ -f "/dbfs/users.csv" ]; then
    echo "✅ Fichier trouvé !"
    break
  fi
  echo "⏳ Attente... ($i/60)"
  sleep 1
done

if [ ! -f "/dbfs/users.csv" ]; then
  echo "❌ ERREUR : Le fichier /dbfs/users.csv est introuvable après 60s."
  exit 1
fi

echo "📊 Pré-vérification du schéma Spark..."

cat <<EOF > /tmp/check_schema.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SchemaCheck").getOrCreate()
df = spark.read.option("header", True).option("inferSchema", True).csv("/dbfs/users.csv")
print("✅ Colonnes détectées :", df.columns)
df.printSchema()
spark.stop()
EOF

spark-submit --master "$SPARK_MASTER" --deploy-mode client /tmp/check_schema.py
echo "✅ Schéma vérifié !"
echo "🔄 Préparation de l'environnement Spark..."

echo "🚀 Lancement du traitement principal Spark"
spark-submit --master "$SPARK_MASTER" --deploy-mode client /opt/spark_jobs/process_users.py
echo "✅ Traitement terminé !"