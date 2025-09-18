1) Objectifs

Pipeline PySpark batch + streaming: Kafka → Delta Lake (Bronze/Silver/Gold), orchestration Airflow, tests pytest, déploiement Docker/k8s.

2) Architecture
Producers → Kafka topics
  → Spark Structured Streaming (checkpoint + watermark)
  → Delta Bronze/Silver/Gold (MERGE/OPTIMIZE/VACUUM)
Airflow DAG: ingest → transform → optimize → validate
Monitoring: Prometheus/Grafana

3) Arborescence
spark_jobs/{batch.py, streaming.py, utils.py}
airflow/dags/spark_delta_pipeline.py
delta/{bronze,silver,gold}/
tests/test_jobs.py
docker-compose.yml

4) Démarrage rapide
docker compose up -d
python tools/produce_events.py                      
airflow dags trigger spark_delta_pipeline
pytest -q

5) Détails techniques

Exactly-once: checkpoints & idempotence.

Delta: MERGE INTO, OPTIMIZE ZORDER (ts), VACUUM.

Watermark: withWatermark("event_time","10 minutes").

6) KPIs

Latence E2E P95, throughput events/s, taille Bronze vs Silver, coût I/O.

7) k8s (option)

KubernetesPodOperator pour lancer le job.

Persist checkpoints sur PVC / objet (S3/ADLS).

8) Troubleshooting

Late data drop → augmenter watermark.

Small files → planifier OPTIMIZE.
