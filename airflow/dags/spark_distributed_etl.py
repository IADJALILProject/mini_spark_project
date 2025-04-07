from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data.team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='daily_distributed_spark_etl',
    default_args=default_args,
    description='🚀 Traitement distribué Spark exécuté chaque jour via Docker',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'distributed', 'etl', 'production']
) as dag:

    run_distributed_etl = BashOperator(
        task_id='run_spark_distributed_job',
        bash_command=(
            'echo "🔥 Lancement du job Spark..." && '
            'docker exec spark-app spark-submit '
            '--master spark://spark-master:7077 '
            '--deploy-mode client '
            '/opt/spark_jobs/process_users.py'
        )
    )

    run_distributed_etl
