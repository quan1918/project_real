from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import subprocess
import requests

# Default args for the Dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['quantruong1918@gmail.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='restaurant_analytics',
    default_args=default_args,
    description='Run Spark ETL and retrain model, then send alert if needed',
    schedule_interval='@hourly',
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=['example', 'spark']
) as dag:
    
    # Task 1: check if Spark Streaming container is running
    def check_streaming_status():
        try:
            resp = requests.get("http://spark-streaming:4040", timeout=5)
            if resp.status_code == 200:
                print("✅ Spark streaming service is running.")
            else:
                raise Exception(f"❌ Spark streaming returned status {resp.status_code}")
        except Exception as e:
            raise Exception(f"❌ Spark streaming service is NOT running or unreachable: {e}")

    check_spark = PythonOperator(
        task_id="check_spark_streaming",
        python_callable=check_streaming_status
    )

    # Task 2: retrain model (batch job)
    retrain = BashOperator(
        task_id='retrain_model',
        bash_command=(
            'export PYSPARK_PYTHON=/usr/local/bin/python && '
            'export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python && '
            '/opt/spark/bin/spark-submit '
            '--master local[*] '
            '/opt/airflow/dags/train_lr.py'
        ),
        env={'PYSPARK_PYTHON': '/usr/local/bin/python'}
    )

    # Task 3: check KPIs and send alert if needed
    def send_alert_func(**context):
        # Use PostgresHook to retrieve kpi from Postgres. Configure an Airflow Connection named
        # 'postgres_restaurant' (or set AIRFLOW_CONN_POSTGRES_RESTAURANT env var).
        pg_conn_id = os.getenv('POSTGRES_CONN_ID', 'postgres_restaurant')
        hook = PostgresHook(postgres_conn_id=pg_conn_id, schema='restaurant_db')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT AVG(avg_revenue) FROM kpi_5min WHERE window_start >= now() - interval '1 hour';")
        avg_rev = cur.fetchone()[0] or 0
        cur.close()
        conn.close()

        # Replace this with real notification (Slack/webhook/email) as needed
        threshold = float(os.getenv('REVENUE_ALERT_THRESHOLD', '100'))
        if avg_rev < threshold:
            # simple print for demo; in production call an email or webhook
            print(f'ALERT: low revenue in last hour = {avg_rev} (threshold={threshold})')
        else:
            print(f'OK: avg revenue in last hour = {avg_rev}')

    send_alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert_func,
        provide_context=True
    )

    # Define order
    check_spark >> retrain >> send_alert