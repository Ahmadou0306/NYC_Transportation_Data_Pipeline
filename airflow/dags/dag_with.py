from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
    'start_date': datetime(2026,1,20),
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG (
    'dag_with_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")
    fin = EmptyOperator(task_id="fin")

    debut >> fin