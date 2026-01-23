from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta

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


dag = DAG(
    "dag_constructeur_exemple",
    default_args=default_args,
    description="Un Simple dag de test. Notre premier dag",
    # schedule = "* * * * *" # https://crontab.guru/
    schedule=timedelta(days=1),
    # start_date=datetime(2025,8,1)
    # end_date=datetime(2026,1,31)
    catchup = True # Activer ou désactiver le rattrapage 
)


def print_hello():
    # res=1/0 #Injection d'erreur 
    print("Hello world")

start = EmptyOperator(
    task_id="start_print_hello",
    dag=dag
)

hello_world = PythonOperator(
    task_id="execte_cmd_print_hello",
    python_callable=print_hello,
    dag=dag
)


end = EmptyOperator(
    task_id="end_print_hello",
    dag=dag
)

# Relancer le scheduler avec airflow scheduler
start >> hello_world >> end