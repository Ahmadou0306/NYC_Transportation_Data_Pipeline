from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import random


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

def task_many_fail(task_name):
    if random.choice([True, False]):
        raise ValueError(f"Tache {task_name} a échoué")
    print(f"Tache {task_name} exécutée")


with DAG (
    'operator_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")
    
    task_bash = BashOperator(
        task_id="afficher_date",
        bash_command="date"
    )
    task_write_bash = BashOperator(
        task_id="saugarde_logs",
        bash_command="echo 'tache executé' >> /tmp/log.txt"
    )

    task_python_1 = PythonOperator(
        task_id="task_python_1",
        python_callable=task_many_fail,
        op_kwargs={"task_name":"task_1"}
    )


    fin = EmptyOperator(
        task_id="fin",
        trigger_rule="all_success"  
    )

    debut >> [task_bash, task_python_1] >> fin
    task_bash >> task_write_bash