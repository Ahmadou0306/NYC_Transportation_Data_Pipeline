from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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



def task_execution(task_name):
    print(f"Tache {task_name} exécutée")

def task_many_fail(task_name):
    if random.choice([True, False]):
        raise ValueError(f"Tache {task_name} a échoué")
    print(f"Tache {task_name} exécutée")

with DAG (
    'triggers_rules_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=task_many_fail,
        op_kwargs={"task_name":"task_1"}
    )
    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=task_many_fail,
        op_kwargs={"task_name":"task_2"}
    )
    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=task_many_fail,
        op_kwargs={"task_name":"task_3"},
        trigger_rule="none_failed" # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules
    )

    task_4 = PythonOperator(
        task_id="task_4",
        python_callable=task_execution,
        op_kwargs={"task_name":"task_4"},
        trigger_rule="one_failed"
    )

    task_5 = PythonOperator(
        task_id="task_5",
        python_callable=task_execution,
        op_kwargs={"task_name":"task_5"},
        trigger_rule="none_failed_min_one_success"
    )

    task_6 = PythonOperator(
        task_id="task_6",
        python_callable=task_execution,
        op_kwargs={"task_name":"task_6"},
        trigger_rule="all_failed"
    )

    fin = EmptyOperator(task_id="fin")

    
    debut >> [task_1, task_2, task_3] >> fin 
    task_1 >> [ task_6, task_4, task_5]
    task_2 >> [ task_6, task_4, task_5]
    task_3 >> [ task_6, task_4, task_5]