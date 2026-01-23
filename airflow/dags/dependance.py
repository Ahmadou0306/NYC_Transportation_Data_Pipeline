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
    'dependance_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")

    branch_1 = EmptyOperator(task_id="branch_1")
    task_1_1 = EmptyOperator(task_id="task_1_1")
    task_1_2 = EmptyOperator(task_id="task_1_2")

    branch_2 = EmptyOperator(task_id="branch_2")
    task_2_1 = EmptyOperator(task_id="task_2_1")
    task_2_2 = EmptyOperator(task_id="task_2_2")
    task_2_3 = EmptyOperator(task_id="task_2_3")

    fin = EmptyOperator(task_id="fin")

    
    debut >> [branch_1, branch_2] >> fin 
    branch_1 >> [task_1_1, task_1_2]
    branch_2 >> [task_2_1, task_2_2, task_2_3]
    
    #debut >> fin
    # fin.set_upstream(debut)

    # fin << debut
    #fin.set_downstream(debut)