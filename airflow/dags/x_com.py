from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 20),
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def push_x_com(ti):
    """Pousse un message dans XCom"""
    ti.xcom_push(key="simple_message", value="Hello depuis la task push xcom")
    print("Message envoyé dans XCom")


def pull_x_com(ti):
    """Récupère le message depuis XCom"""
    # CORRECTION : task_ids (avec un S)
    message = ti.xcom_pull(task_ids="push_task", key="simple_message")
    print(f"Message reçu : {message}")


with DAG(
    'x_com_exemple',
    default_args=default_args,
    description='Exemple XCom simple',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example', 'xcom'],
) as dag:
    
    debut = EmptyOperator(task_id="debut")
    
    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_x_com,
    )
    
    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_x_com,
        trigger_rule="none_failed_min_one_success"
    )
    
    fin = EmptyOperator(task_id="fin")
    
    debut >> push_task >> pull_task >> fin

"""
{
    'ti': <TaskInstance>,
    'ds': '2026-01-20',
    'ds_nodash': '20260120',
    'execution_date': datetime(2026, 1, 20),
    'dag': <DAG>,
    'task': <Task>,
    'dag_run': <DagRun>,
    'run_id': 'scheduled__2026-01-20T00:00:00+00:00',
    'conf': {},
    # ... et beaucoup d'autres
}
"""