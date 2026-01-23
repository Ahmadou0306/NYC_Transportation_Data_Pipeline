from airflow.decorators import dag, task
from datetime import timedelta, datetime

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

@dag(
    'dag_decorateur_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
)
def mon_dag():
    """Documentation du DAG"""
    
    @task
    def task1():
        print("Hello from task1")
        return {"data": "quelque chose"}
    
    @task
    def task2(data_from_task1):
        print(f"Received: {data_from_task1}")
        return "Done"
    
    @task
    def task3(result):
        print(f"Final result: {result}")
    
    # Définir les dépendances et le flux de données
    result1 = task1()
    result2 = task2(result1)
    task3(result2)

# Instancier le DAG
mon_dag()