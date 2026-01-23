from datetime import timedelta, datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
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

def analyse_file():
    print("Analyse du fichier firewall.csv ...")

with DAG (
    'filesensor_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")

    wait_listen_file = FileSensor(
        task_id="wait_listen_file",
        filepath="/opt/airflow/dags/data/filesensor_export.csv",
        poke_interval= 60, # l'intervalle de vérification du finchier
        timeout= 3600 , # Temps de vérification de la condition
        # mode poke c'est le mode actif. Il occupe la ressource jusqu'a ce que la tache soit fini. A privilégié en environnement critique.
        # mode reschedule c'est le léger, il libére les ressource entre les vérifications de condition. A privilégié pour les taches non urgente.
        mode="reschedule", 
    )

    analyze_file_task = PythonOperator(
        task_id="analyze_file_task",
        python_callable=analyse_file,
    )

    fin = EmptyOperator(task_id="fin")

    debut >> wait_listen_file >> analyze_file_task >> fin

