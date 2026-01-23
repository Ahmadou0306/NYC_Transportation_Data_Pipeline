from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


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

def mannipulation_var(ti):
    web_ui = Variable.get("variable_par_web_ui")
    Variable.set("variable_par_python", "Je suis une variable à partir de python")
    python_value = Variable.get("variable_par_python")
    print(f"Affichage variable web UI : {web_ui}")
    print(f"Affichage variable python : {python_value}")



with DAG (
    'variables_exemple',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    catchup=False,
    start_date=datetime(2026, 1,1),
    tags=['example'],
) as dag:
    debut = EmptyOperator(task_id="debut")
    manip_var= PythonOperator(
        task_id="manip_var",
        python_callable=mannipulation_var
    )
    fin = EmptyOperator(task_id="fin")

    debut >> manip_var >> fin