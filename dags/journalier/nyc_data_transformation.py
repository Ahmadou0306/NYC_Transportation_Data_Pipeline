


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator # Correct import for modern Airflow
from config.api_config import DAG_DEFAULT_ARGS, PROJECT_NAME
from utils.utilitaire import get_collected_tags


default_args = DAG_DEFAULT_ARGS.copy()
default_args["retry_delay"] = timedelta(minutes=5)
default_args["execution_timeout"] = timedelta(hours=2)

DBT_PROJECT_PATH = "/opt/airflow/dbt"

with DAG(
    f"{PROJECT_NAME}_transformations",
    default_args=default_args,
    description=(
        "Execution journalieres des transformations dbt"
    ),
    start_date=datetime(2025,2,6),
    schedule='0 3 * * *',  # Tous les jours à 03h
    catchup=False,
    max_active_runs=1,
    tags=get_collected_tags("transformations", "days"),
) as dag:
    
    run_job_task = BashOperator(
        task_id="run_job_task",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            "dbt run --target prod"
        ),
    )

    test_job_task = BashOperator(
        task_id="test_job_task",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            "dbt test --target prod"
        ),
        trigger_rule="all_success", # Succés du parent
    )

    run_job_task >> test_job_task