from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

from config.api_config import API_CONFIG, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, GCP_PROJECT_ID

# ========== CONFIGURATION ==========
DATASET_STAGING = "staging"

# ========== DAG QUOTIDIEN ==========
with DAG(
    f'{PROJECT_NAME}_load_to_bq_hourly',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement des données horaires dans Big query',
    schedule='0 3 * * *',  # 3h du matin (après extractions)
    catchup=True,
    max_active_runs=1,
    tags=['load', 'bigquery', 'daily'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # ===== 311 REQUESTS =====
    load_traffic_spped = GCSToBigQueryOperator(
        task_id='load_311_requests',
        bucket=GCS_BUCKET_NAME,
        source_objects=['raw/NYC_Real_Time_Traffic_Speed/year={{ execution_date.year }}/month={{ execution_date.month }}/day={{ execution_date.day }}/*.json'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{DATASET_STAGING}.raw_311_requests',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )
    
    end = EmptyOperator(task_id='end')
    
    # Exécution en parallèle
    start >> [load_traffic_spped] >> end