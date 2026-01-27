from airflow import DAG, Asset
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, GCP_PROJECT_ID
from utils.utilitaire import get_collected_tags
# ========== CONFIGURATION ==========
COLLECTED_NAME = "NYC_Real_Time_Traffic_Speed"

gcs_traffic_speed = Asset(ASSET_PATH["traffic_speed"])

# ========== DAG QUOTIDIEN ==========
base_tags = ['load', 'bigquery', 'hourly']
collected_tags = get_collected_tags(COLLECTED_NAME, "hour")
all_tags = base_tags + collected_tags
with DAG(
    f'{PROJECT_NAME}_load_to_bq_hourly',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement des données horaires dans Big query',
    schedule=[gcs_traffic_speed],  # Consomme l'asset
    catchup=False,
    max_active_runs=1,
    tags=all_tags,
) as dag:
    
    load_traffic_speed = GCSToBigQueryOperator(
        task_id='load_to_bigquery_hourly',
        bucket=GCS_BUCKET_NAME,
        source_objects=[
            f"raw/{COLLECTED_NAME}/"
            "year={{ execution_date.year }}/"
            "month={{ execution_date.month }}/"
            "day={{ execution_date.day }}/"
            f"{COLLECTED_NAME}_"
            "{{ execution_date.strftime('%Y%m%d_%H') }}00.json"
        ],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.staging.raw_traffic_speed',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )
    
    
    load_traffic_speed