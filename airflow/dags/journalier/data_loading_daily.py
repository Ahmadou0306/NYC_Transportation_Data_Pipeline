from airflow import DAG, Asset
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, GCP_PROJECT_ID
from utils.utilitaire import get_collected_tags
# ========== CONFIGURATION ==========

SOURCES_CONFIG = {
    'traffic_volume': {
        'collected_name': 'Comptages_vehicules_intersection',
        'asset': Asset(ASSET_PATH["traffic_volume"]),
        'source_format': 'CSV',
        'skip_leading_rows': 1,
    },
    'weather': {
        'collected_name': 'NOAA_Weather_Data',
        'asset': Asset(ASSET_PATH["weather"]),
        'source_format': 'NEWLINE_DELIMITED_JSON',
    },
    '311_requests': {
        'collected_name': 'NYC_311_Service_Requests',
        'asset': Asset(ASSET_PATH["311_requests"]),
        'source_format': 'NEWLINE_DELIMITED_JSON',
    },
}

daily_assets = [config['asset'] for config in SOURCES_CONFIG.values()]

# ========== DAG QUOTIDIEN =========
base_tags = ['load', 'bigquery']
collected_tags = get_collected_tags([s['collected_name'] for s in SOURCES_CONFIG.values()])
if collected_tags and isinstance(collected_tags[0], list):
    collected_tags = collected_tags[0]  # Aplatir

print(f"DEBUG: {collected_tags}")
print(f"Type: {type(collected_tags)}")

all_tags = base_tags + collected_tags

with DAG(
    f'{PROJECT_NAME}_load_to_bq_daily',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement quotidien : traffic_volume, weather, 311_requests -> BigQuery',
    schedule=daily_assets,  # Demarre quand TOUS les assets sont prêts
    catchup=False,
    max_active_runs=1,
    tags=all_tags,
) as dag:
    
        
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_tasks = []

    for key, config in SOURCES_CONFIG.items():
        collected_name = config['collected_name']
        load_task = GCSToBigQueryOperator(
            task_id=f'load_{key}',
            bucket=GCS_BUCKET_NAME,
            source_objects=[
                f"raw/{collected_name}/"
                "year={{ execution_date.year }}/"
                "month={{ execution_date.month }}/"
                "day={{ execution_date.day }}/"
                f"{collected_name}_"
                "{{ execution_date.strftime('%Y%m%d') }}"
                f".{'csv' if config['source_format'] == 'CSV' else 'json'}" # ICI les seules sources que nous avons sont les jsons et les csv
            ],
            destination_project_dataset_table=f'{GCP_PROJECT_ID}.staging.raw_{key}',
            source_format=config['source_format'],
            skip_leading_rows=config.get('skip_leading_rows', 0),
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            autodetect=True,
            gcp_conn_id='google_cloud_default',
        )

        load_tasks.append(load_task)

    #ici load_tasks est un taableau donc il y'aura une exécution en paralléle
    start >> load_tasks >> end

    
    
