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
    'yellow_taxi': {
        'collected_name': 'yellow_axi_vehicule_trips',
        'asset': Asset(ASSET_PATH["yellow_taxi"]),
        'source_format': 'PARQUET',
    },
    'for_hire_vehicule': {
        'collected_name': 'for_hire_vehicule_trips',
        'asset': Asset(ASSET_PATH["for_hire_vehicule"]),
        'source_format': 'PARQUET',
    },
    'hvfhv': {
        'collected_name': 'high_volume_vehicule_trips',
        'asset': Asset(ASSET_PATH["hvfhv"]),
        'source_format': 'PARQUET',
    },
}

monthly_assets = [config['asset'] for config in SOURCES_CONFIG.values()]

# ========== DAG QUOTIDIEN ==========

base_tags = ['load', 'bigquery']
collected_tags = get_collected_tags([s['collected_name'] for s in SOURCES_CONFIG.values()], "monthly")

if collected_tags and isinstance(collected_tags[0], list):
    collected_tags = collected_tags[0]  # Aplatir

all_tags = base_tags + collected_tags

with DAG(
    f'{PROJECT_NAME}_load_to_bq_monthly',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement quotidien : yellow_taxi, for_hire_vehicule, high_volume_vehicule_trips -> BigQuery',
    schedule=monthly_assets,  # Demarre quand TOUS les assets sont prêts
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
                f"{collected_name}_"
                "{{ execution_date.strftime('%Y%m') }}.parquet"
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

    
    
