from airflow import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, GCP_PROJECT_ID
from utils.utilitaire import get_collected_tags

# ========== CONFIGURATION ==========
SOURCES_CONFIG = {
    'yellow_taxi': {
        'collected_name': 'yellow_taxi',
        'asset': Asset(ASSET_PATH["yellow_taxi"]),
    },
    'for_hire_vehicule': {
        'collected_name': 'for_hire_vehicule',
        'asset': Asset(ASSET_PATH["for_hire_vehicule"]),
    },
    'hvfhv': {
        'collected_name': 'hvfhv',
        'asset': Asset(ASSET_PATH["hvfhv"]),
    },
}

monthly_assets = [config['asset'] for config in SOURCES_CONFIG.values()]

def get_date_from_asset(**kwargs):
    """Extrait la date depuis l'URI de l'Asset trigger"""
    triggering_asset_events = kwargs.get('triggering_asset_events', {})
    asset_uri = list(triggering_asset_events.keys())[0]
    
    if triggering_asset_events:
        logger.info(f"Asset URI: {asset_uri}")
        
        # Parser la date : "gcs://bucket/.../2020/01"
        match = re.search(r'/(\d{4})/(\d{2})$', asset_uri)
        
        if match:
            year, month = map(int, match.groups())
            logical_date = datetime(year, month, 1)
            logger.info(f"Date extraite de l'Asset: {logical_date}")
        else:
            raise ValueError(f"Impossible de parser la date depuis {asset_uri}")
    else:
        logical_date = kwargs['logical_date']
        logger.info(f"Exécution manuelle: {logical_date}")
    
    ti = kwargs['ti']
    ti.xcom_push(key='file_date', value=logical_date.isoformat())
    
    return logical_date.isoformat()


def load_parquet_to_bigquery(table_name, collected_name, **kwargs):
    """Charge un fichier Parquet depuis GCS vers BigQuery"""
    ti = kwargs['ti']
    date_str = ti.xcom_pull(task_ids='get_file_date', key='file_date')
    logical_date = datetime.fromisoformat(date_str)
    
    # Construire le chemin
    source_path = (
        f"raw/{collected_name}/"
        f"year={logical_date.year}/"
        f"{collected_name}_{logical_date.strftime('%Y%m')}.parquet"
    )
    
    # Charger dans BigQuery
    client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.staging.raw_{table_name}"
    uri = f"gs://{GCS_BUCKET_NAME}/{source_path}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )
    
    logger.info(f"Chargement depuis : {uri}")
    
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )
    
    load_job.result()
    logger.info(f"Chargé {load_job.output_rows} lignes dans {table_id}")


# ========== DAG ==========
base_tags = ['load', 'bigquery', 'monthly']
collected_tags = get_collected_tags(
    [s['collected_name'] for s in SOURCES_CONFIG.values()], 
    "monthly"
)

if collected_tags and isinstance(collected_tags[0], list):
    collected_tags = collected_tags[0]

all_tags = base_tags + collected_tags

with DAG(
    f'{PROJECT_NAME}_load_to_bq_monthly',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement mensuel : yellow_taxi, for_hire_vehicule, hvfhv -> BigQuery',
    schedule=monthly_assets,
    catchup=False,
    max_active_runs=1,
    tags=all_tags,
) as dag:
    
    # Extraire la date de l'Asset
    get_date = PythonOperator(
        task_id='get_file_date',
        python_callable=get_date_from_asset,
    )
    
    # Créer les tâches de chargement
    for key, config in SOURCES_CONFIG.items():
        load_task = PythonOperator(
            task_id=f'load_{key}',
            python_callable=load_parquet_to_bigquery,
            op_kwargs={
                'table_name': key,
                'collected_name': config['collected_name']
            },
        )
        
        get_date >> load_task