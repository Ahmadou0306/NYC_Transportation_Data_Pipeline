from airflow import DAG, Asset
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from google.cloud import bigquery
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import json
import logging
import re
logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, GCP_PROJECT_ID
from utils.utilitaire import get_collected_tags, convert_to_ndjson
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

def get_date_from_asset(**kwargs):

    triggering_asset_events = kwargs.get('triggering_asset_events', {})
    
    if triggering_asset_events:
        # Récupérer l'URI de l'asset
        asset_uri = list(triggering_asset_events.keys())[0]
        logger.info(f"Asset URI: {asset_uri}")
        
        # Parser la date : "gcs://bucket/.../2020/01/15/14"
        match = re.search(r'/(\d{4})/(\d{2})/(\d{2})$', asset_uri)
        
        if match:
            year, month, day = map(int, match.groups())
            logical_date = datetime(year, month, day)
            logger.info(f"Date extraite de l'Asset: {logical_date}")
        else:
            raise ValueError(f"Impossible de parser la date depuis {asset_uri}")
    else:
        # Exécution manuelle
        logical_date = kwargs['logical_date']
        logger.info(f"Exécution manuelle: {logical_date}")
    
    # Stocker dans XCom pour les autres tâches
    ti = kwargs['ti']
    ti.xcom_push(key='file_date', value=logical_date.isoformat())
    
    return logical_date.isoformat()


def convert_json_to_ndjson(collected_name,**kwargs):
    ti = kwargs['ti']
    date_str = ti.xcom_pull(task_ids='get_file_date', key='file_date')
    logical_date = datetime.fromisoformat(date_str)

    source_path = (
        f"raw/{collected_name}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"{collected_name}_{logical_date.strftime('%Y%m%d')}.json"
        )
    dest_path = source_path.replace('.json', '_ndjson.json')
    logger.info(f"gs://{GCS_BUCKET_NAME}/{source_path}")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(source_path)

    json_content = blob.download_as_text()
    data = json.loads(json_content)
   
    logger.info(f"{len(data)} enregistrements trouvés")

    ndjson_content = convert_to_ndjson(data) 

    dest_blob = bucket.blob(dest_path)
    dest_blob.upload_from_string(ndjson_content)
    
    logger.info(f"Fichier NDJSON créé : gs://{GCS_BUCKET_NAME}/{dest_path}")
    
    # Retourner le chemin du fichier NDJSON pour la prochaine tâche
    return dest_path

def load_ndjson_to_bigquery(table_name,collected_name,**kwargs):
    ti = kwargs['ti']
    date_str = ti.xcom_pull(task_ids='get_file_date', key='file_date')
    logical_date = datetime.fromisoformat(date_str)
    
    # Construire le chemin
    source_path = (
        f"raw/{collected_name}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"{collected_name}_{logical_date.strftime('%Y%m%d')}_ndjson.json"
    )
    
    # Charger dans BigQuery
    client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.staging.raw_{table_name}"
    uri = f"gs://{GCS_BUCKET_NAME}/{source_path}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )
    
    logger.info(f"Chargement depuis : {uri}")
    
    load_job = client.load_table_from_uri(
        uri,
        f"{GCP_PROJECT_ID}.staging.raw_{table_name}",
        job_config=job_config
    )
    
    load_job.result()
    logger.info(f"Chargé {load_job.output_rows} lignes dans {table_id}")


def load_csv_to_bigquery(table_name,collected_name,**kwargs):
    ti = kwargs['ti']
    date_str = ti.xcom_pull(task_ids='get_file_date', key='file_date')
    logical_date = datetime.fromisoformat(date_str)
    
    # Construire le chemin
    source_path = (
        f"raw/{collected_name}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"{collected_name}_{logical_date.strftime('%Y%m%d')}.csv"
    )
    
    # Charger dans BigQuery
    client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.staging.raw_{table_name}"
    uri = f"gs://{GCS_BUCKET_NAME}/{source_path}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )
    
    logger.info(f"Chargement depuis : {uri}")
    
    load_job = client.load_table_from_uri(
        uri,
        f"{GCP_PROJECT_ID}.staging.raw_{table_name}",
        job_config=job_config
    )
    
    load_job.result()
    logger.info(f"Chargé {load_job.output_rows} lignes dans {table_id}")



def delete_ndjson(collected_name, **kwargs):
    ti = kwargs['ti']
    date_str = ti.xcom_pull(task_ids='get_file_date', key='file_date')
    logical_date = datetime.fromisoformat(date_str)

    path = (
        f"raw/{collected_name}/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"{collected_name}_{logical_date.strftime('%Y%m%d')}_ndjson.json"
        )
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(path)
        
        # Vérifier si le fichier existe avant de le supprimer
        if blob.exists():
            blob.delete()
            logger.info(f"Fichier supprimé : gs://{GCS_BUCKET_NAME}/{path}")
        else:
            raise Exception(f"Fichier introuvable: : gs://{GCS_BUCKET_NAME}/{path}")   
    except Exception as e:
        logger.error(f"Erreur lors de la suppression : {str(e)}")
        raise


with DAG(
    f'{PROJECT_NAME}_load_to_bq_daily',
    default_args=DAG_DEFAULT_ARGS,
    description='Chargement quotidien : traffic_volume, weather, 311_requests -> BigQuery',
    schedule=daily_assets,  # Demarre quand TOUS les assets sont prêts
    catchup=False,
    max_active_runs=1,
    tags=all_tags,
) as dag:

    get_date = PythonOperator(
        task_id='get_file_date',
        python_callable=get_date_from_asset,
    )
    for key, config in SOURCES_CONFIG.items():
        collected_name = config['collected_name']

        # Si c'est un JSON (pas CSV), on ajoute conversion + cleanup
        if config['source_format'] == 'NEWLINE_DELIMITED_JSON':
            
            convert_task = PythonOperator(
                task_id=f'convert_{key}_to_ndjson',
                python_callable=convert_json_to_ndjson,
                op_kwargs={'collected_name': collected_name},
            )
            
            load_task = PythonOperator(
                task_id=f'load_{key}_to_bigquery_daily',
                python_callable=load_ndjson_to_bigquery,
                op_kwargs={
                    'table_name': key,
                    'collected_name': collected_name
                },
            )
            
            cleanup_task = PythonOperator(
                task_id=f'cleanup_{key}_ndjson',
                python_callable=delete_ndjson,
                op_kwargs={'collected_name': collected_name},
                trigger_rule="all_success",
            )
            
            # Enchaînement : convert -> load -> cleanup
            get_date >> convert_task >> load_task >> cleanup_task

        
        else:  # CSV - pas de conversion
            load_task = PythonOperator(
                task_id=f'load_{key}_to_bigquery_daily',
                python_callable=load_csv_to_bigquery,
                op_kwargs={
                    'table_name': key,
                    'collected_name': collected_name
                },
            )
            
            get_date >> load_task

            