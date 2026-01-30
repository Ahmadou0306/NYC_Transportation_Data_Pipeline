from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import storage, bigquery
from airflow import DAG
import requests
import time
import base64
from io import BytesIO

import logging
logger = logging.getLogger(__name__)

from config.api_config import PROJECT_NAME, GCS_BUCKET_NAME, API_CONFIG, GCP_PROJECT_ID, DAG_DEFAULT_ARGS
from utils.utilitaire import get_collected_tags, build_gcs_path, fetch_data_from_url

COLLECTED_NAME = "monthly_nyc_trips"

API_CONFIGS = {
    'yellow_taxi': "yellow_axi_vehicule_trips",
    'for_hire_vehicule': "for_hire_vehicule_trips",
    'hvfhv': "high_volume_vehicule_trips"
}


def extract_and_load(api_label, **kwargs):
    """Extraction + Upload GCS + Load BigQuery en une seule fonction"""
    logger.info(f"DEBUT PIPELINE {api_label}")
    
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')
    
    config = API_CONFIG[api_label]
    base_url = config['base_url']
    format_response = config['format']
    date = execution_date.strftime('%Y-%m')
    
    logger.info(f"Date: {date}")
    logger.info(f"Source: {base_url}")
    
    # ========== EXTRACTION ==========
    max_retries = 3
    parquet_data = None
    
    for attempt in range(max_retries):
        url = f"{base_url}_{date}.{format_response}"
        try:
            results = fetch_data_from_url(url, timeout=120)
            
            if results['format'] != 'parquet':
                raise Exception(f"Format incorrect, attendu: parquet")
            
            parquet_data = results['data']  # Bytes bruts
            logger.info(f"Extraction réussie: {len(parquet_data)} bytes")
            break
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentative {attempt + 1}/{max_retries})")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur API: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    
    if not parquet_data:
        raise ValueError(f"Aucune donnée extraite pour {date}")
    
    file_size_mb = len(parquet_data) / (1024 * 1024)
    logger.info(f"Taille fichier: {file_size_mb:.2f} MB")
    
    # ========== UPLOAD GCS ==========
    try:
        periode = datetime.strptime(date, '%Y-%m')
        gcs_path = build_gcs_path(periode, api_label, 'monthly', 'parquet')
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        logger.info(f"Upload GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        blob.upload_from_string(
            parquet_data,
            content_type='application/octet-stream'
        )
        
        logger.info(f"Upload GCS réussi: {file_size_mb:.2f} MB")
        
    except Exception as e:
        logger.error(f"Erreur upload GCS: {e}")
        raise
    
    # ========== LOAD BIGQUERY ==========
    try:
        bq_client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.staging.raw_{api_label}"
        
        # Charger depuis GCS (plus efficace que depuis la mémoire pour Parquet)
        uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
        )
        
        logger.info(f"Chargement BigQuery depuis: {uri}")
        
        load_job = bq_client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )
        
        load_job.result()  # Attendre la fin
        
        logger.info(f"BigQuery: {load_job.output_rows} lignes chargées dans {table_id}")
        
        return {
            'gcs_path': f"gs://{GCS_BUCKET_NAME}/{gcs_path}",
            'bq_table': table_id,
            'rows': load_job.output_rows
        }
        
    except Exception as e:
        logger.error(f"Erreur chargement BigQuery: {e}")
        raise


# ========== DAG ==========
default_args = DAG_DEFAULT_ARGS.copy()
default_args["retry_delay"] = timedelta(minutes=5)
default_args["execution_timeout"] = timedelta(hours=2)

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Pipeline ELT mensuel: Extraction fichiers Parquet NYC Trips -> GCS + BigQuery.\n"
        "- Yellow Taxi (taxis jaunes)\n"
        "- For-Hire Vehicle (véhicules de location)\n"
        "- HVFHV (Uber/Lyft)\n"
        f"Sources: {API_CONFIG['yellow_axi_vehicule_trips']['base_url']}"
        f"Sources: {API_CONFIG['for_hire_vehicule_trips']['base_url']}"
        f"Sources: {API_CONFIG['high_volume_vehicule_trips']['base_url']}"
    ),
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2025, 1, 31),
    schedule='0 22 1 * *',  # 1er du mois à 22h
    catchup=True,
    max_active_runs=1,
    tags=get_collected_tags(COLLECTED_NAME, "month"),
) as dag:
    
    debut = EmptyOperator(task_id="debut")
    fin = EmptyOperator(task_id="fin", trigger_rule="all_success")
    
    # Créer les tâches dynamiquement
    tasks = []
    
    for key, api_label in API_CONFIGS.items():
        task = PythonOperator(
            task_id=f'extract_load_{key}',
            python_callable=extract_and_load,
            op_kwargs={'api_label': api_label},
        )
        tasks.append(task)
    
    # Pipeline: Extraction/Load en parallèle
    debut >> tasks >> fin