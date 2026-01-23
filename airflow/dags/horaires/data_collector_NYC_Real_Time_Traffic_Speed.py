from datetime import timedelta, datetime
from airflow import DAG
from utils.utilitaire import get_collected_tags, pull_x_com, push_x_com
from config.api_config import API_CONFIG, PROJECT_NAME, GCS_BUCKET_NAME
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import time
import requests
import json
import os

from google.cloud import storage


import logging
logger = logging.getLogger(__name__)

COLLECTED_NAME = "NYC_Real_Time_Traffic_Speed"

def extract_data_with_pagination(
    base_url: str,
    start_datetime: str,
    end_datetime: str,
    date_field: str = 'data_as_of',
    limit: int = 10000,
    max_retries: int = 3
) -> list:
    all_data = []
    offset = 0
    page = 1

    logger.info(f"Début extraction: {start_datetime} → {end_datetime}")

    while True:
        url = (
            f"{base_url}"
            f"?$limit={limit}"
            f"&$offset={offset}"
            f"&$where={date_field} between '{start_datetime}' and '{end_datetime}'"
            f"&$order={date_field} ASC"
        )
        logger.info(f"Page {page} - Offset {offset}")
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()

                if not data or len(data) == 0:
                    logger.info(f"Pagination terminée. Total: {len(all_data)} lignes")
                    return all_data
                
                all_data.extend(data)
                logger.info(f"{len(data)} lignes (Cumulé: {len(all_data)})")

                if len(data) < limit:
                    logger.info(f"Dernière page. Total: {len(all_data)} lignes")
                    return all_data
                
                offset += limit
                page += 1
                time.sleep(0.5)
                break #Sort de la boucle si tout est ok

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout (tentative {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    raise Exception(f"Timeout après {max_retries} tentatives")
                time.sleep(2 ** attempt)  # Backoff exponentiel
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur API: {e}")
                if attempt == max_retries - 1:
                    raise Exception(f"Erreur API après {max_retries} tentatives: {e}")
                time.sleep(2 ** attempt)
        return all_data

def build_datetime_range(execution_date: datetime) -> tuple:
    start_time = execution_date.replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    logger.info(f"Plage temporelle: {start_str} -> {end_str}")
    return start_str, end_str

def build_gcs_path(execution_date: datetime, source_name: str) -> str:
    year = execution_date.year
    month = f"{execution_date.month:02d}"
    day = f"{execution_date.day:02d}"
    hour = f"{execution_date.hour:02d}"
    minute = f"{execution_date.minute:02d}"

    gcs_path = (
        f"raw/{source_name}/"
        f"year={year}/month={month}/day={day}/"
        f"{source_name}_{year}{month}{day}_{hour}{minute}.json"
    )
    
    logger.info(f"Chemin GCS: {gcs_path}")
    return gcs_path


def extract_traffic_speed(ti, **kwargs):
    logger.info("DÉBUT EXTRACTION TRAFFIC SPEED")
    
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    config = API_CONFIG["traffic_speed"]
    
    start_datetime, end_datetime = build_datetime_range(execution_date)
    
    # Informations de debug
    logger.info(f"Configuration:")
    logger.info(f"Source: {COLLECTED_NAME}")
    logger.info(f"Base URL: {config['base_url']}")
    logger.info(f"Date field: {config['date_field']}")
    logger.info(f"Limit: {config['limit']}")
    logger.info(f"Execution date: {execution_date}")
    
    # Extraction avec pagination
    try:
        all_data = extract_data_with_pagination(
            base_url=config['base_url'],
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            date_field=config['date_field'],
            limit=config['limit']
        )
        
        # Validation
        if not all_data:
            raise ValueError(
                f"Aucune donnée extraite entre les dates suivantes {start_datetime} -> {end_datetime}"
            )
        
        nb_records = len(all_data)
        logger.info(f"Extraction réussie: {nb_records} enregistrements")
        
        # Stocker dans XCom pour la tâche suivante
        ti.xcom_push(key='traffic_data', value=all_data)
        ti.xcom_push(key='nb_records', value=nb_records)
        ti.xcom_push(key='start_datetime', value=start_datetime)
        ti.xcom_push(key='end_datetime', value=end_datetime)
                
        return nb_records
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction: {e}")
        raise

def upload_to_gcs(ti, **kwargs):
    logger.info("DÉBUT UPLOAD VERS GCS")
    
    logger.info(kwargs)
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')
    # Récupérer les données depuis XCom
    data = ti.xcom_pull(task_ids='extract_data', key='traffic_data')
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    start_datetime = ti.xcom_pull(task_ids='extract_data', key='start_datetime')
    end_datetime = ti.xcom_pull(task_ids='extract_data', key='end_datetime')
    
    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {start_datetime} -> {end_datetime}")
    
    # Construire le chemin GCS
    gcs_path = build_gcs_path(execution_date, COLLECTED_NAME)
    
    try:
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Convertir en JSON
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        file_size_mb = len(json_data) / (1024 * 1024)
        
        logger.info(f"Taille: {file_size_mb:.2f} MB")
        logger.info(f"Destination: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        # Upload
        blob.upload_from_string(
            json_data,
            content_type='application/json'
        )
        
        logger.info(f"Upload réussi!")
        logger.info(f"gs://{GCS_BUCKET_NAME}/{gcs_path}")
        logger.info(f"{nb_records} enregistrements")
        logger.info(f"{file_size_mb:.2f} MB")

        
        # Retourner le chemin pour traçabilité
        return f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
        
    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")
        raise



# DÉFINITION DU DAG

default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG (
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Ce Dags est présent pour collecter les données horaires provenant de la plateforme https://data.cityofnewyork.us."
        "Il s'agit Vitesse du trafic en temps réel par segment de rue. Les données seront récupérés toutes les heures."
    ),
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31, 23, 59),
    schedule='5 * * * *', # Toutes les heures + 5 minutes
    catchup=True,
    max_active_runs=1, # Nombre de worker
    tags=get_collected_tags(COLLECTED_NAME, "hour"),
) as dag:
    debut = EmptyOperator(task_id="debut")
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_traffic_speed,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )

    fin = EmptyOperator(task_id="fin")

    debut >> extract_task >> upload_task >> fin