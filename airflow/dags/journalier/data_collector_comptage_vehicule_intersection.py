from config.api_config import API_CONFIG, PROJECT_NAME, GCS_BUCKET_NAME
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from utils.utilitaire import get_collected_tags
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG
import requests
import csv
import time


import logging
logger = logging.getLogger(__name__)


COLLECTED_NAME = "Comptages_véhicules_intersection"
API_LABEL = "traffic_volume"



def all_extract_data(
    base_url: str,
    date:dict,
    limit: int = 10000,
    max_retries: int = 3
) -> list:
    all_data = []
    offset = 0
    page = 1
    yr = date.get('yr')
    m = date.get('m')
    d = date.get('d')
    
    url = f"{base_url}?$limit={limit}&$where=date='{yr:04d}-{m:02d}-{d:02d}'"
    

    logger.info(f"url: {url}")
    logger.info(f"Page {page} - Offset {offset}")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            csv_text = response.text.strip()

            if not csv_text:
                logger.info(f"Pagination terminée")
                break
            
            lines = csv_text.split('\n')

            # Page 1 : garder le header
            if page == 1:
                header = lines[0]
                all_data.append(header)
                data_lines = lines[1:]  # Toutes les lignes sauf header
            else:
                # Pages suivantes : ignorer le header
                data_lines = lines[1:] if len(lines) > 1 else []
            
            logger.info(f"dataline {data_lines}")
            # Si aucune donnée (juste header ou vide)
            if not data_lines or data_lines[0]:
                logger.info(f"Aucune nouvelle donnée à la page {page}")
                break

            # Ajouter les lignes de données
            all_data.extend(data_lines)
            logger.info(f"{len(data_lines)} lignes (Cumulé: {len(all_data)})")
            
            # Si moins que limit, c'est la dernière page
            if len(data_lines) < limit:
                logger.info(f"Dernière page. Total: {len(all_data)} lignes")
                break

            # Page suivante
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
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    logger.info(f"Date: {year}-{month:02d}-{day:02d}")
    
    return {
        'yr': year,
        'm': month,
        'd': day
    }

def build_gcs_path(execution_date: datetime, source_name: str) -> str:
    year = execution_date.year
    month = f"{execution_date.month:02d}"
    day = f"{execution_date.day:02d}"

    gcs_path = (
        f"raw/{source_name}/"
        f"year={year}/month={month}/day={day}/"
        f"{source_name}_{year}{month}{day}.csv"
    )
    
    logger.info(f"Chemin GCS: {gcs_path}")
    return gcs_path


def extract_data(ti,**kwargs):
    logger.info("DÉBUT EXTRACTION TRAFFIC SPEED")

    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    config = API_CONFIG[API_LABEL]


    date=build_datetime_range(execution_date)

    # Informations de debug
    logger.info(f"Configuration:")
    logger.info(f"Source: {COLLECTED_NAME}")
    logger.info(f"Base URL: {config['base_url']}")
    logger.info(f"Limit: {config['limit']}")
    logger.info(f"Execution date: {execution_date}")

    # Extraction avec pagination
    try:
        all_data = all_extract_data(
            base_url=config['base_url'],
            date=date,
            limit=config['limit']
        )

        logger.info(f"longueur des données: {len(all_data)}")
        logger.info(f"data: {all_data}")
        

        # Validation
        if not all_data:
            raise ValueError(
                f"Aucune donnée extraite à la date du {date}"
            )
        
        nb_records = all_data.count('\n') - 1

        logger.info(f"Extraction réussie: {nb_records} enregistrements")
        
        # Stocker dans XCom pour la tâche suivante
        ti.xcom_push(key='traffic_volume', value=all_data)
        ti.xcom_push(key='nb_records', value=nb_records)
        ti.xcom_push(key='date', value=date)
                
        return nb_records
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction: {e}")
        raise

def upload_to_gcs(ti, **kwargs):
    logger.info("DÉBUT UPLOAD VERS GCS")
    
    logger.info(kwargs)
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    # Récupérer les données depuis XCom
    data = ti.xcom_pull(task_ids='extract_data', key='traffic_volume')
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    date = ti.xcom_pull(task_ids='extract_data', key='date')
    
    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {date.get('yr')}-{date.get('m')}-{date.get('d')}")
    
    # Construire le chemin GCS
    gcs_path = build_gcs_path(execution_date, COLLECTED_NAME)
    
    try:
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        file_size_mb = len(data) / (1024 * 1024)
        
        logger.info(f"Taille: {file_size_mb:.2f} MB")
        logger.info(f"Destination: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        # Upload
        blob.upload_from_string(
            data,
            content_type='text/csv'
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

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Collecte quotidienne des comptages de véhicules aux intersections de NYC. "
        "Les données sont récupérées au format CSV et stockées dans GCS avec partitioning par date."
        f"Les données sont présentes ici: {API_CONFIG[API_LABEL]}"
    ),
    start_date=datetime(2013, 1, 1),
    end_date=datetime(2024, 1, 31),
    schedule='5 0 * * *', # Toutes les Jours à 00:05
    catchup=True,
    max_active_runs=1, # Nombre de worker
    tags=get_collected_tags(COLLECTED_NAME, "days"),
) as dag:
    debut = EmptyOperator(task_id="debut")
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )

    fin = EmptyOperator(task_id="fin")

    debut >> extract_task >> upload_task >> fin