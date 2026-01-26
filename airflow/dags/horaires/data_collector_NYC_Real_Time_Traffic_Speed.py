from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG
import requests
import json
import time


import logging
logger = logging.getLogger(__name__)

from config.api_config import API_CONFIG, PROJECT_NAME, GCS_BUCKET_NAME
from utils.utilitaire import get_collected_tags, fetch_data_from_url, build_gcs_path


COLLECTED_NAME = "NYC_Real_Time_Traffic_Speed"
API_LABEL = "traffic_speed"



def build_datetime_range(execution_date: datetime) -> tuple:
    start_time = execution_date.replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    logger.info(f"Plage temporelle: {start_str} -> {end_str}")
    return start_str, end_str


def extract_traffic_speed(ti, **kwargs):
    logger.info("DEBUT EXTRACTION TRAFFIC SPEED")
    
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    config = API_CONFIG[API_LABEL]
    base_url = config['base_url']
    limit = config['limit']
    date_field = config['date_field']
    
    start_datetime, end_datetime = build_datetime_range(execution_date)
    
    # Informations de debug
    logger.info(f"Configuration:")
    logger.info(f"Source: {COLLECTED_NAME}")
    logger.info(f"Base URL: {base_url}")
    logger.info(f"Date field: {date_field}")
    logger.info(f"Limit: {limit}")
    logger.info(f"Execution date: {execution_date}")


    all_data = []
    max_retries=3 #Nombre de tentative
    page=1 # Init page
    offset=0
    while True:
        nb_lignes = 0
        for attempt in range(max_retries):
            url = (
                f"{base_url}"
                f"?$limit={limit}"
                f"&$offset={offset}"
                f"&$where={date_field} between '{start_datetime}' and '{end_datetime}'"
                f"&$order={date_field} ASC"
            )
            logger.info(f"Page {page} - Offset {offset}")
            try:
                results = fetch_data_from_url(url)
                logger.info(f"format de la réponse {results['format']}")

                if results['format'] != 'json':
                    raise Exception(f"Format incorrecte, nous attendons un fichier json.") 
                
                data = results['data'] # Les données sont déja convertis en json
                if not data or len(data) == 0:
                    raise Exception(f"Aucune donnée") 
                
                all_data.extend(data)
                nb_lignes = len(data)
                logger.info(f"{len(data)} lignes (Cumulé: {len(all_data)})")

                offset += limit
                page +=1
                break
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

        if nb_lignes < limit:
            logger.info(f"Fin de pagination: dernière page avec {nb_lignes} lignes")
            break  

    logger.info(f"longueur des données: {len(all_data)}")

    # Validation
    if not all_data:
        raise ValueError(
            f"Aucune donnée extraite à la date du {start_datetime}"
        )
        
    nb_records = len(all_data)
    logger.info(f"Extraction réussie: {nb_records} enregistrements")
    
    # Stocker dans XCom pour la tâche suivante
    ti.xcom_push(key=API_LABEL, value=all_data)
    ti.xcom_push(key='nb_records', value=nb_records)
    ti.xcom_push(key='start_datetime', value=start_datetime)
    ti.xcom_push(key='end_datetime', value=end_datetime)
            
    return nb_records


def upload_to_gcs(ti, **kwargs):
    logger.info("DEBUT UPLOAD VERS GCS")
    
    logger.debug(kwargs)
    # Récupérer les données depuis XCom
    data = ti.xcom_pull(task_ids='extract_data', key=API_LABEL)
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    start_datetime =  datetime.fromisoformat(ti.xcom_pull(task_ids='extract_data', key='start_datetime'))
    end_datetime = datetime.fromisoformat(ti.xcom_pull(task_ids='extract_data', key='end_datetime'))
    
    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {start_datetime} -> {end_datetime}")
    
    # Construire le chemin GCS
    gcs_path = build_gcs_path(
        date=(start_datetime), # Seul les YYYYMMDDHH nous interesse. On aurait pu prendre aussi end_datetime
        source_name=COLLECTED_NAME,
        frequency="hourly",
        extension="json"
    )

    
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
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
#    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# On aurait pu utiliser ici
# from config.api_config import DAG_DEFAULT_ARGS
# default_args = DAG_DEFAULT_ARGS
# default_args["retry_delay"] = timedelta(minutes=5)
# default_args["execution_timeout"] = timedelta(minutes=30)

with DAG (
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Ce Dags est présent pour collecter les données horaires provenant de la plateforme https://data.cityofnewyork.us."
        "Il s'agit Vitesse du trafic en temps réel par segment de rue. Les données seront récupérés toutes les heures."
        f"Les données sont presents ici: {API_CONFIG[API_LABEL]['base_url']}"
    ),
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2025, 1, 31),
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