from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG, Asset
import requests
import json
import time
from io import StringIO
from google.cloud import bigquery


import logging
logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, API_CONFIG, GCP_PROJECT_ID,CONNECTION_BQ_AIRFLOW
from utils.utilitaire import get_collected_tags, build_gcs_path, fetch_data_from_url, convert_to_ndjson


COLLECTED_NAME = "NYC_311_Service_Requests"
API_LABEL = "311_requests"

gcs_311_requets = Asset(ASSET_PATH["311_requests"])



def build_datetime_range(execution_date: datetime) ->tuple:
    start_date = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=1, seconds=-1)

    
    return start_date,end_date


def extract_data(ti, **kwargs):
    logger.info(f"DEBUT EXTRACTION {API_LABEL}")
    
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')
    
    config = API_CONFIG[API_LABEL]
    base_url = config['base_url']
    limit = config['limit']
    
    start_date, end_date = build_datetime_range(execution_date)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    logger.info(f"Période: {start_date_str} -> {end_date_str}")
    logger.info(f"URL: {base_url}")
    logger.info(f"Limit: {limit}")
    
    all_data = []
    max_retries = 3
    page = 1
    offset = 0
    
    while True:
        nb_lignes = 0
        for attempt in range(max_retries):
            # CORRECTION : Utiliser >= et <= au lieu de BETWEEN
            url = (
                f"{base_url}?"
                f"$limit={limit}"
                f"&$offset={offset}"
                f"&$where=created_date>='{start_date_str}' AND created_date<='{end_date_str}'"
                f"&$order=created_date ASC"
            )
            
            logger.info(f"Page {page} - Offset {offset}")
            if page == 1:  # Afficher l'URL complète seulement pour la 1ère page
                logger.info(f"URL: {url}")
            
            try:
                results = fetch_data_from_url(url, timeout=60)
                
                if results['format'] != 'json':
                    raise Exception(f"Format incorrect, attendu: json")
                
                data = results['data']
                
                # Si pas de données, sortir de la boucle (pas une erreur)
                if not data or len(data) == 0:
                    logger.info(f"Aucune donnée page {page} - Fin")
                    nb_lignes = 0
                    break
                
                all_data.extend(data)
                nb_lignes = len(data)
                logger.info(f"{nb_lignes} lignes (Total: {len(all_data)})")
                
                offset += limit
                page += 1
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
        
        # Sortir si aucune donnée
        if nb_lignes == 0:
            logger.info(f"Fin de pagination - Total: {len(all_data)} lignes")
            break
        
        # Sortir si dernière page
        if nb_lignes < limit:
            logger.info(f"Dernière page ({nb_lignes} < {limit}) - Total: {len(all_data)}")
            break
    
    # CORRECTION : nb_records = len(all_data) PAS len(all_data[1:])
    nb_records = len(all_data)
    
    logger.info(f"Total final: {nb_records} enregistrements")
    
    # Accepter 0 résultat (certains jours peuvent être vides)
    if nb_records == 0:
        logger.warning(f"Aucune donnée pour {start_date_str}")
        # Créer un fichier vide pour marquer la date comme traitée
        ti.xcom_push(key=API_LABEL, value=[])
        ti.xcom_push(key='nb_records', value=0)
        ti.xcom_push(key='date', value=start_date_str)
        return 0
    
    # Stocker dans XCom
    ti.xcom_push(key=API_LABEL, value=all_data)
    ti.xcom_push(key='nb_records', value=nb_records)
    ti.xcom_push(key='date', value=start_date_str)
    
    return nb_records


def upload_to_gcs(ti, **kwargs):
    logger.info("DEBUT UPLOAD VERS GCS")
    
    logger.info(kwargs)

    # Récupérons les données depuis XCom
    all_data = ti.xcom_pull(task_ids='extract_data', key=API_LABEL)
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    start_date_str = ti.xcom_pull(task_ids='extract_data', key='date')
    
    periode = datetime.strptime(start_date_str,'%Y-%m-%dT%H:%M:%S')

    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {start_date_str}")
    
    # construction du chemin GCS
    gcs_path = build_gcs_path(
        periode, 
        COLLECTED_NAME,
        'daily',
        'json'
    )
    
    try:
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)

        json_data = json.dumps(all_data, indent=2, ensure_ascii=False)
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

        return f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
        
    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")
        raise

def upload_to_bq(ti, **kwargs):
    logger.info("DEBUT CHARGEMENT BIGQUERY")
    
    # Récupérer les données depuis XCom
    data = ti.xcom_pull(task_ids='extract_data', key=API_LABEL)
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    
    if not data:
        raise ValueError("Aucune donnée à charger dans BigQuery")
    
    logger.info(f"{nb_records} enregistrements à charger")
    
    try:
        # Charger directement dans BigQuery depuis la mémoire
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.staging.raw_311_requests"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
        )
        
        logger.info(f"Table destination: {table_id}")
        
        load_job = client.load_table_from_json(
            data,
            table_id,
            job_config=job_config
        )
        
        load_job.result()  # Attendre la fin
        
        logger.info(f"Chargement BigQuery réussi: {load_job.output_rows} lignes insérées")
        
        return table_id
        
    except Exception as e:
        logger.error(f"Erreur chargement BigQuery: {e}")
        raise


# DÉFINITION DU DAG

default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
#    'start_date': datetime(2020, 1, 1),
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# On aurait pu utiliser ici
# from config.api_config import DAG_DEFAULT_ARGS
# default_args = DAG_DEFAULT_ARGS
# default_args["retry_delay"] = timedelta(minutes=5)
# default_args["execution_timeout"] = timedelta(hours=2)

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Collecte quotidienne des plaintes et demandes de service non-urgentes "
        "Les données sont récupérées au format JSON et stockées dans GCS avec partitioning par date."
        f"Les données sont présentes ici: {API_CONFIG[API_LABEL]['base_url']}"
    ),
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2025, 1, 31),
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

    upload_task_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )
    upload_task_bq = PythonOperator(
        task_id='upload_to_bq',
        python_callable=upload_to_bq,
        trigger_rule="all_success",
    )

    fin = EmptyOperator(task_id="fin")

    debut >> extract_task >> [upload_task_gcs, upload_task_bq] >> fin