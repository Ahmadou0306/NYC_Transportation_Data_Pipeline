from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG, Asset
import requests
import csv
import time
from io import StringIO, BytesIO
from google.cloud import bigquery


import logging
logger = logging.getLogger(__name__)

from config.api_config import ASSET_PATH, PROJECT_NAME, GCS_BUCKET_NAME, API_CONFIG, GCP_PROJECT_ID,CONNECTION_BQ_AIRFLOW
from utils.utilitaire import get_collected_tags, build_gcs_path, fetch_data_from_url, convert_to_ndjson

COLLECTED_NAME = "comptages_vehicules_intersection"
API_LABEL = "traffic_volume"

gcs_traffic_volume = Asset(ASSET_PATH["traffic_volume"])


def build_datetime_range(execution_date: datetime) -> dict:
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    logger.info(f"Date: {year}-{month:02d}-{day:02d}")
    
    return {
        'yr': year,
        'm': month,
        'd': day
    }


def extract_data(ti,**kwargs):
    logger.info(f"DEBUT EXTRACTION {API_LABEL}")

    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    config = API_CONFIG[API_LABEL]

    base_url = config['base_url']
    limit = config['limit']
    date=build_datetime_range(execution_date)
    yr = date.get('yr')
    m = date.get('m')
    d = date.get('d')

    # Informations de debug
    logger.info(f"Configuration:")
    logger.info(f"Source: {COLLECTED_NAME}")
    logger.info(f"URL: {base_url}")
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
                f"{base_url}?"
                f"$limit={limit}"
                f"&$offset={offset}"
                f"&$where=date='{yr:04d}-{m:02d}-{d:02d}'"
            )
            try:
                results = fetch_data_from_url(url)
                logger.info(f"format de la réponse {results['format']}")

                if results['format'] != 'csv':
                    raise Exception(f"Format incorrecte, nous attendons un fichier csv.") 
                
                reader = csv.reader(StringIO(results['data']))
                data = list(reader)


                header = data[0]
                content = data[1:]
                nb_lignes = len(content)

                if page == 1 : # #si premiere page, on récupére le header des données
                    logger.info(f"Premiere page, longueur du header {len(header)}")
                    all_data.append(header)

                all_data.extend(content)
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
            f"Aucune donnée extraite à la date du {date}"
        )
        
    nb_records = len(all_data[1:])

    logger.info(f"Extraction réussie: {nb_records} enregistrements")
        
    # Stocker dans XCom pour la tâche suivante
    ti.xcom_push(key=API_LABEL, value=all_data)
    ti.xcom_push(key='nb_records', value=nb_records)
    ti.xcom_push(key='date', value=date)
                
    return nb_records



def upload_to_gcs(ti, **kwargs):
    logger.info("DEBUT UPLOAD VERS GCS")
    
    logger.info(kwargs)

    # Récupérer les données depuis XCom
    all_data = ti.xcom_pull(task_ids='extract_data', key=API_LABEL)
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    date_dict = ti.xcom_pull(task_ids='extract_data', key='date')
    periode = f"{date_dict.get('yr'):04d}-{date_dict.get('m'):02d}-{date_dict.get('d'):02d}"

    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {periode}")
    
    # construction du chemin GCS
    gcs_path = build_gcs_path(
        datetime.strptime(periode, '%Y-%m-%d'), 
        COLLECTED_NAME,
        'daily',
        'csv'
    )
    
    try:
        # reconversion du csv en string
        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(all_data)
        csv_string = output.getvalue()
        
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)

        file_size_mb = len(csv_string.encode('utf-8')) / (1024 * 1024)
        
        logger.info(f"Taille: {file_size_mb:.2f} MB")
        logger.info(f"Destination: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        # Upload
        blob.upload_from_string(
            csv_string,
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


def upload_to_bq(ti, **kwargs):
    logger.info("DEBUT CHARGEMENT BIGQUERY")
    
    # Récupérer les données depuis XCom
    all_data = ti.xcom_pull(task_ids='extract_data', key=API_LABEL)
    nb_records = ti.xcom_pull(task_ids='extract_data', key='nb_records')
    date_dict = ti.xcom_pull(task_ids='extract_data', key='date')
    periode = f"{date_dict.get('yr'):04d}-{date_dict.get('m'):02d}-{date_dict.get('d'):02d}"

    logger.info(all_data)
    
    if not all_data or len(all_data) < 2:  # Au moins header + 1 ligne
        logger.error(f"Aucune données à cette période {periode}")
        return
    
    logger.info(f"{nb_records} enregistrements à charger")
    
    try:
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.nyc_data.raw_traffic_volume"
        
        # Pour CSV : Convertir en string
        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(all_data)  # Header + données
        csv_string = output.getvalue()
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
        )
        
        logger.info(f"Table destination: {table_id}")
        
        from io import BytesIO
        csv_bytes = csv_string.encode('utf-8')
        
        load_job = client.load_table_from_file(
            BytesIO(csv_bytes),
            table_id,
            job_config=job_config
        )
        
        load_job.result()  # Attendre la fin
        
        logger.info(f"Chargement BigQuery réussi: {load_job.output_rows} lignes")
        
        return table_id
        
    except Exception as e:
        logger.error(f"Erreur chargement BigQuery: {e}")
        raise


# DÉFINITION DU DAG

default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
    'email': ['ahmadou.ndiaye030602@gmail.com'],
#    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
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
        "Collecte quotidienne des comptages de véhicules aux intersections de NYC. "
        "Les données sont récupérées au format CSV et stockées dans GCS avec partitioning par date."
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
        trigger_rule="all_success"
    )
    upload_task_bq = PythonOperator(
        task_id='upload_to_bq',
        python_callable=upload_to_bq,
        trigger_rule="all_success"
    )

    fin = EmptyOperator(task_id="fin")

    debut >> extract_task >> [upload_task_gcs, upload_task_bq] >> fin