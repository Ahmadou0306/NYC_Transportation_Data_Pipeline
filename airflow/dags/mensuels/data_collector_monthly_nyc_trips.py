from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from google.cloud import storage
from airflow import DAG, Asset
import requests
import base64
import time
from io import StringIO

import logging
logger = logging.getLogger(__name__)

from config.api_config import API_CONFIG, PROJECT_NAME, GCS_BUCKET_NAME, DAG_DEFAULT_ARGS, ASSET_PATH
from utils.utilitaire import get_collected_tags, fetch_data_from_url, build_gcs_path

COLLECTED_NAME = "monthly_nyc_trips"
API_LABEL = ["yellow_axi_vehicule_trips","for_hire_vehicule_trips","high_volume_vehicule_trips"]

gcs_yellow_axi_vehicule_trips = Asset(ASSET_PATH["yellow_taxi"])
gcs_for_hire_vehicule_trips = Asset(ASSET_PATH["for_hire_vehicule"])
gcs_high_volume_vehicule_trips = Asset(ASSET_PATH["hvfhv"])



def extract_data(ti,api_label,**kwargs):

    logger.info(f"DEBUT EXTRACTION {api_label}")

    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')

    config = API_CONFIG[api_label]
    base_url = config['base_url']
    format_response = config['format']
    date=execution_date.strftime('%Y-%m')

    # Informations de debug
    logger.info(f"Configuration:")
    logger.info(f"Source: {COLLECTED_NAME}")
    logger.info(f"URL: {base_url}")
    logger.info(f"Format réponse: {format_response}")
    logger.info(f"Execution date: {date}")

    max_retries=3 #Nombre de tentative
    parquet_data = None

    for attempt in range(max_retries):
        url = (
            f"{base_url}_{date}.{format_response}"
        )
        try:
            results = fetch_data_from_url(url, timeout=120)
            logger.info(f"format de la réponse {results['format']}")

            if results['format'] != 'parquet':
                raise Exception(f"Format incorrecte, nous attendons un fichier parquet") 
            
            parquet_data = results['data']  # Bytes bruts
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


    logger.info(f"longueur des données: {len(parquet_data)}")
        
    # Validation
    if not parquet_data:
        raise ValueError(
            f"Aucune donnée extraite à la date du {date}"
        )
        
    nb_records = len(parquet_data)

    logger.info(f"Extraction réussie: {nb_records} enregistrements")
        
    # Pour le transporter via x_com sur une autre tasks, nous devons l'encoder en base 64
    parquet_base64 = base64.b64encode(parquet_data).decode('utf-8')
    ti.xcom_push(key=api_label, value=parquet_base64)
    ti.xcom_push(key='nb_records', value=nb_records)
    ti.xcom_push(key='date', value=date)
                
    return nb_records



def upload_to_gcs(ti,api_label, ti_extract ,**kwargs):
    logger.info("DEBUT UPLOAD VERS GCS")
    
    logger.info(kwargs)

    # Récupérer les données depuis XCom - base64
    parquet_base64 = ti.xcom_pull(task_ids=ti_extract, key=api_label)
    parquet_data = base64.b64decode(parquet_base64)

    nb_records = ti.xcom_pull(task_ids=ti_extract, key='nb_records')
    date_str = ti.xcom_pull(task_ids=ti_extract, key='date')

    periode = datetime.strptime(date_str,'%Y-%m')

    logger.info(f"Données récupérées depuis XCom:")
    logger.info(f"Enregistrements: {nb_records}")
    logger.info(f"Période: {periode}")
    
    
    try:
        # construction du chemin GCS
        gcs_path = build_gcs_path(
            periode, 
            f"{api_label}",
            'monthly',
            'parquet'
        )
        
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)

        file_size_mb = len(parquet_data) / (1024 * 1024)
        
        logger.info(f"Taille: {file_size_mb:.2f} MB")
        logger.info(f"Destination: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        # Upload
        blob.upload_from_string(
            parquet_data,
            content_type='application/octet-stream'
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





default_args = DAG_DEFAULT_ARGS
default_args["retry_delay"] = timedelta(minutes=5)
default_args["execution_timeout"] = timedelta(hours=2) # A cause de l'encodage en Base64 sur des fichiers triplant presque leur taille en mémoire


with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Collecte mensuelles des informations de véhicules à new Yok."
        "Courses de taxis jaunes, récupérées au format parquet et stockées dans GCS avec partitioning par date."
        " Courses de véhicules de location, récupérées au format parquet et stockées dans GCS avec partitioning par date."
        "Courses haute fréquence (Uber, Lyft), récupérées au format parquet et stockées dans GCS avec partitioning par date."

        f"Les données de Courses de taxis jaunes sont présentes ici: {API_CONFIG[API_LABEL[0]]['base_url']}"
        f"Les données de véhicules de location sont présentes ici: {API_CONFIG[API_LABEL[1]]['base_url']}"
        f"Les données de Courses haute fréquence sont présentes ici: {API_CONFIG[API_LABEL[2]]['base_url']}"
    ),
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2025, 1, 31),
    schedule='0 22 1 * *', # Tous les 01 du mois à 22h
    catchup=True,
    max_active_runs=1, # Nombre de worker

    tags=get_collected_tags(COLLECTED_NAME, "days"),
) as dag:
    debut = EmptyOperator(task_id="debut")
    
    extract_task_yellow_taxi = PythonOperator(
        task_id='extract_data_yellow_taxi',
        python_callable=extract_data,
        op_kwargs={
            'api_label': API_LABEL[0]
        }
    )
    extract_task_for_hire_vehicule = PythonOperator(
        task_id='extract_data_for_hire_vehicule',
        python_callable=extract_data,
        op_kwargs={
            'api_label': API_LABEL[1]
        }
    )
    extract_task_high_volume_vehicule = PythonOperator(
        task_id='extract_data_high_volume_vehicule',
        python_callable=extract_data,
        op_kwargs={
            'api_label': API_LABEL[2],
        }
    )

    upload_task_yellow_taxi = PythonOperator(
        task_id='upload_to_gcs_yellow_taxi',
        python_callable=upload_to_gcs,
        op_kwargs={
            'api_label': API_LABEL[0],
            'ti_extract':'extract_data_yellow_taxi'
        },
        trigger_rule="all_success",
        outlets=[gcs_yellow_axi_vehicule_trips]
    )
    upload_task_for_hire_vehicule = PythonOperator(
        task_id='upload_to_gcs_for_hire_vehicule',
        python_callable=upload_to_gcs,
        op_kwargs={
            'api_label': API_LABEL[1],
            'ti_extract':'extract_data_for_hire_vehicule'
        },
        trigger_rule="all_success",
        outlets=[gcs_for_hire_vehicule_trips]
    )


    upload_task_high_volume_vehicule = PythonOperator(
        task_id='upload_to_gcs_high_volume_vehicule',
        python_callable=upload_to_gcs,
        op_kwargs={
            'api_label': API_LABEL[2],
            'ti_extract':'extract_data_high_volume_vehicule'
        },
        trigger_rule="all_success",
        outlets=[gcs_high_volume_vehicule_trips]
    )

    fin = EmptyOperator(task_id="fin", trigger_rule="all_success")

    debut >> [extract_task_yellow_taxi,extract_task_for_hire_vehicule, extract_task_high_volume_vehicule]
    
    extract_task_yellow_taxi >> upload_task_yellow_taxi
    extract_task_for_hire_vehicule >> upload_task_for_hire_vehicule
    extract_task_high_volume_vehicule >> upload_task_high_volume_vehicule

    [upload_task_yellow_taxi,upload_task_for_hire_vehicule,upload_task_high_volume_vehicule] >> fin




