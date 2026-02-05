from datetime import timedelta
import os

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCP_CREDENTIALS_PATH = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/opt/airflow/config/gcp/airflow-gcp-key.json'
)
DATASETS_GBQ = {
    "staging":"nyc_data" # nom du dataset est raw 
}

CONNECTION_BQ_AIRFLOW = os.getenv('CONNECTION_BQ_AIRFLOW','gcp-bigquery')

PROJECT_NAME = "nyc-transport-pipeline"

API_CONFIG = {
    'traffic_speed': {
        'base_url': 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json',
        'limit': 50000,
        'order_by': 'data_as_of ASC',
        'date_field': 'data_as_of',
    },
    '311_requests': {
        'base_url': 'https://data.cityofnewyork.us/resource/erm2-nwe9.json',
        'limit': 50000,
        'order_by': 'created_date ASC',
        'date_field': 'created_date',
    },
    'traffic_volume': {
        'base_url': 'https://data.cityofnewyork.us/resource/btm5-ppia.csv',
        'limit': 50000,
        'date_field': 'yr',  # Utilise yr, m, d séparément
    },
    'weather': {
        'base_url': 'https://www.ncei.noaa.gov/access/services/data/v1',
        'params': {
            'dataset': 'daily-summaries',
            'dataTypes': 'PRCP,TMAX,TMIN,SNOW',
            'stations': 'USW00094728',
            'format': 'json',
            'units': 'metric',
        },
    }, 
    'yellow_axi_vehicule_trips':{
        'base_url':"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata",
        'format':"parquet"
    },
    'for_hire_vehicule_trips':{
        'base_url':" https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata",
        'format':"parquet"
    },
    'high_volume_vehicule_trips':{
        'base_url':"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata",
        'format':"parquet"
    }
}

DAG_DEFAULT_ARGS = {
    'owner': 'ahmad',
    'depends_on_past': False,
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

ASSET_PATH = {
    'traffic_speed': f"gcs://{GCS_BUCKET_NAME}/raw/NYC_Real_Time_Traffic_Speed/",
    '311_requests': f"gcs://{GCS_BUCKET_NAME}/raw/NYC_311_Service_Requests/",
    'traffic_volume': f"gcs://{GCS_BUCKET_NAME}/raw/comptages_vehicules_intersection/",
    'weather': f"gcs://{GCS_BUCKET_NAME}/raw/NOAA_Weather_Data/",
    'yellow_taxi': f"gcs://{GCS_BUCKET_NAME}/raw/yellow_axi_vehicule_trips/",
    'for_hire_vehicule': f"gcs://{GCS_BUCKET_NAME}/raw/for_hire_vehicule_trips/",
    'hvfhv': f"gcs://{GCS_BUCKET_NAME}/raw/high_volume_vehicule_trips/",
}
