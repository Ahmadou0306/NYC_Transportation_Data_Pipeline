import os

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCP_CREDENTIALS_PATH = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/opt/airflow/config/gcp/airflow-gcp-key.json'
)


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
}