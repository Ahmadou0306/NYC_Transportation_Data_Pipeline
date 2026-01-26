import requests
from typing import Dict, Any
import json
from datetime import datetime

PROJECT_DATA_COLLECTOR_NAME = "NYC_TRANSPORTATION_DATA_COLLECT"


def get_collected_tags(collected_name:str, frequency:str="days"):
    
    all_collected_tags = {
        "hour": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:hourly",
            "schedule:every-hour",
            "offset-1",
            "retention:7d",
            "priority:normal"
        ],
        "days": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:daily",
            "schedule:daily",
            "offset-1",
            "retention:90d",
            "priority:high"
        ],
        "weekly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:weekly",
            "schedule:weekly",
            "offset-1",
            "retention:1y",
            "priority:low"
        ],
        "monthly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:monthly",
            "schedule:monthly",
            "offset-1",
            "retention:3y",
            "priority:high"
        ],
        "yearly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:yearly",
            "schedule:yearly",
            "offset-1",
            "retention:10y",
            "priority:low"
        ],
    }

    if frequency not in all_collected_tags.keys():
        raise ValueError(
                    f"Fréquence '{frequency}' invalide. "
                    f"Valeurs acceptées: {list(all_collected_tags.keys())}"
                )    
    return all_collected_tags[frequency]+[collected_name]



def fetch_data_from_url(url:str,timeout: int = 30)-> Dict[str, Any]:
    try:
        # Effectuer la requête GET
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        
        # Récupérer les informations de base
        content_type = response.headers.get('Content-Type', '').lower()
        encoding = response.encoding or 'utf-8'
        
        # Déterminer le format et parser si possible
        result = {
            'status_code': response.status_code,
            'content_type': content_type,
            'encoding': encoding,
            'headers': dict(response.headers),
            'url': response.url,
            'data': None,
            'format': 'unknown',
            'raw_content': response.content
        }
        
        # Détecter et parser selon le format
        if 'application/json' in content_type or url.endswith('.json'):
            try:
                result['data'] = response.json()
                result['format'] = 'json'
            except json.JSONDecodeError:
                result['data'] = response.text
                result['format'] = 'text'
        
        elif 'text/csv' in content_type or url.endswith('.csv'):
            result['data'] = response.text
            result['format'] = 'csv'

        elif 'application/octet-stream' in content_type or url.endswith('.parquet'):
            result['data'] = response.content  # Données binaires brutes
            result['format'] = 'parquet'
        
        elif 'application/xml' in content_type or 'text/xml' in content_type or url.endswith('.xml'):
            result['data'] = response.text
            result['format'] = 'xml'
        
        elif 'text/html' in content_type:
            result['data'] = response.text
            result['format'] = 'html'
        
        elif 'text/' in content_type:
            result['data'] = response.text
            result['format'] = 'text'
        
        else:
            # Format binaire (images, PDFs, etc.)
            result['data'] = response.content
            result['format'] = 'binary'
        return result
        
    except requests.exceptions.Timeout:
        raise requests.RequestException(f"Timeout: La requête a dépassé {timeout} secondes")
    except requests.exceptions.RequestException as e:
        raise requests.RequestException(f"Erreur lors de la requête: {str(e)}")



def build_gcs_path(date: datetime, source_name: str, frequency: str, extension: str) -> str:

    VALID_FREQUENCIES = {"hourly", "daily", "weekly", "monthly", "yearly"}
    VALID_EXTENSIONS = {"json", "csv", "parquet"}
    
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(f"Fréquence '{frequency}' invalide. Valeurs acceptées: {VALID_FREQUENCIES}")
    
    if extension not in VALID_EXTENSIONS:
        raise ValueError(f"Extension '{extension}' invalide. Valeurs acceptées: {VALID_EXTENSIONS}")
    
    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    
    # Mapping fréquence -> (partition_path, filename)
    paths = {
        "yearly": (
            f"raw/{source_name}/year={year}",
            f"{source_name}_{year}.{extension}"
        ),
        "monthly": (
            f"raw/{source_name}/year={year}",
            f"{source_name}_{year}{month}.{extension}"
        ),
        "weekly": (
            f"raw/{source_name}/year={year}/month={month}",
            f"{source_name}_{year}_W{date.isocalendar()[1]:02d}.{extension}"
        ),
        "daily": (
            f"raw/{source_name}/year={year}/month={month}",
            f"{source_name}_{year}{month}{day}.{extension}"
        ),
        "hourly": (
            f"raw/{source_name}/year={year}/month={month}/days={day}",
            f"{source_name}_{year}{month}{day}_{date.hour:02d}{date.minute:02d}.{extension}"
        )
    }
    
    partition_path, filename = paths[frequency]
    return f"{partition_path}/{filename}"