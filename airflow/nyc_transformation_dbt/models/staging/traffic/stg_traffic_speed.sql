WITH source AS (
    SELECT *
    FROM {{ source("raw_data", "raw_traffic_speed") }}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit', 1000) }}
    {% endif %}
),

-- Nettoyage et standardisation
cleaned AS (
    SELECT
        -- Identifiants
        CAST(link_id AS STRING) AS link_id,
        CAST(transcom_id AS STRING) AS transcom_id,
        link_name,
        
        -- Localisation
        borough,
        owner,
        
        -- Timestamp de la mesure
        CAST(data_as_of AS TIMESTAMP) AS measurement_timestamp,
        
        -- Métriques de trafic
        CAST(speed AS FLOAT64) AS speed_mph,
        CAST(travel_time AS INT64) AS travel_time_seconds,
        CAST(status AS INT64) AS status_code,
        
        -- Géographie (garder pour analyses avancées, optionnel)
        link_points,
        encoded_poly_line,
        
        -- Traçabilité
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM source
    WHERE 
        -- Filtrage des données invalides
        link_id IS NOT NULL
        AND data_as_of IS NOT NULL
        AND speed IS NOT NULL
        AND travel_time IS NOT NULL
        AND borough IS NOT NULL
        -- Pas de dates futures
        AND CAST(data_as_of AS TIMESTAMP) <= CURRENT_TIMESTAMP()
        -- Vitesse réaliste (NYC speed limit max ~65 mph)
        AND CAST(speed AS FLOAT64) BETWEEN 0 AND 100
        -- Temps de trajet réaliste (pas 0 ni trop élevé)
        AND CAST(travel_time AS INT64) BETWEEN 1 AND 7200  -- Max 2 heures
)

SELECT * FROM cleaned