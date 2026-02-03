{{
  config(
    materialized='table',
    partition_by={
      "field": "measurement_hour",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["borough", "status_code"]
  )
}}

WITH traffic_speed_enriched AS (
    SELECT
        borough,
        DATE_TRUNC(measurement_timestamp, HOUR) AS measurement_hour,
        EXTRACT(HOUR FROM measurement_hour) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM measurement_hour) AS day_of_week,
        status_code,  -- ← Garder la colonne originale
        
        -- Métriques agrégées
        COUNT(*) AS total_measurements,
        ROUND(AVG(speed_mph), 1) AS avg_speed_mph,
        ROUND(MIN(speed_mph), 1) AS min_speed_mph,
        ROUND(MAX(speed_mph), 1) AS max_speed_mph,
        
        -- Congestion
        COUNTIF(speed_mph < 10) AS severe_congestion_count,
        COUNTIF(speed_mph BETWEEN 10 AND 25) AS moderate_congestion_count,
        
        -- Flags
        CASE 
            WHEN AVG(speed_mph) < 10 THEN TRUE 
            ELSE FALSE 
        END AS is_severe_congestion,
        
        CASE 
            WHEN AVG(speed_mph) < 15 THEN TRUE 
            ELSE FALSE 
        END AS is_congested,
        
        -- Label de statut
        CASE 
            WHEN status_code = 0 THEN 'Données Fiables'
            WHEN status_code = 1 THEN 'Données imprécises'
            WHEN status_code = 2 THEN 'Problème capteur'
            WHEN status_code = 3 THEN 'Maintenance'
            ELSE 'Inconnu'
        END AS status_label
        
    FROM {{ ref('stg_traffic_speed') }}
    WHERE measurement_timestamp IS NOT NULL
      AND speed_mph IS NOT NULL
      AND speed_mph > 0
    GROUP BY 
        borough,
        DATE_TRUNC(measurement_timestamp, HOUR),
        status_code
)

SELECT * FROM traffic_speed_enriched