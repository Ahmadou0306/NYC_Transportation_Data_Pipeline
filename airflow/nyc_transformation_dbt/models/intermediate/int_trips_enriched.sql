-- Partitionnement et clustering
{{
  config(
    materialized='table',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["service_type", "pickup_location_id"]
  )
}}

WITH stg_weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

-- Yellow enrichi
yellow_enriched AS (
    SELECT 
        'Yellow Taxi' AS service_type,
        y.pickup_datetime,
        y.dropoff_datetime,
        y.pickup_location_id,
        y.dropoff_location_id,
        y.loaded_at,
        
        -- Métriques communes
        TIMESTAMP_DIFF(y.dropoff_datetime, y.pickup_datetime, MINUTE) AS trip_duration_minutes,
        y.trip_distance_miles,
        CASE
            WHEN y.trip_distance_miles < 1 THEN 'Intra-quartier'
            WHEN y.trip_distance_miles < 3 THEN 'Local'
            WHEN y.trip_distance_miles < 10 THEN 'Cross-borough'
            ELSE 'Longue distance'
        END AS distance_category,
        
        -- Flags météo
        CASE WHEN w.weather_snow_mm > 0 THEN TRUE ELSE FALSE END AS is_snowy,
        CASE WHEN w.weather_precip_mm > 0 THEN TRUE ELSE FALSE END AS is_rainy,
        
        -- Flag rush hour
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM y.pickup_datetime) BETWEEN 2 AND 6
                 AND (EXTRACT(HOUR FROM y.pickup_datetime) BETWEEN 7 AND 9
                      OR EXTRACT(HOUR FROM y.pickup_datetime) BETWEEN 17 AND 19)
            THEN TRUE 
            ELSE FALSE 
        END AS is_rush_hour
        
    FROM {{ ref('stg_yellow_taxi_trips') }} AS y
    LEFT JOIN stg_weather AS w
        ON w.weather_date = DATE(y.pickup_datetime)
),

-- HVFHV enrichi
hvfhv_enriched AS (
    SELECT 
        h.service_type,  -- Déjà 'HVFHV' dans staging
        h.pickup_datetime,
        h.dropoff_datetime,
        h.pickup_location_id,
        h.dropoff_location_id,
        h.loaded_at,
        
        TIMESTAMP_DIFF(h.dropoff_datetime, h.pickup_datetime, MINUTE) AS trip_duration_minutes,
        h.trip_distance_miles,
        CASE
            WHEN h.trip_distance_miles < 1 THEN 'Intra-quartier'
            WHEN h.trip_distance_miles < 3 THEN 'Local'
            WHEN h.trip_distance_miles < 10 THEN 'Cross-borough'
            ELSE 'Longue distance'
        END AS distance_category,
        
        CASE WHEN w.weather_snow_mm > 0 THEN TRUE ELSE FALSE END AS is_snowy,
        CASE WHEN w.weather_precip_mm > 0 THEN TRUE ELSE FALSE END AS is_rainy,
        
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM h.pickup_datetime) BETWEEN 2 AND 6
                 AND (EXTRACT(HOUR FROM h.pickup_datetime) BETWEEN 7 AND 9
                      OR EXTRACT(HOUR FROM h.pickup_datetime) BETWEEN 17 AND 19)
            THEN TRUE 
            ELSE FALSE 
        END AS is_rush_hour
        
    FROM {{ ref('stg_high_volume_vehicule_trips') }} AS h
    LEFT JOIN stg_weather AS w
        ON w.weather_date = DATE(h.pickup_datetime)
),

-- FHV enrichi
fhv_enriched AS (
    SELECT 
        f.service_type,  -- 'FHV'
        f.pickup_datetime,
        f.dropoff_datetime,
        f.pickup_location_id,
        f.dropoff_location_id,
        f.loaded_at,
        
        TIMESTAMP_DIFF(f.dropoff_datetime, f.pickup_datetime, MINUTE) AS trip_duration_minutes,
        NULL AS trip_distance_miles,  -- FHV n'a pas cette info
        NULL AS distance_category,
        
        CASE WHEN w.weather_snow_mm > 0 THEN TRUE ELSE FALSE END AS is_snowy,
        CASE WHEN w.weather_precip_mm > 0 THEN TRUE ELSE FALSE END AS is_rainy,
        
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM f.pickup_datetime) BETWEEN 2 AND 6
                 AND (EXTRACT(HOUR FROM f.pickup_datetime) BETWEEN 7 AND 9
                      OR EXTRACT(HOUR FROM f.pickup_datetime) BETWEEN 17 AND 19)
            THEN TRUE 
            ELSE FALSE 
        END AS is_rush_hour
        
    FROM {{ ref('stg_for_hire_vehicule_trips') }} AS f
    LEFT JOIN stg_weather AS w
        ON w.weather_date = DATE(f.pickup_datetime)
)

-- UNION des 3 sources
SELECT * FROM yellow_enriched
UNION ALL
SELECT * FROM hvfhv_enriched
UNION ALL
SELECT * FROM fhv_enriched