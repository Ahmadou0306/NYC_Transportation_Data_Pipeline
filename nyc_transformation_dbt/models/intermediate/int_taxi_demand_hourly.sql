{{
  config(
    materialized='table',
    cluster_by=["pickup_location_id", "day_of_week", "hour_of_day"]
  )
}}

WITH trips AS (
    SELECT *
    FROM {{ ref('int_trips_enriched') }}
    WHERE pickup_datetime IS NOT NULL
      AND pickup_location_id IS NOT NULL
),

-- Agrégation par zone + heure + jour de semaine
demand_pattern AS (
    SELECT
        -- Dimensions spatiales
        pickup_location_id,
        
        -- Dimensions temporelles
        EXTRACT(HOUR FROM pickup_datetime) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
        
        -- Labels temporels
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) IN (1, 7) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS time_slot,
        
        -- Métriques de demande
        COUNT(*) AS total_pickups,
        
        -- Par type de service
        COUNTIF(service_type = 'Yellow Taxi') AS yellow_pickups,
        COUNTIF(service_type = 'HVFHV') AS hvfhv_pickups,
        COUNTIF(service_type = 'FHV') AS fhv_pickups,
        
        -- Caractéristiques des courses
        ROUND(AVG(trip_distance_miles), 2) AS avg_trip_distance,
        ROUND(AVG(trip_duration_minutes), 1) AS avg_trip_duration,
        
        -- Distribution par distance
        COUNTIF(distance_category = 'Intra-quartier') AS short_distance_pickups,
        COUNTIF(distance_category = 'Local') AS medium_distance_pickups,
        COUNTIF(distance_category = 'Cross-borough') AS long_distance_pickups,
        
        -- Conditions
        COUNTIF(is_rainy = TRUE) AS rainy_pickups,
        COUNTIF(is_snowy = TRUE) AS snowy_pickups,
        COUNTIF(is_rush_hour = TRUE) AS rush_hour_pickups,
        
        -- % dans des conditions spéciales
        ROUND(COUNTIF(is_rush_hour = TRUE) * 100.0 / COUNT(*), 2) AS pct_rush_hour,
        ROUND(COUNTIF(is_rainy = TRUE) * 100.0 / COUNT(*), 2) AS pct_rainy,
        
        -- Statistiques temporelles
        MIN(pickup_datetime) AS earliest_pickup,
        MAX(pickup_datetime) AS latest_pickup,
        COUNT(DISTINCT DATE(pickup_datetime)) AS days_observed,
        
        -- Moyenne quotidienne (pour normaliser sur la période d'observation)
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(pickup_datetime)), 1) AS avg_daily_pickups,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS aggregated_at
        
    FROM trips
    GROUP BY 
        pickup_location_id,
        EXTRACT(HOUR FROM pickup_datetime),
        EXTRACT(DAYOFWEEK FROM pickup_datetime),
        is_weekend,
        time_slot
)

SELECT * FROM demand_pattern