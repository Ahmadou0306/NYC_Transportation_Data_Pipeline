{{
  config(
    materialized='table',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["service_type", "pickup_borough", "is_stuck_in_traffic"]
  )
}}

WITH trips AS (
    SELECT *
    FROM {{ ref('int_trips_enriched') }}
),

traffic AS (
    SELECT *
    FROM {{ ref('int_traffic_hourly_avg') }}
),

-- Table de mapping zones → boroughs
zone_lookup AS (
    SELECT
        LocationID,
        Borough
    FROM {{ ref('taxi_zone_lookup') }}  -- https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
),

-- Enrichir trips avec le borough
trips_with_borough AS (
    SELECT
        t.*,
        z.Borough AS pickup_borough
    FROM trips t
    LEFT JOIN zone_lookup z
        ON t.pickup_location_id = z.LocationID
),

-- Joindre avec le trafic
trips_with_traffic AS (
    SELECT
        -- Colonnes du trip
        t.service_type,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.pickup_borough,
        t.trip_duration_minutes,
        t.trip_distance_miles,
        t.distance_category,
        t.is_snowy,
        t.is_rainy,
        t.is_rush_hour,
        
        -- Métriques de trafic au moment du pickup
        traf.avg_speed_mph AS traffic_avg_speed_mph,
        traf.is_congested AS traffic_is_congested,
        traf.is_severe_congestion AS traffic_is_severe_congestion,
        traf.pct_congested AS traffic_pct_congested,
        
        -- Calculs basés sur trafic
        CASE 
            WHEN t.trip_distance_miles IS NOT NULL 
                 AND traf.avg_speed_mph IS NOT NULL
                 AND traf.avg_speed_mph > 0
            THEN ROUND((t.trip_distance_miles / traf.avg_speed_mph) * 60, 1)  -- minutes
            ELSE NULL
        END AS estimated_duration_minutes,
        
        -- Écart durée réelle vs estimée
        CASE 
            WHEN t.trip_distance_miles IS NOT NULL 
                 AND traf.avg_speed_mph IS NOT NULL
                 AND traf.avg_speed_mph > 0
            THEN ROUND(
                t.trip_duration_minutes - ((t.trip_distance_miles / traf.avg_speed_mph) * 60),
                1
            )
            ELSE NULL
        END AS duration_diff_minutes,
        
        -- Flag : Bloqué dans le trafic (durée > 150% de l'estimé)
        CASE 
            WHEN t.trip_distance_miles IS NOT NULL 
                 AND traf.avg_speed_mph IS NOT NULL
                 AND traf.avg_speed_mph > 0
                 AND t.trip_duration_minutes > (((t.trip_distance_miles / traf.avg_speed_mph) * 60) * 1.5)
            THEN TRUE
            ELSE FALSE
        END AS is_stuck_in_traffic,
        
        -- Métadonnées
        t.loaded_at,
        CURRENT_TIMESTAMP() AS enriched_at
        
    FROM trips_with_borough t
    LEFT JOIN traffic traf
        ON t.pickup_borough = traf.borough
        AND DATE_TRUNC(t.pickup_datetime, HOUR) = traf.measurement_hour
)

SELECT * FROM trips_with_traffic