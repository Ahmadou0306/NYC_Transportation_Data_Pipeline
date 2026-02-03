{{
  config(
    materialized='table',
    partition_by={
      "field": "weather_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["weather_condition"]
  )
}}

WITH weather AS (
    SELECT
        weather_date,
        weather_temp_min_celcius,
        weather_temp_max_celcius,
        weather_precip_mm,
        weather_snow_mm,
        
        -- Créer weather_condition ICI
        CASE
            WHEN weather_snow_mm > 10 THEN 'Heavy Snow'
            WHEN weather_snow_mm > 0 THEN 'Light Snow'
            WHEN weather_precip_mm > 50 THEN 'Heavy Rain'
            WHEN weather_precip_mm > 10 THEN 'Moderate Rain'
            WHEN weather_precip_mm > 0 THEN 'Light Rain'
            ELSE 'Clear'
        END AS weather_condition,
        
        -- Flags météo extrêmes
        CASE WHEN weather_snow_mm > 10 THEN TRUE ELSE FALSE END AS is_extreme_snow,
        CASE WHEN weather_precip_mm > 50 THEN TRUE ELSE FALSE END AS is_extreme_rain,
        CASE 
            WHEN weather_temp_max_celcius > 35 OR weather_temp_min_celcius < -10 
            THEN TRUE 
            ELSE FALSE 
        END AS is_extreme_temperature
        
    FROM {{ ref('stg_weather') }}
),

-- Agrégation quotidienne des courses
daily_trips AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        
        -- Volume de courses
        COUNT(*) AS total_trips,
        COUNT(DISTINCT service_type) AS active_services,
        
        -- Par type de service
        COUNTIF(service_type = 'Yellow Taxi') AS yellow_trips,
        COUNTIF(service_type = 'HVFHV') AS hvfhv_trips,
        COUNTIF(service_type = 'FHV') AS fhv_trips,
        
        -- Métriques de distance (si disponible)
        ROUND(AVG(trip_distance_miles), 2) AS avg_trip_distance,
        ROUND(AVG(trip_duration_minutes), 1) AS avg_trip_duration,
        
        -- Distribution par catégorie
        COUNTIF(distance_category = 'Intra-quartier') AS short_trips,
        COUNTIF(distance_category = 'Local') AS medium_trips,
        COUNTIF(distance_category = 'Cross-borough') AS long_trips,
        COUNTIF(distance_category = 'Longue distance') AS very_long_trips,
        
        -- Heures de pointe
        COUNTIF(is_rush_hour = TRUE) AS rush_hour_trips,
        ROUND(COUNTIF(is_rush_hour = TRUE) * 100.0 / COUNT(*), 2) AS pct_rush_hour
        
    FROM {{ ref('int_trips_enriched') }}
    WHERE pickup_datetime IS NOT NULL
    GROUP BY DATE(pickup_datetime)
),

-- Agrégation quotidienne du trafic
daily_traffic AS (
    SELECT
        DATE(measurement_hour) AS traffic_date,
        
        -- Métriques de vitesse globales
        ROUND(AVG(avg_speed_mph), 1) AS overall_avg_speed_mph,
        ROUND(MIN(avg_speed_mph), 1) AS min_speed_mph,
        ROUND(MAX(avg_speed_mph), 1) AS max_speed_mph,
        
        -- Congestion
        ROUND(
            AVG(CASE WHEN is_congested THEN 100 ELSE 0 END),
            2
        ) AS avg_pct_congested,
        SUM(severe_congestion_count) AS total_severe_congestion_events,
        SUM(moderate_congestion_count) AS total_moderate_congestion_events,
        
        -- Par borough (optionnel)
        ROUND(AVG(CASE WHEN borough = 'Manhattan' THEN avg_speed_mph END), 1) AS manhattan_avg_speed,
        ROUND(AVG(CASE WHEN borough = 'Brooklyn' THEN avg_speed_mph END), 1) AS brooklyn_avg_speed,
        ROUND(AVG(CASE WHEN borough = 'Queens' THEN avg_speed_mph END), 1) AS queens_avg_speed
        
    FROM {{ ref('int_traffic_hourly_avg') }}
    WHERE measurement_hour IS NOT NULL
    GROUP BY DATE(measurement_hour)
),

-- Agrégation quotidienne des plaintes 311
daily_complaints AS (
    SELECT
        request_date,
        
        -- Volume total
        SUM(total_complaints) AS total_complaints,
        SUM(closed_complaints) AS closed_complaints,
        SUM(open_complaints) AS open_complaints,
        
        -- Par type (top 3)
        SUM(CASE WHEN complaint_type LIKE '%Parking%' THEN total_complaints ELSE 0 END) AS parking_complaints,
        SUM(CASE WHEN complaint_type LIKE '%Traffic%' THEN total_complaints ELSE 0 END) AS traffic_complaints,
        SUM(CASE WHEN complaint_type LIKE '%Street%' THEN total_complaints ELSE 0 END) AS street_complaints,
        
        -- Performance de résolution
        ROUND(AVG(avg_resolution_days), 1) AS avg_resolution_days,
        ROUND(AVG(pct_closed), 2) AS avg_pct_closed
        
    FROM {{ ref('int_311_by_zone_daily') }}
    WHERE request_date IS NOT NULL
    GROUP BY request_date
),

-- Jointure de toutes les métriques quotidiennes
weather_impact AS (
    SELECT
        -- Dimension temporelle
        w.weather_date,
        EXTRACT(DAYOFWEEK FROM w.weather_date) AS day_of_week,
        EXTRACT(MONTH FROM w.weather_date) AS month,
        EXTRACT(YEAR FROM w.weather_date) AS year,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM w.weather_date) IN (1, 7) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Conditions météo
        w.weather_condition,
        w.weather_temp_min_celcius,
        w.weather_temp_max_celcius,
        w.weather_precip_mm,
        w.weather_snow_mm,
        w.is_extreme_snow,
        w.is_extreme_rain,
        w.is_extreme_temperature,
        
        -- Métriques de mobilité (courses)
        COALESCE(t.total_trips, 0) AS total_trips,
        COALESCE(t.yellow_trips, 0) AS yellow_trips,
        COALESCE(t.hvfhv_trips, 0) AS hvfhv_trips,
        COALESCE(t.fhv_trips, 0) AS fhv_trips,
        t.avg_trip_distance,
        t.avg_trip_duration,
        t.pct_rush_hour,
        
        -- Métriques de trafic
        COALESCE(traf.overall_avg_speed_mph, 0) AS avg_traffic_speed_mph,
        traf.avg_pct_congested,
        COALESCE(traf.total_severe_congestion_events, 0) AS severe_congestion_events,
        traf.manhattan_avg_speed,
        traf.brooklyn_avg_speed,
        traf.queens_avg_speed,
        
        -- Métriques 311
        COALESCE(c.total_complaints, 0) AS total_311_complaints,
        COALESCE(c.parking_complaints, 0) AS parking_complaints,
        COALESCE(c.traffic_complaints, 0) AS traffic_complaints,
        c.avg_resolution_days,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS aggregated_at
        
    FROM weather w
    LEFT JOIN daily_trips t
        ON w.weather_date = t.trip_date
    LEFT JOIN daily_traffic traf
        ON w.weather_date = traf.traffic_date
    LEFT JOIN daily_complaints c
        ON w.weather_date = c.request_date
)

SELECT * FROM weather_impact