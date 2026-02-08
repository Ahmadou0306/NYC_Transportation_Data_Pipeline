{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["service_type", "pickup_borough", "is_stuck_in_traffic"]
    )
}}

-- incremental va permettre de juste ajouter les nouvelles lignes sans mettre à jours toute la table
-- reduction de coût pouvant passer de 5$ par requete à moins de 1$


WITH trips AS (
    SELECT
        -- Hash déterministe au lieu de GENERATE_UUID
        TO_BASE64(MD5(CONCAT(
            t.service_type,
            CAST(t.pickup_datetime AS STRING),
            CAST(t.pickup_location_id AS STRING),
            CAST(t.dropoff_location_id AS STRING)
        ))) AS trip_id,
        
        t.service_type,
        t.pickup_datetime,

        -- Dimension date
        d.year AS pickup_year,
        d.month AS pickup_month,
        d.month_name AS pickup_month_name,
        d.day AS pickup_day,
        d.day_of_week AS pickup_day_of_week,
        d.day_of_week_name AS pickup_day_of_week_name,
        d.is_weekend AS pickup_is_weekend,
        d.quarter AS pickup_quarter,

        -- Dimension heure
        h.hours AS pickup_hour,
        h.time_slot AS pickup_time_slot,
        h.is_rush_hour AS pickup_is_rush_hour,

        -- Dimension zone
        pickup_z.zone_id AS pickup_zone_id,
        pickup_z.zone_name AS pickup_zone_name,
        t.pickup_borough,
        pickup_z.zone_category AS pickup_zone_category,
        
        -- Dimension dropoff zone
        dropoff_z.zone_id AS dropoff_zone_id,
        dropoff_z.zone_name AS dropoff_zone_name,
        dropoff_z.borough AS dropoff_borough,

        -- Météo
        w.weather_condition,
        w.weather_temp_avg_celcius,
        w.is_extreme_snow,
        w.is_extreme_rain,

        -- Métriques
        t.trip_duration_minutes,
        t.trip_distance_miles,
        t.distance_category,
        t.traffic_avg_speed_mph,
        t.traffic_is_congested,
        t.estimated_duration_minutes,
        t.duration_diff_minutes,
        t.is_stuck_in_traffic

    FROM {{ ref('int_trips_with_traffic') }} AS t

    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON DATE(t.pickup_datetime) = d.date

    LEFT JOIN {{ ref('dim_hours') }} AS h
        ON EXTRACT(HOUR FROM t.pickup_datetime) = h.hours

    LEFT JOIN {{ ref('dim_zones') }} AS pickup_z
        ON t.pickup_location_id = pickup_z.zone_id

    LEFT JOIN {{ ref('dim_zones') }} AS dropoff_z
        ON t.dropoff_location_id = dropoff_z.zone_id

    LEFT JOIN {{ ref('dim_weather') }} AS w
        ON DATE(t.pickup_datetime) = w.weather_date

    {% if is_incremental() %}
    WHERE t.pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% endif %}
)

SELECT * FROM trips