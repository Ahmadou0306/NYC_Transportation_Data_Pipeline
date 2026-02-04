WITH error AS (
SELECT
    service_type,
    pickup_datetime,
    pickup_borough,
    traffic_avg_speed_mph,
    is_stuck_in_traffic,
    CASE
        WHEN pickup_borough IS NULL THEN 'borough_missing'
        WHEN traffic_avg_speed_mph IS NOT NULL 
             AND (traffic_avg_speed_mph < 0 OR traffic_avg_speed_mph > 100) THEN 'speed_invalid'
        WHEN estimated_duration_minutes IS NOT NULL 
             AND estimated_duration_minutes < 0 THEN 'negative_estimate'
        ELSE NULL
    END AS error_type
FROM {{ ref('int_trips_with_traffic') }}
)

SELECT * FROM error
WHERE error_type IS NOT NULL
LIMIT 1000