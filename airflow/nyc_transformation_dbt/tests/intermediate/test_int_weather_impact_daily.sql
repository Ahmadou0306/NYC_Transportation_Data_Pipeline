WITH error as (
    SELECT
        weather_date,
        weather_condition,
        total_trips,
        avg_traffic_speed_mph,
        CASE
            WHEN weather_date > CURRENT_DATE() THEN 'date_future'
            WHEN weather_condition IS NULL THEN 'condition_missing'
            WHEN total_trips < 0 THEN 'negative_trips'
            WHEN avg_traffic_speed_mph IS NOT NULL 
                AND (avg_traffic_speed_mph < 0 OR avg_traffic_speed_mph > 100) THEN 'speed_invalid'
            ELSE NULL
        END AS error_type
    FROM {{ ref('int_weather_impact_daily') }}
)


SELECT * FROM error
WHERE error_type IS NOT NULL
LIMIT 1000