WITH error as (
    SELECT
        pickup_location_id,
        hour_of_day,
        day_of_week,
        total_pickups,
        CASE
            WHEN pickup_location_id < 1 OR pickup_location_id > 265 THEN 'location_invalid'
            WHEN hour_of_day < 0 OR hour_of_day > 23 THEN 'hour_invalid'
            WHEN day_of_week < 1 OR day_of_week > 7 THEN 'day_invalid'
            WHEN total_pickups < 0 THEN 'negative_pickups'
            WHEN avg_daily_pickups < 0 THEN 'negative_avg'
            ELSE NULL
        END AS error_type
    FROM {{ ref('int_taxi_demand_hourly') }}
)

SELECT *
FROM error
WHERE error_type IS NOT NULL