WITH error AS (
    SELECT
        service_type,
        pickup_datetime,
        dropoff_datetime,
        trip_duration_minutes,
        distance_category,
        CASE
            WHEN service_type NOT IN ('Yellow Taxi', 'HVFHV', 'FHV') THEN 'service_invalid'
            WHEN pickup_datetime > CURRENT_TIMESTAMP() THEN 'pickup_future'
            WHEN dropoff_datetime < pickup_datetime THEN 'dropoff_before_pickup'
            WHEN trip_duration_minutes < 0 THEN 'negative_duration'
            WHEN trip_duration_minutes > 1440 THEN 'duration_too_long'
            WHEN pickup_location_id < 1 OR pickup_location_id > 265 THEN 'location_invalid'
            ELSE NULL
        END AS error_type
    FROM {{ ref('int_trips_enriched') }}
)

SELECT *
FROM error
WHERE error_type IS NOT NULL
LIMIT 1000