SELECT 
    dispatching_base_num,
    affiliated_base_number,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    
    -- Identifier le type d'erreur
    CASE 
        WHEN dispatching_base_num IS NULL THEN 'dispatching_base_null'
        WHEN affiliated_base_number IS NULL THEN 'affiliated_base_null'
        WHEN pickup_datetime IS NULL THEN 'pickup_datetime_null'
        WHEN dropoff_datetime IS NULL THEN 'dropoff_datetime_null'
        WHEN pickup_location_id IS NULL THEN 'pickup_location_null'
        WHEN dropoff_location_id IS NULL THEN 'dropoff_location_null'
        WHEN pickup_location_id = 0 THEN 'pickup_location_zero'
        WHEN dropoff_location_id = 0 THEN 'dropoff_location_zero'
        WHEN pickup_location_id < 1 OR pickup_location_id > 265 THEN 'pickup_location_out_of_range'
        WHEN dropoff_location_id < 1 OR dropoff_location_id > 265 THEN 'dropoff_location_out_of_range'
        WHEN pickup_datetime > CURRENT_TIMESTAMP() THEN 'pickup_future'
        WHEN dropoff_datetime > CURRENT_TIMESTAMP() THEN 'dropoff_future'
        WHEN dropoff_datetime < pickup_datetime THEN 'dropoff_before_pickup'
        WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) < 1 THEN 'trip_too_short'
        WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, HOUR) > 24 THEN 'trip_too_long'
        ELSE NULL
    END AS error_type
    
FROM {{ ref("stg_for_hire_vehicule_trips") }}
WHERE 
    -- Colonnes obligatoires NULL
    dispatching_base_num IS NULL
    OR affiliated_base_number IS NULL
    OR pickup_datetime IS NULL
    OR dropoff_datetime IS NULL
    OR pickup_location_id IS NULL
    OR dropoff_location_id IS NULL
    
    -- Valeurs invalides
    OR pickup_location_id = 0
    OR dropoff_location_id = 0
    OR pickup_location_id < 1 OR pickup_location_id > 265
    OR dropoff_location_id < 1 OR dropoff_location_id > 265
    
    -- Dates aberrantes
    OR pickup_datetime > CURRENT_TIMESTAMP()
    OR dropoff_datetime > CURRENT_TIMESTAMP()
    OR dropoff_datetime < pickup_datetime
    
    -- Durée aberrante
    OR TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) < 1
    OR TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, HOUR) > 24

ORDER BY error_type, pickup_datetime DESC

LIMIT 1000