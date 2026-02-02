-- tests/staging/assert_hvfhv_trips_data_quality.sql
-- Vérifie la qualité des données HVFHV (Uber/Lyft)

SELECT 
    hvfhs_license_num,
    dispatching_base_num,
    request_datetime,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    trip_distance_miles,
    trip_time_seconds,
    base_passenger_fare,
    
    -- Identifier le type d'erreur
    CASE 
        -- Colonnes obligatoires NULL
        WHEN hvfhs_license_num IS NULL THEN 'hvfhs_license_null'
        WHEN dispatching_base_num IS NULL THEN 'dispatching_base_null'
        WHEN pickup_datetime IS NULL THEN 'pickup_datetime_null'
        WHEN dropoff_datetime IS NULL THEN 'dropoff_datetime_null'
        WHEN pickup_location_id IS NULL THEN 'pickup_location_null'
        WHEN dropoff_location_id IS NULL THEN 'dropoff_location_null'
        
        -- Dates aberrantes
        WHEN request_datetime IS NOT NULL AND request_datetime > CURRENT_TIMESTAMP() THEN 'request_future'
        WHEN on_scene_datetime IS NOT NULL AND on_scene_datetime > CURRENT_TIMESTAMP() THEN 'on_scene_future'
        WHEN pickup_datetime > CURRENT_TIMESTAMP() THEN 'pickup_future'
        WHEN dropoff_datetime > CURRENT_TIMESTAMP() THEN 'dropoff_future'
        WHEN pickup_datetime < TIMESTAMP('2015-01-01') THEN 'pickup_too_old'
        
        -- Ordre chronologique des timestamps
        WHEN request_datetime IS NOT NULL AND on_scene_datetime IS NOT NULL 
             AND on_scene_datetime < request_datetime THEN 'on_scene_before_request'
        WHEN on_scene_datetime IS NOT NULL AND pickup_datetime IS NOT NULL 
             AND pickup_datetime < on_scene_datetime THEN 'pickup_before_on_scene'
        WHEN dropoff_datetime < pickup_datetime THEN 'dropoff_before_pickup'
        
        -- Localisation invalide
        WHEN pickup_location_id = 0 THEN 'pickup_location_zero'
        WHEN dropoff_location_id = 0 THEN 'dropoff_location_zero'
        WHEN pickup_location_id < 1 OR pickup_location_id > 265 THEN 'pickup_location_out_of_range'
        WHEN dropoff_location_id < 1 OR dropoff_location_id > 265 THEN 'dropoff_location_out_of_range'
        
        -- Métriques de course aberrantes
        WHEN trip_distance_miles IS NULL THEN 'trip_distance_null'
        WHEN trip_distance_miles < 0 THEN 'trip_distance_negative'
        WHEN trip_distance_miles > 200 THEN 'trip_distance_too_long'
        WHEN trip_time_seconds IS NULL THEN 'trip_time_null'
        WHEN trip_time_seconds < 0 THEN 'trip_time_negative'
        WHEN trip_time_seconds > 86400 THEN 'trip_time_too_long'  -- > 24h
        
        -- Montants aberrants
        WHEN base_passenger_fare IS NULL THEN 'base_fare_null'
        WHEN base_passenger_fare < 0 THEN 'base_fare_negative'
        WHEN base_passenger_fare > 1000 THEN 'base_fare_too_high'
        WHEN tolls_amount < 0 THEN 'tolls_negative'
        WHEN sales_tax < 0 THEN 'sales_tax_negative'
        WHEN tip_amount < 0 THEN 'tip_negative'
        WHEN driver_pay < 0 THEN 'driver_pay_negative'
        
        -- Flags invalides
        WHEN shared_request_flag NOT IN ('Y', 'N', '') AND shared_request_flag IS NOT NULL THEN 'shared_request_flag_invalid'
        WHEN shared_match_flag NOT IN ('Y', 'N', '') AND shared_match_flag IS NOT NULL THEN 'shared_match_flag_invalid'
        WHEN access_a_ride_flag NOT IN ('Y', 'N', '', ' ') AND access_a_ride_flag IS NOT NULL THEN 'access_a_ride_flag_invalid'
        WHEN wav_request_flag NOT IN ('Y', 'N', '') AND wav_request_flag IS NOT NULL THEN 'wav_request_flag_invalid'
        WHEN wav_match_flag NOT IN ('Y', 'N', '') AND wav_match_flag IS NOT NULL THEN 'wav_match_flag_invalid'
        
        -- Incohérences métier
        WHEN trip_distance_miles = 0 AND base_passenger_fare > 10 THEN 'zero_distance_but_high_fare'
        WHEN trip_distance_miles > 0 AND base_passenger_fare = 0 THEN 'distance_but_zero_fare'
        WHEN trip_time_seconds < 60 AND trip_distance_miles > 5 THEN 'impossible_speed'
        WHEN shared_request_flag = 'Y' AND shared_match_flag = 'N' AND base_passenger_fare = driver_pay THEN 'shared_request_mismatch'
        
        ELSE NULL
    END AS error_type
    
FROM {{ ref("stg_hvfhv_trips") }}
WHERE 
    -- Colonnes obligatoires NULL
    hvfhs_license_num IS NULL
    OR dispatching_base_num IS NULL
    OR pickup_datetime IS NULL
    OR dropoff_datetime IS NULL
    OR pickup_location_id IS NULL
    OR dropoff_location_id IS NULL
    OR trip_distance_miles IS NULL
    OR trip_time_seconds IS NULL
    OR base_passenger_fare IS NULL
    
    -- Dates aberrantes
    OR (request_datetime IS NOT NULL AND request_datetime > CURRENT_TIMESTAMP())
    OR (on_scene_datetime IS NOT NULL AND on_scene_datetime > CURRENT_TIMESTAMP())
    OR pickup_datetime > CURRENT_TIMESTAMP()
    OR dropoff_datetime > CURRENT_TIMESTAMP()
    OR pickup_datetime < TIMESTAMP('2015-01-01')
    
    -- Ordre chronologique
    OR (request_datetime IS NOT NULL AND on_scene_datetime IS NOT NULL AND on_scene_datetime < request_datetime)
    OR (on_scene_datetime IS NOT NULL AND pickup_datetime IS NOT NULL AND pickup_datetime < on_scene_datetime)
    OR dropoff_datetime < pickup_datetime
    
    -- Localisation invalide
    OR pickup_location_id = 0
    OR dropoff_location_id = 0
    OR pickup_location_id < 1 OR pickup_location_id > 265
    OR dropoff_location_id < 1 OR dropoff_location_id > 265
    
    -- Métriques aberrantes
    OR trip_distance_miles < 0
    OR trip_distance_miles > 200
    OR trip_time_seconds < 0
    OR trip_time_seconds > 86400
    
    -- Montants aberrants
    OR base_passenger_fare < 0
    OR base_passenger_fare > 1000
    OR tolls_amount < 0
    OR sales_tax < 0
    OR tip_amount < 0
    OR driver_pay < 0
    
    -- Flags invalides
    OR (shared_request_flag NOT IN ('Y', 'N', '') AND shared_request_flag IS NOT NULL)
    OR (shared_match_flag NOT IN ('Y', 'N', '') AND shared_match_flag IS NOT NULL)
    OR (wav_request_flag NOT IN ('Y', 'N', '') AND wav_request_flag IS NOT NULL)
    OR (wav_match_flag NOT IN ('Y', 'N', '') AND wav_match_flag IS NOT NULL)
    
    -- Incohérences métier
    OR (trip_distance_miles = 0 AND base_passenger_fare > 10)
    OR (trip_distance_miles > 0 AND base_passenger_fare = 0)
    OR (trip_time_seconds < 60 AND trip_distance_miles > 5)

ORDER BY error_type, pickup_datetime DESC
LIMIT 1000