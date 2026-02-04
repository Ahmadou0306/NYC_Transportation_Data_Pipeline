SELECT 
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance_miles,
    fare_amount,
    total_amount,
    
    -- Identifier le type d'erreur
    CASE 
        -- Colonnes obligatoires NULL
        WHEN vendor_id IS NULL THEN 'vendor_id_null'
        WHEN pickup_datetime IS NULL THEN 'pickup_datetime_null'
        WHEN dropoff_datetime IS NULL THEN 'dropoff_datetime_null'
        WHEN pickup_location_id IS NULL THEN 'pickup_location_null'
        WHEN dropoff_location_id IS NULL THEN 'dropoff_location_null'
        
        -- Dates aberrantes
        WHEN pickup_datetime > CURRENT_TIMESTAMP() THEN 'pickup_future'
        WHEN dropoff_datetime > CURRENT_TIMESTAMP() THEN 'dropoff_future'
        WHEN dropoff_datetime < pickup_datetime THEN 'dropoff_before_pickup'
        WHEN pickup_datetime < TIMESTAMP('2009-01-01') THEN 'pickup_too_old'
        
        -- Localisation invalide
        WHEN pickup_location_id = 0 THEN 'pickup_location_zero'
        WHEN dropoff_location_id = 0 THEN 'dropoff_location_zero'
        WHEN pickup_location_id < 1 OR pickup_location_id > 265 THEN 'pickup_location_out_of_range'
        WHEN dropoff_location_id < 1 OR dropoff_location_id > 265 THEN 'dropoff_location_out_of_range'
        
        -- Passagers aberrants
        WHEN passenger_count IS NULL THEN 'passenger_count_null'
        WHEN passenger_count = 0 THEN 'passenger_count_zero'
        WHEN passenger_count > 9 THEN 'passenger_count_too_high'
        
        -- Distance aberrante
        WHEN trip_distance_miles IS NULL THEN 'trip_distance_null'
        WHEN trip_distance_miles < 0 THEN 'trip_distance_negative'
        WHEN trip_distance_miles > 100 THEN 'trip_distance_too_long'
        
        -- Montants aberrants
        WHEN fare_amount IS NULL THEN 'fare_amount_null'
        WHEN fare_amount < 0 THEN 'fare_amount_negative'
        WHEN fare_amount > 500 THEN 'fare_amount_too_high'
        WHEN total_amount IS NULL THEN 'total_amount_null'
        WHEN total_amount < 0 THEN 'total_amount_negative'
        WHEN total_amount > 1000 THEN 'total_amount_too_high'
        
        -- Incohérences métier
        WHEN trip_distance_miles = 0 AND fare_amount > 10 THEN 'zero_distance_but_high_fare'
        WHEN trip_distance_miles > 0 AND fare_amount = 0 THEN 'distance_but_zero_fare'
        WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) < 1 THEN 'trip_too_short'
        WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, HOUR) > 12 THEN 'trip_too_long'
        
        -- Tarifs négatifs
        WHEN tip_amount < 0 THEN 'tip_negative'
        WHEN tolls_amount < 0 THEN 'tolls_negative'
        WHEN extra_charges < 0 THEN 'extra_negative'
        
        ELSE NULL
    END AS error_type
    
FROM {{ ref("stg_yellow_taxi_trips") }}
WHERE 
    -- Colonnes obligatoires NULL
    vendor_id IS NULL
    OR pickup_datetime IS NULL
    OR dropoff_datetime IS NULL
    OR pickup_location_id IS NULL
    OR dropoff_location_id IS NULL
    OR passenger_count IS NULL
    OR trip_distance_miles IS NULL
    OR fare_amount IS NULL
    OR total_amount IS NULL
    
    -- Dates aberrantes
    OR pickup_datetime > CURRENT_TIMESTAMP()
    OR dropoff_datetime > CURRENT_TIMESTAMP()
    OR dropoff_datetime < pickup_datetime
    OR pickup_datetime < TIMESTAMP('2009-01-01')
    
    -- Localisation invalide
    OR pickup_location_id = 0
    OR dropoff_location_id = 0
    OR pickup_location_id < 1 OR pickup_location_id > 265
    OR dropoff_location_id < 1 OR dropoff_location_id > 265
    
    -- Passagers aberrants
    OR passenger_count = 0
    OR passenger_count > 9
    
    -- Distance aberrante
    OR trip_distance_miles < 0
    OR trip_distance_miles > 100
    
    -- Montants aberrants
    OR fare_amount < 0
    OR fare_amount > 500
    OR total_amount < 0
    OR total_amount > 1000
    
    -- Incohérences métier
    OR (trip_distance_miles = 0 AND fare_amount > 10)
    OR (trip_distance_miles > 0 AND fare_amount = 0)
    OR TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) < 1
    OR TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, HOUR) > 12
    
    -- Tarifs négatifs
    OR tip_amount < 0
    OR tolls_amount < 0
    OR extra_charges < 0

ORDER BY error_type, pickup_datetime DESC
LIMIT 1000