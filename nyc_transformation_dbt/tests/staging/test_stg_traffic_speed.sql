SELECT 
    link_id,
    link_name,
    borough,
    measurement_timestamp,
    speed_mph,
    travel_time_seconds,
    status_code,
    
    -- Identifier le type d'erreur
    CASE 
        WHEN link_id IS NULL THEN 'link_id_null'
        WHEN link_name IS NULL THEN 'link_name_null'
        WHEN borough IS NULL THEN 'borough_null'
        WHEN measurement_timestamp IS NULL THEN 'measurement_timestamp_null'
        WHEN speed_mph IS NULL THEN 'speed_null'
        WHEN travel_time_seconds IS NULL THEN 'travel_time_null'
        WHEN status_code IS NULL THEN 'status_code_null'
        
        -- Valeurs hors plage
        WHEN speed_mph < 0 THEN 'speed_negative'
        WHEN speed_mph > 100 THEN 'speed_too_high'
        WHEN travel_time_seconds < 1 THEN 'travel_time_zero_or_negative'
        WHEN travel_time_seconds > 7200 THEN 'travel_time_too_long'
        
        -- Dates aberrantes
        WHEN measurement_timestamp > CURRENT_TIMESTAMP() THEN 'measurement_future'
        WHEN measurement_timestamp < TIMESTAMP('2010-01-01') THEN 'measurement_too_old'
        
        -- Borough invalide
        WHEN borough NOT IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island') THEN 'borough_invalid'
        
        -- Status invalide (typiquement 0 = OK, 1+ = problème)
        WHEN status_code NOT IN (0, 1, 2, 3) THEN 'status_code_invalid'
        
        -- Incohérence vitesse vs temps        
        ELSE NULL
    END AS error_type
    
FROM {{ ref("stg_traffic_speed") }}
WHERE 
    -- Colonnes obligatoires NULL
    link_id IS NULL
    OR link_name IS NULL
    OR borough IS NULL
    OR measurement_timestamp IS NULL
    OR speed_mph IS NULL
    OR travel_time_seconds IS NULL
    OR status_code IS NULL
    
    -- Valeurs hors plage
    OR speed_mph < 0
    OR speed_mph > 100
    OR travel_time_seconds < 1
    OR travel_time_seconds > 7200
    
    -- Dates aberrantes
    OR measurement_timestamp > CURRENT_TIMESTAMP()
    OR measurement_timestamp < TIMESTAMP('2010-01-01')
    
    -- Borough invalide
    OR borough NOT IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island')
    
    -- Status invalide
    OR status_code NOT IN (0, 1, 2, 3)

ORDER BY error_type, measurement_timestamp DESC
LIMIT 1000