SELECT 
    *
FROM {{ ref('stg_weather') }}
WHERE 

    -- Colonnes obligatoires nulles
    weather_station IS NULL
    OR weather_date IS NULL
    OR weather_temp_min_celcius IS NULL
    OR weather_temp_max_celcius IS NULL
    OR weather_precip_mm IS NULL
    OR weather_snow_mm IS NULL
    OR loaded_at IS NULL
    
    -- Valeurs incohérentes
    OR weather_temp_min_celcius < -50 
    OR weather_temp_min_celcius > 70
    OR weather_temp_max_celcius < -50 
    OR weather_temp_max_celcius > 70
    OR weather_precip_mm < 0
    OR weather_snow_mm < 0
    
    -- Logique métier
    OR weather_temp_max_celcius < weather_temp_min_celcius  -- Max < Min impossible
    OR weather_date > CURRENT_DATE()  -- Pas de dates futures
LIMIT 1000