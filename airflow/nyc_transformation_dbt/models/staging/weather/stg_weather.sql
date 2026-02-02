WITH source AS (
    SELECT *
    FROM {{source("raw_data","raw_weather")}}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit') }}
    {% endif %}
),

-- renommage des colonnes et conversion des colonnes
colonne_formatage AS (
    SELECT
        STATION as weather_station,

        -- Conversion en Degrés celsius
        CAST(TMIN AS FLOAT64)/ 10 as weather_temp_min_celcius,
        CAST(TMAX AS FLOAT64)/ 10 as weather_temp_max_celcius,

        -- Neige en mm
        CAST(SNOW AS FLOAT64) as weather_snow_mm,
        
        -- Précipitations en 10e de mm
        CAST(PRCP AS FLOAT64)/10 as weather_precip_mm,
        DATE(DATE) as weather_date,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source
),

-- formatage des date 
valid_value AS (
    SELECT * 
    FROM colonne_formatage
    WHERE 
        weather_date IS NOT NULL 
        OR (DATE(weather_date) < CURRENT_DATE AND weather_date >= '2020-01-01')
        OR weather_temp_min_celcius BETWEEN -50 AND 70
        OR weather_temp_max_celcius BETWEEN -50 AND 70
        OR weather_precip_mm > 0
)

SELECT * 
FROM valid_value