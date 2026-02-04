WITH weather_impact AS (
    SELECT
        -- Dimension temporelle
        weather_date,        
        weather_temp_min_celcius,
        weather_temp_max_celcius,
        ROUND((weather_temp_min_celcius + weather_temp_max_celcius) / 2, 1) AS weather_temp_avg_celcius,
        weather_condition,
        weather_precip_mm,
        weather_snow_mm,
        is_extreme_snow,
        is_extreme_rain,
        is_extreme_temperature
    FROM {{ref("int_weather_impact_daily")}}
)

SELECT *
FROM weather_impact