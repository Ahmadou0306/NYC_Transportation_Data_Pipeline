{{
  config(
    materialized='table',
    partition_by={
      "field": "request_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["borough", "complaint_type", "status"]
  )
}}

WITH complaints_daily AS (
    SELECT
        -- Clés temporelles
        c.request_date,
        d.year AS request_year,
        d.month AS request_month,
        d.month_name AS request_month_name,
        d.day AS request_day,
        d.day_of_week AS request_day_of_week,
        d.day_of_week_name AS request_day_of_week_name,
        d.is_weekend AS request_is_weekend,
        d.quarter AS request_quarter,
        
        -- Dimensions géographiques et business
        c.borough,
        c.complaint_type,
        c.status,
        
        -- Métriques de volume
        c.total_complaints,
        c.closed_complaints,
        c.open_complaints,
        c.pending_complaints,
        c.pct_closed,
        
        -- Métriques de performance
        c.avg_resolution_days,
        c.unique_agencies,
        c.unique_zip_codes,
        
        -- Météo
        w.weather_condition,
        w.weather_temp_avg_celcius AS temp_avg_celsius,
        w.weather_precip_mm AS precip_mm,
        w.weather_snow_mm AS snow_mm,
        w.is_extreme_snow,
        w.is_extreme_rain,
        w.is_extreme_temperature,
        
        -- Flags dérivés
        CASE 
            WHEN c.avg_resolution_days IS NOT NULL AND c.avg_resolution_days <= 1 
            THEN TRUE 
            ELSE FALSE 
        END AS is_resolved_within_24h,
        
        CASE 
            WHEN c.avg_resolution_days IS NOT NULL AND c.avg_resolution_days <= 7 
            THEN TRUE 
            ELSE FALSE 
        END AS is_resolved_within_week,
        
        CASE 
            WHEN c.avg_resolution_days IS NOT NULL AND c.avg_resolution_days > 30 
            THEN TRUE 
            ELSE FALSE 
        END AS is_slow_resolution,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM {{ ref('int_311_by_zone_daily') }} AS c
    
    LEFT JOIN {{ ref('dim_dates') }} AS d
        ON c.request_date = d.date
        
    LEFT JOIN {{ ref('dim_weather') }} AS w
        ON c.request_date = w.weather_date
)

SELECT * FROM complaints_daily