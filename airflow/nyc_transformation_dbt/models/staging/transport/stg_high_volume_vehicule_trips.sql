WITH source AS (
    SELECT *
    FROM {{ source("raw_data", "raw_high_volume_vehicule_trips") }}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit', 1000) }}
    {% endif %}
),

-- Nettoyage et standardisation
cleaned AS (
    SELECT
        -- Identifiants de service
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        
        -- Timeline de la course (4 timestamps différents!)
        CAST(request_datetime AS TIMESTAMP) AS request_datetime,
        CAST(on_scene_datetime AS TIMESTAMP) AS on_scene_datetime,
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        -- Localisation
        CAST(PULocationID AS INT64) AS pickup_location_id,
        CAST(DOLocationID AS INT64) AS dropoff_location_id,
        
        -- Métriques de course
        CAST(trip_miles AS FLOAT64) AS trip_distance_miles,
        CAST(trip_time AS INT64) AS trip_time_seconds,
        
        -- Tarification (plus détaillée que Yellow Taxi)
        CAST(base_passenger_fare AS FLOAT64) AS base_passenger_fare,
        CAST(tolls AS FLOAT64) AS tolls_amount,
        CAST(bcf AS FLOAT64) AS black_car_fund,  -- Black Car Fund
        CAST(sales_tax AS FLOAT64) AS sales_tax,
        CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
        CAST(airport_fee AS FLOAT64) AS airport_fee,
        CAST(tips AS FLOAT64) AS tip_amount,
        CAST(driver_pay AS FLOAT64) AS driver_pay,
        
        -- Flags de service (Y/N)
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,  -- Service handicapés
        wav_request_flag,    -- Wheelchair Accessible Vehicle demandé
        wav_match_flag,      -- Wheelchair Accessible Vehicle fourni
        
        -- Type de service (pour union)
        'HVFHV' AS service_type,
        
        -- Traçabilité
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM source
    WHERE 
        -- Filtrage des données invalides essentielles
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
        
        -- Dates cohérentes
        AND CAST(pickup_datetime AS TIMESTAMP) <= CURRENT_TIMESTAMP()
        AND CAST(dropoff_datetime AS TIMESTAMP) <= CURRENT_TIMESTAMP()
        AND CAST(dropoff_datetime AS TIMESTAMP) >= CAST(pickup_datetime AS TIMESTAMP)
        
        -- Valeurs de base cohérentes
        AND CAST(PULocationID AS INT64) > 0
        AND CAST(DOLocationID AS INT64) > 0
        AND CAST(trip_miles AS FLOAT64) >= 0
        AND CAST(trip_time AS INT64) >= 0
        AND CAST(base_passenger_fare AS FLOAT64) >= 0
)

SELECT * FROM cleaned