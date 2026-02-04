WITH source AS (
    SELECT *
    FROM {{ source("raw_data", "raw_yellow_axi_vehicule_trips") }}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit', 1000) }}
    {% endif %}
),

-- Nettoyage et standardisation
cleaned AS (
    SELECT
        -- Identifiants
        CAST(VendorID AS INT64) AS vendor_id,
        
        -- Timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        -- Localisation
        CAST(PULocationID AS INT64) AS pickup_location_id,
        CAST(DOLocationID AS INT64) AS dropoff_location_id,
        
        -- Passagers et distance
        CAST(passenger_count AS INT64) AS passenger_count,
        CAST(trip_distance AS FLOAT64) AS trip_distance_miles,
        
        -- Tarification
        CAST(RatecodeID AS INT64) AS rate_code_id,
        CAST(payment_type AS INT64) AS payment_type_id,
        
        -- Montants
        CAST(fare_amount AS FLOAT64) AS fare_amount,
        CAST(extra AS FLOAT64) AS extra_charges,
        CAST(mta_tax AS FLOAT64) AS mta_tax,
        CAST(tip_amount AS FLOAT64) AS tip_amount,
        CAST(tolls_amount AS FLOAT64) AS tolls_amount,
        CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
        CAST(total_amount AS FLOAT64) AS total_amount,
        CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
        CAST(airport_fee AS FLOAT64) AS airport_fee,
        
        -- Métadonnées
        store_and_fwd_flag,
        
        -- Type de service (pour l'union avec FHV/HVFHV)
        'Yellow Taxi' AS service_type,
        
        -- Traçabilité
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM source
    WHERE 
        -- Filtrage des données invalides essentielles
        tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
        
        -- Dates cohérentes
        AND CAST(tpep_pickup_datetime AS TIMESTAMP) <= CURRENT_TIMESTAMP()
        AND CAST(tpep_dropoff_datetime AS TIMESTAMP) <= CURRENT_TIMESTAMP()
        AND CAST(tpep_dropoff_datetime AS TIMESTAMP) >= CAST(tpep_pickup_datetime AS TIMESTAMP)
        
        -- Valeurs de base cohérentes
        AND CAST(PULocationID AS INT64) > 0
        AND CAST(DOLocationID AS INT64) > 0
        AND CAST(passenger_count AS FLOAT64) >= 0
        AND CAST(trip_distance AS FLOAT64) >= 0
        AND CAST(total_amount AS FLOAT64) >= 0
)

SELECT * FROM cleaned