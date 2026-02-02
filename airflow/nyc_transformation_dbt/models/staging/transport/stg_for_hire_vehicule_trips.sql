WITH source AS (
    SELECT * 
    FROM {{source("raw_data","raw_for_hire_vehicule_trips")}}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit', 1000) }}
    {% endif %}
),

-- Convertion des colonnes
convertion AS (
    SELECT
        dispatching_base_num, 
        affiliated_base_number,
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(ROUND(DOlocationID) AS INT64) AS dropoff_location_id,
        CAST(ROUND(PUlocationID ) AS INT64) AS pickup_location_id,
        CURRENT_TIMESTAMP() AS loaded_at,
        'FHV' AS service_type,
    FROM source
),

valeur_aberante AS (
    SELECT * 
    FROM convertion
    WHERE 
        pickup_datetime IS NOT NULL
        AND dropOff_datetime IS NOT NULL
        AND dropoff_location_id IS NOT NULL
        AND pickup_location_id IS NOT NULL
)

SELECT * FROM valeur_aberante
