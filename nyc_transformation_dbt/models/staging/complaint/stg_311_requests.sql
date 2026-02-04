WITH source AS (
    SELECT *
    FROM {{ source("raw_data", "raw_311_requests") }}
    {% if var('is_dev', false) %}
    LIMIT {{ var('dev_limit', 1000) }}
    {% endif %}
),

-- Filtrer SEULEMENT les plaintes liées au transport
transport_related AS (
    SELECT *
    FROM source
    WHERE 
        unique_key IS NOT NULL
        AND created_date IS NOT NULL
        AND borough IS NOT NULL
        AND (latitude IS NOT NULL AND longitude IS NOT NULL)
        /*
        -- Critère 1 : Types de plaintes pertinents
        (
            complaint_type LIKE '%Parking%'
            OR complaint_type LIKE '%Traffic%'
            OR complaint_type LIKE '%Street%'
            OR complaint_type LIKE '%Noise - Vehicle%'
            OR complaint_type LIKE '%Noise - Street%'
            OR complaint_type LIKE '%Blocked Driveway%'
        )
        -- Critère 2 : Données de qualité
        AND unique_key IS NOT NULL
        AND created_date IS NOT NULL
        AND borough IS NOT NULL
        AND (latitude IS NOT NULL AND longitude IS NOT NULL)
        */
),

-- Nettoyage des colonnes
cleaned AS (
    SELECT
        -- Identifiants
        CAST(unique_key AS STRING) AS request_id,
        CAST(created_date AS TIMESTAMP) AS request_created_at,
        CAST(closed_date AS TIMESTAMP) AS request_closed_at,  
        
        -- Classification
        complaint_type,
        descriptor AS complaint_descriptor,
        agency,
        agency_name,
        status,
        
        -- Localisation
        borough,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        CAST(incident_zip AS STRING) AS zip_code,
        
        -- Adresse (prend la première non-NULL)
        COALESCE(
            incident_address,
            street_name,
            intersection_street_1
        ) AS location_address,
        
        -- Résolution
        resolution_description,
        CAST(resolution_action_updated_date AS TIMESTAMP) AS resolution_updated_at,
        
        -- Traçabilité
        CURRENT_TIMESTAMP() AS loaded_at
        
    FROM transport_related
)

SELECT * FROM cleaned