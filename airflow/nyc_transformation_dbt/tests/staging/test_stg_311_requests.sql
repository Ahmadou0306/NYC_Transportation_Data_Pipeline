-- tests/staging/assert_311_requests_data_quality.sql
-- Vérifie la qualité des données 311 nettoyées (plaintes transport uniquement)

WITH quality_checks AS (
    SELECT 
        request_id,
        request_created_at,
        request_closed_at,
        complaint_type,
        agency,
        status,
        borough,
        latitude,
        longitude,
        zip_code,
        resolution_updated_at,
        loaded_at,
        
        -- Flags d'erreurs
        CASE 
            WHEN request_id IS NULL THEN 'request_id_null'
            WHEN request_created_at IS NULL THEN 'created_at_null'
            WHEN request_created_at > CURRENT_TIMESTAMP() THEN 'created_at_future'
            WHEN complaint_type IS NULL THEN 'complaint_type_null'
            WHEN agency IS NULL THEN 'agency_null'
            WHEN status IS NULL THEN 'status_null'
            WHEN borough IS NULL THEN 'borough_null'
            WHEN borough NOT IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'Unspecified') THEN 'borough_invalid'
            WHEN latitude IS NULL THEN 'latitude_null'
            WHEN latitude < 40.4 OR latitude > 41.0 THEN 'latitude_out_of_range'
            WHEN longitude IS NULL THEN 'longitude_null'
            WHEN longitude < -74.3 OR longitude > -73.7 THEN 'longitude_out_of_range'
            WHEN status = 'Closed' AND request_closed_at IS NULL THEN 'closed_status_but_no_date'
            WHEN request_closed_at IS NOT NULL AND request_closed_at < request_created_at THEN 'closed_before_created'
            WHEN request_closed_at IS NOT NULL AND request_closed_at > CURRENT_TIMESTAMP() THEN 'closed_at_future'
            WHEN resolution_updated_at IS NOT NULL AND resolution_updated_at > CURRENT_TIMESTAMP() THEN 'resolution_future'
            WHEN zip_code IS NOT NULL AND LENGTH(zip_code) != 5 THEN 'zip_code_invalid_length'
            WHEN loaded_at IS NULL THEN 'loaded_at_null'
            WHEN NOT (
                complaint_type LIKE '%Parking%'
                OR complaint_type LIKE '%Traffic%'
                OR complaint_type LIKE '%Street%'
                OR complaint_type LIKE '%Noise - Vehicle%'
                OR complaint_type LIKE '%Noise - Street%'
                OR complaint_type LIKE '%Blocked Driveway%'
            ) THEN 'complaint_type_not_transport_related'
            ELSE NULL
        END AS error_type
        
    FROM {{ ref('stg_311_requests') }}
)

-- Retourner toutes les lignes avec des erreurs
SELECT 
    request_id,
    error_type,
    request_created_at,
    request_closed_at,
    complaint_type,
    status,
    borough,
    latitude,
    longitude,
    zip_code
FROM quality_checks
WHERE error_type IS NOT NULL
ORDER BY error_type, request_id
LIMIT 1000