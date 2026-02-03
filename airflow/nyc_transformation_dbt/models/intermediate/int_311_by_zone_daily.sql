{{
    config(
        materialized='table',
        partition_by={
        "field": "request_date",
        "data_type": "date",
        "granularity": "day"
        },
        cluster_by=["borough","complaint_type"]
    )
}}


WITH source AS (
    SELECT
        request_id,
        request_created_at,
        request_closed_at,
        complaint_type,
        complaint_descriptor,
        agency,
        agency_name,
        status,
        borough,
        latitude,
        longitude,
        zip_code,
        location_address,
        resolution_description,
        resolution_updated_at,
    FROM {{ref("stg_311_requests")}}
),

zone_plus_plainte AS (
    SELECT 
        DATE(request_created_at) AS request_date,
        borough,
        complaint_type,
        status,
        COUNT(*) AS total_complaints,
        COUNTIF(status = 'Closed') AS closed_complaints,
        COUNTIF(status = 'Open') AS open_complaints,
        COUNTIF(status = 'Pending') AS pending_complaints,
        ROUND(COUNTIF(status = 'Closed') * 100.0 / COUNT(*), 2) AS pct_closed,

        ROUND(
            AVG(
                CASE 
                    WHEN status = 'Closed' AND request_closed_at IS NOT NULL
                    THEN DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY)
                    ELSE NULL
                END
            ),
            1
        ) AS avg_resolution_days,

        COUNT(DISTINCT agency) AS unique_agencies,
        COUNT(DISTINCT zip_code) AS unique_zip_codes,

    FROM source
    GROUP BY 
        DATE(request_created_at),
        borough,
        complaint_type,
        status
)

SELECT * FROM zone_plus_plainte
