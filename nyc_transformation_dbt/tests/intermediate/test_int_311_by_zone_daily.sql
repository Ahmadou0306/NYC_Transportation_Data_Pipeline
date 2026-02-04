WITH errors AS (
    SELECT
        request_date,
        borough,
        complaint_type,
        total_complaints,
        pct_closed,
        CASE
            WHEN request_date > CURRENT_DATE() THEN 'date_future'
            WHEN total_complaints = 0 THEN 'zero_complaints'
            WHEN pct_closed > 100 THEN 'pct_invalid'
            WHEN closed_complaints > total_complaints THEN 'closed_exceeds_total'
            ELSE NULL
        END AS error_type
    FROM {{ ref('int_311_by_zone_daily') }}
)

SELECT *
FROM errors
WHERE error_type IS NOT NULL