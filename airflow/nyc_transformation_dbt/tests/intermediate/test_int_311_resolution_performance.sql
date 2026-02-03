WITH error as (
    SELECT
        month,
        agency,
        complaint_type,
        performance_score,
        avg_resolution_days,
        CASE
            WHEN DATE(month) > CURRENT_DATE() THEN 'month_future'
            WHEN performance_score > 100 THEN 'score_invalid'
            WHEN avg_resolution_days < 0 THEN 'negative_resolution'
            WHEN pct_closed > 100 THEN 'pct_invalid'
            ELSE NULL
        END AS error_type
    FROM {{ ref('int_311_resolution_performance') }}
)

SELECT *
FROM error
WHERE error_type IS NOT NULL