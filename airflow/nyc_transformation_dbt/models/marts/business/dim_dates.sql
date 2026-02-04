WITH dates_array AS (
    SELECT 
        GENERATE_DATE_ARRAY(
            DATE('2020-01-01'),
            CURRENT_DATE,
            INTERVAL 1 DAY
        ) AS date_array_generated
),

-- ['2020-01-01', '2020-01-02', ... ,'2026-12-31']

dates_table AS (
    SELECT date
    FROM dates_array,
    UNNEST(date_array_generated) AS date
),
-- ['date'], 
-- ['2020-01-01']
-- ['2020-01-02']
-- ...
-- ['2026-12-31']

colone_calcule AS (
    SELECT 
        date,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        FORMAT_DATE('%B', date) AS month_name,
        EXTRACT(DAY FROM date) as day,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
        FORMAT_DATE('%A', date) AS day_of_week_name,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date) in (1,7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        EXTRACT(QUARTER FROM date) as quarter
)

SELECT *
FROM colone_calcule