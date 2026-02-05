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
        dt.date,
        EXTRACT(YEAR FROM dt.date) AS year,
        EXTRACT(MONTH FROM dt.date) AS month,
        FORMAT_DATE('%B', dt.date) AS month_name,
        EXTRACT(DAY FROM dt.date) AS day,
        EXTRACT(DAYOFWEEK FROM dt.date) AS day_of_week,
        FORMAT_DATE('%A', dt.date) AS day_of_week_name,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM dt.date) IN (1,7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        EXTRACT(QUARTER FROM dt.date) AS quarter
    FROM dates_table AS dt  -- Add table alias and FROM clause
)

SELECT *
FROM colone_calcule