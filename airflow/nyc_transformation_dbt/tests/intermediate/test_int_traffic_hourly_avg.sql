WITH error AS (
  SELECT
      borough,
      measurement_hour,
      avg_speed_mph,
      status_code,
      CASE
          WHEN avg_speed_mph < 0 THEN 'speed_invalid'
          WHEN total_measurements = 0 THEN 'zero_measurements'
          WHEN status_code NOT IN (0, 1, 2, 3) THEN 'status_invalid'
          ELSE NULL
      END AS error_type
  FROM {{ ref('int_traffic_hourly_avg') }}
)
SELECT *
FROM error
WHERE error_type IS NOT NULL