WITH manually_array AS (
    SELECT GENERATE_ARRAY(0, 23, 1) as hour_generated
),

unnest_hour_array AS (
    SELECT hours
    FROM manually_array,
    UNNEST(hour_generated) as hours
),

hours_calculled AS (
    SELECT 
        hours,
        CASE
            WHEN hours = 0 THEN 12
            WHEN hours <= 12 then hours
            ELSE hours-12
        END AS hour_12,

        CASE
            WHEN hours < 12 then 'AM'
            ELSE 'PM'
        END AS am_pm,

        CASE
            WHEN hours >= 6 AND hours < 12 then 'Matin'
            WHEN hours >= 12 AND hours < 18 then 'Aprés midi'
            WHEN hours >= 18 AND hours < 21 then 'Soir'
            ELSE 'Nuit'
        END AS time_slot,

        CASE
            WHEN hours >= 6 AND hours <= 18 then TRUE
            ELSE FALSE
        END AS is_worked_hours,

        CASE
            WHEN (hours >= 7 AND hours < 10) OR (hours >= 16 AND hours < 19) then TRUE
            ELSE FALSE
        END AS is_rush_hour
    FROM unnest_hour_array    
)


SELECT *
FROM hours_calculled