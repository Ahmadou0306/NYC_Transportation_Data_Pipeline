-- jointure selon la date ou la station
WITH stg_weather AS (
    SELECT *
    FROM {{source("staging_data","stg_weather")}}
)

stg_yellow_taxi_trips AS (
    SELECT *
    FROM {{source("staging_data","stg_yellow_taxi_trips")}}
),

-- Jointure de stg_yellow_taxi_trips et  stg_weather

stg_high_volume_vehicule_trips AS (
    SELECT *
    FROM {{source("staging_data","stg_high_volume_vehicule_trips")}}
),

stg_for_hire_vehicule_trips AS (
    SELECT *
    FROM {{source("staging_data","stg_for_hire_vehicule_trips")}}
),


yellow_taxi_trips_and_weather AS (
    SELECT 
        pickup_datetime, 
        dropoff_datetime, 
        pickup_location_id, 
        dropoff_location_id, 
        passenger_count, 
        trip_distance_miles, 
        fare_amount, 
        extra_charges, 
        mta_tax, tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge, 
        airport_fee, 
        store_and_fwd_flag, 
        service_type, 
        loaded_at,
        CASE 
            WHEN rate_code_id = 1 THEN 'Standard rate'
            WHEN rate_code_id = 2 THEN 'JFK'
            WHEN rate_code_id = 3 THEN 'Newark'
            WHEN rate_code_id = 4 THEN 'Nassau or Westchester'
            WHEN rate_code_id = 5 THEN 'Negotiated fare'
            WHEN rate_code_id = 6 THEN 'Group ride '
            ELSE NULL
        END AS rate_code_str,
        CASE 
            WHEN payment_type_id = 1 THEN 'Credit card'
            WHEN payment_type_id = 2 THEN 'Cash'     
            WHEN payment_type_id = 3 THEN 'No charge'
            WHEN payment_type_id = 4 THEN 'Dispute'
            WHEN payment_type_id = 5 THEN 'Unknown'
            WHEN payment_type_id = 6 THEN 'Voided trip' 
            ELSE NULL
        END AS payment_type_str,
        CASE WHEN w.weather_snow_mm>0 THEN TRUE ELSE FALSE END AS is_snowy,
        CASE WHEN w.weather_precip_mm>0 THEN TRUE ELSE FALSE END AS is_rainy,
--        CASE WHEN w.weather_snow_mm>0 THEN TRUE ELSE FALSE END AS is_heure_pointe,
        

    FROM stg_yellow_taxi_trips as y
    LEFT JOIN stg_weather as w
    ON w.weather_date = DATE(y.pickup_datetime)
    WHERE y.payment_type_str IS NOT NULL
    AND y.rate_code_str IS NOT NULL
),

high_volume_vehicule_trips_and_weather AS (
    SELECT * 
    FROM stg_high_volume_vehicule_trips as h
    LEFT JOIN stg_weather as w
    ON w.weather_date = DATE(h.pickup_datetime)
),

for_hire_vehicule_trips_and_weather AS (
    SELECT * 
    FROM stg_for_hire_vehicule_trips as h
    LEFT JOIN stg_weather as w
    ON w.eather_date = DATE(h.pickup_datetime)
)

all_table as (
    SELECT 
)


