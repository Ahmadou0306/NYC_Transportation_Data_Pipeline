WITH zone AS (
    SELECT
        LocationID AS zone_id,
        Borough AS borough,
        Zone AS zone_name,
        service_zone AS service_zone,
        
        LOWER(Borough) = 'manhattan' AS is_manhattan,
        LOWER(Borough) = 'brooklyn' AS is_brooklyn,
        LOWER(Borough) = 'bronx' AS is_bronx,
        LOWER(Borough) = 'queens' AS is_queens,
        LOWER(Borough) = 'staten island' AS is_staten_island,
        
        CASE 
            WHEN LOWER(Zone) LIKE '%airport%'
                 OR LOWER(Zone) LIKE '%jfk%'
                 OR LOWER(Zone) LIKE '%laguardia%'
            THEN 'Airport'
            
            WHEN LOWER(Zone) LIKE '%times square%'
                 OR LOWER(Zone) LIKE '%central park%'
                 OR LOWER(Zone) LIKE '%financial district%'
            THEN 'Zones touristiques'
            
            WHEN LOWER(Zone) LIKE '%midtown%'
                 OR LOWER(Zone) LIKE '%downtown manhattan%'
            THEN 'Business Districts'
            
            ELSE 'Autre'
        END AS zone_category

    FROM {{ ref('taxi_zone_lookup') }} 
)

SELECT *
FROM zone