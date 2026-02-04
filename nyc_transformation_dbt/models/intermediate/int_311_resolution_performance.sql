{{
  config(
    materialized='table',
    partition_by={
      "field": "month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["agency", "complaint_type"]
  )
}}

WITH complaints AS (
    SELECT *
    FROM {{ ref('stg_311_requests') }}
    WHERE request_created_at IS NOT NULL
),

-- Agrégation par agence, type de plainte, et mois
performance_metrics AS (
    SELECT
        -- Dimensions
        DATE_TRUNC(request_created_at, MONTH) AS month,
        agency,
        agency_name,
        complaint_type,
        
        -- Volume de plaintes
        COUNT(*) AS total_complaints,
        
        -- Statuts
        COUNTIF(status = 'Closed') AS closed_complaints,
        COUNTIF(status = 'Open') AS open_complaints,
        COUNTIF(status = 'Pending') AS pending_complaints,
        COUNTIF(status IN ('Assigned', 'In Progress', 'Started')) AS in_progress_complaints,
        
        -- % de fermeture
        ROUND(COUNTIF(status = 'Closed') * 100.0 / COUNT(*), 2) AS pct_closed,
        
        -- Temps de résolution (seulement pour plaintes fermées)
        ROUND(
            AVG(
                CASE WHEN status = 'Closed' AND request_closed_at IS NOT NULL
                THEN DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY)
                END
            ),
            1
        ) AS avg_resolution_days,
        
        ROUND(
            APPROX_QUANTILES(
                CASE WHEN status = 'Closed' AND request_closed_at IS NOT NULL
                THEN DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY)
                END,
                2
            )[OFFSET(1)],
            1
        ) AS median_resolution_days,
        
        -- Résolution rapide (< 7 jours)
        COUNTIF(
            status = 'Closed' 
            AND request_closed_at IS NOT NULL
            AND DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY) <= 7
        ) AS resolved_within_week,
        
        ROUND(
            COUNTIF(
                status = 'Closed' 
                AND DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY) <= 7
            ) * 100.0 / NULLIF(COUNTIF(status = 'Closed'), 0),
            2
        ) AS pct_resolved_within_week,
        
        -- Résolution sous 24h
        COUNTIF(
            status = 'Closed' 
            AND request_closed_at IS NOT NULL
            AND DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY) <= 1
        ) AS resolved_within_24h,
        
        ROUND(
            COUNTIF(
                status = 'Closed' 
                AND DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY) <= 1
            ) * 100.0 / NULLIF(COUNTIF(status = 'Closed'), 0),
            2
        ) AS pct_resolved_within_24h,
        
        -- Résolution lente (> 30 jours)
        COUNTIF(
            status = 'Closed' 
            AND request_closed_at IS NOT NULL
            AND DATE_DIFF(DATE(request_closed_at), DATE(request_created_at), DAY) > 30
        ) AS resolved_over_30_days,
        
        -- Plaintes anciennes encore ouvertes
        COUNTIF(
            status IN ('Open', 'Pending', 'Assigned', 'In Progress')
            AND DATE_DIFF(CURRENT_DATE(), DATE(request_created_at), DAY) > 30
        ) AS open_over_30_days,
        
        -- Distribution géographique
        COUNT(DISTINCT borough) AS boroughs_covered,
        COUNT(DISTINCT zip_code) AS zip_codes_covered,
        
        -- Métadonnées
        MIN(request_created_at) AS period_start,
        MAX(request_created_at) AS period_end,
        CURRENT_TIMESTAMP() AS aggregated_at
        
    FROM complaints
    GROUP BY 
        DATE_TRUNC(request_created_at, MONTH),
        agency,
        agency_name,
        complaint_type
),

-- Ajouter des rankings
performance_with_rankings AS (
    SELECT
        *,
        
        -- Ranking par rapidité de résolution (par type de plainte)
        RANK() OVER (
            PARTITION BY month, complaint_type 
            ORDER BY avg_resolution_days ASC
        ) AS speed_rank_by_complaint_type,
        
        -- Ranking par volume traité (par agence)
        RANK() OVER (
            PARTITION BY month, agency 
            ORDER BY total_complaints DESC
        ) AS volume_rank_by_agency,
        
        -- Score de performance (0-100)
        ROUND(
            (pct_closed * 0.4) +  -- 40% poids sur taux de fermeture
            (pct_resolved_within_week * 0.3) +  -- 30% sur rapidité
            (GREATEST(0, 100 - avg_resolution_days * 2) * 0.3),  -- 30% sur temps moyen
            2
        ) AS performance_score
        
    FROM performance_metrics
)

SELECT * FROM performance_with_rankings