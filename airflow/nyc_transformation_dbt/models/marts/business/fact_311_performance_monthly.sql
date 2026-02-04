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

WITH performance_monthly AS (
    SELECT
        -- Clés temporelles
        p.month,
        EXTRACT(YEAR FROM p.month) AS year,
        EXTRACT(MONTH FROM p.month) AS month_number,
        FORMAT_DATE('%B', p.month) AS month_name,
        EXTRACT(QUARTER FROM p.month) AS quarter,
        
        -- Dimensions business
        p.agency,
        p.agency_name,
        p.complaint_type,
        
        -- Métriques de volume
        p.total_complaints,
        p.closed_complaints,
        p.open_complaints,
        p.pending_complaints,
        p.in_progress_complaints,
        
        -- Métriques de performance - Taux de résolution
        p.pct_closed,
        
        -- Métriques de performance - Temps de résolution
        p.avg_resolution_days,
        p.median_resolution_days,
        
        -- Métriques de performance - Résolution rapide
        p.resolved_within_week,
        p.pct_resolved_within_week,
        p.resolved_within_24h,
        p.pct_resolved_within_24h,
        
        -- Métriques de performance - Résolution lente
        p.resolved_over_30_days,
        p.open_over_30_days,
        
        -- Distribution géographique
        p.boroughs_covered,
        p.zip_codes_covered,
        
        -- Rankings et scores
        p.speed_rank_by_complaint_type,
        p.volume_rank_by_agency,
        p.performance_score,
        
        -- Flags de performance
        CASE 
            WHEN p.performance_score >= 80 THEN 'Excellent'
            WHEN p.performance_score >= 60 THEN 'Bon'
            WHEN p.performance_score >= 40 THEN 'Moyen'
            ELSE 'À améliorer'
        END AS performance_category,
        
        CASE 
            WHEN p.avg_resolution_days <= 7 THEN TRUE 
            ELSE FALSE 
        END AS is_fast_agency,
        
        CASE 
            WHEN p.pct_closed >= 90 THEN TRUE 
            ELSE FALSE 
        END AS is_high_closure_rate,
        
        -- Période d'observation
        p.period_start,
        p.period_end,
        
        -- Métadonnées
        p.aggregated_at AS loaded_at
        
    FROM {{ ref('int_311_resolution_performance') }} AS p
)

SELECT * FROM performance_monthly