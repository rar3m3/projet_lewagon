{{ config(
    materialized='table'
) }}
WITH all_candidates AS (
    SELECT 
        'Le Pen' AS Candidat, 
        '6h-9h' AS Tranche_Horaire, 
        ROUND(
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        ) AS Total_Temps
    FROM {{ ref('antenne_lepen_premiertour') }}
    
    UNION ALL

    SELECT 'Le Pen', '9h-18h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_lepen_premiertour') }}

    UNION ALL

    SELECT 'Le Pen', '18h-24h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_lepen_premiertour') }}

    UNION ALL

    SELECT 'Le Pen', '0h-6h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_lepen_premiertour') }}
    
    UNION ALL

    SELECT 'Macron', '6h-9h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_macron_premiertour') }}

    UNION ALL

    SELECT 'Macron', '9h-18h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_macron_premiertour') }}

    UNION ALL

    SELECT 'Macron', '18h-24h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_macron_premiertour') }}

    UNION ALL

    SELECT 'Macron', '0h-6h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_macron_premiertour') }}

    UNION ALL

    SELECT 'Mélenchon', '6h-9h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_6h_9h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_melenchon_premiertour') }}

    UNION ALL

    SELECT 'Mélenchon', '9h-18h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_9h_18h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_melenchon_premiertour') }}

    UNION ALL

    SELECT 'Mélenchon', '18h-24h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_18h_24h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_melenchon_premiertour') }}

    UNION ALL

    SELECT 'Mélenchon', '0h-6h', 
        ROUND(
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(0)] AS FLOAT64) +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(1)] AS FLOAT64) / 60 +
            SAFE_CAST(SPLIT(Total_0h_6h, ':')[SAFE_OFFSET(2)] AS FLOAT64) / 3600, 2
        )
    FROM {{ ref('antenne_melenchon_premiertour') }}
)

SELECT 
    Candidat,
    ROUND(SUM(CASE WHEN Tranche_Horaire = '6h-9h' THEN Total_Temps ELSE 0 END), 2) AS Total_6h_9h,
    ROUND(SUM(CASE WHEN Tranche_Horaire = '9h-18h' THEN Total_Temps ELSE 0 END), 2) AS Total_9h_18h,
    ROUND(SUM(CASE WHEN Tranche_Horaire = '18h-24h' THEN Total_Temps ELSE 0 END), 2) AS Total_18h_24h,
    ROUND(SUM(CASE WHEN Tranche_Horaire = '0h-6h' THEN Total_Temps ELSE 0 END), 2) AS Total_0h_6h
FROM all_candidates
GROUP BY Candidat
ORDER BY Candidat
