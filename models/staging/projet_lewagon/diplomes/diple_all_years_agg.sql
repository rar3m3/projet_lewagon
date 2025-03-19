{{ config(
    materialized='table'
) }}

SELECT 
    *
FROM {{ ref('dipl_1999_clean') }}
UNION ALL
SELECT 
    *
FROM {{ ref('dipl_2010_clean') }}
UNION ALL
SELECT 
    *
FROM {{ ref('dipl_2021_clean') }}