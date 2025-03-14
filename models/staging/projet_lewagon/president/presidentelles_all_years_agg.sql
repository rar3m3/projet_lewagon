{{ config(
    materialized='table'
) }}

SELECT
    annee,
    code_departement,
    max_voix_t1,
    max_voix_t2
FROM {{ ref('presidentielle2002') }}
UNION ALL
SELECT
    annee,
    code_departement,
    max_voix_t1,
    max_voix_t2
FROM {{ ref('presidentielle2007') }}
UNION ALL
SELECT
    annee,
    code_departement,
    max_voix_t1,
    max_voix_t2
FROM {{ ref('presidentielle2012') }}
UNION ALL
SELECT
    annee,
    code_departement,
    max_voix_t1,
    max_voix_t2
FROM {{ ref('presidentielle2017') }}
UNION ALL
SELECT
    annee,
    code_departement,
    max_voix_t1,
    max_voix_t2
FROM {{ ref('presidentielle2022') }}