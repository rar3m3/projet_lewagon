{{ config(
    materialized='table'
) }}

WITH agg_table AS(
select
    PARSE_DATE('%Y',annee) AS annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2002') }}
UNPIVOT (score for candidat in (chirac
,le_pen
))
UNION ALL
select
    PARSE_DATE('%Y',annee) AS annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2007') }}
UNPIVOT (score for candidat in (sarkozy
,royal))
UNION ALL
select
    PARSE_DATE('%Y',annee) AS annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2012') }}
UNPIVOT (score for candidat in (hollande
,sarkozy))
UNION ALL
select
    PARSE_DATE('%Y',annee) AS annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2017') }}
UNPIVOT (score for candidat in (macron
,le_pen))
UNION ALL
select
    PARSE_DATE('%Y',annee) AS annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2022') }}
UNPIVOT (score for candidat in (macron
,le_pen))
)
SELECT 
    *
    , CASE 
        ------2002
        WHEN (candidat LIKE "%chirac%" AND annee = "2002-01-01") THEN "Droite"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2002-01-01") THEN "Extreme Droite"
        ------2007
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2007-01-01") THEN "Droite"
        WHEN (candidat LIKE "%royal%" AND annee = "2007-01-01") THEN "Gauche"
        -------2012
        WHEN (candidat LIKE "%hollande%" AND annee = "2012-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2012-01-01") THEN "Droite"
        -------2017
        WHEN (candidat LIKE "%macron%" AND annee = "2017-01-01") THEN "Centre"
        WHEN (candidat LIKE "%_pen%" AND annee = "2017-01-01") THEN "Extreme Droite"
        -------2022
        WHEN (candidat LIKE "%macron%" AND annee = "2022-01-01") THEN "Centre"
        WHEN (candidat LIKE "%_pen%" AND annee = "2022-01-01") THEN "Extreme Droite"
    END AS affiliation
    , CASE 
        ------2002
        WHEN (candidat LIKE "%chirac%" AND annee = "2002-01-01") THEN "RPR"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2002-01-01") THEN "FN/RN"
        ------2007
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2007-01-01") THEN "UMP"
        WHEN (candidat LIKE "%royal%" AND annee = "2007-01-01") THEN "PS"
        -------2012
        WHEN (candidat LIKE "%hollande%" AND annee = "2012-01-01") THEN "PS"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2012-01-01") THEN "FN/RN"
        -------2017
        WHEN (candidat LIKE "%macron%" AND annee = "2017-01-01") THEN "EM/LREM"
        WHEN (candidat LIKE "%_pen%" AND annee = "2017-01-01") THEN "FN/RN"
        -------2022
        WHEN (candidat LIKE "%macron%" AND annee = "2022-01-01") THEN "EM/LREM"
        WHEN (candidat LIKE "%_pen%" AND annee = "2022-01-01") THEN "FN/RN"
    END AS parti
From agg_table


