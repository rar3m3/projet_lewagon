{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_projet_lewagon__pres2007') }}
),
renamed AS (
  WITH aggregated_data AS (
    SELECT
      "2007" AS annee,
     code_departement,
      SUM(inscrits) AS inscrits,
      SUM(votants) AS votants,
      SUM(exprimes) AS exprimes,
      SUM(voixLAGUILLER) AS LAGUILLER,
      SUM(voixBESANCENOT) AS BESANCENOT,
      SUM(voixSCHIVARDI) AS SCHIVARDI,
      SUM(voixBUFFET) AS BUFFET,
      SUM(voixBOVE) AS BOVE,
      SUM(voixVOYNET) AS VOYNET,
      SUM(voixROYAL) AS ROYAL,
      SUM(voixNIHOUS) AS NIHOUS,
      SUM(voixBAYROU) AS BAYROU,
      SUM(voixSARKOZY) AS SARKOZY,
      SUM(voixVILLIERS) AS VILLIERS,
      SUM(voixLEPEN) AS LEPEN,
      SUM(voixT2ROYAL) AS T2ROYAL,
      SUM(voixT2SARKOZY) AS T2SARKOZY
    FROM 
      source
    GROUP BY code_departement
  )
  SELECT 
    ad.*,
    x.column_name AS max_voix_t1,
    x2.column_name AS max_voix_t2
  FROM 
    aggregated_data ad,
    UNNEST([ 
      STRUCT('LAGUILLER' AS column_name, LAGUILLER AS value),
      STRUCT('BESANCENOT' AS column_name, BESANCENOT AS value),
      STRUCT('SCHIVARDI' AS column_name, SCHIVARDI AS value),
      STRUCT('BUFFET' AS column_name, BUFFET AS value),
      STRUCT('BOVE' AS column_name, BOVE AS value),
      STRUCT('VOYNET' AS column_name, VOYNET AS value),
      STRUCT('ROYAL' AS column_name, ROYAL AS value),
      STRUCT('NIHOUS' AS column_name, NIHOUS AS value),
      STRUCT('BAYROU' AS column_name, BAYROU AS value),
      STRUCT('SARKOZY' AS column_name, SARKOZY AS value),
      STRUCT('VILLIERS' AS column_name, VILLIERS AS value),
      STRUCT('LEPEN' AS column_name, LEPEN AS value)
    ]) x,
    UNNEST([ 
      STRUCT('T2ROYAL' AS column_name, T2ROYAL AS value),
      STRUCT('T2SARKOZY' AS column_name, T2SARKOZY AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      LAGUILLER, BESANCENOT, SCHIVARDI, 
      BUFFET, BOVE, VOYNET, 
      ROYAL, NIHOUS, BAYROU, 
      SARKOZY, VILLIERS, LEPEN
    )
    AND 
    x2.value = GREATEST(
      T2ROYAL, 
      T2SARKOZY
    )
)
SELECT * 
FROM renamed
